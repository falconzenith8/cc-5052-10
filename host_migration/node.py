"""Host-migration kernel.

Roles: exactly one host advances the world simulation and applies player
inputs; clients send sequenced inputs and render from host checkpoints.

Migration: the host seals a verifiable input boundary (tick + per-client
applied sequence numbers), transfers the full world snapshot (object
identities, in-progress actions, mechanisms, sim clock) to a candidate, and
only then may the candidate resume the clock at boundary+1 under epoch+1.
Clients switch on the explicit epoch, buffer inputs during the transition
(declared local-queue mode) and replay only inputs past the boundary, so
reconnects never re-execute. Crash takeover resumes from the last verified
checkpoint via the deterministic succession list.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque

from .protocol import state_hash, short
from .world import World

BUFFER_MODE = "local_queue"  # declared transition input-buffering method


class Node:
    def __init__(self, node_id: str, peer_ids: list[str], transport, *,
                 initial_host: bool = False, host_id: str | None = None,
                 session_id: str = "session-0000", tick_dt: float = 0.05,
                 checkpoint_every: int = 10, heartbeat_timeout: float = 1.0,
                 handoff_ack_timeout: float = 2.0,
                 new_host_timeout: float = 3.0):
        self.id = node_id
        self.peers = peer_ids
        self.transport = transport
        self.tick_dt = tick_dt
        self.checkpoint_every = checkpoint_every
        self.heartbeat_timeout = heartbeat_timeout
        self.handoff_ack_timeout = handoff_ack_timeout
        self.new_host_timeout = new_host_timeout

        self.role = "host" if initial_host else "client"
        self.host_id = node_id if initial_host else host_id
        self.epoch = 1
        self.phase = "hosting" if initial_host else "joining"
        self.world: World | None = World.genesis(session_id) if initial_host else None
        self.session_id = session_id

        # host-side input bookkeeping: verifiable boundary material
        self.last_applied: dict[str, int] = {}
        self.pending: list[tuple[str, int, str]] = []
        self.clients: list[str] = []  # join order = succession list
        self._sealed = False

        # client-side input bookkeeping
        self.next_seq = 1
        self.unacked: dict[int, str] = {}
        self.buffering = False
        self.last_checkpoint: dict | None = None
        self.succession: list[str] = []
        self._welcomed = initial_host  # client joins only on directed welcome
        self.last_tick = -1
        self._tick_floor = 0  # ticks below this are pre-sync and ignored
        self.last_hash = ""
        self.last_host_msg = time.monotonic()

        # migration state
        self._handoff_ack: asyncio.Event | None = None
        self._migration_report: dict | None = None
        self._awaiting_since: float | None = None

        self.events: deque[dict] = deque(maxlen=200)
        self.continuity: list[dict] = []
        self._tasks: list[asyncio.Task] = []
        self._stopped = False

    # ------------------------------------------------------------------ util

    def log(self, kind: str, msg: str) -> None:
        self.events.appendleft(
            {"t": round(time.monotonic(), 1), "kind": kind, "msg": msg})

    async def start(self) -> None:
        self.transport.set_handler(self._on_message)
        self.transport.set_peer_down_handler(self._on_peer_down)
        await self.transport.start()
        if self.role == "host":
            self.world.add_player(self.id)
            self.last_applied[self.id] = 0
            self._tasks.append(asyncio.create_task(self._host_loop()))
        else:
            self._tasks.append(asyncio.create_task(self._join_loop()))
        self._tasks.append(asyncio.create_task(self._monitor()))

    async def _join_loop(self) -> None:
        while not self._stopped and not self._welcomed:
            await self.transport.send(self.host_id,
                                      {"type": "hello", "id": self.id})
            await asyncio.sleep(0.3)

    async def stop(self) -> None:
        self._stopped = True
        for t in self._tasks:
            t.cancel()
        await self.transport.stop()

    async def simulate_crash(self) -> None:
        """Abrupt failure: no goodbye, in-flight state lost with the process."""
        self._stopped = True
        for t in self._tasks:
            t.cancel()
        await self.transport.crash()

    # ------------------------------------------------------------ input path

    def submit_input(self, action: str) -> None:
        seq = self.next_seq
        self.next_seq += 1
        if self.role == "host":
            if self._sealed:
                self.unacked[seq] = action  # host is becoming a client
                return
            self.pending.append((self.id, seq, action))
        else:
            self.unacked[seq] = action
            if not self.buffering:
                asyncio.create_task(self.transport.send(self.host_id, {
                    "type": "input", "cid": self.id, "seq": seq, "action": action}))
            # else: declared transition mode local_queue -> stays in unacked

    # ------------------------------------------------------------- host loop

    def _apply_pending(self) -> None:
        self.pending.sort(key=lambda x: (x[0], x[1]))
        for cid, seq, action in self.pending:
            if seq > self.last_applied.get(cid, 0):
                self.world.apply_input(cid, action)
                self.last_applied[cid] = seq
        self.pending.clear()

    async def _host_loop(self) -> None:
        while not self._stopped and self.role == "host":
            if not self._sealed:
                self._apply_pending()
                self.world.step()
                snap = self.world.snapshot()
                h = state_hash(snap)
                await self.transport.broadcast({
                    "type": "tick", "epoch": self.epoch, "tick": self.world.tick,
                    "hash": h, "last_applied": dict(self.last_applied)})
                if self.world.tick % self.checkpoint_every == 0:
                    await self._broadcast_checkpoint(snap, h)
                if (self._migration_report is not None
                        and not self._migration_report.get("ticked")):
                    r = self._migration_report
                    r["ticked"] = True
                    r["complete"] = self._report_checks()
                    self.continuity.append({
                        "session": r["session"], "from_epoch": r["from_epoch"],
                        "to_epoch": r["to_epoch"],
                        "boundary_tick": r["boundary_tick"],
                        "resumed_at": self.world.tick,
                        "hash": short(state_hash(snap)), "hash_ok": True,
                        "entities": r["entities"], "replayed_inputs": 0,
                        "reason": r["reason"]})
                    self.log("migrate", f"epoch {self.epoch} clock resumed at "
                                        f"tick {self.world.tick}")
                    # Refresh succession/input boundary everywhere at once,
                    # so a later crash never relies on pre-migration data.
                    await self._broadcast_checkpoint()
            await asyncio.sleep(self.tick_dt)

    async def _broadcast_checkpoint(self, snap=None, h=None) -> None:
        snap = snap or self.world.snapshot()
        h = h or state_hash(snap)
        succession = list(self.clients)
        await self.transport.broadcast({
            "type": "checkpoint", "epoch": self.epoch, "tick": snap["tick"],
            "snapshot": snap, "hash": h,
            "last_applied": dict(self.last_applied),
            "succession": succession})
        if self.role == "host":
            self.last_checkpoint = {"tick": snap["tick"], "snapshot": snap,
                                    "hash": h,
                                    "last_applied": dict(self.last_applied),
                                    "succession": succession}

    # ------------------------------------------------------- graceful handoff

    async def start_handoff(self) -> dict:
        if self.role != "host":
            err = "not host: cannot hand off"
            self.log("error", err)
            return {"ok": False, "error": err}
        if not self.clients:
            err = "handoff failed: no candidate client"
            self.log("error", err)
            return {"ok": False, "error": err}
        candidate = self.clients[0]
        # Seal the input boundary: stop applying, freeze the clock.
        self._sealed = True
        snap = self.world.snapshot()
        h = state_hash(snap)
        boundary = {"tick": snap["tick"], "last_applied": dict(self.last_applied)}
        self.phase = "offer_sent"
        self._handoff_ack = asyncio.Event()
        self.log("migrate", f"sealed boundary at tick {snap['tick']}, "
                            f"offering to {candidate} (hash {short(h)})")
        await self.transport.broadcast({
            "type": "migrate_begin", "epoch": self.epoch, "candidate": candidate,
            "boundary_tick": snap["tick"], "buffer_mode": BUFFER_MODE})
        ok = await self.transport.send(candidate, {
            "type": "migrate_offer", "epoch": self.epoch, "snapshot": snap,
            "hash": h, "boundary": boundary, "succession": list(self.clients)})
        if not ok:
            return await self._abort_handoff("offer send failed")
        try:
            await asyncio.wait_for(self._handoff_ack.wait(),
                                   timeout=self.handoff_ack_timeout)
        except asyncio.TimeoutError:
            return await self._abort_handoff("candidate ack timed out")
        # Handed off. Become a client of the new host; new_host message
        # (broadcast to everyone, us included) completes the switch.
        self.log("migrate", "candidate acked; old host steps down")
        return {"ok": True}

    async def _abort_handoff(self, why: str) -> dict:
        self.log("error", f"handoff aborted: {why}; resuming as host")
        self._sealed = False
        self.phase = "hosting"
        await self.transport.broadcast(
            {"type": "migrate_abort", "epoch": self.epoch})
        return {"ok": False, "error": why}

    def _on_migrate_offer(self, frm: str, msg: dict) -> None:
        if msg["epoch"] != self.epoch:
            return
        snap = msg["snapshot"]
        if state_hash(snap) != msg["hash"]:
            self.log("error", "migrate_offer hash mismatch; refusing host role")
            asyncio.create_task(self.transport.send(frm, {
                "type": "migrate_nack", "epoch": msg["epoch"]}))
            return
        # Verified. Restore full world: identities, in-progress actions, clock.
        self.world = World.from_snapshot(snap)
        self.last_applied = dict(msg["boundary"]["last_applied"])
        self.clients = [c for c in msg["succession"] if c != self.id]
        self._adopt_self_inputs()
        old_epoch = self.epoch
        self.epoch = msg["epoch"] + 1
        self.role = "host"
        self.host_id = self.id
        self._sealed = False
        self.buffering = False
        self.phase = "announcing"
        resume_tick = snap["tick"] + 1
        self._migration_report = {
            "reason": "graceful", "from_epoch": old_epoch, "to_epoch": self.epoch,
            "boundary_tick": snap["tick"], "resume_tick": resume_tick,
            "hash_verified": True, "boundary_sealed": True,
            "world_restored": True, "entities": len(self.world.entity_ids()),
            "session": self.world.session, "ticked": False, "complete": False}
        asyncio.create_task(self.transport.send(frm, {
            "type": "migrate_ack", "epoch": msg["epoch"]}))
        asyncio.create_task(self._announce_new_host(snap, msg["hash"],
                                                    msg["boundary"], "graceful"))
        self._tasks.append(asyncio.create_task(self._host_loop()))
        self.phase = "hosting"
        self.log("migrate", f"verified snapshot {short(msg['hash'])}; "
                            f"hosting epoch {self.epoch} from tick {resume_tick}")

    async def _announce_new_host(self, snap, h, boundary, reason) -> None:
        await self.transport.broadcast({
            "type": "new_host", "epoch": self.epoch, "host_id": self.id,
            "resume_tick": snap["tick"] + 1, "hash": h, "snapshot": snap,
            "boundary": boundary, "reason": reason})

    def _adopt_self_inputs(self) -> None:
        """On promotion: keep sequence continuity and re-queue own inputs
        that the boundary shows as not yet applied."""
        mine = self.last_applied.get(self.id, 0)
        for s, a in sorted(self.unacked.items()):
            if s > mine:
                self.pending.append((self.id, s, a))
        self.unacked.clear()
        self.next_seq = max(self.next_seq, mine + 1)

    # --------------------------------------------------------- crash takeover

    def _on_host_lost(self, why: str) -> None:
        if self.role != "client" or self.phase in ("failed",):
            return
        if self.last_checkpoint is None:
            self.phase = "failed"
            self.log("error", f"host lost ({why}) and no checkpoint: "
                              f"cannot migrate")
            return
        self.buffering = True  # declared local_queue mode
        lost = self.host_id
        # The succession list may be stale (pre-migration); the just-lost
        # host can never be its own successor.
        candidates = [c for c in self.succession if c != lost]
        cp = self.last_checkpoint
        if candidates and candidates[0] == self.id:
            if state_hash(cp["snapshot"]) != cp["hash"]:
                self.phase = "failed"
                self.log("error", "checkpoint hash mismatch; refusing to "
                                  "promote self")
                return
            self.phase = "announcing"
            self.log("migrate", f"host lost ({why}); promoting self from "
                                f"checkpoint tick {cp['tick']}")
            self.world = World.from_snapshot(cp["snapshot"])
            self.last_applied = dict(cp["last_applied"])
            self.clients = [c for c in self.succession if c != self.id]
            self._adopt_self_inputs()
            old_epoch = self.epoch
            self.epoch += 1
            self.role = "host"
            self.host_id = self.id
            self._sealed = False
            self.buffering = False
            self._migration_report = {
                "reason": f"crash:{why}", "from_epoch": old_epoch,
                "to_epoch": self.epoch, "boundary_tick": cp["tick"],
                "resume_tick": cp["tick"] + 1, "hash_verified": True,
                "boundary_sealed": True, "world_restored": True,
                "entities": len(self.world.entity_ids()),
                "session": self.world.session, "ticked": False,
                "complete": False}
            asyncio.create_task(self._announce_new_host(
                cp["snapshot"], cp["hash"],
                {"tick": cp["tick"], "last_applied": cp["last_applied"]},
                "crash"))
            self._tasks.append(asyncio.create_task(self._host_loop()))
            self.phase = "hosting"
        else:
            self.phase = "awaiting_new_host"
            self._awaiting_since = time.monotonic()
            self.log("migrate", f"host lost ({why}); buffering inputs, "
                                f"waiting for {candidates[0] if candidates else '?'}")

    # ---------------------------------------------------------- client switch

    def _on_new_host(self, frm: str, msg: dict) -> None:
        if msg["epoch"] != self.epoch + 1:
            self.log("error", f"ignoring new_host for epoch {msg['epoch']} "
                              f"(mine {self.epoch})")
            return
        snap = msg["snapshot"]
        if state_hash(snap) != msg["hash"]:
            self.log("error", "new_host snapshot hash mismatch; not switching")
            return
        if msg["resume_tick"] != snap["tick"] + 1:
            self.log("error", "new_host resume tick not boundary+1; not switching")
            return
        if snap["session"] != self.session_id and self.world is not None:
            self.log("error", "session id changed: refusing fake continuity")
            return
        old_epoch = self.epoch
        self.world = World.from_snapshot(snap)
        self.epoch = msg["epoch"]
        self.host_id = msg["host_id"]
        self.role = "client"
        self.phase = "client"
        self.buffering = False
        self._awaiting_since = None
        self.last_tick = -1
        self._tick_floor = msg["resume_tick"]
        self.last_hash = msg["hash"]
        self.last_host_msg = time.monotonic()
        # Input boundary is checkable: replay only what the boundary lacks.
        boundary_seq = msg["boundary"]["last_applied"].get(self.id, 0)
        replay = [(s, a) for s, a in sorted(self.unacked.items()) if s > boundary_seq]
        self.unacked = {s: a for s, a in self.unacked.items() if s > boundary_seq}
        asyncio.create_task(self.transport.send(self.host_id, {
            "type": "resume", "cid": self.id, "epoch": self.epoch,
            "inputs": [{"seq": s, "action": a} for s, a in replay]}))
        self.continuity.append({
            "session": snap["session"], "from_epoch": old_epoch,
            "to_epoch": self.epoch, "boundary_tick": snap["tick"],
            "resumed_at": msg["resume_tick"], "hash": short(msg["hash"]),
            "hash_ok": True, "entities": len(self.world.entity_ids()),
            "replayed_inputs": len(replay), "reason": msg.get("reason", "?")})
        self.log("migrate", f"switched to {self.host_id} at epoch {self.epoch}; "
                            f"resumed tick {msg['resume_tick']}, "
                            f"replayed {len(replay)} buffered inputs")

    # -------------------------------------------------------- message routing

    def _on_message(self, frm: str, msg: dict) -> None:
        t = msg["type"]
        if t == "hello" and self.role == "host":
            self._on_hello(frm, msg)
        elif t == "input" and self.role == "host":
            self._on_input(msg)
        elif t == "resume" and self.role == "host":
            self._on_resume(msg)
        elif t == "tick":
            self._on_tick(msg)
        elif t == "checkpoint":
            self._on_checkpoint(msg)
        elif t == "migrate_begin":
            self._on_migrate_begin(msg)
        elif t == "migrate_offer":
            self._on_migrate_offer(frm, msg)
        elif t == "migrate_ack" and self.role == "host":
            if self._handoff_ack is not None:
                self._handoff_ack.set()
        elif t == "migrate_nack" and self.role == "host":
            self.log("error", "candidate rejected snapshot (nack)")
            asyncio.create_task(self._abort_handoff("candidate nack"))
        elif t == "migrate_abort":
            self._on_migrate_abort(msg)
        elif t == "new_host":
            self._on_new_host(frm, msg)
        elif t == "welcome":
            self._on_welcome(msg)

    def _on_welcome(self, msg: dict) -> None:
        if self._welcomed:
            return
        self._welcomed = True
        self.session_id = msg["session"]
        self.phase = "client"
        self.log("net", f"welcomed into session {self.session_id} by "
                        f"{self.host_id}")

    def _on_hello(self, frm: str, msg: dict) -> None:
        cid = msg["id"]
        if cid not in self.clients:
            self.clients.append(cid)
            self.world.add_player(cid)
            self.last_applied.setdefault(cid, 0)
            self.log("net", f"{cid} joined; succession={self.clients}")
        asyncio.create_task(self.transport.send(cid, {
            "type": "welcome", "session": self.world.session, "epoch": self.epoch}))
        snap = self.world.snapshot()
        asyncio.create_task(self.transport.send(cid, {
            "type": "checkpoint", "epoch": self.epoch, "tick": snap["tick"],
            "snapshot": snap, "hash": state_hash(snap),
            "last_applied": dict(self.last_applied),
            "succession": list(self.clients)}))

    def _on_input(self, msg: dict) -> None:
        if self._sealed:
            return  # boundary sealed: sender will replay to the new host
        cid, seq = msg["cid"], msg["seq"]
        if seq <= self.last_applied.get(cid, 0):
            return  # duplicate (e.g. reconnect replay): never re-execute
        self.pending.append((cid, seq, msg["action"]))

    def _on_resume(self, msg: dict) -> None:
        if msg.get("epoch") != self.epoch:
            return
        cid = msg["cid"]
        if cid not in self.clients:
            self.clients.append(cid)
            self.world.add_player(cid)
            self.last_applied.setdefault(cid, 0)
        applied = 0
        for item in sorted(msg["inputs"], key=lambda i: i["seq"]):
            if item["seq"] > self.last_applied.get(cid, 0):
                self.pending.append((cid, item["seq"], item["action"]))
                applied += 1
        self.log("net", f"{cid} resumed; {applied} inputs queued past boundary")
        asyncio.create_task(self.transport.send(cid, {
            "type": "resume_ack", "epoch": self.epoch,
            "last_applied": self.last_applied.get(cid, 0)}))

    def _on_tick(self, msg: dict) -> None:
        if msg["epoch"] != self.epoch or self.role != "client":
            return
        self.last_host_msg = time.monotonic()
        if self.last_tick < 0:
            # Just (re)synced: accept the first live tick at or past the
            # verified boundary as the sync point; older ones are stale.
            if msg["tick"] < self._tick_floor:
                return
            self.last_tick = msg["tick"]
        else:
            if msg["tick"] != self.last_tick + 1:
                self.log("error", f"tick gap: {self.last_tick} -> {msg['tick']}")
            self.last_tick = msg["tick"]
        self.last_hash = msg["hash"]
        mine = msg["last_applied"].get(self.id, 0)
        self.unacked = {s: a for s, a in self.unacked.items() if s > mine}

    def _on_checkpoint(self, msg: dict) -> None:
        if msg["epoch"] != self.epoch or self.role != "client":
            return
        self.last_host_msg = time.monotonic()
        if state_hash(msg["snapshot"]) != msg["hash"]:
            self.log("error", "checkpoint hash mismatch; ignored")
            return
        if self.world is None:
            self.world = World.from_snapshot(msg["snapshot"])
            self.session_id = msg["snapshot"]["session"]
            self.last_tick = -1
            self._tick_floor = msg["tick"]
            self.last_hash = msg["hash"]
        self.last_checkpoint = {"tick": msg["tick"], "snapshot": msg["snapshot"],
                                "hash": msg["hash"],
                                "last_applied": msg["last_applied"],
                                "succession": msg["succession"]}
        self.succession = msg["succession"]

    def _on_migrate_begin(self, msg: dict) -> None:
        if msg["epoch"] != self.epoch or self.role != "client":
            return
        self.buffering = True  # declared mode: msg["buffer_mode"]
        self.phase = "switching(buffering)"
        self.log("migrate", f"migration announced by host; buffering inputs "
                            f"({msg['buffer_mode']}), boundary tick "
                            f"{msg['boundary_tick']}")

    def _on_migrate_abort(self, msg: dict) -> None:
        if msg["epoch"] != self.epoch or self.role != "client":
            return
        self.buffering = False
        self.phase = "client"
        self.log("migrate", "migration aborted; flushing buffered inputs to host")
        for s, a in sorted(self.unacked.items()):
            asyncio.create_task(self.transport.send(self.host_id, {
                "type": "input", "cid": self.id, "seq": s, "action": a}))

    def _on_peer_down(self, pid: str) -> None:
        if pid == self.host_id and self.role == "client":
            self._on_host_lost("connection lost")

    async def _monitor(self) -> None:
        while not self._stopped:
            await asyncio.sleep(0.1)
            now = time.monotonic()
            if (self.role == "client" and not self.buffering
                    and self.phase in ("client",)
                    and now - self.last_host_msg > self.heartbeat_timeout):
                self._on_host_lost("heartbeat timeout")
            if (self.phase == "awaiting_new_host" and self._awaiting_since
                    and now - self._awaiting_since > self.new_host_timeout):
                self.phase = "failed"
                self.log("error", "no new host announced before deadline; "
                                  "migration failed")

    def _report_checks(self) -> bool:
        r = self._migration_report
        return bool(r and r["hash_verified"] and r["boundary_sealed"]
                    and r["world_restored"] and r["ticked"])

    # ------------------------------------------------------------- UI bridge

    async def ui_action(self, action: str) -> dict:
        """Entry point for window controls; failures feed back into the kernel."""
        if action in ("move_left", "move_right", "stop", "press"):
            self.submit_input(action)
            return {"ok": True}
        if action == "handoff":
            return await self.start_handoff()
        return {"ok": False, "error": f"unknown action {action}"}

    def ui_state(self) -> dict:
        w = self.world.snapshot() if self.world else None
        if self.role == "host" and self.world is not None:
            tick, h = self.world.tick, short(state_hash(w))
        else:
            tick, h = self.last_tick, short(self.last_hash)
        return {
            "id": self.id, "role": self.role, "phase": self.phase,
            "epoch": self.epoch, "host_id": self.host_id,
            "session": self.session_id,
            "tick": tick,
            "hash": h,
            "buffering": self.buffering, "buffer_mode": BUFFER_MODE,
            "unacked": len(self.unacked),
            "boundary": (self._migration_report or {}).get("boundary_tick"),
            "world": w, "events": list(self.events)[:40],
            "continuity": self.continuity[-5:],
            "migration": self._migration_report,
        }
