"""Host-migration kernel.

Roles: exactly one host advances the world simulation and applies player
inputs; clients send sequenced inputs and render from host checkpoints.

Migration is split into explicit stages so each side knows exactly what has
been confirmed:

    select_candidate  -> the host picks the next client by the project's
                         existing rule (join order, first live client).
    load_snapshot     -> the candidate verifies the hash and restores the
                         world; it reports back with a completion callback.
    submit_inputs     -> the candidate is now host, announces itself and
                         accepts inputs. The old host steps down when it sees
                         that announcement, not merely the callback.

The sealed boundary records *confirmed* steps (per-client last applied
sequence) and *unconfirmed* steps the old host had accepted but not broadcast.
Depending on strategy those unconfirmed steps are either migrated in the
handoff payload or left to the client to resend; both paths are deduplicated
so an input executes at most once.

Two publication strategies are supported:

    revocable_log   keep the open handoff log and re-offer to the next
                    candidate if the chosen one disappears; a superseded
                    candidate's late completion callback is ignored.
    frozen_once     publish the frozen snapshot exactly once and never
                    republish; if that candidate disappears the handoff is
                    aborted and normal crash takeover applies later.

Every accepted / applied / dropped input is written to an input audit log
that travels with the snapshot, so a takeover viewport can trace, stage by
stage, which host accepted which player input and where duplicates died.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque

from .protocol import state_hash, short
from .world import World

BUFFER_MODE = "local_queue"  # declared transition input-buffering method
STRATEGIES = ("revocable_log", "frozen_once")


class Node:
    def __init__(self, node_id: str, peer_ids: list[str], transport, *,
                 initial_host: bool = False, host_id: str | None = None,
                 session_id: str = "session-0000", tick_dt: float = 0.05,
                 checkpoint_every: int = 10, heartbeat_timeout: float = 1.0,
                 handoff_ack_timeout: float = 2.0,
                 new_host_timeout: float = 3.0,
                 post_load_delay: float = 0.0):
        self.id = node_id
        self.peers = peer_ids
        self.transport = transport
        self.tick_dt = tick_dt
        self.checkpoint_every = checkpoint_every
        self.heartbeat_timeout = heartbeat_timeout
        self.handoff_ack_timeout = handoff_ack_timeout
        self.new_host_timeout = new_host_timeout
        self.post_load_delay = post_load_delay

        self.role = "host" if initial_host else "client"
        self.host_id = node_id if initial_host else host_id
        self.epoch = 1
        self.phase = "hosting" if initial_host else "joining"
        self.world: World | None = World.genesis(session_id) if initial_host else None
        self.session_id = session_id

        # host-side input bookkeeping: verifiable boundary material
        self.last_applied: dict[str, int] = {}
        self.pending: list[tuple[str, int, str, str]] = []  # (cid, seq, action, source)
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
        self._migration_report: dict | None = None
        self._awaiting_since: float | None = None
        self._down_peers: set[str] = set()

        # staged handoff bookkeeping
        self._handoff: dict | None = None
        self.handoff_log: list[dict] = []
        self._handoff_seq = 0
        self._current_candidate: str | None = None
        self._cand_event: asyncio.Event | None = None
        self._cand_result: str | None = None
        self._cand_reason = ""
        self.publish_count = 0
        self.last_strategy: str | None = None

        # input audit log travels with the snapshot so it survives takeovers
        self.input_log: deque[dict] = deque(maxlen=400)
        self._log_n = 0

        self.events: deque[dict] = deque(maxlen=200)
        self.continuity: list[dict] = []
        self._tasks: list[asyncio.Task] = []
        self._stopped = False

    # ------------------------------------------------------------------ util

    def log(self, kind: str, msg: str) -> None:
        self.events.appendleft(
            {"t": round(time.monotonic(), 1), "kind": kind, "msg": msg})

    def _step(self, attempt: dict, stage: str, note: str) -> None:
        attempt.setdefault("steps", []).append(
            {"stage": stage, "at": round(time.monotonic(), 3), "note": note})
        attempt["stage"] = stage
        self.log("migrate", f"[handoff {attempt['id']}] {stage}: {note}")

    def _log_input(self, cid: str, seq: int, action: str, status: str,
                   source: str) -> None:
        self._log_n += 1
        self.input_log.append({
            "eid": f"{self.id}:{self._log_n}",
            "host": self.id, "epoch": self.epoch,
            "cid": cid, "seq": seq, "action": action,
            "status": status, "source": source,
            "tick": self.world.tick if self.world else None,
            "boundary_tick": (self._handoff or {}).get("boundary_tick"),
        })

    def _adopt_input_log(self, received: list[dict] | None) -> None:
        if received and len(received) >= len(self.input_log):
            self.input_log = deque(received[-400:], maxlen=400)

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

    async def voluntary_exit(self, strategy: str = "revocable_log") -> dict:
        """Leave on purpose: hand the session over, then stop serving."""
        res = await self.start_handoff(strategy=strategy)
        if res.get("ok"):
            self.log("migrate", f"voluntary exit after handoff (epoch "
                                f"{res.get('to_epoch')})")
            await self.stop()
        return res

    # ------------------------------------------------------------ input path

    def submit_input(self, action: str) -> None:
        seq = self.next_seq
        self.next_seq += 1
        if self.role == "host":
            if self._sealed:
                self.unacked[seq] = action  # host is becoming a client
                return
            self._accept_input(self.id, seq, action, "direct")
        else:
            self.unacked[seq] = action
            if not self.buffering:
                asyncio.create_task(self.transport.send(self.host_id, {
                    "type": "input", "cid": self.id, "seq": seq, "action": action}))
            # else: declared transition mode local_queue -> stays in unacked

    def _accept_input(self, cid: str, seq: int, action: str, source: str) -> bool:
        """Queue an input at most once; duplicates are logged, not applied."""
        if seq <= self.last_applied.get(cid, 0):
            self._log_input(cid, seq, action, "dropped_duplicate", source)
            return False
        if any(cid == c and seq == s for c, s, _, _ in self.pending):
            self._log_input(cid, seq, action, "dropped_duplicate", source)
            return False
        self.pending.append((cid, seq, action, source))
        self._log_input(cid, seq, action, "queued", source)
        return True

    # ------------------------------------------------------------- host loop

    def _apply_pending(self) -> None:
        self.pending.sort(key=lambda x: (x[0], x[1]))
        for cid, seq, action, source in self.pending:
            if seq > self.last_applied.get(cid, 0):
                self.world.apply_input(cid, action, seq)
                self.last_applied[cid] = seq
                self._log_input(cid, seq, action, "applied", source)
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
            "succession": succession,
            "input_log": list(self.input_log)[-100:]})
        if self.role == "host":
            self.last_checkpoint = {"tick": snap["tick"], "snapshot": snap,
                                    "hash": h,
                                    "last_applied": dict(self.last_applied),
                                    "succession": succession,
                                    "input_log": list(self.input_log)[-100:]}

    # ------------------------------------------------------- graceful handoff

    def _election_order(self) -> list[str]:
        """Original project rule: join order, first live client, minus self."""
        return [c for c in self.clients
                if c != self.id and c not in self._down_peers]

    def _seal_boundary(self) -> list[dict]:
        """Freeze the clock and split accepted inputs into the explicit
        confirmed boundary / still-unconfirmed handover lists."""
        unconfirmed: list[dict] = []
        for cid, seq, action, source in self.pending:
            if seq > self.last_applied.get(cid, 0):
                unconfirmed.append({"cid": cid, "seq": seq, "action": action,
                                    "source": source})
                if cid == self.id:
                    # old host owns this input; keep it for a possible resend
                    self.unacked.setdefault(seq, action)
        self.pending.clear()
        return unconfirmed

    async def start_handoff(self, strategy: str = "revocable_log") -> dict:
        if self.role != "host":
            err = "not host: cannot hand off"
            self.log("error", err)
            return {"ok": False, "error": err}
        if strategy not in STRATEGIES:
            err = f"unknown strategy {strategy}"
            self.log("error", err)
            return {"ok": False, "error": err}
        candidates = self._election_order()
        if not candidates:
            err = "handoff failed: no candidate client"
            self.log("error", err)
            return {"ok": False, "error": err}

        carry_unconfirmed = strategy == "revocable_log"
        # Seal the input boundary: stop applying, freeze the clock. All of
        # this runs before the first await, so nothing slips in afterwards.
        self._sealed = True
        snap = self.world.snapshot()
        h = state_hash(snap)
        unconfirmed = self._seal_boundary()
        confirmed = dict(self.last_applied)

        self._handoff_seq += 1
        attempt = {
            "id": self._handoff_seq, "strategy": strategy,
            "from_epoch": self.epoch, "to_epoch": self.epoch + 1,
            "boundary_tick": snap["tick"], "snapshot_hash": short(h),
            "confirmed": dict(confirmed), "unconfirmed": list(unconfirmed),
            "carry_unconfirmed": carry_unconfirmed,
            "candidates": list(candidates), "candidate": None,
            "rejected": [], "publish_count": 0, "stage": "selecting",
            "done": False, "steps": [],
        }
        self._handoff = attempt
        self.handoff_log.append(attempt)
        self.last_strategy = strategy
        self._step(attempt, "select_candidate",
                   f"{candidates[0]} (order {candidates}); "
                   f"confirmed={confirmed}; unconfirmed={len(unconfirmed)}")
        self.log("migrate", f"sealed boundary at tick {snap['tick']}, "
                            f"hash {short(h)}, strategy {strategy}")

        for cand in candidates:
            if self._stopped or self.role != "host":
                break
            attempt["candidate"] = cand
            self._current_candidate = cand
            self._cand_event = asyncio.Event()
            self._cand_result = None
            self._cand_reason = ""
            attempt["publish_count"] += 1
            self.publish_count += 1
            self._step(attempt, "load_snapshot",
                       f"publish #{attempt['publish_count']} -> {cand}")

            await self.transport.broadcast({
                "type": "migrate_begin", "epoch": self.epoch,
                "candidate": cand, "boundary_tick": snap["tick"],
                "buffer_mode": BUFFER_MODE})
            payload = {
                "type": "migrate_offer", "epoch": self.epoch,
                "attempt": attempt["id"], "candidate": cand,
                "strategy": strategy, "carry_unconfirmed": carry_unconfirmed,
                "snapshot": snap, "hash": h,
                "boundary": {"tick": snap["tick"], "last_applied": dict(confirmed),
                             "unconfirmed": unconfirmed if carry_unconfirmed else []},
                "input_log": list(self.input_log)[-100:],
                "succession": list(self.clients)}
            ok = await self.transport.send(cand, payload)
            if not ok:
                attempt["rejected"].append(cand)
                self._step(attempt, "candidate_failed",
                           f"{cand}: offer could not be delivered")
                if strategy != "revocable_log":
                    break
                continue

            try:
                await asyncio.wait_for(
                    self._cand_event.wait(),
                    timeout=self.handoff_ack_timeout + self.new_host_timeout)
            except asyncio.TimeoutError:
                self._cand_result = "failed"
                self._cand_reason = "candidate did not complete in time"

            if self._cand_result == "success":
                attempt["done"] = True
                attempt["to_epoch"] = self.epoch
                self._step(attempt, "submit_inputs",
                           f"{cand} announced epoch {self.epoch}; old host "
                           f"steps down")
                return {"ok": True, "strategy": strategy, "candidate": cand,
                        "attempt": attempt["id"], "to_epoch": self.epoch,
                        "publish_count": attempt["publish_count"],
                        "unconfirmed": len(unconfirmed)}

            attempt["rejected"].append(cand)
            self._step(attempt, "candidate_failed",
                       f"{cand}: {self._cand_reason or 'no completion'}")
            self._current_candidate = None
            if strategy != "revocable_log":
                break  # frozen snapshot is published once; never republish

        return await self._abort_handoff("no candidate completed handoff",
                                         attempt)

    async def _abort_handoff(self, why: str, attempt: dict | None = None) -> dict:
        self.log("error", f"handoff aborted: {why}; resuming as host")
        if attempt is not None:
            attempt["stage"] = "abandoned"
            attempt["done"] = True
        self._current_candidate = None
        self._sealed = False
        self.phase = "hosting"
        await self.transport.broadcast(
            {"type": "migrate_abort", "epoch": self.epoch})
        return {"ok": False, "error": why}

    # ---------------------------------------------------- candidate: load

    def _on_migrate_offer(self, frm: str, msg: dict) -> None:
        if msg.get("epoch") != self.epoch:
            return
        if self.role != "client":
            return
        snap = msg["snapshot"]
        if state_hash(snap) != msg["hash"]:
            self.log("error", "migrate_offer hash mismatch; refusing host role")
            asyncio.create_task(self.transport.send(frm, {
                "type": "migrate_nack", "epoch": msg["epoch"],
                "attempt": msg.get("attempt")}))
            return
        # Stage: load_snapshot. Restore identities, in-progress actions, clock.
        self.world = World.from_snapshot(snap)
        self.last_applied = dict(msg["boundary"]["last_applied"])
        self.clients = [c for c in msg["succession"] if c != self.id]
        self._adopt_input_log(msg.get("input_log"))
        self.buffering = False
        self.phase = "snapshot_loaded"
        self.log("migrate", f"loaded snapshot {short(msg['hash'])} for "
                            f"attempt {msg.get('attempt')} at tick {snap['tick']}")
        # Completion callback: "snapshot loaded" (not yet host).
        self._tasks.append(asyncio.create_task(self._send_loaded_ack(frm, msg)))
        self._tasks.append(asyncio.create_task(self._commit_as_host(frm, msg)))

    async def _send_loaded_ack(self, frm: str, msg: dict) -> None:
        await self.transport.send(frm, {
            "type": "migrate_ack", "epoch": msg["epoch"],
            "attempt": msg.get("attempt"), "stage": "loaded"})

    async def _commit_as_host(self, frm: str, msg: dict) -> None:
        if self.post_load_delay:
            # Window in which the candidate can still disappear after loading.
            await asyncio.sleep(self.post_load_delay)
        if self._stopped or self.epoch != msg["epoch"]:
            return
        old_epoch = self.epoch
        self.epoch = msg["epoch"] + 1
        self.role = "host"
        self.host_id = self.id
        self._sealed = False
        self.buffering = False
        self.phase = "announcing"
        # Stage: submit_inputs. Carry over the old host's accepted-but-
        # unconfirmed steps, then adopt our own buffered inputs. Dedupe makes
        # the migrated copy win the race against the client's resend.
        boundary = msg["boundary"]
        if msg.get("carry_unconfirmed"):
            for item in boundary.get("unconfirmed", []):
                self._accept_input(item["cid"], item["seq"], item["action"],
                                   "migrated")
        mine = self.last_applied.get(self.id, 0)
        for s, a in sorted(self.unacked.items()):
            if s > mine:
                self._accept_input(self.id, s, a, "client_resend")
        self.unacked.clear()
        self.next_seq = max(self.next_seq, mine + 1)
        resume_tick = msg["snapshot"]["tick"] + 1
        self._migration_report = {
            "reason": "graceful", "from_epoch": old_epoch, "to_epoch": self.epoch,
            "boundary_tick": msg["snapshot"]["tick"], "resume_tick": resume_tick,
            "hash_verified": True, "boundary_sealed": True,
            "world_restored": True, "entities": len(self.world.entity_ids()),
            "session": self.world.session, "ticked": False, "complete": False,
            "strategy": msg.get("strategy"), "attempt": msg.get("attempt"),
            "carried_unconfirmed": len(boundary.get("unconfirmed", [])
                                       if msg.get("carry_unconfirmed") else []),
        }
        asyncio.create_task(self._announce_new_host(
            msg["snapshot"], msg["hash"], boundary, "graceful",
            attempt=msg.get("attempt"), strategy=msg.get("strategy")))
        self._tasks.append(asyncio.create_task(self._host_loop()))
        self.phase = "hosting"
        self.log("migrate", f"verified snapshot {short(msg['hash'])}; hosting "
                            f"epoch {self.epoch} from tick {resume_tick}")

    async def _announce_new_host(self, snap, h, boundary, reason, *,
                                 attempt=None, strategy=None) -> None:
        await self.transport.broadcast({
            "type": "new_host", "epoch": self.epoch, "host_id": self.id,
            "resume_tick": snap["tick"] + 1, "hash": h, "snapshot": snap,
            "boundary": boundary, "reason": reason, "attempt": attempt,
            "strategy": strategy, "input_log": list(self.input_log)[-100:]})

    # -------------------------------------------------------- message routing

    def _on_message(self, frm: str, msg: dict) -> None:
        self._down_peers.discard(frm)  # hearing from a peer means it is live
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
        elif t == "migrate_ack":
            self._on_migrate_ack(frm, msg)
        elif t == "migrate_nack":
            self._on_migrate_nack(frm, msg)
        elif t == "migrate_abort":
            self._on_migrate_abort(msg)
        elif t == "new_host":
            self._on_new_host(frm, msg)
        elif t == "welcome":
            self._on_welcome(msg)

    def _on_migrate_ack(self, frm: str, msg: dict) -> None:
        attempt = self._handoff
        mine = attempt and msg.get("attempt") == attempt["id"]
        if (self.role == "host" and mine
                and frm == self._current_candidate and not attempt["done"]):
            self._step(attempt, "load_snapshot",
                       f"completion callback from {frm} (loaded)")
            return
        # A superseded candidate's callback must never complete a new attempt.
        self.log("migrate", f"stale completion callback from {frm} "
                            f"(attempt {msg.get('attempt')}) ignored")

    def _on_migrate_nack(self, frm: str, msg: dict) -> None:
        attempt = self._handoff
        if (self.role == "host" and attempt
                and msg.get("attempt") == attempt["id"]
                and frm == self._current_candidate and not attempt["done"]):
            self.log("error", "candidate rejected snapshot (nack)")
            self._cand_result = "failed"
            self._cand_reason = "candidate nack"
            if self._cand_event is not None:
                self._cand_event.set()
        else:
            self.log("migrate", f"stale nack from {frm} ignored")

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
            "succession": list(self.clients),
            "input_log": list(self.input_log)[-100:]}))

    def _on_input(self, msg: dict) -> None:
        if self._sealed:
            return  # boundary sealed: sender will replay to the new host
        self._accept_input(msg["cid"], msg["seq"], msg["action"], "direct")

    def _on_resume(self, msg: dict) -> None:
        if msg.get("epoch") != self.epoch:
            return
        cid = msg["cid"]
        if cid not in self.clients:
            self.clients.append(cid)
            self.world.add_player(cid)
            self.last_applied.setdefault(cid, 0)
        queued = 0
        for item in sorted(msg["inputs"], key=lambda i: i["seq"]):
            if self._accept_input(cid, item["seq"], item["action"],
                                  "client_resend"):
                queued += 1
        self.log("net", f"{cid} resumed; {queued} inputs queued past boundary")
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
        self._adopt_input_log(msg.get("input_log"))
        self.last_checkpoint = {"tick": msg["tick"], "snapshot": msg["snapshot"],
                                "hash": msg["hash"],
                                "last_applied": msg["last_applied"],
                                "succession": msg["succession"],
                                "input_log": msg.get("input_log", [])}
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
        stepped_down = self.role == "host"
        old_epoch = self.epoch
        self._adopt_input_log(msg.get("input_log"))
        self.world = World.from_snapshot(snap)
        self.epoch = msg["epoch"]
        self.host_id = msg["host_id"]
        self.role = "client"
        self.phase = "client"
        self.buffering = False
        self._sealed = False
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
        if stepped_down:
            self.log("migrate", f"step-down confirmed by new_host epoch "
                                f"{self.epoch}; now client of {self.host_id}")
        self.log("migrate", f"switched to {self.host_id} at epoch {self.epoch}; "
                            f"resumed tick {msg['resume_tick']}, "
                            f"replayed {len(replay)} buffered inputs")
        # Complete the staged handoff on the old host: the announcement, not
        # the earlier callback, is the commit point.
        attempt = self._handoff
        if (attempt and msg.get("attempt") == attempt["id"]
                and self._current_candidate == msg["host_id"]):
            self._cand_result = "success"
            if self._cand_event is not None:
                self._cand_event.set()

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
        candidates = [c for c in self.succession
                      if c != lost and c not in self._down_peers]
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
            self._adopt_input_log(cp.get("input_log"))
            mine = self.last_applied.get(self.id, 0)
            for s, a in sorted(self.unacked.items()):
                if s > mine:
                    self._accept_input(self.id, s, a, "client_resend")
            self.unacked.clear()
            self.next_seq = max(self.next_seq, mine + 1)
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
                "complete": False, "strategy": "crash_checkpoint"}
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

    def _on_peer_down(self, pid: str) -> None:
        self._down_peers.add(pid)
        if pid == self.host_id and self.role == "client":
            self._on_host_lost("connection lost")
        attempt = self._handoff
        if (self.role == "host" and attempt and not attempt["done"]
                and pid == self._current_candidate):
            self.log("migrate", f"[handoff {attempt['id']}] candidate {pid} "
                                f"lost after snapshot stage")
            self._cand_result = "failed"
            self._cand_reason = f"candidate {pid} disconnected"
            if self._cand_event is not None:
                self._cand_event.set()

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
        if action in ("move_left", "move_right", "stop", "press", "pickup"):
            self.submit_input(action)
            return {"ok": True}
        if action == "handoff":
            return await self.start_handoff("revocable_log")
        if action == "handoff_log":
            return await self.start_handoff("revocable_log")
        if action == "handoff_frozen":
            return await self.start_handoff("frozen_once")
        if action == "exit":
            return await self.voluntary_exit("revocable_log")
        return {"ok": False, "error": f"unknown action {action}"}

    def _handoff_view(self) -> dict | None:
        if not self._handoff:
            return None
        a = self._handoff
        return {k: a[k] for k in (
            "id", "strategy", "from_epoch", "to_epoch", "boundary_tick",
            "snapshot_hash", "confirmed", "unconfirmed", "carry_unconfirmed",
            "candidates", "candidate", "rejected", "publish_count", "stage",
            "done", "steps") if k in a}

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
            "handoff": self._handoff_view(),
            "handoff_log": [
                {"id": a["id"], "strategy": a["strategy"],
                 "candidate": a["candidate"], "rejected": a["rejected"],
                 "stage": a["stage"], "done": a["done"],
                 "publish_count": a["publish_count"]}
                for a in self.handoff_log[-6:]],
            "strategy": self.last_strategy,
            "publish_count": self.publish_count,
            "input_log": list(self.input_log)[-60:],
            "inventory": (w or {}).get("inventory", {}),
            "items_created": (w or {}).get("items_created", 0),
        }
