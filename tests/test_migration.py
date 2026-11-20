"""Migration-kernel tests over a scriptable in-memory network.

Covered: graceful handoff in the middle of a shared action, late transfer
data, clients learning about the new host out of sync, crash takeover from
checkpoint, exactly-once inputs across reconnects, no-empty-scene continuity,
and refusal to host on unverifiable transfer data.
"""

from __future__ import annotations

import asyncio
import unittest

from host_migration.node import Node
from host_migration.protocol import state_hash
from host_migration.transport import MemNetwork, MemTransport

TICK = 0.02


async def wait_until(pred, timeout=4.0):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if pred():
            return True
        await asyncio.sleep(0.01)
    return False


class Cluster:
    def __init__(self, n=3, delay_rule=None):
        self.net = MemNetwork(delay_rule=delay_rule)
        self.nodes: dict[str, Node] = {}
        self.n = n

    async def start(self):
        for i in range(self.n):
            nid = f"n{i}"
            t = MemTransport(nid, self.net)
            self.nodes[nid] = Node(
                nid, [f"n{j}" for j in range(self.n) if j != i], t,
                initial_host=(i == 0), host_id="n0",
                session_id="session-test", tick_dt=TICK,
                checkpoint_every=5, heartbeat_timeout=0.4,
                handoff_ack_timeout=1.0, new_host_timeout=1.5)
        for nd in self.nodes.values():
            await nd.start()
        ok = await wait_until(
            lambda: all(nd.phase in ("hosting", "client")
                        for nd in self.nodes.values()), 3)
        assert ok, "cluster did not converge: " + str(
            [(k, v.phase) for k, v in self.nodes.items()])

    async def stop(self):
        for nd in self.nodes.values():
            await nd.stop()

    def host(self) -> Node:
        return next(nd for nd in self.nodes.values() if nd.role == "host")


class MigrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        if getattr(self, "c", None):
            await self.c.stop()

    # -- graceful handoff in the middle of a shared multi-tick action ------

    async def test_graceful_handoff_mid_action(self):
        self.c = Cluster(3)
        await self.c.start()
        host = self.c.host()
        n1 = self.c.nodes["n1"]

        n1.submit_input("press")  # 5-tick in-progress action
        ok = await wait_until(
            lambda: host.world.players["n1"]["action"] is not None, 2)
        self.assertTrue(ok, "press action never started on host")

        result = await host.start_handoff()
        self.assertTrue(result["ok"], result)
        ok = await wait_until(
            lambda: self.c.nodes["n1"].role == "host"
            and self.c.nodes["n1"]._migration_report
            and self.c.nodes["n1"]._migration_report["complete"], 3)
        self.assertTrue(ok, "new host did not complete verified migration")

        new_host = self.c.nodes["n1"]
        report = new_host._migration_report
        # verifiable boundary: clock resumes exactly at boundary+1
        self.assertEqual(report["resume_tick"], report["boundary_tick"] + 1)
        self.assertTrue(report["hash_verified"])
        # session survived: not a reopened empty scene
        self.assertEqual(new_host.world.session, "session-test")
        self.assertEqual(set(new_host.world.entity_ids()),
                         {"n0", "n1", "n2", "plat1", "door1", "btn1", "gear1"})
        # the in-progress action completes after the takeover
        ok = await wait_until(lambda: new_host.world.press_count == 1, 2)
        self.assertTrue(ok, "in-progress press action was lost in transfer")
        # mechanism kept moving across the boundary
        ok = await wait_until(lambda: new_host.world.door["open"] > 0, 2)
        self.assertTrue(ok, "door mechanism state was lost")
        # every client switched on the explicit epoch with continuity proof
        for nid in ("n0", "n2"):
            nd = self.c.nodes[nid]
            ok = await wait_until(lambda: nd.continuity, 2)
            self.assertTrue(ok, f"{nid} never switched")
            rec = nd.continuity[-1]
            self.assertEqual(rec["to_epoch"], 2)
            self.assertEqual(rec["resumed_at"], rec["boundary_tick"] + 1)
            self.assertTrue(rec["hash_ok"])
            self.assertEqual(rec["session"], "session-test")

    # -- transfer data arrives late: world must not run ahead ---------------

    async def test_late_transfer_data(self):
        def rule(msg, src, dst):
            if msg["type"] == "migrate_offer":
                return 0.6  # transfer data is late
            return 0.0

        self.c = Cluster(3, delay_rule=rule)
        await self.c.start()
        host = self.c.host()
        n2 = self.c.nodes["n2"]

        task = asyncio.create_task(host.start_handoff())
        ok = await wait_until(lambda: host._sealed, 1)
        self.assertTrue(ok)
        boundary = host.world.tick

        # while the offer is in flight, nobody advances past the boundary
        await asyncio.sleep(0.3)
        self.assertLessEqual(host.world.tick, boundary)
        self.assertLessEqual(n2.last_tick, boundary)
        self.assertEqual(n2.phase, "switching(buffering)")
        self.assertFalse(any(nd.role == "host" and nd.id != "n0"
                             for nd in self.c.nodes.values()),
                         "new host claimed completion before data arrived")

        # client input during the declared buffering window is queued
        n2.submit_input("press")
        self.assertTrue(n2.buffering and n2.unacked)

        self.assertTrue((await task)["ok"])
        ok = await wait_until(
            lambda: self.c.nodes["n1"].role == "host"
            and self.c.nodes["n1"].world.press_count == 1, 4)
        self.assertTrue(ok, "buffered input was not applied after late transfer")
        rec = n2.continuity[-1]
        self.assertEqual(rec["resumed_at"], rec["boundary_tick"] + 1)

    # -- clients learn about the new host out of sync ------------------------

    async def test_clients_learn_new_host_out_of_sync(self):
        def rule(msg, src, dst):
            if msg["type"] == "new_host" and dst == "n2":
                return 0.7  # n2 hears about the new host late
            return 0.0

        self.c = Cluster(3, delay_rule=rule)
        await self.c.start()
        host = self.c.host()
        n2 = self.c.nodes["n2"]

        await host.start_handoff()
        ok = await wait_until(
            lambda: self.c.nodes["n1"].role == "host"
            and self.c.nodes["n1"]._migration_report["complete"], 3)
        self.assertTrue(ok)

        # n2 has not heard yet: still buffering, has not switched
        self.assertEqual(n2.epoch, 1)
        self.assertTrue(n2.buffering)
        n2.submit_input("press")
        await asyncio.sleep(0.2)
        self.assertEqual(n2.epoch, 1, "n2 switched before announcement arrived")

        # delayed delivery arrives: n2 catches up and replays exactly once
        ok = await wait_until(lambda: n2.epoch == 2 and n2.continuity, 3)
        self.assertTrue(ok)
        new_host = self.c.nodes["n1"]
        ok = await wait_until(lambda: new_host.world.press_count == 1, 3)
        self.assertTrue(ok, "n2's buffered input was lost")
        # duplicate resume (reconnect) must not re-execute the input
        new_host._on_resume({"cid": "n2", "epoch": 2,
                             "inputs": [{"seq": 1, "action": "press"}]})
        await asyncio.sleep(0.3)
        self.assertEqual(new_host.world.press_count, 1,
                         "reconnect replay re-executed an input")
        self.assertFalse(any(e["kind"] == "error" and "gap" in e["msg"]
                             for e in n2.events),
                         "n2 observed a tick gap after catching up")

    # -- crash takeover: successor resumes from last verified checkpoint ----

    async def test_crash_takeover_mid_action(self):
        self.c = Cluster(3)
        await self.c.start()
        host = self.c.host()
        n1, n2 = self.c.nodes["n1"], self.c.nodes["n2"]

        n2.submit_input("press")
        ok = await wait_until(
            lambda: host.world.players["n2"]["action"] is not None, 2)
        self.assertTrue(ok)
        # wait for a broadcast checkpoint with full membership
        ok = await wait_until(
            lambda: n1.last_checkpoint is not None
            and set(n1.last_checkpoint["snapshot"]["players"]) == {"n0", "n1", "n2"},
            2)
        self.assertTrue(ok, "no full-membership checkpoint before crash")
        cp_tick = n1.last_checkpoint["tick"]
        cp_entities = set(n1.last_checkpoint["snapshot"]["players"])

        await host.simulate_crash()  # abrupt: no goodbye, no transfer
        ok = await wait_until(
            lambda: n1.role == "host" and n1._migration_report
            and n1._migration_report["complete"], 4)
        self.assertTrue(ok, "successor did not take over after crash")

        report = n1._migration_report
        self.assertEqual(report["resume_tick"], report["boundary_tick"] + 1)
        self.assertGreaterEqual(report["boundary_tick"], cp_tick)
        # continuous scene: same session, same object identities, not empty
        self.assertEqual(n1.world.session, "session-test")
        self.assertEqual(set(n1.world.players), cp_entities)
        self.assertIn("gear1", n1.world.entity_ids())
        # the interrupted action still completes on the surviving world
        ok = await wait_until(lambda: n1.world.press_count == 1, 3)
        self.assertTrue(ok, "in-progress action lost across crash takeover")
        # remaining client switched over with continuity proof
        ok = await wait_until(lambda: n2.continuity, 3)
        self.assertTrue(ok)
        self.assertEqual(n2.continuity[-1]["to_epoch"], 2)

    # -- inputs are never re-executed on reconnect ---------------------------

    async def test_inputs_not_duplicated_on_reconnect(self):
        self.c = Cluster(3)
        await self.c.start()
        host = self.c.host()

        self.c.nodes["n1"].submit_input("press")
        ok = await wait_until(lambda: host.world.press_count == 1, 3)
        self.assertTrue(ok)
        self.assertEqual(host.last_applied["n1"], 1)

        # reconnect replay: same input offered again via both paths
        host._on_input({"cid": "n1", "seq": 1, "action": "press"})
        host._on_resume({"cid": "n1", "epoch": 1,
                         "inputs": [{"seq": 1, "action": "press"}]})
        await asyncio.sleep(0.3)
        self.assertEqual(host.world.press_count, 1,
                         "input re-executed after reconnect")

    # -- unverifiable transfer data is refused and reported ------------------

    async def test_unverifiable_offer_is_refused(self):
        self.c = Cluster(3)
        await self.c.start()
        host = self.c.host()
        candidate = self.c.nodes["n1"]

        snap = host.world.snapshot()
        forged = {"type": "migrate_offer", "epoch": 1, "snapshot": snap,
                  "hash": state_hash(snap), "succession": ["n1", "n2"],
                  "boundary": {"tick": snap["tick"],
                               "last_applied": dict(host.last_applied)}}
        forged["snapshot"]["platform"]["x"] += 50  # tamper after hashing
        candidate._on_migrate_offer("n0", forged)

        self.assertEqual(candidate.role, "client",
                         "candidate claimed host on unverifiable data")
        ok = await wait_until(
            lambda: any("hash mismatch" in e["msg"] for e in candidate.events), 1)
        self.assertTrue(ok, "refusal was not fed back into kernel events")

    async def test_handoff_without_candidate_fails_back_to_ui(self):
        self.c = Cluster(1)
        await self.c.start()
        host = self.c.host()
        result = await host.start_handoff()
        self.assertFalse(result["ok"])
        self.assertTrue(any(e["kind"] == "error" for e in host.events),
                        "failure was not reported back to the kernel/UI")

    # -- staged handoff: candidate drops after loading the snapshot ---------

    async def test_candidate_drops_after_load_next_takes_over(self):
        """n1 loads the snapshot then disconnects before becoming host; the
        handoff falls to n2. n1's late completion callback must be ignored."""
        self.c = Cluster(3)
        await self.c.start()
        host = self.c.host()          # n0
        n1, n2 = self.c.nodes["n1"], self.c.nodes["n2"]
        n1.post_load_delay = 0.5

        task = asyncio.create_task(host.start_handoff("revocable_log"))
        ok = await wait_until(lambda: n1.phase == "snapshot_loaded", 2)
        self.assertTrue(ok, "n1 never loaded the snapshot")
        attempt_id = host._handoff["id"]

        n1._stopped = True            # candidate disappears after the load
        await n1.transport.crash()
        res = await asyncio.wait_for(task, 5)
        self.assertTrue(res["ok"], res)
        self.assertEqual(res["candidate"], "n2")

        ok = await wait_until(
            lambda: n2.role == "host" and n2._migration_report
            and n2._migration_report["complete"], 4)
        self.assertTrue(ok, "n2 did not complete the fallback handoff")
        self.assertEqual(host._handoff["rejected"], ["n1"])
        self.assertEqual(host._handoff["stage"], "submit_inputs")

        # The dropped candidate's completion callback arrives afterwards.
        # It must not be credited to the live handoff.
        host._on_migrate_ack("n1", {"type": "migrate_ack", "epoch": 1,
                                    "attempt": attempt_id, "stage": "loaded"})
        self.assertTrue(any("stale completion callback" in e["msg"]
                            for e in host.events))
        self.assertEqual(n2.role, "host")

    async def test_frozen_once_publishes_single_snapshot(self):
        """frozen_once never republishes to a second candidate."""
        self.c = Cluster(3)
        await self.c.start()
        host = self.c.host()
        n1 = self.c.nodes["n1"]
        n1.post_load_delay = 0.5

        task = asyncio.create_task(host.start_handoff("frozen_once"))
        ok = await wait_until(lambda: n1.phase == "snapshot_loaded", 2)
        self.assertTrue(ok)
        n1._stopped = True
        await n1.transport.crash()
        res = await asyncio.wait_for(task, 5)
        self.assertFalse(res["ok"], "frozen_once should abort, not republish")
        self.assertEqual(host.publish_count, 1)
        self.assertEqual(host._handoff["publish_count"], 1)
        self.assertNotIn("n2", host._handoff["rejected"])
        # old host resumed normally and keeps serving
        self.assertTrue(host._sealed is False)
        self.assertEqual(host.phase, "hosting")

    # -- the replay scenario: two takeovers + one voluntary exit ------------

    async def test_two_takeovers_one_exit_inventory_boundary(self):
        self.c = Cluster(3)
        await self.c.start()
        n0, n1, n2 = (self.c.nodes[k] for k in ("n0", "n1", "n2"))

        async def until(pred, t=4):
            ok = await wait_until(pred, t)
            self.assertTrue(ok)

        # Step 1: n1 picks up one item; the running host applies it.
        n1.submit_input("pickup")
        await until(lambda: n0.world.items_created == 1)
        self.assertEqual(n0.world.inventory["n1"], ["item-n1-1"])

        # Straddle the first handoff: the old host accepts a bag op for
        # itself but the simulator never applies/broadcasts it before the
        # boundary seals. It is accepted-but-unconfirmed.
        n0.submit_input("pickup")            # n0 seq 1, sits in pending
        res = await n0.start_handoff("revocable_log")
        self.assertTrue(res["ok"], res)
        old_view = n0.ui_state()["handoff"]
        self.assertEqual(old_view["stage"], "submit_inputs")
        stages = [s["stage"] for s in old_view["steps"]]
        self.assertEqual(stages[0], "select_candidate")
        self.assertIn("load_snapshot", stages)
        self.assertEqual(stages[-1], "submit_inputs")
        self.assertEqual(old_view["confirmed"].get("n1"), 1)
        self.assertEqual([(u["cid"], u["seq"]) for u in old_view["unconfirmed"]],
                         [("n0", 1)])

        h1 = n1
        await until(lambda: h1.role == "host" and h1._migration_report
                    and h1._migration_report["complete"])
        # The migrated step executed; the departing host's own resend of the
        # same step is dropped as a duplicate. Exactly one item enters.
        await until(lambda: h1.world.items_created == 2)
        self.assertEqual(h1.world.inventory["n0"], ["item-n0-1"])
        rec = [e for e in h1.input_log if e["cid"] == "n0" and e["seq"] == 1]
        self.assertTrue(any(e["source"] == "migrated" and e["status"] == "applied"
                            for e in rec), rec)
        self.assertTrue(any(e["status"] == "dropped_duplicate"
                            and e["source"] == "client_resend" for e in rec),
                        "the resent migrated step was not deduplicated")

        # Step 2: another pickup, then a crash takeover to n2. The step must
        # be inside a verified checkpoint first, since crash takeover resumes
        # from the last checkpoint rather than inventing later state.
        n1.submit_input("pickup")            # n1 seq 2, applied on n1
        await until(lambda: h1.world.items_created == 3)
        await until(lambda: n2.last_checkpoint is not None
                    and n2.last_checkpoint["snapshot"].get("items_created") == 3)
        await h1.simulate_crash()
        await until(lambda: n2.role == "host" and n2._migration_report
                    and n2._migration_report["complete"])
        h2 = n2
        self.assertEqual(h2.world.items_created, 3)
        self.assertIn("item-n1-2", h2.world.inventory["n1"])

        # Step 3: voluntary exit of n2 -> n0 (the only remaining candidate).
        exit_res = await n2.voluntary_exit("revocable_log")
        self.assertTrue(exit_res["ok"], exit_res)
        await until(lambda: n0.role == "host" and n0._migration_report
                    and n0._migration_report["complete"])

        # Ownership survived every switch; the straddling op is one item.
        self.assertEqual(n0.world.items_created, 3)
        self.assertEqual(n0.world.inventory["n0"], ["item-n0-1"])
        self.assertEqual(n0.world.inventory["n1"], ["item-n1-1", "item-n1-2"])
        # The audit log survived every takeover and attributes each step.
        self.assertGreaterEqual(len({e["epoch"] for e in n0.input_log}), 2)
        applied = [e for e in n0.input_log
                   if e["cid"] == "n1" and e["status"] == "applied"]
        self.assertEqual(sorted({e["seq"] for e in applied}), [1, 2])


if __name__ == "__main__":
    unittest.main()
