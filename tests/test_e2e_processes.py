"""End-to-end: real multi-process cluster driven through the scene-window API.

Spawns three node processes, sends inputs and triggers host migration via
each node's HTTP window (the same API the page buttons use), then kills the
host process and verifies crash takeover. Proves multi-node delivery, not
just in-process simulation.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import unittest
import urllib.request

BASE_NET = 7210
BASE_UI = 8210
SESSION = "session-e2e"


def api(port, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data)
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())


def wait_state(port, pred, timeout=15, interval=0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = api(port, "/api/state")
            if pred(s):
                return s
        except Exception:
            pass
        time.sleep(interval)
    return None


class E2EProcessTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ids = ["n0", "n1", "n2"]
        peers = ",".join(f"{nid}=127.0.0.1:{BASE_NET + i}"
                         for i, nid in enumerate(ids))
        cls.procs = {}
        for i, nid in enumerate(ids):
            cmd = [sys.executable, "-m", "host_migration.node_main",
                   "--id", nid, "--port", str(BASE_NET + i),
                   "--ui-port", str(BASE_UI + i), "--peers", peers,
                   "--host-id", "n0", "--session", SESSION,
                   "--tick-dt", "0.1"]
            if i == 0:
                cmd.append("--initial-host")
            cls.procs[nid] = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            time.sleep(0.4)

    @classmethod
    def tearDownClass(cls):
        for p in cls.procs.values():
            if p.poll() is None:
                p.terminate()
        for p in cls.procs.values():
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

    def ui(self, nid):
        return BASE_UI + int(nid[1])

    def test_full_migration_lifecycle(self):
        # all windows up, n0 hosting
        for nid in ("n0", "n1", "n2"):
            s = wait_state(self.ui(nid), lambda s: s["phase"] in ("hosting", "client"))
            self.assertIsNotNone(s, f"{nid} window never came up")
        s0 = api(self.ui("n0"), "/api/state")
        self.assertEqual(s0["role"], "host")
        self.assertEqual(s0["session"], SESSION)

        # a shared multi-tick action starts on the host
        r = api(self.ui("n2"), "/api/action", {"action": "press"})
        self.assertTrue(r["ok"])
        s = wait_state(self.ui("n0"),
                       lambda s: (s["world"] and s["world"]["players"]
                                  .get("n2", {}).get("action") is not None),
                       interval=0.05)
        self.assertIsNotNone(s, "press action never started on host")

        # graceful handoff triggered from the host window, mid-action
        r = api(self.ui("n0"), "/api/action", {"action": "handoff"})
        self.assertTrue(r["ok"], r)

        # a new host emerges with a *complete* verified migration report
        new_host = None
        for nid in ("n1", "n2"):
            s = wait_state(self.ui(nid),
                           lambda s: s["role"] == "host" and s["epoch"] == 2
                           and s["migration"] and s["migration"]["complete"],
                           timeout=15)
            if s:
                new_host, hs = nid, s
                break
        self.assertIsNotNone(new_host, "no node completed verified takeover")
        m = hs["migration"]
        self.assertEqual(m["resume_tick"], m["boundary_tick"] + 1)
        self.assertEqual(m["session"], SESSION)
        self.assertTrue(m["hash_verified"] and m["boundary_sealed"])
        self.assertGreaterEqual(m["entities"], 7)  # players + mechanisms

        # the interrupted action completes; mechanism state continuous
        s = wait_state(self.ui(new_host),
                       lambda s: s["world"] and s["world"]["press_count"] >= 1)
        self.assertIsNotNone(s, "in-progress action lost across handoff")
        self.assertGreater(s["world"]["gear"]["angle"], 0)

        # every surviving window shows the session surviving the switch
        for nid in ("n0", "n1", "n2"):
            s = wait_state(self.ui(nid),
                           lambda s: s["epoch"] == 2 and s["continuity"])
            self.assertIsNotNone(s, f"{nid} shows no continuity record")
            rec = s["continuity"][-1]
            self.assertEqual(rec["resumed_at"], rec["boundary_tick"] + 1)
            self.assertTrue(rec["hash_ok"])

        # now kill the new host process (crash button does os._exit)
        try:
            urllib.request.urlopen(
                f"http://127.0.0.1:{self.ui(new_host)}/api/crash",
                data=b"{}", timeout=2)
        except Exception:
            pass
        self.procs[new_host].wait(timeout=5)
        self.assertIsNotNone(self.procs[new_host].poll(),
                             "host process did not die on crash")

        # successor takes over from the last verified checkpoint
        survivors = [n for n in ("n0", "n1", "n2") if n != new_host]
        taken_over = None
        for nid in survivors:
            s = wait_state(self.ui(nid),
                           lambda s: s["role"] == "host" and s["epoch"] == 3
                           and s["migration"] and s["migration"]["complete"],
                           timeout=20)
            if s:
                taken_over, ts = nid, s
                break
        self.assertIsNotNone(taken_over, "no crash takeover to epoch 3")
        m = ts["migration"]
        self.assertEqual(m["resume_tick"], m["boundary_tick"] + 1)
        self.assertEqual(m["session"], SESSION)
        self.assertGreaterEqual(m["entities"], 7)
        print(f"\nE2E OK: graceful handoff n0->{new_host}, "
              f"crash takeover ->{taken_over}; session {SESSION} survived")


class E2EViewportTest(unittest.TestCase):
    """Staged handoff viewport + voluntary exit over real processes."""

    @classmethod
    def setUpClass(cls):
        ids = ["n0", "n1", "n2"]
        base_net, base_ui = 7310, 8310
        peers = ",".join(f"{nid}=127.0.0.1:{base_net + i}"
                         for i, nid in enumerate(ids))
        cls.base_ui = base_ui
        cls.procs = {}
        for i, nid in enumerate(ids):
            cmd = [sys.executable, "-m", "host_migration.node_main",
                   "--id", nid, "--port", str(base_net + i),
                   "--ui-port", str(base_ui + i), "--peers", peers,
                   "--host-id", "n0", "--session", "session-vp",
                   "--tick-dt", "0.1"]
            if i == 0:
                cmd.append("--initial-host")
            cls.procs[nid] = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            time.sleep(0.4)

    @classmethod
    def tearDownClass(cls):
        for p in cls.procs.values():
            if p.poll() is None:
                p.terminate()
        for p in cls.procs.values():
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

    def ui(self, nid):
        return self.base_ui + int(nid[1])

    def test_staged_viewport_and_voluntary_exit(self):
        for nid in ("n0", "n1", "n2"):
            self.assertIsNotNone(
                wait_state(self.ui(nid), lambda s: s["phase"] in ("hosting", "client")),
                f"{nid} never came up")

        # one bag op, then a handoff with the revocable-log strategy
        r = api(self.ui("n1"), "/api/action", {"action": "pickup"})
        self.assertTrue(r["ok"])
        s = wait_state(self.ui("n0"),
                       lambda s: s["world"] and s["world"]["items_created"] >= 1,
                       interval=0.1)
        self.assertIsNotNone(s)

        r = api(self.ui("n0"), "/api/action", {"action": "handoff_log"})
        self.assertTrue(r["ok"], r)

        new_host = None
        for nid in ("n1", "n2"):
            s = wait_state(self.ui(nid),
                           lambda s: s["role"] == "host" and s["epoch"] == 2
                           and s["migration"] and s["migration"]["complete"])
            if s:
                new_host = nid
                break
        self.assertIsNotNone(new_host)

        # The old host's viewport still exposes the staged handover record
        # with its confirmation boundary and the legal input sources.
        s = wait_state(self.ui("n0"), lambda s: s["epoch"] == 2)
        self.assertIsNotNone(s)
        h = s["handoff"]
        self.assertEqual(h["strategy"], "revocable_log")
        self.assertEqual(h["stage"], "submit_inputs")
        self.assertIn("select_candidate", [x["stage"] for x in h["steps"]])
        self.assertIn("load_snapshot", [x["stage"] for x in h["steps"]])
        # audit rows carry an explicit source per input
        for row in s["input_log"]:
            self.assertIn(row["source"],
                          ("direct", "migrated", "client_resend"))

        # voluntary exit of the new host hands over again (epoch 3)
        r = api(self.ui(new_host), "/api/action", {"action": "exit"})
        self.assertTrue(r["ok"], r)
        survivors = [n for n in ("n0", "n1", "n2") if n != new_host]
        taken = None
        for nid in survivors:
            s = wait_state(self.ui(nid),
                           lambda s: s["role"] == "host" and s["epoch"] == 3
                           and s["migration"] and s["migration"]["complete"],
                           timeout=20)
            if s:
                taken = nid
                break
        self.assertIsNotNone(taken, "voluntary exit did not hand over")
        s = wait_state(self.ui(taken),
                       lambda s: s["world"] and s["world"]["items_created"] >= 1)
        self.assertIsNotNone(s)


if __name__ == "__main__":
    unittest.main()
