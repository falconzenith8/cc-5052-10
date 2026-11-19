"""Spawn a real multi-node cluster (one process per node) and print the URLs."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
import uuid


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--base-port", type=int, default=7100)
    ap.add_argument("--base-ui", type=int, default=8100)
    args = ap.parse_args()

    ids = [f"n{i}" for i in range(args.nodes)]
    peers = ",".join(f"{nid}=127.0.0.1:{args.base_port + i}"
                     for i, nid in enumerate(ids))
    session = f"session-{uuid.uuid4().hex[:8]}"
    procs = []
    for i, nid in enumerate(ids):
        cmd = [sys.executable, "-m", "host_migration.node_main",
               "--id", nid, "--port", str(args.base_port + i),
               "--ui-port", str(args.base_ui + i), "--peers", peers,
               "--host-id", ids[0], "--session", session]
        if i == 0:
            cmd.append("--initial-host")
        procs.append(subprocess.Popen(cmd))
        time.sleep(0.3)  # let the listener come up before dialers
    print(f"session: {session}")
    for i, nid in enumerate(ids):
        role = "HOST  " if i == 0 else "client"
        print(f"  {role} {nid}: http://127.0.0.1:{args.base_ui + i}")
    print("Open the windows, move/press, then use 'graceful handoff' or "
          "'simulate host crash' on the host window. Ctrl-C to stop.")
    try:
        for p in procs:
            p.wait()
    except KeyboardInterrupt:
        for p in procs:
            p.terminate()


if __name__ == "__main__":
    main()
