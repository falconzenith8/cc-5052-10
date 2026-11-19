"""CLI entry for one node process: real TCP transport + scene window UI."""

from __future__ import annotations

import argparse
import asyncio

from .node import Node
from .transport import TcpTransport
from .ui import start_ui


def parse_peers(spec: str) -> dict[str, tuple[str, int]]:
    peers = {}
    for part in spec.split(","):
        nid, addr = part.split("=")
        host, port = addr.rsplit(":", 1)
        peers[nid] = (host, int(port))
    return peers


async def amain() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--ui-port", type=int, required=True)
    ap.add_argument("--peers", required=True,
                    help="id=host:port,id=host:port (all nodes, incl. self)")
    ap.add_argument("--initial-host", action="store_true")
    ap.add_argument("--host-id", default=None)
    ap.add_argument("--session", default="session-0000")
    ap.add_argument("--tick-dt", type=float, default=0.05)
    args = ap.parse_args()

    all_nodes = parse_peers(args.peers)
    peers = {k: v for k, v in all_nodes.items() if k != args.id}
    transport = TcpTransport(args.id, "127.0.0.1", args.port, peers)
    node = Node(args.id, list(peers), transport,
                initial_host=args.initial_host,
                host_id=args.host_id or args.id,
                session_id=args.session, tick_dt=args.tick_dt)
    await node.start()
    start_ui(node, asyncio.get_running_loop(), args.ui_port)
    print(f"[{args.id}] role={node.role} net=:{args.port} "
          f"ui=http://127.0.0.1:{args.ui_port}", flush=True)
    while True:
        await asyncio.sleep(3600)


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
