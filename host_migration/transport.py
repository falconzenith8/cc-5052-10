"""Transports. One interface, two implementations:

- TcpTransport: real sockets, one JSON-lines connection per node pair,
  dialed by the lexicographically smaller node id, re-dialed on failure.
- MemTransport: in-memory network for tests, with per-message delay/drop
  rules so tests can script late transfer data and out-of-sync delivery.
"""

from __future__ import annotations

import asyncio
import json


class TcpTransport:
    def __init__(self, node_id: str, host: str, port: int,
                 peers: dict[str, tuple[str, int]]):
        self.id = node_id
        self.host = host
        self.port = port
        self.peers = peers  # id -> (host, port), excludes self
        self._handler = None
        self._peer_down = None
        self._writers: dict[str, asyncio.StreamWriter] = {}
        self._server = None
        self._dial_tasks: dict[str, asyncio.Task] = {}
        self._alive = False

    def set_handler(self, fn) -> None:
        self._handler = fn

    def set_peer_down_handler(self, fn) -> None:
        self._peer_down = fn

    async def start(self) -> None:
        self._alive = True
        self._server = await asyncio.start_server(self._on_accept,
                                                  self.host, self.port)
        for pid in self.peers:
            if self.id < pid:  # smaller id dials
                self._dial_tasks[pid] = asyncio.create_task(self._dial_loop(pid))

    async def _on_accept(self, reader, writer):
        try:
            line = await reader.readline()
            hello = json.loads(line.decode())
            pid = hello["id"]
            self._register(pid, writer)
            await self._read_loop(pid, reader)
        except (asyncio.IncompleteReadError, ConnectionError, json.JSONDecodeError):
            pass
        finally:
            writer.close()

    def _register(self, pid: str, writer) -> None:
        old = self._writers.get(pid)
        if old is not None and old is not writer:
            old.close()
        self._writers[pid] = writer

    async def _dial_loop(self, pid: str) -> None:
        host, port = self.peers[pid]
        while self._alive:
            if pid in self._writers:
                await asyncio.sleep(0.5)
                continue
            try:
                reader, writer = await asyncio.open_connection(host, port)
                writer.write((json.dumps({"id": self.id}) + "\n").encode())
                await writer.drain()
                self._register(pid, writer)
                await self._read_loop(pid, reader)
            except (ConnectionError, OSError, asyncio.IncompleteReadError):
                pass
            finally:
                self._writers.pop(pid, None)
                if self._alive and self._peer_down:
                    self._peer_down(pid)
            if self._alive:
                await asyncio.sleep(0.3)

    async def _read_loop(self, pid: str, reader) -> None:
        while self._alive:
            line = await reader.readline()
            if not line:
                break
            msg = json.loads(line.decode())
            if self._handler:
                self._handler(pid, msg)

    async def send(self, peer_id: str, msg: dict) -> bool:
        w = self._writers.get(peer_id)
        if w is None:
            return False
        try:
            w.write((json.dumps(msg) + "\n").encode())
            await w.drain()
            return True
        except (ConnectionError, OSError):
            self._writers.pop(peer_id, None)
            return False

    async def broadcast(self, msg: dict) -> None:
        for pid in list(self.peers):
            await self.send(pid, msg)

    async def stop(self) -> None:
        self._alive = False
        for t in self._dial_tasks.values():
            t.cancel()
        for w in self._writers.values():
            w.close()
        if self._server:
            self._server.close()

    async def crash(self) -> None:
        """Abrupt failure: sockets die without any goodbye message."""
        await self.stop()


class MemTransport:
    """In-memory transport for tests. The network object applies rules."""

    def __init__(self, node_id: str, network: "MemNetwork"):
        self.id = node_id
        self.net = network
        self._handler = None
        self._peer_down = None
        self._alive = False

    def set_handler(self, fn) -> None:
        self._handler = fn

    def set_peer_down_handler(self, fn) -> None:
        self._peer_down = fn

    async def start(self) -> None:
        self._alive = True
        self.net.register(self)

    async def send(self, peer_id: str, msg: dict) -> bool:
        if not self._alive:
            return False
        self.net.deliver(self.id, peer_id, msg)
        return True

    async def broadcast(self, msg: dict) -> None:
        for pid in list(self.net.nodes):
            if pid != self.id:
                await self.send(pid, msg)

    async def stop(self) -> None:
        self._alive = False
        self.net.unregister(self.id, abrupt=False)

    async def crash(self) -> None:
        self._alive = False
        self.net.unregister(self.id, abrupt=True)

    def _receive(self, peer_id: str, msg: dict) -> None:
        if self._alive and self._handler:
            self._handler(peer_id, msg)


class MemNetwork:
    def __init__(self, delay_rule=None):
        self.nodes: dict[str, MemTransport] = {}
        # delay_rule(msg, src, dst) -> seconds or None to drop
        self.delay_rule = delay_rule or (lambda m, s, d: 0.0)
        self.loop = asyncio.get_event_loop()

    def register(self, t: MemTransport) -> None:
        self.nodes[t.id] = t

    def unregister(self, nid: str, abrupt: bool) -> None:
        self.nodes.pop(nid, None)
        if abrupt:
            for other in self.nodes.values():
                if other._peer_down:
                    other._peer_down(nid)

    def deliver(self, src: str, dst: str, msg: dict) -> None:
        target = self.nodes.get(dst)
        if target is None:
            return
        delay = self.delay_rule(msg, src, dst)
        if delay is None:
            return  # dropped
        msg = json.loads(json.dumps(msg))  # force value semantics
        if delay <= 0:
            target._receive(src, msg)
        else:
            self.loop.call_later(delay, self._late, dst, src, msg)

    def _late(self, dst: str, src: str, msg: dict) -> None:
        target = self.nodes.get(dst)
        if target is not None:
            target._receive(src, msg)
