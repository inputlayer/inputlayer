"""A real WebSocket server on localhost that tests script frame by frame."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import parse_qs, urlparse

from websockets.asyncio.server import Server, ServerConnection, serve

EPOCH = "00112233aabbccdd"


class Peer:
    """The server side of one client connection."""

    def __init__(self, ws: ServerConnection, server: MockServer) -> None:
        self.ws = ws
        self.server = server
        query = parse_qs(urlparse(ws.request.path if ws.request else "").query)
        self.params = {k: v[0] for k, v in query.items()}

    async def recv(self, timeout: float = 5.0) -> dict[str, Any]:
        frame: dict[str, Any] = json.loads(await asyncio.wait_for(self.ws.recv(), timeout))
        self.server.received.append(frame)
        return frame

    async def recv_type(self, *types: str, timeout: float = 5.0) -> dict[str, Any]:
        """The next frame of one of *types*, skipping keepalive pings."""
        while True:
            frame = await self.recv(timeout)
            if frame["type"] in types:
                return frame
            if frame["type"] == "ping":
                await self.send({"type": "pong", "id": frame.get("id")})
                continue
            raise AssertionError(f"expected {types}, got {frame}")

    async def send(self, frame: dict[str, Any]) -> None:
        await self.ws.send(json.dumps({k: v for k, v in frame.items() if v is not None}))

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self.ws.close(code, reason)

    async def authenticate(self, *, epoch: str = EPOCH, kg: str | None = None) -> dict[str, Any]:
        login = await self.recv_type("login", "authenticate")
        self.server.sessions += 1
        await self.send({
            "type": "authenticated",
            "id": login.get("id"),
            "session_id": str(self.server.sessions),
            "knowledge_graph": kg or self.params.get("kg", "default"),
            "version": "0.0.0-mock",
            "role": "admin",
            "protocol_version": 5,
            "stream_epoch": epoch,
        })
        return login

    async def serve_results(self, answer: Callable[[str], dict[str, Any]] | None = None) -> None:
        """Answer every execute with ``answer(program)`` (an empty result by
        default) and every ping with a pong, until the client goes away."""
        with contextlib.suppress(Exception):
            while True:
                frame = await self.recv(timeout=60)
                if frame["type"] == "ping":
                    await self.send({"type": "pong", "id": frame.get("id")})
                elif frame["type"] == "execute":
                    reply = answer(frame["program"]) if answer else result([])
                    await self.send({**reply, "id": frame.get("id")})


Handler = Callable[[Peer], Awaitable[None]]


class MockServer:
    """Accepts connections and hands each to the next handler in turn (the
    last handler serves every later connection)."""

    def __init__(self, *handlers: Handler) -> None:
        self._handlers = list(handlers)
        self._server: Server | None = None
        self.peers: list[Peer] = []
        self.received: list[dict[str, Any]] = []
        self.sessions = 0
        self.errors: list[BaseException] = []

    async def __aenter__(self) -> MockServer:
        self._server = await serve(self._handle, "127.0.0.1", 0)
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.stop()
        if self.errors and exc[0] is None:
            raise self.errors[0]

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._server.wait_closed(), 5)
            self._server = None

    @property
    def url(self) -> str:
        assert self._server is not None
        port = next(iter(self._server.sockets)).getsockname()[1]
        return f"ws://127.0.0.1:{port}/ws"

    async def _handle(self, ws: ServerConnection) -> None:
        peer = Peer(ws, self)
        index = len(self.peers)
        self.peers.append(peer)
        handler = self._handlers[min(index, len(self._handlers) - 1)]
        try:
            await handler(peer)
        except Exception as e:  # surfaced by __aexit__
            if not isinstance(e, asyncio.CancelledError):
                self.errors.append(e)
        with contextlib.suppress(Exception):
            await ws.wait_closed()


def result(rows: list[list[Any]], columns: list[str] | None = None, **extra: Any) -> dict[str, Any]:
    return {
        "type": "result",
        "columns": columns if columns is not None else (["x"] if rows else []),
        "rows": rows,
        "row_count": len(rows),
        "total_count": len(rows),
        "truncated": False,
        "execution_time_ms": 0,
        **extra,
    }


def notification(seq: int, relation: str = "demo", kg: str = "default") -> dict[str, Any]:
    return {
        "type": "persistent_update",
        "seq": seq,
        "timestamp_ms": 0,
        "knowledge_graph": kg,
        "relation": relation,
        "operation": "insert",
        "count": 1,
    }
