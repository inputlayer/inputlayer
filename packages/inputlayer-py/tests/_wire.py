"""A scripted in-memory socket driven through the real connection reader."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

from inputlayer.connection import Connection

# Frames that finish the reply to one request.
_TERMINAL = {"result", "error", "result_end", "pong", "cancel_ack", "authenticated"}


class ScriptedWire:
    """Answers each request with the next scripted reply.

    Frames are released in order after each request is sent, up to and
    including the frame that finishes one reply (pushes before it go with it),
    each stamped with the request's ``id`` unless it is a push or a notice. A
    connection attached with :func:`attach` reads them through its real reader.
    """

    def __init__(self, *frames: dict[str, Any]) -> None:
        self._frames = [dict(f) for f in frames]
        self._queue: asyncio.Queue[str | None] = asyncio.Queue()
        self.sent: list[str] = []
        self.requests: list[dict[str, Any]] = []

    async def send(self, raw: str) -> None:
        frame = json.loads(raw)
        self.requests.append(frame)
        if "program" in frame:
            self.sent.append(frame["program"])
        while self._frames:
            reply = self._frames.pop(0)
            is_reply = reply["type"] in _TERMINAL | {"result_start", "result_chunk"}
            if is_reply and "id" not in reply and "id" in frame:
                reply["id"] = frame["id"]
            self._queue.put_nowait(json.dumps(reply))
            if reply["type"] in _TERMINAL:
                break

    async def close(self) -> None:
        self._queue.put_nowait(None)

    async def __aiter__(self) -> AsyncIterator[str]:
        while True:
            raw = await self._queue.get()
            if raw is None:
                return
            yield raw

    @property
    def drained(self) -> bool:
        return not self._frames


def attach(conn: Connection, wire: ScriptedWire, kg: str = "default") -> Connection:
    """Mark *conn* open on *wire* and start its reader on it."""
    conn._primitives()[1].set()
    conn._ws = wire  # type: ignore[assignment]
    conn._state = "open"
    conn._current_kg = kg
    conn._reader = asyncio.ensure_future(conn._read_loop(wire))  # type: ignore[arg-type]
    return conn
