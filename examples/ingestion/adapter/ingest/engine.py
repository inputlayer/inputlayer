"""A minimal InputLayer WebSocket client that never hides a failed write.

The protocol is documented in docs/content/docs/guides/websocket-api.mdx. Every
request carries an `id` and its reply frames echo it; frames without an `id`
(notifications, standing-query pushes, notices) are never mistaken for a
reply. Every `error` frame and every non-empty per-statement `errors` list
raises, so a rejected program can never be mistaken for a committed one.
"""

from __future__ import annotations

import json
import logging
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass
from itertools import count
from typing import Any

import aiohttp

from .config import EngineSettings

PROTOCOL_VERSION = 2
"""The oldest protocol with request ids, which this client relies on."""

_AUTH_REPLIES = frozenset({"authenticated", "auth_error"})
"""Never unsolicited, so accepted without an `id`: protocol 1 engines echo none,
and must still get a clear version error instead of a hang."""

log = logging.getLogger(__name__)


class EngineError(RuntimeError):
    """The engine rejected a request; nothing of a rejected program was applied."""

    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message if code is None else f"[{code}] {message}")
        self.code = code


class EngineUnavailable(ConnectionError):
    """The engine could not be reached, or the connection broke mid-request.

    A write that hit this may or may not have committed; retrying it is safe
    because applied revisions are skipped.
    """


@dataclass(frozen=True, slots=True)
class Result:
    columns: list[str]
    rows: list[list[Any]]
    truncated: bool
    frame: dict[str, Any]
    """The `result` (or `result_start`) frame, for fields such as `subscribed`."""


class EngineSession:
    """One authenticated WebSocket bound to one knowledge graph.

    Requests run one at a time. Unsolicited frames whose type is in
    `keep_pushes` are buffered for `pushes()`; others are dropped (notices
    are logged).
    """

    def __init__(self, ws: aiohttp.ClientWebSocketResponse, keep_pushes: frozenset[str]):
        self._ws = ws
        self._keep = keep_pushes
        self._buffered: deque[dict[str, Any]] = deque()
        self._ids = count(1)

    @classmethod
    async def open(
        cls,
        http: aiohttp.ClientSession,
        settings: EngineSettings,
        keep_pushes: frozenset[str] = frozenset(),
    ) -> EngineSession:
        url = f"{settings.url}?kg={settings.knowledge_graph}"
        try:
            ws = await http.ws_connect(url, max_msg_size=0)
        except (aiohttp.ClientError, OSError) as err:
            raise EngineUnavailable(f"cannot connect to {url}: {err}") from err
        session = cls(ws, keep_pushes)
        try:
            auth = {"type": "authenticate", "api_key": settings.api_key}
            _, reply = await session._send_request(auth, untagged_replies=_AUTH_REPLIES)
            if reply.get("type") != "authenticated":
                raise EngineError(f"authentication failed: {reply.get('message', reply)}")
            if reply.get("protocol_version", 1) < PROTOCOL_VERSION:
                raise EngineError(
                    f"engine speaks WebSocket protocol {reply.get('protocol_version', 1)};"
                    f" this client needs {PROTOCOL_VERSION} or later"
                )
        except BaseException:
            await ws.close()
            raise
        return session

    async def close(self) -> None:
        await self._ws.close()

    async def execute(self, program: str) -> Result:
        """Run one program; raises EngineError if any statement failed."""
        request_id, reply = await self._send_request({"type": "execute", "program": program})
        kind = reply.get("type")
        if kind == "error":
            raise EngineError(str(reply.get("message")), reply.get("code"))
        if kind == "result":
            rows = reply.get("rows", [])
        elif kind == "result_start":
            rows = await self._read_stream(request_id)
        else:
            raise EngineError(f"unexpected reply {reply!r}")
        errors = reply.get("errors") or []
        if errors:
            first = errors[0]
            raise EngineError(str(first.get("message")), first.get("code"))
        return Result(reply.get("columns", []), rows, reply.get("truncated", False), reply)

    async def pushes(self) -> AsyncIterator[dict[str, Any]]:
        """Kept unsolicited frames, in arrival order; ends with EngineUnavailable."""
        while True:
            while self._buffered:
                yield self._buffered.popleft()
            frame = await self._receive()
            if "id" not in frame:
                self._unsolicited(frame)

    async def _send_request(
        self, message: dict[str, Any], untagged_replies: frozenset[str] = frozenset()
    ) -> tuple[str, dict[str, Any]]:
        request_id = str(next(self._ids))
        try:
            await self._ws.send_str(json.dumps({**message, "id": request_id}))
        except (aiohttp.ClientError, ConnectionError) as err:
            raise EngineUnavailable(f"send failed: {err}") from err
        return request_id, await self._reply(request_id, untagged_replies)

    async def _read_stream(self, request_id: str) -> list[list[Any]]:
        rows: list[list[Any]] = []
        while (frame := await self._reply(request_id)).get("type") == "result_chunk":
            rows.extend(frame["rows"])
        if frame.get("type") != "result_end":
            raise EngineError(f"unexpected frame in streamed result {frame!r}")
        return rows

    async def _reply(
        self, request_id: str, untagged_replies: frozenset[str] = frozenset()
    ) -> dict[str, Any]:
        """The next frame answering `request_id`, setting unsolicited frames aside."""
        while True:
            frame = await self._receive()
            if "id" not in frame:
                if frame.get("type") in untagged_replies:
                    return frame
                self._unsolicited(frame)
            elif frame["id"] == request_id:
                return frame
            else:
                raise EngineError(f"reply for unknown request {frame['id']!r}")

    def _unsolicited(self, frame: dict[str, Any]) -> None:
        kind = frame.get("type")
        if kind == "notice":
            log.warning("engine notice %s: %s", frame.get("code"), frame.get("message"))
        if kind in self._keep:
            self._buffered.append(frame)

    async def _receive(self) -> dict[str, Any]:
        msg = await self._ws.receive()
        if msg.type != aiohttp.WSMsgType.TEXT:
            raise EngineUnavailable(f"connection closed ({msg.type.name})")
        frame: dict[str, Any] = json.loads(msg.data)
        return frame
