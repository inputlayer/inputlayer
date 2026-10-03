"""The client against a scripted engine: unsolicited frames never pass as replies."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import aiohttp
import pytest
from aiohttp import web

from ingest.config import EngineSettings
from ingest.engine import EngineError, EngineSession
from ingest.watch import Watcher

Script = Callable[[dict[str, Any]], list[dict[str, Any]]]


def authenticated(request: dict[str, Any], protocol: int = 2) -> list[dict[str, Any]]:
    return [{"type": "authenticated", "id": request["id"], "protocol_version": protocol}]


Engine = Callable[[Script], Awaitable[EngineSettings]]


@pytest.fixture
async def engine() -> AsyncIterator[Engine]:
    """Start fake engines that answer each request with the frames `script` returns."""
    runners: list[web.AppRunner] = []

    async def start(script: Script) -> EngineSettings:
        async def ws_handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            async for msg in ws:
                for frame in script(json.loads(msg.data)):
                    await ws.send_str(json.dumps(frame))
            return ws

        app = web.Application()
        app.router.add_get("/ws", ws_handler)
        runner = web.AppRunner(app)
        runners.append(runner)
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        port = runner.addresses[0][1]
        return EngineSettings(f"ws://127.0.0.1:{port}/ws", "key", "kg")

    yield start
    for runner in runners:
        await runner.cleanup()


async def test_notices_and_pushes_before_a_reply_are_set_aside(engine: Engine) -> None:
    def script(request: dict[str, Any]) -> list[dict[str, Any]]:
        if request["type"] == "authenticate":
            return authenticated(request)
        return [
            {"type": "notice", "code": "notifications_missed", "message": "slow"},
            {"type": "persistent_update", "relation": "r", "seq": 9},
            {"type": "result", "id": request["id"], "columns": ["X"], "rows": [[1]], "errors": []},
        ]

    async with aiohttp.ClientSession() as http:
        session = await EngineSession.open(http, await engine(script))
        assert (await session.execute("?r(X)")).rows == [[1]]
        await session.close()


async def test_statement_errors_raise(engine: Engine) -> None:
    def script(request: dict[str, Any]) -> list[dict[str, Any]]:
        if request["type"] == "authenticate":
            return authenticated(request)
        failure = {"index": 1, "code": "validation", "message": "type mismatch"}
        return [{"type": "result", "id": request["id"], "rows": [], "errors": [failure]}]

    async with aiohttp.ClientSession() as http:
        session = await EngineSession.open(http, await engine(script))
        with pytest.raises(EngineError, match="type mismatch"):
            await session.execute("+r(1)\n+r(2)")
        await session.close()


async def test_protocol_1_engines_are_refused(engine: Engine) -> None:
    # Protocol 1 echoes no request id and sends no protocol_version.
    def script(request: dict[str, Any]) -> list[dict[str, Any]]:
        return [{"type": "authenticated", "session_id": "s"}]

    async with aiohttp.ClientSession() as http:
        with pytest.raises(EngineError, match="protocol 1"):
            await EngineSession.open(http, await engine(script))


async def test_watcher_drops_stale_generations_and_raises_on_a_lost_delta(engine: Engine) -> None:
    def delta(seq: int, generation: int) -> dict[str, Any]:
        return {
            "type": "subscription_delta",
            "subscription": "s",
            "generation": generation,
            "seq": seq,
            "revision": 10 + seq,
            "inserted": [[seq]],
            "retracted": [],
        }

    def script(request: dict[str, Any]) -> list[dict[str, Any]]:
        if request["type"] == "authenticate":
            return authenticated(request)
        subscribed = {"subscription": "s", "generation": 2, "revision": 10}
        reply = {"type": "result", "id": request["id"], "rows": [], "subscribed": subscribed}
        return [delta(5, generation=1), reply, delta(1, 2), delta(3, 2)]

    async with aiohttp.ClientSession() as http:
        watcher = await Watcher.open(http, await engine(script))
        await watcher.subscribe("s", "?r(X)")
        deltas = watcher.deltas()
        first = await anext(deltas)
        assert (first.seq, first.revision, first.inserted) == (1, 11, [[1]])
        with pytest.raises(EngineError, match="delta 2 lost"):
            await anext(deltas)
        await watcher.close()
