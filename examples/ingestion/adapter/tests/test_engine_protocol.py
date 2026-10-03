"""The client against a scripted engine: unsolicited frames never pass as replies."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from dataclasses import replace
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from ingest import config, server
from ingest.config import EngineSettings
from ingest.engine import EngineError, EngineSession, EngineUnavailable
from ingest.watch import Watcher

Script = Callable[[dict[str, Any]], list[dict[str, Any]] | AsyncIterator[dict[str, Any]]]


def authenticated(request: dict[str, Any], protocol: int = 2) -> list[dict[str, Any]]:
    return [{"type": "authenticated", "id": request["id"], "protocol_version": protocol}]


Engine = Callable[[Script], Awaitable[EngineSettings]]


@pytest.fixture
def sockets() -> list[web.WebSocketResponse]:
    return []


@pytest.fixture
async def engine(sockets: list[web.WebSocketResponse]) -> AsyncIterator[Engine]:
    """Start fake engines that answer each request with the frames `script` returns."""
    runners: list[web.AppRunner] = []

    async def start(script: Script) -> EngineSettings:
        async def ws_handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            sockets.append(ws)
            with suppress(ConnectionResetError):
                async for msg in ws:
                    frames = script(json.loads(msg.data))
                    if isinstance(frames, list):
                        for frame in frames:
                            await ws.send_json(frame)
                    else:
                        async for frame in frames:
                            await ws.send_json(frame)
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


async def wait_closed(ws: web.WebSocketResponse) -> None:
    async with asyncio.timeout(1):
        while not ws.closed:
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("phase", ["authentication", "execution", "stream"])
async def test_silent_engine_times_out_and_closes_connection(
    engine: Engine, sockets: list[web.WebSocketResponse], phase: str
) -> None:
    accepted = asyncio.Event()

    def script(request: dict[str, Any]) -> list[dict[str, Any]]:
        if request["type"] == "authenticate" and phase != "authentication":
            return authenticated(request)
        accepted.set()
        if phase == "stream":
            return [
                {"type": "result_start", "id": request["id"], "columns": ["X"]},
                {"type": "result_chunk", "id": request["id"], "rows": [[1]]},
            ]
        return []

    settings = replace(await engine(script), request_timeout=0.05)
    async with aiohttp.ClientSession() as http:
        async with asyncio.timeout(1):
            with pytest.raises(EngineUnavailable, match="timed out"):
                session = await EngineSession.open(http, settings)
                await session.execute("?r(X)")
        assert accepted.is_set()
        await wait_closed(sockets[0])


@pytest.mark.parametrize("timeout", [0.1, 1.0])
async def test_stream_uses_one_deadline_through_final_frame(engine: Engine, timeout: float) -> None:
    async def script(request: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
        if request["type"] == "authenticate":
            yield authenticated(request)[0]
            return
        yield {"type": "result_start", "id": request["id"], "columns": ["X"]}
        for value in range(3):
            await asyncio.sleep(0.04)
            yield {"type": "result_chunk", "id": request["id"], "rows": [[value]]}
        yield {"type": "result_end", "id": request["id"]}

    settings = replace(await engine(script), request_timeout=timeout)
    async with aiohttp.ClientSession() as http:
        session = await EngineSession.open(http, settings)
        try:
            async with asyncio.timeout(2):
                if timeout < 0.12:
                    with pytest.raises(EngineUnavailable, match="timed out"):
                        await session.execute("?r(X)")
                else:
                    assert (await session.execute("?r(X)")).rows == [[0], [1], [2]]
        finally:
            await session.close()


async def test_deadline_includes_sending(
    engine: Engine, sockets: list[web.WebSocketResponse], monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = replace(await engine(authenticated), request_timeout=0.05)
    sending = asyncio.Event()

    async def blocked_send(*args: Any, **kwargs: Any) -> None:
        sending.set()
        await asyncio.Event().wait()

    async with aiohttp.ClientSession() as http:
        session = await EngineSession.open(http, settings)
        monkeypatch.setattr(aiohttp.ClientWebSocketResponse, "send_str", blocked_send)
        async with asyncio.timeout(1):
            with pytest.raises(EngineUnavailable, match="timed out"):
                await session.execute("?r(X)")
        assert sending.is_set()
        await wait_closed(sockets[0])


def adapter_settings(
    monkeypatch: pytest.MonkeyPatch, engine_settings: EngineSettings
) -> config.Settings:
    monkeypatch.setenv("ENGINE_URL", engine_settings.url)
    monkeypatch.setenv("ENGINE_API_KEY", engine_settings.api_key)
    monkeypatch.setenv("ENGINE_KG", engine_settings.knowledge_graph)
    monkeypatch.setenv("ENGINE_REQUEST_TIMEOUT", "0.05")
    monkeypatch.setenv("CDC_WEBHOOK_SECRET", "whsec_dGVzdC1vbmx5")
    monkeypatch.setenv("WEBHOOK_SECRET_BILLING", "whsec_dGVzdC1vbmx5")
    return config.from_env()


@pytest.mark.parametrize("phase", ["authentication", "declaration"])
async def test_startup_retries_after_timeout(
    engine: Engine,
    sockets: list[web.WebSocketResponse],
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    stalled = False
    authentications = 0

    def script(request: dict[str, Any]) -> list[dict[str, Any]]:
        nonlocal stalled, authentications
        auth = request["type"] == "authenticate"
        if auth:
            authentications += 1
        if not stalled and auth == (phase == "authentication"):
            stalled = True
            return []
        if auth:
            return authenticated(request)
        return [{"type": "result", "id": request["id"], "rows": [], "errors": []}]

    settings = adapter_settings(monkeypatch, await engine(script))
    app = server.build_app(settings)
    async with TestClient(TestServer(app)) as client:
        async with asyncio.timeout(2):
            await app[server.READY].wait()
        assert (await client.get("/healthz")).status == 200
        assert authentications == 2
        await wait_closed(sockets[0])


@pytest.mark.parametrize("feed", ["cdc", "billing"])
@pytest.mark.parametrize("phase", ["revision", "write"])
async def test_http_timeout_releases_lock_and_reconnects(
    engine: Engine,
    sockets: list[web.WebSocketResponse],
    monkeypatch: pytest.MonkeyPatch,
    feed: str,
    phase: str,
) -> None:
    stall_next = False
    accepted = asyncio.Event()
    authentications = 0

    def script(request: dict[str, Any]) -> list[dict[str, Any]]:
        nonlocal stall_next, authentications
        if request["type"] == "authenticate":
            authentications += 1
            return authenticated(request)
        query = request["program"].startswith("?")
        if stall_next and query == (phase == "revision"):
            stall_next = False
            accepted.set()
            return []
        rows = [] if query else [["Update: 1 deleted, 2 inserted."]]
        return [{"type": "result", "id": request["id"], "rows": rows, "errors": []}]

    settings = adapter_settings(monkeypatch, await engine(script))
    app = server.build_app(settings)
    if feed == "cdc":
        path = "/cdc"
        event = {
            "op": "c",
            "after": {"id": 1, "name": "Acme", "tier": "enterprise"},
            "source": {"schema": "public", "table": "customers", "lsn": 10},
        }
        signer = settings.cdc_signer
    else:
        path = "/webhooks/billing"
        event = {
            "type": "payment.failed",
            "sequence": 10,
            "data": {"invoice": "inv_1", "customer_id": 1, "amount_cents": 42},
        }
        signer = settings.webhook_sources["billing"].signer
    body = json.dumps(event).encode()
    headers = signer.headers("evt_timeout", body)
    async with TestClient(TestServer(app)) as client:
        async with asyncio.timeout(2):
            await app[server.READY].wait()
            stall_next = True
            async with asyncio.TaskGroup() as tasks:
                first = tasks.create_task(client.post(path, data=body, headers=headers))
                await accepted.wait()
                second = tasks.create_task(client.post(path, data=body, headers=headers))
            assert first.result().status == 503
            assert "timed out" in await first.result().text()
            assert second.result().status == 200
            assert await second.result().json() == {"applied": 1, "skipped": 0}
        assert authentications == 2
        await wait_closed(sockets[0])


def test_request_timeout_environment_and_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENGINE_API_KEY", "test")
    monkeypatch.delenv("ENGINE_REQUEST_TIMEOUT", raising=False)
    assert config.engine_from_env().request_timeout == 10.0
    monkeypatch.setenv("ENGINE_REQUEST_TIMEOUT", "2.5")
    assert config.engine_from_env().request_timeout == 2.5


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "-inf", "invalid"])
def test_invalid_request_deadlines_are_rejected(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("ENGINE_API_KEY", "test")
    monkeypatch.setenv("ENGINE_REQUEST_TIMEOUT", value)
    with pytest.raises(ValueError):
        config.engine_from_env()


async def test_idle_subscription_wait_is_not_a_request_timeout(engine: Engine) -> None:
    async def script(request: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
        if request["type"] == "authenticate":
            yield authenticated(request)[0]
            return
        yield {
            "type": "result",
            "id": request["id"],
            "rows": [],
            "subscribed": {"subscription": "s", "generation": 1, "revision": 0},
        }
        await asyncio.sleep(0.15)
        yield {
            "type": "subscription_delta",
            "subscription": "s",
            "generation": 1,
            "seq": 1,
            "revision": 1,
            "inserted": [[1]],
            "retracted": [],
        }

    settings = replace(await engine(script), request_timeout=0.05)
    async with aiohttp.ClientSession() as http:
        watcher = await Watcher.open(http, settings)
        try:
            await watcher.subscribe("s", "?r(X)")
            async with asyncio.timeout(1):
                assert (await anext(watcher.deltas())).inserted == [[1]]
        finally:
            await watcher.close()
