"""The connection core against a live engine.

Set INPUTLAYER_TEST_SERVER=ws://localhost:8080/ws to enable (``make
python-test-live`` does).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import pytest

from inputlayer import InputLayer
from inputlayer.exceptions import DeadlineExceeded
from inputlayer.notifications import ConnectionEvent, NotificationEvent

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not os.environ.get("INPUTLAYER_TEST_SERVER"),
        reason="INPUTLAYER_TEST_SERVER not set",
    ),
]


def _client(**kwargs: Any) -> InputLayer:
    return InputLayer(
        os.environ["INPUTLAYER_TEST_SERVER"],
        username=os.environ.get("INPUTLAYER_TEST_USER", "admin"),
        password=os.environ.get("INPUTLAYER_TEST_PASSWORD", "admin"),
        **kwargs,
    )


def _name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _until(condition: Any, timeout: float = 5.0) -> None:
    async def poll() -> None:
        while not condition():
            await asyncio.sleep(0.02)

    await asyncio.wait_for(poll(), timeout)


async def test_pipelined_queries_beyond_the_in_flight_bound() -> None:
    async with _client() as il:
        kg = il.knowledge_graph(_name("live_pipe"))
        await kg.execute("+n[" + ", ".join(f"({i})" for i in range(40)) + "]")
        results = await asyncio.gather(*(kg.execute(f"?n({i})") for i in range(40)))
        assert [r.rows for r in results] == [[[i]] for i in range(40)]
        assert kg._conn.in_flight == 0
        await il.drop_knowledge_graph(kg.name)


async def test_missing_graph_is_created_and_bound() -> None:
    async with _client() as il:
        name = _name("live_fresh")
        kg = il.knowledge_graph(name)
        await kg.execute("+t(1)")
        assert kg._conn.current_kg == name
        assert name in await il.list_knowledge_graphs()
        await il.drop_knowledge_graph(name)


async def test_two_handles_subscription_survives_queries_on_the_other() -> None:
    async with _client() as il:
        a, b = il.knowledge_graph(_name("live_a")), il.knowledge_graph(_name("live_b"))
        await a.execute("+item(0)")
        await b.execute("+other(0)")
        pushes: list[Any] = []
        a._conn.add_route("s1", pushes.append)
        reply = await a.execute(".subscribe s1 ?item(X)")
        assert reply.rows == [[0]]
        for i in range(10):
            assert (await b.execute("?other(X)")).row_count == i + 1
            await b.execute(f"+other({i + 1})")
        await a.execute("+item(1)")
        await _until(lambda: pushes)
        assert pushes[0].subscription == "s1"
        assert pushes[0].inserted == [[1]]
        assert a._conn.stale_pushes == 0
        await il.drop_knowledge_graph(a.name)
        await il.drop_knowledge_graph(b.name)


async def test_notifications_arrive_while_idle() -> None:
    name = _name("live_idle")
    async with _client(initial_kg="default") as writer:
        kg = writer.knowledge_graph(name)
        await kg.execute("+seed(0)")
        async with _client(initial_kg=name) as watcher:
            seen: list[NotificationEvent] = []
            watcher.on("persistent_update", relation="seed")(seen.append)
            await kg.execute("+seed(1)")
            # The watcher sends nothing; only its reader is running.
            await _until(lambda: seen)
            assert seen[0].knowledge_graph == name
        await writer.drop_knowledge_graph(name)


async def test_deadline_stops_a_long_query_and_the_connection_stays_usable() -> None:
    async with _client() as il:
        kg = il.knowledge_graph(_name("live_deadline"))
        chain = ", ".join(f"({i}, {i + 1})" for i in range(1500))
        await kg.execute(f"+edge[{chain}]")
        await kg.execute("+reach(X, Y) <- edge(X, Y)\n+reach(X, Z) <- reach(X, Y), edge(Y, Z)")
        with pytest.raises(DeadlineExceeded):
            await kg.execute("?reach(X, Y)", timeout=0.001)
        assert (await kg.execute("?edge(0, Y)")).rows == [[0, 1]]
        await _until(lambda: kg._conn.in_flight == 0)
        await il.drop_knowledge_graph(kg.name)


async def test_cancelling_a_call_cancels_it_on_the_server() -> None:
    async with _client() as il:
        kg = il.knowledge_graph(_name("live_cancel"))
        chain = ", ".join(f"({i}, {i + 1})" for i in range(1500))
        await kg.execute(f"+edge[{chain}]")
        await kg.execute("+reach(X, Y) <- edge(X, Y)\n+reach(X, Z) <- reach(X, Y), edge(Y, Z)")
        call = asyncio.ensure_future(kg.execute("?reach(X, Y)", timeout=60))
        await asyncio.sleep(0.05)
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call
        # The cancel's ack and the target's reply both come back; nothing leaks.
        await _until(lambda: kg._conn.in_flight == 0, timeout=30)
        assert (await kg.execute("?edge(0, Y)")).rows == [[0, 1]]
        await il.drop_knowledge_graph(kg.name)


async def test_reconnect_restores_graph_and_replays_missed_notifications() -> None:
    name = _name("live_reconnect")
    async with _client() as writer:
        kg = writer.knowledge_graph(name)
        await kg.execute("+seed(0)")
        async with _client(initial_kg=name, reconnect_delay=0.5) as watcher:
            seen: list[int] = []
            events: list[ConnectionEvent] = []
            watcher.on("persistent_update", relation="seed")(lambda e: seen.append(e.count))
            watcher.events.on(callback=events.append)
            await kg.execute("+seed(1)")
            await _until(lambda: len(seen) == 1)
            conn = watcher._conn
            # Drop the socket under the client, as a network failure would.
            assert conn._ws is not None
            conn._ws.transport.abort()
            await _until(lambda: conn.state == "reconnecting")
            await kg.execute("+seed[(2), (3)]")  # committed while the watcher is away
            await _until(lambda: any(e.type == "reconnected" for e in events))
            assert conn.current_kg == name
            await _until(lambda: len(seen) == 2)
            assert seen == [1, 2]
            assert [e.type for e in events][:3] == ["disconnected", "session_reset", "reconnected"]
            assert (await watcher.knowledge_graph(name).execute("?seed(X)")).row_count == 4
        await writer.drop_knowledge_graph(name)
