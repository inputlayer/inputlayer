"""The connection core against a mock server on a real socket.

The shared frame scenarios live in packages/conformance/connection and run in
test_connection_conformance.py; these tests cover what needs timing or more
than one connection: ids, the in-flight bound, deadlines and cancel, idle
delivery, keepalive, reconnect and the per-graph connection pool.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from inputlayer import InputLayer
from inputlayer.connection import Connection
from inputlayer.exceptions import (
    AuthenticationError,
    Cancelled,
    ConnectionError,
    ConnectionLost,
    DeadlineExceeded,
    OutcomeUnknownError,
    RateLimited,
)
from inputlayer.notifications import ConnectionEvent

from ._mock_server import EPOCH, MockServer, Peer, notification, result

pytestmark = pytest.mark.asyncio


def _conn(server: MockServer, **kwargs: Any) -> Connection:
    kwargs.setdefault("auto_reconnect", False)
    return Connection(server.url, username="admin", password="pw", **kwargs)


async def _until(condition: Any, timeout: float = 5.0) -> None:
    async def poll() -> None:
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


# ── Authentication ───────────────────────────────────────────────────


class TestAuthentication:
    async def test_login_carries_an_id_and_binds_the_graph(self) -> None:
        async def handler(peer: Peer) -> None:
            login = await peer.authenticate()
            assert login == {
                "type": "login", "id": "r1", "username": "admin", "password": "pw",
            }
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server, initial_kg="sales")
            await conn.connect()
            assert server.peers[0].params == {"kg": "sales"}
            assert conn.current_kg == "sales"
            assert conn.session_id == "1"
            assert conn.stream_epoch == EPOCH
            await conn.close()

    async def test_api_key(self) -> None:
        async def handler(peer: Peer) -> None:
            assert (await peer.authenticate())["api_key"] == "ilk_test"
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = Connection(server.url, api_key="ilk_test", auto_reconnect=False)
            await conn.connect()
            assert conn.connected
            await conn.close()

    async def test_auth_error(self) -> None:
        async def handler(peer: Peer) -> None:
            login = await peer.recv()
            await peer.send(
                {"type": "auth_error", "id": login["id"], "message": "Invalid credentials"}
            )

        async with MockServer(handler) as server:
            with pytest.raises(AuthenticationError, match="Invalid credentials"):
                await _conn(server).connect()

    async def test_no_credentials(self) -> None:
        async with MockServer(Peer.serve_results) as server:
            conn = Connection(server.url, auto_reconnect=False)
            with pytest.raises(AuthenticationError, match="No credentials"):
                await conn.connect()

    async def test_execute_before_connect_raises(self) -> None:
        conn = Connection("ws://127.0.0.1:1/ws", username="a", password="b")
        with pytest.raises(ConnectionError, match="Not connected"):
            await conn.execute("?a(X)")


# ── Requests ─────────────────────────────────────────────────────────


class TestRequests:
    async def test_every_request_has_an_id_and_a_deadline(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server, default_timeout=2.5)
            await conn.connect()
            await conn.execute("?a(X)")
            await conn.execute("?b(X)", timeout=0.25)
            executes = [f for f in server.received if f["type"] == "execute"]
            assert [f["id"] for f in executes] == ["r2", "r3"]
            assert 2400 <= executes[0]["timeout_ms"] <= 2500
            assert 150 <= executes[1]["timeout_ms"] <= 250
            await conn.close()

    async def test_no_timeout_leaves_the_engine_default(self) -> None:
        async with MockServer(_serve) as server:
            conn = _conn(server, default_timeout=None)
            await conn.connect()
            await conn.execute("?a(X)")
            assert "timeout_ms" not in server.received[-1]
            await conn.close()

    async def test_queries_are_pipelined_not_serialized(self) -> None:
        # The server reads both requests before answering either; a client
        # that waited for each reply would deadlock here.
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            a = await peer.recv_type("execute")
            b = await peer.recv_type("execute")
            await peer.send({**result([[2]]), "id": b["id"]})
            await peer.send({**result([[1]]), "id": a["id"]})
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server)
            await conn.connect()
            first, second = await asyncio.gather(conn.execute("?a(X)"), conn.execute("?b(X)"))
            assert (first.rows, second.rows) == ([[1]], [[2]])
            await conn.close()

    async def test_in_flight_bound(self) -> None:
        held: list[dict[str, Any]] = []
        release = asyncio.Event()

        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            held.append(await peer.recv_type("execute"))
            held.append(await peer.recv_type("execute"))
            await release.wait()
            await peer.send({**result([[1]]), "id": held[0]["id"]})
            held.append(await peer.recv_type("execute"))
            await peer.send({**result([[2]]), "id": held[1]["id"]})
            await peer.send({**result([[3]]), "id": held[2]["id"]})
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server, max_in_flight=2)
            await conn.connect()
            calls = [asyncio.ensure_future(conn.execute(f"?q{i}(X)")) for i in range(3)]
            await _until(lambda: len(held) == 2)
            await asyncio.sleep(0.1)
            # The third request waits for a slot; it has not been sent.
            assert [f["program"] for f in server.received if f["type"] == "execute"] == [
                "?q0(X)", "?q1(X)",
            ]
            release.set()
            assert [r.rows for r in await asyncio.gather(*calls)] == [[[1]], [[2]], [[3]]]
            assert conn.in_flight == 0
            await conn.close()


async def _serve(peer: Peer) -> None:
    await peer.authenticate()
    await peer.serve_results()


# ── Deadlines and cancel ─────────────────────────────────────────────


class TestDeadlines:
    async def test_server_deadline_is_typed(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            q = await peer.recv_type("execute")
            assert 50 <= q["timeout_ms"] <= 100
            await peer.send({
                "type": "error", "id": q["id"], "code": "deadline_exceeded",
                "message": "Request deadline of 100 ms exceeded",
            })
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server)
            await conn.connect()
            with pytest.raises(DeadlineExceeded) as caught:
                await conn.execute("?slow(X)", timeout=0.1)
            assert caught.value.code == "deadline_exceeded"
            await conn.close()

    async def test_silent_server_gets_a_cancel_and_the_call_a_deadline(self) -> None:
        frames: list[dict[str, Any]] = []

        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            q = await peer.recv_type("execute")
            cancel = await peer.recv_type("cancel")
            frames.extend([q, cancel])
            # Answer only after the client gave up: the late reply is dropped
            # and its slot comes back.
            await asyncio.sleep(0.2)
            await peer.send({**result([[1]]), "id": q["id"]})
            await peer.send({
                "type": "cancel_ack", "id": cancel["id"], "target": q["id"],
                "outcome": "not_found",
            })
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server, deadline_grace=0.05, max_in_flight=1)
            await conn.connect()
            with pytest.raises(DeadlineExceeded):
                await conn.execute("?slow(X)", timeout=0.1)
            assert frames[1]["target"] == frames[0]["id"]
            # The one slot is held by the abandoned request until its reply.
            assert (await conn.execute("?next(X)", timeout=2)).rows == []
            assert conn.in_flight == 0
            await conn.close()

    async def test_silent_server_leaves_a_write_outcome_unknown(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            q = await peer.recv_type("execute")
            cancel = await peer.recv_type("cancel")
            await asyncio.sleep(0.2)
            await peer.send({**result([]), "id": q["id"]})
            await peer.send({
                "type": "cancel_ack", "id": cancel["id"], "target": q["id"],
                "outcome": "not_found",
            })
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server, deadline_grace=0.05)
            await conn.connect()
            with pytest.raises(OutcomeUnknownError) as caught:
                await conn.execute("+a(1)", timeout=0.1)
            assert not isinstance(caught.value, DeadlineExceeded)
            await _until(lambda: conn.in_flight == 0)
            await conn.close()

    async def test_confirmed_cancel_of_a_silent_write_is_a_deadline(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            q = await peer.recv_type("execute")
            cancel = await peer.recv_type("cancel")
            await peer.send({
                "type": "cancel_ack", "id": cancel["id"], "target": q["id"],
                "outcome": "cancelled",
            })
            await asyncio.sleep(0.2)
            await peer.send(
                {"type": "error", "id": q["id"], "code": "cancelled", "message": "Cancelled"}
            )
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server, deadline_grace=0.05)
            await conn.connect()
            with pytest.raises(DeadlineExceeded):
                await conn.execute("+a(1)", timeout=0.1)
            await _until(lambda: conn.in_flight == 0)
            await conn.close()

    async def test_server_deadline_counts_the_wait_for_a_slot(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            first = await peer.recv_type("execute")
            await asyncio.sleep(0.4)
            await peer.send({**result([]), "id": first["id"]})
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server, max_in_flight=1)
            await conn.connect()
            held = asyncio.ensure_future(conn.execute("?a(X)"))
            await _until(lambda: conn.in_flight == 1)
            await conn.execute("?b(X)", timeout=1.0)
            await held
            second = [f for f in server.received if f["type"] == "execute"][1]
            assert second["timeout_ms"] <= 700
            await conn.close()

    async def test_rate_limited_is_not_retried_past_the_deadline(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            q = await peer.recv_type("execute")
            await peer.send({
                "type": "error", "id": q["id"], "code": "rate_limited", "message": "slow down",
            })
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server)
            await conn.connect()
            with pytest.raises(RateLimited):
                await conn.execute("?a(X)", timeout=0.5)
            assert len([f for f in server.received if f["type"] == "execute"]) == 1
            await conn.close()

    async def test_too_late_cancel_returns_the_committed_result(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            q = await peer.recv_type("execute")
            cancel = await peer.recv_type("cancel")
            inserted = result([["Inserted 1 fact(s) into 'a'."]], ["message"])
            await peer.send({**inserted, "id": q["id"]})
            await peer.send({
                "type": "cancel_ack", "id": cancel["id"], "target": q["id"],
                "outcome": "too_late",
            })
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server, deadline_grace=0.05)
            await conn.connect()
            reply = await conn.execute("+a(1)", timeout=0.05)
            assert reply.rows == [["Inserted 1 fact(s) into 'a'."]]
            await conn.close()

    async def test_cancelling_the_caller_cancels_on_the_server(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            q = await peer.recv_type("execute")
            cancel = await peer.recv_type("cancel")
            assert cancel["target"] == q["id"]
            await peer.send(
                {"type": "error", "id": q["id"], "code": "cancelled", "message": "Cancelled"}
            )
            await peer.send({
                "type": "cancel_ack", "id": cancel["id"], "target": q["id"],
                "outcome": "cancelled",
            })
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server)
            await conn.connect()
            call = asyncio.ensure_future(conn.execute("?slow(X)"))
            await _until(lambda: any(f["type"] == "execute" for f in server.received))
            call.cancel()
            with pytest.raises(asyncio.CancelledError):
                await call
            await _until(lambda: conn.in_flight == 0)
            await conn.close()

    async def test_explicit_cancel_returns_the_outcome(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            q = await peer.recv_type("execute")
            cancel = await peer.recv_type("cancel")
            await peer.send(
                {"type": "error", "id": q["id"], "code": "cancelled", "message": "Cancelled"}
            )
            await peer.send({
                "type": "cancel_ack", "id": cancel["id"], "target": q["id"],
                "outcome": "cancelled",
            })
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server)
            await conn.connect()
            call = asyncio.ensure_future(conn.execute("?slow(X)"))
            await _until(lambda: any(f["type"] == "execute" for f in server.received))
            assert await conn.cancel("r2") == "cancelled"
            with pytest.raises(Cancelled):
                await call
            await conn.close()


# ── Idle delivery and keepalive ──────────────────────────────────────


class TestIdle:
    async def test_notifications_arrive_while_idle(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            await asyncio.sleep(0.05)
            await peer.send(notification(1))
            await peer.send(notification(2))
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server)
            await conn.connect()
            seen: list[int] = []

            async def consume() -> None:
                async for event in conn.dispatcher:
                    seen.append(event.seq)
                    if len(seen) == 2:
                        return

            await asyncio.wait_for(consume(), 2)
            assert seen == [1, 2]
            assert conn.last_seq == 2
            await conn.close()

    async def test_keepalive_pings_an_idle_connection(self) -> None:
        pings = asyncio.Event()

        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            frame = await peer.recv(timeout=3)
            assert frame["type"] == "ping" and frame["id"].startswith("r")
            await peer.send({"type": "pong", "id": frame["id"]})
            pings.set()
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server, keepalive=0.1)
            await conn.connect()
            await asyncio.wait_for(pings.wait(), 3)
            await _until(lambda: conn.in_flight == 0)
            await conn.close()

    async def test_keepalive_does_not_ping_while_a_reply_is_owed(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            q = await peer.recv_type("execute")
            await asyncio.sleep(0.5)
            await peer.send({**result([]), "id": q["id"]})
            await peer.serve_results()

        async with MockServer(handler) as server:
            conn = _conn(server, keepalive=0.1)
            await conn.connect()
            await conn.execute("?slow(X)")
            assert [f["type"] for f in server.received[1:]] == ["execute"]
            await _until(lambda: any(f["type"] == "ping" for f in server.received))
            await conn.close()

    async def test_keepalive_waits_while_requests_flow(self) -> None:
        async with MockServer(_serve) as server:
            conn = _conn(server, keepalive=0.3)
            await conn.connect()
            for _ in range(5):
                await conn.execute("?a(X)")
                await asyncio.sleep(0.1)
            assert not [f for f in server.received if f["type"] == "ping"]
            await conn.close()


# ── Losing the connection and reconnecting ───────────────────────────


class TestReconnect:
    async def test_reconnect_restores_graph_and_notification_cursor(self) -> None:
        async def first(peer: Peer) -> None:
            await peer.authenticate()
            q = await peer.recv_type("execute")
            assert q["program"] == ".kg use other"
            switched = result([["Switched"]], ["message"], switched_kg="other")
            await peer.send({**switched, "id": q["id"]})
            await peer.send(notification(41, kg="other"))
            await peer.send(notification(42, kg="other"))
            await peer.recv_type("execute")  # the write that will be lost
            await peer.close(1011)

        async def second(peer: Peer) -> None:
            await peer.authenticate()
            await peer.send({"type": "notice", "code": "replay_gap", "message": "evicted"})
            await peer.serve_results()

        async with MockServer(first, second) as server:
            conn = _conn(server, auto_reconnect=True, reconnect_delay=0.01, initial_kg="main")
            events: list[ConnectionEvent] = []
            conn.events.on(callback=events.append)
            hooked: list[str | None] = []

            async def hook(c: Connection) -> None:
                hooked.append(c.current_kg)

            conn.add_reconnect_hook(hook)
            await conn.connect()
            await conn.execute(".kg use other")
            await _until(lambda: conn.last_seq == 42)
            with pytest.raises(ConnectionLost) as caught:
                await conn.execute("+a(1)")
            assert caught.value.may_have_committed is True
            await _until(lambda: any(e.type == "notification_gap" for e in events))
            assert server.peers[1].params == {
                "kg": "other", "last_seq": "42", "epoch": EPOCH,
            }
            assert [e.type for e in events] == [
                "disconnected", "session_reset", "reconnected", "notification_gap",
            ]
            assert hooked == ["other"]
            assert (await conn.execute("?a(X)")).rows == []
            await conn.close()

    async def test_new_engine_run_restarts_the_cursor(self) -> None:
        async def first(peer: Peer) -> None:
            await peer.authenticate()
            await peer.send(notification(100))
            await asyncio.sleep(0.1)
            await peer.close(1011)

        async def second(peer: Peer) -> None:
            await peer.authenticate(epoch="ffeeddccbbaa9988")
            await peer.send({"type": "notice", "code": "replay_gap", "message": "new run"})
            await peer.send(notification(5))
            await asyncio.sleep(0.1)
            await peer.close(1011)

        async def third(peer: Peer) -> None:
            await peer.authenticate(epoch="ffeeddccbbaa9988")
            await peer.serve_results()

        async with MockServer(first, second, third) as server:
            conn = _conn(server, auto_reconnect=True, reconnect_delay=0.01)
            await conn.connect()
            await _until(lambda: len(server.peers) == 3 and conn.connected)
            assert server.peers[2].params == {
                "kg": "default", "last_seq": "5", "epoch": "ffeeddccbbaa9988",
            }
            assert conn.last_seq == 5
            assert conn.dispatcher.last_seq == 5
            await conn.close()

    async def test_reconnect_without_a_cursor_reports_a_gap(self) -> None:
        async def first(peer: Peer) -> None:
            await peer.authenticate()
            await peer.close(1011)

        async with MockServer(first, _serve) as server:
            conn = _conn(server, auto_reconnect=True, reconnect_delay=0.01)
            events: list[ConnectionEvent] = []
            conn.events.on(callback=events.append)
            await conn.connect()
            await _until(lambda: any(e.type == "notification_gap" for e in events))
            assert "last_seq" not in server.peers[1].params
            assert [e.type for e in events] == [
                "disconnected", "session_reset", "reconnected", "notification_gap",
            ]
            await conn.close()

    async def test_calls_during_a_reconnect_wait_for_it(self) -> None:
        async def first(peer: Peer) -> None:
            await peer.authenticate()
            await peer.close(1011)

        async def second(peer: Peer) -> None:
            await asyncio.sleep(0.2)
            await peer.authenticate()
            await peer.serve_results(lambda program: result([[program]]))

        async with MockServer(first, second) as server:
            conn = _conn(server, auto_reconnect=True, reconnect_delay=0.01)
            await conn.connect()
            await _until(lambda: conn.state == "reconnecting")
            assert (await conn.execute("?a(X)", timeout=5)).rows == [["?a(X)"]]
            await conn.close()

    async def test_reconnect_gives_up_and_ends_iterators(self) -> None:
        async def first(peer: Peer) -> None:
            await peer.authenticate()
            await peer.close(1011)

        async def refuse(peer: Peer) -> None:
            await peer.close(1013)

        async with MockServer(first, refuse) as server:
            conn = _conn(
                server, auto_reconnect=True, reconnect_delay=0.01, max_reconnect_attempts=2
            )
            await conn.connect()

            async def consume() -> None:
                async for _ in conn.dispatcher:
                    pass

            consumer = asyncio.ensure_future(consume())
            await asyncio.sleep(0)
            with pytest.raises(ConnectionLost):
                await asyncio.wait_for(consumer, 5)
            assert conn.state == "closed"
            with pytest.raises(ConnectionLost):
                await conn.execute("?a(X)")
            await conn.close()

    async def test_revoked_credential_does_not_reconnect(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            await peer.send({"type": "notice", "code": "credential_revoked", "message": "revoked"})
            await peer.close(1008)

        async with MockServer(handler) as server:
            conn = _conn(server, auto_reconnect=True, reconnect_delay=0.01)
            closed = asyncio.Event()
            conn.events.on("closed", callback=lambda e: closed.set())
            await conn.connect()
            await asyncio.wait_for(closed.wait(), 2)
            assert len(server.peers) == 1
            with pytest.raises(ConnectionLost) as caught:
                await conn.execute("?a(X)")
            assert caught.value.code == "credential_revoked"
            await conn.close()

    async def test_close_fails_pending_calls(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            await peer.recv_type("execute")
            await asyncio.sleep(5)

        async with MockServer(handler) as server:
            conn = _conn(server)
            await conn.connect()
            call = asyncio.ensure_future(conn.execute("?a(X)"))
            await _until(lambda: conn.in_flight == 1)
            await conn.close()
            with pytest.raises(ConnectionLost) as caught:
                await call
            assert caught.value.may_have_committed is False


# ── The client: one connection per knowledge graph ───────────────────


class TestPool:
    async def test_each_graph_gets_its_own_bound_connection(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            kg = peer.params.get("kg", "default")
            await peer.serve_results(lambda program: result([[kg, program]], ["kg", "p"]))

        async with MockServer(handler) as server:
            async with InputLayer(server.url, username="a", password="b") as il:
                sales, ops = il.knowledge_graph("sales"), il.knowledge_graph("ops")
                default = il.knowledge_graph("default")
                rows = await asyncio.gather(
                    sales.execute("?s(X)"), ops.execute("?o(X)"), default.execute("?d(X)")
                )
                assert [r.rows for r in rows] == [
                    [["sales", "?s(X)"]], [["ops", "?o(X)"]], [["default", "?d(X)"]],
                ]
            # Every handle has its own connection; no .kg use was sent.
            assert sorted(p.params.get("kg", "") for p in server.peers) == [
                "", "default", "ops", "sales",
            ]
            assert not [f for f in server.received if f.get("program", "").startswith(".kg use")]

    async def test_missing_graph_is_created_then_bound(self) -> None:
        created: list[str] = []

        async def handler(peer: Peer) -> None:
            kg = peer.params.get("kg")
            if kg == "fresh" and "fresh" not in created:
                login = await peer.recv()
                await peer.send({
                    "type": "auth_error", "id": login["id"],
                    "message": "Knowledge graph 'fresh' not found",
                })
                return
            await peer.authenticate()

            def answer(program: str) -> dict[str, Any]:
                if program == ".kg create fresh":
                    created.append("fresh")
                return result([])

            await peer.serve_results(answer)

        async with MockServer(handler) as server:
            async with InputLayer(server.url, username="a", password="b") as il:
                await il.knowledge_graph("fresh").execute("?a(X)")
            assert created == ["fresh"]

    async def test_a_notification_on_two_connections_is_delivered_once(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            await peer.send({
                "type": "kg_change", "seq": 9, "timestamp_ms": 0, "knowledge_graph": "new",
            })
            await peer.serve_results()

        async with MockServer(handler) as server:
            il = InputLayer(server.url, username="a", password="b")
            seen: list[int] = []
            il.on("kg_change")(lambda e: seen.append(e.seq))
            async with il:
                await il.knowledge_graph("other").execute("?a(X)")
                await asyncio.sleep(0.1)
                assert len(server.peers) == 2
                assert seen == [9]
                assert il.last_seq == 9

    async def test_creating_a_graph_does_not_switch_any_handle(self) -> None:
        created: list[str] = []

        async def handler(peer: Peer) -> None:
            kg = peer.params.get("kg")
            if kg == "b" and "b" not in created:
                login = await peer.recv()
                await peer.send({
                    "type": "auth_error", "id": login["id"],
                    "message": "Knowledge graph 'b' not found",
                })
                return
            await peer.authenticate()

            def answer(program: str) -> dict[str, Any]:
                if program.startswith(".kg create "):
                    name = program.split()[-1]
                    created.append(name)
                    return result([["created"]], ["message"], switched_kg=name)
                if program.startswith(".kg drop "):
                    return result([["dropped"]], ["message"])
                return result([[kg]], ["kg"])

            await peer.serve_results(answer)

        async with MockServer(handler) as server:
            async with InputLayer(server.url, username="a", password="b", initial_kg="a") as il:
                a, b = il.knowledge_graph("a"), il.knowledge_graph("b")
                assert (await b.execute("?x(X)")).rows == [["b"]]
                assert (await a.execute("?x(X)")).rows == [["a"]]
                assert il._conn.current_kg == "a"
            assert created == ["b"]
            assert len([p for p in server.peers if "kg" not in p.params]) == 1
            assert not [f for f in server.received if f.get("program", "").startswith(".kg use")]

    async def test_dropping_a_graph_leaves_nothing_bound_to_it(self) -> None:
        graphs = {"default", "a"}

        async def handler(peer: Peer) -> None:
            kg = peer.params.get("kg", "default")
            if kg not in graphs:
                login = await peer.recv()
                await peer.send({
                    "type": "auth_error", "id": login["id"],
                    "message": f"Knowledge graph '{kg}' not found",
                })
                return
            await peer.authenticate()

            def answer(program: str) -> dict[str, Any]:
                verb, _, name = program.partition(" ")[2].partition(" ")
                if verb == "create":
                    graphs.add(name)
                    return result([["created"]], ["message"], switched_kg=name)
                if verb == "use":
                    return result([["switched"]], ["message"], switched_kg=name)
                if verb == "drop":
                    graphs.discard(name)
                    return result([["dropped"]], ["message"])
                return result([[kg]], ["kg"])

            await peer.serve_results(answer)

        async with MockServer(handler) as server:
            async with InputLayer(server.url, username="a", password="b", initial_kg="a") as il:
                stale = il.knowledge_graph("a")
                await stale.execute("?x(X)")
                await il.drop_knowledge_graph("a")
                assert graphs == {"default"}
                assert il._conn.current_kg == "default"
                with pytest.raises(ConnectionLost):
                    await stale.execute("+x(1)")
                assert graphs == {"default"}
                fresh = il.knowledge_graph("a")
                assert fresh is not stale
            # The client's own connection and the handle's; no reopen after the drop.
            assert len([p for p in server.peers if p.params.get("kg") == "a"]) == 2
            assert not [f for f in server.received if f.get("program") == ".kg create a"]

    async def test_losing_one_graph_does_not_end_the_client_notifications(self) -> None:
        go = asyncio.Event()

        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            if peer.params.get("kg") == "other":
                q = await peer.recv_type("execute")
                await peer.send({**result([]), "id": q["id"]})
                await peer.close(1011)
                return
            await go.wait()
            await peer.send(notification(7))
            await peer.serve_results()

        async with MockServer(handler) as server:
            il = InputLayer(server.url, username="a", password="b", auto_reconnect=False)
            closed: list[str | None] = []
            il.events.on("closed", callback=lambda e: closed.append(e.knowledge_graph))
            async with il:
                seen: list[int] = []

                async def consume() -> None:
                    async for event in il.notifications():
                        seen.append(event.seq)
                        return

                consumer = asyncio.ensure_future(consume())
                await asyncio.sleep(0)
                await il.knowledge_graph("other").execute("?a(X)")
                await _until(lambda: closed == ["other"])
                go.set()
                await asyncio.wait_for(consumer, 2)
                assert seen == [7]
            assert len(server.peers) == 2

    async def test_handles_work_again_after_close_and_connect(self) -> None:
        async def handler(peer: Peer) -> None:
            await peer.authenticate()
            kg = peer.params.get("kg", "default")
            await peer.serve_results(lambda program: result([[kg]], ["kg"]))

        async with MockServer(handler) as server:
            il = InputLayer(server.url, username="a", password="b")
            async with il:
                sales, default = il.knowledge_graph("sales"), il.knowledge_graph("default")
                await sales.execute("?a(X)")
            async with il:
                assert (await sales.execute("?a(X)")).rows == [["sales"]]
                assert (await default.execute("?a(X)")).rows == [["default"]]
                assert (await il.knowledge_graph("sales").execute("?a(X)")).rows == [["sales"]]

    async def test_a_call_during_close_does_not_reopen(self) -> None:
        async with MockServer(_serve) as server:
            conn = _conn(server, lazy=True)
            await conn.execute("?a(X)")
            closing = asyncio.ensure_future(conn.close())
            await asyncio.sleep(0)
            with pytest.raises(ConnectionLost):
                await conn.execute("?b(X)")
            await closing
            assert len(server.peers) == 1
            assert (await conn.execute("?c(X)")).rows == []
            assert len(server.peers) == 2
            await conn.close(final=True)
            with pytest.raises(ConnectionLost):
                await conn.execute("?d(X)")
            assert len(server.peers) == 2
