"""Subscriptions against a scripted engine on a real socket.

A fake engine answers ``.subscribe`` with a snapshot (and a new generation),
``.unsubscribe`` and ``.session``; each test pushes the frames it needs and
reads the ``Change`` events the SDK makes of them.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from inputlayer import (
    Change,
    ConnectionLost,
    InputLayer,
    InputLayerSync,
    Relation,
    Row,
    Subscription,
    SubscriptionRejected,
)
from inputlayer.aggregations import count
from inputlayer.knowledge_graph import KnowledgeGraph

from ._mock_server import MockServer, Peer, result


class Edge(Relation):
    x: int
    y: int


async def _until(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    async def poll() -> None:
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


async def _next(sub: Subscription[Any], timeout: float = 5.0) -> Change[Any]:
    return await asyncio.wait_for(sub.__anext__(), timeout)


class Engine:
    """Answers what a subscription sends; the test pushes the rest."""

    def __init__(self, rows: list[list[Any]] | None = None, columns: list[str] | None = None):
        self.rows = rows or []
        self.columns = columns or ["X", "Y"]
        self.revision = 10
        self.generation = 0
        self.programs: list[str] = []
        self.refuse: str | None = None
        self.session_rules: list[str] = []
        self.peer: Peer | None = None
        # Frames sent before the reply to the next .subscribe.
        self.before_reply: list[dict[str, Any]] = []

    async def handler(self, peer: Peer) -> None:
        await peer.authenticate()
        self.peer = peer
        try:
            while True:
                frame = await peer.recv(timeout=60)
                if frame["type"] == "ping":
                    await peer.send({"type": "pong", "id": frame.get("id")})
                elif frame["type"] == "execute":
                    await self._answer(peer, frame)
        except Exception:
            return

    async def _answer(self, peer: Peer, frame: dict[str, Any]) -> None:
        program = frame["program"]
        self.programs.append(program)
        if program.startswith(".subscribe"):
            if self.refuse is not None:
                await peer.send({"type": "error", "message": self.refuse, "id": frame["id"]})
                return
            self.generation += 1
            for early in self.before_reply:
                await peer.send({**early, "generation": self.generation})
            self.before_reply = []
            reply = result(
                [list(r) for r in self.rows],
                self.columns if self.rows else [],
                subscribed={
                    "subscription": program.split()[1],
                    "generation": self.generation,
                    "revision": self.revision,
                },
            )
        elif program == ".session":
            lines = [f"Session rules ({len(self.session_rules)}):"] + [
                f"  {i + 1}. {rule}" for i, rule in enumerate(self.session_rules)
            ]
            reply = result(
                [[line] for line in lines]
                if self.session_rules
                else [["No session data defined."]],
                ["message"],
            )
        else:
            reply = result([])
        await peer.send({**reply, "id": frame["id"]})

    def subscribes(self) -> list[str]:
        return [p.split()[0] for p in self.programs if p.startswith((".subscribe", ".unsubscribe"))]

    async def push(self, frame: dict[str, Any]) -> None:
        assert self.peer is not None
        await self.peer.send(frame)

    async def delta(
        self,
        sub: Subscription[Any],
        seq: int,
        inserted: list[list[Any]] = (),  # type: ignore[assignment]
        retracted: list[list[Any]] = (),  # type: ignore[assignment]
        *,
        generation: int | None = None,
    ) -> None:
        self.revision += 1
        await self.push(
            {
                "type": "subscription_delta",
                "subscription": sub.id,
                "generation": generation if generation is not None else self.generation,
                "knowledge_graph": "default",
                "seq": seq,
                "revision": self.revision,
                "columns": self.columns,
                "inserted": list(inserted),
                "retracted": list(retracted),
            }
        )


async def _setup(engine: Engine, **client: Any) -> tuple[MockServer, InputLayer, KnowledgeGraph]:
    server = MockServer(engine.handler)
    await server.__aenter__()
    client.setdefault("reconnect_delay", 0.05)
    il = InputLayer(server.url, username="admin", password="pw", **client)
    return server, il, il.knowledge_graph("default")


async def _teardown(server: MockServer, il: InputLayer) -> None:
    await il.close()
    await server.stop()


def _values(rows: list[Any]) -> list[tuple[Any, ...]]:
    return sorted(tuple(r.values_tuple()) if isinstance(r, Row) else (r.x, r.y) for r in rows)


# ── Snapshot and deltas ──────────────────────────────────────────────


async def test_snapshot_then_deltas_with_typed_rows() -> None:
    engine = Engine([[1, 2], [3, 4]])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(Edge)
    assert sub.query == "?edge(X, Y)"
    snap = await _next(sub)
    assert snap.kind == "snapshot" and snap.verified and snap.seq == 0
    assert snap.revision == 10 and snap.retracted == []
    assert sorted(snap.inserted, key=lambda e: e.x) == [Edge(x=1, y=2), Edge(x=3, y=4)]
    await engine.delta(sub, 1, inserted=[[5, 6]], retracted=[[1, 2]])
    delta = await _next(sub)
    assert (delta.kind, delta.seq, delta.revision) == ("delta", 1, 11)
    assert delta.inserted == [Edge(x=5, y=6)] and delta.retracted == [Edge(x=1, y=2)]
    await sub.close()
    assert engine.subscribes() == [".subscribe", ".unsubscribe"]
    await _teardown(server, il)


async def test_raw_iql_rows_are_records_by_column() -> None:
    engine = Engine([[1, "a"]], ["N", "S"])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(iql="?pair(N, S)")
    snap = await _next(sub)
    [row] = snap.inserted
    assert isinstance(row, Row)
    assert (row.N, row["S"], dict(row)) == (1, "a", {"N": 1, "S": "a"})
    assert row == Row(["N", "S"], [1, "a"]) and len({row, Row(["N", "S"], [1, "a"])}) == 1
    await sub.close()
    await _teardown(server, il)


async def test_streamed_snapshot_and_streamed_delta_are_assembled() -> None:
    engine = Engine()
    server, il, kg = await _setup(engine)

    answer = engine._answer

    async def streamed_subscribe(peer: Peer, frame: dict[str, Any]) -> None:
        if not frame["program"].startswith(".subscribe"):
            return await answer(peer, frame)
        engine.programs.append(frame["program"])
        engine.generation += 1
        rid = frame["id"]
        sub_id = frame["program"].split()[1]
        await peer.send(
            {
                "type": "result_start",
                "id": rid,
                "columns": ["X", "Y"],
                "total_count": 3,
                "truncated": False,
                "execution_time_ms": 0,
                "subscribed": {"subscription": sub_id, "generation": 1, "revision": 10},
            }
        )
        await peer.send(
            {"type": "result_chunk", "id": rid, "chunk_index": 0, "rows": [[1, 1], [2, 2]]}
        )
        await peer.send({"type": "result_chunk", "id": rid, "chunk_index": 1, "rows": [[3, 3]]})
        await peer.send({"type": "result_end", "id": rid, "row_count": 3, "chunk_count": 2})

    engine._answer = streamed_subscribe  # type: ignore[method-assign]
    sub = kg.subscribe(Edge)
    snap = await _next(sub)
    assert _values(snap.inserted) == [(1, 1), (2, 2), (3, 3)]
    base = {"subscription": sub.id, "generation": 1, "seq": 1}
    await engine.push(
        {
            **base,
            "type": "subscription_delta_start",
            "knowledge_graph": "default",
            "revision": 12,
            "columns": ["X", "Y"],
        }
    )
    await engine.push(
        {
            **base,
            "type": "subscription_delta_chunk",
            "chunk_index": 0,
            "inserted": [[4, 4]],
            "retracted": [[1, 1]],
        }
    )
    await engine.push(
        {
            **base,
            "type": "subscription_delta_chunk",
            "chunk_index": 1,
            "inserted": [[5, 5]],
            "retracted": [],
        }
    )
    await engine.push(
        {
            **base,
            "type": "subscription_delta_end",
            "chunk_count": 2,
            "inserted_count": 2,
            "retracted_count": 1,
        }
    )
    delta = await _next(sub)
    assert (delta.kind, delta.seq, delta.revision) == ("delta", 1, 12)
    assert _values(delta.inserted) == [(4, 4), (5, 5)] and _values(delta.retracted) == [(1, 1)]
    await sub.close()
    await _teardown(server, il)


async def test_a_push_before_the_snapshot_reply_is_applied_after_it() -> None:
    engine = Engine([[1, 1]])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(Edge)
    engine.before_reply = [
        {
            "type": "subscription_delta",
            "subscription": sub.id,
            "knowledge_graph": "default",
            "seq": 1,
            "revision": 11,
            "columns": ["X", "Y"],
            "inserted": [[2, 2]],
            "retracted": [],
        }
    ]
    assert (await _next(sub)).kind == "snapshot"
    delta = await _next(sub)
    assert delta.kind == "delta" and _values(delta.inserted) == [(2, 2)]
    await sub.close()
    await _teardown(server, il)


# ── Projection ───────────────────────────────────────────────────────


async def test_projection_two_supporting_tuples() -> None:
    engine = Engine([[1, 1], [1, 2]])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(Edge.x)
    snap = await _next(sub)
    assert snap.inserted == [Row(["x"], [1])]
    # (1, 1) leaves, (1, 2) still supports x = 1: nothing to report.
    await engine.delta(sub, 1, retracted=[[1, 1]])
    await engine.delta(sub, 2, inserted=[[2, 1]])
    second = await _next(sub)
    assert second.seq == 2 and second.inserted == [Row(["x"], [2])] and second.retracted == []
    await engine.delta(sub, 3, retracted=[[1, 2]])
    third = await _next(sub)
    assert third.retracted == [Row(["x"], [1])] and third.inserted == []
    # A support moving within one delta is no change either.
    await engine.delta(sub, 4, inserted=[[2, 5]], retracted=[[2, 1]])
    await engine.delta(sub, 5, inserted=[[7, 7]])
    assert (await _next(sub)).seq == 5
    await sub.close()
    await _teardown(server, il)


# ── Unverified and resync ────────────────────────────────────────────


async def test_seq_gap_unsubscribes_and_resyncs_with_the_exact_difference() -> None:
    engine = Engine([[1, 1], [2, 2]])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(Edge)
    await _next(sub)
    engine.rows = [[2, 2], [3, 3]]
    await engine.delta(sub, 2, inserted=[[9, 9]])
    gap = await _next(sub)
    assert (gap.kind, gap.reason, gap.verified) == ("unverified", "seq_gap", False)
    assert (gap.inserted, gap.retracted, gap.revision, gap.seq) == ([], [], 10, 0)
    resync = await _next(sub)
    assert resync.kind == "resync" and resync.verified and resync.seq == 0
    assert _values(resync.inserted) == [(3, 3)] and _values(resync.retracted) == [(1, 1)]
    assert engine.subscribes() == [".subscribe", ".unsubscribe", ".subscribe"]
    assert sub.stats.resubscribes == 1
    # The new generation's deltas count from 1 again.
    await engine.delta(sub, 1, inserted=[[4, 4]])
    assert (await _next(sub)).seq == 1
    await sub.close()
    await _teardown(server, il)


async def test_stale_generation_is_dropped_and_counted() -> None:
    engine = Engine([[1, 1]])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(Edge)
    await _next(sub)
    await engine.delta(sub, 5)  # a gap: reopen with generation 2
    assert (await _next(sub)).kind == "unverified"
    assert (await _next(sub)).kind == "resync"
    await engine.delta(sub, 1, inserted=[[8, 8]], generation=1)
    await engine.delta(sub, 1, inserted=[[2, 2]])
    delta = await _next(sub)
    assert _values(delta.inserted) == [(2, 2)]
    assert sub.stats.stale_dropped == 1
    await sub.close()
    await _teardown(server, il)


@pytest.mark.parametrize(
    "frames",
    [
        # A chunk is missing.
        [("start", 1), ("chunk", 0), ("chunk", 2), ("end", 3)],
        # The end announces other counts.
        [("start", 1), ("chunk", 0), ("end", 5)],
        # A chunk without a start.
        [("chunk", 0)],
        # A whole delta inside a streamed one.
        [("start", 1), ("delta", 2)],
    ],
)
async def test_broken_stream_resyncs(frames: list[tuple[str, int]]) -> None:
    engine = Engine([[1, 1]])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(Edge)
    await _next(sub)
    base = {"subscription": sub.id, "generation": 1, "seq": 1}
    for kind, n in frames:
        if kind == "start":
            await engine.push(
                {
                    **base,
                    "type": "subscription_delta_start",
                    "knowledge_graph": "default",
                    "revision": 11,
                    "columns": ["X", "Y"],
                }
            )
        elif kind == "chunk":
            await engine.push(
                {
                    **base,
                    "type": "subscription_delta_chunk",
                    "chunk_index": n,
                    "inserted": [[n + 5, n + 5]],
                    "retracted": [],
                }
            )
        elif kind == "end":
            await engine.push(
                {
                    **base,
                    "type": "subscription_delta_end",
                    "chunk_count": n,
                    "inserted_count": n,
                    "retracted_count": 0,
                }
            )
        else:
            await engine.delta(sub, n)
    broken = await _next(sub)
    assert (broken.kind, broken.reason) == ("unverified", "broken_stream")
    assert (await _next(sub)).kind == "resync"
    await sub.close()
    await _teardown(server, il)


async def test_retracting_a_row_not_held_is_a_broken_stream() -> None:
    engine = Engine([[1, 1]])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(Edge)
    await _next(sub)
    await engine.delta(sub, 1, retracted=[[5, 5]])
    assert (await _next(sub)).reason == "broken_stream"
    await sub.close()
    await _teardown(server, il)


async def test_reset_resubscribes_without_unsubscribing() -> None:
    engine = Engine([[1, 1]])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(Edge)
    await _next(sub)
    engine.rows = []
    await engine.push(
        {
            "type": "subscription_reset",
            "subscription": sub.id,
            "generation": 1,
            "message": "Delta 4 has a row over the message limit.",
        }
    )
    reset = await _next(sub)
    assert (reset.kind, reset.reason) == ("unverified", "subscription_reset")
    assert reset.message == "Delta 4 has a row over the message limit."
    resync = await _next(sub)
    assert _values(resync.retracted) == [(1, 1)] and resync.inserted == []
    assert engine.subscribes() == [".subscribe", ".subscribe"]
    await sub.close()
    await _teardown(server, il)


async def test_subscription_error_unsubscribes_then_resubscribes() -> None:
    engine = Engine([[1, 1]])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(Edge)
    await _next(sub)
    await engine.push(
        {
            "type": "subscription_error",
            "subscription": sub.id,
            "generation": 1,
            "message": "evaluation failed",
        }
    )
    error = await _next(sub)
    assert (error.reason, error.message) == ("subscription_error", "evaluation failed")
    resync = await _next(sub)
    assert (resync.kind, resync.inserted, resync.retracted) == ("resync", [], [])
    assert engine.subscribes() == [".subscribe", ".unsubscribe", ".subscribe"]
    await sub.close()
    await _teardown(server, il)


async def test_a_refused_reopen_ends_the_iterator_with_the_refusal() -> None:
    engine = Engine([[1, 1]])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(Edge)
    await _next(sub)
    engine.refuse = "Access denied: no read access to knowledge graph 'default'"
    await engine.push(
        {
            "type": "subscription_error",
            "subscription": sub.id,
            "generation": 1,
            "message": "Access denied",
        }
    )
    assert (await _next(sub)).reason == "subscription_error"
    with pytest.raises(SubscriptionRejected) as err:
        await _next(sub)
    assert err.value.reason == "access_denied"
    with pytest.raises(StopAsyncIteration):
        await _next(sub)
    await _teardown(server, il)


async def test_slow_consumer_gets_unverified_then_a_resync_once_it_has_read() -> None:
    engine = Engine([[0, 0]])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(Edge, queue=2)
    await _next(sub)
    for seq in range(1, 4):
        await engine.delta(sub, seq, inserted=[[seq, seq]])
    await _until(lambda: ".unsubscribe" in engine.subscribes())
    engine.rows = [[0, 0], [1, 1], [2, 2], [3, 3]]
    kinds = []
    for _ in range(4):
        change = await _next(sub)
        kinds.append(change.reason or change.kind)
    assert kinds == ["delta", "delta", "slow_consumer", "resync"]
    assert engine.subscribes() == [".subscribe", ".unsubscribe", ".subscribe"]
    await sub.close()
    await _teardown(server, il)


async def test_reconnect_gives_unverified_at_once_then_a_resync() -> None:
    engine = Engine([[1, 1], [2, 2]])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(Edge)
    await _next(sub)
    engine.rows = [[2, 2], [3, 3]]
    assert engine.peer is not None
    await engine.peer.close()
    lost = await _next(sub)
    assert (lost.kind, lost.reason) == ("unverified", "connection_lost")
    resync = await _next(sub)
    assert _values(resync.inserted) == [(3, 3)] and _values(resync.retracted) == [(1, 1)]
    # The old connection took the subscription with it: no .unsubscribe.
    assert engine.subscribes() == [".subscribe", ".subscribe"]
    assert len(server.peers) == 2
    await sub.close()
    await _teardown(server, il)


async def test_a_transient_failure_of_the_first_open_is_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("inputlayer.subscription.RESUBSCRIBE_DELAY", 0.01)
    engine = Engine([[1, 1]])
    server, il, kg = await _setup(engine)
    answer = engine._answer
    dropped: list[str] = []

    async def drop_first_subscribe(peer: Peer, frame: dict[str, Any]) -> None:
        if frame["program"].startswith(".subscribe") and not dropped:
            dropped.append(frame["program"])
            await peer.close()
            return
        await answer(peer, frame)

    engine._answer = drop_first_subscribe  # type: ignore[method-assign]
    sub = kg.subscribe(Edge)
    snap = await _next(sub)
    assert (snap.kind, snap.verified, _values(snap.inserted)) == ("snapshot", True, [(1, 1)])
    assert dropped and engine.subscribes() == [".subscribe"]
    assert len(server.peers) == 2
    await sub.close()
    await _teardown(server, il)


async def test_connection_closed_for_good_ends_with_connection_lost() -> None:
    engine = Engine([[1, 1]])
    server, il, kg = await _setup(engine, auto_reconnect=False)
    sub = kg.subscribe(Edge)
    await _next(sub)
    assert engine.peer is not None
    await engine.peer.close()
    assert (await _next(sub)).reason == "connection_lost"
    with pytest.raises(ConnectionLost):
        await _next(sub)
    await _teardown(server, il)


async def test_client_close_ends_the_iterator_quietly() -> None:
    engine = Engine([[1, 1]])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(Edge)
    await _next(sub)
    await il.close()
    with pytest.raises(StopAsyncIteration):
        await _next(sub)
    await server.stop()


async def test_leaving_the_loop_unsubscribes() -> None:
    engine = Engine([[1, 1]])
    server, il, kg = await _setup(engine)

    async def first() -> Change[Any]:
        async for change in kg.subscribe(Edge):
            return change
        raise AssertionError("no event")

    assert (await first()).kind == "snapshot"
    await _until(lambda: engine.subscribes() == [".subscribe", ".unsubscribe"])
    await _teardown(server, il)


# ── Refusals ─────────────────────────────────────────────────────────


async def test_refused_before_anything_is_sent() -> None:
    kg = InputLayer("ws://127.0.0.1:1/ws", username="a", password="b").knowledge_graph("g")
    cases: list[tuple[Callable[[], Any], str]] = [
        (lambda: kg.subscribe(Edge, limit=1), "limit_offset"),
        (lambda: kg.subscribe(Edge, offset=2), "limit_offset"),
        (lambda: kg.subscribe(Edge, where=lambda e: (e.x == 1) | (e.x == 2)), "or_branches"),
        (lambda: kg.subscribe(Edge.x, count(Edge.y)), "session_view"),
        (lambda: kg.subscribe(Edge, where=lambda e: ~(e.x == 3)), None),
    ]
    for make, reason in cases:
        if reason is None:
            assert make().query.startswith("?edge(")
            continue
        with pytest.raises(SubscriptionRejected) as err:
            make()
        assert err.value.reason == reason


async def test_session_rules_are_refused() -> None:
    engine = Engine()
    engine.session_rules = ["mine(X) <- edge(X, _)"]
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(iql="?mine(X)")
    with pytest.raises(SubscriptionRejected) as err:
        await _next(sub)
    assert err.value.reason == "session_view"
    assert engine.subscribes() == []
    await _teardown(server, il)


@pytest.mark.parametrize(
    ("message", "reason"),
    [
        (
            "Subscriptions track the whole result set; remove limit/offset from the query.",
            "limit_offset",
        ),
        ("Result exceeds max_result_rows (1000)", "result_cap"),
        ("Access denied", "access_denied"),
        ("Subscription 'x' already exists on this connection", "id_taken"),
        ("Subscription limit reached (64 per connection)", "subscription_limit"),
        ("Parse error: unexpected token", "rejected"),
    ],
)
async def test_engine_refusals_are_typed(message: str, reason: str) -> None:
    engine = Engine()
    engine.refuse = message
    server, il, kg = await _setup(engine)
    sub = kg.subscribe(Edge)
    with pytest.raises(SubscriptionRejected) as err:
        await _next(sub)
    assert (err.value.reason, err.value.message) == (reason, message)
    assert err.value.query == f".subscribe {sub.id} ?edge(X, Y)"
    await _teardown(server, il)


# ── Levels and callbacks ─────────────────────────────────────────────


async def test_watch_yields_the_whole_result_and_its_verified_state() -> None:
    engine = Engine([[1, 1]])
    server, il, kg = await _setup(engine)
    levels = kg.watch(Edge)
    first = await asyncio.wait_for(levels.__anext__(), 5)
    assert (first.rows, first.verified, first.revision) == ([Edge(x=1, y=1)], True, 10)
    sub_id = next(p for p in engine.programs if p.startswith(".subscribe")).split()[1]
    engine.revision += 1
    await engine.push(
        {
            "type": "subscription_delta",
            "subscription": sub_id,
            "generation": 1,
            "knowledge_graph": "default",
            "seq": 1,
            "revision": 11,
            "columns": ["X", "Y"],
            "inserted": [[2, 2]],
            "retracted": [],
        }
    )
    second = await asyncio.wait_for(levels.__anext__(), 5)
    assert second.rows == [Edge(x=1, y=1), Edge(x=2, y=2)] and second.revision == 11
    assert engine.peer is not None
    await engine.peer.close()
    lost = await asyncio.wait_for(levels.__anext__(), 5)
    assert (lost.verified, lost.reason, len(lost.rows)) == (False, "connection_lost", 2)
    back = await asyncio.wait_for(levels.__anext__(), 5)
    assert back.verified and back.rows == [Edge(x=1, y=1)]
    await levels.aclose()  # type: ignore[attr-defined]
    await _until(lambda: engine.subscribes()[-1] == ".unsubscribe")
    await _teardown(server, il)


async def test_on_calls_back_with_each_change() -> None:
    engine = Engine([[1, 1]])
    server, il, kg = await _setup(engine)
    seen: list[Change[Any]] = []
    errors: list[BaseException] = []

    async def callback(change: Change[Any]) -> None:
        seen.append(change)
        if change.kind == "delta":
            raise ValueError("callback failed")

    handle = kg.on(Edge, callback, on_error=errors.append)
    await _until(lambda: len(seen) == 1)
    sub_id = handle.subscription.id
    await engine.push(
        {
            "type": "subscription_delta",
            "subscription": sub_id,
            "generation": 1,
            "knowledge_graph": "default",
            "seq": 1,
            "revision": 11,
            "columns": ["X", "Y"],
            "inserted": [[2, 2]],
            "retracted": [],
        }
    )
    await _until(lambda: len(seen) == 2)
    assert [c.kind for c in seen] == ["snapshot", "delta"]
    await _until(lambda: len(errors) == 1)
    assert isinstance(errors[0], ValueError)
    await handle.close()
    assert engine.subscribes() == [".subscribe", ".unsubscribe"]
    await _teardown(server, il)


def test_sync_subscribe_is_a_blocking_iterator() -> None:
    from inputlayer._sync import run_sync

    engine = Engine([[1, 1]])
    server = MockServer(engine.handler)
    run_sync(server.__aenter__())
    il = InputLayerSync(server.url, username="admin", password="pw")
    kg = il.knowledge_graph("default")
    sub = kg.subscribe(Edge)
    assert sub.next().inserted == [Edge(x=1, y=1)]
    run_sync(engine.delta(sub._sub, 1, inserted=[[2, 2]]))
    assert sub.next().inserted == [Edge(x=2, y=2)]
    sub.close()
    seen: list[Change[Any]] = []
    handle = kg.on(Edge, seen.append)
    run_sync(_until(lambda: len(seen) == 1))
    handle.close()
    levels = kg.watch(Edge)
    assert next(levels).rows == [Edge(x=1, y=1)]
    levels.close()  # type: ignore[attr-defined]
    run_sync(_until(lambda: engine.subscribes().count(".unsubscribe") == 3))
    il.close()
    run_sync(server.stop())
