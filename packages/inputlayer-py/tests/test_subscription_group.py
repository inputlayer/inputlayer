"""Snapshot reads and subscription groups against a scripted engine on a real socket.

A fake engine answers ``read`` and ``subscribe`` frames with snapshots (one
frame or streamed), and ``.unsubscribe`` and ``.session``; each test pushes the
group frames it needs and reads what the SDK makes of them.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from inputlayer import (
    Cancelled,
    CompileError,
    DeadlineExceeded,
    GroupChange,
    GroupSubscription,
    InputLayer,
    InputLayerSync,
    InternalError,
    QueryError,
    Relation,
    Row,
    SubscriptionRejected,
)
from inputlayer.aggregations import count
from inputlayer.knowledge_graph import KnowledgeGraph

from ._mock_server import MockServer, Peer, result


class Order(Relation):
    s: int
    o: int


class Eta(Relation):
    o: int
    t: int


COLUMNS = {"orders": ["S", "O"], "eta": ["O", "T"], "s": ["S", "O"], "mine": ["X"]}


async def _until(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    async def poll() -> None:
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


async def _next(sub: GroupSubscription, timeout: float = 5.0) -> GroupChange:
    return await asyncio.wait_for(sub.__anext__(), timeout)


def _snapshot(
    rid: str,
    names: list[str],
    rows: dict[str, list[list[Any]]],
    revision: int,
    *,
    stream: bool,
    subscribed: dict[str, Any] | None = None,
    truncated: frozenset[str] = frozenset(),
) -> list[dict[str, Any]]:
    """A snapshot reply: one frame, or streamed with one row per chunk."""
    if not stream:
        return [
            {
                "type": "snapshot",
                "id": rid,
                "knowledge_graph": "default",
                "revision": revision,
                "results": [
                    {
                        "name": name,
                        "columns": COLUMNS[name],
                        "rows": rows[name],
                        "total_count": len(rows[name]),
                        "truncated": name in truncated,
                    }
                    for name in names
                ],
                "execution_time_ms": 1,
                "subscribed": subscribed,
            }
        ]
    frames: list[dict[str, Any]] = [
        {
            "type": "snapshot_start",
            "id": rid,
            "knowledge_graph": "default",
            "revision": revision,
            "results": [
                {
                    "name": name,
                    "columns": COLUMNS[name],
                    "row_count": len(rows[name]),
                    "total_count": len(rows[name]),
                    "truncated": False,
                }
                for name in names
            ],
            "execution_time_ms": 1,
            "subscribed": subscribed,
        }
    ]
    index = 0
    for i, name in enumerate(names):
        for row in rows[name]:
            frames.append(
                {"type": "snapshot_chunk", "id": rid, "result": i, "chunk_index": index,
                 "rows": [row]}
            )
            index += 1
    frames.append({"type": "snapshot_end", "id": rid, "chunk_count": index})
    return frames


class Engine:
    """Answers what reads and groups send; the test pushes the rest."""

    def __init__(self, **rows: list[list[Any]]) -> None:
        self.rows: dict[str, list[list[Any]]] = {name: list(r) for name, r in rows.items()}
        self.revision = 10
        self.generation = 0
        self.frames: list[dict[str, Any]] = []
        self.refuse: str | None = None
        self.session_rules: list[str] = []
        self.stream = False
        self.truncated: frozenset[str] = frozenset()
        self.peer: Peer | None = None
        # Replies the next reads get instead of a snapshot, in order.
        self.read_replies: list[Callable[[str], list[dict[str, Any]]] | None] = []
        # Frames sent before the reply to the next subscribe.
        self.before_reply: list[dict[str, Any]] = []
        # Result names the next subscribe replies with instead of the request's.
        self.wrong_names: list[str] | None = None

    async def handler(self, peer: Peer) -> None:
        await peer.authenticate()
        self.peer = peer
        try:
            while True:
                frame = await peer.recv(timeout=60)
                self.frames.append(frame)
                if frame["type"] == "ping":
                    await peer.send({"type": "pong", "id": frame.get("id")})
                elif frame["type"] == "execute":
                    await self._execute(peer, frame)
                elif frame["type"] == "read":
                    await self._read(peer, frame)
                elif frame["type"] == "subscribe":
                    await self._subscribe(peer, frame)
        except Exception:
            return

    async def _execute(self, peer: Peer, frame: dict[str, Any]) -> None:
        program = frame["program"]
        if program == ".session":
            lines = [f"Session rules ({len(self.session_rules)}):"] + [
                f"  {i + 1}. {rule}" for i, rule in enumerate(self.session_rules)
            ]
            reply = result(
                [[line] for line in lines] if self.session_rules
                else [["No session data defined."]],
                ["message"],
            )
        else:
            reply = result([])
        await peer.send({**reply, "id": frame["id"]})

    async def _read(self, peer: Peer, frame: dict[str, Any]) -> None:
        names = [q["name"] for q in frame["queries"]]
        reply = self.read_replies.pop(0) if self.read_replies else None
        if reply is not None:
            for out in reply(frame["id"]):
                await peer.send(out)
            return
        for out in _snapshot(
            frame["id"], names, self.rows, self.revision, stream=self.stream,
            truncated=self.truncated,
        ):
            await peer.send(out)

    async def _subscribe(self, peer: Peer, frame: dict[str, Any]) -> None:
        if self.refuse is not None:
            await peer.send(
                {"type": "error", "id": frame["id"], "code": "validation", "message": self.refuse}
            )
            return
        self.generation += 1
        for early in self.before_reply:
            await peer.send({**early, "generation": self.generation})
        self.before_reply = []
        names = [q["name"] for q in frame["queries"]]
        if self.wrong_names is not None:
            names, self.wrong_names = self.wrong_names, None
        subscribed = {
            "subscription": frame["subscription"],
            "generation": self.generation,
            "revision": self.revision,
        }
        for out in _snapshot(
            frame["id"], names, self.rows, self.revision, stream=self.stream,
            subscribed=subscribed,
        ):
            await peer.send(out)

    def requests(self) -> list[str]:
        """The subscribes and unsubscribes received, in order."""
        out = []
        for frame in self.frames:
            if frame["type"] == "subscribe":
                out.append("subscribe")
            elif frame["type"] == "execute" and frame["program"].startswith(".unsubscribe"):
                out.append(".unsubscribe")
        return out

    def of_type(self, kind: str) -> list[dict[str, Any]]:
        return [frame for frame in self.frames if frame["type"] == kind]

    async def push(self, frame: dict[str, Any]) -> None:
        assert self.peer is not None
        await self.peer.send(frame)

    async def delta(
        self,
        sub: GroupSubscription,
        seq: int,
        members: dict[str, tuple[list[list[Any]], list[list[Any]]]],
        *,
        generation: int | None = None,
    ) -> None:
        """A one-frame group delta: every member of *sub* in order, unchanged
        unless *members* gives its inserted and retracted rows."""
        self.revision += 1
        await self.push(
            {
                "type": "subscription_group_delta",
                "subscription": sub.id,
                "generation": generation if generation is not None else self.generation,
                "knowledge_graph": "default",
                "seq": seq,
                "revision": self.revision,
                "members": [
                    {
                        "name": name,
                        "unchanged": not any(members.get(name, ([], []))),
                        "columns": COLUMNS[name],
                        "inserted": members.get(name, ([], []))[0],
                        "retracted": members.get(name, ([], []))[1],
                    }
                    for name in sub.queries
                ],
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
    return sorted(
        r.values_tuple() if isinstance(r, Row) else tuple(dict(r).values()) for r in rows
    )


def _group(kg: KnowledgeGraph, **kwargs: Any) -> GroupSubscription:
    return kg.subscribe_group({"orders": Order, "eta": "?eta(O, T)"}, **kwargs)


# ── Reads ────────────────────────────────────────────────────────────


async def test_read_answers_every_query_at_one_revision() -> None:
    engine = Engine(orders=[[1, 2], [1, 3]], eta=[[2, 10]], s=[[1, 2], [1, 3]])
    server, il, kg = await _setup(engine)
    snap = await kg.read({"orders": Order, "eta": "?eta(O, T)", "s": Order.s}, timeout=5)
    assert snap.revision == 10 and snap.truncated == []
    assert snap.results["orders"] == [Order(s=1, o=2), Order(s=1, o=3)]
    assert snap.results["eta"] == [Row(["O", "T"], [2, 10])]
    # A projection's row that two engine rows support comes once.
    assert snap.results["s"] == [Row(["s"], [1])]
    [frame] = engine.of_type("read")
    assert [q["name"] for q in frame["queries"]] == ["orders", "eta", "s"]
    assert frame["queries"][0]["query"] == "?order(S, O)"
    assert frame["queries"][1]["query"] == "?eta(O, T)"
    assert 0 < frame["timeout_ms"] <= 5000
    await _teardown(server, il)


async def test_read_keeps_the_engine_order_and_reports_cut_results() -> None:
    engine = Engine(orders=[[1, 3], [1, 2]])
    engine.truncated = frozenset({"orders"})
    server, il, kg = await _setup(engine)
    snap = await kg.read(
        {"orders": {"select": Order, "order_by": Order.o.desc(), "limit": 2}}
    )
    assert snap.results["orders"] == [Order(s=1, o=3), Order(s=1, o=2)]
    assert snap.truncated == ["orders"]
    query = engine.of_type("read")[0]["queries"][0]["query"]
    assert query.startswith("?order(") and ":desc" in query and "limit(2)" in query
    await _teardown(server, il)


async def test_streamed_read_is_assembled_per_result() -> None:
    engine = Engine(orders=[[1, 1], [2, 2], [3, 3]], eta=[])
    engine.stream = True
    server, il, kg = await _setup(engine)
    snap = await kg.read({"orders": Order, "eta": Eta})
    assert snap.results == {"orders": [Order(s=1, o=1), Order(s=2, o=2), Order(s=3, o=3)],
                            "eta": []}
    await _teardown(server, il)


def _start(rid: str, counts: tuple[int, int]) -> dict[str, Any]:
    return _snapshot(
        rid, ["orders", "eta"],
        {"orders": [[0, 0]] * counts[0], "eta": [[0, 0]] * counts[1]}, 10, stream=True,
    )[0]


def _chunk(rid: str, index: int, result_index: int, rows: int = 1) -> dict[str, Any]:
    return {"type": "snapshot_chunk", "id": rid, "result": result_index, "chunk_index": index,
            "rows": [[index, index]] * rows}


def _end(rid: str, chunks: int) -> dict[str, Any]:
    return {"type": "snapshot_end", "id": rid, "chunk_count": chunks}


BROKEN_SNAPSHOTS: dict[str, Callable[[str], list[dict[str, Any]]]] = {
    "missing_chunk": lambda r: [_start(r, (2, 0)), _chunk(r, 0, 0), _chunk(r, 2, 0), _end(r, 2)],
    "short_result": lambda r: [_start(r, (2, 0)), _chunk(r, 0, 0), _end(r, 1)],
    "chunk_count": lambda r: [_start(r, (1, 0)), _chunk(r, 0, 0), _end(r, 2)],
    "result_goes_back": lambda r: [
        _start(r, (1, 1)), _chunk(r, 0, 1), _chunk(r, 1, 0), _end(r, 2)
    ],
    "next_result_too_early": lambda r: [
        _start(r, (2, 1)), _chunk(r, 0, 0), _chunk(r, 1, 1), _chunk(r, 2, 0), _end(r, 3)
    ],
    "too_many_rows": lambda r: [_start(r, (1, 0)), _chunk(r, 0, 0, rows=2), _end(r, 1)],
    "empty_chunk": lambda r: [_start(r, (0, 0)), _chunk(r, 0, 0, rows=0), _end(r, 1)],
    "result_out_of_range": lambda r: [_start(r, (1, 0)), _chunk(r, 0, 2), _end(r, 1)],
    "chunk_without_start": lambda r: [_chunk(r, 0, 0)],
    "end_without_start": lambda r: [_end(r, 0)],
    "second_start": lambda r: [_start(r, (1, 0)), _start(r, (1, 0))],
}


@pytest.mark.parametrize("case", sorted(BROKEN_SNAPSHOTS))
async def test_a_broken_streamed_read_is_an_internal_error_never_a_short_result(
    case: str,
) -> None:
    engine = Engine(orders=[[1, 1]], eta=[])
    engine.read_replies = [BROKEN_SNAPSHOTS[case]]
    server, il, kg = await _setup(engine)
    with pytest.raises(InternalError):
        await kg.read({"orders": Order, "eta": Eta}, timeout=5)
    # The connection still serves the next read.
    assert (await kg.read({"orders": Order, "eta": Eta})).results["orders"] == [Order(s=1, o=1)]
    await _teardown(server, il)


async def test_an_error_before_the_snapshot_end_discards_the_rows() -> None:
    engine = Engine(orders=[[1, 1]], eta=[])
    engine.read_replies = [
        lambda r: [
            _start(r, (2, 0)),
            _chunk(r, 0, 0),
            {"type": "error", "id": r, "code": "internal", "message": "Snapshot too large"},
        ]
    ]
    server, il, kg = await _setup(engine)
    with pytest.raises(QueryError) as err:
        await kg.read({"orders": Order, "eta": Eta})
    assert (err.value.code, err.value.message) == ("internal", "Snapshot too large")
    await _teardown(server, il)


async def test_a_read_fails_as_a_whole_naming_the_query() -> None:
    engine = Engine()
    engine.read_replies = [
        lambda r: [{"type": "error", "id": r, "code": "validation",
                    "message": "Query 'eta': Invalid atom: eta("}]
    ]
    server, il, kg = await _setup(engine)
    with pytest.raises(QueryError) as err:
        await kg.read({"orders": Order, "eta": "?eta("})
    assert type(err.value) is QueryError and err.value.code == "validation"
    assert err.value.message == "Query 'eta': Invalid atom: eta("
    assert err.value.query == "orders: ?order(S, O)\neta: ?eta("
    await _teardown(server, il)


@pytest.mark.parametrize(
    ("code", "error"), [("deadline_exceeded", DeadlineExceeded), ("cancelled", Cancelled)]
)
async def test_a_stopped_read_is_typed_as_execute_is(code: str, error: type) -> None:
    engine = Engine()
    engine.read_replies = [lambda r: [{"type": "error", "id": r, "code": code, "message": code}]]
    server, il, kg = await _setup(engine)
    with pytest.raises(error):
        await kg.read({"orders": Order})
    await _teardown(server, il)


async def test_a_silent_read_is_cancelled_past_its_deadline() -> None:
    engine = Engine()
    engine.read_replies = [lambda r: []]
    server, il, kg = await _setup(engine)
    await il.connect()
    kg._conn._deadline_grace = 0.1
    with pytest.raises(DeadlineExceeded):
        await kg.read({"orders": Order}, timeout=0.2)
    [read_frame] = engine.of_type("read")
    assert 0 < read_frame["timeout_ms"] <= 200
    await _until(lambda: bool(engine.of_type("cancel")))
    assert engine.of_type("cancel")[0]["target"] == read_frame["id"]
    await _teardown(server, il)


async def test_abandoning_a_read_cancels_it_on_the_server() -> None:
    engine = Engine()
    engine.read_replies = [lambda r: []]
    server, il, kg = await _setup(engine)
    task = asyncio.ensure_future(kg.read({"orders": Order}, timeout=30))
    await _until(lambda: bool(engine.of_type("read")))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await _until(lambda: bool(engine.of_type("cancel")))
    assert engine.of_type("cancel")[0]["target"] == engine.of_type("read")[0]["id"]
    await _teardown(server, il)


async def test_a_reply_naming_other_results_is_an_internal_error() -> None:
    engine = Engine(orders=[], eta=[])
    engine.read_replies = [
        lambda r: _snapshot(r, ["eta", "orders"], engine.rows, 10, stream=False)
    ]
    server, il, kg = await _setup(engine)
    with pytest.raises(InternalError):
        await kg.read({"orders": Order, "eta": Eta})
    await _teardown(server, il)


async def test_a_result_frame_answering_a_read_is_an_internal_error() -> None:
    engine = Engine()
    engine.read_replies = [lambda r: [{**result([[1, 2]], ["S", "O"]), "id": r}]]
    server, il, kg = await _setup(engine)
    with pytest.raises(InternalError):
        await kg.read({"orders": Order})
    await _teardown(server, il)


async def test_reads_refused_before_anything_is_sent() -> None:
    kg = InputLayer("ws://127.0.0.1:1/ws", username="a", password="b").knowledge_graph("g")
    cases: list[tuple[dict[str, Any], str]] = [
        ({"o": {"select": Order, "where": lambda o: (o.s == 1) | (o.s == 2)}}, "or_branches"),
        ({"o": (Order.s, count(Order.o))}, "session_view"),
        ({"o": {"select": [Order.s], "limit": 1}}, "limit_offset"),
        ({"o": "?order(S, O)\n?eta(O, T)"}, "rejected"),
        ({"o": "order(S, O)"}, "rejected"),
        ({"o": {"select": Order, "offset": 2}}, "limit_offset"),
        ({"o": {"iql": "?order(S, O)", "limit": 1}}, "limit_offset"),
        ({"o": {"iql": "?order(S, O)", "offset": 1, "limit": 1}}, "limit_offset"),
        ({"o": {"iql": "?order(S, O)", "order_by": Order.s}}, "limit_offset"),
        ({}, "rejected"),
        ({"": Order}, "rejected"),
    ]
    for queries, reason in cases:
        with pytest.raises(SubscriptionRejected) as err:
            await kg.read(queries)
        assert err.value.reason == reason, queries
        if queries and "" not in queries:
            assert err.value.message.startswith("Query 'o': "), err.value.message
    with pytest.raises(CompileError) as compile_err:
        await kg.read({"o": Order, "p": {"iql": "?order(S, O)", "select": Order}})
    assert str(compile_err.value).startswith("Query 'p': "), str(compile_err.value)
    assert compile_err.value.hint is not None
    # A page of a whole relation is one ? query: allowed.
    shape = kg._target_shape({"select": Order, "limit": 1, "offset": 2}, read=True)
    assert shape.query.startswith("?order(") and "\n" not in shape.query


async def test_a_read_of_a_session_rule_is_refused() -> None:
    engine = Engine(orders=[[1, 2]], mine=[])
    engine.session_rules = ["mine(X) <- order(X, _)"]
    server, il, kg = await _setup(engine)
    with pytest.raises(SubscriptionRejected) as err:
        await kg.read({"orders": Order, "mine": "?mine(X)"})
    assert (err.value.reason, err.value.query) == ("session_view", "?mine(X)")
    assert err.value.message.startswith("Query 'mine': 'mine' is a session rule")
    # Without the session rule the same read answers.
    engine.session_rules = []
    snap = await kg.read({"orders": Order, "mine": "?mine(X)"})
    assert snap.results == {"orders": [Order(s=1, o=2)], "mine": []}
    await _teardown(server, il)


def test_sync_read() -> None:
    from inputlayer._sync import run_sync

    engine = Engine(orders=[[1, 2]], eta=[])
    server = MockServer(engine.handler)
    run_sync(server.__aenter__())
    il = InputLayerSync(server.url, username="admin", password="pw")
    snap = il.knowledge_graph("default").read({"orders": Order, "eta": Eta}, timeout=5)
    assert (snap.revision, snap.results) == (10, {"orders": [Order(s=1, o=2)], "eta": []})
    il.close()
    run_sync(server.stop())


# ── Group snapshot and deltas ────────────────────────────────────────


async def test_group_snapshot_then_deltas_mark_unchanged_members() -> None:
    engine = Engine(orders=[[1, 2]], eta=[])
    server, il, kg = await _setup(engine)
    sub = _group(kg)
    assert sub.queries == {"orders": "?order(S, O)", "eta": "?eta(O, T)"}
    snap = await _next(sub)
    assert (snap.kind, snap.verified, snap.revision, snap.seq) == ("snapshot", True, 10, 0)
    assert list(snap.members) == ["orders", "eta"]
    assert snap.members["orders"].inserted == [Order(s=1, o=2)]
    assert not snap.members["orders"].unchanged
    assert (snap.members["eta"].inserted, snap.members["eta"].unchanged) == ([], True)
    [frame] = engine.of_type("subscribe")
    assert frame["subscription"] == sub.id
    assert frame["queries"] == [
        {"name": "orders", "query": "?order(S, O)"},
        {"name": "eta", "query": "?eta(O, T)"},
    ]

    await engine.delta(sub, 1, {"orders": ([[5, 6]], [[1, 2]])})
    delta = await _next(sub)
    assert (delta.kind, delta.seq, delta.revision, delta.verified) == ("delta", 1, 11, True)
    orders, eta = delta.members["orders"], delta.members["eta"]
    assert (orders.inserted, orders.retracted, orders.unchanged) == (
        [Order(s=5, o=6)], [Order(s=1, o=2)], False
    )
    assert (eta.inserted, eta.retracted, eta.unchanged) == ([], [], True)

    await engine.delta(sub, 2, {"eta": ([[6, 30]], [])})
    second = await _next(sub)
    assert second.members["orders"].unchanged
    assert second.members["eta"].inserted == [Row(["O", "T"], [6, 30])]
    await sub.close()
    assert engine.requests() == ["subscribe", ".unsubscribe"]
    assert engine.of_type("execute")[-1]["program"] == f".unsubscribe {sub.id}"
    await _teardown(server, il)


async def test_streamed_group_snapshot_and_streamed_group_delta_are_assembled() -> None:
    engine = Engine(orders=[[1, 1], [2, 2]], eta=[[1, 10]])
    engine.stream = True
    server, il, kg = await _setup(engine)
    sub = _group(kg)
    snap = await _next(sub)
    assert _values(snap.members["orders"].inserted) == [(1, 1), (2, 2)]
    assert _values(snap.members["eta"].inserted) == [(1, 10)]
    base = {"subscription": sub.id, "generation": 1, "seq": 1}
    await engine.push({
        **base, "type": "subscription_group_delta_start", "knowledge_graph": "default",
        "revision": 12,
        "members": [
            {"name": "orders", "unchanged": False, "columns": ["S", "O"],
             "inserted_count": 2, "retracted_count": 1},
            {"name": "eta", "unchanged": False, "columns": ["O", "T"],
             "inserted_count": 0, "retracted_count": 1},
        ],
    })
    await engine.push({**base, "type": "subscription_group_delta_chunk", "chunk_index": 0,
                       "member": 0, "inserted": [[3, 3]], "retracted": []})
    await engine.push({**base, "type": "subscription_group_delta_chunk", "chunk_index": 1,
                       "member": 0, "inserted": [[4, 4]], "retracted": [[1, 1]]})
    await engine.push({**base, "type": "subscription_group_delta_chunk", "chunk_index": 2,
                       "member": 1, "inserted": [], "retracted": [[1, 10]]})
    await engine.push({**base, "type": "subscription_group_delta_end", "chunk_count": 3})
    delta = await _next(sub)
    assert (delta.kind, delta.seq, delta.revision) == ("delta", 1, 12)
    assert _values(delta.members["orders"].inserted) == [(3, 3), (4, 4)]
    assert _values(delta.members["orders"].retracted) == [(1, 1)]
    assert _values(delta.members["eta"].retracted) == [(1, 10)]
    await sub.close()
    await _teardown(server, il)


async def test_a_push_before_the_snapshot_reply_is_applied_after_it() -> None:
    engine = Engine(orders=[[1, 1]], eta=[])
    server, il, kg = await _setup(engine)
    sub = _group(kg)
    engine.before_reply = [{
        "type": "subscription_group_delta", "subscription": sub.id, "knowledge_graph": "default",
        "seq": 1, "revision": 11,
        "members": [
            {"name": "orders", "unchanged": False, "columns": ["S", "O"],
             "inserted": [[2, 2]], "retracted": []},
            {"name": "eta", "unchanged": True, "columns": ["O", "T"],
             "inserted": [], "retracted": []},
        ],
    }]
    assert (await _next(sub)).kind == "snapshot"
    delta = await _next(sub)
    assert delta.kind == "delta" and delta.members["orders"].inserted == [Order(s=2, o=2)]
    await sub.close()
    await _teardown(server, il)


async def test_projection_is_counted_per_member() -> None:
    engine = Engine(s=[[1, 1], [1, 2]], eta=[])
    server, il, kg = await _setup(engine)
    sub = kg.subscribe_group({"s": Order.s, "eta": "?eta(O, T)"})
    snap = await _next(sub)
    assert snap.members["s"].inserted == [Row(["s"], [1])]
    # (1, 1) leaves, (1, 2) still supports s = 1: s is unchanged, eta is not.
    await engine.delta(sub, 1, {"s": ([], [[1, 1]]), "eta": ([[3, 3]], [])})
    first = await _next(sub)
    assert first.members["s"].unchanged and first.members["s"].retracted == []
    assert first.members["eta"].inserted == [Row(["O", "T"], [3, 3])]
    # Only a support moving: no member changed, nothing is delivered.
    await engine.delta(sub, 2, {"s": ([[1, 5]], [[1, 2]])})
    await engine.delta(sub, 3, {"s": ([], [[1, 5]])})
    third = await _next(sub)
    assert third.seq == 3 and third.members["s"].retracted == [Row(["s"], [1])]
    assert third.members["eta"].unchanged
    await sub.close()
    await _teardown(server, il)


async def test_a_group_delta_applies_to_every_member_or_to_none() -> None:
    engine = Engine(orders=[[1, 1]], eta=[[1, 10]])
    server, il, kg = await _setup(engine)
    sub = _group(kg)
    await _next(sub)
    # orders' insert is fine, eta retracts a row it does not hold.
    await engine.delta(sub, 1, {"orders": ([[2, 2]], []), "eta": ([], [[9, 9]])})
    broken = await _next(sub)
    assert (broken.kind, broken.reason) == ("unverified", "broken_stream")
    resync = await _next(sub)
    # The engine's result did not change, and orders' insert was never held.
    assert resync.kind == "resync"
    assert all(m.unchanged and not m.inserted and not m.retracted
               for m in resync.members.values())
    await sub.close()
    await _teardown(server, il)


# ── Unverified and resync ────────────────────────────────────────────


async def test_seq_gap_unsubscribes_and_resyncs_each_member_exactly() -> None:
    engine = Engine(orders=[[1, 1], [2, 2]], eta=[[1, 10]])
    server, il, kg = await _setup(engine)
    sub = _group(kg)
    await _next(sub)
    engine.rows["orders"] = [[2, 2], [3, 3]]
    await engine.delta(sub, 2, {"orders": ([[9, 9]], [])})
    gap = await _next(sub)
    assert (gap.kind, gap.reason, gap.verified, gap.revision, gap.seq) == (
        "unverified", "seq_gap", False, 10, 0
    )
    assert list(gap.members) == ["orders", "eta"]
    assert all(m.unchanged and not m.inserted and not m.retracted for m in gap.members.values())
    resync = await _next(sub)
    assert (resync.kind, resync.verified, resync.seq) == ("resync", True, 0)
    assert _values(resync.members["orders"].inserted) == [(3, 3)]
    assert _values(resync.members["orders"].retracted) == [(1, 1)]
    assert resync.members["eta"].unchanged
    assert engine.requests() == ["subscribe", ".unsubscribe", "subscribe"]
    assert sub.stats.resubscribes == 1
    # The new generation's deltas count from 1 again.
    await engine.delta(sub, 1, {"eta": ([[4, 40]], [])})
    assert (await _next(sub)).seq == 1
    await sub.close()
    await _teardown(server, il)


async def test_stale_generation_is_dropped_and_counted() -> None:
    engine = Engine(orders=[[1, 1]], eta=[])
    server, il, kg = await _setup(engine)
    sub = _group(kg)
    await _next(sub)
    await engine.delta(sub, 5, {})  # a gap: reopen with generation 2
    assert (await _next(sub)).kind == "unverified"
    assert (await _next(sub)).kind == "resync"
    await engine.delta(sub, 1, {"orders": ([[8, 8]], [])}, generation=1)
    await engine.delta(sub, 1, {"orders": ([[2, 2]], [])})
    delta = await _next(sub)
    assert delta.members["orders"].inserted == [Order(s=2, o=2)]
    assert sub.stats.stale_dropped == 1
    await sub.close()
    await _teardown(server, il)


def _gstart(sub: GroupSubscription, counts: list[tuple[int, int]],
            names: list[str] | None = None) -> dict[str, Any]:
    names = names or list(sub.queries)
    return {
        "type": "subscription_group_delta_start", "subscription": sub.id, "generation": 1,
        "knowledge_graph": "default", "seq": 1, "revision": 11,
        "members": [
            {"name": name, "unchanged": ins == 0 and ret == 0, "columns": COLUMNS[name],
             "inserted_count": ins, "retracted_count": ret}
            for name, (ins, ret) in zip(names, counts, strict=True)
        ],
    }


def _gchunk(sub: GroupSubscription, index: int, member: int, rows: int = 1) -> dict[str, Any]:
    return {
        "type": "subscription_group_delta_chunk", "subscription": sub.id, "generation": 1,
        "seq": 1, "chunk_index": index, "member": member,
        "inserted": [[index + 5, index + 5]] * rows, "retracted": [],
    }


def _gend(sub: GroupSubscription, chunks: int) -> dict[str, Any]:
    return {"type": "subscription_group_delta_end", "subscription": sub.id, "generation": 1,
            "seq": 1, "chunk_count": chunks}


def _gdelta(sub: GroupSubscription, members: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "subscription_group_delta", "subscription": sub.id, "generation": 1,
            "knowledge_graph": "default", "seq": 1, "revision": 11, "members": members}


def _member(name: str, unchanged: bool, inserted: list[list[Any]]) -> dict[str, Any]:
    return {"name": name, "unchanged": unchanged, "columns": COLUMNS[name],
            "inserted": inserted, "retracted": []}


BROKEN_DELTAS: dict[str, Callable[[GroupSubscription], list[dict[str, Any]]]] = {
    "missing_chunk": lambda s: [
        _gstart(s, [(2, 0), (0, 0)]), _gchunk(s, 0, 0), _gchunk(s, 2, 0), _gend(s, 2)
    ],
    "chunk_count": lambda s: [_gstart(s, [(1, 0), (0, 0)]), _gchunk(s, 0, 0), _gend(s, 2)],
    "short_member": lambda s: [_gstart(s, [(2, 0), (0, 0)]), _gchunk(s, 0, 0), _gend(s, 1)],
    "too_many_rows": lambda s: [
        _gstart(s, [(1, 0), (0, 0)]), _gchunk(s, 0, 0, rows=2), _gend(s, 1)
    ],
    "member_goes_back": lambda s: [
        _gstart(s, [(1, 0), (1, 0)]), _gchunk(s, 0, 1), _gchunk(s, 1, 0), _gend(s, 2)
    ],
    "next_member_too_early": lambda s: [
        _gstart(s, [(2, 0), (1, 0)]), _gchunk(s, 0, 0), _gchunk(s, 1, 1), _gend(s, 2)
    ],
    "member_out_of_range": lambda s: [_gstart(s, [(1, 0), (0, 0)]), _gchunk(s, 0, 2)],
    "empty_chunk": lambda s: [_gstart(s, [(0, 0), (0, 0)]), _gchunk(s, 0, 0, rows=0)],
    "chunk_without_start": lambda s: [_gchunk(s, 0, 0)],
    "end_without_start": lambda s: [_gend(s, 0)],
    "delta_inside_a_stream": lambda s: [
        _gstart(s, [(1, 0), (0, 0)]),
        _gdelta(s, [_member("orders", False, [[7, 7]]), _member("eta", True, [])]),
    ],
    "header_names": lambda s: [_gstart(s, [(0, 0), (1, 0)], names=["eta", "orders"])],
    "header_unchanged_lies": lambda s: [
        {**_gstart(s, [(1, 0), (0, 0)]),
         "members": [{**_gstart(s, [(1, 0), (0, 0)])["members"][0], "unchanged": True},
                     _gstart(s, [(1, 0), (0, 0)])["members"][1]]}
    ],
    "names": lambda s: [
        _gdelta(s, [_member("eta", True, []), _member("orders", False, [[7, 7]])])
    ],
    "missing_member": lambda s: [_gdelta(s, [_member("orders", False, [[7, 7]])])],
    "unchanged_lies": lambda s: [
        _gdelta(s, [_member("orders", True, [[7, 7]]), _member("eta", True, [])])
    ],
    "plain_delta": lambda s: [{
        "type": "subscription_delta", "subscription": s.id, "generation": 1,
        "knowledge_graph": "default", "seq": 1, "revision": 11, "columns": ["S", "O"],
        "inserted": [[7, 7]], "retracted": [],
    }],
    "retract_not_held": lambda s: [_gdelta(s, [
        {**_member("orders", False, []), "retracted": [[5, 5]]}, _member("eta", True, [])
    ])],
}


@pytest.mark.parametrize("case", sorted(BROKEN_DELTAS))
async def test_a_broken_group_delta_is_unverified_then_resynced(case: str) -> None:
    engine = Engine(orders=[[1, 1]], eta=[])
    server, il, kg = await _setup(engine)
    sub = _group(kg)
    await _next(sub)
    for frame in BROKEN_DELTAS[case](sub):
        await engine.push(frame)
    broken = await _next(sub)
    assert (broken.kind, broken.reason) == ("unverified", "broken_stream")
    resync = await _next(sub)
    assert resync.kind == "resync"
    assert all(m.unchanged for m in resync.members.values())
    await sub.close()
    await _teardown(server, il)


async def test_reset_resubscribes_without_unsubscribing() -> None:
    engine = Engine(orders=[[1, 1]], eta=[])
    server, il, kg = await _setup(engine)
    sub = _group(kg)
    await _next(sub)
    engine.rows["orders"] = []
    await engine.push({"type": "subscription_reset", "subscription": sub.id, "generation": 1,
                       "message": "Delta 4 has a row over the message limit."})
    reset = await _next(sub)
    assert (reset.kind, reset.reason) == ("unverified", "subscription_reset")
    assert reset.message == "Delta 4 has a row over the message limit."
    resync = await _next(sub)
    assert resync.members["orders"].retracted == [Order(s=1, o=1)]
    assert resync.members["eta"].unchanged
    assert engine.requests() == ["subscribe", "subscribe"]
    await sub.close()
    await _teardown(server, il)


async def test_subscription_error_unsubscribes_then_resubscribes() -> None:
    engine = Engine(orders=[[1, 1]], eta=[])
    server, il, kg = await _setup(engine)
    sub = _group(kg)
    await _next(sub)
    await engine.push({"type": "subscription_error", "subscription": sub.id, "generation": 1,
                       "message": "evaluation failed"})
    error = await _next(sub)
    assert (error.reason, error.message) == ("subscription_error", "evaluation failed")
    assert (await _next(sub)).kind == "resync"
    assert engine.requests() == ["subscribe", ".unsubscribe", "subscribe"]
    await sub.close()
    await _teardown(server, il)


async def test_slow_consumer_gets_unverified_then_a_resync_once_it_has_read() -> None:
    engine = Engine(orders=[[0, 0]], eta=[])
    server, il, kg = await _setup(engine)
    sub = _group(kg, queue=2)
    await _next(sub)
    for seq in range(1, 4):
        await engine.delta(sub, seq, {"orders": ([[seq, seq]], [])})
    await _until(lambda: ".unsubscribe" in engine.requests())
    engine.rows["orders"] = [[0, 0], [1, 1], [2, 2], [3, 3]]
    kinds = []
    for _ in range(4):
        change = await _next(sub)
        kinds.append(change.reason or change.kind)
    assert kinds == ["delta", "delta", "slow_consumer", "resync"]
    assert engine.requests() == ["subscribe", ".unsubscribe", "subscribe"]
    await sub.close()
    await _teardown(server, il)


async def test_reconnect_gives_unverified_at_once_then_a_resync() -> None:
    engine = Engine(orders=[[1, 1], [2, 2]], eta=[])
    server, il, kg = await _setup(engine)
    sub = _group(kg)
    await _next(sub)
    engine.rows["orders"] = [[2, 2], [3, 3]]
    assert engine.peer is not None
    await engine.peer.close()
    lost = await _next(sub)
    assert (lost.kind, lost.reason) == ("unverified", "connection_lost")
    resync = await _next(sub)
    assert resync.members["orders"].inserted == [Order(s=3, o=3)]
    assert resync.members["orders"].retracted == [Order(s=1, o=1)]
    # The old connection took the group with it: no .unsubscribe.
    assert engine.requests() == ["subscribe", "subscribe"]
    await sub.close()
    await _teardown(server, il)


async def test_a_reply_naming_other_results_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("inputlayer.subscription.RESUBSCRIBE_DELAY", 0.01)
    engine = Engine(orders=[[1, 1]], eta=[])
    engine.wrong_names = ["eta", "orders"]
    server, il, kg = await _setup(engine)
    sub = _group(kg)
    snap = await _next(sub)
    assert snap.kind == "snapshot" and snap.members["orders"].inserted == [Order(s=1, o=1)]
    # The first reply registered the group: it is removed before subscribing again.
    assert engine.requests() == ["subscribe", ".unsubscribe", "subscribe"]
    await sub.close()
    await _teardown(server, il)


async def test_leaving_the_loop_unsubscribes() -> None:
    engine = Engine(orders=[[1, 1]], eta=[])
    server, il, kg = await _setup(engine)

    async def first() -> GroupChange:
        async for change in _group(kg):
            return change
        raise AssertionError("no event")

    assert (await first()).kind == "snapshot"
    await _until(lambda: engine.requests() == ["subscribe", ".unsubscribe"])
    await _teardown(server, il)


async def test_async_with_closes_the_group() -> None:
    engine = Engine(orders=[], eta=[])
    server, il, kg = await _setup(engine)
    async with _group(kg) as sub:
        assert (await _next(sub)).kind == "snapshot"
    with pytest.raises(StopAsyncIteration):
        await _next(sub)
    assert engine.requests() == ["subscribe", ".unsubscribe"]
    await _teardown(server, il)


# ── Refusals ─────────────────────────────────────────────────────────


async def test_groups_refused_before_anything_is_sent() -> None:
    kg = InputLayer("ws://127.0.0.1:1/ws", username="a", password="b").knowledge_graph("g")
    cases: list[tuple[dict[str, Any], str]] = [
        ({"o": {"select": Order, "limit": 1}}, "limit_offset"),
        ({"o": {"select": Order, "where": lambda o: (o.s == 1) | (o.s == 2)}}, "or_branches"),
        ({"o": (Order.s, count(Order.o))}, "session_view"),
        ({"o": "order(S, O)"}, "rejected"),
        ({"o": {"iql": "?order(S, O)", "limit": 1}}, "limit_offset"),
        ({}, "rejected"),
        ({"": Order}, "rejected"),
    ]
    for members, reason in cases:
        with pytest.raises(SubscriptionRejected) as err:
            kg.subscribe_group(members)
        assert err.value.reason == reason, members


async def test_a_session_rule_member_is_refused() -> None:
    engine = Engine()
    engine.session_rules = ["mine(X) <- order(X, _)"]
    server, il, kg = await _setup(engine)
    sub = kg.subscribe_group({"orders": Order, "mine": "?mine(X)"})
    with pytest.raises(SubscriptionRejected) as err:
        await _next(sub)
    assert (err.value.reason, err.value.query) == ("session_view", "?mine(X)")
    assert engine.requests() == []
    await _teardown(server, il)


@pytest.mark.parametrize(
    ("message", "reason"),
    [
        ("Subscriptions track the whole result set; remove limit/offset from the query.",
         "limit_offset"),
        ("Query 'eta': Result exceeds max_result_rows (1000)", "result_cap"),
        ("Access denied", "access_denied"),
        ("Subscription 'x' already exists on this connection", "id_taken"),
        ("Subscription limit reached (64 per connection)", "subscription_limit"),
        ("Failed to parse query: Invalid atom: eta(", "rejected"),
    ],
)
async def test_engine_refusals_are_typed(message: str, reason: str) -> None:
    engine = Engine()
    engine.refuse = message
    server, il, kg = await _setup(engine)
    sub = _group(kg)
    with pytest.raises(SubscriptionRejected) as err:
        await _next(sub)
    assert (err.value.reason, err.value.message) == (reason, message)
    assert err.value.query == "orders: ?order(S, O)\neta: ?eta(O, T)"
    with pytest.raises(StopAsyncIteration):
        await _next(sub)
    await _teardown(server, il)


def test_sync_subscribe_group_is_a_blocking_iterator() -> None:
    from inputlayer._sync import run_sync

    engine = Engine(orders=[[1, 1]], eta=[])
    server = MockServer(engine.handler)
    run_sync(server.__aenter__())
    il = InputLayerSync(server.url, username="admin", password="pw")
    kg = il.knowledge_graph("default")
    with kg.subscribe_group({"orders": Order, "eta": "?eta(O, T)"}) as sub:
        assert sub.queries == {"orders": "?order(S, O)", "eta": "?eta(O, T)"}
        assert sub.next().members["orders"].inserted == [Order(s=1, o=1)]
        run_sync(engine.delta(sub._sub, 1, {"eta": ([[1, 10]], [])}))
        change = sub.next()
        assert change.members["eta"].inserted == [Row(["O", "T"], [1, 10])]
        assert change.members["orders"].unchanged
    run_sync(_until(lambda: engine.requests() == ["subscribe", ".unsubscribe"]))
    il.close()
    run_sync(server.stop())
