"""Live tests of ``kg.subscribe()``, ``kg.watch()``, ``kg.on()``,
``kg.subscribe_group()`` and ``kg.read()`` against a real engine.

Snapshot plus deltas must be exact over seeded random histories: the
differential oracle's universe (``tests/differential_oracle/generate.rs``,
ported with the same SplitMix64, rules and queries), with a fresh query as
the recompute reference at every checkpoint. Then projection multiplicity,
streamed snapshots and deltas, a ``seq`` gap and a reset, a slow consumer, a
reconnect, an ACL revoke, and the refusals. Then reads and subscription
groups: one revision per answer under concurrent writers (every group event
and every read sees two relations a writer changes together as equal),
streamed snapshots and group deltas, a gap and a reset, a reconnect, and the
refusals. The cases mirror the JS SDK's
``tests/subscriptions.integration.test.ts``.

Set INPUTLAYER_TEST_SERVER (and INPUTLAYER_TEST_USER /
INPUTLAYER_TEST_PASSWORD) to enable; ``make python-sdk-live`` starts a
server and runs them. INPUTLAYER_ORACLE_SEEDS scales the random histories
(default 6). The file is not named ``test_*`` so the unit-test run does not
collect it.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from inputlayer import (
    Change,
    GroupChange,
    GroupSubscription,
    InputLayer,
    KnowledgeGraph,
    QueryError,
    Relation,
    Row,
    Subscription,
    SubscriptionRejected,
    count,
)
from inputlayer._protocol import deserialize_message
from inputlayer.subscription import read as read_shapes

SERVER_URL = os.environ.get("INPUTLAYER_TEST_SERVER", "")
USERNAME = os.environ.get("INPUTLAYER_TEST_USER", "admin")
PASSWORD = os.environ.get("INPUTLAYER_TEST_PASSWORD", "admin")

PREFIX = "test_subscriptions_py"
SEEDS = int(os.environ.get("INPUTLAYER_ORACLE_SEEDS", "6"))
HISTORY_LENGTH = 30

pytestmark = pytest.mark.skipif(not SERVER_URL, reason="INPUTLAYER_TEST_SERVER not set")


def _client(username: str = USERNAME, password: str = PASSWORD) -> InputLayer:
    return InputLayer(SERVER_URL, username=username, password=password, reconnect_delay=0.05)


@contextlib.asynccontextmanager
async def _session() -> AsyncIterator[tuple[InputLayer, Callable[[str], Any]]]:
    """A connected admin client and a maker of fresh graphs, dropped afterwards."""
    il = _client()
    await il.connect()
    graphs: list[str] = []

    async def fresh(name: str) -> KnowledgeGraph:
        full = f"{PREFIX}_{name}"
        with contextlib.suppress(Exception):
            await il.drop_knowledge_graph(full)
        graphs.append(full)
        return il.knowledge_graph(full)

    try:
        yield il, fresh
    finally:
        for name in graphs:
            with contextlib.suppress(Exception):
                await il.drop_knowledge_graph(name)
        await il.close()


async def _until(condition: Callable[[], Any], timeout: float = 10.0) -> None:
    async def poll() -> None:
        while not condition():
            await asyncio.sleep(0.02)

    await asyncio.wait_for(poll(), timeout)


def _values(row: Any) -> tuple[Any, ...]:
    if isinstance(row, Row):
        return row.values_tuple()
    return tuple(getattr(row, name) for name in type(row).model_fields)


class Consumer:
    """Applies a subscription's events to a set, as a consumer would, checking
    each event against the set it applies to: an insert is new, a retract is
    held, revisions never go back. A failed check ends it with ``error``."""

    def __init__(self, sub: Subscription[Any]) -> None:
        self.sub = sub
        self.rows: dict[tuple[Any, ...], Any] = {}
        self.events: list[Change[Any]] = []
        self.revision = -1
        self.verified = False
        self.error: BaseException | None = None
        self._task = asyncio.ensure_future(self._run())

    async def _run(self) -> None:
        try:
            while True:
                try:
                    change = await self.sub.__anext__()
                except StopAsyncIteration:
                    return
                self.apply(change)
        except Exception as e:
            self.error = e

    def apply(self, change: Change[Any]) -> None:
        self.events.append(change)
        assert change.revision >= self.revision, f"revision went back: {change}"
        self.revision = change.revision
        self.verified = change.verified
        if change.kind == "snapshot":
            assert not self.rows, "a snapshot after rows were held"
        for row in change.retracted:
            key = _values(row)
            assert key in self.rows, f"retracted {key}, not held"
            del self.rows[key]
        for row in change.inserted:
            key = _values(row)
            assert key not in self.rows, f"inserted {key}, already held"
            self.rows[key] = row

    def values(self) -> list[tuple[Any, ...]]:
        return sorted(self.rows)

    def kinds(self) -> list[str]:
        return [f"unverified:{e.reason}" if e.kind == "unverified" else e.kind for e in self.events]

    async def close(self) -> None:
        await self.sub.close()
        with contextlib.suppress(Exception):
            await self._task


async def _converges(
    kg: KnowledgeGraph,
    consumer: Consumer,
    query: str,
    project: Callable[[list[Any]], list[Any]] = lambda row: row,
    timeout: float = 10.0,
) -> None:
    """Wait until the consumer holds exactly the engine's current answer to
    *query*, projected by *project*."""
    expected: list[tuple[Any, ...]] = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        rows = (await kg.execute(query)).rows
        expected = sorted({tuple(project(row)) for row in rows})
        if consumer.verified and consumer.values() == expected:
            return
        if loop.time() > deadline or consumer.error is not None:
            break
        await asyncio.sleep(0.02)
    assert consumer.error is None, f"{query}: {consumer.error!r}"
    assert consumer.values() == expected, f"{query} after {', '.join(consumer.kinds())}"


def _inject(kg: KnowledgeGraph, frame: dict[str, Any]) -> None:
    """Feed a frame to the connection's reader, as if the server sent it."""
    kg._conn._route(deserialize_message(json.dumps(frame)))


def _generation(kg: KnowledgeGraph, sub: Subscription[Any]) -> int:
    route = kg._conn._routes[sub.id]
    assert route.generation is not None
    return route.generation


# ── The differential oracle's random histories ───────────────────────

_MASK = (1 << 64) - 1


class _Rng:
    """SplitMix64, as ``tests/differential_oracle/generate.rs``, so a seed means
    the same history."""

    def __init__(self, seed: int) -> None:
        self.state = seed

    def next(self) -> int:
        self.state = (self.state + 0x9E3779B97F4A7C15) & _MASK
        z = self.state
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _MASK
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _MASK
        return z ^ (z >> 31)

    def below(self, n: int) -> int:
        return self.next() % n

    def pick(self, items: list[Any]) -> Any:
        return items[self.below(len(items))]


NODES = 5

RULES: list[tuple[str, list[list[str]]]] = [
    (
        "reach",
        [
            ["+reach(X, Y) <- edge(X, Y)", "+reach(X, Z) <- reach(X, Y), edge(Y, Z)"],
            ["+reach(X, Y) <- edge(X, Y)", "+reach(X, Z) <- edge(X, Y), reach(Y, Z)"],
            ["+reach(X, Y) <- edge(X, Y), X < Y"],
        ],
    ),
    ("two_hop", [["+two_hop(X, Z) <- edge(X, Y), edge(Y, Z)"]]),
    ("linked", [["+linked(X, Y) <- edge(X, Y)", "+linked(X, Y) <- edge(Y, X)"]]),
    (
        "open",
        [["+open(X, Y) <- reach(X, Y), !blocked(Y)"], ["+open(X, Y) <- edge(X, Y), !blocked(X)"]],
    ),
    ("degree", [["+degree(X, count<Y>) <- edge(X, Y)"], ["+degree(X, max<Y>) <- edge(X, Y)"]]),
    ("reach_count", [["+reach_count(X, count<Y>) <- reach(X, Y)"]]),
    ("weight_sum", [["+weight_sum(sum<Y>) <- edge(_, Y)"]]),
]

QUERIES = [
    "?edge(X, Y)",
    "?blocked(X)",
    "?reach(X, Y)",
    "?reach(0, Y)",
    "?two_hop(X, Y)",
    "?linked(X, Y)",
    "?open(X, Y)",
    "?degree(X, N)",
    "?reach_count(X, N)",
    "?weight_sum(S)",
]


def _history(seed: int, length: int) -> list[tuple[str, str]]:
    """``generate::history(seed, length)``, less restarts (a test cannot restart
    its server): ``("execute", program)`` and ``("checkpoint", name)`` steps."""
    rng = _Rng(seed)
    edges: set[tuple[int, int]] = set()
    blocked: set[int] = set()
    rules: set[str] = set()
    steps: list[tuple[str, str]] = []

    def node() -> int:
        return rng.below(NODES)

    for i in range(length):
        r = rng.below(20)
        if r <= 6:
            added = [(node(), node()) for _ in range(rng.below(3) + 1)]
            edges.update(added)
            steps.append(("execute", f"+edge[{', '.join(f'({a}, {b})' for a, b in added)}]"))
        elif r <= 11:
            existing = sorted(edges)
            a, b = (node(), node()) if not existing or rng.below(5) == 0 else rng.pick(existing)
            edges.discard((a, b))
            steps.append(("execute", f"-edge({a}, {b})"))
        elif r == 12:
            n = node()
            blocked.add(n)
            steps.append(("execute", f"+blocked({n})"))
        elif r == 13:
            n = min(blocked) if blocked else node()
            blocked.discard(n)
            steps.append(("execute", f"-blocked({n})"))
        elif r <= 17:
            name, variants = rng.pick(RULES)
            if name in rules:
                steps.append(("execute", f".rule drop {name}"))
            rules.add(name)
            steps.extend(("execute", clause) for clause in rng.pick(variants))
        elif r == 18:
            defined = sorted(rules)
            if defined:
                name = rng.pick(defined)
                rules.discard(name)
                steps.append(("execute", f".rule drop {name}"))
        # 19: a restart in the oracle; nothing here.
        if i % 3 == 2:
            steps.append(("checkpoint", f"c{i // 3}"))
    return steps


def test_history_matches_the_oracle_generator() -> None:
    # SplitMix64's published test vector, then the oracle's first steps for seed 1.
    rng = _Rng(1234567)
    assert [rng.next(), rng.next()] == [6457827717110365317, 3203168211198807973]
    assert _history(1, HISTORY_LENGTH)[:5] == [
        ("execute", "+edge[(0, 0), (1, 3)]"),
        ("execute", "+edge[(0, 0)]"),
        ("execute", "+linked(X, Y) <- edge(X, Y)"),
        ("execute", "+linked(X, Y) <- edge(Y, X)"),
        ("checkpoint", "c0"),
    ]


class Edge(Relation):
    x: int
    y: int


class Reach(Relation):
    x: int
    y: int


class E(Relation):
    a: int
    b: int


# ── Tests ────────────────────────────────────────────────────────────


async def test_snapshot_plus_deltas_equals_a_fresh_query_over_random_histories() -> None:
    async with _session() as (_, fresh):
        for seed in range(1, SEEDS + 1):
            kg = await fresh(f"oracle_{seed}")
            await kg.execute("+edge(0, 1)\n+blocked(9)\n-edge(0, 1)\n-blocked(9)")
            consumers = [Consumer(kg.subscribe(iql=q)) for q in QUERIES]
            # Projections through the query builder: rows supported several times.
            projected = [
                (Consumer(kg.subscribe(Edge.x)), "?edge(X, Y)", lambda r: [r[0]]),
                (Consumer(kg.subscribe(Reach.y)), "?reach(X, Y)", lambda r: [r[1]]),
            ]
            for i, (kind, step) in enumerate(_history(seed, HISTORY_LENGTH)):
                if kind == "execute":
                    await kg.execute(step)
                    continue
                for query, consumer in zip(QUERIES, consumers, strict=True):
                    await _converges(kg, consumer, query)
                    assert consumer.error is None, f"seed {seed} step {i} {query}"
                for consumer, query, project in projected:
                    await _converges(kg, consumer, query, project)
                    assert consumer.error is None, f"seed {seed} step {i} projected {query}"
            for consumer in [*consumers, *(c for c, _, _ in projected)]:
                assert [k for k in consumer.kinds() if k.startswith("unverified")] == []
                await consumer.close()


async def test_projection_two_supporting_tuples() -> None:
    async with _session() as (_, fresh):
        kg = await fresh("projection")

        class A(Relation):
            x: int
            y: int

        await kg.define(A)
        consumer = Consumer(kg.subscribe(A.x))
        await _until(lambda: consumer.events)
        await kg.insert([A(x=1, y=1), A(x=1, y=2)])
        await _until(lambda: len(consumer.rows) == 1)
        await kg.delete(A(x=1, y=1))
        # A later write proves the delete's delta (if any) arrived first.
        await kg.insert(A(x=2, y=1))
        await _until(lambda: len(consumer.rows) == 2)
        assert consumer.values() == [(1,), (2,)]
        await kg.delete(A(x=1, y=2))
        await _until(lambda: len(consumer.rows) == 1)
        assert consumer.values() == [(2,)]
        assert [r for e in consumer.events for r in e.retracted] == [Row(["x"], [1])]
        assert consumer.error is None
        await consumer.close()


async def test_streamed_snapshot_and_streamed_deltas_are_assembled() -> None:
    async with _session() as (_, fresh):
        kg = await fresh("streamed")
        body = "x" * 400
        # A program is at most 1 MiB: insert in parts, then let a rule move all
        # the rows at once, so the snapshot and each delta are over 1 MiB.
        for start in range(0, 4000, 1000):
            rows = ", ".join(f'({i}, "{body}")' for i in range(start, start + 1000))
            await kg.execute(f"+doc[{rows}]")
        await kg.execute("+big(I, B) <- doc(I, B)")
        frames: list[str] = []
        conn = kg._conn
        route = conn._route

        def recording(frame: Any) -> None:
            frames.append(type(frame).__name__)
            route(frame)

        conn._route = recording  # type: ignore[method-assign]
        consumer = Consumer(kg.subscribe(iql="?big(I, B)"))
        await _until(lambda: len(consumer.rows) == 4000)
        await kg.execute(".rule drop big")
        await _until(lambda: not consumer.rows)
        await kg.execute("+big(I, B) <- doc(I, B)")
        await _until(lambda: len(consumer.rows) == 4000)
        assert consumer.kinds() == ["snapshot", "delta", "delta"]
        assert "ResultStartResponse" in frames
        assert frames.count("SubscriptionDeltaStartResponse") == 2
        await _converges(kg, consumer, "?big(I, B)")
        await consumer.close()


async def test_a_seq_gap_and_a_reset_end_in_unverified_then_an_exact_resync() -> None:
    async with _session() as (_, fresh):
        kg = await fresh("gap")
        await kg.define(E)
        await kg.insert(E(a=1, b=1))
        sub = kg.subscribe(E)
        consumer = Consumer(sub)
        await _until(lambda: len(consumer.rows) == 1)

        _inject(
            kg,
            {
                "type": "subscription_delta",
                "subscription": sub.id,
                "generation": _generation(kg, sub),
                "knowledge_graph": kg.name,
                "seq": 7,
                "revision": 10**9,
                "columns": ["a", "b"],
                "inserted": [[9, 9]],
                "retracted": [],
            },
        )
        await kg.insert(E(a=2, b=2))
        await _converges(kg, consumer, "?e(A, B)")
        assert consumer.kinds() == ["snapshot", "unverified:seq_gap", "resync"]
        assert consumer.events[2].inserted == [E(a=2, b=2)]

        # A real reset removes the subscription on the server first.
        await kg.execute(f".unsubscribe {sub.id}")
        _inject(
            kg,
            {
                "type": "subscription_reset",
                "subscription": sub.id,
                "generation": _generation(kg, sub),
                "message": "Delta 4 has a row over the message limit. The subscription was "
                "removed; subscribe again.",
            },
        )
        await kg.delete(E(a=1, b=1))
        await _converges(kg, consumer, "?e(A, B)")
        assert consumer.kinds()[3:] == ["unverified:subscription_reset", "resync"]
        assert consumer.events[4].retracted == [E(a=1, b=1)]
        assert sub.stats.resubscribes == 2
        await consumer.close()


async def test_a_slow_consumer_gets_unverified_then_a_resync_once_it_has_read() -> None:
    async with _session() as (_, fresh):
        kg = await fresh("slow")
        await kg.define(E)
        sub = kg.subscribe(E, queue=2)
        first = await asyncio.wait_for(sub.__anext__(), 10)
        assert first.kind == "snapshot"
        for i in range(6):
            await kg.insert(E(a=i, b=i))
        await kg.delete(E(a=0, b=0))
        # Not reading: the deltas pile up past the queue.
        await asyncio.sleep(0.5)
        consumer = Consumer(sub)
        consumer.apply(first)
        await _converges(kg, consumer, "?e(A, B)")
        kinds = consumer.kinds()
        assert "unverified:slow_consumer" in kinds
        assert kinds[-1] == "resync"
        await consumer.close()


async def test_reconnect_gives_unverified_at_once_then_only_what_changed() -> None:
    async with _session() as (_, fresh):
        writer = await fresh("reconnect")
        await writer.define(E)
        await writer.insert([E(a=1, b=1), E(a=2, b=2)])
        listener = _client()
        try:
            kg = listener.knowledge_graph(writer.name)
            consumer = Consumer(kg.subscribe(E))
            await _until(lambda: len(consumer.rows) == 2)
            reconnected = asyncio.Event()
            listener.events.on("reconnected", callback=lambda _: reconnected.set())
            assert kg._conn._ws is not None
            kg._conn._ws.transport.abort()
            await _until(lambda: consumer.kinds()[-1:] == ["unverified:connection_lost"])
            await writer.delete(E(a=1, b=1))
            await writer.insert(E(a=3, b=3))
            await asyncio.wait_for(reconnected.wait(), 10)
            await _converges(writer, consumer, "?e(A, B)")
            assert consumer.kinds() == ["snapshot", "unverified:connection_lost", "resync"]
            resync = consumer.events[2]
            assert resync.inserted == [E(a=3, b=3)]
            assert resync.retracted == [E(a=1, b=1)]
            await consumer.close()
        finally:
            await listener.close()


async def test_an_acl_revoke_ends_the_subscription_with_access_denied() -> None:
    async with _session() as (il, fresh):
        kg = await fresh("acl")
        await kg.define(E)
        await kg.insert(E(a=1, b=1))
        user = f"{PREFIX}_reader"
        with contextlib.suppress(Exception):
            await il.drop_user(user)
        await il.create_user(user, "reader-pw-1", "viewer")
        await kg.grant_access(user, "viewer")
        # Lazy: the reader may read only this graph, so it never opens another.
        reader = _client(user, "reader-pw-1")
        try:
            consumer = Consumer(reader.knowledge_graph(kg.name, create=False).subscribe(E))
            await _until(lambda: len(consumer.rows) == 1)
            await kg.revoke_access(user)
            await kg.insert(E(a=2, b=2))
            await _until(lambda: consumer.error is not None)
            assert consumer.kinds() == ["snapshot", "unverified:subscription_error"]
            assert isinstance(consumer.error, SubscriptionRejected)
            assert consumer.error.reason == "access_denied"
        finally:
            await reader.close()
            with contextlib.suppress(Exception):
                await il.drop_user(user)


async def test_refused_before_or_at_the_engine() -> None:
    async with _session() as (_, fresh):
        kg = await fresh("refused")
        await kg.define(E)

        class Mine(Relation):
            a: int

        # A session rule: subscriptions would never see it.
        await kg.execute("mine(A) <- e(A, _)")
        cases: list[tuple[Callable[[], Subscription[Any]], str]] = [
            (lambda: kg.subscribe(E, limit=1), "limit_offset"),
            (lambda: kg.subscribe(E, where=lambda e: (e.a == 1) | (e.a == 2)), "or_branches"),
            (lambda: kg.subscribe(E.a, count(E.b)), "session_view"),
            (lambda: kg.subscribe(Mine), "session_view"),
            (lambda: kg.subscribe(iql="?mine(A)"), "session_view"),
            (lambda: kg.subscribe(iql="?e(A, B), limit(1)"), "limit_offset"),
        ]
        for make, reason in cases:
            with pytest.raises(SubscriptionRejected) as err:
                sub = make()
                await asyncio.wait_for(sub.__anext__(), 10)
            assert err.value.reason == reason, f"{reason}: {err.value}"


async def test_watch_yields_the_whole_result_and_on_calls_back() -> None:
    async with _session() as (_, fresh):
        kg = await fresh("watch")
        await kg.define(E)
        await kg.insert(E(a=1, b=1))
        seen: list[Change[Any]] = []
        handle = kg.on(E, seen.append)
        levels = kg.watch(E)
        first = await asyncio.wait_for(levels.__anext__(), 10)
        assert (first.rows, first.verified) == ([E(a=1, b=1)], True)
        await kg.insert(E(a=2, b=2))
        second = await asyncio.wait_for(levels.__anext__(), 10)
        assert sorted(second.rows, key=lambda e: e.a) == [E(a=1, b=1), E(a=2, b=2)]
        assert second.revision > first.revision
        await _until(lambda: len(seen) == 2)
        assert [c.kind for c in seen] == ["snapshot", "delta"]
        assert seen[1].inserted == [E(a=2, b=2)]
        await levels.aclose()  # type: ignore[attr-defined]
        await handle.close()


# ── Reads and subscription groups ────────────────────────────────────


class GroupConsumer:
    """Applies a group's events per member, as ``Consumer`` does for one
    subscription, and runs ``check`` on the held members after every verified
    event. A failed check ends it with ``error``."""

    def __init__(
        self,
        sub: GroupSubscription,
        check: Callable[[dict[str, list[tuple[Any, ...]]]], None] | None = None,
    ) -> None:
        self.sub = sub
        self.check = check
        self.rows: dict[str, dict[tuple[Any, ...], Any]] = {name: {} for name in sub.queries}
        self.events: list[GroupChange] = []
        self.revision = -1
        self.verified = False
        self.checked = 0
        self.error: BaseException | None = None
        self._task = asyncio.ensure_future(self._run())

    async def _run(self) -> None:
        try:
            while True:
                try:
                    change = await self.sub.__anext__()
                except StopAsyncIteration:
                    return
                self.apply(change)
        except Exception as e:
            self.error = e

    def apply(self, change: GroupChange) -> None:
        self.events.append(change)
        assert change.revision >= self.revision, f"revision went back: {change}"
        self.revision = change.revision
        self.verified = change.verified
        assert list(change.members) == list(self.rows), f"members: {list(change.members)}"
        for name, member in change.members.items():
            assert member.unchanged == (not member.inserted and not member.retracted)
            held = self.rows[name]
            if change.kind == "snapshot":
                assert not held, "a snapshot after rows were held"
            for row in member.retracted:
                key = _values(row)
                assert key in held, f"{name}: retracted {key}, not held"
                del held[key]
            for row in member.inserted:
                key = _values(row)
                assert key not in held, f"{name}: inserted {key}, already held"
                held[key] = row
        if change.verified and self.check is not None:
            self.check(self.values())
            self.checked += 1

    def values(self) -> dict[str, list[tuple[Any, ...]]]:
        return {name: sorted(rows) for name, rows in self.rows.items()}

    def kinds(self) -> list[str]:
        return [f"unverified:{e.reason}" if e.kind == "unverified" else e.kind for e in self.events]

    async def close(self) -> None:
        await self.sub.close()
        with contextlib.suppress(Exception):
            await self._task


async def _group_converges(
    kg: KnowledgeGraph, consumer: GroupConsumer, timeout: float = 10.0
) -> None:
    """Wait until every member holds the engine's current answer to its query
    (read with the members' own shapes, so projections compare as projected)."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    expected: dict[str, list[tuple[Any, ...]]] = {}
    while True:
        snap = await read_shapes(kg._conn, consumer.sub._members)
        expected = {name: sorted(_values(r) for r in rows) for name, rows in snap.results.items()}
        if consumer.verified and consumer.values() == expected:
            return
        if loop.time() > deadline or consumer.error is not None:
            break
        await asyncio.sleep(0.02)
    assert consumer.error is None, repr(consumer.error)
    assert consumer.values() == expected, ", ".join(consumer.kinds())


async def test_read_answers_several_queries_at_one_revision() -> None:
    async with _session() as (_, fresh):
        kg = await fresh("read")
        await kg.define(E)
        await kg.insert([E(a=1, b=1), E(a=1, b=2), E(a=2, b=3)])
        await kg.execute("+f(1, 10)")
        before = await kg.read({"e": E, "f": "?f(A, B)", "a": E.a})
        assert before.results["e"] == [E(a=1, b=1), E(a=1, b=2), E(a=2, b=3)]
        assert before.results["f"] == [Row(["A", "B"], [1, 10])]
        assert before.results["a"] == [Row(["a"], [1]), Row(["a"], [2])]
        await kg.insert(E(a=3, b=4))
        after = await kg.read({"page": {"select": E, "order_by": E.b.desc(), "limit": 2}})
        assert after.revision > before.revision
        assert after.results["page"] == [E(a=3, b=4), E(a=2, b=3)]
        assert after.truncated == ["page"]
        # Session data is invisible to a read, as to a subscription: refused.
        await kg.execute("mine(A) <- e(A, _)")
        with pytest.raises(SubscriptionRejected) as session:
            await kg.read({"e": E, "mine": "?mine(A)"})
        assert session.value.reason == "session_view"
        with pytest.raises(QueryError) as err:
            await kg.read({"e": E, "bad": "?f("})
        assert err.value.code == "validation" and "Query 'bad'" in err.value.message
        with pytest.raises(SubscriptionRejected) as refused:
            await kg.read({"e": {"select": [E.a], "limit": 1}})
        assert refused.value.reason == "limit_offset"


async def test_group_snapshot_then_deltas_with_unchanged_members() -> None:
    async with _session() as (_, fresh):
        kg = await fresh("group")
        await kg.define(E)
        await kg.insert(E(a=1, b=1))
        consumer = GroupConsumer(kg.subscribe_group({"e": E, "a": E.a, "f": "?f(X, Y)"}))
        await _until(lambda: consumer.events)
        snap = consumer.events[0]
        assert snap.kind == "snapshot" and snap.members["f"].unchanged
        assert snap.members["a"].inserted == [Row(["a"], [1])]
        # A second tuple behind a = 1: e changes, a does not.
        await kg.insert(E(a=1, b=2))
        await _until(lambda: len(consumer.events) == 2)
        delta = consumer.events[1]
        assert delta.kind == "delta" and delta.revision > snap.revision
        assert delta.members["e"].inserted == [E(a=1, b=2)]
        assert delta.members["a"].unchanged and delta.members["f"].unchanged
        # One program changing two members: one event at one revision.
        await kg.execute("+f(1, 2)\n-e(1, 1)")
        await _until(lambda: len(consumer.events) == 3)
        both = consumer.events[2]
        assert both.members["f"].inserted == [Row(["X", "Y"], [1, 2])]
        assert both.members["e"].retracted == [E(a=1, b=1)]
        assert both.members["a"].unchanged
        assert both.seq == delta.seq + 1
        await _group_converges(kg, consumer)
        assert consumer.error is None
        await consumer.close()


async def test_groups_and_reads_are_atomic_under_concurrent_writes() -> None:
    """Writers commit programs that insert or delete one key in two relations
    together; a rule derives the keys in both. Every verified group event and
    every read must hold the three members equal."""
    async with _session() as (_, fresh):
        kg = await fresh("atomic")
        await kg.execute("+left(0)\n+right(0)\n+both(K) <- left(K), right(K)")
        queries = {"left": "?left(K)", "right": "?right(K)", "both": "?both(K)"}

        def equal(values: dict[str, list[tuple[Any, ...]]]) -> None:
            assert values["left"] == values["right"] == values["both"], values

        subscriber = _client()
        reader = _client()
        writers = [_client() for _ in range(3)]
        try:
            group = GroupConsumer(
                subscriber.knowledge_graph(kg.name).subscribe_group(queries), equal
            )
            await _until(lambda: group.events)
            done = asyncio.Event()
            reads = 0

            async def read_loop() -> None:
                nonlocal reads
                rkg = reader.knowledge_graph(kg.name)
                while not done.is_set():
                    snap = await rkg.read(queries)
                    equal({n: sorted(_values(r) for r in rows)
                           for n, rows in snap.results.items()})
                    reads += 1

            async def write_loop(client: InputLayer, seed: int) -> None:
                wkg = client.knowledge_graph(kg.name)
                rng = _Rng(seed)
                for _ in range(150):
                    key = rng.below(25)
                    if rng.below(2):
                        await wkg.execute(f"+left({key})\n+right({key})")
                    else:
                        await wkg.execute(f"-left({key})\n-right({key})")

            reading = asyncio.ensure_future(read_loop())
            await asyncio.gather(*(write_loop(w, i + 1) for i, w in enumerate(writers)))
            done.set()
            await reading
            await _group_converges(kg, group)
            final = await kg.read(queries)
            values = {n: sorted(_values(r) for r in rows) for n, rows in final.results.items()}
            equal(values)
            assert group.values() == values
            assert group.error is None
            assert [k for k in group.kinds() if k.startswith("unverified")] == []
            # The checks ran: many group events and reads, under the writers.
            assert group.checked > 20 and reads > 20, (group.checked, reads)
            await group.close()
        finally:
            for client in [subscriber, reader, *writers]:
                await client.close()


async def test_streamed_read_group_snapshot_and_group_deltas_are_assembled() -> None:
    async with _session() as (_, fresh):
        kg = await fresh("group_streamed")
        body = "x" * 400
        for start in range(0, 4000, 1000):
            rows = ", ".join(f'({i}, "{body}")' for i in range(start, start + 1000))
            await kg.execute(f"+doc[{rows}]")
        await kg.execute("+big(I, B) <- doc(I, B)")
        frames: list[str] = []
        conn = kg._conn
        route = conn._route

        def recording(frame: Any) -> None:
            frames.append(type(frame).__name__)
            route(frame)

        conn._route = recording  # type: ignore[method-assign]
        snap = await kg.read({"big": "?big(I, B)", "doc": "?doc(I, B)"})
        assert len(snap.results["big"]) == len(snap.results["doc"]) == 4000
        assert "SnapshotStartResponse" in frames
        consumer = GroupConsumer(kg.subscribe_group({"big": "?big(I, B)", "small": "?s(X)"}))
        await _until(lambda: len(consumer.rows["big"]) == 4000)
        await kg.execute(".rule drop big")
        await _until(lambda: not consumer.rows["big"])
        await kg.execute("+big(I, B) <- doc(I, B)\n+s(1)")
        await _until(lambda: len(consumer.rows["big"]) == 4000 and consumer.rows["small"])
        assert consumer.kinds() == ["snapshot", "delta", "delta"]
        assert frames.count("SnapshotStartResponse") == 2
        assert frames.count("SubscriptionGroupDeltaStartResponse") == 2
        await _group_converges(kg, consumer)
        await consumer.close()


async def test_a_group_seq_gap_and_reset_end_in_unverified_then_an_exact_resync() -> None:
    async with _session() as (_, fresh):
        kg = await fresh("group_gap")
        await kg.define(E)
        await kg.insert(E(a=1, b=1))
        sub = kg.subscribe_group({"e": E, "f": "?f(X)"})
        consumer = GroupConsumer(sub)
        await _until(lambda: consumer.events)
        _inject(
            kg,
            {
                "type": "subscription_group_delta",
                "subscription": sub.id,
                "generation": kg._conn._routes[sub.id].generation,
                "knowledge_graph": kg.name,
                "seq": 7,
                "revision": 10**9,
                "members": [
                    {"name": "e", "unchanged": False, "columns": ["a", "b"],
                     "inserted": [[9, 9]], "retracted": []},
                    {"name": "f", "unchanged": True, "columns": ["X"],
                     "inserted": [], "retracted": []},
                ],
            },
        )
        await kg.execute("+e(2, 2)\n+f(5)")
        await _group_converges(kg, consumer)
        assert consumer.kinds() == ["snapshot", "unverified:seq_gap", "resync"]
        resync = consumer.events[2]
        assert resync.members["e"].inserted == [E(a=2, b=2)]
        assert resync.members["f"].inserted == [Row(["X"], [5])]

        await kg.execute(f".unsubscribe {sub.id}")
        _inject(
            kg,
            {
                "type": "subscription_reset",
                "subscription": sub.id,
                "generation": kg._conn._routes[sub.id].generation,
                "message": "Delta 4 has a row over the message limit.",
            },
        )
        await kg.delete(E(a=1, b=1))
        await _group_converges(kg, consumer)
        assert consumer.kinds()[3:] == ["unverified:subscription_reset", "resync"]
        assert consumer.events[4].members["e"].retracted == [E(a=1, b=1)]
        assert consumer.events[4].members["f"].unchanged
        assert sub.stats.resubscribes == 2
        await consumer.close()


async def test_a_group_reconnect_gives_unverified_then_only_what_changed() -> None:
    async with _session() as (_, fresh):
        writer = await fresh("group_reconnect")
        await writer.define(E)
        await writer.insert([E(a=1, b=1), E(a=2, b=2)])
        listener = _client()
        try:
            kg = listener.knowledge_graph(writer.name)
            consumer = GroupConsumer(kg.subscribe_group({"e": E, "f": "?f(X)"}))
            await _until(lambda: len(consumer.rows["e"]) == 2)
            reconnected = asyncio.Event()
            listener.events.on("reconnected", callback=lambda _: reconnected.set())
            assert kg._conn._ws is not None
            kg._conn._ws.transport.abort()
            await _until(lambda: consumer.kinds()[-1:] == ["unverified:connection_lost"])
            await writer.delete(E(a=1, b=1))
            await asyncio.wait_for(reconnected.wait(), 10)
            await _group_converges(writer, consumer)
            assert consumer.kinds() == ["snapshot", "unverified:connection_lost", "resync"]
            resync = consumer.events[2]
            assert resync.members["e"].retracted == [E(a=1, b=1)]
            assert resync.members["e"].inserted == [] and resync.members["f"].unchanged
            await consumer.close()
        finally:
            await listener.close()


async def test_groups_refused_at_the_engine() -> None:
    async with _session() as (_, fresh):
        kg = await fresh("group_refused")
        await kg.define(E)
        await kg.execute("mine(A) <- e(A, _)")
        cases: list[tuple[dict[str, Any], str]] = [
            ({"e": E, "page": "?e(A, B), limit(1)"}, "limit_offset"),
            ({"e": E, "bad": "?e("}, "rejected"),
            ({"e": E, "mine": "?mine(A)"}, "session_view"),
        ]
        for members, reason in cases:
            with pytest.raises(SubscriptionRejected) as err:
                await asyncio.wait_for(kg.subscribe_group(members).__anext__(), 10)
            assert err.value.reason == reason, f"{reason}: {err.value}"
