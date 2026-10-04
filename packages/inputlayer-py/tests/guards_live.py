"""Live tests for R.any()/~R.any(), kg.program().when() and kg.claim().

Each runs the compiled form against a real engine, so a guard that applies
partially, a write form the engine reads as a rule, or a change in the
engine's count replies or abort behaviour fails here (the count grammar and
the abort form are engine behaviour, not a contract: this file is their
per-build conformance check).

Set INPUTLAYER_TEST_SERVER (and INPUTLAYER_TEST_USER /
INPUTLAYER_TEST_PASSWORD) to enable; ``make python-sdk-live`` starts a
server and runs them. The file is not named ``test_*`` so the unit-test run
does not collect it.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import random
import time
from collections.abc import AsyncIterator
from typing import Any, ClassVar

import pytest
import pytest_asyncio

from inputlayer import (
    Derived,
    From,
    InputLayer,
    KnowledgeGraph,
    PreconditionFailed,
    Relation,
)
from inputlayer.program import parse_write_message

SERVER_URL = os.environ.get("INPUTLAYER_TEST_SERVER", "")
USERNAME = os.environ.get("INPUTLAYER_TEST_USER", "admin")
PASSWORD = os.environ.get("INPUTLAYER_TEST_PASSWORD", "admin")

KG_NAME = "test_guards_py"

pytestmark = pytest.mark.skipif(not SERVER_URL, reason="INPUTLAYER_TEST_SERVER not set")


# The README hero's declarations.
class Shipment(Relation):
    order: str
    shipment: str


class Eta(Relation):
    shipment: str
    due: str


class Promised(Relation):
    order: str
    due: str


class ToolPolicy(Relation):
    tool: str
    mode: str


class KillSwitch(Relation):
    tool: str


class Attempt(Relation):
    order: str
    tool: str
    attempt: str


class CheckNeeded(Derived):
    order: str
    shipment: str
    rules: ClassVar[list] = []


CheckNeeded.rules = [
    From(Shipment, Eta, Promised, ToolPolicy)
    .where(
        lambda s, e, p, t: (
            (e.shipment == s.shipment)
            & (p.order == s.order)
            & (e.due > p.due)
            & (t.tool == "carrier_check")
            & (t.mode == "auto")
            & ~KillSwitch.any(tool=t.tool)
        )
    )
    .select(order=Shipment.order, shipment=Shipment.shipment)
]


# Write-gate shapes of the design's lab (T-A1 to T-A4, R3-*).
class VoiceSession(Relation):
    session: str
    status: str


class UtteranceCursor(Relation):
    session: str
    last: int


class Goal(Relation):
    session: str
    goal: str
    kind: str


class AttemptDone(Relation):
    attempt: str
    status: str


class CarrierNote(Relation):
    shipment: str
    reason: str
    src_rev: int


class PackVersion(Relation):
    name: str
    version: str


class LateOrder(Relation):
    order: str


class Late(Derived):
    order: str
    rules: ClassVar[list] = []


class LateV2(Derived):
    """The new body of ``late`` a deploy ships."""

    __relation_name__ = "late"
    order: str
    rules: ClassVar[list] = []


Late.rules = [From(Shipment).select(order=Shipment.order)]
LateV2.rules = [
    From(Shipment, Eta).where(lambda s, e: e.shipment == s.shipment).select(order=Shipment.order)
]


class RaceAttempt(Relation):
    work: str
    attempt: str


class PropA(Relation):
    k: int


class PropB(Relation):
    k: int
    v: str


class PropFlag(Relation):
    f: str


class GhostProbe(Relation):
    id: int
    on: bool
    tag: str


ALL = (
    Shipment, Eta, Promised, ToolPolicy, KillSwitch, Attempt,
    VoiceSession, UtteranceCursor, Goal, AttemptDone, CarrierNote, PackVersion,
    RaceAttempt, PropA, PropB, PropFlag, GhostProbe,
)  # fmt: skip


@pytest_asyncio.fixture
async def il() -> AsyncIterator[InputLayer]:
    client = InputLayer(SERVER_URL, username=USERNAME, password=PASSWORD)
    await client.connect()
    yield client
    await client.close()


@pytest_asyncio.fixture
async def kg(il: InputLayer) -> AsyncIterator[KnowledgeGraph]:
    with contextlib.suppress(Exception):  # not there yet
        await il.drop_knowledge_graph(KG_NAME)
    graph = il.knowledge_graph(KG_NAME)
    await graph.define(*ALL)
    yield graph
    with contextlib.suppress(Exception):
        await il.drop_knowledge_graph(KG_NAME)


class _Others:
    """More connections to the test graph; API-key auth, since the engine
    throttles bursts of password logins."""

    def __init__(self, il: InputLayer) -> None:
        self._il = il
        self._key: str | None = None
        self.clients: list[InputLayer] = []

    async def kg(self) -> KnowledgeGraph:
        if self._key is None:
            self._key = await self._il.create_api_key(f"test-guards-py-{time.time_ns()}")
        other = InputLayer(SERVER_URL, api_key=self._key)
        await other.connect()
        self.clients.append(other)
        return other.knowledge_graph(KG_NAME)


@pytest_asyncio.fixture
async def others(il: InputLayer) -> AsyncIterator[_Others]:
    o = _Others(il)
    yield o
    for c in o.clients:
        with contextlib.suppress(Exception):
            await c.close()


async def rows(kg: KnowledgeGraph, iql: str) -> list[list[Any]]:
    return (await kg.execute(iql)).rows


async def assert_no_guard_leftovers(kg: KnowledgeGraph) -> None:
    """The SDK's guard relations hold nothing once a program is done."""
    for q in ("?il_txn(T)", "?il_txn_pending(T)", "?il_assert(V)", "?il_txn_const_s(K)"):
        assert await rows(kg, q) == [], q


# ── Conformance: the count-reply grammar ─────────────────────────────


async def test_count_reply_grammar(kg: KnowledgeGraph) -> None:
    replies = await rows(
        kg,
        "\n".join(
            [
                '+flag_grammar("a")',
                '-il_ghost(0), +flag_grammar("b") <- flag_grammar("a")',
                '-flag_grammar(X) <- flag_grammar(X), X = "a"',
                '-flag_grammar("b")',
            ]
        ),
    )
    counts = [parse_write_message(str(r[0])) for r in replies]
    assert [(c.inserted, c.deleted) for c in counts] == [(1, 0), (1, 0), (0, 1), (0, 1)]


# ── The hero ─────────────────────────────────────────────────────────


async def test_hero_claim_once_per_need_cancel_on_kill_switch_reclaim_when_lifted(
    kg: KnowledgeGraph,
) -> None:
    await kg.define_rules(CheckNeeded)
    await kg.insert(Shipment(order="ORD-4821", shipment="S-77"))
    await kg.insert(Promised(order="ORD-4821", due="2026-10-08"))
    await kg.insert(
        [ToolPolicy(tool="carrier_check", mode="auto"), ToolPolicy(tool="refund", mode="confirm")]
    )
    await kg.insert(Eta(shipment="S-77", due="2026-10-10"))
    assert (await kg.query(CheckNeeded)).rows == [["ORD-4821", "S-77"]]

    def claim_for(attempt: str) -> Any:
        return kg.claim(
            Attempt(order="ORD-4821", tool="carrier_check", attempt=attempt),
            when=[CheckNeeded.any(order="ORD-4821")],
            unless=Attempt.any(order="ORD-4821", tool="carrier_check"),
        )

    b1 = Attempt(order="ORD-4821", tool="carrier_check", attempt="b-1")
    first = await claim_for("b-1")
    assert (first.won, first.holder) == (True, b1)
    second = await claim_for("b-2")
    assert (second.won, second.holder) == (False, b1)

    # The kill switch retracts the need; the loop cancels and frees the attempt.
    await kg.insert(KillSwitch(tool="carrier_check"))
    assert (await kg.query(CheckNeeded)).rows == []
    assert (await kg.retract(b1)).count == 1
    third = await claim_for("b-3")
    assert (third.won, third.holder) == (False, None)

    # Lifted: the need comes back and a new claim wins.
    await kg.retract(KillSwitch, tool="carrier_check")
    assert (await claim_for("b-4")).won
    assert await rows(kg, "?attempt(O, T, A)") == [["ORD-4821", "carrier_check", "b-4"]]


async def test_read_path_constant_only_negation_does_not_outlive_the_query(
    kg: KnowledgeGraph,
) -> None:
    await kg.insert(
        [ToolPolicy(tool="carrier_check", mode="auto"), ToolPolicy(tool="refund", mode="confirm")]
    )

    async def policies() -> list[list[Any]]:
        rs = await kg.query(
            ToolPolicy,
            where=lambda t: ~KillSwitch.any(tool="carrier_check"),
            order_by=ToolPolicy.tool.asc(),
        )
        return rs.rows

    assert await policies() == [["carrier_check", "auto"], ["refund", "confirm"]]
    await kg.insert(KillSwitch(tool="carrier_check"))
    assert await policies() == []
    await kg.retract(KillSwitch, tool="carrier_check")
    assert await rows(kg, "?il_const_s(K)") == []
    assert await rows(kg, ".session") == [["No session data defined."]]


# ── Guards ───────────────────────────────────────────────────────────


async def test_guard_cursor_advance_applies_whole(kg: KnowledgeGraph, others: _Others) -> None:
    """A write gate that advances the cursor it guards on (T-A1 to T-A4)."""
    await kg.insert(VoiceSession(session="s-42", status="open"))
    await kg.insert(UtteranceCursor(session="s-42", last=6))

    def utterance(k: KnowledgeGraph, goal: str, at: int) -> Any:
        return (
            k.program()
            .insert(Goal(session="s-42", goal=goal, kind="reschedule"))
            .retract(UtteranceCursor, session="s-42")
            .insert(UtteranceCursor(session="s-42", last=at))
            .when(
                VoiceSession.any(session="s-42", status="open"),
                UtteranceCursor.any(session="s-42"),
                UtteranceCursor.last < at,
            )
            .commit(strict=False)
        )

    r = await utterance(kg, "g-7", 7)
    assert (r.applied, r.inserted, r.deleted) == (True, 2, 1)
    assert await rows(kg, "?utterance_cursor(S, L)") == [["s-42", 7]]

    # Replaying the same utterance applies nothing.
    replay = await utterance(kg, "g-7b", 7)
    assert (replay.applied, replay.inserted, replay.deleted) == (False, 0, 0)
    assert await rows(kg, "?goal(S, G, K)") == [["s-42", "g-7", "reschedule"]]

    # Two connections race the next utterance: exactly one applies.
    other = await others.kg()
    await other.execute("?goal(S, G, K)")  # open its connection before the race
    race = await asyncio.gather(utterance(kg, "g-8a", 8), utterance(other, "g-8b", 8))
    assert sum(x.applied for x in race) == 1
    assert await rows(kg, "?utterance_cursor(S, L)") == [["s-42", 8]]
    assert len(await rows(kg, "?goal(S, G, K)")) == 2
    await assert_no_guard_leftovers(kg)


async def test_guard_first_terminal_outcome(kg: KnowledgeGraph) -> None:
    """A negation-only guard binds through a staged il_txn_const row (R3-3c)."""

    def outcome(status: str) -> Any:
        return (
            kg.program()
            .insert(AttemptDone(attempt="att-y", status=status))
            .when(~AttemptDone.any(attempt="att-y"))
            .commit(strict=False)
        )

    assert (await outcome("ok")).applied
    assert not (await outcome("failed")).applied
    assert await rows(kg, "?attempt_done(A, S)") == [["att-y", "ok"]]
    await assert_no_guard_leftovers(kg)


async def test_guard_note_replacement(kg: KnowledgeGraph) -> None:
    """A program that deletes the note its guard reads applies whole."""
    await kg.insert(CarrierNote(shipment="S-77", reason="unknown", src_rev=2100))
    r = await (
        kg.program()
        .retract(CarrierNote, shipment="S-77")
        .insert(CarrierNote(shipment="S-77", reason="weather_delay", src_rev=2201))
        .when(CarrierNote.any(shipment="S-77"), CarrierNote.src_rev < 2201)
        .commit()
    )
    assert (r.applied, r.inserted, r.deleted) == (True, 1, 1)
    assert await rows(kg, "?carrier_note(S, R, V)") == [["S-77", "weather_delay", 2201]]
    await assert_no_guard_leftovers(kg)


async def test_guarded_writes_keep_a_stored_row_equal_to_a_typed_placeholder(
    kg: KnowledgeGraph,
) -> None:
    await kg.insert(GhostProbe(id=-1, on=False, tag=""))
    r = await (
        kg.program().insert(GhostProbe(id=1, on=True, tag="a")).when(GhostProbe.any(id=-1)).commit()
    )
    assert (r.applied, r.inserted, r.deleted) == (True, 1, 0)
    c = await kg.claim(GhostProbe(id=2, on=True, tag="b"), when=GhostProbe.any(id=-1), key=["id"])
    assert c.won
    got = sorted(map(tuple, await rows(kg, "?ghost_probe(I, O, T)")))
    assert got == [(-1, False, ""), (1, True, "a"), (2, True, "b")]
    await assert_no_guard_leftovers(kg)


async def test_strict_guard_raises_precondition_failed_from_error_index(
    kg: KnowledgeGraph,
) -> None:
    p = (
        kg.program()
        .insert(AttemptDone(attempt="att-z", status="ok"))
        .when(AttemptDone.any(attempt="never-there"))
    )
    with pytest.raises(PreconditionFailed):
        await p.commit()
    assert await rows(kg, '?attempt_done("att-z", S)') == []
    await assert_no_guard_leftovers(kg)


def _deploy(kg: KnowledgeGraph, expected: str) -> Any:
    return (
        kg.program()
        .clear_rule("late")
        .define_rules(LateV2)
        .define(LateOrder)
        .retract(PackVersion, name="delivery")
        .insert(PackVersion(name="delivery", version="v3"))
        .when(PackVersion.any(name="delivery", version=expected))
        .commit()
    )


async def test_stale_deploy_is_refused_including_rules(kg: KnowledgeGraph) -> None:
    await kg.insert(PackVersion(name="delivery", version="v2"))
    await kg.define_rules(Late)
    before = await kg.rule_definition("late")

    with pytest.raises(PreconditionFailed):
        await _deploy(kg, "v1")
    assert await kg.rule_definition("late") == before
    assert await rows(kg, "?pack_version(N, V)") == [["delivery", "v2"]]
    assert "late_order" not in {r.name for r in await kg.relations()}
    await assert_no_guard_leftovers(kg)


async def test_deploy_applies_whole_when_guard_holds(kg: KnowledgeGraph) -> None:
    await kg.insert(PackVersion(name="delivery", version="v2"))
    await kg.define_rules(Late)
    before = await kg.rule_definition("late")

    r = await _deploy(kg, "v2")
    assert (r.applied, r.inserted, r.deleted) == (True, 1, 1)
    after = await kg.rule_definition("late")
    assert after != before
    assert "eta(" in "\n".join(after)
    assert await rows(kg, "?pack_version(N, V)") == [["delivery", "v3"]]
    await assert_no_guard_leftovers(kg)


# ── Claims ───────────────────────────────────────────────────────────


async def test_claim_twenty_way_race(kg: KnowledgeGraph, others: _Others) -> None:
    """One winner; the nineteen others see its row (G3)."""
    kgs = [kg]
    while len(kgs) < 20:
        kgs.append(await others.kg())
    # Bind each handle to the graph before the race, so the claims go out
    # together. One at a time: each handle opens its own socket, and the
    # engine caps unauthenticated sockets per address (ws_max_preauth_per_ip).
    for k in kgs:
        await k.execute("?race_attempt(W, A)")
    claims = await asyncio.gather(
        *(
            k.claim(RaceAttempt(work="w-1", attempt=f"a-{i}"), key=["work"])
            for i, k in enumerate(kgs)
        )
    )
    winners = [c for c in claims if c.won]
    assert len(winners) == 1
    assert all(c.holder == winners[0].holder for c in claims)
    assert len(await rows(kg, '?race_attempt("w-1", A)')) == 1
    await assert_no_guard_leftovers(kg)


# ── Property: all or nothing ─────────────────────────────────────────


async def test_guarded_program_is_all_or_nothing(others: _Others, kg: KnowledgeGraph) -> None:
    """Random guarded programs apply whole or not at all."""
    # A connection of its own: the engine limits each connection's message rate.
    k = await others.kg()
    rng = random.Random(20261004)

    async def state() -> tuple[list[int], list[str]]:
        a = sorted(r[0] for r in await rows(k, "?prop_a(K)"))
        b = sorted(f"{r[0]}:{r[1]}" for r in await rows(k, "?prop_b(K, V)"))
        return a, b

    for round_ in range(25):
        await asyncio.sleep(0.1)  # stay under the per-connection message rate limit
        flag_on = rng.randrange(2) == 0
        if flag_on:
            await k.insert(PropFlag(f="on"))
        else:
            await k.retract(PropFlag, f="on")
        strict = rng.randrange(2) == 0
        p = k.program()
        before = await state()
        expect_a = set(before[0])
        expect_b = dict(kv.split(":") for kv in before[1])
        for _ in range(1 + rng.randrange(5)):
            key = rng.randrange(6)
            op = rng.randrange(4)
            if op == 0:
                p.insert(PropA(k=key))
                expect_a.add(key)
            elif op == 1:
                p.retract(PropA, k=key)
                expect_a.discard(key)
            elif op == 2:
                p.retract(PropB, k=key)
                p.insert(PropB(k=key, v=f"r{round_}"))
                expect_b[str(key)] = f"r{round_}"
            else:
                p.retract(PropB, k=key)
                expect_b.pop(str(key), None)
        p.when(PropFlag.any(f="on"))
        if not flag_on and strict:
            with pytest.raises(PreconditionFailed):
                await p.commit(strict=strict)
        else:
            assert (await p.commit(strict=strict)).applied == flag_on
        after = await state()
        if flag_on:
            assert after == (sorted(expect_a), sorted(f"{k_}:{v}" for k_, v in expect_b.items()))
        else:
            assert after == before
    await assert_no_guard_leftovers(k)
