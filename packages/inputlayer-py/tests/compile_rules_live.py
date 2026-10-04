"""Live pins for the compile rules R-TYPE, R-META, R-IN, R-NEG and R-LIT.

Each test runs an SDK call against a real engine, so a form the engine
rejects or misreads fails here: the shared meta-command fixture
(``packages/conformance/meta-commands.json``) is sent command by command, and
random values are round-tripped through the literal encoder (encode, insert,
read back, compare; ``INPUTLAYER_FUZZ_SEED`` replays a run).

Set INPUTLAYER_TEST_SERVER (and INPUTLAYER_TEST_USER /
INPUTLAYER_TEST_PASSWORD) to enable; ``make python-sdk-live`` starts a
server and runs them. The file is not named ``test_*`` so the unit-test run
does not collect it.
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import random
import struct
import sys
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar

import pytest
import pytest_asyncio

from inputlayer import Derived, From, InputLayer, KnowledgeGraph, Relation, Timestamp, Vector
from inputlayer._ast import Literal, MatchExpr
from inputlayer._literal import I64_MAX, I64_MIN
from inputlayer.aggregations import min_
from inputlayer.exceptions import CompileError, QueryError

SERVER_URL = os.environ.get("INPUTLAYER_TEST_SERVER", "")
USERNAME = os.environ.get("INPUTLAYER_TEST_USER", "admin")
PASSWORD = os.environ.get("INPUTLAYER_TEST_PASSWORD", "admin")

KG_NAME = "test_compile_rules_py"

pytestmark = pytest.mark.skipif(not SERVER_URL, reason="INPUTLAYER_TEST_SERVER not set")

FIXTURE = json.loads(
    (Path(__file__).parents[2] / "conformance" / "meta-commands.json").read_text("utf-8")
)


class Employee(Relation):
    id: int
    name: str
    department: str


class Manager(Relation):
    team: str
    employee_id: int


class ToolPolicy(Relation):
    tool: str
    mode: str


class KillSwitch(Relation):
    tool: str


class Reading(Relation):
    sensor: str
    at: datetime
    seen: Timestamp


class Doc(Relation):
    id: int
    embedding: Vector[3]


class Allowed(Derived):
    tool: str
    rules: ClassVar[list] = []


class Edge(Relation):
    src: int
    dst: int


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
    await graph.define(Employee, Manager, ToolPolicy, KillSwitch)
    await graph.insert(
        [
            Employee(id=1, name="Alice", department="eng"),
            Employee(id=2, name="Bob", department="hr"),
            Employee(id=3, name="Cara", department="eng"),
        ]
    )
    await graph.insert([Manager(team="core", employee_id=1), Manager(team="ops", employee_id=2)])
    await graph.insert(
        [
            ToolPolicy(tool="carrier_check", mode="auto"),
            ToolPolicy(tool="refund", mode="confirm"),
        ]
    )
    yield graph
    with contextlib.suppress(Exception):
        await il.drop_knowledge_graph(KG_NAME)


def _not_killed(tool: str) -> MatchExpr:
    # The negated atom `~KillSwitch.any(tool="...")` compiles to (P1-I adds the surface).
    return MatchExpr("kill_switch", {"tool": Literal(tool)}, negated=True, columns=("tool",))


# ── R-META ────────────────────────────────────────────────────────────


async def test_every_fixture_command_runs_and_rejected_spellings_fail(kg: KnowledgeGraph) -> None:
    from inputlayer import _meta

    for statement in FIXTURE["setup"]:
        await kg.execute(statement)
    for command in FIXTURE["commands"]:
        text = _meta.COMMANDS[command["name"]](*command["args"])
        await kg.execute(text)  # raises on an engine error
    for rejected in FIXTURE["rejected"]:
        with pytest.raises(QueryError):
            await kg.execute(rejected["text"])


async def test_rule_definition_reads_the_clauses(kg: KnowledgeGraph) -> None:
    class Reach(Derived):
        src: int
        dst: int
        rules: ClassVar[list] = []

    Reach.rules = [
        From(Edge).select(src=Edge.src, dst=Edge.dst),
        From(Reach, Edge).where(lambda r, e: r.dst == e.src).select(src=Reach.src, dst=Edge.dst),
    ]
    await kg.define(Edge)
    await kg.define_rules(Reach)
    # `.rule show` (the old spelling) is a query of a rule named `show`: [].
    assert await kg.rule_definition(Reach) == [
        "reach(Src, Dst) <- edge(Src, Dst)",
        "reach(Src, Dst_1) <- reach(Src, Dst), edge(Dst, Dst_1)",
    ]
    assert [(r.name, r.clause_count) for r in await kg.list_rules()] == [("reach", 2)]
    await kg.drop_rule_clause(Reach, 2)
    assert await kg.rule_definition("reach") == ["reach(Src, Dst) <- edge(Src, Dst)"]


async def test_session_rules_list_and_drop_by_clause(kg: KnowledgeGraph) -> None:
    class Hop(Derived):
        a: int
        b: int
        rules: ClassVar[list] = []

    Hop.rules = [
        From(Edge).select(a=Edge.src, b=Edge.dst),
        From(Edge).select(a=Edge.dst, b=Edge.src),
    ]
    await kg.define(Edge)
    await kg.session.define_rules(Hop)
    assert await kg.session.list_rules() == [
        "hop(Src, Dst) <- edge(Src, Dst)",
        "hop(Dst, Src) <- edge(Src, Dst)",
    ]
    await kg.session.drop_rule("hop", index=1)
    assert await kg.session.list_rules() == ["hop(Dst, Src) <- edge(Src, Dst)"]
    await kg.session.clear()
    assert await kg.session.list_rules() == []


# ── R-TYPE ────────────────────────────────────────────────────────────


async def test_datetime_columns_define_insert_and_read_back(kg: KnowledgeGraph) -> None:
    await kg.define(Reading, Doc)
    early = datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)  # 1000 ms: Pydantic reads seconds
    late = datetime(2026, 10, 4, 12, 30, 0, 250000, tzinfo=timezone.utc)
    await kg.insert(
        [
            Reading(sensor="a", at=early, seen=Timestamp(5)),
            Reading(sensor="b", at=late, seen=Timestamp.from_datetime(late)),
        ]
    )
    rows = {r.sensor: r for r in await kg.query(Reading)}
    assert rows["a"].at == early
    assert rows["b"].at == late
    assert rows["b"].seen == 1791117000250
    recent = await kg.query(Reading.sensor, where=lambda r: r.at > datetime(2026, 1, 1))
    assert recent.rows == [["b"]]
    projected = await kg.query(Reading.at, Reading.seen, where=lambda r: r.sensor == "b")
    assert projected.rows == [[late, 1791117000250]]
    assert projected.to_dicts() == [{"at": late, "seen": 1791117000250}]
    assert [r.at for r in projected] == [late]
    whole = await kg.query(Reading, where=lambda r: r.sensor == "a")
    assert whole.rows == [["a", early, 5]]
    assert (await kg.query(min_(Reading.at))).scalar() == early
    await kg.insert(Doc(id=1, embedding=[1.0, 0.5, 0.25]))
    assert (await kg.query(Doc)).rows == [[1, [1.0, 0.5, 0.25]]]


# ── R-IN ──────────────────────────────────────────────────────────────


async def test_in_and_not_in_queries(kg: KnowledgeGraph) -> None:
    managers = await kg.query(Employee.name, where=lambda e: e.id.in_(Manager.employee_id))
    assert sorted(r[0] for r in managers.rows) == ["Alice", "Bob"]
    others = await kg.query(Employee.name, where=lambda e: ~e.id.in_(Manager.employee_id))
    assert others.rows == [["Cara"]]


async def test_in_in_a_rule_and_a_conditional_delete(kg: KnowledgeGraph) -> None:
    class Led(Derived):
        name: str
        rules: ClassVar[list] = []

    Led.rules = [
        From(Employee).where(lambda e: e.id.in_(Manager.employee_id)).select(name=Employee.name)
    ]
    await kg.define_rules(Led)
    assert sorted(r.name for r in await kg.query(Led)) == ["Alice", "Bob"]
    deleted = await kg.delete(Employee, where=lambda e: ~e.id.in_(Manager.employee_id))
    assert deleted is not None
    remaining = await kg.query(Employee.name)
    assert sorted(r[0] for r in remaining.rows) == ["Alice", "Bob"]


# ── R-NEG ─────────────────────────────────────────────────────────────


async def test_ground_negation_binds_a_constant_for_the_request_only(kg: KnowledgeGraph) -> None:
    # T-B2: every policy row while refund is not killed, nothing once it is.
    query = dict(where=lambda p: _not_killed("refund"))
    rows = await kg.query(ToolPolicy, **query)
    assert sorted(r.tool for r in rows) == ["carrier_check", "refund"]
    assert rows.columns == ["tool", "mode"]
    # The constant was a session fact of that one request (T-B3, T-B4).
    assert (await kg.execute(".session")).rows == [["No session data defined."]]
    assert (await kg.execute("?il_const_s(K)")).rows == []
    await kg.insert(KillSwitch(tool="refund"))
    assert (await kg.query(ToolPolicy, **query)).rows == []


async def test_negation_sharing_no_variable_is_refused_before_sending(
    kg: KnowledgeGraph,
) -> None:
    with pytest.raises(CompileError, match="shares no variable"):
        await kg.query(Employee.name, where=lambda e: ~Manager.employee_id.in_(Manager.employee_id))


async def test_view_with_ground_negation(kg: KnowledgeGraph) -> None:
    Allowed.rules = [
        From(ToolPolicy)
        .where(lambda p: (p.mode == "auto") & _not_killed("carrier_check"))
        .select(tool=ToolPolicy.tool)
    ]
    await kg.define_rules(Allowed)
    assert [r.tool for r in await kg.query(Allowed)] == ["carrier_check"]
    await kg.insert(KillSwitch(tool="carrier_check"))
    assert (await kg.query(Allowed)).rows == []
    await kg.delete(KillSwitch(tool="carrier_check"))
    assert [r.tool for r in await kg.query(Allowed)] == ["carrier_check"]
    # The view's constant is a persistent row the program wrote.
    assert (await kg.execute("?il_const_s(K)")).rows == [["carrier_check"]]


async def test_session_view_with_ground_negation(kg: KnowledgeGraph) -> None:
    class Open(Derived):
        tool: str
        rules: ClassVar[list] = []

    Open.rules = [
        From(ToolPolicy).where(lambda p: _not_killed("refund")).select(tool=ToolPolicy.tool)
    ]
    await kg.session.define_rules(Open)
    assert sorted(r.tool for r in await kg.query(Open)) == ["carrier_check", "refund"]
    await kg.insert(KillSwitch(tool="refund"))
    assert (await kg.query(Open)).rows == []
    await kg.session.clear()


async def test_negated_comparison(kg: KnowledgeGraph) -> None:
    rows = await kg.query(Employee.name, where=lambda e: ~(e.department == "eng"))
    assert rows.rows == [["Bob"]]


# ── R-LIT ─────────────────────────────────────────────────────────────


class Lit(Relation):
    id: int
    s: str
    i: int
    f: float
    b: bool
    v: Vector


_ALPHABETS = [
    (0x00, 0x7F),  # ASCII, controls included
    (0x80, 0x24F),  # Latin
    (0x2000, 0x206F),  # punctuation, line/paragraph separators
    (0x4E00, 0x4FFF),  # CJK
    (0xFFF0, 0xFFFF),  # specials
    (0x1F300, 0x1F64F),  # emoji
]
_TRICKY = ['"', "\\", "\n", "\r", "\t", ",", "(", ")", "<-", "?", "//", "/*", "*/", "%", "\\n", "_"]


def _random_string(rng: random.Random) -> str:
    parts = []
    for _ in range(rng.randint(0, 12)):
        if rng.random() < 0.3:
            parts.append(rng.choice(_TRICKY))
        else:
            lo, hi = rng.choice(_ALPHABETS)
            parts.append(chr(rng.randint(lo, hi)))
    return "".join(parts)


def _random_float(rng: random.Random) -> float:
    pick = rng.random()
    if pick < 0.2:
        return rng.choice([0.0, -0.0, 0.1, -1.5, 1e20, 1e-5, 5e-324, 1.7976931348623157e308])
    if pick < 0.6:
        return rng.uniform(-1e6, 1e6)
    while True:  # any finite bit pattern
        value = struct.unpack("<d", rng.getrandbits(64).to_bytes(8, "little"))[0]
        if math.isfinite(value):
            return value


def _f32(value: float) -> float:
    return struct.unpack("<f", struct.pack("<f", value))[0]


async def test_literals_round_trip_through_the_engine(kg: KnowledgeGraph) -> None:
    seed = int(os.environ.get("INPUTLAYER_FUZZ_SEED", random.SystemRandom().randrange(2**32)))
    print(f"INPUTLAYER_FUZZ_SEED={seed}", file=sys.stderr)
    rng = random.Random(seed)
    await kg.define(Lit)
    rows = [
        Lit(
            id=n,
            s=_random_string(rng),
            i=rng.choice([I64_MIN, I64_MAX, 0, -1, rng.randint(I64_MIN, I64_MAX)]),
            f=_random_float(rng),
            b=rng.random() < 0.5,
            # Vectors are stored as float32.
            v=[_f32(rng.uniform(-1e3, 1e3)) for _ in range(rng.randint(0, 4))],
        )
        for n in range(200)
    ]
    for start in range(0, len(rows), 50):
        await kg.insert(rows[start : start + 50])
    stored = {r[0]: r for r in (await kg.query(Lit)).rows}
    assert len(stored) == len(rows)
    for row in rows:
        expected = [row.id, row.s, row.i, row.f, row.b, row.v]
        got = stored[row.id]
        assert got == expected, f"seed {seed}: {expected!r} came back as {got!r}"
        assert math.copysign(1, got[3]) == math.copysign(1, row.f), (
            f"seed {seed}: sign of {row.f!r}"
        )
    # The same literals as condition constants find their rows.
    for row in rng.sample(rows, 25):
        found = await kg.query(
            Lit.id,
            where=lambda t, row=row: (
                (t.s == row.s) & (t.i == row.i) & (t.f == row.f) & (t.b == row.b)
            ),
        )
        assert [row.id] in found.rows, f"seed {seed}: no match for {row!r}"


async def test_unrepresentable_values_are_refused_before_sending(kg: KnowledgeGraph) -> None:
    await kg.define(Lit)
    for bad in [{"f": math.nan}, {"f": math.inf}, {"s": "\ud800"}]:
        values = dict(id=1, s="x", i=1, f=1.0, b=True, v=[]) | bad
        with pytest.raises(CompileError):
            await kg.insert(Lit.model_construct(**values))
    assert (await kg.query(Lit)).rows == []
