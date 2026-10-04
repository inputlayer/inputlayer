"""Live tests for every query example in docs/content/docs/guides/python-sdk.mdx.

Each test runs the documented code against a real engine, so a query form
the engine rejects, or a result shaped differently from what the guide
shows, fails here. The cases mirror the JS SDK's
``tests/docs-queries.integration.test.ts``, plus pins for the compile rules
the Python SDK's queries rest on (one program per call, projection by
position, sorting and pagination in the engine, program-local aggregates).

Set INPUTLAYER_TEST_SERVER (and INPUTLAYER_TEST_USER /
INPUTLAYER_TEST_PASSWORD) to enable; ``make python-sdk-live`` starts a
server and runs them. The file is not named ``test_*`` so the unit-test run
does not collect it.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections.abc import AsyncIterator
from typing import ClassVar

import pytest
import pytest_asyncio

from inputlayer import (
    Derived,
    From,
    InputLayer,
    KnowledgeGraph,
    Relation,
    Vector,
    avg,
    count,
    max_,
    top_k,
)
from inputlayer import functions as fn

SERVER_URL = os.environ.get("INPUTLAYER_TEST_SERVER", "")
USERNAME = os.environ.get("INPUTLAYER_TEST_USER", "admin")
PASSWORD = os.environ.get("INPUTLAYER_TEST_PASSWORD", "admin")

KG_NAME = "test_docs_queries_py"

pytestmark = pytest.mark.skipif(not SERVER_URL, reason="INPUTLAYER_TEST_SERVER not set")


# Schemas as the guide defines them.
class Employee(Relation):
    id: int
    name: str
    department: str
    salary: float
    active: bool


class Department(Relation):
    name: str
    budget: float


# The guide's Document also has `created_at: Timestamp`, which no query
# example reads, and a dimensioned `Vector[384]`; the SDK declares those as
# `timestamp` and `vector[N]`, which the engine's schema parser rejects
# (column types are the next compile-path change), so Document keeps an
# undimensioned vector and leaves the timestamp out.
class Document(Relation):
    id: int
    title: str
    content: str
    embedding: Vector


class Edge(Relation):
    src: int
    dst: int


class Reachable(Derived):
    src: int
    dst: int
    rules: ClassVar[list] = []


Reachable.rules = [
    # Base case: direct edges are reachable
    From(Edge).select(src=Edge.src, dst=Edge.dst),
    # Recursive case: if A reaches B and B reaches C, then A reaches C
    From(Reachable, Edge)
    .where(lambda r, e: r.dst == e.src)
    .select(src=Reachable.src, dst=Edge.dst),
]


# Timestamps are Unix milliseconds.
class SensorReading(Relation):
    sensor_id: int
    value: float
    timestamp: int


class Article(Relation):
    title: str
    published_at: int


# The salaries of fix-report item 1: two measures of one column differ in
# every group but hr.
class Staff(Relation):
    id: int
    department: str
    salary: float


NOW = int(time.time() * 1000)


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
    await graph.define(Employee, Department, Document, Edge, SensorReading, Article, Staff)
    await graph.insert([
        Employee(id=1, name="Alice", department="eng", salary=120000.0, active=True),
        Employee(id=2, name="Bob", department="hr", salary=90000.0, active=True),
        Employee(id=3, name="Charlie", department="eng", salary=110000.0, active=False),
        Employee(id=4, name="Dana", department="eng", salary=130000.0, active=True),
        Employee(id=5, name="Eve", department="hr", salary=95000.0, active=True),
    ])
    await graph.insert([
        Department(name="eng", budget=1000000.0),
        Department(name="hr", budget=300000.0),
    ])
    await graph.insert([
        Document(id=1, title="same", content="a", embedding=[1.0, 0.0, 0.0]),
        Document(id=2, title="orthogonal", content="b", embedding=[0.0, 1.0, 0.0]),
    ])
    await graph.insert([Edge(src=1, dst=2), Edge(src=2, dst=3), Edge(src=3, dst=4)])
    await graph.insert([
        SensorReading(sensor_id=1, value=1.5, timestamp=NOW - 60_000),
        SensorReading(sensor_id=2, value=2.5, timestamp=NOW - 86_400_000),
    ])
    await graph.insert(Article(title="fresh", published_at=NOW))
    await graph.insert([
        Staff(id=1, department="eng", salary=120.0),
        Staff(id=2, department="eng", salary=100.0),
        Staff(id=3, department="eng", salary=100.0),
        Staff(id=4, department="hr", salary=90.0),
    ])
    yield graph
    with contextlib.suppress(Exception):  # best effort
        await il.drop_knowledge_graph(KG_NAME)


# ── Basic Queries ─────────────────────────────────────────────────────


async def test_basic_queries_all_rows(kg: KnowledgeGraph) -> None:
    # All rows from a relation
    result = await kg.query(Employee)
    lines = []
    for emp in result:
        lines.append(f"{emp.name} - {emp.department}")
    assert result.columns == ["id", "name", "department", "salary", "active"]
    assert sorted(lines) == [
        "Alice - eng", "Bob - hr", "Charlie - eng", "Dana - eng", "Eve - hr",
    ]
    # Selecting one whole relation iterates as instances of it.
    assert all(isinstance(emp, Employee) for emp in result)


async def test_basic_queries_with_a_filter(kg: KnowledgeGraph) -> None:
    engineers = await kg.query(
        Employee,
        where=lambda e: (e.department == "eng") & (e.active == True),  # noqa: E712
    )
    assert sorted(e.name for e in engineers) == ["Alice", "Dana"]


async def test_selecting_specific_columns(kg: KnowledgeGraph) -> None:
    # Sign-off F8: the selected columns, in select order, named by column.
    result = await kg.query(
        Employee.name, Employee.salary,
        join=[Employee],
        where=lambda e: e.department == "eng",
    )
    lines = []
    for row in result:
        lines.append(f"{row.name}: ${row.salary:.0f}")
    assert result.columns == ["name", "salary"]
    assert sorted(lines) == ["Alice: $120000", "Charlie: $110000", "Dana: $130000"]


async def test_a_projection_is_a_set(kg: KnowledgeGraph) -> None:
    result = await kg.query(Employee.department, join=[Employee])
    assert sorted(result.to_tuples()) == [("eng",), ("hr",)]
    # A page of a projection counts distinct rows.
    page = await kg.query(
        Employee.department, join=[Employee], order_by=Employee.department.asc(), limit=1, offset=1
    )
    assert page.to_tuples() == [("hr",)]


async def test_joins(kg: KnowledgeGraph) -> None:
    result = await kg.query(
        Employee.name, Department.budget,
        join=[Employee, Department],
        on=lambda e, d: e.department == d.name,
    )
    assert result.columns == ["name", "budget"]
    assert sorted(result.to_tuples()) == [
        ("Alice", 1000000.0),
        ("Bob", 300000.0),
        ("Charlie", 1000000.0),
        ("Dana", 1000000.0),
        ("Eve", 300000.0),
    ]


async def test_self_joins(kg: KnowledgeGraph) -> None:
    e1, e2 = Employee.refs(2)
    result = await kg.query(
        e1.name, e2.name,
        join=[e1, e2],
        on=lambda a, b: (a.department == b.department) & (a.id != b.id),
    )
    # Every ordered pair of distinct colleagues, never a row with itself.
    assert result.columns == ["name", "name_2"]
    assert sorted(f"{a}-{b}" for a, b in result.to_tuples()) == [
        "Alice-Charlie", "Alice-Dana", "Bob-Eve", "Charlie-Alice",
        "Charlie-Dana", "Dana-Alice", "Dana-Charlie", "Eve-Bob",
    ]


async def test_computed_columns(kg: KnowledgeGraph) -> None:
    result = await kg.query(
        Employee.name,
        join=[Employee],
        bonus=Employee.salary * 0.1,
    )
    assert result.columns == ["name", "bonus"]
    bonus = {row.name: row.bonus for row in result}
    assert bonus["Alice"] == pytest.approx(12000)
    assert bonus["Bob"] == pytest.approx(9000)
    assert len(result) == 5


async def test_ordering_and_pagination(kg: KnowledgeGraph) -> None:
    # Top 10 highest paid
    result = await kg.query(
        Employee,
        order_by=Employee.salary.desc(),
        limit=10,
    )
    # Second page
    page2 = await kg.query(
        Employee,
        order_by=Employee.name.asc(),
        limit=10,
        offset=10,
    )
    assert [e.name for e in result] == ["Dana", "Alice", "Charlie", "Eve", "Bob"]
    assert len(page2) == 0

    # The same pagination on a page that has rows.
    page = await kg.query(Employee, order_by=Employee.name.asc(), limit=2, offset=2)
    assert [e.name for e in page] == ["Charlie", "Dana"]


async def test_sort_and_limit_run_in_the_engine(kg: KnowledgeGraph) -> None:
    # R-SORT: the annotation sits on the first atom and the engine applies
    # the limit after sorting, so the result is the top row, not a sorted
    # sample of the first rows.
    plan = await kg.debug(Employee, order_by=Employee.salary.desc(), limit=1)
    assert plan.iql == "?employee(Id, Name, Department, Salary:desc, Active), limit(1)"
    result = await kg.query(Employee, order_by=Employee.salary.desc(), limit=1)
    assert [e.name for e in result] == ["Dana"]
    assert result.total_count == 5


# ── Aggregations ──────────────────────────────────────────────────────


async def test_aggregations_group_by_department_with_stats(kg: KnowledgeGraph) -> None:
    result = await kg.query(
        Employee.department,
        count(Employee.id),
        avg(Employee.salary),
        max_(Employee.salary),
        join=[Employee],
    )
    lines = []
    for row in result:
        lines.append(f"{row.department}: {row.count_id} employees, avg ${row.avg_salary:.0f}")
    assert result.columns == ["department", "count_id", "avg_salary", "max_salary"]
    assert sorted(result.to_tuples()) == [
        ("eng", 3, 120000.0, 130000.0),
        ("hr", 2, 92500.0, 95000.0),
    ]
    assert sorted(lines) == ["eng: 3 employees, avg $120000", "hr: 2 employees, avg $92500"]


async def test_agg_two_measures_same_column(kg: KnowledgeGraph) -> None:
    # Fix-report item 1: the query atom repeated the measure's variable
    # (?il_agg_x(Department, Salary, Salary)), which joined avg and max as
    # equal and silently dropped every group where they differ.
    result = await kg.query(Staff.department, avg(Staff.salary), max_(Staff.salary), join=[Staff])
    rows = sorted(result.to_tuples())
    assert [r[0] for r in rows] == ["eng", "hr"]
    assert rows[0][1] == pytest.approx(106.67, abs=0.01)
    assert rows[0][2] == 120
    assert rows[1][1:] == (90, 90)


async def test_aggregates_leave_no_session_rule(kg: KnowledgeGraph) -> None:
    # Fix-report item 2: the aggregate's rule and query are one program, so
    # the rule lasts only for that request and nothing needs cleaning up.
    for _ in range(100):
        await kg.query(Employee.department, count(Employee.id), join=[Employee])
        await asyncio.sleep(0.02)  # under the engine's 100 messages/s rate limit
    session = await kg.execute(".session")
    assert session.rows == [["No session data defined."]]


async def test_count_without_a_column(kg: KnowledgeGraph) -> None:
    # Fix-report item 5: count() compiled to count<>, which the engine rejects.
    result = await kg.query(count(), join=[Employee])
    assert result.columns == ["count"]
    assert result.scalar() == 5


async def test_aggregations_top_k_per_group(kg: KnowledgeGraph) -> None:
    # Top 3 highest-paid employees per department
    result = await kg.query(
        Employee.department,
        top_k(3, Employee.name, order_by=Employee.salary, desc=True),
        join=[Employee],
    )
    assert result.columns == ["department", "name", "salary"]
    assert sorted(result.to_tuples()) == [
        ("eng", "Alice", 120000.0),
        ("eng", "Charlie", 110000.0),
        ("eng", "Dana", 130000.0),
        ("hr", "Bob", 90000.0),
        ("hr", "Eve", 95000.0),
    ]

    # k bounds each group.
    top1 = await kg.query(
        Employee.department,
        top_k(1, Employee.name, order_by=Employee.salary, desc=True),
        join=[Employee],
    )
    assert sorted(top1.to_tuples()) == [("eng", "Dana", 130000.0), ("hr", "Eve", 95000.0)]


async def test_aggregate_ordered_and_limited(kg: KnowledgeGraph) -> None:
    result = await kg.query(
        Employee.department, count(Employee.id),
        join=[Employee],
        order_by=Employee.department.desc(),
        limit=1,
    )
    assert result.to_tuples() == [("hr", 2)]


# ── Working with Results ──────────────────────────────────────────────


async def test_working_with_results(kg: KnowledgeGraph) -> None:
    result = await kg.query(Employee)

    # Iterate as typed objects
    assert sorted(emp.name for emp in result) == ["Alice", "Bob", "Charlie", "Dana", "Eve"]

    # Check result metadata
    assert len(result) == 5
    assert result.total_count == 5
    assert isinstance(result.execution_time_ms, int)

    # Get the first row (or None if empty)
    first = result.first()
    assert hasattr(first, "name")

    # Get a single scalar value
    total = (await kg.query(count(Employee.id), join=[Employee])).scalar()
    assert total == 5

    # Convert to different formats
    dicts = result.to_dicts()
    tuples = result.to_tuples()
    assert len(dicts) == 5
    assert len(tuples[0]) == 5
    df = result.to_df()
    assert list(df.columns) == ["id", "name", "department", "salary", "active"]


# ── Query Plans and proofs ────────────────────────────────────────────


async def test_query_plans(kg: KnowledgeGraph) -> None:
    plan = await kg.debug(
        Employee,
        where=lambda e: e.department == "eng",
    )
    assert plan.iql == '?employee(Id, Name, Department, Salary, Active), Department = "eng"'
    assert "employee" in plan.plan


async def test_debug_takes_the_arguments_of_query(kg: KnowledgeGraph) -> None:
    # Sign-off F9: kg.debug(join=...) raised TypeError.
    plan = await kg.debug(
        Employee.name, Department.budget,
        join=[Employee, Department],
        on=lambda e, d: e.department == d.name,
    )
    assert plan.iql == (
        "?employee(Id, Name, Department, Salary, Active), department(Department, Budget)"
    )
    assert "department" in plan.plan


async def test_debug_shows_the_plan_of_an_aggregate_query(kg: KnowledgeGraph) -> None:
    plan = await kg.debug(Employee.department, count(Employee.id), join=[Employee])
    assert "employee" in plan.plan


async def test_why_returns_the_selected_columns_with_a_proof_per_row(kg: KnowledgeGraph) -> None:
    why = await kg.why(
        Employee.name, Employee.salary,
        join=[Employee],
        where=lambda e: e.department == "eng",
    )
    assert why.results.columns == ["name", "salary"]
    assert sorted(why.results.to_tuples()) == [
        ("Alice", 120000.0), ("Charlie", 110000.0), ("Dana", 130000.0),
    ]
    assert len(why.proof_trees) == 3


async def test_why_explains_an_aggregate_query(kg: KnowledgeGraph) -> None:
    why = await kg.why(Employee.department, count(Employee.id), join=[Employee])
    assert why.results.columns == ["department", "count_id"]
    assert sorted(why.results.to_tuples()) == [("eng", 3), ("hr", 2)]
    assert len(why.proof_trees) == 2


async def test_why_returns_a_column_of_the_second_joined_relation(kg: KnowledgeGraph) -> None:
    why = await kg.why(
        Employee.name, Department.budget,
        join=[Employee, Department],
        on=lambda e, d: e.department == d.name,
        where=lambda e, d: e.salary > 115000,
    )
    assert why.results.columns == ["name", "budget"]
    assert sorted(why.results.to_tuples()) == [("Alice", 1000000.0), ("Dana", 1000000.0)]
    assert len(why.proof_trees) == 2


async def test_why_returns_a_computed_column(kg: KnowledgeGraph) -> None:
    why = await kg.why(
        Employee.name,
        join=[Employee],
        where=lambda e: e.department == "hr",
        doubled=Employee.salary * 2,
    )
    assert why.results.columns == ["name", "doubled"]
    assert sorted(why.results.to_tuples()) == [("Bob", 180000.0), ("Eve", 190000.0)]


async def test_an_offset_without_a_limit_skips_rows(kg: KnowledgeGraph) -> None:
    opts = {
        "join": [Employee],
        "order_by": Employee.salary.desc(),
        "offset": 3,
    }
    expected = [("Eve",), ("Bob",)]
    assert (await kg.query(Employee.name, **opts)).to_tuples() == expected
    assert (await kg.why(Employee.name, **opts)).results.to_tuples() == expected
    merged = await kg.query(
        Employee.name,
        where=lambda e: (e.department == "hr") | (e.salary > 100000),
        **opts,
    )
    assert merged.to_tuples() == expected


async def test_why_orders_and_paginates_like_query(kg: KnowledgeGraph) -> None:
    opts = {"join": [Employee], "order_by": Employee.salary.desc(), "limit": 2, "offset": 1}
    why = await kg.why(Employee.name, **opts)
    assert why.results.to_tuples() == (await kg.query(Employee.name, **opts)).to_tuples()
    assert why.results.to_tuples() == [("Alice",), ("Charlie",)]
    assert len(why.proof_trees) == 2


async def test_why_orders_and_limits_an_aggregate_query_like_query(kg: KnowledgeGraph) -> None:
    opts = {"join": [Employee], "order_by": Employee.department.desc(), "limit": 1}
    why = await kg.why(Employee.department, count(Employee.id), **opts)
    query = await kg.query(Employee.department, count(Employee.id), **opts)
    assert why.results.to_tuples() == query.to_tuples() == [("hr", 2)]
    assert len(why.proof_trees) == 1


# ── Raw IQL, rules, sessions ──────────────────────────────────────────


async def test_raw_iql(kg: KnowledgeGraph) -> None:
    result = await kg.execute("?employee(Id, Name, D, Salary, A), Salary > 100000")
    assert sorted(row[1] for row in result.rows) == ["Alice", "Charlie", "Dana"]


async def test_derived_relations_querying_a_recursive_rule(kg: KnowledgeGraph) -> None:
    # Deploy the rule (persistent - survives restarts)
    await kg.define_rules(Reachable)

    # Query it
    result = await kg.query(Reachable, where=lambda r: r.src == 1)
    reached = []
    for row in result:
        reached.append(row.dst)
    assert sorted(reached) == [2, 3, 4]


async def test_sessions_session_facts_mix_with_persistent_data(kg: KnowledgeGraph) -> None:
    await kg.session.insert([
        Employee(id=999, name="Temp", department="eng", salary=0.0, active=True),
    ])
    # Query as normal - session facts mix with persistent data
    result = await kg.query(Employee)
    assert "Temp" in [e.name for e in result]
    assert len(result) == 6


# ── Built-in Functions ────────────────────────────────────────────────


async def test_distance_functions(kg: KnowledgeGraph) -> None:
    query_vec = [1.0, 0.0, 0.0]
    result = await kg.query(
        Document.title,
        join=[Document],
        distance=fn.cosine(Document.embedding, query_vec),
    )
    assert result.columns == ["title", "distance"]
    distance = {row.title: row.distance for row in result}
    assert distance["same"] == pytest.approx(0)
    assert distance["orthogonal"] == pytest.approx(1)


async def test_temporal_functions(kg: KnowledgeGraph) -> None:
    # Rows from the last hour
    result = await kg.query(
        SensorReading,
        where=lambda r: r.timestamp > fn.time_sub(fn.time_now(), 3600000),
    )
    assert [r.sensor_id for r in result] == [1]

    # Time-decayed scoring
    scored = await kg.query(
        Article.title,
        join=[Article],
        score=fn.time_decay(Article.published_at, fn.time_now(), 86400000),
    )
    assert scored.columns == ["title", "score"]
    assert scored.first().score > 0.99


async def test_or_conditions_merge_their_branches(kg: KnowledgeGraph) -> None:
    result = await kg.query(
        Employee.name,
        join=[Employee],
        where=lambda e: (e.department == "hr") | (e.salary > 115000),
        order_by=Employee.salary.desc(),
        limit=3,
    )
    assert [row.name for row in result] == ["Dana", "Alice", "Eve"]
