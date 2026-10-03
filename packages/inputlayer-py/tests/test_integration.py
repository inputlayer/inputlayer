"""Integration tests - require a running InputLayer server.

Set INPUTLAYER_TEST_SERVER=ws://localhost:8080/ws to enable.
"""

from __future__ import annotations

import os
from typing import ClassVar

import pytest

from inputlayer import (
    Derived,
    From,
    InputLayer,
    QueryError,
    Relation,
    StatementFailedError,
    Vector,
    count,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("INPUTLAYER_TEST_SERVER"),
    reason="INPUTLAYER_TEST_SERVER not set",
)


# ── Test Relations ────────────────────────────────────────────────────

class Edge(Relation):
    src: int
    dst: int


class Employee(Relation):
    id: int
    name: str
    department: str
    salary: float
    active: bool


class Reachable(Derived):
    src: int
    dst: int

    rules: ClassVar[list] = []


class Document(Relation):
    id: int
    title: str
    embedding: Vector[3]


# Set up rules after class definition
Reachable.rules = [
    From(Edge).select(src=Edge.src, dst=Edge.dst),
    From(Reachable, Edge)
    .where(lambda r, e: r.dst == e.src)
    .select(src=Reachable.src, dst=Edge.dst),
]


# ── Connection Tests ──────────────────────────────────────────────────

class TestConnection:
    @pytest.mark.asyncio
    async def test_connect_auth(self, client: InputLayer):
        assert client.connected
        assert client.session_id is not None
        assert client.server_version is not None
        assert client.role is not None


# ── Schema Tests ──────────────────────────────────────────────────────

class TestSchema:
    @pytest.mark.asyncio
    async def test_define_and_list(self, client: InputLayer):
        kg = client.knowledge_graph("test_schema_py")
        try:
            await kg.define(Edge)
            # `.rel` only surfaces relations after at least one row exists
            # in storage; defining the schema is necessary but not
            # sufficient. Insert a sentinel row before listing.
            await kg.insert([Edge(src=1, dst=2)])
            rels = await kg.relations()
            names = [r.name for r in rels]
            assert "edge" in names
        finally:
            await client.drop_knowledge_graph("test_schema_py")


# ── Insert + Query Tests ─────────────────────────────────────────────

class TestInsertQuery:
    @pytest.mark.asyncio
    async def test_insert_and_query(self, client: InputLayer):
        kg = client.knowledge_graph("test_iq_py")
        try:
            await kg.define(Edge)
            await kg.insert([
                Edge(src=1, dst=2),
                Edge(src=2, dst=3),
                Edge(src=3, dst=4),
            ])
            result = await kg.query(Edge)
            assert len(result) == 3
        finally:
            await client.drop_knowledge_graph("test_iq_py")

    @pytest.mark.asyncio
    async def test_query_with_filter(self, client: InputLayer):
        kg = client.knowledge_graph("test_filter_py")
        try:
            await kg.define(Employee)
            await kg.insert([
                Employee(id=1, name="Alice", department="eng", salary=120000.0, active=True),
                Employee(id=2, name="Bob", department="hr", salary=90000.0, active=True),
                Employee(id=3, name="Charlie", department="eng", salary=110000.0, active=False),
            ])
            result = await kg.query(
                Employee,
                where=lambda e: (e.department == "eng") & (e.active == True),  # noqa
            )
            assert len(result) >= 1
        finally:
            await client.drop_knowledge_graph("test_filter_py")


# ── Rule Tests ────────────────────────────────────────────────────────

class TestRules:
    @pytest.mark.asyncio
    async def test_recursive_rule(self, client: InputLayer):
        kg = client.knowledge_graph("test_rules_py")
        try:
            await kg.define(Edge)
            await kg.insert([
                Edge(src=1, dst=2),
                Edge(src=2, dst=3),
                Edge(src=3, dst=4),
            ])
            await kg.define_rules(Reachable)
            result = await kg.query(Reachable)
            # Should have transitive closure: 1→2, 1→3, 1→4, 2→3, 2→4, 3→4
            assert len(result) >= 6
        finally:
            await client.drop_knowledge_graph("test_rules_py")


# ── Delete Tests ──────────────────────────────────────────────────────

class TestDelete:
    @pytest.mark.asyncio
    async def test_conditional_delete(self, client: InputLayer):
        kg = client.knowledge_graph("test_delete_py")
        try:
            await kg.define(Employee)
            await kg.insert([
                Employee(id=1, name="Alice", department="eng",
                         salary=120000.0, active=True),
                Employee(id=2, name="Bob", department="hr",
                         salary=90000.0, active=True),
            ])
            await kg.delete(Employee, where=lambda e: e.department == "hr")
            result = await kg.query(Employee)
            assert len(result) == 1
            assert result.rows[0][1] == "Alice"
        finally:
            await client.drop_knowledge_graph("test_delete_py")


# ── Session Tests ─────────────────────────────────────────────────────

class TestSession:
    @pytest.mark.asyncio
    async def test_session_insert(self, client: InputLayer):
        kg = client.knowledge_graph("test_session_py")
        try:
            await kg.define(Edge)
            # Persistent insert
            await kg.insert(Edge(src=1, dst=2))
            # Session insert
            await kg.session.insert(Edge(src=10, dst=20))
            # Both should be queryable
            result = await kg.query(Edge)
            assert len(result) >= 2
        finally:
            await client.drop_knowledge_graph("test_session_py")


# ── Aggregation Tests ─────────────────────────────────────────────────

class TestAggregation:
    @pytest.mark.asyncio
    async def test_count(self, client: InputLayer):
        kg = client.knowledge_graph("test_agg_py")
        try:
            await kg.define(Edge)
            await kg.insert([
                Edge(src=1, dst=2),
                Edge(src=2, dst=3),
                Edge(src=3, dst=4),
            ])
            result = await kg.query(count(Edge.src), join=[Edge])
            assert result.scalar() == 3
        finally:
            await client.drop_knowledge_graph("test_agg_py")


# ── Multi-KG Tests ────────────────────────────────────────────────────

class TestMultiKG:
    @pytest.mark.asyncio
    async def test_list_kgs(self, client: InputLayer):
        kgs = await client.list_knowledge_graphs()
        assert isinstance(kgs, list)
        assert "default" in kgs


# ── Error Handling Tests ──────────────────────────────────────────────

class TestErrors:
    @pytest.mark.asyncio
    async def test_raw_execute(self, client: InputLayer):
        kg = client.knowledge_graph("default")
        result = await kg.execute(".status")
        assert len(result) > 0

    @pytest.mark.asyncio
    async def test_failed_insert_raises(self, client: InputLayer):
        kg = client.knowledge_graph("test_err_insert_py")
        try:
            await kg.define_rules(Reachable)
            with pytest.raises(QueryError) as caught:
                await kg.insert(Reachable(src=1, dst=2))
            assert "derived relation" in caught.value.message
            assert caught.value.code is not None
            assert not isinstance(caught.value, StatementFailedError)
        finally:
            await client.drop_knowledge_graph("test_err_insert_py")

    @pytest.mark.asyncio
    async def test_insert_counts_stored_facts(self, client: InputLayer):
        kg = client.knowledge_graph("test_err_count_py")
        try:
            await kg.define(Edge)
            first = await kg.insert([Edge(src=1, dst=2), Edge(src=2, dst=3)])
            again = await kg.insert([Edge(src=1, dst=2), Edge(src=3, dst=4)])
            assert (first.count, again.count) == (2, 1)
        finally:
            await client.drop_knowledge_graph("test_err_count_py")

    @pytest.mark.asyncio
    async def test_partial_failure_names_each_statement(self, client: InputLayer):
        kg = client.knowledge_graph("test_err_partial_py")
        try:
            await kg.define(Edge)
            with pytest.raises(StatementFailedError) as caught:
                await kg.execute("+edge[(1, 2)]\n.rel drop no_such_rel\n+edge[(2, 3)]")
            assert [(e.index, e.code) for e in caught.value.errors] == [(1, "not_found")]
            # The engine ran the other statements.
            assert len(await kg.query(Edge)) == 2
        finally:
            await client.drop_knowledge_graph("test_err_partial_py")

    @pytest.mark.asyncio
    async def test_partial_failure_survives_a_chunked_result(self, client: InputLayer):
        kg = client.knowledge_graph("test_err_chunked_py")
        pad = "x" * 64
        try:
            for start in (0, 10_000):
                rows = ", ".join(f'({i}, "{pad}")' for i in range(start, start + 10_000))
                await kg.execute(f"+big[{rows}]")
            with pytest.raises(StatementFailedError) as caught:
                await kg.execute(".rel drop no_such_rel\n?big(Id, Pad)")
            assert [(e.index, e.code) for e in caught.value.errors] == [(0, "not_found")]
            assert len(caught.value.result.rows) == 20_000
        finally:
            await client.drop_knowledge_graph("test_err_chunked_py")

    @pytest.mark.asyncio
    async def test_load_raises_on_a_failed_statement(self, client: InputLayer, tmp_path):
        kg = client.knowledge_graph("test_err_load_py")
        path = tmp_path / "seed.iql"
        path.write_text("+edge[(1, 2)]\n+edge[(1, 2, 3)]\n")
        try:
            await kg.define(Edge)
            with pytest.raises(StatementFailedError) as caught:
                await kg.load(str(path))
            assert [(e.index, e.code) for e in caught.value.errors] == [(1, "validation")]
        finally:
            await client.drop_knowledge_graph("test_err_load_py")
