"""Tests for inputlayer.derived - Derived relations, From/Where/Select builder."""

from typing import ClassVar

import pytest

from inputlayer._ast import Column as AstColumn, BoolExpr, Comparison, Literal
from inputlayer._proxy import ColumnProxy, RelationProxy
from inputlayer.compiler import compile_rule
from inputlayer.derived import Derived, From, FromWhere, RuleClause
from inputlayer.relation import Relation


# ── Test Relations ────────────────────────────────────────────────────

class Edge(Relation):
    src: int
    dst: int


class Reachable(Derived):
    src: int
    dst: int

    rules: ClassVar[list] = []  # Will be set after class definition


class Employee(Relation):
    id: int
    name: str
    department: str
    salary: float
    active: bool


class HighEarner(Derived):
    id: int
    name: str

    rules: ClassVar[list] = []


# ── From builder ──────────────────────────────────────────────────────

class TestFrom:
    def test_from_single_relation(self):
        f = From(Edge)
        assert len(f._relations) == 1
        assert f._relations[0][0] == "edge"

    def test_from_multiple_relations(self):
        f = From(Edge, Employee)
        assert len(f._relations) == 2

    def test_from_invalid(self):
        with pytest.raises(TypeError):
            From("not_a_relation")  # type: ignore

    def test_select_directly(self):
        clause = From(Edge).select(src=Edge.src, dst=Edge.dst)
        assert isinstance(clause, RuleClause)
        assert clause.condition is None
        assert len(clause.select_map) == 2

    def test_where_then_select(self):
        clause = (
            From(Employee)
            .where(lambda e: e.salary > 100000)
            .select(id=Employee.id, name=Employee.name)
        )
        assert isinstance(clause, RuleClause)
        assert clause.condition is not None
        assert len(clause.select_map) == 2


class TestFromWhere:
    def test_select(self):
        cond = Comparison("=", AstColumn("edge", "src"), Literal(1))
        fw = FromWhere(
            [("edge", Edge, None)],
            cond,
        )
        clause = fw.select(src=Edge.src, dst=Edge.dst)
        assert isinstance(clause, RuleClause)
        assert clause.condition is cond

    def test_select_invalid_type(self):
        cond = Comparison("=", AstColumn("edge", "src"), Literal(1))
        fw = FromWhere([("edge", Edge, None)], cond)
        with pytest.raises(TypeError):
            fw.select(src=42)  # type: ignore


# ── RuleClause compilation ───────────────────────────────────────────

class TestRuleClauseCompilation:
    def test_base_case(self):
        clause = From(Edge).select(src=Edge.src, dst=Edge.dst)
        result = compile_rule(
            "reachable",
            ["src", "dst"],
            clause.select_map,
            clause.relations,
            clause.condition,
            persistent=True,
        )
        assert result == "+reachable(Src, Dst) <- edge(Src, Dst)"

    def test_with_condition(self):
        clause = (
            From(Employee)
            .where(lambda e: e.salary > 100000)
            .select(id=Employee.id, name=Employee.name)
        )
        result = compile_rule(
            "high_earner",
            ["id", "name"],
            clause.select_map,
            clause.relations,
            clause.condition,
            persistent=True,
        )
        assert "+high_earner(Id, Name)" in result
        assert "Salary > 100000" in result

    def test_session_rule(self):
        clause = From(Edge).select(src=Edge.src, dst=Edge.dst)
        result = compile_rule(
            "reachable",
            ["src", "dst"],
            clause.select_map,
            clause.relations,
            clause.condition,
            persistent=False,
        )
        assert result.startswith("reachable(")
        assert not result.startswith("+")


class TestDerivedClass:
    def test_is_relation(self):
        assert issubclass(Derived, Relation)

    def test_subclass_columns(self):
        cols = Relation._get_columns(Reachable)
        assert cols == ["src", "dst"]


class TestFromWithLambdaProxy:
    def test_lambda_receives_proxy(self):
        """The where lambda should receive RelationProxy objects."""
        captured = []

        def cond(e):
            captured.append(type(e))
            return e.salary > 100000

        From(Employee).where(cond)
        assert captured[0] is RelationProxy

    def test_multi_relation_lambda(self):
        """Multi-relation where should receive multiple proxies."""
        captured = []

        def cond(e, d):
            captured.append((type(e), type(d)))
            return e.department == d.name

        From(Employee, Edge).where(cond)
        assert len(captured) == 1

    def test_where_invalid_return(self):
        with pytest.raises(TypeError):
            From(Employee).where(lambda e: "not a bool expr")


class TestFromWithRelationRef:
    def test_self_join(self):
        r1, r2 = Edge.refs(2)
        clause = (
            From(r1, r2)
            .where(lambda a, b: a.dst == b.src)
            .select(src=r1.src, dst=r2.dst)
        )
        assert isinstance(clause, RuleClause)
        assert len(clause.relations) == 2
        # Check aliases
        assert clause.relations[0][2] == "edge_1"
        assert clause.relations[1][2] == "edge_2"
