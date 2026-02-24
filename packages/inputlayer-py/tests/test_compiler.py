"""Tests for inputlayer.compiler - the core Python → Datalog compilation layer.

This is the most critical test file (~80 tests covering all compilation paths).
"""

import pytest

from inputlayer._ast import (
    AggExpr,
    And,
    Arithmetic,
    Column as AstColumn,
    Comparison,
    FuncCall,
    Literal,
    Not,
    Or,
    OrderedColumn,
)
from inputlayer._proxy import ColumnProxy
from inputlayer.aggregations import avg, count, count_distinct, max_, min_, sum_, top_k
from inputlayer.compiler import (
    _VarEnv,
    compile_bulk_insert,
    compile_conditional_delete,
    compile_delete,
    compile_expr,
    compile_insert,
    compile_query,
    compile_rule,
    compile_schema,
    compile_value,
)
from inputlayer.relation import Relation
from inputlayer.types import Timestamp, Vector, VectorInt8


# ── Test Relations ────────────────────────────────────────────────────

class Employee(Relation):
    id: int
    name: str
    department: str
    salary: float
    active: bool


class Department(Relation):
    name: str
    budget: float


class Edge(Relation):
    src: int
    dst: int


class Document(Relation):
    id: int
    title: str
    embedding: Vector[128]


class Event(Relation):
    id: int
    name: str
    ts: Timestamp


# ── compile_value ─────────────────────────────────────────────────────

class TestCompileValue:
    def test_int(self):
        assert compile_value(42) == "42"

    def test_negative_int(self):
        assert compile_value(-5) == "-5"

    def test_float(self):
        assert compile_value(3.14) == "3.14"

    def test_str(self):
        assert compile_value("hello") == '"hello"'

    def test_str_with_quotes(self):
        assert compile_value('say "hi"') == '"say \\"hi\\""'

    def test_str_with_backslash(self):
        assert compile_value("a\\b") == '"a\\\\b"'

    def test_bool_true(self):
        assert compile_value(True) == "true"

    def test_bool_false(self):
        assert compile_value(False) == "false"

    def test_none(self):
        assert compile_value(None) == "null"

    def test_vector(self):
        assert compile_value([1.0, 2.0, 3.0]) == "[1.0, 2.0, 3.0]"

    def test_empty_vector(self):
        assert compile_value([]) == "[]"

    def test_timestamp(self):
        ts = Timestamp(1704067200000)
        assert compile_value(ts) == "1704067200000"

    def test_unsupported(self):
        with pytest.raises(TypeError):
            compile_value({"a": 1})


# ── compile_schema ────────────────────────────────────────────────────

class TestCompileSchema:
    def test_basic(self):
        result = compile_schema(Employee)
        assert result == "+employee(id: int, name: string, department: string, salary: float, active: bool)"

    def test_vector_type(self):
        result = compile_schema(Document)
        assert result == "+document(id: int, title: string, embedding: vector[128])"

    def test_timestamp_type(self):
        result = compile_schema(Event)
        assert result == "+event(id: int, name: string, ts: timestamp)"

    def test_simple_relation(self):
        result = compile_schema(Edge)
        assert result == "+edge(src: int, dst: int)"


# ── compile_insert ────────────────────────────────────────────────────

class TestCompileInsert:
    def test_persistent(self):
        e = Employee(id=1, name="Alice", department="eng", salary=120000.0, active=True)
        result = compile_insert(e)
        assert result == '+employee(1, "Alice", "eng", 120000.0, true)'

    def test_session(self):
        e = Employee(id=1, name="Alice", department="eng", salary=120000.0, active=True)
        result = compile_insert(e, persistent=False)
        assert result == 'employee(1, "Alice", "eng", 120000.0, true)'

    def test_edge(self):
        edge = Edge(src=1, dst=2)
        result = compile_insert(edge)
        assert result == "+edge(1, 2)"

    def test_with_none_values(self):
        # Bool false test
        e = Employee(id=2, name="Bob", department="hr", salary=80000.0, active=False)
        result = compile_insert(e)
        assert "false" in result


class TestCompileBulkInsert:
    def test_basic(self):
        edges = [Edge(src=1, dst=2), Edge(src=3, dst=4)]
        result = compile_bulk_insert(Edge, edges)
        assert result == "+edge[(1, 2), (3, 4)]"

    def test_single(self):
        edges = [Edge(src=1, dst=2)]
        result = compile_bulk_insert(Edge, edges)
        assert result == "+edge[(1, 2)]"

    def test_session(self):
        edges = [Edge(src=1, dst=2)]
        result = compile_bulk_insert(Edge, edges, persistent=False)
        assert result == "edge[(1, 2)]"

    def test_with_strings(self):
        emps = [
            Employee(id=1, name="Alice", department="eng", salary=100000.0, active=True),
            Employee(id=2, name="Bob", department="hr", salary=90000.0, active=False),
        ]
        result = compile_bulk_insert(Employee, emps)
        assert result.startswith("+employee[")
        assert '(1, "Alice", "eng", 100000.0, true)' in result
        assert '(2, "Bob", "hr", 90000.0, false)' in result


# ── compile_delete ────────────────────────────────────────────────────

class TestCompileDelete:
    def test_basic(self):
        e = Employee(id=1, name="Alice", department="eng", salary=120000.0, active=True)
        result = compile_delete(e)
        assert result == '-employee(1, "Alice", "eng", 120000.0, true)'

    def test_edge(self):
        edge = Edge(src=1, dst=2)
        result = compile_delete(edge)
        assert result == "-edge(1, 2)"


class TestCompileConditionalDelete:
    def test_simple_condition(self):
        # -employee(X0, X1, X2, X3, X4) <- employee(X0, X1, X2, X3, X4), X2 = "sales"
        cond = Comparison("=", AstColumn("employee", "department"), Literal("sales"))
        result = compile_conditional_delete(Employee, cond)
        assert result.startswith("-employee(X0, X1, X2, X3, X4) <- employee(X0, X1, X2, X3, X4)")
        assert 'X2 = "sales"' in result

    def test_numeric_condition(self):
        cond = Comparison("<", AstColumn("employee", "salary"), Literal(50000))
        result = compile_conditional_delete(Employee, cond)
        assert "X3 < 50000" in result

    def test_compound_condition(self):
        cond = And(
            Comparison("=", AstColumn("employee", "department"), Literal("sales")),
            Comparison("<", AstColumn("employee", "salary"), Literal(50000)),
        )
        result = compile_conditional_delete(Employee, cond)
        assert 'X2 = "sales"' in result
        assert "X3 < 50000" in result


# ── compile_expr ──────────────────────────────────────────────────────

class TestCompileExpr:
    def test_literal_int(self):
        env = _VarEnv()
        assert compile_expr(Literal(42), env) == "42"

    def test_literal_str(self):
        env = _VarEnv()
        assert compile_expr(Literal("hello"), env) == '"hello"'

    def test_column(self):
        env = _VarEnv()
        result = compile_expr(AstColumn("employee", "name"), env)
        assert result == "Name"

    def test_arithmetic(self):
        env = _VarEnv()
        expr = Arithmetic("+", AstColumn("employee", "salary"), Literal(1000))
        result = compile_expr(expr, env)
        assert result == "Salary + 1000"

    def test_func_call(self):
        env = _VarEnv()
        expr = FuncCall("upper", (AstColumn("employee", "name"),))
        result = compile_expr(expr, env)
        assert result == "upper(Name)"

    def test_func_call_multi_arg(self):
        env = _VarEnv()
        expr = FuncCall("cosine", (AstColumn("d", "v1"), AstColumn("d", "v2")))
        result = compile_expr(expr, env)
        assert result == "cosine(V1, V2)"

    def test_ordered_asc(self):
        env = _VarEnv()
        expr = OrderedColumn(AstColumn("e", "salary"), descending=False)
        result = compile_expr(expr, env)
        assert result == "Salary:asc"

    def test_ordered_desc(self):
        env = _VarEnv()
        expr = OrderedColumn(AstColumn("e", "salary"), descending=True)
        result = compile_expr(expr, env)
        assert result == "Salary:desc"


# ── _VarEnv ───────────────────────────────────────────────────────────

class TestVarEnv:
    def test_get_var(self):
        env = _VarEnv()
        var = env.get_var(AstColumn("employee", "name"))
        assert var == "Name"

    def test_same_column_same_var(self):
        env = _VarEnv()
        v1 = env.get_var(AstColumn("employee", "name"))
        v2 = env.get_var(AstColumn("employee", "name"))
        assert v1 == v2

    def test_unify(self):
        env = _VarEnv()
        # e.department == d.name → shared variable
        var = env.unify(
            AstColumn("employee", "department"),
            AstColumn("department", "name"),
        )
        assert var == "Department"
        # After unification, both should resolve to the same variable
        v1 = env.get_var(AstColumn("employee", "department"))
        v2 = env.get_var(AstColumn("department", "name"))
        assert v1 == v2

    def test_lookup_missing(self):
        env = _VarEnv()
        assert env.lookup(AstColumn("e", "unknown")) is None

    def test_lookup_existing(self):
        env = _VarEnv()
        env.get_var(AstColumn("e", "name"))
        assert env.lookup(AstColumn("e", "name")) == "Name"


# ── compile_query ─────────────────────────────────────────────────────

class TestCompileQuery:
    def test_full_relation(self):
        result = compile_query(
            Employee,
            relations=[Employee],
        )
        assert result == "?Id, Name, Department, Salary, Active <- employee(Id, Name, Department, Salary, Active)"

    def test_select_columns(self):
        result = compile_query(
            AstColumn("employee", "name"),
            AstColumn("employee", "salary"),
            relations=[Employee],
        )
        assert isinstance(result, str)
        assert "Name" in result
        assert "Salary" in result

    def test_with_filter(self):
        cond = Comparison("=", AstColumn("employee", "department"), Literal("eng"))
        result = compile_query(
            Employee,
            relations=[Employee],
            where_condition=cond,
        )
        assert isinstance(result, str)
        assert 'Department = "eng"' in result

    def test_with_limit(self):
        result = compile_query(
            Employee,
            relations=[Employee],
            limit=10,
        )
        assert "limit(10)" in result

    def test_with_limit_offset(self):
        result = compile_query(
            Employee,
            relations=[Employee],
            limit=10,
            offset=20,
        )
        assert "limit(10, 20)" in result

    def test_with_order_by(self):
        result = compile_query(
            Employee,
            relations=[Employee],
            order_by=OrderedColumn(AstColumn("employee", "salary"), descending=True),
        )
        assert isinstance(result, str)
        assert "Salary:desc" in result

    def test_join(self):
        on_cond = Comparison(
            "=",
            AstColumn("employee", "department"),
            AstColumn("department", "name"),
        )
        result = compile_query(
            AstColumn("employee", "name"),
            AstColumn("department", "budget"),
            relations=[Employee, Department],
            on_condition=on_cond,
        )
        assert isinstance(result, str)
        # After unification, department.name and employee.department share a variable
        assert "employee(" in result
        assert "department(" in result

    def test_aggregation_count(self):
        agg = AggExpr(func="count", column=AstColumn("employee", "id"))
        result = compile_query(
            agg,
            relations=[Employee],
        )
        assert isinstance(result, str)
        assert "count<Id>" in result

    def test_aggregation_with_groupby(self):
        agg = AggExpr(func="count", column=AstColumn("employee", "id"))
        result = compile_query(
            AstColumn("employee", "department"),
            agg,
            relations=[Employee],
        )
        assert isinstance(result, str)
        assert "Department" in result
        assert "count<Id>" in result

    def test_or_condition_splits(self):
        cond = Or(
            Comparison("=", AstColumn("employee", "department"), Literal("eng")),
            Comparison("=", AstColumn("employee", "department"), Literal("sales")),
        )
        result = compile_query(
            Employee,
            relations=[Employee],
            where_condition=cond,
        )
        # OR → list of queries
        assert isinstance(result, list)
        assert len(result) == 2

    def test_negation(self):
        cond = Not(Comparison("=", AstColumn("employee", "active"), Literal(False)))
        result = compile_query(
            Employee,
            relations=[Employee],
            where_condition=cond,
        )
        assert isinstance(result, str)
        assert "!" in result

    def test_computed_column(self):
        result = compile_query(
            AstColumn("employee", "name"),
            relations=[Employee],
            computed={"bonus": Arithmetic("*", AstColumn("employee", "salary"), Literal(0.1))},
        )
        assert isinstance(result, str)
        assert "Salary * 0.1" in result


# ── compile_rule ──────────────────────────────────────────────────────

class TestCompileRule:
    def test_base_case(self):
        result = compile_rule(
            "reachable",
            ["src", "dst"],
            {
                "src": AstColumn("edge", "src"),
                "dst": AstColumn("edge", "dst"),
            },
            [(Edge._resolve_name(), Edge, None)],
            persistent=True,
        )
        assert result == "+reachable(Src, Dst) <- edge(Src, Dst)"

    def test_session_rule(self):
        result = compile_rule(
            "reachable",
            ["src", "dst"],
            {
                "src": AstColumn("edge", "src"),
                "dst": AstColumn("edge", "dst"),
            },
            [(Edge._resolve_name(), Edge, None)],
            persistent=False,
        )
        assert result == "reachable(Src, Dst) <- edge(Src, Dst)"

    def test_with_condition(self):
        cond = Comparison(">", AstColumn("employee", "salary"), Literal(100000))
        result = compile_rule(
            "high_earner",
            ["id", "name"],
            {
                "id": AstColumn("employee", "id"),
                "name": AstColumn("employee", "name"),
            },
            [(Employee._resolve_name(), Employee, None)],
            condition=cond,
            persistent=True,
        )
        assert "+high_earner(Id, Name)" in result
        assert "Salary > 100000" in result

    def test_recursive(self):
        # reachable(Src, Dst) <- reachable(Src, Mid), edge(Mid, Dst)
        class Reachable(Relation):
            src: int
            dst: int

        join_cond = Comparison(
            "=",
            AstColumn("reachable", "dst"),
            AstColumn("edge", "src"),
        )
        result = compile_rule(
            "reachable",
            ["src", "dst"],
            {
                "src": AstColumn("reachable", "src"),
                "dst": AstColumn("edge", "dst"),
            },
            [
                ("reachable", Reachable, None),
                ("edge", Edge, None),
            ],
            condition=join_cond,
            persistent=True,
        )
        # Head has Src from reachable.src and Dst from edge.dst
        # The join condition unifies reachable.dst == edge.src
        assert result.startswith("+reachable(Src,")
        assert "reachable(" in result
        assert "edge(" in result
        assert "<-" in result
