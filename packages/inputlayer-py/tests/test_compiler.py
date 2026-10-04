"""Tests for inputlayer.compiler - the core Python → IQL compilation layer.

This is the most critical test file (~80 tests covering all compilation paths).
"""

import pytest

from inputlayer._ast import (
    AggExpr,
    And,
    Arithmetic,
    Comparison,
    FuncCall,
    Literal,
    Not,
    Or,
    OrderedColumn,
)
from inputlayer._ast import (
    Column as AstColumn,
)
from inputlayer.compiler import (
    _VarEnv,
    compile_bulk_insert,
    compile_conditional_delete,
    compile_delete,
    compile_expr,
    compile_insert,
    compile_query,
    compile_query_plan,
    compile_rule,
    compile_schema,
    compile_value,
)
from inputlayer.exceptions import CompileError, InternalError
from inputlayer.relation import Relation
from inputlayer.types import Timestamp, Vector

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

    def test_str_with_control_chars(self):
        assert compile_value("a\nb\r\tc") == '"a\\nb\\r\\tc"'

    def test_bool_true(self):
        assert compile_value(True) == "true"

    def test_bool_false(self):
        assert compile_value(False) == "false"

    def test_none_is_refused(self):
        # IQL has no null literal; the engine would read `null` as an unquoted atom.
        with pytest.raises(CompileError, match="no null"):
            compile_value(None)

    def test_vector(self):
        assert compile_value([1.0, 2.0, 3.0]) == "[1.0, 2.0, 3.0]"

    def test_empty_vector(self):
        assert compile_value([]) == "[]"

    def test_timestamp(self):
        ts = Timestamp(1704067200000)
        assert compile_value(ts) == "1704067200000"

    def test_unsupported(self):
        with pytest.raises(CompileError):
            compile_value({"a": 1})


# ── compile_schema ────────────────────────────────────────────────────

class TestCompileSchema:
    def test_basic(self):
        result = compile_schema(Employee)
        assert result == (
            "+employee(id: int, name: string, department: string,"
            " salary: float, active: bool)"
        )

    def test_vector_type(self):
        result = compile_schema(Document)
        # The engine's schema parser reads vector(N), not vector[N].
        assert result == "+document(id: int, title: string, embedding: vector(128))"

    def test_timestamp_type(self):
        # R-TYPE: the engine has no timestamp schema type; Unix ms in an int column.
        result = compile_schema(Event)
        assert result == "+event(id: int, name: string, ts: int)"

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


def _emp(col: str) -> AstColumn:
    return AstColumn("employee", col)


def _dept(col: str) -> AstColumn:
    return AstColumn("department", col)


EMP_ATOM = "employee(Id, Name, Department, Salary, Active)"


class TestCompileQuery:
    """R-QUERY: one ``?`` query binding every column of every atom."""

    def test_full_relation(self):
        result = compile_query(Employee, relations=[Employee])
        assert result == f"?{EMP_ATOM}"

    def test_selecting_a_relation_joins_it(self):
        assert compile_query(Employee) == f"?{EMP_ATOM}"

    def test_select_columns_binds_every_column(self):
        plan = compile_query_plan(_emp("name"), _emp("salary"), relations=[Employee])
        assert plan.program == f"?{EMP_ATOM}"
        assert plan.columns == ("Id", "Name", "Department", "Salary", "Active")
        assert plan.labels == ["name", "salary"]
        assert plan.dedupe

    def test_with_filter(self):
        cond = Comparison("=", _emp("department"), Literal("eng"))
        result = compile_query(Employee, relations=[Employee], where_condition=cond)
        assert result == f'?{EMP_ATOM}, Department = "eng"'

    def test_with_limit(self):
        assert compile_query(Employee, limit=10) == f"?{EMP_ATOM}, limit(10)"

    def test_with_limit_offset(self):
        assert compile_query(Employee, limit=10, offset=20) == f"?{EMP_ATOM}, limit(10, 20)"

    def test_join_shares_a_variable(self):
        on_cond = Comparison("=", _emp("department"), _dept("name"))
        plan = compile_query_plan(
            _emp("name"), _dept("budget"),
            relations=[Employee, Department],
            on_condition=on_cond,
        )
        assert plan.program == f"?{EMP_ATOM}, department(Department, Budget)"
        assert plan.columns == ("Id", "Name", "Department", "Salary", "Active", "Budget")
        assert plan.labels == ["name", "budget"]

    def test_self_join_keeps_inequality_and_suffixes_labels(self):
        from inputlayer._proxy import RelationRef

        e1, e2 = RelationRef(Employee, "e1"), RelationRef(Employee, "e2")
        on = And(
            Comparison("=", AstColumn("employee", "department", "e1"),
                       AstColumn("employee", "department", "e2")),
            Comparison("!=", AstColumn("employee", "id", "e1"), AstColumn("employee", "id", "e2")),
        )
        plan = compile_query_plan(
            AstColumn("employee", "name", "e1"), AstColumn("employee", "name", "e2"),
            relations=[e1, e2], on_condition=on,
        )
        assert plan.program == (
            "?employee(Id, Name, Department, Salary, Active), "
            "employee(Id_1, Name_2, Department, Salary_3, Active_4), Id != Id_1"
        )
        assert plan.labels == ["name", "name_2"]

    def test_negated_comparison_flips_its_operator(self):
        # IQL negates atoms only; `!(Active = false)` is a parse error.
        cond = Not(Comparison("=", _emp("active"), Literal(False)))
        result = compile_query(Employee, relations=[Employee], where_condition=cond)
        assert result == f"?{EMP_ATOM}, Active != false"

    def test_computed_column_is_a_binding_after_the_atoms(self):
        plan = compile_query_plan(
            _emp("name"),
            relations=[Employee],
            computed={"bonus": Arithmetic("*", _emp("salary"), Literal(0.1))},
        )
        assert plan.program == f"?{EMP_ATOM}, Bonus = Salary * 0.1"
        assert plan.columns[-1] == "Bonus"
        assert plan.labels == ["name", "bonus"]

    def test_no_relation_is_a_compile_error(self):
        with pytest.raises(CompileError):
            compile_query(_emp("name"))

    def test_shape_picks_columns_by_position(self):
        plan = compile_query_plan(_emp("salary"), _emp("name"), relations=[Employee])
        rows = [[1, "A", "eng", 10.0, True], [2, "B", "hr", 20.0, False]]
        assert plan.shape(rows) == [[10.0, "A"], [20.0, "B"]]

    def test_projection_is_a_set(self):
        plan = compile_query_plan(_emp("department"), relations=[Employee])
        rows = [[1, "A", "eng", 10.0, True], [2, "B", "eng", 20.0, True], [3, "C", "hr", 5.0, True]]
        assert plan.shape(rows) == [["eng"], ["hr"]]

    def test_shape_refuses_an_unexpected_width(self):
        plan = compile_query_plan(Employee)
        with pytest.raises(InternalError):
            plan.shape([[1, "A"]])


class TestCompileQuerySort:
    """R-SORT: annotations on the first atom, never client-side."""

    def test_order_by_annotates_the_first_atom(self):
        result = compile_query(
            Employee,
            relations=[Employee],
            order_by=OrderedColumn(_emp("salary"), descending=True),
            limit=10,
        )
        assert result == "?employee(Id, Name, Department, Salary:desc, Active), limit(10)"

    def test_ordered_relation_moves_first(self):
        plan = compile_query_plan(
            Employee, Department,
            relations=[Employee, Department],
            on_condition=Comparison("=", _emp("department"), _dept("name")),
            order_by=OrderedColumn(_dept("budget"), descending=False),
            limit=3,
        )
        assert plan.program == (
            f"?department(Department, Budget:asc), {EMP_ATOM}, limit(3)"
        )
        assert plan.columns == ("Department", "Budget", "Id", "Name", "Salary", "Active")
        assert plan.labels == ["id", "name", "department", "salary", "active", "name_2", "budget"]

    def test_offset_without_limit_pages_in_the_engine(self):
        plan = compile_query_plan(
            Employee, order_by=OrderedColumn(_emp("salary"), descending=True), offset=3
        )
        assert plan.program == (
            "?employee(Id, Name, Department, Salary:desc, Active), "
            "limit(9223372036854775807, 3)"
        )

    def test_page_of_a_projection_goes_through_the_query_rule(self):
        # A limit over a projection counts distinct projected rows.
        plan = compile_query_plan(
            _emp("department"),
            relations=[Employee],
            order_by=OrderedColumn(_emp("department"), descending=False),
            limit=2,
            offset=1,
        )
        assert plan.program == (
            f"il_q(Department) <- {EMP_ATOM}\n"
            "?il_q(Department:asc), limit(2, 1)"
        )
        assert plan.columns == ("Department",)

    @pytest.mark.parametrize("page", [{"limit": 2}, {"offset": 1}])
    def test_paging_a_projection_ordered_by_an_unselected_column_is_a_compile_error(
        self, page
    ):
        with pytest.raises(CompileError, match="ordering by a column you do not select"):
            compile_query_plan(
                _emp("name"),
                relations=[Employee],
                order_by=OrderedColumn(_emp("salary"), descending=True),
                **page,
            )

    def test_why_keeps_one_proof_per_distinct_projected_row(self):
        plan = compile_query_plan(
            _emp("department"),
            relations=[Employee],
            order_by=OrderedColumn(_emp("department"), descending=False),
            limit=2,
        )
        assert plan.why_columns == ("Department", "Id", "Name", "Salary", "Active")
        rows = [
            ["hr", 2, "Bob", 90000.0, True],
            ["eng", 1, "Alice", 120000.0, True],
            ["hr", 5, "Eve", 95000.0, True],
            ["eng", 3, "Charlie", 110000.0, False],
        ]
        picked = plan.shape_why(rows)
        assert picked == [1, 0]
        assert [plan.project_why(rows[i]) for i in picked] == [["eng"], ["hr"]]

    def test_projection_ordered_by_an_unselected_column_without_a_page(self):
        plan = compile_query_plan(
            _emp("name"),
            relations=[Employee],
            order_by=OrderedColumn(_emp("salary"), descending=True),
        )
        assert plan.program == "?employee(Id, Name, Department, Salary:desc, Active)"
        assert plan.dedupe

    def test_order_by_an_unjoined_relation_is_a_compile_error(self):
        with pytest.raises(CompileError):
            compile_query(Employee, order_by=_dept("budget"))


class TestCompileQueryOr:
    """An OR split is one program: a union rule the engine orders and pages."""

    def test_or_branches_union_in_one_program(self):
        cond = Or(
            Comparison("=", _emp("department"), Literal("hr")),
            Comparison(">", _emp("salary"), Literal(115000)),
        )
        plan = compile_query_plan(
            _emp("name"),
            _emp("salary"),
            relations=[Employee],
            where_condition=cond,
            order_by=OrderedColumn(_emp("salary"), descending=True),
            limit=3,
        )
        assert plan.program == (
            f'il_q(Name, Salary) <- {EMP_ATOM}, Department = "hr"\n'
            f"il_q(Name, Salary) <- {EMP_ATOM}, Salary > 115000\n"
            "?il_q(Name, Salary:desc), limit(3)"
        )
        assert plan.debug == f'?{EMP_ATOM}, Department = "hr"'

    def test_or_over_whole_rows_keeps_every_column(self):
        cond = Or(
            Comparison("=", _emp("department"), Literal("eng")),
            Comparison("=", _emp("department"), Literal("sales")),
        )
        plan = compile_query_plan(Employee, where_condition=cond)
        assert plan.program == (
            f'il_q(Id, Name, Department, Salary, Active) <- {EMP_ATOM}, Department = "eng"\n'
            f'il_q(Id, Name, Department, Salary, Active) <- {EMP_ATOM}, Department = "sales"\n'
            "?il_q(Id, Name, Department, Salary, Active)"
        )
        assert not plan.dedupe

    def test_or_in_a_join_condition_is_a_compile_error(self):
        with pytest.raises(CompileError):
            compile_query(
                Employee, Department,
                on_condition=Or(
                    Comparison("=", _emp("department"), _dept("name")),
                    Comparison("=", _emp("name"), _dept("name")),
                ),
            )


class TestCompileQueryAggregates:
    """R-AGG: a program-local rule and its query, fresh variable per position."""

    def test_two_measures_of_one_column(self):
        # Fix-report item 1: ?il_agg_x(Department, Salary, Salary) joined the
        # two measures as equal and dropped every group where they differ.
        plan = compile_query_plan(
            _emp("department"),
            AggExpr(func="avg", column=_emp("salary")),
            AggExpr(func="max", column=_emp("salary")),
            relations=[Employee],
        )
        assert plan.program == (
            f"il_q(Department, avg<Salary>, max<Salary>) <- {EMP_ATOM}\n"
            "?il_q(Department, AvgSalary, MaxSalary)"
        )
        assert plan.labels == ["department", "avg_salary", "max_salary"]

    def test_count_without_a_column_counts_the_first_variable(self):
        # Fix-report item 5: count<> is rejected by the engine.
        plan = compile_query_plan(AggExpr(func="count"), relations=[Employee])
        assert plan.program == f"il_q(count<Id>) <- {EMP_ATOM}\n?il_q(Count)"
        assert plan.labels == ["count"]

    def test_rule_and_query_are_one_program(self):
        # Fix-report item 2: no separate setup statement, nothing to clean up.
        program = compile_query(AggExpr(func="count", column=_emp("id")), relations=[Employee])
        assert program == f"il_q(count<Id>) <- {EMP_ATOM}\n?il_q(CountId)"

    def test_keyword_names_the_aggregate(self):
        plan = compile_query_plan(
            _emp("department"),
            relations=[Employee],
            computed={"n": AggExpr(func="count", column=_emp("id"))},
        )
        assert plan.program == f"il_q(Department, count<Id>) <- {EMP_ATOM}\n?il_q(Department, N)"
        assert plan.labels == ["department", "n"]

    def test_order_and_limit_on_the_query_atom(self):
        plan = compile_query_plan(
            _emp("department"),
            AggExpr(func="count", column=_emp("id")),
            relations=[Employee],
            order_by=OrderedColumn(_emp("department"), descending=True),
            limit=1,
        )
        assert plan.program == (
            f"il_q(Department, count<Id>) <- {EMP_ATOM}\n"
            "?il_q(Department:desc, CountId), limit(1)"
        )

    def test_order_by_an_unselected_column_is_a_compile_error(self):
        with pytest.raises(CompileError):
            compile_query(
                _emp("department"),
                AggExpr(func="count", column=_emp("id")),
                relations=[Employee],
                order_by=_emp("salary"),
            )

    def test_top_k_contributes_its_columns(self):
        plan = compile_query_plan(
            _emp("department"),
            AggExpr(
                func="top_k", params=(3,), passthrough=(_emp("name"),),
                order_column=_emp("salary"), desc=True,
            ),
            relations=[Employee],
        )
        assert plan.program == (
            f"il_q(Department, top_k<3, Name, Salary:desc>) <- {EMP_ATOM}\n"
            "?il_q(Department, Name, Salary)"
        )
        assert plan.labels == ["department", "name", "salary"]

    def test_or_aggregates_the_union(self):
        cond = Or(
            Comparison("=", _emp("department"), Literal("hr")),
            Comparison(">", _emp("salary"), Literal(100)),
        )
        plan = compile_query_plan(
            AggExpr(func="count", column=_emp("id")),
            relations=[Employee],
            where_condition=cond,
        )
        src = "il_q_src(Id, Name, Department, Salary, Active)"
        assert plan.program == (
            f'{src} <- {EMP_ATOM}, Department = "hr"\n'
            f"{src} <- {EMP_ATOM}, Salary > 100\n"
            f"il_q(count<Id>) <- {src}\n"
            "?il_q(CountId)"
        )


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

    def test_count_without_a_column_counts_the_first_column(self):
        result = compile_rule(
            "edge_count",
            ["n"],
            {"n": AggExpr(func="count")},
            [(Edge._resolve_name(), Edge, None)],
        )
        assert result == "+edge_count(count<Src>) <- edge(Src, _)"

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
        # The condition column must be BOUND in the body atom. Emitting
        # employee(Id, Name, _, _, _) alongside `Salary > 100000` leaves
        # Salary unbound - the engine accepts that and silently satisfies
        # the comparison, deriving wrong results.
        assert "employee(Id, Name, _, Salary, _)" in result

    def test_condition_only_column_is_bound(self):
        # Regression: a column referenced ONLY by the where-condition (not
        # selected into the head) must still be bound in its atom.
        cond = Comparison("=", AstColumn("employee", "department"), Literal("eng"))
        result = compile_rule(
            "eng_member",
            ["name"],
            {"name": AstColumn("employee", "name")},
            [(Employee._resolve_name(), Employee, None)],
            condition=cond,
            persistent=True,
        )
        assert result == (
            '+eng_member(Name) <- employee(_, Name, Department, _, _), Department = "eng"'
        )

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
