"""Compiled text for R-TYPE, R-IN, R-NEG and R-LIT (SDK design, section 9).

Each form here is also run against a live engine in
``tests/compile_rules_live.py``.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pytest

from inputlayer import From, Relation, Timestamp
from inputlayer._ast import Literal, MatchExpr
from inputlayer._literal import I64_MAX, I64_MIN, encode, ms_to_datetime
from inputlayer._proxy import ColumnProxy
from inputlayer.compiler import (
    compile_conditional_delete,
    compile_insert,
    compile_query_plan,
    compile_rule,
    compile_rule_clause,
    compile_schema,
)
from inputlayer.exceptions import CompileError
from inputlayer.migrations.operations import CreateRelation
from inputlayer.migrations.state import ModelState
from inputlayer.result import ResultSet


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


EMP = "employee(Id, Name, Department)"


def _query(*select, where=None, relations=None, **kw):
    select = tuple(s._to_ast() if isinstance(s, ColumnProxy) else s for s in select)
    return compile_query_plan(
        *select, relations=relations or [Employee], where_condition=where, **kw
    )


# ── R-TYPE ────────────────────────────────────────────────────────────


class TestTimestamps:
    def test_datetime_and_timestamp_columns_declare_int(self) -> None:
        assert compile_schema(Reading) == "+reading(sensor: string, at: int, seen: int)"

    def test_migrations_declare_int_too(self) -> None:
        state = ModelState.from_models(relations=[Reading])
        (forward,) = CreateRelation("reading", state.relations["reading"]).forward_commands()
        assert forward == "+reading(sensor: string, at: int, seen: int)"

    def test_values_are_unix_milliseconds(self) -> None:
        at = datetime(2026, 10, 4, 12, 0, 0, 123000, tzinfo=timezone.utc)
        row = Reading(sensor="s", at=at, seen=Timestamp(5))
        assert compile_insert(row) == '+reading("s", 1791115200123, 5)'

    def test_naive_datetime_is_utc(self) -> None:
        assert encode(datetime(1970, 1, 1, 0, 0, 1)) == "1000"

    def test_aware_datetime_in_another_zone(self) -> None:
        cet = timezone(timedelta(hours=2))
        assert encode(datetime(1970, 1, 1, 2, 0, 1, tzinfo=cet)) == "1000"

    def test_condition_on_a_datetime_column(self) -> None:
        since = datetime(2026, 1, 1, tzinfo=timezone.utc)
        plan = _query(Reading, relations=[Reading], where=Reading.at > since)
        assert plan.program == "?reading(Sensor, At, Seen), At > 1767225600000"

    def test_typed_rows_convert_milliseconds_back(self) -> None:
        rs = ResultSet(
            columns=["sensor", "at", "seen"], rows=[["s", 1000, 7]], _relation_cls=Reading
        )
        row = rs.first()
        assert row.at == datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        assert row.seen == 7

    def test_ms_to_datetime_is_exact(self) -> None:
        assert ms_to_datetime(1791115200123).microsecond == 123000


# ── R-IN ──────────────────────────────────────────────────────────────


class TestIn:
    def test_in_is_an_atom_of_the_target_relation(self) -> None:
        plan = _query(Employee.name, where=Employee.id.in_(Manager.employee_id))
        assert plan.program == f"?{EMP}, manager(_, Id)"

    def test_not_in_is_the_negated_atom(self) -> None:
        plan = _query(Employee.name, where=~Employee.id.in_(Manager.employee_id))
        assert plan.program == f"?{EMP}, !manager(_, Id)"

    def test_negating_an_in_condition(self) -> None:
        cond = ~(Employee.id.in_(Manager.employee_id) & (Employee.department == "eng"))
        plan = _query(Employee.name, where=cond & (Employee.name != "x"))
        # De Morgan: (!in | dept != eng) & name != x, split into two branches.
        assert "!manager(_, Id)" in plan.program
        assert 'Department != "eng"' in plan.program

    def test_in_from_a_where_callback_column(self) -> None:
        clause = (
            From(Employee).where(lambda e: e.id.in_(Manager.employee_id)).select(name=Employee.name)
        )
        assert (
            compile_rule("in_team", ["name"], clause.select_map, clause.relations, clause.condition)
            == "+in_team(Name) <- employee(Id, Name, _), manager(_, Id)"
        )

    def test_in_to_a_relation_ref_column(self) -> None:
        (m,) = Manager.refs(1)
        plan = _query(Employee.name, where=Employee.id.in_(m.employee_id))
        assert plan.program == f"?{EMP}, manager(_, Id)"

    def test_in_conditional_delete(self) -> None:
        iql = compile_conditional_delete(Employee, ~Employee.id.in_(Manager.employee_id))
        assert iql == "-employee(X0, X1, X2) <- employee(X0, X1, X2), !manager(_, X0)"

    def test_in_needs_a_declared_relation(self) -> None:
        loose = ColumnProxy("manager", "employee_id")
        with pytest.raises(CompileError, match="declared relation"):
            _query(Employee.name, where=Employee.id.in_(loose))


# ── R-NEG ─────────────────────────────────────────────────────────────


def _not_killed(tool: str) -> MatchExpr:
    return MatchExpr("kill_switch", {"tool": Literal(tool)}, negated=True, columns=("tool",))


class TestNegation:
    def test_negated_atom_sharing_a_variable_is_sent_as_is(self) -> None:
        cond = ~ToolPolicy.tool.matches(KillSwitch, on={"tool": "tool"})
        plan = _query(ToolPolicy, relations=[ToolPolicy], where=cond)
        assert plan.program == "?tool_policy(Tool, Mode), !kill_switch(Tool)"
        assert plan.setup == ()

    def test_matches_fills_unbound_columns(self) -> None:
        cond = ~Employee.id.matches(Manager, on={"employee_id": "id"})
        plan = _query(Employee.name, where=cond)
        assert plan.program == f"?{EMP}, !manager(_, Id)"

    def test_negated_atom_sharing_nothing_is_refused(self) -> None:
        with pytest.raises(CompileError, match="shares no variable"):
            _query(Employee.name, where=~Manager.employee_id.in_(Manager.employee_id))

    def test_ground_negation_binds_a_program_local_constant(self) -> None:
        # T-B2: the session fact lasts only for the request.
        plan = _query(ToolPolicy, relations=[ToolPolicy], where=_not_killed("refund"))
        assert plan.program == (
            'il_const_s("refund")\n'
            '?tool_policy(Tool, Mode), il_const_s(K), K = "refund", !kill_switch(K)'
        )
        assert plan.setup == ('il_const_s("refund")',)
        assert plan.columns == ("Tool", "Mode", "K")
        assert plan.shape([["refund", "confirm", "refund"]]) == [["refund", "confirm"]]

    def test_constant_types_pick_their_relation(self) -> None:
        for value, rel in [(3, "il_const_i"), (1.5, "il_const_f"), (True, "il_const_b")]:
            cond = MatchExpr("kill_switch", {"tool": Literal(value)}, negated=True)
            plan = _query(ToolPolicy, relations=[ToolPolicy], where=cond)
            assert plan.setup == (f"{rel}({encode(value)})",)

    def test_aggregate_query_binds_the_constant_too(self) -> None:
        from inputlayer import count

        plan = _query(
            ToolPolicy.mode,
            count(ToolPolicy.tool),
            relations=[ToolPolicy],
            where=_not_killed("refund"),
        )
        assert plan.program.startswith('il_const_s("refund")\nil_q(Mode, count<Tool>) <- ')
        assert 'il_const_s(K), K = "refund", !kill_switch(K)' in plan.program

    def test_view_binds_a_persistent_constant_in_canonical_order(self) -> None:
        compiled = compile_rule_clause(
            "allowed",
            ["tool"],
            {"tool": ToolPolicy.tool._to_ast()},
            [("tool_policy", ToolPolicy, None)],
            (ToolPolicy.mode == "auto") & _not_killed("refund"),
        )
        assert compiled.clause == (
            "+allowed(Tool) <- tool_policy(Tool, Mode), il_const_s(K), "
            '!kill_switch(K), Mode = "auto", K = "refund"'
        )
        assert compiled.constants == ('+il_const_s("refund")',)

    def test_compile_rule_sends_the_constant_row_with_the_clause(self) -> None:
        iql = compile_rule(
            "allowed",
            ["tool"],
            {"tool": ToolPolicy.tool._to_ast()},
            [("tool_policy", ToolPolicy, None)],
            _not_killed("refund"),
            persistent=False,
        )
        assert iql.split("\n") == [
            'il_const_s("refund")',
            'allowed(Tool) <- tool_policy(Tool, _), il_const_s(K), !kill_switch(K), K = "refund"',
        ]

    def test_view_orders_negated_atoms_before_comparisons(self) -> None:
        cond = (Employee.department != "hr") & ~Employee.id.in_(Manager.employee_id)
        iql = compile_rule(
            "loners",
            ["name"],
            {"name": Employee.name._to_ast()},
            [("employee", Employee, None)],
            cond,
        )
        assert iql == (
            '+loners(Name) <- employee(Id, Name, Department), !manager(_, Id), Department != "hr"'
        )

    def test_conditional_delete_cannot_bind_a_constant(self) -> None:
        with pytest.raises(CompileError, match="conditional delete"):
            compile_conditional_delete(
                Employee, MatchExpr("manager", {"team": Literal("x")}, negated=True)
            )

    def test_negated_comparisons_flip(self) -> None:
        cond = ~((Employee.id < 3) | (Employee.department == "hr"))
        plan = _query(Employee.name, where=cond)
        assert plan.program == f'?{EMP}, Id >= 3, Department != "hr"'


# ── R-LIT ─────────────────────────────────────────────────────────────


class TestLiterals:
    @pytest.mark.parametrize(
        ("value", "text"),
        [
            ("plain", '"plain"'),
            ('q"\\', '"q\\"\\\\"'),
            ("a\nb\tc\rd", '"a\\nb\\tc\\rd"'),
            ("nul\x00bell\x07", '"nul\x00bell\x07"'),
            ("\u2028\xe9\U0001f600", '"\u2028\xe9\U0001f600"'),
            ("", '""'),
            (True, "true"),
            (False, "false"),
            (0, "0"),
            (I64_MIN, str(I64_MIN)),
            (I64_MAX, str(I64_MAX)),
            (-0.0, "-0.0"),
            (1e20, "1e+20"),
            (5e-324, "5e-324"),
            (0.1, "0.1"),
            ([1, 2.5], "[1.0, 2.5]"),
            ([], "[]"),
        ],
    )
    def test_encodes(self, value, text) -> None:
        assert encode(value) == text

    @pytest.mark.parametrize(
        "value",
        [
            math.nan,
            math.inf,
            -math.inf,
            None,
            I64_MAX + 1,
            I64_MIN - 1,
            "\ud800",
            {"a": 1},
            ["x"],
            [True],
            [math.nan],
        ],
    )
    def test_refuses(self, value) -> None:
        with pytest.raises(CompileError):
            encode(value)

    def test_nan_is_refused_in_a_condition(self) -> None:
        with pytest.raises(CompileError, match="infinity or NaN"):
            _query(Employee.name, where=Employee.id > math.nan)
