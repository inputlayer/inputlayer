"""Compiled text of R.any()/~R.any(), programs with .when() and claim().

The live tests in guards_live.py run the same forms against an engine.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from inputlayer import (
    CompileError,
    Derived,
    From,
    KnowledgeGraph,
    PreconditionFailed,
    Program,
    Relation,
    Vector,
)
from inputlayer._protocol import ResultResponse, StatementError
from inputlayer.compiler import (
    compile_conditional_delete,
    compile_guard,
    compile_query_plan,
    compile_rule,
)
from inputlayer.exceptions import Conflict, InternalError, StatementFailedError
from inputlayer.program import compile_claim, parse_write_message


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


class AttemptDone(Relation):
    attempt: str
    status: str


class CheckNeeded(Relation):
    order: str
    shipment: str


class UtteranceCursor(Relation):
    session: str
    last: int


class PackVersion(Relation):
    name: str
    version: str


class Doc(Relation):
    id: int
    emb: Vector[2]
    ok: bool
    score: float


class Late(Derived):
    order: str
    rules: ClassVar[list] = []


Late.rules = [From(Shipment).select(order=Shipment.order)]


def _rule(name: str, columns: list[str], clause: Any) -> str:
    return compile_rule(name, columns, clause.select_map, clause.relations, clause.condition)


# ── any() in rules ────────────────────────────────────────────────────


class TestAnyInRules:
    def test_hero_rule_negation_bound_to_a_joined_column_after_the_positive_atoms(self) -> None:
        clause = (
            From(Shipment, Eta, Promised, ToolPolicy)
            .where(
                lambda s, e, p, t: (e.shipment == s.shipment)
                & (p.order == s.order)
                & (e.due > p.due)
                & (t.tool == "carrier_check")
                & (t.mode == "auto")
                & ~KillSwitch.any(tool=t.tool)
            )
            .select(order=Shipment.order, shipment=Shipment.shipment)
        )
        assert _rule("check_needed", ["order", "shipment"], clause) == (
            "+check_needed(Order, Shipment) <- shipment(Order, Shipment), eta(Shipment, Due), "
            "promised(Order, Due_1), tool_policy(Tool, Mode), !kill_switch(Tool), "
            'Due > Due_1, Tool = "carrier_check", Mode = "auto"'
        )

    def test_shares_a_variable_with_an_equality_already_pinning_the_constant(self) -> None:
        clause = (
            From(ToolPolicy)
            .where(lambda t: (t.tool == "carrier_check") & ~KillSwitch.any(tool="carrier_check"))
            .select(tool=ToolPolicy.tool)
        )
        assert _rule("allowed", ["tool"], clause) == (
            '+allowed(Tool) <- tool_policy(Tool, _), !kill_switch(Tool), Tool = "carrier_check"'
        )

    def test_a_constant_only_negation_binds_through_a_persistent_constant_row(self) -> None:
        clause = (
            From(ToolPolicy)
            .where(~KillSwitch.any(tool="carrier_check"))
            .select(tool=ToolPolicy.tool)
        )
        assert _rule("allowed", ["tool"], clause) == (
            '+il_const_s("carrier_check")\n'
            "+allowed(Tool) <- tool_policy(Tool, _), il_const_s(K), !kill_switch(K), "
            'K = "carrier_check"'
        )

    def test_refuses_a_negation_bound_to_a_relation_the_body_does_not_join(self) -> None:
        clause = (
            From(ToolPolicy).where(~KillSwitch.any(tool=Attempt.tool)).select(tool=ToolPolicy.tool)
        )
        with pytest.raises(CompileError, match="no positive atom"):
            _rule("allowed", ["tool"], clause)

    def test_positive_any_is_an_existence_atom_with_wildcards(self) -> None:
        clause = (
            From(Shipment)
            .where(lambda s: Eta.any(shipment=s.shipment))
            .select(order=Shipment.order)
        )
        assert _rule("has_eta", ["order"], clause) == (
            "+has_eta(Order) <- shipment(Order, Shipment), eta(Shipment, _)"
        )

    def test_refuses_an_unknown_column(self) -> None:
        with pytest.raises(CompileError, match="no column nope"):
            KillSwitch.any(nope="x")

    def test_negated_any_in_a_conditional_delete(self) -> None:
        iql = compile_conditional_delete(Attempt, ~AttemptDone.any(attempt=Attempt.attempt))
        assert iql == "-attempt(X0, X1, X2) <- attempt(X0, X1, X2), !attempt_done(X2, _)"

    def test_derived_any(self) -> None:
        clause = (
            From(Shipment)
            .where(lambda s: ~Late.any(order=s.order))
            .select(order=Shipment.order)
        )
        assert _rule("not_late", ["order"], clause) == (
            "+not_late(Order) <- shipment(Order, _), !late(Order)"
        )


# ── any() in queries ──────────────────────────────────────────────────


class TestAnyInQueries:
    def test_constant_only_negation_binds_through_a_session_fact(self) -> None:
        plan = compile_query_plan(ToolPolicy, where_condition=~KillSwitch.any(tool="carrier_check"))
        assert plan.program == (
            'il_const_s("carrier_check")\n'
            '?tool_policy(Tool, Mode), il_const_s(K), K = "carrier_check", !kill_switch(K)'
        )
        assert plan.shape([["carrier_check", "auto", "carrier_check"]]) == [
            ["carrier_check", "auto"]
        ]

    def test_positive_any_adds_no_result_columns(self) -> None:
        plan = compile_query_plan(
            Shipment,
            where_condition=Eta.any(shipment=Shipment.shipment)
            & ~Attempt.any(order=Shipment.order),
        )
        assert plan.program == "?shipment(Order, Shipment), eta(Shipment, _), !attempt(Order, _, _)"
        assert plan.columns == ("Order", "Shipment")

    def test_any_of_a_joined_relation_gets_its_own_columns(self) -> None:
        plan = compile_query_plan(
            Attempt, where_condition=Attempt.any(order="ORD-2") & (Attempt.order == "ORD-1")
        )
        assert plan.program == (
            '?attempt(Order, Tool, Attempt), attempt("ORD-2", _, _), Order = "ORD-1"'
        )


# ── Guards ────────────────────────────────────────────────────────────


class TestGuard:
    def test_comparison_over_an_any_column_takes_a_variable(self) -> None:
        g = compile_guard([UtteranceCursor.any(session="s-42"), UtteranceCursor.last < 7])
        assert g.body == 'utterance_cursor("s-42", Last), Last < 7'
        assert g.const_rows == ()

    def test_refuses_or(self) -> None:
        with pytest.raises(CompileError, match="OR"):
            compile_guard([PackVersion.any(name="a") | PackVersion.any(name="b")])

    def test_refuses_a_negation_binding_no_column(self) -> None:
        with pytest.raises(CompileError, match="binds no column"):
            compile_guard([~KillSwitch.any()])

    def test_joins_two_any_atoms_on_an_equality(self) -> None:
        g = compile_guard(
            [Attempt.any(order="O"), CheckNeeded.any(), Attempt.order == CheckNeeded.order]
        )
        assert g.body == 'attempt(Order, _, _), check_needed(Order, _), Order = "O"'


# ── Programs ──────────────────────────────────────────────────────────


T = "t-1"


class TestProgram:
    def test_plain_statements_commit_as_one_program(self) -> None:
        p = (
            Program()
            .retract(Eta, shipment="S-77")
            .insert(Eta(shipment="S-77", due="2026-10-10"))
            .retract(Attempt, order="O", tool="x", attempt="a")
        )
        c = p.compile(True, T)
        assert c.iql == (
            '-eta("S-77", Due) <- eta("S-77", Due)\n'
            '+eta("S-77", "2026-10-10")\n'
            '-attempt("O", "x", "a")'
        )
        assert c.write_indexes == (0, 1, 2)
        assert c.token_index is None

    def test_when_strict_false_is_the_token_form(self) -> None:
        p = (
            Program()
            .retract(UtteranceCursor, session="s-42")
            .insert(UtteranceCursor(session="s-42", last=7))
            .when(UtteranceCursor.any(session="s-42"), UtteranceCursor.last < 7)
        )
        c = p.compile(False, T)
        assert c.iql.split("\n") == [
            '-il_txn(""), +il_txn("t-1") <- utterance_cursor("s-42", Last), Last < 7',
            '-utterance_cursor("s-42", Last) <- utterance_cursor("s-42", Last), il_txn("t-1")',
            '-il_ghost(0), +utterance_cursor("s-42", 7) <- il_txn("t-1")',
            '-il_txn("t-1") <- il_txn("t-1")',
        ]
        assert (c.token_index, c.assert_index, c.write_indexes) == (0, None, (1, 2))

    def test_when_strict_is_the_abort_form(self) -> None:
        p = (
            Program()
            .clear_rule("late")
            .define_rules(Late)
            .retract(PackVersion, name="delivery")
            .insert(PackVersion(name="delivery", version="v3"))
            .when(PackVersion.any(name="delivery", version="v2"))
        )
        c = p.compile(True, "t-d2")
        assert c.iql.split("\n") == [
            '+il_txn_pending("t-d2")',
            '-il_txn(""), +il_txn("t-d2") <- pack_version("delivery", "v2")',
            '-il_assert(0), +il_assert("precondition_failed:t-d2") <- '
            'il_txn_pending(K), K = "t-d2", !il_txn(K)',
            ".rule clear late",
            "+late(Order) <- shipment(Order, _)",
            '-pack_version("delivery", Version) <- '
            'pack_version("delivery", Version), il_txn("t-d2")',
            '-il_ghost(0), +pack_version("delivery", "v3") <- il_txn("t-d2")',
            '-il_txn("t-d2") <- il_txn("t-d2")',
            '-il_txn_pending("t-d2")',
        ]
        assert (c.token_index, c.assert_index, c.write_indexes) == (1, 2, (5, 6))

    def test_rule_constant_rows_count_as_their_own_statements(self) -> None:
        class Allowed(Derived):
            tool: str
            rules: ClassVar[list] = []

        Allowed.rules = [
            From(ToolPolicy).where(~KillSwitch.any(tool="t")).select(tool=ToolPolicy.tool)
        ]
        p = Program().define_rules(Allowed).insert(KillSwitch(tool="x"))
        c = p.compile(True, T)
        lines = c.iql.split("\n")
        assert lines[0] == '+il_const_s("t")'
        assert c.write_indexes == (2,)
        assert lines[2] == '+kill_switch("x")'

        g = p.when(PackVersion.any(name="delivery")).compile(True, T)
        glines = g.iql.split("\n")
        assert g.write_indexes == (5,)
        assert glines[3] == '+il_const_s("t")'
        assert glines[5] == '-il_ghost(0), +kill_switch("x") <- il_txn("t-1")'

    def test_carries_a_negation_constant_through_a_positive_atom_carrying_it(self) -> None:
        p = (
            Program()
            .insert(AttemptDone(attempt="att-9f3", status="ok"))
            .when(Attempt.any(attempt="att-9f3"), ~AttemptDone.any(attempt="att-9f3"))
        )
        assert p.compile(False, T).iql.split("\n")[0] == (
            '-il_txn(""), +il_txn("t-1") <- attempt(_, _, Attempt), '
            '!attempt_done(Attempt, _), Attempt = "att-9f3"'
        )

    def test_negation_only_guard_binds_through_a_staged_il_txn_const_row(self) -> None:
        p = (
            Program()
            .insert(AttemptDone(attempt="att-y", status="ok"))
            .when(~AttemptDone.any(attempt="att-y"))
        )
        c = p.compile(True, T)
        assert c.iql.split("\n") == [
            '+il_txn_const_s("att-y")',
            '+il_txn_pending("t-1")',
            '-il_txn(""), +il_txn("t-1") <- il_txn_const_s(K), !attempt_done(K, _), K = "att-y"',
            '-il_assert(0), +il_assert("precondition_failed:t-1") <- '
            'il_txn_pending(K), K = "t-1", !il_txn(K)',
            '-il_ghost(0), +attempt_done("att-y", "ok") <- il_txn("t-1")',
            '-il_txn("t-1") <- il_txn("t-1")',
            '-il_txn_pending("t-1")',
            '-il_txn_const_s("att-y")',
        ]
        assert (c.token_index, c.assert_index, c.write_indexes) == (2, 3, (4,))

    def test_guarded_insert_anchors_on_the_sdk_ghost_row(self) -> None:
        doc = Doc(id=-1, emb=[0.5, 1.5], ok=False, score=2.5)
        p = Program().insert(doc).when(PackVersion.any())
        assert p.compile(False, T).iql.split("\n")[1] == (
            '-il_ghost(0), +doc(-1, [0.5, 1.5], false, 2.5) <- il_txn("t-1")'
        )

    def test_refuses_strict_false_with_rules_or_schema(self) -> None:
        p = Program().define(Eta).when(PackVersion.any(name="x"))
        with pytest.raises(CompileError, match="strict=False"):
            p.compile(False, T)

    def test_refuses_a_guard_condition_over_an_unbound_relation(self) -> None:
        p = Program().insert(Eta(shipment="S", due="d")).when(UtteranceCursor.last < 7)
        with pytest.raises(CompileError, match="no positive any"):
            p.compile(True, T)

    def test_refuses_a_retract_naming_no_column(self) -> None:
        with pytest.raises(CompileError, match="every row"):
            Program().retract(Eta).compile(True, T)

    def test_refuses_an_unknown_retract_column(self) -> None:
        with pytest.raises(CompileError, match="no column nope"):
            Program().retract(Eta, nope="x").compile(True, T)

    def test_refuses_an_empty_program(self) -> None:
        with pytest.raises(CompileError):
            Program().compile(True, T)

    def test_each_program_has_its_own_token(self) -> None:
        p = Program().insert(Eta(shipment="S", due="d")).when(PackVersion.any())
        assert p.iql() != p.iql()


# ── Claims ────────────────────────────────────────────────────────────


class TestClaim:
    def test_hero_claim_is_a_guarded_insert_then_a_query_on_the_key(self) -> None:
        c = compile_claim(
            Attempt(order="ORD-1", tool="carrier_check", attempt="a1"),
            when=[CheckNeeded.any(order="ORD-1")],
            unless=Attempt.any(order="ORD-1", tool="carrier_check"),
        )
        assert c.key == ("order", "tool")
        assert c.iql.split("\n") == [
            '-il_ghost(0), +attempt("ORD-1", "carrier_check", "a1") <- check_needed(Order, _), '
            '!attempt(Order, "carrier_check", _), Order = "ORD-1"',
            '?attempt("ORD-1", "carrier_check", Attempt)',
        ]

    def test_derives_unless_from_key_and_binds_a_lone_negation_through_il_txn_const(self) -> None:
        c = compile_claim(Attempt(order="ORD-2", tool="t", attempt="b"), key=[Attempt.order])
        assert c.iql.split("\n") == [
            '+il_txn_const_s("ORD-2")',
            '-il_ghost(0), +attempt("ORD-2", "t", "b") <- '
            'il_txn_const_s(K), !attempt(K, _, _), K = "ORD-2"',
            '-il_txn_const_s("ORD-2")',
            '?attempt("ORD-2", Tool, Attempt)',
        ]

    def test_without_a_guard_is_a_plain_insert_read_back_by_every_column(self) -> None:
        assert compile_claim(KillSwitch(tool="x")).iql == '+kill_switch("x")\n?kill_switch("x")'

    def test_refuses_an_unknown_key_column(self) -> None:
        with pytest.raises(CompileError, match="no column nope"):
            compile_claim(KillSwitch(tool="x"), key=["nope"])


# ── Reply grammar ─────────────────────────────────────────────────────


class TestWriteReplyGrammar:
    def test_reads_every_count_reply(self) -> None:
        def counts(m: str) -> tuple[int, int]:
            c = parse_write_message(m)
            return c.inserted, c.deleted

        assert counts("Inserted 2 fact(s) into 'eta'.") == (2, 0)
        assert counts("Update: 1 deleted, 3 inserted.") == (3, 1)
        assert counts("Conditional delete: 4 fact(s) deleted from 'eta'.") == (0, 4)
        assert counts("Deleted 5 facts from 'eta'.") == (0, 5)

    def test_refuses_an_unknown_reply(self) -> None:
        with pytest.raises(InternalError, match="Unexpected write reply"):
            parse_write_message("Rule registered")


# ── Reading the engine's reply ────────────────────────────────────────


class _Conn:
    """A connection that records programs and answers from a script."""

    def __init__(self, *replies: ResultResponse | Exception) -> None:
        self.sent: list[str] = []
        self._replies = list(replies)

    async def execute(self, iql: str, *, timeout: float | None = None) -> ResultResponse:
        self.sent.append(iql)
        reply = self._replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


def _reply(
    rows: list[list[Any]] | None = None, errors: list[StatementError] | None = None
) -> ResultResponse:
    rows = rows or []
    return ResultResponse(
        columns=[],
        rows=rows,
        row_count=len(rows),
        total_count=len(rows),
        truncated=False,
        execution_time_ms=0,
        errors=errors,
    )


def _rows(*messages: str) -> ResultResponse:
    return _reply([[m] for m in messages])


def _failed(index: int, code: Any) -> StatementFailedError:
    reply = _reply(errors=[StatementError(index=index, code=code, message="m")])
    assert reply.errors is not None
    return StatementFailedError(reply.errors, reply)


def _kg(conn: _Conn) -> KnowledgeGraph:
    return KnowledgeGraph("kg", conn)  # type: ignore[arg-type]


class TestCommit:
    async def test_define_declares_the_guard_relations_in_the_same_program(self) -> None:
        conn = _Conn(_rows())
        await _kg(conn).define(Eta)
        assert conn.sent == [
            "+eta(shipment: string, due: string)\n"
            "+il_txn(id: string)\n+il_txn_pending(id: string)\n+il_assert(v: int)"
        ]

    async def test_token_form_reads_applied_and_counts(self) -> None:
        conn = _Conn(
            _rows(),  # guard relations
            _rows(
                "Update: 0 deleted, 1 inserted.",
                "Conditional delete: 1 fact(s) deleted from 'utterance_cursor'.",
                "Update: 0 deleted, 1 inserted.",
                "Conditional delete: 1 fact(s) deleted from 'il_txn'.",
            ),
        )
        kg = _kg(conn)
        r = await (
            kg.program()
            .retract(UtteranceCursor, session="s-42")
            .insert(UtteranceCursor(session="s-42", last=7))
            .when(UtteranceCursor.any(session="s-42"))
            .commit(strict=False)
        )
        assert (r.applied, r.inserted, r.deleted) == (True, 1, 1)
        assert conn.sent[0].startswith("+il_txn(id: string)")

    async def test_token_form_not_applied(self) -> None:
        conn = _Conn(
            _rows(),
            _rows(
                "Update: 0 deleted, 0 inserted.",
                "Update: 0 deleted, 0 inserted.",
                "Conditional delete: 0 fact(s) deleted from 'il_txn'.",
            ),
        )
        r = await (
            _kg(conn)
            .program()
            .insert(KillSwitch(tool="x"))
            .when(PackVersion.any())
            .commit(strict=False)
        )
        assert (r.applied, r.inserted, r.deleted) == (False, 0, 0)

    async def test_abort_form_raises_precondition_failed_from_the_assertion_index(self) -> None:
        conn = _Conn(_rows(), _failed(2, "validation"))
        with pytest.raises(PreconditionFailed) as info:
            await _kg(conn).program().insert(KillSwitch(tool="x")).when(PackVersion.any()).commit()
        assert info.value.iql == conn.sent[1]

    async def test_another_statement_failing_is_not_a_precondition(self) -> None:
        conn = _Conn(_rows(), _failed(4, "validation"))
        with pytest.raises(StatementFailedError) as info:
            await _kg(conn).program().insert(KillSwitch(tool="x")).when(PackVersion.any()).commit()
        assert not isinstance(info.value, PreconditionFailed)

    async def test_conflict_is_typed(self) -> None:
        conn = _Conn(_rows(), _failed(1, "conflict"))
        with pytest.raises(Conflict):
            await _kg(conn).program().insert(KillSwitch(tool="x")).when(PackVersion.any()).commit()

    async def test_claim_reads_the_holder(self) -> None:
        row = Attempt(order="O", tool="t", attempt="a-2")
        held = _reply([["O", "t", "a-1"]])
        c = await _kg(_Conn(held)).claim(row, key=["order"])
        assert (c.won, c.holder) == (False, Attempt(order="O", tool="t", attempt="a-1"))
        won = _reply([["O", "t", "a-2"]])
        c = await _kg(_Conn(won)).claim(row, key=["order"])
        assert (c.won, c.holder) == (True, row)
        none = _reply()
        c = await _kg(_Conn(none)).claim(row, key=["order"], when=[PackVersion.any()])
        assert (c.won, c.holder) == (False, None)

    async def test_retract_counts_deleted(self) -> None:
        conn = _Conn(_rows("Conditional delete: 2 fact(s) deleted from 'eta'."))
        r = await _kg(conn).retract(Eta, shipment="S-77")
        assert r.count == 2
        assert conn.sent == ['-eta("S-77", Due) <- eta("S-77", Due)']
