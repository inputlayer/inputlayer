"""Write programs: batched statements, guards and claims, compiled to IQL.

A program is one request and one transaction. ``.when()`` makes it
conditional through a transaction token (R-GUARD): the guard is evaluated
once into an ``il_txn`` row, every fact write is conditioned on that row,
and the row is removed last, so the program applies whole or not at all.
The single-head ``+r(...) <- body`` form is never emitted for a fact write:
the engine registers it as a rule. A guarded insert is the update form
with a ghost delete, ``-il_ghost(0), +r(...) <- body``.

Every function here is pure; ``KnowledgeGraph`` runs what they return.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from inputlayer._ast import BoolExpr, MatchExpr
from inputlayer._literal import encode as encode_literal
from inputlayer._naming import column_to_variable
from inputlayer._proxy import ColumnProxy
from inputlayer.compiler import (
    compile_bulk_insert,
    compile_guard,
    compile_insert,
    compile_rule_clause,
    compile_schema,
)
from inputlayer.exceptions import CompileError, InternalError
from inputlayer.relation import Relation

if TYPE_CHECKING:
    from inputlayer.derived import Derived

#: Token relation: one row while a guarded program runs, none after.
TXN = "il_txn"
#: Abort form: the token id, staged before the guard.
TXN_PENDING = "il_txn_pending"
#: Abort form: an int relation the assertion inserts a string into.
ASSERT = "il_assert"

#: Schemas of the SDK's guard relations, declared by ``kg.define()``.
GUARD_SCHEMAS = (f"+{TXN}(id: string)", f"+{TXN_PENDING}(id: string)", f"+{ASSERT}(v: int)")

#: Delete anchor of a guarded insert: a row of an SDK-owned relation nothing writes.
_GHOST = "-il_ghost(0)"

R = TypeVar("R", bound=Relation)


# ── Statements ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Insert:
    rel: type[Relation]
    facts: tuple[Relation, ...]


@dataclass(frozen=True)
class _Retract:
    rel: type[Relation]
    fact: Relation


@dataclass(frozen=True)
class _RetractKey:
    rel: type[Relation]
    key: dict[str, Any]


@dataclass(frozen=True)
class _Plain:
    """A rule, schema or ``.rule clear`` statement: unconditional."""

    text: str


_Statement = _Insert | _Retract | _RetractKey | _Plain


def _row_values(fact: Relation) -> list[str]:
    return [encode_literal(getattr(fact, c)) for c in Relation._get_columns(type(fact))]


def _column_vars(rel: type[Relation]) -> list[str]:
    """Variables for a relation's columns, distinct from each other."""
    used: set[str] = set()
    out: list[str] = []
    for c in Relation._get_columns(rel):
        base = column_to_variable(c)
        var, n = base, 2
        while var in used:
            var, n = f"{base}_{n}", n + 1
        used.add(var)
        out.append(var)
    return out


def _check_columns(rel: type[Relation], names: Sequence[str], what: str) -> None:
    columns = Relation._get_columns(rel)
    for name in names:
        if name not in columns:
            raise CompileError(
                f"{what}: relation {Relation._resolve_name(rel)} has no column {name}",
                hint=f"its columns are {', '.join(columns)}",
            )


def _keyed_delete(rel: type[Relation], key: dict[str, Any], extra: list[str]) -> str:
    """``-r(k, V..) <- r(k, V..)`` for a partial key: every row with those values."""
    name = Relation._resolve_name(rel)
    if not key:
        raise CompileError(
            f"retract() on {name} names no column, so it would remove every row",
            hint="give the columns of the rows to remove; kg.delete(where=) removes by condition",
        )
    _check_columns(rel, list(key), "retract()")
    args = [
        encode_literal(key[c]) if c in key else v
        for c, v in zip(Relation._get_columns(rel), _column_vars(rel), strict=True)
    ]
    atom = f"{name}({', '.join(args)})"
    return f"-{atom} <- {', '.join([atom, *extra])}"


def _write_statements(s: _Statement, cond: str | None = None) -> list[str]:
    """Fact-write statements of one statement, conditioned on *cond* when given."""
    if isinstance(s, _Insert):
        if cond is None:
            if len(s.facts) == 1:
                return [compile_insert(s.facts[0])]
            return [compile_bulk_insert(s.rel, list(s.facts))]
        name = Relation._resolve_name(s.rel)
        return [f"{_GHOST}, +{name}({', '.join(_row_values(f))}) <- {cond}" for f in s.facts]
    if isinstance(s, _Retract):
        atom = f"{Relation._resolve_name(s.rel)}({', '.join(_row_values(s.fact))})"
        return [f"-{atom}" if cond is None else f"-{atom} <- {atom}, {cond}"]
    if isinstance(s, _RetractKey):
        columns = Relation._get_columns(s.rel)
        if s.key and set(s.key) == set(columns):
            # Every column given: the row itself.
            values = ", ".join(encode_literal(s.key[c]) for c in columns)
            atom = f"{Relation._resolve_name(s.rel)}({values})"
            return [f"-{atom}" if cond is None else f"-{atom} <- {atom}, {cond}"]
        return [_keyed_delete(s.rel, s.key, [] if cond is None else [cond])]
    raise InternalError(f"not a fact write: {s!r}")


def new_token_id() -> str:
    """A token id unique to one program."""
    return f"t-{uuid.uuid4()}"


# ── Programs ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CompiledProgram:
    """A compiled program and how to read its reply."""

    iql: str
    #: Index of the token statement, whose insert count says whether the guard held.
    token_index: int | None = None
    #: Index of the abort form's assertion: the statement that fails when the
    #: guard does not hold.
    assert_index: int | None = None
    #: Indexes of the caller's fact writes, whose counts make ``inserted``/``deleted``.
    write_indexes: tuple[int, ...] = ()


@dataclass(frozen=True)
class ProgramResult:
    """What a committed program did."""

    #: False only for a guarded ``strict=False`` program whose guard did not hold.
    applied: bool
    #: Facts newly stored by the program's writes.
    inserted: int
    #: Facts removed by the program's writes.
    deleted: int
    #: The program that was sent.
    iql: str


class Program:
    """A batch of writes committed as one request and one transaction.

    ``kg.program()`` builds one; methods chain::

        result = await (
            kg.program()
            .retract(CarrierNote, shipment="S-77")
            .insert(CarrierNote(shipment="S-77", reason="weather_delay"))
            .when(Attempt.any(attempt="att-9f3"), ~AttemptDone.any(attempt="att-9f3"))
            .commit()
        )
    """

    def __init__(
        self, runner: Callable[[Program, bool], Awaitable[ProgramResult]] | None = None
    ) -> None:
        self._runner = runner
        self._statements: list[_Statement] = []
        self._guards: list[BoolExpr] = []

    async def commit(self, *, strict: bool = True) -> ProgramResult:
        """Send the program as one request and one transaction.

        With a guard and ``strict=True`` (the default), a guard that does
        not hold raises ``PreconditionFailed`` and nothing is applied, rules
        and schema included. With ``strict=False`` it returns
        ``applied=False`` instead; that form cannot hold rules or schema.
        """
        if self._runner is None:
            raise InternalError(
                "This program is not bound to a knowledge graph; build it with kg.program()"
            )
        return await self._runner(self, strict)

    def insert(self, *facts: Relation | Sequence[Relation]) -> Program:
        """Insert facts (instances, or lists of them)."""
        flat: list[Relation] = []
        for f in facts:
            if isinstance(f, Relation):
                flat.append(f)
            else:
                flat.extend(f)
        by_rel: dict[type[Relation], list[Relation]] = {}
        for f in flat:
            if not isinstance(f, Relation):
                raise TypeError(f"insert() takes Relation instances, got {type(f).__name__}")
            by_rel.setdefault(type(f), []).append(f)
        for rel, group in by_rel.items():
            self._statements.append(_Insert(rel, tuple(group)))
        return self

    def retract(self, row_or_relation: Relation | type[Relation], **key: Any) -> Program:
        """Retract a row, or every row of a relation matching the given columns:
        ``retract(shipment_row)``, ``retract(Eta, shipment="S-77")``."""
        if isinstance(row_or_relation, Relation):
            if key:
                raise CompileError(
                    "retract(row) takes no column values",
                    hint="pass a row, or a relation class and column values",
                )
            self._statements.append(_Retract(type(row_or_relation), row_or_relation))
            return self
        rel = row_or_relation
        if not (isinstance(rel, type) and issubclass(rel, Relation)):
            raise TypeError(f"retract() takes a row or a Relation class, got {type(rel).__name__}")
        self._statements.append(_RetractKey(rel, {k: v for k, v in key.items() if v is not None}))
        return self

    def define(self, *relations: type[Relation]) -> Program:
        """Declare relation schemas in the program (unconditional: needs the abort form)."""
        self._statements.extend(_Plain(compile_schema(r)) for r in relations)
        return self

    def define_rules(self, *targets: type[Derived]) -> Program:
        """Add persistent rule clauses in the program (unconditional: needs the abort form)."""
        for target in targets:
            for clause in target.rules:
                compiled = compile_rule_clause(
                    Relation._resolve_name(target),
                    Relation._get_columns(target),
                    clause.select_map,
                    clause.relations,
                    clause.condition,
                    persistent=True,
                )
                self._statements.extend(
                    _Plain(text) for text in [*compiled.constants, compiled.clause]
                )
        return self

    def clear_rule(self, name: str | type) -> Program:
        """Remove every clause of an existing rule (unconditional: needs the abort form)."""
        from inputlayer import _meta

        if isinstance(name, type):
            name = Relation._resolve_name(name)
        self._statements.append(_Plain(_meta.rule_clear(name)))
        return self

    def when(self, *conditions: BoolExpr) -> Program:
        """Make the whole program conditional: it applies only when every
        condition holds at commit. Conditions are ``R.any()``/``~R.any()``
        atoms and comparisons over their columns; repeated calls add
        conditions."""
        for c in conditions:
            if not isinstance(c, BoolExpr):
                raise TypeError(f"when() takes conditions, got {type(c).__name__}")
        self._guards.extend(conditions)
        return self

    @property
    def has_rules_or_schema(self) -> bool:
        """True when the program holds statements a token cannot condition."""
        return any(isinstance(s, _Plain) for s in self._statements)

    @property
    def guarded(self) -> bool:
        return bool(self._guards)

    def iql(self, *, strict: bool = True) -> str:
        """The IQL this program sends (with a fresh token id each call)."""
        return self.compile(strict).iql

    def compile(self, strict: bool, token: str | None = None) -> CompiledProgram:
        """Compile to IQL; *token* fixes the token id for tests."""
        if not self._statements:
            raise CompileError(
                "The program holds no statement", hint="add insert(), retract() or define() calls"
            )
        if not self.guarded:
            lines: list[str] = []
            writes: list[int] = []
            for s in self._statements:
                if isinstance(s, _Plain):
                    lines.append(s.text)
                    continue
                for line in _write_statements(s):
                    writes.append(len(lines))
                    lines.append(line)
            return CompiledProgram("\n".join(lines), write_indexes=tuple(writes))

        if not strict and self.has_rules_or_schema:
            raise CompileError(
                "strict=False cannot guard a program holding rules or schema: a guard can "
                "only stop them by aborting the program",
                hint="commit it with strict=True (the default)",
            )
        guard = compile_guard(self._guards)
        if not guard.body:
            raise CompileError(
                "The guard has no condition", hint="pass R.any()/~R.any() conditions to when()"
            )
        token = token or new_token_id()
        tok = encode_literal(token)
        token_atom = f"{TXN}({tok})"
        lines = [f"+{row}" for row in guard.const_rows]
        writes = []
        if strict:
            lines.append(f"+{TXN_PENDING}({tok})")
        token_index = len(lines)
        lines.append(f'-{TXN}(""), +{token_atom} <- {guard.body}')
        assert_index: int | None = None
        if strict:
            # When the token is absent this inserts a string into an int
            # relation: the engine rejects the statement and rolls the whole
            # program back, rules and schema included.
            assert_index = len(lines)
            lines.append(
                f"-{ASSERT}(0), +{ASSERT}({encode_literal(f'precondition_failed:{token}')}) <- "
                f"{TXN_PENDING}(K), K = {tok}, !{TXN}(K)"
            )
        for s in self._statements:
            if isinstance(s, _Plain):
                lines.append(s.text)
                continue
            for line in _write_statements(s, token_atom):
                writes.append(len(lines))
                lines.append(line)
        lines.append(f"-{token_atom} <- {token_atom}")
        if strict:
            lines.append(f"-{TXN_PENDING}({tok})")
        lines.extend(f"-{row}" for row in guard.const_rows)
        return CompiledProgram(
            "\n".join(lines),
            token_index=token_index,
            assert_index=assert_index,
            write_indexes=tuple(writes),
        )


# ── Claims ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Claim(Generic[R]):
    """Outcome of ``kg.claim()``."""

    #: True when the claimed row holds the key after the commit.
    won: bool
    #: The row holding the key: the claimed row when won, the row already
    #: there when lost, or None when the ``when`` guard did not hold.
    holder: R | None


@dataclass(frozen=True)
class CompiledClaim:
    """A compiled claim: one program, ending with the query that reads the holder."""

    iql: str
    key: tuple[str, ...] = field(default=())


def _key_names(key: Sequence[str | ColumnProxy]) -> list[str]:
    return [k.name if isinstance(k, ColumnProxy) else k for k in key]


def compile_claim(
    row: Relation,
    *,
    when: BoolExpr | Sequence[BoolExpr] | None = None,
    unless: BoolExpr | None = None,
    key: Sequence[str | ColumnProxy] | None = None,
) -> CompiledClaim:
    """``claim()`` (R-CLAIM): the guarded insert of *row* with *unless* as a
    negated atom in the guard, then a query on the key in the same program.
    The query sees the staged insert, so its rows say who holds the key."""
    if not isinstance(row, Relation):
        raise TypeError(f"claim() takes a Relation instance, got {type(row).__name__}")
    rel = type(row)
    name = Relation._resolve_name(rel)
    columns = Relation._get_columns(rel)
    values = _row_values(row)
    key_names = _key_names(key) if key is not None else None
    if key_names is not None:
        _check_columns(rel, key_names, "claim() key")
    if unless is None and key_names is not None:
        unless = rel.any(**{k: getattr(row, k) for k in key_names})
    if key_names is None:
        key_names = (
            list(unless.bindings)
            if isinstance(unless, MatchExpr) and unless.relation == name
            else list(columns)
        )

    conditions = list(when) if isinstance(when, Sequence) else [] if when is None else [when]
    if unless is not None:
        conditions.append(~unless)
    guard = compile_guard(conditions)
    row_text = f"{name}({', '.join(values)})"
    insert = f"+{row_text}" if not guard.body else f"{_GHOST}, +{row_text} <- {guard.body}"
    query_args = [
        v if c in key_names else var
        for c, v, var in zip(columns, values, _column_vars(rel), strict=True)
    ]
    lines = [
        *(f"+{r}" for r in guard.const_rows),
        insert,
        *(f"-{r}" for r in guard.const_rows),
        f"?{name}({', '.join(query_args)})",
    ]
    return CompiledClaim("\n".join(lines), tuple(key_names))


# ── Reading replies ───────────────────────────────────────────────────


@dataclass(frozen=True)
class WriteCounts:
    """Counts of one write statement's reply."""

    inserted: int
    deleted: int


# The engine reports per-statement outcomes only as text, so counts are read
# from these replies. The grammar is pinned by a live test against every
# engine build in CI.
_INSERTED = re.compile(r"Inserted (\d+) fact\(s\) into '.*'\.")
_UPDATED = re.compile(r"Update: (\d+) deleted, (\d+) inserted\.")
_COND_DELETED = re.compile(r"Conditional delete: (\d+) fact\(s\) deleted from '.*'\.")
_DELETED = re.compile(r"Deleted (\d+) facts from '.*'\.")


def parse_write_message(message: str) -> WriteCounts:
    """Counts of a write statement's reply message."""
    if m := _INSERTED.fullmatch(message):
        return WriteCounts(inserted=int(m.group(1)), deleted=0)
    if m := _UPDATED.fullmatch(message):
        return WriteCounts(inserted=int(m.group(2)), deleted=int(m.group(1)))
    if m := _COND_DELETED.fullmatch(message) or _DELETED.fullmatch(message):
        return WriteCounts(inserted=0, deleted=int(m.group(1)))
    raise InternalError(f"Unexpected write reply from the engine: {message!r}")
