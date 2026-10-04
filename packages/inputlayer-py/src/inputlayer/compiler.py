"""Compiler: Python objects and AST nodes → IQL text.

This is the core compilation layer. Every method is pure (no I/O),
taking Python objects and returning IQL strings.
"""

from __future__ import annotations

import operator
import types
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from typing import TYPE_CHECKING, Any, Union, get_args, get_origin

from inputlayer._ast import (
    AggExpr,
    And,
    Arithmetic,
    BoolExpr,
    Comparison,
    Expr,
    FuncCall,
    InExpr,
    Literal,
    MatchExpr,
    NegatedIn,
    Not,
    Or,
    OrderedColumn,
)
from inputlayer._ast import (
    Column as AstColumn,
)
from inputlayer._literal import encode as encode_literal
from inputlayer._literal import ms_to_datetime
from inputlayer._naming import column_to_variable
from inputlayer.exceptions import CompileError, InternalError
from inputlayer.types import python_type_to_iql, schema_type

if TYPE_CHECKING:
    from inputlayer.relation import Relation


# ── Value compilation ─────────────────────────────────────────────────


def compile_value(value: Any) -> str:
    """Compile a Python value to its IQL literal (the R-LIT encoder)."""
    return encode_literal(value)


# ── Expression compilation ────────────────────────────────────────────


class _VarEnv:
    """Variable environment for tracking column→variable mappings with union-find.

    Ensures that join conditions like e.department == d.name produce a single
    shared IQL variable.
    """

    def __init__(self) -> None:
        self._map: dict[str, str] = {}  # "relation.col" or "alias.col" → Var
        self._counter = 0
        self._parent: dict[str, str] = {}  # Union-find parent

    def _find(self, key: str) -> str:
        """Find root of union-find set."""
        while self._parent.get(key, key) != key:
            self._parent[key] = self._parent.get(self._parent[key], self._parent[key])
            key = self._parent[key]
        return key

    def _union(self, a: str, b: str) -> None:
        """Merge two variable sets."""
        ra, rb = self._find(a), self._find(b)
        if ra != rb:
            self._parent[rb] = ra

    def get_var(self, col: AstColumn) -> str:
        """Get or create an IQL variable for a column."""
        key = f"{col.ref_alias or col.relation}.{col.name}"
        root = self._find(key)
        if root in self._map:
            return self._map[root]
        var = column_to_variable(col.name)
        # If this var name is already used by a different root, disambiguate
        used_vars = set(self._map.values())
        if var in used_vars:
            self._counter += 1
            var = f"{var}_{self._counter}"
        self._map[root] = var
        return var

    def unify(self, col_a: AstColumn, col_b: AstColumn) -> str:
        """Unify two columns to the same IQL variable (join condition)."""
        key_a = f"{col_a.ref_alias or col_a.relation}.{col_a.name}"
        key_b = f"{col_b.ref_alias or col_b.relation}.{col_b.name}"
        self._union(key_a, key_b)
        root = self._find(key_a)
        if root in self._map:
            return self._map[root]
        var = column_to_variable(col_a.name)
        used_vars = set(self._map.values())
        if var in used_vars:
            self._counter += 1
            var = f"{var}_{self._counter}"
        self._map[root] = var
        return var

    def fresh(self, base: str) -> str:
        """A variable named *base* (suffixed on collision) bound to no column."""
        used = set(self._map.values())
        var = base
        while var in used:
            self._counter += 1
            var = f"{base}_{self._counter}"
        self._map[f"\0{var}"] = var
        return var

    def lookup(self, col: AstColumn) -> str | None:
        """Look up existing variable for a column without creating one."""
        key = f"{col.ref_alias or col.relation}.{col.name}"
        root = self._find(key)
        return self._map.get(root)


def compile_expr(expr: Expr, env: _VarEnv) -> str:
    """Compile an Expr AST node to IQL text."""
    if isinstance(expr, AstColumn):
        return env.get_var(expr)
    if isinstance(expr, Literal):
        return compile_value(expr.value)
    if isinstance(expr, Arithmetic):
        left = compile_expr(expr.left, env)
        right = compile_expr(expr.right, env)
        return f"{left} {expr.op} {right}"
    if isinstance(expr, FuncCall):
        args = ", ".join(compile_expr(a, env) for a in expr.args)
        return f"{expr.name}({args})"
    if isinstance(expr, OrderedColumn):
        var = compile_expr(expr.column, env)
        suffix = ":desc" if expr.descending else ":asc"
        return f"{var}{suffix}"
    if isinstance(expr, AggExpr):
        return _compile_agg_expr(expr, env)
    raise TypeError(f"Cannot compile expression: {expr!r}")


def _compile_agg_expr(agg: AggExpr, env: _VarEnv, *, count_var: str | None = None) -> str:
    """Compile an aggregation expression to IQL syntax.

    ``count()`` without a column counts *count_var*, a variable of the
    body: the engine rejects ``count<>`` (fix-report item 5).
    """
    func = agg.func
    parts: list[str] = []

    # Params first (k, threshold, radius, etc.)
    for p in agg.params:
        parts.append(compile_value(p))

    # Passthrough columns
    for pt in agg.passthrough:
        parts.append(compile_expr(pt, env))

    # The aggregated column (for top_k this is the ordering column)
    if agg.order_column is not None:
        order_var = compile_expr(agg.order_column, env)
        suffix = ":desc" if agg.desc else ":asc"
        parts.append(f"{order_var}{suffix}")
    elif agg.column is not None:
        parts.append(compile_expr(agg.column, env))
    elif count_var is not None:
        parts.append(count_var)
    else:
        raise CompileError(
            f"{agg.func}() needs a column here", hint=f"pass one, as in {agg.func}(Employee.id)"
        )

    inner = ", ".join(parts)
    return f"{func}<{inner}>"


# ── Boolean expression compilation ───────────────────────────────────


@dataclass(frozen=True)
class _Atom:
    """A body atom a condition contributes: ``in_()``, ``matches()``, or their negation.

    Each term is ``("var", name)``, ``("const", value)`` or ``("wild", None)``.
    """

    relation: str
    terms: tuple[tuple[str, Any], ...]
    negated: bool = False

    def variables(self) -> set[str]:
        return {v for kind, v in self.terms if kind == "var"}

    def text(self) -> str:
        args = ", ".join(
            v if kind == "var" else "_" if kind == "wild" else encode_literal(v)
            for kind, v in self.terms
        )
        return f"{'!' if self.negated else ''}{self.relation}({args})"


#: A compiled condition: an atom, or a comparison's text.
_Part = _Atom | str


def compile_bool_expr(expr: BoolExpr, env: _VarEnv) -> list[str]:
    """Compile a BoolExpr to a list of IQL body literals.

    AND → multiple literals; OR → raises (must be handled by caller splitting).
    Returns a list of IQL body atoms/conditions joined by comma in the caller.
    """
    return [p.text() if isinstance(p, _Atom) else p for p in _compile_parts(expr, env)]


def _compile_parts(expr: BoolExpr, env: _VarEnv) -> list[_Part]:
    if isinstance(expr, Comparison):
        return [_compile_comparison(expr, env)]
    if isinstance(expr, And):
        return _compile_parts(expr.left, env) + _compile_parts(expr.right, env)
    if isinstance(expr, Or):
        raise ValueError(
            "OR conditions require query splitting. "
            "Use compile_or_branches() instead."
        )
    if isinstance(expr, Not):
        return _compile_parts(_negate(push_not(expr.operand)), env)
    if isinstance(expr, InExpr):
        return [_compile_in(expr, env, negated=False)]
    if isinstance(expr, NegatedIn):
        return [_compile_in(expr, env, negated=True)]
    if isinstance(expr, MatchExpr):
        return [_compile_match(expr, env)]
    raise TypeError(f"Cannot compile boolean expression: {expr!r}")


_FLIPPED = {"=": "!=", "!=": "=", "<": ">=", "<=": ">", ">": "<=", ">=": "<"}


def push_not(expr: BoolExpr) -> BoolExpr:
    """*expr* with every ``~`` pushed down to an atom or a comparison.

    IQL negates atoms only (``!r(...)``): a negated comparison flips its
    operator, a negated ``in_()``/``matches()`` negates its atom, and De
    Morgan's laws carry a negation through ``&`` and ``|``.
    """
    if isinstance(expr, And):
        return And(push_not(expr.left), push_not(expr.right))
    if isinstance(expr, Or):
        return Or(push_not(expr.left), push_not(expr.right))
    if isinstance(expr, Not):
        return _negate(push_not(expr.operand))
    return expr


def _negate(expr: BoolExpr) -> BoolExpr:
    """The negation of a ``push_not`` result."""
    if isinstance(expr, Comparison):
        return Comparison(_FLIPPED[expr.op], expr.left, expr.right)
    if isinstance(expr, And):
        return Or(_negate(expr.left), _negate(expr.right))
    if isinstance(expr, Or):
        return And(_negate(expr.left), _negate(expr.right))
    if isinstance(expr, InExpr):
        return NegatedIn(expr.column, expr.target_column, expr.target_columns)
    if isinstance(expr, NegatedIn):
        return InExpr(expr.column, expr.target_column, expr.target_columns)
    if isinstance(expr, MatchExpr):
        return replace(expr, negated=not expr.negated)
    if isinstance(expr, Not):
        return expr.operand
    raise TypeError(f"Cannot negate boolean expression: {expr!r}")


def _compile_comparison(comp: Comparison, env: _VarEnv) -> str:
    """Compile a single comparison to IQL."""
    # Check for join condition: Column == Column → unify variables
    if (
        comp.op == "="
        and isinstance(comp.left, AstColumn)
        and isinstance(comp.right, AstColumn)
    ):
        env.unify(comp.left, comp.right)
        return ""  # Join expressed through shared variable, no explicit condition
    left = compile_expr(comp.left, env)
    right = compile_expr(comp.right, env)
    return f"{left} {comp.op} {right}"


def _term(expr: Expr, env: _VarEnv, what: str) -> tuple[str, Any]:
    if isinstance(expr, AstColumn):
        return ("var", env.get_var(expr))
    if isinstance(expr, Literal):
        return ("const", expr.value)
    raise CompileError(
        f"{what} takes a column or a constant, got {expr!r}",
        hint="bind the expression to a column first",
    )


def _compile_in(expr: InExpr | NegatedIn, env: _VarEnv, *, negated: bool) -> _Atom:
    """``a.in_(b)``: an atom of b's relation with a in b's column and ``_`` elsewhere (R-IN).

    ``Employee.id.in_(Manager.employee_id)`` is ``manager(_, Id)``; the
    negated form is ``!manager(_, Id)``.
    """
    target = expr.target_column
    columns = expr.target_columns
    if not isinstance(target, AstColumn) or columns is None:
        raise CompileError(
            "in_() needs a column of a declared relation",
            hint="pass a column read from a Relation class, as in "
            "Employee.id.in_(Manager.employee_id)",
        )
    if target.name not in columns:
        raise CompileError(
            f"in_(): relation {target.relation} has no column {target.name}",
            hint=f"its columns are {', '.join(columns)}",
        )
    term = _term(expr.column, env, "in_()")
    terms = tuple(term if c == target.name else ("wild", None) for c in columns)
    return _Atom(target.relation, terms, negated)


def _compile_match(match: MatchExpr, env: _VarEnv) -> _Atom:
    """Compile a MatchExpr to an atom of its relation, ``_`` in unbound columns."""
    columns = match.columns if match.columns is not None else tuple(match.bindings)
    unknown = [c for c in match.bindings if c not in columns]
    if unknown:
        raise CompileError(
            f"matches(): relation {match.relation} has no column {', '.join(unknown)}",
            hint=f"its columns are {', '.join(columns)}",
        )
    terms = tuple(
        _term(match.bindings[c], env, "matches()") if c in match.bindings else ("wild", None)
        for c in columns
    )
    return _Atom(match.relation, terms, match.negated)


def compile_or_branches(expr: BoolExpr, env: _VarEnv) -> list[list[str]]:
    """Split OR conditions into separate branches, each a list of body literals."""
    return [
        [p.text() if isinstance(p, _Atom) else p for p in branch]
        for branch in _compile_branches(expr, env)
    ]


def _compile_branches(expr: BoolExpr, env: _VarEnv) -> list[list[_Part]]:
    if isinstance(expr, Or):
        return _compile_branches(expr.left, env) + _compile_branches(expr.right, env)
    if isinstance(expr, And) and (_has_or(expr.left) or _has_or(expr.right)):
        # (a | b) & c is (a & c) | (b & c).
        return [
            left + right
            for left in _compile_branches(expr.left, env)
            for right in _compile_branches(expr.right, env)
        ]
    return [_compile_parts(expr, env)]


# ── Negation binding (R-NEG) ─────────────────────────────────────────
#
# The engine needs every negated atom to share a variable with a positive
# atom; it refuses ``!kill_switch("refund")`` and, inside a rule, accepts the
# clause and then fails every query of that rule. A negated atom whose only
# link to the body is a constant binds it through an SDK-owned one-column
# relation instead: ``il_const_s(K), K = "refund", !kill_switch(K)``. A
# query sends the row as a session fact in its own program, gone after the
# request; a persistent view writes it as a persistent row in the program
# that defines the view (rows are never deleted: other views may share them).

#: The constant relation per literal type.
_CONST_RELATIONS = {str: "il_const_s", bool: "il_const_b", int: "il_const_i", float: "il_const_f"}


def _const_relation(value: Any) -> str:
    from datetime import datetime

    if isinstance(value, datetime):
        return _CONST_RELATIONS[int]
    for tp, name in _CONST_RELATIONS.items():
        if isinstance(value, tp):
            return name
    raise CompileError(
        f"A negated atom cannot be bound through the constant {value!r}",
        hint="test a column of a joined relation instead",
    )


@dataclass(frozen=True)
class _Body:
    """Conditions ready for a body, and the constant rows they need."""

    literals: list[str]
    constants: list[str]  # ``il_const_<t>(<literal>)``, deduplicated
    #: The variables the constant atoms bind, which a query also returns.
    variables: list[str]


def _bind_negations(
    parts: list[_Part],
    positive_vars: set[str],
    env: _VarEnv,
    *,
    canonical: bool,
    constants_allowed: bool = True,
) -> _Body:
    """Check every negated atom against the positive atoms and bind constants (R-NEG).

    *positive_vars* are the variables of the body's own atoms. With
    *canonical* (a view clause) the literals are ordered positive atoms,
    negated atoms, then comparisons and equalities; otherwise they keep the
    caller's order, with a constant's binding just before its negated atom.
    """
    bound = set(positive_vars)
    for p in parts:
        if isinstance(p, _Atom) and not p.negated:
            bound |= p.variables()
    positives: list[str] = []
    negatives: list[str] = []
    comparisons: list[str] = []
    ordered: list[str] = []
    constants: list[str] = []
    variables: list[str] = []
    for p in parts:
        if not p:
            continue
        if isinstance(p, str):
            comparisons.append(p)
            ordered.append(p)
            continue
        if not p.negated or p.variables() & bound:
            (negatives if p.negated else positives).append(p.text())
            ordered.append(p.text())
            continue
        at = next((i for i, (kind, _) in enumerate(p.terms) if kind == "const"), None)
        if at is None:
            raise CompileError(
                f"The negated atom {p.text()} shares no variable with a positive atom",
                hint="test a column of a relation the query or rule joins",
            )
        if not constants_allowed:
            raise CompileError(
                f"The negated atom {p.text()} is linked to the body by a constant only, "
                "which a conditional delete cannot bind",
                hint="test a column of the deleted relation in the negated atom",
            )
        value = p.terms[at][1]
        relation = _const_relation(value)
        var = env.fresh("K")
        variables.append(var)
        literal = encode_literal(value)
        const_atom, equality = f"{relation}({var})", f"{var} = {literal}"
        negated = replace(p, terms=(*p.terms[:at], ("var", var), *p.terms[at + 1 :])).text()
        row = f"{relation}({literal})"
        if row not in constants:
            constants.append(row)
        positives.append(const_atom)
        negatives.append(negated)
        comparisons.append(equality)
        ordered.extend([const_atom, equality, negated])
    if canonical:
        return _Body([*positives, *negatives, *comparisons], constants, variables)
    return _Body(ordered, constants, variables)


# ── Schema compilation ────────────────────────────────────────────────


def compile_schema(relation_cls: type[Relation]) -> str:
    """Compile a Relation class to a schema definition statement.

    Example: +employee(id: int, name: string, salary: float)
    """
    from inputlayer.relation import Relation

    name = Relation._resolve_name(relation_cls)
    columns = Relation._get_columns(relation_cls)
    col_types = Relation._get_column_types(relation_cls)

    parts = []
    for col in columns:
        tp = col_types[col]
        parts.append(f"{col}: {schema_type(python_type_to_iql(tp))}")

    return f"+{name}({', '.join(parts)})"


# ── Insert compilation ────────────────────────────────────────────────


def compile_insert(fact: Relation, *, persistent: bool = True) -> str:
    """Compile a single Relation instance to an insert statement.

    persistent=True  → +employee(1, "Alice", ...)
    persistent=False → employee(1, "Alice", ...)   (session fact)
    """
    from inputlayer.relation import Relation

    name = Relation._resolve_name(type(fact))
    columns = Relation._get_columns(type(fact))
    values = [compile_value(getattr(fact, col)) for col in columns]
    prefix = "+" if persistent else ""
    return f"{prefix}{name}({', '.join(values)})"


def compile_bulk_insert(
    relation_cls: type[Relation],
    facts: Sequence[Relation],
    *,
    persistent: bool = True,
) -> str:
    """Compile a list of facts to a bulk insert statement.

    +employee[(1, "Alice", ...), (2, "Bob", ...)]
    """
    from inputlayer.relation import Relation

    name = Relation._resolve_name(relation_cls)
    columns = Relation._get_columns(relation_cls)
    tuples = []
    for fact in facts:
        values = [compile_value(getattr(fact, col)) for col in columns]
        tuples.append(f"({', '.join(values)})")
    prefix = "+" if persistent else ""
    return f"{prefix}{name}[{', '.join(tuples)}]"


# ── Delete compilation ────────────────────────────────────────────────


def compile_delete(fact: Relation) -> str:
    """Compile a single fact deletion.

    -employee(1, "Alice", ...)
    """
    from inputlayer.relation import Relation

    name = Relation._resolve_name(type(fact))
    columns = Relation._get_columns(type(fact))
    values = [compile_value(getattr(fact, col)) for col in columns]
    return f"-{name}({', '.join(values)})"


def compile_conditional_delete(
    relation_cls: type[Relation],
    condition: BoolExpr,
) -> str:
    """Compile a conditional delete.

    -employee(X0, X1, X2, X3) <- employee(X0, X1, X2, X3), X2 = "sales"
    """
    from inputlayer.relation import Relation

    name = Relation._resolve_name(relation_cls)
    columns = Relation._get_columns(relation_cls)

    # Generate X0, X1, ... variables for each column
    vars_ = [f"X{i}" for i in range(len(columns))]
    head = f"-{name}({', '.join(vars_)})"

    # Build a variable environment that maps columns to X0, X1, ...
    env = _VarEnv()
    for i, col in enumerate(columns):
        key = f"{name}.{col}"
        env._map[key] = vars_[i]

    # Auto-join: include the target relation in the body
    body_rel = f"{name}({', '.join(vars_)})"

    # Compile the condition
    condition = push_not(condition)
    if _has_or(condition):
        raise CompileError(
            "OR is not supported in a conditional delete", hint="delete once per branch"
        )
    body = _bind_negations(
        _compile_parts(condition, env), set(vars_), env, canonical=False, constants_allowed=False
    )
    return f"{head} <- {', '.join([body_rel, *body.literals])}"


# ── Query compilation ─────────────────────────────────────────────────
#
# One SDK call is one program (R-ONE). An IQL query has no head: the
# engine returns every variable the query binds, in order of first
# appearance (the atoms' variables, then the bindings'), and names the
# columns after the first relation's schema or after the variables
# depending on the width. The SDK therefore binds every column of every
# atom to a variable, predicts that order, and picks the selected columns
# out of the rows by position (R-QUERY); it never reads the engine's
# column names.

#: Program-local rule an aggregate or union query evaluates. A rule sent in
#: the same program as its query lasts only for that request, so nothing is
#: left in the session (R-AGG).
QUERY_RULE = "il_q"
#: Program-local rule holding the union of an OR split that QUERY_RULE aggregates.
QUERY_SOURCE_RULE = "il_q_src"


@dataclass(frozen=True)
class QueryOutput:
    """One result column: the label the caller sees and the variable carrying it.

    ``is_datetime`` marks a column carrying a ``datetime`` field, which the
    engine holds as Unix milliseconds (R-TYPE).
    """

    label: str
    variable: str
    is_datetime: bool = False


@dataclass(frozen=True)
class QueryPlan:
    """A compiled query: the one program to send and how to shape its reply.

    ``columns`` are the variables the engine returns, by position. When
    ``dedupe`` is set the SDK removes repeated rows of a projection: IQL is
    set-valued, and a projection of distinct tuples can repeat.
    """

    program: str
    columns: tuple[str, ...]
    outputs: tuple[QueryOutput, ...]
    #: The one statement ``.debug`` takes.
    debug: str
    #: The rule ``.why`` takes, and the variable of each of its result
    #: columns by position. ``.why`` ignores ordering and pagination, so
    #: the SDK applies ``order``, ``offset`` and ``limit`` to its rows.
    why: str
    why_columns: tuple[str, ...]
    order: tuple[str, bool] | None
    limit: int | None
    offset: int | None
    dedupe: bool
    #: Session facts the program sends before its rules and query: the
    #: constants its negated atoms bind through (R-NEG). They last only for
    #: the request; ``.why`` does not see them.
    setup: tuple[str, ...] = ()

    @property
    def labels(self) -> list[str]:
        return [o.label for o in self.outputs]

    def shape(self, rows: list[list[Any]]) -> list[list[Any]]:
        """Pick the selected columns out of the engine's rows, by position."""
        rows = self.pick(rows)
        if self.dedupe:
            rows = _distinct(rows)
        return [self.from_engine(row) for row in rows]

    def pick(self, rows: list[list[Any]]) -> list[list[Any]]:
        """The selected columns of the engine's rows, by position, as the
        engine sent them: a projection keeps its repeats."""
        if rows and len(rows[0]) != len(self.columns):
            raise InternalError(
                f"The engine returned {len(rows[0])} columns for a query binding "
                f"{len(self.columns)} variables ({', '.join(self.columns)}): {self.program}"
            )
        idx = [self.columns.index(o.variable) for o in self.outputs]
        if idx != list(range(len(self.columns))):
            if len(idx) == 1:
                rows = [[row[idx[0]]] for row in rows]
            else:
                pick = operator.itemgetter(*idx)
                rows = [list(pick(row)) for row in rows]
        return rows

    def from_engine(self, row: list[Any]) -> list[Any]:
        """A picked row with engine values made Python values (R-TYPE)."""
        return [
            ms_to_datetime(v)
            if o.is_datetime and isinstance(v, int) and not isinstance(v, bool)
            else v
            for o, v in zip(self.outputs, row, strict=False)
        ]

    def shape_why(self, rows: list[list[Any]]) -> list[int]:
        """Indexes of the ``.why`` rows the query returns, ordered and paginated."""
        picked = list(range(len(rows)))
        if self.order is not None:
            order_var, descending = self.order
            at = self.why_columns.index(order_var)
            picked = _sorted_by(picked, lambda i: rows[i][at], descending=descending)
        if self.dedupe:
            firsts: dict[Any, int] = {}
            for i in picked:
                firsts.setdefault(_row_key(self.project_why(rows[i])), i)
            picked = list(firsts.values())
        start = self.offset or 0
        end = start + self.limit if self.limit is not None else None
        return picked[start:end]

    def project_why(self, row: list[Any]) -> list[Any]:
        return self.from_engine([row[self.why_columns.index(o.variable)] for o in self.outputs])


def compile_query(
    *select: type[Relation] | Expr,
    relations: list[type[Relation] | Any] | None = None,
    on_condition: BoolExpr | None = None,
    where_condition: BoolExpr | None = None,
    order_by: Expr | None = None,
    limit: int | None = None,
    offset: int | None = None,
    computed: dict[str, Expr] | None = None,
) -> str:
    """Compile a query to the one IQL program the SDK sends for it."""
    return compile_query_plan(
        *select,
        relations=relations,
        on_condition=on_condition,
        where_condition=where_condition,
        order_by=order_by,
        limit=limit,
        offset=offset,
        computed=computed,
    ).program


@dataclass
class _Shape:
    """What every query form shares: the atoms and the conditions."""

    env: _VarEnv
    relations: list[tuple[str, type[Relation], str | None]]
    atom_vars: list[list[str]]
    atoms: list[str]
    row_vars: list[str]
    conditions: list[str]
    branches: list[list[str]] | None
    order: tuple[AstColumn, bool] | None
    limit: int | None
    offset: int | None
    #: Session facts of the constant relations the conditions bind through.
    constants: list[str]
    #: The variables those bind in ``conditions``; a ``?`` query returns them last.
    const_vars: list[str]

    def first_branch(self) -> list[str]:
        return self.branches[0] if self.branches is not None else self.conditions


def compile_query_plan(
    *select: type[Relation] | Expr,
    relations: list[type[Relation] | Any] | None = None,
    on_condition: BoolExpr | None = None,
    where_condition: BoolExpr | None = None,
    order_by: Expr | None = None,
    limit: int | None = None,
    offset: int | None = None,
    computed: dict[str, Expr] | None = None,
) -> QueryPlan:
    """Compile a query to the program to send and the shape of its result."""
    from inputlayer._proxy import RelationRef
    from inputlayer.relation import Relation

    env = _VarEnv()
    rels: list[tuple[str, type[Relation], str | None]] = []
    for r in relations or []:
        if isinstance(r, RelationRef):
            rels.append((r.relation_name, r.relation_cls, r.alias))
        elif isinstance(r, type) and issubclass(r, Relation):
            rels.append((Relation._resolve_name(r), r, None))
    # Selecting a whole relation joins it.
    for s in select:
        if isinstance(s, type) and issubclass(s, Relation):
            name = Relation._resolve_name(s)
            if not any(rn == name and alias is None for rn, _, alias in rels):
                rels.append((name, s, None))
    if not rels:
        raise CompileError(
            "A query needs at least one relation",
            hint="select a relation or pass it in join=",
        )

    # Join conditions first, so unified columns share a variable; their
    # other comparisons (a.id != b.id) filter like a where condition.
    on_parts: list[_Part] = []
    if on_condition is not None:
        on_condition = push_not(on_condition)
        if _has_or(on_condition):
            raise CompileError("OR is not supported in a join condition", hint="put it in where=")
        _process_join_condition(on_condition, env)
        on_parts = _compile_parts(on_condition, env)

    where_parts: list[_Part] = []
    branch_parts: list[list[_Part]] | None = None
    if where_condition is not None:
        where_condition = push_not(where_condition)
        if _has_or(where_condition):
            branch_parts = [on_parts + b for b in _compile_branches(where_condition, env)]
        else:
            where_parts = _compile_parts(where_condition, env)

    computed = computed or {}
    is_agg = any(isinstance(s, AggExpr) for s in select) or any(
        isinstance(v, AggExpr) for v in computed.values()
    )

    order = _resolve_order(order_by)
    if order is not None and not is_agg:
        # The engine reads sort annotations on the first atom only (R-SORT);
        # the ordered relation moves first, which leaves the answer unchanged.
        col = order[0]
        for i, (rn, _, alias) in enumerate(rels):
            if rn == col.relation and alias == col.ref_alias:
                rels.insert(0, rels.pop(i))
                break

    # Every column of every atom gets a variable, so the engine's rows
    # always line up with the atoms.
    atom_vars = [
        [env.get_var(AstColumn(rn, col, alias)) for col in Relation._get_columns(cls)]
        for rn, cls, alias in rels
    ]
    # Negated atoms are checked against the atoms; one linked to them by a
    # constant only binds it through a session fact sent first (R-NEG).
    positive = {v for vs in atom_vars for v in vs}
    constants: list[str] = []

    def finish(parts: list[_Part]) -> _Body:
        body = _bind_negations(parts, positive, env, canonical=False)
        constants.extend(c for c in body.constants if c not in constants)
        return body

    conditions = finish(on_parts + where_parts)
    branches = (
        [finish(b).literals for b in branch_parts] if branch_parts is not None else None
    )
    shape = _Shape(
        env=env,
        relations=rels,
        atom_vars=atom_vars,
        atoms=[f"{rn}({', '.join(vs)})" for (rn, _, _), vs in zip(rels, atom_vars, strict=True)],
        row_vars=_unique(v for vs in atom_vars for v in vs),
        conditions=conditions.literals,
        branches=branches,
        constants=constants,
        const_vars=conditions.variables,
        order=order,
        limit=limit,
        offset=offset,
    )
    if is_agg:
        return _compile_agg_plan(shape, select, computed)
    return _compile_plain_plan(shape, select, computed)


def _compile_plain_plan(
    shape: _Shape, select: tuple[Any, ...], computed: dict[str, Expr]
) -> QueryPlan:
    from inputlayer.relation import Relation

    env = shape.env
    outputs: list[QueryOutput] = []
    bindings: list[str] = []
    bound_vars: list[str] = []

    def bind(label: str, expr: Expr) -> None:
        var = env.fresh(column_to_variable(label))
        bindings.append(f"{var} = {compile_expr(expr, env)}")
        bound_vars.append(var)
        outputs.append(QueryOutput(label, var, _is_datetime(shape, expr)))

    for s in select:
        if isinstance(s, type) and issubclass(s, Relation):
            rn = Relation._resolve_name(s)
            for col in Relation._get_columns(s):
                c = AstColumn(rn, col)
                outputs.append(QueryOutput(col, env.get_var(c), _is_datetime(shape, c)))
        elif isinstance(s, AstColumn):
            outputs.append(QueryOutput(s.name, env.get_var(s), _is_datetime(shape, s)))
        else:
            bind("expr", s)
    for alias, expr in computed.items():
        bind(alias, expr)

    order_var: str | None = None
    if shape.order is not None:
        order_var = env.lookup(shape.order[0])
        if order_var is None or order_var not in shape.atom_vars[0]:
            raise CompileError(
                f"order_by column {shape.order[0].name} is not in a joined relation",
                hint="order by a column of a relation in join=",
            )

    outputs = _unique_labels(outputs)
    out_vars = [o.variable for o in outputs]
    # A projection that leaves out a column of some atom can repeat rows.
    lossy = not set(shape.row_vars) <= set(out_vars)
    all_vars = shape.row_vars + bound_vars
    why_columns = _unique([*out_vars, *shape.row_vars])
    why = (
        f"{QUERY_RULE}({', '.join(why_columns)}) <- "
        f"{', '.join([*shape.atoms, *bindings, *shape.first_branch()])}"
    )
    def plan(program: str, columns: list[str], debug: str) -> QueryPlan:
        return QueryPlan(
            program=program,
            columns=tuple(columns),
            outputs=tuple(outputs),
            debug=debug,
            why=why,
            why_columns=tuple(why_columns),
            order=(order_var, shape.order[1]) if order_var and shape.order else None,
            limit=shape.limit,
            offset=shape.offset,
            dedupe=lossy,
            setup=tuple(shape.constants),
        )

    def annotate(vars_: list[str]) -> list[str]:
        if order_var is None or shape.order is None:
            return list(vars_)
        suffix = ":desc" if shape.order[1] else ":asc"
        return [f"{v}{suffix}" if v == order_var else v for v in vars_]

    paged = shape.limit is not None or shape.offset is not None
    if paged and order_var is not None and order_var not in out_vars:
        raise CompileError(
            "ordering by a column you do not select with limit is ambiguous; "
            "select it or drop limit",
            hint="add the order_by column to the selection, or drop limit and offset",
        )
    if shape.branches is None and not (lossy and paged):
        # The plain form: one ``?`` query with the sort on its first atom.
        first = f"{shape.relations[0][0]}({', '.join(annotate(shape.atom_vars[0]))})"
        body = [first, *shape.atoms[1:], *bindings, *shape.conditions]
        query = "?" + ", ".join(body + _limit_atom(shape.limit, shape.offset))
        return plan(
            "\n".join([*shape.constants, query]), [*all_vars, *shape.const_vars], query
        )

    # An OR split, or a page of a projection: a program-local rule collects
    # the rows (the union of the branches; the distinct projected rows), so
    # the engine deduplicates, orders and paginates them in one program.
    head = out_vars if lossy and (order_var is None or order_var in out_vars) else all_vars
    head_text = f"{QUERY_RULE}({', '.join(head)})"
    clauses = [
        f"{head_text} <- {', '.join([*shape.atoms, *bindings, *branch])}"
        for branch in (shape.branches if shape.branches is not None else [shape.conditions])
    ]
    query = f"?{QUERY_RULE}({', '.join(annotate(head))})"
    program = "\n".join(
        [*shape.constants, *clauses, ", ".join([query, *_limit_atom(shape.limit, shape.offset)])]
    )
    debug = "?" + ", ".join([*shape.atoms, *bindings, *shape.first_branch()])
    return plan(program, head, debug)


def _compile_agg_plan(
    shape: _Shape, select: tuple[Any, ...], computed: dict[str, Expr]
) -> QueryPlan:
    from inputlayer.relation import Relation

    env = shape.env
    # count() without a column counts the first variable of the first atom.
    count_var = shape.atom_vars[0][0]
    head: list[str] = []
    # Each column, with the body variable it carries ("" for an aggregate value).
    outputs: list[QueryOutput] = []
    bindings: list[str] = []
    bound_vars: list[str] = []

    def bind(label: str, expr: Expr) -> None:
        var = env.fresh(column_to_variable(label))
        bindings.append(f"{var} = {compile_expr(expr, env)}")
        bound_vars.append(var)
        head.append(var)
        outputs.append(QueryOutput(label, var, _is_datetime(shape, expr)))

    def aggregate(agg: AggExpr, label: str | None) -> None:
        head.append(_compile_agg_expr(agg, env, count_var=count_var))
        cols = _agg_outputs(agg, shape)
        if label is not None and len(cols) == 1:
            cols = [replace(cols[0], label=label)]
        outputs.extend(cols)

    for s in select:
        if isinstance(s, type) and issubclass(s, Relation):
            rn = Relation._resolve_name(s)
            for col in Relation._get_columns(s):
                c = AstColumn(rn, col)
                var = env.get_var(c)
                head.append(var)
                outputs.append(QueryOutput(col, var, _is_datetime(shape, c)))
        elif isinstance(s, AggExpr):
            aggregate(s, None)
        elif isinstance(s, AstColumn):
            var = env.get_var(s)
            head.append(var)
            outputs.append(QueryOutput(s.name, var, _is_datetime(shape, s)))
        else:
            bind("expr", s)
    for alias, expr in computed.items():
        if isinstance(expr, AggExpr):
            aggregate(expr, alias)
        else:
            bind(alias, expr)

    labelled = _unique_labels(outputs)
    # Every position of the query atom is a fresh variable: a repeated one
    # would join those columns as equal and drop groups (fix-report item 1).
    query_vars = _unique_names([column_to_variable(o.label) for o in labelled])

    query_args = list(query_vars)
    order: tuple[str, bool] | None = None
    if shape.order is not None:
        order_col, descending = shape.order
        order_var = env.lookup(order_col)
        at = next(
            (i for i, o in enumerate(outputs) if o.variable and o.variable == order_var), None
        )
        if at is None:
            raise CompileError(
                "In an aggregate query, order_by must be a selected column "
                f"({order_col.name} is not)",
                hint="select the column, or order by a grouping column",
            )
        query_args[at] = f"{query_vars[at]}{':desc' if descending else ':asc'}"
        order = (query_vars[at], descending)

    head_text = f"{QUERY_RULE}({', '.join(head)})"
    explain = f"{head_text} <- {', '.join([*shape.atoms, *bindings, *shape.first_branch()])}"
    if shape.branches is None:
        rules = [explain]
    else:
        # Aggregate over the union of the branches, collected first.
        src = f"{QUERY_SOURCE_RULE}({', '.join([*shape.row_vars, *bound_vars])})"
        rules = [
            f"{src} <- {', '.join([*shape.atoms, *bindings, *branch])}"
            for branch in shape.branches
        ]
        rules.append(f"{head_text} <- {src}")
    query = ", ".join(
        [f"?{QUERY_RULE}({', '.join(query_args)})", *_limit_atom(shape.limit, shape.offset)]
    )
    return QueryPlan(
        program="\n".join([*shape.constants, *rules, query]),
        columns=tuple(query_vars),
        outputs=tuple(
            replace(o, variable=v) for o, v in zip(labelled, query_vars, strict=True)
        ),
        debug=explain,
        why=explain,
        why_columns=tuple(query_vars),
        order=order,
        limit=shape.limit,
        offset=shape.offset,
        dedupe=False,
        setup=tuple(shape.constants),
    )


def _agg_outputs(agg: AggExpr, shape: _Shape) -> list[QueryOutput]:
    """Result columns an aggregate contributes, with the variable each carries."""

    def column(expr: Expr) -> QueryOutput:
        if isinstance(expr, AstColumn):
            return QueryOutput(expr.name, shape.env.get_var(expr), _is_datetime(shape, expr))
        return QueryOutput("expr", "")

    if agg.order_column is not None:
        # top_k, top_k_threshold, within_radius: the passthrough columns,
        # then the ordered column.
        return [column(p) for p in agg.passthrough] + [column(agg.order_column)]
    if isinstance(agg.column, AstColumn):
        # The least or greatest of a datetime column is a datetime.
        return [
            QueryOutput(
                f"{agg.func}_{agg.column.name}",
                "",
                agg.func in ("min", "max") and _is_datetime(shape, agg.column),
            )
        ]
    return [QueryOutput(agg.func, "")]


def _is_datetime(shape: _Shape, expr: Expr) -> bool:
    """Whether *expr* is a column of a joined relation annotated ``datetime``."""
    from inputlayer.relation import Relation

    if not isinstance(expr, AstColumn):
        return False
    for rn, cls, alias in shape.relations:
        if rn == expr.relation and alias == expr.ref_alias:
            tp = Relation._get_column_types(cls).get(expr.name)
            members = get_args(tp) if get_origin(tp) in (Union, types.UnionType) else (tp,)
            return any(isinstance(m, type) and issubclass(m, datetime) for m in members)
    return False


def _resolve_order(order_by: Expr | None) -> tuple[AstColumn, bool] | None:

    if order_by is None:
        return None
    if isinstance(order_by, OrderedColumn) and isinstance(order_by.column, AstColumn):
        return order_by.column, order_by.descending
    if isinstance(order_by, AstColumn):
        return order_by, False
    raise CompileError(
        "order_by must be a column", hint="pass a column, optionally with .asc() or .desc()"
    )


# The largest limit the engine parses, sent with an offset that has no
# limit so the engine still pages. The server's own max_result_rows cap
# still bounds the rows and sets truncated.
MAX_LIMIT = 9223372036854775807


def _limit_atom(limit: int | None, offset: int | None) -> list[str]:
    for name, value in (("limit", limit), ("offset", offset)):
        if value is not None and (
            not isinstance(value, int) or isinstance(value, bool) or value < 0
        ):
            raise CompileError(
                f"{name} must be a non-negative int, got {value!r}",
                hint=f"pass {name} as an int of 0 or more",
            )
    if limit is None:
        if not offset:
            return []
        limit = MAX_LIMIT
    if offset:
        return [f"limit({encode_literal(limit)}, {encode_literal(offset)})"]
    return [f"limit({encode_literal(limit)})"]


def _unique(items: Any) -> list[str]:
    return list(dict.fromkeys(items))


def _unique_names(names: list[str]) -> list[str]:
    """Suffix repeats with _2, _3, ... so every name is distinct."""
    seen: set[str] = set()
    out: list[str] = []
    for name in names:
        candidate, n = name, 2
        while candidate in seen:
            candidate, n = f"{name}_{n}", n + 1
        seen.add(candidate)
        out.append(candidate)
    return out


def _unique_labels(outputs: list[QueryOutput]) -> list[QueryOutput]:
    labels = _unique_names([o.label for o in outputs])
    return [replace(o, label=label) for label, o in zip(labels, outputs, strict=True)]


def _distinct(rows: list[list[Any]]) -> list[list[Any]]:
    """Rows without repeats, in first-seen order."""
    seen: set[Any] = set()
    out: list[list[Any]] = []
    for row in rows:
        key = _row_key(row)
        if key not in seen:
            seen.add(key)
            out.append(row)
    return out


def _row_key(row: list[Any]) -> Any:
    try:
        key: Any = tuple(row)
        hash(key)
    except TypeError:  # a vector value is a list
        key = repr(row)
    return key


def _sorted_by(items: list[int], key: Any, *, descending: bool) -> list[int]:
    """Sort by engine value, nulls last in either direction."""
    present = [i for i in items if key(i) is not None]
    missing = [i for i in items if key(i) is None]
    try:
        present.sort(key=key, reverse=descending)
    except TypeError:
        present.sort(key=lambda i: repr(key(i)), reverse=descending)
    return present + missing


def _process_join_condition(condition: BoolExpr, env: _VarEnv) -> None:
    """Process join conditions to set up variable unification."""
    if (
        isinstance(condition, Comparison)
        and condition.op == "="
        and isinstance(condition.left, AstColumn)
        and isinstance(condition.right, AstColumn)
    ):
        env.unify(condition.left, condition.right)
        return
    if isinstance(condition, And):
        _process_join_condition(condition.left, env)
        _process_join_condition(condition.right, env)


def _has_or(expr: BoolExpr) -> bool:
    """Check if expression contains any OR nodes."""
    if isinstance(expr, Or):
        return True
    if isinstance(expr, And):
        return _has_or(expr.left) or _has_or(expr.right)
    if isinstance(expr, Not):
        return _has_or(expr.operand)
    return False


# ── Rule compilation ──────────────────────────────────────────────────


def compile_rule(
    head_name: str,
    head_columns: list[str],
    select_map: dict[str, Expr],
    body_relations: list[tuple[str, type[Relation], str | None]],
    condition: BoolExpr | None = None,
    *,
    persistent: bool = True,
) -> str:
    """Compile a rule definition to IQL.

    persistent=True  → +reachable(Src, Dst) <- edge(Src, Dst)
    persistent=False →  reachable(Src, Dst) <- edge(Src, Dst)

    A clause whose negated atom binds a constant (R-NEG) is preceded by the
    constant's row, in the same program: ``+il_const_s("x")`` for a
    persistent rule, the session fact ``il_const_s("x")`` for a session rule.
    """
    compiled = compile_rule_clause(
        head_name,
        head_columns,
        select_map,
        body_relations,
        condition,
        persistent=persistent,
    )
    return "\n".join([*compiled.constants, compiled.clause])


@dataclass(frozen=True)
class CompiledRule:
    """A rule clause and the constant rows its body binds through (R-NEG)."""

    clause: str
    #: Insert statements for the constant rows (``+il_const_s("x")``, or
    #: ``il_const_s("x")`` for a session rule), to send with the clause.
    constants: tuple[str, ...] = ()


def compile_rule_clause(
    head_name: str,
    head_columns: list[str],
    select_map: dict[str, Expr],
    body_relations: list[tuple[str, type[Relation], str | None]],
    condition: BoolExpr | None = None,
    *,
    persistent: bool = True,
) -> CompiledRule:
    """Compile a rule clause, keeping apart the constant rows it needs.

    The body is canonical: the relations' atoms and the other positive
    atoms, then negated atoms, then comparisons and equalities, so identical
    views compile byte-identical (R-NEG).
    """
    from inputlayer.relation import Relation

    env = _VarEnv()
    if condition is not None:
        condition = push_not(condition)
        if _has_or(condition):
            raise CompileError(
                "OR is not supported in a rule condition",
                hint="write one clause per branch",
            )

    # Process condition first for join unification
    if condition:
        _process_join_condition(condition, env)

    # count() without a column counts the first column of the first body relation.
    count_var: str | None = None
    if body_relations and any(
        isinstance(e, AggExpr) and e.column is None and e.order_column is None
        for e in select_map.values()
    ):
        rn, cls, alias = body_relations[0]
        count_var = env.get_var(AstColumn(rn, Relation._get_columns(cls)[0], alias))

    # Build head
    head_parts = []
    for col in head_columns:
        expr = select_map.get(col)
        if isinstance(expr, AggExpr):
            head_parts.append(_compile_agg_expr(expr, env, count_var=count_var))
        elif expr is not None:
            compiled = compile_expr(expr, env)
            head_parts.append(compiled)
        else:
            head_parts.append(column_to_variable(col))

    # Compile filter conditions BEFORE building body atoms, so every column
    # the condition references is registered in the env and gets bound to a
    # variable in its atom. Compiling them after produced atoms with `_` for
    # condition-only columns while the condition referenced an unbound
    # variable - which the engine accepts and silently satisfies, deriving
    # wrong results (e.g. a tier == "gold" filter matching every row).
    cond_parts = _compile_parts(condition, env) if condition else []

    # Build body atoms
    body_atoms: list[str] = []
    positive: set[str] = set()
    for rn, cls, alias in body_relations:
        cols = Relation._get_columns(cls)
        atom_parts = []
        for col in cols:
            ast_col = AstColumn(rn, col, alias)
            var = env.lookup(ast_col)
            if var is not None:
                atom_parts.append(var)
                positive.add(var)
            else:
                atom_parts.append("_")
        body_atoms.append(f"{rn}({', '.join(atom_parts)})")

    body = _bind_negations(cond_parts, positive, env, canonical=True)
    prefix = "+" if persistent else ""
    head_str = f"{prefix}{head_name}({', '.join(head_parts)})"
    return CompiledRule(
        clause=f"{head_str} <- {', '.join(body_atoms + body.literals)}",
        constants=tuple(f"{prefix}{row}" for row in body.constants),
    )
