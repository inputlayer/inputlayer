"""Compiler: Python objects and AST nodes → IQL text.

This is the core compilation layer. Every method is pure (no I/O),
taking Python objects and returning IQL strings.
"""

from __future__ import annotations

import operator
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

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
from inputlayer._naming import column_to_variable
from inputlayer.exceptions import CompileError, InternalError
from inputlayer.types import Timestamp, python_type_to_iql

if TYPE_CHECKING:
    from inputlayer.relation import Relation


# ── Value compilation ─────────────────────────────────────────────────


def compile_value(value: Any) -> str:
    """Compile a Python value to its IQL literal representation."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        escaped = (
            value.replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
        )
        return f'"{escaped}"'
    if isinstance(value, (list, tuple)):
        # Vector literal: [1.0, 2.0, 3.0]
        inner = ", ".join(compile_value(v) for v in value)
        return f"[{inner}]"
    if isinstance(value, Timestamp):
        return str(int(value))
    raise TypeError(f"Cannot compile value of type {type(value).__name__}: {value!r}")


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


def compile_bool_expr(expr: BoolExpr, env: _VarEnv) -> list[str]:
    """Compile a BoolExpr to a list of IQL body literals.

    AND → multiple literals; OR → raises (must be handled by caller splitting).
    Returns a list of IQL body atoms/conditions joined by comma in the caller.
    """
    if isinstance(expr, Comparison):
        return [_compile_comparison(expr, env)]
    if isinstance(expr, And):
        return compile_bool_expr(expr.left, env) + compile_bool_expr(expr.right, env)
    if isinstance(expr, Or):
        raise ValueError(
            "OR conditions require query splitting. "
            "Use compile_or_branches() instead."
        )
    if isinstance(expr, Not):
        inner_parts = compile_bool_expr(expr.operand, env)
        return [f"!({', '.join(inner_parts)})"]
    if isinstance(expr, InExpr):
        return [_compile_in(expr, env, negated=False)]
    if isinstance(expr, NegatedIn):
        return [_compile_in(expr, env, negated=True)]
    if isinstance(expr, MatchExpr):
        return [_compile_match(expr, env)]
    raise TypeError(f"Cannot compile boolean expression: {expr!r}")


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


def _compile_in(expr: InExpr | NegatedIn, env: _VarEnv, *, negated: bool) -> str:
    """Compile in_() / negated in_() to IQL."""
    compile_expr(expr.column, env)
    assert isinstance(expr.target_column, AstColumn)
    tgt_col = expr.target_column
    # Build a body atom for the target relation with the column bound
    tgt_var = env.get_var(tgt_col)
    # Force unification: src_var should equal tgt_var
    # This is expressed by using the same variable in both positions
    env.unify(expr.column, expr.target_column)  # type: ignore[arg-type]
    # Re-fetch after unification
    tgt_var = env.get_var(tgt_col)
    prefix = "!" if negated else ""
    # We need to produce the target relation atom
    return f"{prefix}{tgt_col.relation}(..., {tgt_var}, ...)"


def _compile_match(match: MatchExpr, env: _VarEnv) -> str:
    """Compile a MatchExpr to an IQL body atom."""
    parts = []
    for _col_name, source_expr in match.bindings.items():
        var = compile_expr(source_expr, env)
        parts.append(var)
    atom_inner = ", ".join(parts)
    prefix = "!" if match.negated else ""
    return f"{prefix}{match.relation}({atom_inner})"


def compile_or_branches(expr: BoolExpr, env: _VarEnv) -> list[list[str]]:
    """Split OR conditions into separate branches, each a list of body literals."""
    if isinstance(expr, Or):
        left_branches = compile_or_branches(expr.left, env)
        right_branches = compile_or_branches(expr.right, env)
        return left_branches + right_branches
    return [compile_bool_expr(expr, env)]


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
        iql_type = python_type_to_iql(tp)
        parts.append(f"{col}: {iql_type}")

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
    cond_parts = compile_bool_expr(condition, env)
    cond_parts = [p for p in cond_parts if p]  # Remove empty strings from join unification

    body_parts = [body_rel, *cond_parts]
    return f"{head} <- {', '.join(body_parts)}"


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
    """One result column: the label the caller sees and the variable carrying it."""

    label: str
    variable: str


@dataclass(frozen=True)
class QueryPlan:
    """A compiled query: the one program to send and how to shape its reply.

    ``columns`` are the variables the engine returns, by position. The
    SDK applies ``skip`` (an offset without a limit, which the engine does
    not paginate) and, when ``dedupe`` is set, removes repeated rows of a
    projection: IQL is set-valued, and a projection of distinct tuples can
    repeat.
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
    skip: int
    dedupe: bool

    @property
    def labels(self) -> list[str]:
        return [o.label for o in self.outputs]

    def shape(self, rows: list[list[Any]]) -> list[list[Any]]:
        """Pick the selected columns out of the engine's rows, by position."""
        rows = rows[self.skip :] if self.skip else rows
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
        if self.dedupe:
            rows = _distinct(rows)
        return rows

    def shape_why(self, rows: list[list[Any]]) -> list[int]:
        """Indexes of the ``.why`` rows the query returns, ordered and paginated."""
        picked = list(range(len(rows)))
        if self.order is not None:
            order_var, descending = self.order
            at = self.why_columns.index(order_var)
            picked = _sorted_by(picked, lambda i: rows[i][at], descending=descending)
        start = self.offset or 0
        end = start + self.limit if self.limit is not None else None
        return picked[start:end]

    def project_why(self, row: list[Any]) -> list[Any]:
        return [row[self.why_columns.index(o.variable)] for o in self.outputs]


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

    @property
    def skip(self) -> int:
        # The engine paginates only with a limit; an offset alone is the SDK's.
        return (self.offset or 0) if self.limit is None else 0

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
    on_parts: list[str] = []
    if on_condition is not None:
        if _has_or(on_condition):
            raise CompileError("OR is not supported in a join condition", hint="put it in where=")
        _process_join_condition(on_condition, env)
        on_parts = [p for p in compile_bool_expr(on_condition, env) if p]

    where_parts: list[str] = []
    branches: list[list[str]] | None = None
    if where_condition is not None:
        if _has_or(where_condition):
            branches = [
                on_parts + [p for p in branch if p]
                for branch in compile_or_branches(where_condition, env)
            ]
        else:
            where_parts = [p for p in compile_bool_expr(where_condition, env) if p]

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
    shape = _Shape(
        env=env,
        relations=rels,
        atom_vars=atom_vars,
        atoms=[f"{rn}({', '.join(vs)})" for (rn, _, _), vs in zip(rels, atom_vars, strict=True)],
        row_vars=_unique(v for vs in atom_vars for v in vs),
        conditions=on_parts + where_parts,
        branches=branches,
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
        outputs.append(QueryOutput(label, var))

    for s in select:
        if isinstance(s, type) and issubclass(s, Relation):
            rn = Relation._resolve_name(s)
            for col in Relation._get_columns(s):
                outputs.append(QueryOutput(col, env.get_var(AstColumn(rn, col))))
        elif isinstance(s, AstColumn):
            outputs.append(QueryOutput(s.name, env.get_var(s)))
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
            skip=shape.skip,
            dedupe=lossy,
        )

    def annotate(vars_: list[str]) -> list[str]:
        if order_var is None or shape.order is None:
            return list(vars_)
        suffix = ":desc" if shape.order[1] else ":asc"
        return [f"{v}{suffix}" if v == order_var else v for v in vars_]

    paged = shape.limit is not None or shape.offset is not None
    if shape.branches is None and not (lossy and paged):
        # The plain form: one ``?`` query with the sort on its first atom.
        first = f"{shape.relations[0][0]}({', '.join(annotate(shape.atom_vars[0]))})"
        body = [first, *shape.atoms[1:], *bindings, *shape.conditions]
        program = "?" + ", ".join(body + _limit_atom(shape.limit, shape.offset))
        return plan(program, all_vars, program)

    # An OR split, or a page of a projection: a program-local rule collects
    # the rows (the union of the branches; the distinct projected rows), so
    # the engine deduplicates, orders and paginates them in one program.
    head = _unique([*out_vars, *([order_var] if order_var else [])]) if lossy else all_vars
    head_text = f"{QUERY_RULE}({', '.join(head)})"
    clauses = [
        f"{head_text} <- {', '.join([*shape.atoms, *bindings, *branch])}"
        for branch in (shape.branches if shape.branches is not None else [shape.conditions])
    ]
    query = f"?{QUERY_RULE}({', '.join(annotate(head))})"
    program = "\n".join([*clauses, ", ".join([query, *_limit_atom(shape.limit, shape.offset)])])
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
    # (label, the body variable the column carries, or None for an aggregate value)
    outputs: list[tuple[str, str | None]] = []
    bindings: list[str] = []
    bound_vars: list[str] = []

    def bind(label: str, expr: Expr) -> None:
        var = env.fresh(column_to_variable(label))
        bindings.append(f"{var} = {compile_expr(expr, env)}")
        bound_vars.append(var)
        head.append(var)
        outputs.append((label, var))

    def aggregate(agg: AggExpr, label: str | None) -> None:
        head.append(_compile_agg_expr(agg, env, count_var=count_var))
        cols = _agg_outputs(agg, env)
        if label is not None and len(cols) == 1:
            cols = [(label, cols[0][1])]
        outputs.extend(cols)

    for s in select:
        if isinstance(s, type) and issubclass(s, Relation):
            rn = Relation._resolve_name(s)
            for col in Relation._get_columns(s):
                var = env.get_var(AstColumn(rn, col))
                head.append(var)
                outputs.append((col, var))
        elif isinstance(s, AggExpr):
            aggregate(s, None)
        elif isinstance(s, AstColumn):
            var = env.get_var(s)
            head.append(var)
            outputs.append((s.name, var))
        else:
            bind("expr", s)
    for alias, expr in computed.items():
        if isinstance(expr, AggExpr):
            aggregate(expr, alias)
        else:
            bind(alias, expr)

    labelled = _unique_labels([QueryOutput(label, var or "") for label, var in outputs])
    # Every position of the query atom is a fresh variable: a repeated one
    # would join those columns as equal and drop groups (fix-report item 1).
    query_vars = _unique_names([column_to_variable(o.label) for o in labelled])

    query_args = list(query_vars)
    order: tuple[str, bool] | None = None
    if shape.order is not None:
        order_col, descending = shape.order
        order_var = env.lookup(order_col)
        at = next((i for i, (_, var) in enumerate(outputs) if var and var == order_var), None)
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
        program="\n".join([*rules, query]),
        columns=tuple(query_vars),
        outputs=tuple(
            QueryOutput(o.label, v) for o, v in zip(labelled, query_vars, strict=True)
        ),
        debug=explain,
        why=explain,
        why_columns=tuple(query_vars),
        order=order,
        limit=shape.limit,
        offset=shape.offset,
        skip=shape.skip,
        dedupe=False,
    )


def _agg_outputs(agg: AggExpr, env: _VarEnv) -> list[tuple[str, str | None]]:
    """Result columns an aggregate contributes: (label, carried variable)."""

    def column(expr: Expr) -> tuple[str, str | None]:
        if isinstance(expr, AstColumn):
            return expr.name, env.get_var(expr)
        return "expr", None

    if agg.order_column is not None:
        # top_k, top_k_threshold, within_radius: the passthrough columns,
        # then the ordered column.
        return [column(p) for p in agg.passthrough] + [column(agg.order_column)]
    if isinstance(agg.column, AstColumn):
        return [(f"{agg.func}_{agg.column.name}", None)]
    return [(agg.func, None)]


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


def _limit_atom(limit: int | None, offset: int | None) -> list[str]:
    if limit is None:
        return []
    return [f"limit({limit}, {offset})" if offset else f"limit({limit})"]


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
    return [QueryOutput(label, o.variable) for label, o in zip(labels, outputs, strict=True)]


def _distinct(rows: list[list[Any]]) -> list[list[Any]]:
    """Rows without repeats, in first-seen order."""
    seen: set[Any] = set()
    out: list[list[Any]] = []
    for row in rows:
        try:
            key: Any = tuple(row)
            hash(key)
        except TypeError:  # a vector value is a list
            key = repr(row)
        if key not in seen:
            seen.add(key)
            out.append(row)
    return out


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
    """
    from inputlayer.relation import Relation

    env = _VarEnv()

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
    cond_parts: list[str] = []
    if condition:
        cond_parts = compile_bool_expr(condition, env)
        cond_parts = [p for p in cond_parts if p]

    # Build body atoms
    body_atoms: list[str] = []
    for rn, cls, alias in body_relations:
        cols = Relation._get_columns(cls)
        atom_parts = []
        for col in cols:
            ast_col = AstColumn(rn, col, alias)
            var = env.lookup(ast_col)
            if var is not None:
                atom_parts.append(var)
            else:
                atom_parts.append("_")
        body_atoms.append(f"{rn}({', '.join(atom_parts)})")

    all_body = body_atoms + cond_parts
    prefix = "+" if persistent else ""
    head_str = f"{prefix}{head_name}({', '.join(head_parts)})"

    return f"{head_str} <- {', '.join(all_body)}"
