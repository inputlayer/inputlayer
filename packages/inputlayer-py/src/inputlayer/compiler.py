"""Compiler: Python objects and AST nodes → Datalog text.

This is the core compilation layer. Every method is pure (no I/O),
taking Python objects and returning Datalog strings.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from inputlayer._ast import (
    AggExpr,
    And,
    Arithmetic,
    BoolExpr,
    Column as AstColumn,
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
from inputlayer._naming import column_to_variable
from inputlayer.types import Timestamp, Vector, VectorInt8, python_type_to_datalog

if TYPE_CHECKING:
    from inputlayer.relation import Relation


# ── Value compilation ─────────────────────────────────────────────────


def compile_value(value: Any) -> str:
    """Compile a Python value to its Datalog literal representation."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
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
    shared Datalog variable.
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
        """Get or create a Datalog variable for a column."""
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
        """Unify two columns to the same Datalog variable (join condition)."""
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

    def lookup(self, col: AstColumn) -> str | None:
        """Look up existing variable for a column without creating one."""
        key = f"{col.ref_alias or col.relation}.{col.name}"
        root = self._find(key)
        return self._map.get(root)


def compile_expr(expr: Expr, env: _VarEnv) -> str:
    """Compile an Expr AST node to Datalog text."""
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


def _compile_agg_expr(agg: AggExpr, env: _VarEnv) -> str:
    """Compile an aggregation expression to Datalog syntax."""
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

    inner = ", ".join(parts)
    return f"{func}<{inner}>"


# ── Boolean expression compilation ───────────────────────────────────


def compile_bool_expr(expr: BoolExpr, env: _VarEnv) -> list[str]:
    """Compile a BoolExpr to a list of Datalog body literals.

    AND → multiple literals; OR → raises (must be handled by caller splitting).
    Returns a list of Datalog body atoms/conditions joined by comma in the caller.
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
    """Compile a single comparison to Datalog."""
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
    """Compile in_() / negated in_() to Datalog."""
    src_var = compile_expr(expr.column, env)
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
    """Compile a MatchExpr to a Datalog body atom."""
    parts = []
    for col_name, source_expr in match.bindings.items():
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
        dl_type = python_type_to_datalog(tp)
        parts.append(f"{col}: {dl_type}")

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
        col_ast = AstColumn(name, col)
        key = f"{name}.{col}"
        env._map[key] = vars_[i]

    # Auto-join: include the target relation in the body
    body_rel = f"{name}({', '.join(vars_)})"

    # Compile the condition
    cond_parts = compile_bool_expr(condition, env)
    cond_parts = [p for p in cond_parts if p]  # Remove empty strings from join unification

    body_parts = [body_rel] + cond_parts
    return f"{head} <- {', '.join(body_parts)}"


# ── Query compilation ─────────────────────────────────────────────────


def compile_query(
    *select: type[Relation] | Expr,
    relations: list[type[Relation] | Any] | None = None,
    on_condition: BoolExpr | None = None,
    where_condition: BoolExpr | None = None,
    order_by: Expr | None = None,
    limit: int | None = None,
    offset: int | None = None,
    computed: dict[str, Expr] | None = None,
) -> str | list[str]:
    """Compile a query to Datalog.

    Returns a single string, or a list of strings if OR conditions require splitting.
    """
    from inputlayer.relation import Relation
    from inputlayer._proxy import RelationRef

    env = _VarEnv()

    # Determine which relations are involved
    all_relations: list[tuple[str, type[Relation], str | None]] = []  # (name, cls, alias)

    if relations:
        for r in relations:
            if isinstance(r, RelationRef):
                all_relations.append((r.relation_name, r.relation_cls, r.alias))
            elif isinstance(r, type) and issubclass(r, Relation):
                all_relations.append((Relation._resolve_name(r), r, None))

    # Process join conditions first to set up unification
    if on_condition:
        _process_join_condition(on_condition, env)

    # Process where conditions
    where_parts: list[str] = []
    or_branches: list[list[str]] | None = None
    if where_condition:
        if _has_or(where_condition):
            or_branches = compile_or_branches(where_condition, env)
        else:
            where_parts = compile_bool_expr(where_condition, env)
            where_parts = [p for p in where_parts if p]

    # Build the head (select) and body
    has_agg = any(isinstance(s, AggExpr) for s in select)
    computed = computed or {}
    has_computed_agg = any(isinstance(v, AggExpr) for v in computed.values())

    if has_agg or has_computed_agg:
        return _compile_agg_query(
            select, env, all_relations, where_parts, or_branches,
            order_by, limit, offset, computed,
        )

    # Simple query (no aggregations)
    head_parts: list[str] = []
    body_atoms: list[str] = []

    # Collect selected columns per relation
    selected_by_rel: dict[str, list[AstColumn]] = {}
    full_relations: list[tuple[str, type[Relation], str | None]] = []

    for s in select:
        if isinstance(s, type) and issubclass(s, Relation):
            rn = Relation._resolve_name(s)
            full_relations.append((rn, s, None))
        elif isinstance(s, AstColumn):
            key = s.ref_alias or s.relation
            selected_by_rel.setdefault(key, []).append(s)

    # If selecting full relations, select all their columns
    if full_relations:
        for rn, cls, alias in full_relations:
            cols = Relation._get_columns(cls)
            for col in cols:
                ast_col = AstColumn(rn, col, alias)
                var = env.get_var(ast_col)
                head_parts.append(var)
            # Also ensure relation is in the body
            if not any(r[0] == rn and r[2] == alias for r in all_relations):
                all_relations.append((rn, cls, alias))

    # Add individual selected columns to head
    for s in select:
        if isinstance(s, AstColumn):
            var = env.get_var(s)
            head_parts.append(var)

    # Add computed columns to head
    for alias_name, expr in computed.items():
        compiled = compile_expr(expr, env)
        head_parts.append(compiled)

    # Handle order_by
    if order_by is not None:
        # Find and replace the matching head variable with ordered version
        if isinstance(order_by, OrderedColumn):
            order_var = compile_expr(order_by.column, env)
            suffix = ":desc" if order_by.descending else ":asc"
            # Replace in head_parts
            for i, hp in enumerate(head_parts):
                if hp == order_var:
                    head_parts[i] = f"{order_var}{suffix}"
                    break
        elif isinstance(order_by, AstColumn):
            order_var = env.get_var(order_by)
            suffix = ":asc"
            for i, hp in enumerate(head_parts):
                if hp == order_var:
                    head_parts[i] = f"{order_var}{suffix}"
                    break

    # Build body atoms for each relation
    for rn, cls, alias in all_relations:
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

    # Combine body
    all_body = body_atoms + where_parts
    if limit is not None:
        if offset is not None:
            all_body.append(f"limit({limit}, {offset})")
        else:
            all_body.append(f"limit({limit})")

    head_str = ", ".join(head_parts)

    if or_branches is not None:
        # Multiple queries for OR
        queries = []
        for branch_parts in or_branches:
            branch_parts = [p for p in branch_parts if p]
            branch_body = body_atoms + branch_parts
            if limit is not None:
                if offset is not None:
                    branch_body.append(f"limit({limit}, {offset})")
                else:
                    branch_body.append(f"limit({limit})")
            queries.append(f"?{head_str} <- {', '.join(branch_body)}")
        return queries

    if all_body:
        return f"?{head_str} <- {', '.join(all_body)}"
    return f"?{head_str}"


def _process_join_condition(condition: BoolExpr, env: _VarEnv) -> None:
    """Process join conditions to set up variable unification."""
    if isinstance(condition, Comparison) and condition.op == "=":
        if isinstance(condition.left, AstColumn) and isinstance(condition.right, AstColumn):
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


def _compile_agg_query(
    select: tuple,
    env: _VarEnv,
    all_relations: list[tuple[str, type, str | None]],
    where_parts: list[str],
    or_branches: list[list[str]] | None,
    order_by: Expr | None,
    limit: int | None,
    offset: int | None,
    computed: dict[str, Expr],
) -> str:
    """Compile a query with aggregation."""
    from inputlayer.relation import Relation

    head_parts: list[str] = []
    agg_parts: list[str] = []

    # Separate grouping keys from aggregations
    for s in select:
        if isinstance(s, AggExpr):
            agg_parts.append(compile_expr(s, env))
        elif isinstance(s, AstColumn):
            head_parts.append(env.get_var(s))
        elif isinstance(s, type) and issubclass(s, Relation):
            rn = Relation._resolve_name(s)
            cols = Relation._get_columns(s)
            for col in cols:
                ast_col = AstColumn(rn, col)
                head_parts.append(env.get_var(ast_col))

    for alias_name, expr in computed.items():
        if isinstance(expr, AggExpr):
            agg_parts.append(compile_expr(expr, env))
        else:
            head_parts.append(compile_expr(expr, env))

    # Build body
    body_atoms: list[str] = []
    for rn, cls, alias in all_relations:
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

    all_body = body_atoms + where_parts
    if limit is not None:
        if offset is not None:
            all_body.append(f"limit({limit}, {offset})")
        else:
            all_body.append(f"limit({limit})")

    all_head = head_parts + agg_parts
    head_str = ", ".join(all_head)

    if all_body:
        return f"?{head_str} <- {', '.join(all_body)}"
    return f"?{head_str}"


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
    """Compile a rule definition to Datalog.

    persistent=True  → +reachable(Src, Dst) <- edge(Src, Dst)
    persistent=False →  reachable(Src, Dst) <- edge(Src, Dst)
    """
    from inputlayer.relation import Relation

    env = _VarEnv()

    # Process condition first for join unification
    if condition:
        _process_join_condition(condition, env)

    # Build head
    head_parts = []
    for col in head_columns:
        expr = select_map.get(col)
        if expr is not None:
            compiled = compile_expr(expr, env)
            head_parts.append(compiled)
        else:
            head_parts.append(column_to_variable(col))

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

    # Compile filter conditions
    cond_parts: list[str] = []
    if condition:
        cond_parts = compile_bool_expr(condition, env)
        cond_parts = [p for p in cond_parts if p]

    all_body = body_atoms + cond_parts
    prefix = "+" if persistent else ""
    head_str = f"{prefix}{head_name}({', '.join(head_parts)})"

    return f"{head_str} <- {', '.join(all_body)}"
