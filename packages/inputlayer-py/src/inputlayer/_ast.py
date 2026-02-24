"""Internal AST nodes for expression trees compiled to Datalog."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# ── Base ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Expr:
    """Base class for all expression AST nodes."""


@dataclass(frozen=True)
class BoolExpr:
    """Base class for boolean expression AST nodes (conditions)."""


# ── Leaf nodes ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Column(Expr):
    """Reference to a relation column."""
    relation: str
    name: str
    ref_alias: str | None = None  # For self-join disambiguation

    @property
    def qualified(self) -> str:
        prefix = self.ref_alias or self.relation
        return f"{prefix}.{self.name}"


@dataclass(frozen=True)
class Literal(Expr):
    """A constant value."""
    value: Any  # int, float, str, bool, list (vector), None


# ── Arithmetic ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Arithmetic(Expr):
    """Binary arithmetic: +, -, *, /, %."""
    op: str  # "+", "-", "*", "/", "%"
    left: Expr
    right: Expr


# ── Function call ─────────────────────────────────────────────────────

@dataclass(frozen=True)
class FuncCall(Expr):
    """Built-in function call: distance(V1, V2), upper(S), etc."""
    name: str
    args: tuple[Expr, ...] = ()


# ── Aggregation ───────────────────────────────────────────────────────

@dataclass(frozen=True)
class AggExpr(Expr):
    """Aggregation expression: count<X>, sum<X>, top_k<k, ...>, etc."""
    func: str  # "count", "sum", "min", "max", "avg", "count_distinct",
    # "top_k", "top_k_threshold", "within_radius"
    column: Expr | None = None  # The aggregated column (None for count(*))
    params: tuple[Any, ...] = ()  # Extra params (k, threshold, etc.)
    passthrough: tuple[Expr, ...] = ()  # Passthrough columns
    order_column: Expr | None = None  # For top_k: the ordering column
    desc: bool = True  # For top_k: descending order


# ── Ordering ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class OrderedColumn(Expr):
    """A column with sort direction."""
    column: Expr
    descending: bool = False


# ── Boolean expressions ──────────────────────────────────────────────

@dataclass(frozen=True)
class Comparison(BoolExpr):
    """Binary comparison: ==, !=, <, <=, >, >=."""
    op: str  # "=", "!=", "<", "<=", ">", ">="
    left: Expr
    right: Expr


@dataclass(frozen=True)
class And(BoolExpr):
    """Logical AND of two conditions (Datalog comma)."""
    left: BoolExpr
    right: BoolExpr


@dataclass(frozen=True)
class Or(BoolExpr):
    """Logical OR - requires splitting into multiple queries."""
    left: BoolExpr
    right: BoolExpr


@dataclass(frozen=True)
class Not(BoolExpr):
    """Negation: !relation(X, Y) in Datalog."""
    operand: BoolExpr


@dataclass(frozen=True)
class InExpr(BoolExpr):
    """Membership test: Column appears in another relation."""
    column: Expr
    target_column: Expr


@dataclass(frozen=True)
class NegatedIn(BoolExpr):
    """Negated membership test."""
    column: Expr
    target_column: Expr


@dataclass(frozen=True)
class MatchExpr(BoolExpr):
    """Multi-column negation/existence check against a relation."""
    relation: str
    bindings: dict[str, Expr]  # target_col -> source expr
    negated: bool = False
