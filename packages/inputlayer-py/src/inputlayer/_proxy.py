"""Column proxy objects for building expression ASTs via operator overloading."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from inputlayer._ast import (
    AggExpr,
    And,
    Arithmetic,
    BoolExpr,
    Column as AstColumn,
    Comparison,
    Expr,
    InExpr,
    Literal,
    MatchExpr,
    NegatedIn,
    Not,
    Or,
    OrderedColumn,
)

if TYPE_CHECKING:
    from inputlayer.relation import Relation


class ColumnProxy:
    """Proxy returned by Relation.column_name - builds AST nodes via operators."""

    def __init__(self, relation: str, name: str, *, ref_alias: str | None = None) -> None:
        self._relation = relation
        self._name = name
        self._ref_alias = ref_alias

    @property
    def relation(self) -> str:
        return self._relation

    @property
    def name(self) -> str:
        return self._name

    @property
    def ref_alias(self) -> str | None:
        return self._ref_alias

    def _to_ast(self) -> AstColumn:
        return AstColumn(self._relation, self._name, self._ref_alias)

    # ── Comparison operators → BoolExpr ───────────────────────────────

    def __eq__(self, other: Any) -> Comparison:  # type: ignore[override]
        return Comparison("=", self._to_ast(), _wrap(other))

    def __ne__(self, other: Any) -> Comparison:  # type: ignore[override]
        return Comparison("!=", self._to_ast(), _wrap(other))

    def __lt__(self, other: Any) -> Comparison:
        return Comparison("<", self._to_ast(), _wrap(other))

    def __le__(self, other: Any) -> Comparison:
        return Comparison("<=", self._to_ast(), _wrap(other))

    def __gt__(self, other: Any) -> Comparison:
        return Comparison(">", self._to_ast(), _wrap(other))

    def __ge__(self, other: Any) -> Comparison:
        return Comparison(">=", self._to_ast(), _wrap(other))

    # ── Arithmetic operators → Expr ───────────────────────────────────

    def __add__(self, other: Any) -> Arithmetic:
        return Arithmetic("+", self._to_ast(), _wrap(other))

    def __radd__(self, other: Any) -> Arithmetic:
        return Arithmetic("+", _wrap(other), self._to_ast())

    def __sub__(self, other: Any) -> Arithmetic:
        return Arithmetic("-", self._to_ast(), _wrap(other))

    def __rsub__(self, other: Any) -> Arithmetic:
        return Arithmetic("-", _wrap(other), self._to_ast())

    def __mul__(self, other: Any) -> Arithmetic:
        return Arithmetic("*", self._to_ast(), _wrap(other))

    def __rmul__(self, other: Any) -> Arithmetic:
        return Arithmetic("*", _wrap(other), self._to_ast())

    def __truediv__(self, other: Any) -> Arithmetic:
        return Arithmetic("/", self._to_ast(), _wrap(other))

    def __rtruediv__(self, other: Any) -> Arithmetic:
        return Arithmetic("/", _wrap(other), self._to_ast())

    def __mod__(self, other: Any) -> Arithmetic:
        return Arithmetic("%", self._to_ast(), _wrap(other))

    def __rmod__(self, other: Any) -> Arithmetic:
        return Arithmetic("%", _wrap(other), self._to_ast())

    # ── Negation (bitwise NOT used as logical NOT) ────────────────────

    def __invert__(self) -> ColumnProxy:
        """Returns a negated proxy (for ~Relation.col.in_(...) patterns)."""
        return _NegatedColumnProxy(self)

    # ── Membership ────────────────────────────────────────────────────

    def in_(self, other: ColumnProxy) -> InExpr:
        """Test if this column's value appears in another relation's column."""
        return InExpr(self._to_ast(), other._to_ast())

    # ── Ordering ──────────────────────────────────────────────────────

    def asc(self) -> OrderedColumn:
        return OrderedColumn(self._to_ast(), descending=False)

    def desc(self) -> OrderedColumn:
        return OrderedColumn(self._to_ast(), descending=True)

    # ── Multi-column match ────────────────────────────────────────────

    def matches(
        self, relation: type[Relation], on: dict[str, str]
    ) -> MatchExpr:
        """Check if columns match entries in another relation."""
        from inputlayer.relation import Relation as RelBase

        rel_name = RelBase._resolve_name(relation)
        bindings = {}
        for target_col, source_col_name in on.items():
            # source_col_name refers to a column on self's relation
            bindings[target_col] = AstColumn(self._relation, source_col_name, self._ref_alias)
        return MatchExpr(rel_name, bindings, negated=False)

    def __repr__(self) -> str:
        if self._ref_alias:
            return f"ColumnProxy({self._ref_alias}.{self._name})"
        return f"ColumnProxy({self._relation}.{self._name})"


class _NegatedColumnProxy(ColumnProxy):
    """Wrapper returned by ~col to flip .in_() to NegatedIn."""

    def __init__(self, inner: ColumnProxy) -> None:
        super().__init__(inner._relation, inner._name, ref_alias=inner._ref_alias)
        self._inner = inner

    def in_(self, other: ColumnProxy) -> NegatedIn:  # type: ignore[override]
        return NegatedIn(self._inner._to_ast(), other._to_ast())

    def matches(  # type: ignore[override]
        self, relation: type[Relation], on: dict[str, str]
    ) -> MatchExpr:
        from inputlayer.relation import Relation as RelBase

        rel_name = RelBase._resolve_name(relation)
        bindings = {}
        for target_col, source_col_name in on.items():
            bindings[target_col] = AstColumn(self._relation, source_col_name, self._ref_alias)
        return MatchExpr(rel_name, bindings, negated=True)


class RelationProxy:
    """Proxy object passed to where/on lambdas. Attribute access returns ColumnProxy."""

    def __init__(self, relation_name: str, *, ref_alias: str | None = None) -> None:
        self._relation_name = relation_name
        self._ref_alias = ref_alias

    def __getattr__(self, name: str) -> ColumnProxy:
        if name.startswith("_"):
            raise AttributeError(name)
        return ColumnProxy(self._relation_name, name, ref_alias=self._ref_alias)

    def __repr__(self) -> str:
        if self._ref_alias:
            return f"RelationProxy({self._ref_alias})"
        return f"RelationProxy({self._relation_name})"


class RelationRef:
    """Independent reference to a relation for self-joins. Created by Relation.refs(n)."""

    def __init__(self, relation_cls: type[Relation], alias: str) -> None:
        self._relation_cls = relation_cls
        self._alias = alias
        from inputlayer.relation import Relation as RelBase

        self._relation_name = RelBase._resolve_name(relation_cls)

    @property
    def alias(self) -> str:
        return self._alias

    @property
    def relation_name(self) -> str:
        return self._relation_name

    @property
    def relation_cls(self) -> type[Relation]:
        return self._relation_cls

    def __getattr__(self, name: str) -> ColumnProxy:
        if name.startswith("_"):
            raise AttributeError(name)
        return ColumnProxy(self._relation_name, name, ref_alias=self._alias)

    def __repr__(self) -> str:
        return f"RelationRef({self._relation_name} as {self._alias})"


# ── BoolExpr combinator operators ─────────────────────────────────────
# Monkey-patch & | ~ on BoolExpr subclasses so cond1 & cond2 works.

def _bool_and(self: BoolExpr, other: BoolExpr) -> And:
    return And(self, other)


def _bool_or(self: BoolExpr, other: BoolExpr) -> Or:
    return Or(self, other)


def _bool_not(self: BoolExpr) -> Not:
    return Not(self)


for _cls in (Comparison, And, Or, Not, InExpr, NegatedIn, MatchExpr):
    _cls.__and__ = _bool_and  # type: ignore[attr-defined]
    _cls.__or__ = _bool_or  # type: ignore[attr-defined]
    _cls.__invert__ = _bool_not  # type: ignore[attr-defined]


# ── Helpers ───────────────────────────────────────────────────────────

def _wrap(value: Any) -> Expr:
    """Wrap a raw Python value or proxy into an AST Expr."""
    if isinstance(value, ColumnProxy):
        return value._to_ast()
    if isinstance(value, Expr):
        return value
    return Literal(value)
