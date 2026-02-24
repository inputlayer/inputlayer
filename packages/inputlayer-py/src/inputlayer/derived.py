"""Derived relations and the From/Where/Select rule builder."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from inputlayer._ast import BoolExpr, Expr
from inputlayer._proxy import ColumnProxy, RelationProxy, RelationRef
from inputlayer.relation import Relation

if TYPE_CHECKING:
    pass


class Derived(Relation):
    """Base class for derived (rule-computed) relations.

    Subclass with ``rules: ClassVar[list[RuleClause]]``::

        class Reachable(Derived):
            src: int
            dst: int

            rules = [
                From(Edge).select(src=Edge.x, dst=Edge.y),
                From(Reachable, Edge)
                    .where(lambda r, e: r.dst == e.x)
                    .select(src=Reachable.src, dst=Edge.y),
            ]
    """

    rules: ClassVar[list[RuleClause]]


@dataclass
class RuleClause:
    """A single compiled rule clause: head column map + body relations + condition."""

    relations: list[tuple[str, type[Relation], str | None]]  # (name, cls, alias)
    select_map: dict[str, Expr]  # head_column → body Expr
    condition: BoolExpr | None = None


@dataclass
class _FromBase:
    """Internal: holds the relations for a From(...) builder."""

    _relations: list[tuple[str, type[Relation], str | None]]

    def _build_proxy_args(self) -> list[RelationProxy]:
        """Build proxy objects matching the From(...) arguments."""
        proxies = []
        for rn, cls, alias in self._relations:
            proxies.append(RelationProxy(rn, ref_alias=alias))
        return proxies


class FromWhere(_FromBase):
    """Intermediate builder after .where() - only .select() remains."""

    _condition: BoolExpr

    def __init__(
        self,
        relations: list[tuple[str, type[Relation], str | None]],
        condition: BoolExpr,
    ) -> None:
        self._relations = relations
        self._condition = condition

    def select(self, **columns: ColumnProxy | Expr) -> RuleClause:
        """Map derived columns to body expressions.

        Keyword argument names must match the Derived class field names.
        """
        select_map: dict[str, Expr] = {}
        for name, val in columns.items():
            if isinstance(val, ColumnProxy):
                select_map[name] = val._to_ast()
            elif isinstance(val, Expr):
                select_map[name] = val
            else:
                raise TypeError(
                    f"select() value for '{name}' must be a Column or Expr, "
                    f"got {type(val).__name__}"
                )
        return RuleClause(
            relations=self._relations,
            select_map=select_map,
            condition=self._condition,
        )


class From(_FromBase):
    """Rule builder: From(Relation1, Relation2, ...).where(...).select(...)"""

    def __init__(self, *relations: type[Relation] | RelationRef) -> None:
        self._relations = []
        for r in relations:
            if isinstance(r, RelationRef):
                self._relations.append((r.relation_name, r.relation_cls, r.alias))
            elif isinstance(r, type) and issubclass(r, Relation):
                rn = Relation._resolve_name(r)
                self._relations.append((rn, r, None))
            else:
                raise TypeError(
                    f"From() expects Relation subclasses or RelationRef, "
                    f"got {type(r).__name__}"
                )

    def where(self, condition: Any) -> FromWhere:
        """Add a filter condition. Accepts a BoolExpr or a lambda taking proxies."""
        if callable(condition) and not isinstance(condition, BoolExpr):
            proxies = self._build_proxy_args()
            condition = condition(*proxies)
        if not isinstance(condition, BoolExpr):
            raise TypeError(
                f"where() condition must be a BoolExpr or callable returning BoolExpr, "
                f"got {type(condition).__name__}"
            )
        return FromWhere(self._relations, condition)

    def select(self, **columns: ColumnProxy | Expr) -> RuleClause:
        """Map derived columns to body expressions (no filter)."""
        select_map: dict[str, Expr] = {}
        for name, val in columns.items():
            if isinstance(val, ColumnProxy):
                select_map[name] = val._to_ast()
            elif isinstance(val, Expr):
                select_map[name] = val
            else:
                raise TypeError(
                    f"select() value for '{name}' must be a Column or Expr, "
                    f"got {type(val).__name__}"
                )
        return RuleClause(
            relations=self._relations,
            select_map=select_map,
            condition=None,
        )
