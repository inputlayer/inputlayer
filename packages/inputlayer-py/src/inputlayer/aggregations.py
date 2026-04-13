"""Aggregation functions that compile to IQL aggregates."""

from __future__ import annotations

from typing import TYPE_CHECKING

from inputlayer._ast import AggExpr, Expr
from inputlayer._proxy import ColumnProxy

if TYPE_CHECKING:
    from inputlayer.relation import Relation


def _to_expr(col: ColumnProxy | Expr) -> Expr:
    if isinstance(col, ColumnProxy):
        return col._to_ast()
    return col


def count(column: ColumnProxy | type[Relation] | None = None) -> AggExpr:
    """Count rows. If a column is given, counts non-null values.

    IQL: count<Var>
    """
    if column is None or (isinstance(column, type)):
        # count(*) - needs at least one column from the body
        return AggExpr(func="count", column=None)
    return AggExpr(func="count", column=_to_expr(column))


def count_distinct(column: ColumnProxy) -> AggExpr:
    """Count distinct values.

    IQL: count_distinct<Var>
    """
    return AggExpr(func="count_distinct", column=_to_expr(column))


def sum_(column: ColumnProxy) -> AggExpr:
    """Sum numeric values.

    IQL: sum<Var>
    """
    return AggExpr(func="sum", column=_to_expr(column))


def min_(column: ColumnProxy) -> AggExpr:
    """Minimum value.

    IQL: min<Var>
    """
    return AggExpr(func="min", column=_to_expr(column))


def max_(column: ColumnProxy) -> AggExpr:
    """Maximum value.

    IQL: max<Var>
    """
    return AggExpr(func="max", column=_to_expr(column))


def avg(column: ColumnProxy) -> AggExpr:
    """Average value.

    IQL: avg<Var>
    """
    return AggExpr(func="avg", column=_to_expr(column))


def top_k(
    k: int,
    *passthrough: ColumnProxy,
    order_by: ColumnProxy,
    desc: bool = True,
) -> AggExpr:
    """Top-K aggregation with ordering.

    IQL: top_k<k, Passthrough..., OrderCol:desc>
    """
    return AggExpr(
        func="top_k",
        params=(k,),
        passthrough=tuple(_to_expr(p) for p in passthrough),
        order_column=_to_expr(order_by),
        desc=desc,
    )


def top_k_threshold(
    k: int,
    threshold: float,
    *passthrough: ColumnProxy,
    order_by: ColumnProxy,
    desc: bool = True,
) -> AggExpr:
    """Top-K with threshold aggregation.

    IQL: top_k_threshold<k, threshold, Passthrough..., OrderCol:desc>
    """
    return AggExpr(
        func="top_k_threshold",
        params=(k, threshold),
        passthrough=tuple(_to_expr(p) for p in passthrough),
        order_column=_to_expr(order_by),
        desc=desc,
    )


def within_radius(
    max_distance: float,
    *passthrough: ColumnProxy,
    distance: ColumnProxy | Expr,
    asc: bool = True,
) -> AggExpr:
    """Within-radius aggregation.

    IQL: within_radius<r, Passthrough..., DistCol:asc>
    """
    return AggExpr(
        func="within_radius",
        params=(max_distance,),
        passthrough=tuple(_to_expr(p) for p in passthrough),
        order_column=_to_expr(distance),
        desc=not asc,
    )
