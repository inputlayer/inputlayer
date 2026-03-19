/**
 * Aggregation functions that compile to IQL aggregates.
 */

import { type Expr, type AggExpr, aggExpr } from './ast.js';
import type { ColumnProxy } from './proxy.js';
import { wrap } from './proxy.js';

function toExpr(col: ColumnProxy | Expr): Expr {
  if ('toAst' in col && typeof (col as ColumnProxy).toAst === 'function') {
    return (col as ColumnProxy).toAst();
  }
  return col as Expr;
}

/**
 * Count rows. If a column is given, counts non-null values.
 * IQL: count<Var>
 */
export function count(column?: ColumnProxy | Expr): AggExpr {
  if (column === undefined) {
    return aggExpr({ func: 'count' });
  }
  return aggExpr({ func: 'count', column: toExpr(column) });
}

/**
 * Count distinct values.
 * IQL: count_distinct<Var>
 */
export function countDistinct(column: ColumnProxy | Expr): AggExpr {
  return aggExpr({ func: 'count_distinct', column: toExpr(column) });
}

/**
 * Sum numeric values.
 * IQL: sum<Var>
 */
export function sum(column: ColumnProxy | Expr): AggExpr {
  return aggExpr({ func: 'sum', column: toExpr(column) });
}

/**
 * Minimum value.
 * IQL: min<Var>
 */
export function min(column: ColumnProxy | Expr): AggExpr {
  return aggExpr({ func: 'min', column: toExpr(column) });
}

/**
 * Maximum value.
 * IQL: max<Var>
 */
export function max(column: ColumnProxy | Expr): AggExpr {
  return aggExpr({ func: 'max', column: toExpr(column) });
}

/**
 * Average value.
 * IQL: avg<Var>
 */
export function avg(column: ColumnProxy | Expr): AggExpr {
  return aggExpr({ func: 'avg', column: toExpr(column) });
}

/**
 * Top-K aggregation with ordering.
 * IQL: top_k<k, Passthrough..., OrderCol:desc>
 */
export function topK(opts: {
  k: number;
  passthrough?: Array<ColumnProxy | Expr>;
  orderBy: ColumnProxy | Expr;
  desc?: boolean;
}): AggExpr {
  return aggExpr({
    func: 'top_k',
    params: [opts.k],
    passthrough: (opts.passthrough ?? []).map(toExpr),
    orderColumn: toExpr(opts.orderBy),
    desc: opts.desc ?? true,
  });
}

/**
 * Top-K with threshold aggregation.
 * IQL: top_k_threshold<k, threshold, Passthrough..., OrderCol:desc>
 */
export function topKThreshold(opts: {
  k: number;
  threshold: number;
  passthrough?: Array<ColumnProxy | Expr>;
  orderBy: ColumnProxy | Expr;
  desc?: boolean;
}): AggExpr {
  return aggExpr({
    func: 'top_k_threshold',
    params: [opts.k, opts.threshold],
    passthrough: (opts.passthrough ?? []).map(toExpr),
    orderColumn: toExpr(opts.orderBy),
    desc: opts.desc ?? true,
  });
}

/**
 * Within-radius aggregation.
 * IQL: within_radius<r, Passthrough..., DistCol:asc>
 */
export function withinRadius(opts: {
  maxDistance: number;
  passthrough?: Array<ColumnProxy | Expr>;
  distance: ColumnProxy | Expr;
  asc?: boolean;
}): AggExpr {
  return aggExpr({
    func: 'within_radius',
    params: [opts.maxDistance],
    passthrough: (opts.passthrough ?? []).map(toExpr),
    orderColumn: toExpr(opts.distance),
    desc: !(opts.asc ?? true),
  });
}
