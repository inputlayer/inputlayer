/**
 * Built-in functions that compile to IQL function calls.
 *
 * Each function returns a FuncCall AST node (an Expr) that the compiler
 * serializes as func_name(arg1, arg2, ...).
 */

import { type Expr, type FuncCall, funcCall, literal } from './ast.js';
import type { ColumnProxy } from './proxy.js';
import { wrap } from './proxy.js';

type ExprArg = ColumnProxy | Expr | number | string | number[];

function e(v: ExprArg): Expr {
  if (typeof v === 'object' && v !== null && 'toAst' in v && typeof (v as ColumnProxy).toAst === 'function') {
    return (v as ColumnProxy).toAst();
  }
  if (typeof v === 'object' && v !== null && !Array.isArray(v) && '_tag' in (v as unknown as Record<string, unknown>)) {
    return v as unknown as Expr;
  }
  return literal(v);
}

// ── Distance (4) ────────────────────────────────────────────────────

export function euclidean(v1: ExprArg, v2: ExprArg): FuncCall {
  return funcCall('euclidean', [e(v1), e(v2)]);
}

export function cosine(v1: ExprArg, v2: ExprArg): FuncCall {
  return funcCall('cosine', [e(v1), e(v2)]);
}

export function dot(v1: ExprArg, v2: ExprArg): FuncCall {
  return funcCall('dot', [e(v1), e(v2)]);
}

export function manhattan(v1: ExprArg, v2: ExprArg): FuncCall {
  return funcCall('manhattan', [e(v1), e(v2)]);
}

// ── Vector Operations (4) ──────────────────────────────────────────

export function normalize(v: ExprArg): FuncCall {
  return funcCall('normalize', [e(v)]);
}

export function vecDim(v: ExprArg): FuncCall {
  return funcCall('vec_dim', [e(v)]);
}

export function vecAdd(v1: ExprArg, v2: ExprArg): FuncCall {
  return funcCall('vec_add', [e(v1), e(v2)]);
}

export function vecScale(v: ExprArg, s: ExprArg): FuncCall {
  return funcCall('vec_scale', [e(v), e(s)]);
}

// ── LSH (3) ────────────────────────────────────────────────────────

export function lshBucket(v: ExprArg, tableIdx: ExprArg, numHp: ExprArg): FuncCall {
  return funcCall('lsh_bucket', [e(v), e(tableIdx), e(numHp)]);
}

export function lshProbes(bucket: ExprArg, numHp: ExprArg, numProbes: ExprArg): FuncCall {
  return funcCall('lsh_probes', [e(bucket), e(numHp), e(numProbes)]);
}

export function lshMultiProbe(
  v: ExprArg,
  tableIdx: ExprArg,
  numHp: ExprArg,
  numProbes: ExprArg,
): FuncCall {
  return funcCall('lsh_multi_probe', [e(v), e(tableIdx), e(numHp), e(numProbes)]);
}

// ── Quantization (4) ───────────────────────────────────────────────

export function quantizeLinear(v: ExprArg): FuncCall {
  return funcCall('quantize_linear', [e(v)]);
}

export function quantizeSymmetric(v: ExprArg): FuncCall {
  return funcCall('quantize_symmetric', [e(v)]);
}

export function dequantize(v: ExprArg): FuncCall {
  return funcCall('dequantize', [e(v)]);
}

export function dequantizeScaled(v: ExprArg, s: ExprArg): FuncCall {
  return funcCall('dequantize_scaled', [e(v), e(s)]);
}

// ── Int8 Distance (4) ──────────────────────────────────────────────

export function euclideanInt8(v1: ExprArg, v2: ExprArg): FuncCall {
  return funcCall('euclidean_int8', [e(v1), e(v2)]);
}

export function cosineInt8(v1: ExprArg, v2: ExprArg): FuncCall {
  return funcCall('cosine_int8', [e(v1), e(v2)]);
}

export function dotInt8(v1: ExprArg, v2: ExprArg): FuncCall {
  return funcCall('dot_int8', [e(v1), e(v2)]);
}

export function manhattanInt8(v1: ExprArg, v2: ExprArg): FuncCall {
  return funcCall('manhattan_int8', [e(v1), e(v2)]);
}

// ── Temporal (14) ──────────────────────────────────────────────────

export function timeNow(): FuncCall {
  return funcCall('time_now', []);
}

export function timeDiff(t1: ExprArg, t2: ExprArg): FuncCall {
  return funcCall('time_diff', [e(t1), e(t2)]);
}

export function timeAdd(ts: ExprArg, dur: ExprArg): FuncCall {
  return funcCall('time_add', [e(ts), e(dur)]);
}

export function timeSub(ts: ExprArg, dur: ExprArg): FuncCall {
  return funcCall('time_sub', [e(ts), e(dur)]);
}

export function timeDecay(ts: ExprArg, now: ExprArg, halfLife: ExprArg): FuncCall {
  return funcCall('time_decay', [e(ts), e(now), e(halfLife)]);
}

export function timeDecayLinear(ts: ExprArg, now: ExprArg, maxAge: ExprArg): FuncCall {
  return funcCall('time_decay_linear', [e(ts), e(now), e(maxAge)]);
}

export function timeBefore(t1: ExprArg, t2: ExprArg): FuncCall {
  return funcCall('time_before', [e(t1), e(t2)]);
}

export function timeAfter(t1: ExprArg, t2: ExprArg): FuncCall {
  return funcCall('time_after', [e(t1), e(t2)]);
}

export function timeBetween(ts: ExprArg, start: ExprArg, end: ExprArg): FuncCall {
  return funcCall('time_between', [e(ts), e(start), e(end)]);
}

export function withinLast(ts: ExprArg, now: ExprArg, dur: ExprArg): FuncCall {
  return funcCall('within_last', [e(ts), e(now), e(dur)]);
}

export function intervalsOverlap(
  s1: ExprArg,
  e1: ExprArg,
  s2: ExprArg,
  e2: ExprArg,
): FuncCall {
  return funcCall('intervals_overlap', [e(s1), e(e1), e(s2), e(e2)]);
}

export function intervalContains(
  s1: ExprArg,
  e1Arg: ExprArg,
  s2: ExprArg,
  e2: ExprArg,
): FuncCall {
  return funcCall('interval_contains', [e(s1), e(e1Arg), e(s2), e(e2)]);
}

export function intervalDuration(s: ExprArg, end: ExprArg): FuncCall {
  return funcCall('interval_duration', [e(s), e(end)]);
}

export function pointInInterval(ts: ExprArg, s: ExprArg, end: ExprArg): FuncCall {
  return funcCall('point_in_interval', [e(ts), e(s), e(end)]);
}

// ── Math (15) ──────────────────────────────────────────────────────

export function abs(x: ExprArg): FuncCall {
  return funcCall('abs', [e(x)]);
}

export function absInt64(x: ExprArg): FuncCall {
  return funcCall('abs_int64', [e(x)]);
}

export function absFloat64(x: ExprArg): FuncCall {
  return funcCall('abs_float64', [e(x)]);
}

export function sqrt(x: ExprArg): FuncCall {
  return funcCall('sqrt', [e(x)]);
}

export function pow(base: ExprArg, exp: ExprArg): FuncCall {
  return funcCall('pow', [e(base), e(exp)]);
}

export function log(x: ExprArg): FuncCall {
  return funcCall('log', [e(x)]);
}

export function exp(x: ExprArg): FuncCall {
  return funcCall('exp', [e(x)]);
}

export function sin(x: ExprArg): FuncCall {
  return funcCall('sin', [e(x)]);
}

export function cos(x: ExprArg): FuncCall {
  return funcCall('cos', [e(x)]);
}

export function tan(x: ExprArg): FuncCall {
  return funcCall('tan', [e(x)]);
}

export function floor(x: ExprArg): FuncCall {
  return funcCall('floor', [e(x)]);
}

export function ceil(x: ExprArg): FuncCall {
  return funcCall('ceil', [e(x)]);
}

export function sign(x: ExprArg): FuncCall {
  return funcCall('sign', [e(x)]);
}

export function minVal(a: ExprArg, b: ExprArg): FuncCall {
  return funcCall('min_val', [e(a), e(b)]);
}

export function maxVal(a: ExprArg, b: ExprArg): FuncCall {
  return funcCall('max_val', [e(a), e(b)]);
}

// ── String (7) ─────────────────────────────────────────────────────

export function len(s: ExprArg): FuncCall {
  return funcCall('len', [e(s)]);
}

export function upper(s: ExprArg): FuncCall {
  return funcCall('upper', [e(s)]);
}

export function lower(s: ExprArg): FuncCall {
  return funcCall('lower', [e(s)]);
}

export function trim(s: ExprArg): FuncCall {
  return funcCall('trim', [e(s)]);
}

export function substr(s: ExprArg, start: ExprArg, length: ExprArg): FuncCall {
  return funcCall('substr', [e(s), e(start), e(length)]);
}

export function replace(s: ExprArg, find: ExprArg, repl: ExprArg): FuncCall {
  return funcCall('replace', [e(s), e(find), e(repl)]);
}

export function concat(...args: ExprArg[]): FuncCall {
  return funcCall('concat', args.map(e));
}

// ── Type Conversion (2) ───────────────────────────────────────────

export function toFloat(x: ExprArg): FuncCall {
  return funcCall('to_float', [e(x)]);
}

export function toInt(x: ExprArg): FuncCall {
  return funcCall('to_int', [e(x)]);
}

// ── HNSW Direct (1) ───────────────────────────────────────────────

/**
 * Direct HNSW nearest-neighbor search.
 * IQL: hnsw_nearest("idx", [0.1, 0.2], 10, Id, Dist)
 */
export function hnswNearest(
  indexName: string,
  queryVec: ExprArg,
  k: number,
  opts?: { efSearch?: number },
): FuncCall {
  const args: Expr[] = [literal(indexName), e(queryVec), literal(k)];
  if (opts?.efSearch !== undefined) {
    args.push(literal(opts.efSearch));
  }
  return funcCall('hnsw_nearest', args);
}
