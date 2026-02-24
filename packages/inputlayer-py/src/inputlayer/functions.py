"""Built-in functions that compile to Datalog function calls.

Each function returns a FuncCall AST node (an Expr) that the compiler
serialises as ``func_name(arg1, arg2, ...)``.
"""

from __future__ import annotations

from inputlayer._ast import Expr, FuncCall, Literal
from inputlayer._proxy import ColumnProxy


def _e(v: ColumnProxy | Expr | int | float | str | list) -> Expr:
    """Coerce a value to an Expr."""
    if isinstance(v, ColumnProxy):
        return v._to_ast()
    if isinstance(v, Expr):
        return v
    return Literal(v)


# ── Distance (4) ──────────────────────────────────────────────────────

def euclidean(v1: ColumnProxy | Expr, v2: ColumnProxy | Expr | list) -> FuncCall:
    return FuncCall("euclidean", (_e(v1), _e(v2)))

def cosine(v1: ColumnProxy | Expr, v2: ColumnProxy | Expr | list) -> FuncCall:
    return FuncCall("cosine", (_e(v1), _e(v2)))

def dot(v1: ColumnProxy | Expr, v2: ColumnProxy | Expr | list) -> FuncCall:
    return FuncCall("dot", (_e(v1), _e(v2)))

def manhattan(v1: ColumnProxy | Expr, v2: ColumnProxy | Expr | list) -> FuncCall:
    return FuncCall("manhattan", (_e(v1), _e(v2)))


# ── Vector Operations (4) ────────────────────────────────────────────

def normalize(v: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("normalize", (_e(v),))

def vec_dim(v: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("vec_dim", (_e(v),))

def vec_add(v1: ColumnProxy | Expr, v2: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("vec_add", (_e(v1), _e(v2)))

def vec_scale(v: ColumnProxy | Expr, s: ColumnProxy | Expr | float) -> FuncCall:
    return FuncCall("vec_scale", (_e(v), _e(s)))


# ── LSH (3) ───────────────────────────────────────────────────────────

def lsh_bucket(
    v: ColumnProxy | Expr,
    table_idx: ColumnProxy | Expr | int,
    num_hp: ColumnProxy | Expr | int,
) -> FuncCall:
    return FuncCall("lsh_bucket", (_e(v), _e(table_idx), _e(num_hp)))

def lsh_probes(
    bucket: ColumnProxy | Expr | int,
    num_hp: ColumnProxy | Expr | int,
    num_probes: ColumnProxy | Expr | int,
) -> FuncCall:
    return FuncCall("lsh_probes", (_e(bucket), _e(num_hp), _e(num_probes)))

def lsh_multi_probe(
    v: ColumnProxy | Expr,
    table_idx: ColumnProxy | Expr | int,
    num_hp: ColumnProxy | Expr | int,
    num_probes: ColumnProxy | Expr | int,
) -> FuncCall:
    return FuncCall("lsh_multi_probe", (_e(v), _e(table_idx), _e(num_hp), _e(num_probes)))


# ── Quantization (4) ─────────────────────────────────────────────────

def quantize_linear(v: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("quantize_linear", (_e(v),))

def quantize_symmetric(v: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("quantize_symmetric", (_e(v),))

def dequantize(v: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("dequantize", (_e(v),))

def dequantize_scaled(v: ColumnProxy | Expr, s: ColumnProxy | Expr | float) -> FuncCall:
    return FuncCall("dequantize_scaled", (_e(v), _e(s)))


# ── Int8 Distance (4) ────────────────────────────────────────────────

def euclidean_int8(v1: ColumnProxy | Expr, v2: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("euclidean_int8", (_e(v1), _e(v2)))

def cosine_int8(v1: ColumnProxy | Expr, v2: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("cosine_int8", (_e(v1), _e(v2)))

def dot_int8(v1: ColumnProxy | Expr, v2: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("dot_int8", (_e(v1), _e(v2)))

def manhattan_int8(v1: ColumnProxy | Expr, v2: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("manhattan_int8", (_e(v1), _e(v2)))


# ── Temporal (14) ─────────────────────────────────────────────────────

def time_now() -> FuncCall:
    return FuncCall("time_now", ())

def time_diff(t1: ColumnProxy | Expr, t2: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("time_diff", (_e(t1), _e(t2)))

def time_add(ts: ColumnProxy | Expr, dur: ColumnProxy | Expr | int) -> FuncCall:
    return FuncCall("time_add", (_e(ts), _e(dur)))

def time_sub(ts: ColumnProxy | Expr, dur: ColumnProxy | Expr | int) -> FuncCall:
    return FuncCall("time_sub", (_e(ts), _e(dur)))

def time_decay(
    ts: ColumnProxy | Expr,
    now: ColumnProxy | Expr,
    half_life: ColumnProxy | Expr | int,
) -> FuncCall:
    return FuncCall("time_decay", (_e(ts), _e(now), _e(half_life)))

def time_decay_linear(
    ts: ColumnProxy | Expr,
    now: ColumnProxy | Expr,
    max_age: ColumnProxy | Expr | int,
) -> FuncCall:
    return FuncCall("time_decay_linear", (_e(ts), _e(now), _e(max_age)))

def time_before(t1: ColumnProxy | Expr, t2: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("time_before", (_e(t1), _e(t2)))

def time_after(t1: ColumnProxy | Expr, t2: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("time_after", (_e(t1), _e(t2)))

def time_between(
    ts: ColumnProxy | Expr,
    start: ColumnProxy | Expr,
    end: ColumnProxy | Expr,
) -> FuncCall:
    return FuncCall("time_between", (_e(ts), _e(start), _e(end)))

def within_last(
    ts: ColumnProxy | Expr,
    now: ColumnProxy | Expr,
    dur: ColumnProxy | Expr | int,
) -> FuncCall:
    return FuncCall("within_last", (_e(ts), _e(now), _e(dur)))

def intervals_overlap(
    s1: ColumnProxy | Expr, e1: ColumnProxy | Expr,
    s2: ColumnProxy | Expr, e2: ColumnProxy | Expr,
) -> FuncCall:
    return FuncCall("intervals_overlap", (_e(s1), _e(e1), _e(s2), _e(e2)))

def interval_contains(
    s1: ColumnProxy | Expr, e1: ColumnProxy | Expr,
    s2: ColumnProxy | Expr, e2: ColumnProxy | Expr,
) -> FuncCall:
    return FuncCall("interval_contains", (_e(s1), _e(e1), _e(s2), _e(e2)))

def interval_duration(
    s: ColumnProxy | Expr, e: ColumnProxy | Expr,
) -> FuncCall:
    return FuncCall("interval_duration", (_e(s), _e(e)))

def point_in_interval(
    ts: ColumnProxy | Expr,
    s: ColumnProxy | Expr,
    e: ColumnProxy | Expr,
) -> FuncCall:
    return FuncCall("point_in_interval", (_e(ts), _e(s), _e(e)))


# ── Math (15) ─────────────────────────────────────────────────────────

def abs_(x: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("abs", (_e(x),))

def abs_int64(x: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("abs_int64", (_e(x),))

def abs_float64(x: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("abs_float64", (_e(x),))

def sqrt(x: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("sqrt", (_e(x),))

def pow_(base: ColumnProxy | Expr, exp: ColumnProxy | Expr | float) -> FuncCall:
    return FuncCall("pow", (_e(base), _e(exp)))

def log(x: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("log", (_e(x),))

def exp(x: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("exp", (_e(x),))

def sin(x: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("sin", (_e(x),))

def cos(x: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("cos", (_e(x),))

def tan(x: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("tan", (_e(x),))

def floor(x: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("floor", (_e(x),))

def ceil(x: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("ceil", (_e(x),))

def sign(x: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("sign", (_e(x),))

def min_val(a: ColumnProxy | Expr, b: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("min_val", (_e(a), _e(b)))

def max_val(a: ColumnProxy | Expr, b: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("max_val", (_e(a), _e(b)))


# ── String (7) ────────────────────────────────────────────────────────

def len_(s: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("len", (_e(s),))

def upper(s: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("upper", (_e(s),))

def lower(s: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("lower", (_e(s),))

def trim(s: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("trim", (_e(s),))

def substr(
    s: ColumnProxy | Expr,
    start: ColumnProxy | Expr | int,
    length: ColumnProxy | Expr | int,
) -> FuncCall:
    return FuncCall("substr", (_e(s), _e(start), _e(length)))

def replace(
    s: ColumnProxy | Expr,
    find: ColumnProxy | Expr | str,
    repl: ColumnProxy | Expr | str,
) -> FuncCall:
    return FuncCall("replace", (_e(s), _e(find), _e(repl)))

def concat(*args: ColumnProxy | Expr | str) -> FuncCall:
    return FuncCall("concat", tuple(_e(a) for a in args))


# ── Type Conversion (2) ──────────────────────────────────────────────

def to_float(x: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("to_float", (_e(x),))

def to_int(x: ColumnProxy | Expr) -> FuncCall:
    return FuncCall("to_int", (_e(x),))


# ── HNSW Direct (1) ──────────────────────────────────────────────────

def hnsw_nearest(
    index_name: str,
    query_vec: ColumnProxy | Expr | list,
    k: int,
    *,
    ef_search: int | None = None,
) -> FuncCall:
    """Direct HNSW nearest-neighbor search.

    Datalog: hnsw_nearest("idx", [0.1, 0.2], 10, Id, Dist)
    """
    args: list[Expr] = [Literal(index_name), _e(query_vec), Literal(k)]
    if ef_search is not None:
        args.append(Literal(ef_search))
    return FuncCall("hnsw_nearest", tuple(args))
