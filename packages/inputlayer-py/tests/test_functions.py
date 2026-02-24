"""Tests for inputlayer.functions - 55+ built-in functions → FuncCall AST."""

from inputlayer._ast import FuncCall, Literal
from inputlayer._proxy import ColumnProxy
from inputlayer.compiler import _VarEnv, compile_expr
from inputlayer.functions import (
    abs_,
    abs_float64,
    abs_int64,
    ceil,
    concat,
    cos,
    cosine,
    cosine_int8,
    dequantize,
    dequantize_scaled,
    dot,
    dot_int8,
    euclidean,
    euclidean_int8,
    exp,
    floor,
    hnsw_nearest,
    interval_contains,
    interval_duration,
    intervals_overlap,
    len_,
    log,
    lower,
    lsh_bucket,
    lsh_multi_probe,
    lsh_probes,
    manhattan,
    manhattan_int8,
    max_val,
    min_val,
    normalize,
    point_in_interval,
    pow_,
    quantize_linear,
    quantize_symmetric,
    replace,
    sign,
    sin,
    sqrt,
    substr,
    tan,
    time_add,
    time_after,
    time_before,
    time_between,
    time_decay,
    time_decay_linear,
    time_diff,
    time_now,
    time_sub,
    to_float,
    to_int,
    trim,
    upper,
    vec_add,
    vec_dim,
    vec_scale,
    within_last,
)


def _col(r: str, n: str) -> ColumnProxy:
    return ColumnProxy(r, n)


def _compile(fc: FuncCall) -> str:
    env = _VarEnv()
    return compile_expr(fc, env)


# ── Distance ──────────────────────────────────────────────────────────

class TestDistance:
    def test_euclidean(self):
        assert _compile(euclidean(_col("d", "v1"), _col("d", "v2"))) == "euclidean(V1, V2)"

    def test_cosine(self):
        assert _compile(cosine(_col("d", "v1"), _col("d", "v2"))) == "cosine(V1, V2)"

    def test_dot(self):
        assert _compile(dot(_col("d", "v1"), _col("d", "v2"))) == "dot(V1, V2)"

    def test_manhattan(self):
        assert _compile(manhattan(_col("d", "v1"), _col("d", "v2"))) == "manhattan(V1, V2)"

    def test_cosine_with_literal(self):
        result = _compile(cosine(_col("d", "embedding"), [1.0, 2.0, 3.0]))
        assert result == "cosine(Embedding, [1.0, 2.0, 3.0])"


# ── Vector Ops ────────────────────────────────────────────────────────

class TestVectorOps:
    def test_normalize(self):
        assert _compile(normalize(_col("d", "v"))) == "normalize(V)"

    def test_vec_dim(self):
        assert _compile(vec_dim(_col("d", "v"))) == "vec_dim(V)"

    def test_vec_add(self):
        assert _compile(vec_add(_col("d", "v1"), _col("d", "v2"))) == "vec_add(V1, V2)"

    def test_vec_scale(self):
        assert _compile(vec_scale(_col("d", "v"), 2.0)) == "vec_scale(V, 2.0)"


# ── LSH ───────────────────────────────────────────────────────────────

class TestLSH:
    def test_lsh_bucket(self):
        assert _compile(lsh_bucket(_col("d", "v"), 0, 8)) == "lsh_bucket(V, 0, 8)"

    def test_lsh_probes(self):
        assert _compile(lsh_probes(42, 8, 3)) == "lsh_probes(42, 8, 3)"

    def test_lsh_multi_probe(self):
        assert _compile(lsh_multi_probe(_col("d", "v"), 0, 8, 3)) == "lsh_multi_probe(V, 0, 8, 3)"


# ── Quantization ──────────────────────────────────────────────────────

class TestQuantization:
    def test_quantize_linear(self):
        assert _compile(quantize_linear(_col("d", "v"))) == "quantize_linear(V)"

    def test_quantize_symmetric(self):
        assert _compile(quantize_symmetric(_col("d", "v"))) == "quantize_symmetric(V)"

    def test_dequantize(self):
        assert _compile(dequantize(_col("d", "v"))) == "dequantize(V)"

    def test_dequantize_scaled(self):
        assert _compile(dequantize_scaled(_col("d", "v"), 1.5)) == "dequantize_scaled(V, 1.5)"


# ── Int8 Distance ─────────────────────────────────────────────────────

class TestInt8Distance:
    def test_euclidean_int8(self):
        assert _compile(euclidean_int8(_col("d", "v1"), _col("d", "v2"))) == "euclidean_int8(V1, V2)"

    def test_cosine_int8(self):
        assert _compile(cosine_int8(_col("d", "v1"), _col("d", "v2"))) == "cosine_int8(V1, V2)"

    def test_dot_int8(self):
        assert _compile(dot_int8(_col("d", "v1"), _col("d", "v2"))) == "dot_int8(V1, V2)"

    def test_manhattan_int8(self):
        assert _compile(manhattan_int8(_col("d", "v1"), _col("d", "v2"))) == "manhattan_int8(V1, V2)"


# ── Temporal ──────────────────────────────────────────────────────────

class TestTemporal:
    def test_time_now(self):
        assert _compile(time_now()) == "time_now()"

    def test_time_diff(self):
        assert _compile(time_diff(_col("e", "t1"), _col("e", "t2"))) == "time_diff(T1, T2)"

    def test_time_add(self):
        assert _compile(time_add(_col("e", "ts"), 1000)) == "time_add(Ts, 1000)"

    def test_time_sub(self):
        assert _compile(time_sub(_col("e", "ts"), 1000)) == "time_sub(Ts, 1000)"

    def test_time_decay(self):
        result = _compile(time_decay(_col("e", "ts"), _col("e", "now"), 3600000))
        assert result == "time_decay(Ts, Now, 3600000)"

    def test_time_decay_linear(self):
        result = _compile(time_decay_linear(_col("e", "ts"), _col("e", "now"), 86400000))
        assert result == "time_decay_linear(Ts, Now, 86400000)"

    def test_time_before(self):
        assert _compile(time_before(_col("e", "t1"), _col("e", "t2"))) == "time_before(T1, T2)"

    def test_time_after(self):
        assert _compile(time_after(_col("e", "t1"), _col("e", "t2"))) == "time_after(T1, T2)"

    def test_time_between(self):
        result = _compile(time_between(_col("e", "ts"), _col("e", "start"), _col("e", "end")))
        assert result == "time_between(Ts, Start, End)"

    def test_within_last(self):
        result = _compile(within_last(_col("e", "ts"), _col("e", "now"), 86400000))
        assert result == "within_last(Ts, Now, 86400000)"

    def test_intervals_overlap(self):
        result = _compile(intervals_overlap(
            _col("e", "s1"), _col("e", "e1"), _col("e", "s2"), _col("e", "e2"),
        ))
        assert result == "intervals_overlap(S1, E1, S2, E2)"

    def test_interval_contains(self):
        result = _compile(interval_contains(
            _col("e", "s1"), _col("e", "e1"), _col("e", "s2"), _col("e", "e2"),
        ))
        assert result == "interval_contains(S1, E1, S2, E2)"

    def test_interval_duration(self):
        assert _compile(interval_duration(_col("e", "start"), _col("e", "end"))) == "interval_duration(Start, End)"

    def test_point_in_interval(self):
        result = _compile(point_in_interval(_col("e", "ts"), _col("e", "start"), _col("e", "end")))
        assert result == "point_in_interval(Ts, Start, End)"


# ── Math ──────────────────────────────────────────────────────────────

class TestMath:
    def test_abs(self):
        assert _compile(abs_(_col("e", "x"))) == "abs(X)"

    def test_abs_int64(self):
        assert _compile(abs_int64(_col("e", "x"))) == "abs_int64(X)"

    def test_abs_float64(self):
        assert _compile(abs_float64(_col("e", "x"))) == "abs_float64(X)"

    def test_sqrt(self):
        assert _compile(sqrt(_col("e", "x"))) == "sqrt(X)"

    def test_pow(self):
        assert _compile(pow_(_col("e", "x"), 2)) == "pow(X, 2)"

    def test_log(self):
        assert _compile(log(_col("e", "x"))) == "log(X)"

    def test_exp(self):
        assert _compile(exp(_col("e", "x"))) == "exp(X)"

    def test_sin(self):
        assert _compile(sin(_col("e", "x"))) == "sin(X)"

    def test_cos(self):
        assert _compile(cos(_col("e", "x"))) == "cos(X)"

    def test_tan(self):
        assert _compile(tan(_col("e", "x"))) == "tan(X)"

    def test_floor(self):
        assert _compile(floor(_col("e", "x"))) == "floor(X)"

    def test_ceil(self):
        assert _compile(ceil(_col("e", "x"))) == "ceil(X)"

    def test_sign(self):
        assert _compile(sign(_col("e", "x"))) == "sign(X)"

    def test_min_val(self):
        assert _compile(min_val(_col("e", "a"), _col("e", "b"))) == "min_val(A, B)"

    def test_max_val(self):
        assert _compile(max_val(_col("e", "a"), _col("e", "b"))) == "max_val(A, B)"


# ── String ────────────────────────────────────────────────────────────

class TestString:
    def test_len(self):
        assert _compile(len_(_col("e", "name"))) == "len(Name)"

    def test_upper(self):
        assert _compile(upper(_col("e", "name"))) == "upper(Name)"

    def test_lower(self):
        assert _compile(lower(_col("e", "name"))) == "lower(Name)"

    def test_trim(self):
        assert _compile(trim(_col("e", "name"))) == "trim(Name)"

    def test_substr(self):
        assert _compile(substr(_col("e", "name"), 0, 5)) == "substr(Name, 0, 5)"

    def test_replace(self):
        assert _compile(replace(_col("e", "name"), "old", "new")) == 'replace(Name, "old", "new")'

    def test_concat(self):
        assert _compile(concat(_col("e", "first"), " ", _col("e", "last"))) == 'concat(First, " ", Last)'


# ── Type Conversion ───────────────────────────────────────────────────

class TestTypeConversion:
    def test_to_float(self):
        assert _compile(to_float(_col("e", "x"))) == "to_float(X)"

    def test_to_int(self):
        assert _compile(to_int(_col("e", "x"))) == "to_int(X)"


# ── HNSW ──────────────────────────────────────────────────────────────

class TestHNSW:
    def test_hnsw_nearest(self):
        result = _compile(hnsw_nearest("doc_idx", [0.1, 0.2, 0.3], 10))
        assert result == 'hnsw_nearest("doc_idx", [0.1, 0.2, 0.3], 10)'

    def test_hnsw_nearest_with_ef(self):
        result = _compile(hnsw_nearest("doc_idx", [0.1, 0.2], 5, ef_search=100))
        assert result == 'hnsw_nearest("doc_idx", [0.1, 0.2], 5, 100)'
