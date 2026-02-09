# InputLayer Builtin Functions Reference

**Version**: 2.1
**Date**: 2026-02-05
**Status**: Complete - All 55 functions implemented and tested

---

## Overview

InputLayer provides 55 builtin functions for vector operations, temporal processing, quantization, string manipulation, and math utilities. All functions are implemented and tested (unit + snapshot tests).

**Test Coverage Summary**:
- 55/55 functions have full test coverage
- 127+ unit tests in `vector_ops.rs`
- 60+ unit tests in `temporal_ops.rs`
- Snapshot tests in `examples/datalog/`

---

## Table of Contents

1. [Distance Functions](#1-distance-functions)
2. [Vector Operations](#2-vector-operations)
3. [LSH Functions](#3-lsh-locality-sensitive-hashing-functions)
4. [Quantization Functions](#4-quantization-functions)
5. [Int8 Distance Functions](#5-int8-distance-functions)
6. [Temporal Functions](#6-temporal-functions)
7. [Math Functions](#7-math-functions)
8. [String Functions](#8-string-functions)
9. [Scalar Min/Max Functions](#9-scalar-minmax-functions)

---

## 1. Distance Functions

Distance functions for f32 vectors. All return `Float64`.

### euclidean(v1, v2)

Euclidean (L2) distance between two vectors.

```datalog
% Syntax
D = euclidean(V1, V2)

% Example
similar(Id1, Id2, Dist) :-
    vectors(Id1, V1),
    vectors(Id2, V2),
    Id1 < Id2,
    Dist = euclidean(V1, V2),
    Dist < 1.0.
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v1 | Vector (f32) | First vector |
| v2 | Vector (f32) | Second vector |
| **Returns** | Float64 | Euclidean distance (>= 0) |

**Aliases**: `euclidean_distance`
**Implementation**: `src/vector_ops.rs`
**Tests**: `test_euclidean_distance`, snapshot `16_vectors/01_euclidean_distance.dl`

---

### cosine(v1, v2)

Cosine distance (1 - cosine similarity) between two vectors.

```datalog
% Syntax
D = cosine(V1, V2)

% Example - Find similar documents
similar_docs(Id1, Id2) :-
    doc_embedding(Id1, V1),
    doc_embedding(Id2, V2),
    D = cosine(V1, V2),
    D < 0.1.  % Very similar (close to 0)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v1 | Vector (f32) | First vector |
| v2 | Vector (f32) | Second vector |
| **Returns** | Float64 | Cosine distance (range [0, 2]) |

**Note**: Returns 0 for identical directions, 1 for orthogonal, 2 for opposite directions.

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_cosine_distance`, snapshot `16_vectors/02_cosine_distance.dl`

---

### dot(v1, v2)

Dot product of two vectors.

```datalog
% Syntax
Score = dot(V1, V2)

% Example - Compute relevance scores
relevance(QueryId, DocId, Score) :-
    query_vector(QueryId, Q),
    doc_vector(DocId, D),
    Score = dot(Q, D).
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v1 | Vector (f32) | First vector |
| v2 | Vector (f32) | Second vector |
| **Returns** | Float64 | Dot product |

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_dot_product`, snapshot `16_vectors/03_dot_product.dl`

---

### manhattan(v1, v2)

Manhattan (L1) distance between two vectors. Good for sparse vectors.

```datalog
% Syntax
D = manhattan(V1, V2)

% Example
nearby(Id1, Id2) :-
    location(Id1, V1),
    location(Id2, V2),
    D = manhattan(V1, V2),
    D < 10.0.
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v1 | Vector (f32) | First vector |
| v2 | Vector (f32) | Second vector |
| **Returns** | Float64 | Manhattan distance (>= 0) |

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_manhattan_distance`, snapshot `16_vectors/04_manhattan_distance.dl`

---

## 2. Vector Operations

Operations for manipulating vectors.

### normalize(v)

Normalize vector to unit length.

```datalog
% Syntax
Normalized = normalize(V)

% Example - Normalize before cosine similarity
normalized_embedding(Id, NormV) :-
    raw_embedding(Id, V),
    NormV = normalize(V).
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v | Vector (f32) | Input vector |
| **Returns** | Vector (f32) | Unit vector (length = 1) |

**Note**: Returns zero vector if input is zero vector.

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_normalize`, `test_normalize_zero`, snapshot `16_vectors/07_normalize.dl`

---

### vec_dim(v)

Get the dimension (length) of a float32 vector.

```datalog
% Syntax
Dim = vec_dim(V)

% Example - Filter by dimension
valid_embedding(Id, V) :-
    embedding(Id, V),
    Dim = vec_dim(V),
    Dim = 128.
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v | Vector (f32) | Input vector |
| **Returns** | Int64 | Number of dimensions |

**Implementation**: `src/vector_ops.rs`
**Tests**: snapshot `16_vectors/05_vec_operations.dl`

---

### vec_add(v1, v2)

Element-wise addition of two vectors.

```datalog
% Syntax
Sum = vec_add(V1, V2)

% Example - Combine embeddings
combined(Id, SumV) :-
    text_embedding(Id, T),
    image_embedding(Id, I),
    SumV = vec_add(T, I).
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v1 | Vector (f32) | First vector |
| v2 | Vector (f32) | Second vector |
| **Returns** | Vector (f32) | Element-wise sum |

**Note**: Vectors must have same dimension.

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_vector_add`, snapshot `16_vectors/08_vec_add.dl`

---

### vec_scale(v, scalar)

Scale vector by a scalar value.

```datalog
% Syntax
Scaled = vec_scale(V, S)

% Example - Apply weight
weighted(Id, ScaledV) :-
    embedding(Id, V),
    weight(Id, W),
    ScaledV = vec_scale(V, W).
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v | Vector (f32) | Input vector |
| scalar | Float64 | Scale factor |
| **Returns** | Vector (f32) | Scaled vector |

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_vector_scale`, snapshot `16_vectors/09_vec_scale.dl`

---

## 3. LSH (Locality Sensitive Hashing) Functions

Functions for approximate nearest neighbor search.

### lsh_bucket(v, table_idx, num_hyperplanes)

Compute LSH bucket ID for a float32 vector.

```datalog
% Syntax
Bucket = lsh_bucket(V, TableIdx, NumHyperplanes)

% Example - Build LSH index
lsh_index(Id, Table, Bucket) :-
    embedding(Id, V),
    Table = 0,
    Bucket = lsh_bucket(V, Table, 8).  % 8 hyperplanes = 256 buckets
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v | Vector (f32) | Input vector |
| table_idx | Int64 | Hash table index (for multiple tables) |
| num_hyperplanes | Int64 | Number of hyperplanes (controls granularity) |
| **Returns** | Int64 | Bucket ID |

**Note**: More hyperplanes = more buckets = higher precision but lower recall.

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_lsh_bucket_*` (6 tests), snapshot `31_lsh/01_lsh_bucket.dl`

---

### lsh_probes(bucket, num_hp, num_probes)

Generate multi-probe sequence for a bucket by Hamming distance.

```datalog
% Syntax
Probes = lsh_probes(Bucket, NumHyperplanes, NumProbes)

% Example - Get probe sequence
probe_buckets(OrigBucket, Probes) :-
    query_bucket(OrigBucket),
    Probes = lsh_probes(OrigBucket, 8, 4).
```

| Parameter | Type | Description |
|-----------|------|-------------|
| bucket | Int64 | Original bucket ID |
| num_hp | Int64 | Number of hyperplanes used |
| num_probes | Int64 | Number of probe buckets to generate |
| **Returns** | Vector | Probe bucket IDs |

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_lsh_probes_*` (8 tests), snapshot `31_lsh/02_lsh_probes.dl`

---

### lsh_multi_probe(v, table_idx, num_hp, num_probes)

Compute LSH bucket and probes in one call for float32 vectors.

```datalog
% Syntax
Buckets = lsh_multi_probe(V, TableIdx, NumHyperplanes, NumProbes)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v | Vector (f32) | Query vector |
| table_idx | Int64 | Hash table index |
| num_hp | Int64 | Number of hyperplanes |
| num_probes | Int64 | Number of probes |
| **Returns** | Vector | Bucket IDs to probe |

**Implementation**: `src/code_generator/mod.rs`
**Tests**: `test_lsh_multi_probe_*` (4 tests), snapshot `31_lsh/03_lsh_multi_probe.dl`

---

## 4. Quantization Functions

Functions for int8 vector quantization (memory-efficient storage).

### quantize_linear(v)

Linear quantization: maps [min, max] to [-128, 127].

```datalog
% Syntax
QV = quantize_linear(V)

% Example - Compress embeddings
compressed(Id, QV) :-
    embedding(Id, V),
    QV = quantize_linear(V).
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v | Vector (f32) | Input vector |
| **Returns** | VectorInt8 | Quantized vector |

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_quantize_linear_basic`, snapshot `30_quantization/01_quantize_linear.dl`

---

### quantize_symmetric(v)

Symmetric quantization: maps [-max_abs, max_abs] to [-127, 127], preserving zero.

```datalog
QV = quantize_symmetric(V)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v | Vector (f32) | Input vector |
| **Returns** | VectorInt8 | Quantized vector |

**Note**: Better preserves zero values; recommended for normalized vectors.

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_quantize_symmetric_basic`, snapshot `30_quantization/02_quantize_symmetric.dl`

---

### dequantize(v)

Convert int8 vector back to f32.

```datalog
FV = dequantize(QV)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v | VectorInt8 | Quantized vector |
| **Returns** | Vector (f32) | Dequantized vector |

**Note**: Lossy conversion - original precision not fully recovered.

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_dequantize_basic`, snapshot `30_quantization/03_dequantize.dl`

---

### dequantize_scaled(v, scale)

Dequantize with explicit scale factor.

```datalog
FV = dequantize_scaled(QV, Scale)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v | VectorInt8 | Quantized vector |
| scale | Float64 | Scale factor |
| **Returns** | Vector (f32) | Dequantized vector |

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_dequantize_with_scale`, snapshot `30_quantization/04_dequantize_scaled.dl`

---

## 5. Int8 Distance Functions

### Native (Fast) Int8 Distance

Direct computation on int8 values for maximum speed.

### euclidean_int8(v1, v2)

Euclidean distance for int8 vectors.

```datalog
D = euclidean_int8(QV1, QV2)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v1 | VectorInt8 | First quantized vector |
| v2 | VectorInt8 | Second quantized vector |
| **Returns** | Float64 | Euclidean distance |

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_euclidean_distance_int8_*` (3 tests), snapshot `30_quantization/05_euclidean_int8.dl`

---

### cosine_int8(v1, v2)

Cosine distance for int8 vectors.

```datalog
D = cosine_int8(QV1, QV2)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v1 | VectorInt8 | First quantized vector |
| v2 | VectorInt8 | Second quantized vector |
| **Returns** | Float64 | Cosine distance [0, 2] |

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_cosine_distance_int8_*` (3 tests), snapshot `30_quantization/06_cosine_int8.dl`

---

### dot_int8(v1, v2)

Dot product for int8 vectors.

```datalog
Score = dot_int8(QV1, QV2)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v1 | VectorInt8 | First quantized vector |
| v2 | VectorInt8 | Second quantized vector |
| **Returns** | Float64 | Dot product |

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_dot_product_int8`, snapshot `30_quantization/07_dot_int8.dl`

---

### manhattan_int8(v1, v2)

Manhattan distance for int8 vectors.

```datalog
D = manhattan_int8(QV1, QV2)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| v1 | VectorInt8 | First quantized vector |
| v2 | VectorInt8 | Second quantized vector |
| **Returns** | Float64 | Manhattan distance |

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_manhattan_distance_int8`, snapshot `30_quantization/08_manhattan_int8.dl`

---

## 6. Temporal Functions

Functions for time-based queries and temporal reasoning.

### time_now()

Get current Unix timestamp in milliseconds.

```datalog
Now = time_now()
```

| Parameter | Type | Description |
|-----------|------|-------------|
| (none) | - | No parameters |
| **Returns** | Int64 | Unix timestamp (milliseconds) |

**Implementation**: `src/temporal_ops.rs`
**Tests**: `test_time_now_returns_reasonable_value`, snapshot `29_temporal/01_time_now.dl`

---

### time_diff(t1, t2)

Compute difference between two timestamps.

```datalog
Diff = time_diff(T1, T2)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| t1 | Int64 | First timestamp |
| t2 | Int64 | Second timestamp |
| **Returns** | Int64 | Difference (t1 - t2) in milliseconds |

**Implementation**: `src/temporal_ops.rs`
**Tests**: `test_time_diff_*` (4 tests), snapshot `29_temporal/02_time_diff.dl`

---

### time_add(ts, duration_ms)

Add duration to timestamp.

```datalog
NewTime = time_add(Ts, DurationMs)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| ts | Int64 | Base timestamp |
| duration_ms | Int64 | Duration to add (milliseconds) |
| **Returns** | Int64 | New timestamp |

**Implementation**: `src/temporal_ops.rs`
**Tests**: `test_time_add_*` (3 tests), snapshot `29_temporal/03_time_add_sub.dl`

---

### time_sub(ts, duration_ms)

Subtract duration from timestamp.

```datalog
NewTime = time_sub(Ts, DurationMs)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| ts | Int64 | Base timestamp |
| duration_ms | Int64 | Duration to subtract (milliseconds) |
| **Returns** | Int64 | New timestamp |

**Implementation**: `src/temporal_ops.rs`
**Tests**: `test_time_sub_*` (2 tests), snapshot `29_temporal/03_time_add_sub.dl`

---

### time_decay(ts, now, half_life_ms)

Exponential time decay. Formula: `0.5^(age/half_life)`.

```datalog
Weight = time_decay(Ts, Now, HalfLifeMs)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| ts | Int64 | Event timestamp |
| now | Int64 | Current timestamp |
| half_life_ms | Int64 | Half-life in milliseconds |
| **Returns** | Float64 | Decay factor [0, 1] |

**Note**: Returns 1.0 at ts=now, 0.5 at age=half_life, approaches 0 for old events.

**Implementation**: `src/temporal_ops.rs`
**Tests**: `test_time_decay_*` (7 tests), snapshot `29_temporal/04_time_decay.dl`

---

### time_decay_linear(ts, now, max_age_ms)

Linear time decay. Formula: `max(0, 1 - age/max_age)`.

```datalog
Weight = time_decay_linear(Ts, Now, MaxAgeMs)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| ts | Int64 | Event timestamp |
| now | Int64 | Current timestamp |
| max_age_ms | Int64 | Maximum age for decay |
| **Returns** | Float64 | Decay factor [0, 1] |

**Note**: Returns 1.0 at ts=now, 0.5 at age=max_age/2, 0.0 at age>=max_age.

**Implementation**: `src/temporal_ops.rs`
**Tests**: `test_time_decay_linear_*` (6 tests), snapshot `29_temporal/05_time_decay_linear.dl`

---

### time_before(t1, t2)

Check if t1 is before t2.

| Parameter | Type | Description |
|-----------|------|-------------|
| t1 | Int64 | First timestamp |
| t2 | Int64 | Second timestamp |
| **Returns** | Bool | true if t1 < t2 |

**Implementation**: `src/temporal_ops.rs`
**Tests**: `test_time_before`, snapshot `29_temporal/06_time_comparisons.dl`

---

### time_after(t1, t2)

Check if t1 is after t2.

| Parameter | Type | Description |
|-----------|------|-------------|
| t1 | Int64 | First timestamp |
| t2 | Int64 | Second timestamp |
| **Returns** | Bool | true if t1 > t2 |

**Implementation**: `src/temporal_ops.rs`
**Tests**: `test_time_after`, snapshot `29_temporal/06_time_comparisons.dl`

---

### time_between(ts, start, end)

Check if timestamp is within range [start, end] (inclusive).

| Parameter | Type | Description |
|-----------|------|-------------|
| ts | Int64 | Timestamp to check |
| start | Int64 | Window start |
| end | Int64 | Window end |
| **Returns** | Bool | true if start <= ts <= end |

**Implementation**: `src/temporal_ops.rs`
**Tests**: `test_time_between*` (2 tests), snapshot `29_temporal/06_time_comparisons.dl`

---

### within_last(ts, now, duration_ms)

Check if timestamp is within the last duration from now.

| Parameter | Type | Description |
|-----------|------|-------------|
| ts | Int64 | Timestamp to check |
| now | Int64 | Current timestamp |
| duration_ms | Int64 | Window size in milliseconds |
| **Returns** | Bool | true if (now - duration_ms) <= ts <= now |

**Implementation**: `src/temporal_ops.rs`
**Tests**: `test_within_last_*` (5 tests), snapshot `29_temporal/07_within_last.dl`

---

### intervals_overlap(s1, e1, s2, e2)

Check if two time intervals overlap.

| Parameter | Type | Description |
|-----------|------|-------------|
| s1 | Int64 | First interval start |
| e1 | Int64 | First interval end |
| s2 | Int64 | Second interval start |
| e2 | Int64 | Second interval end |
| **Returns** | Bool | true if intervals overlap |

**Implementation**: `src/temporal_ops.rs`
**Tests**: `test_intervals_overlap_*` (5 tests), snapshot `29_temporal/08_intervals_overlap.dl`

---

### interval_contains(s1, e1, s2, e2)

Check if interval [s1, e1] fully contains interval [s2, e2].

| Parameter | Type | Description |
|-----------|------|-------------|
| s1 | Int64 | Outer interval start |
| e1 | Int64 | Outer interval end |
| s2 | Int64 | Inner interval start |
| e2 | Int64 | Inner interval end |
| **Returns** | Bool | true if [s1,e1] contains [s2,e2] |

**Implementation**: `src/temporal_ops.rs`
**Tests**: `test_interval_contains_*` (2 tests), snapshot `29_temporal/09_interval_contains.dl`

---

### interval_duration(start, end)

Get the duration of an interval.

| Parameter | Type | Description |
|-----------|------|-------------|
| start | Int64 | Interval start |
| end | Int64 | Interval end |
| **Returns** | Int64 | Duration (end - start) in milliseconds |

**Implementation**: `src/temporal_ops.rs`
**Tests**: `test_interval_duration_*` (3 tests), snapshot `29_temporal/10_interval_duration.dl`

---

### point_in_interval(ts, start, end)

Check if a point is within an interval [start, end] (inclusive).

| Parameter | Type | Description |
|-----------|------|-------------|
| ts | Int64 | Point timestamp |
| start | Int64 | Interval start |
| end | Int64 | Interval end |
| **Returns** | Bool | true if start <= ts <= end |

**Implementation**: `src/temporal_ops.rs`
**Tests**: `test_point_in_interval_*` (4 tests), snapshot `29_temporal/12_point_in_interval.dl`

---

## 7. Math Functions

General-purpose math functions. All accept Int64 or Float64 inputs (coerced to f64 internally unless noted).

### abs(x)

Generic absolute value. Preserves input type.

```datalog
A = abs(X)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| x | Int64 or Float64 | Input value |
| **Returns** | Same as input | Absolute value |

---

### abs_int64(x)

Absolute value of an integer.

```datalog
AbsVal = abs_int64(X)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| x | Int64 | Input integer |
| **Returns** | Int64 | Absolute value |

**Aliases**: `abs_i64`
**Note**: Uses saturating arithmetic for `i64::MIN`.

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_abs_i64_*` (4 tests), snapshot `32_math/01_abs_int64.dl`

---

### abs_float64(x)

Absolute value of a float.

```datalog
AbsVal = abs_float64(X)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| x | Float64 | Input float |
| **Returns** | Float64 | Absolute value |

**Aliases**: `abs_f64`

**Implementation**: `src/vector_ops.rs`
**Tests**: `test_abs_f64_*` (4 tests), snapshot `32_math/02_abs_float64.dl`

---

### sqrt(x)

Square root.

```datalog
R = sqrt(X)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| x | Float64 or Int64 | Input value (>= 0) |
| **Returns** | Float64 | Square root |

**Note**: Returns Null if x < 0.

**Implementation**: `src/code_generator/mod.rs`

---

### pow(base, exp)

Power function.

```datalog
R = pow(Base, Exp)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| base | Float64 or Int64 | Base value |
| exp | Float64 or Int64 | Exponent |
| **Returns** | Float64 | base^exp |

**Aliases**: `power`

**Implementation**: `src/code_generator/mod.rs`

---

### log(x)

Natural logarithm (base e).

```datalog
R = log(X)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| x | Float64 or Int64 | Input value (> 0) |
| **Returns** | Float64 | Natural logarithm |

**Aliases**: `ln`
**Note**: Returns Null if x <= 0.

**Implementation**: `src/code_generator/mod.rs`

---

### exp(x)

Exponential function (e^x).

```datalog
R = exp(X)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| x | Float64 or Int64 | Input value |
| **Returns** | Float64 | e^x |

**Implementation**: `src/code_generator/mod.rs`

---

### sin(x)

Sine function (radians).

```datalog
R = sin(X)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| x | Float64 or Int64 | Angle in radians |
| **Returns** | Float64 | Sine of x |

**Implementation**: `src/code_generator/mod.rs`

---

### cos(x)

Cosine function (radians).

```datalog
R = cos(X)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| x | Float64 or Int64 | Angle in radians |
| **Returns** | Float64 | Cosine of x |

**Implementation**: `src/code_generator/mod.rs`

---

### tan(x)

Tangent function (radians).

```datalog
R = tan(X)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| x | Float64 or Int64 | Angle in radians |
| **Returns** | Float64 | Tangent of x |

**Implementation**: `src/code_generator/mod.rs`

---

### floor(x)

Round down to nearest integer.

```datalog
R = floor(X)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| x | Float64 or Int64 | Input value |
| **Returns** | Int64 | Largest integer <= x |

**Implementation**: `src/code_generator/mod.rs`

---

### ceil(x)

Round up to nearest integer.

```datalog
R = ceil(X)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| x | Float64 or Int64 | Input value |
| **Returns** | Int64 | Smallest integer >= x |

**Aliases**: `ceiling`

**Implementation**: `src/code_generator/mod.rs`

---

### sign(x)

Sign function. Returns -1, 0, or 1.

```datalog
S = sign(X)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| x | Float64 or Int64 | Input value |
| **Returns** | Int64 | -1 (negative), 0 (zero), or 1 (positive) |

**Aliases**: `signum`
**Note**: For float NaN, returns 0.

**Implementation**: `src/code_generator/mod.rs`

---

## 8. String Functions

Functions for string manipulation.

### len(s)

Get string length (byte count).

```datalog
L = len(S)

% Example
long_names(Name, L) :-
    names(Name),
    L = len(Name),
    L > 10.
```

| Parameter | Type | Description |
|-----------|------|-------------|
| s | String | Input string |
| **Returns** | Int64 | Byte length of string |

**Aliases**: `length`, `strlen`

**Implementation**: `src/code_generator/mod.rs`

---

### upper(s)

Convert string to uppercase (Unicode-aware).

```datalog
U = upper(S)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| s | String | Input string |
| **Returns** | String | Uppercase string |

**Aliases**: `uppercase`, `toupper`

**Implementation**: `src/code_generator/mod.rs`

---

### lower(s)

Convert string to lowercase (Unicode-aware).

```datalog
L = lower(S)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| s | String | Input string |
| **Returns** | String | Lowercase string |

**Aliases**: `lowercase`, `tolower`

**Implementation**: `src/code_generator/mod.rs`

---

### trim(s)

Remove leading and trailing whitespace.

```datalog
T = trim(S)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| s | String | Input string |
| **Returns** | String | Trimmed string |

**Implementation**: `src/code_generator/mod.rs`

---

### substr(s, start, len)

Extract a substring.

```datalog
Sub = substr(S, Start, Len)

% Example - Get first 3 characters
prefix(Name, P) :-
    names(Name),
    P = substr(Name, 0, 3).
```

| Parameter | Type | Description |
|-----------|------|-------------|
| s | String | Input string |
| start | Int64 | Start byte index (0-based) |
| len | Int64 | Maximum length to extract |
| **Returns** | String | Extracted substring |

**Aliases**: `substring`
**Note**: Returns empty string if start > string length. Clamps to string bounds.

**Implementation**: `src/code_generator/mod.rs`

---

### replace(s, find, replacement)

Replace all occurrences of a substring.

```datalog
R = replace(S, Find, Replacement)

% Example
cleaned(R) :-
    raw("hello-world"),
    R = replace("hello-world", "-", " ").
```

| Parameter | Type | Description |
|-----------|------|-------------|
| s | String | Input string |
| find | String | Substring to find |
| replacement | String | Replacement string |
| **Returns** | String | String with all occurrences replaced |

**Implementation**: `src/code_generator/mod.rs`

---

### concat(s1, s2, ...)

Concatenate multiple values into a string. Variable arity (2+ arguments).

```datalog
R = concat(S1, S2)

% Example - Build full name
full_name(First, Last, Full) :-
    person(First, Last),
    Full = concat(First, " ", Last).
```

| Parameter | Type | Description |
|-----------|------|-------------|
| s1, s2, ... | String/Int64/Float64 | Values to concatenate |
| **Returns** | String | Concatenated string |

**Aliases**: `string_concat`
**Note**: Non-string values are automatically converted to their string representation.

**Implementation**: `src/code_generator/mod.rs`

---

## 9. Scalar Min/Max Functions

Scalar comparison functions returning the minimum or maximum of two values.

### min_val(a, b)

Return the smaller of two values.

```datalog
M = min_val(A, B)

% Example - Clamp to range
clamped(X, C) :-
    values(X),
    C = max_val(0, min_val(X, 100)).
```

| Parameter | Type | Description |
|-----------|------|-------------|
| a | Int64/Float64/String | First value |
| b | Int64/Float64/String | Second value |
| **Returns** | Same as input | Smaller of the two values |

**Aliases**: `min_int64`, `min_float64`, `min`
**Note**: Mixed numeric types (Int64 + Float64) return Float64. Strings compare lexicographically.

**Implementation**: `src/code_generator/mod.rs`

---

### max_val(a, b)

Return the larger of two values.

```datalog
M = max_val(A, B)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| a | Int64/Float64/String | First value |
| b | Int64/Float64/String | Second value |
| **Returns** | Same as input | Larger of the two values |

**Aliases**: `max_int64`, `max_float64`, `max`
**Note**: Mixed numeric types (Int64 + Float64) return Float64. Strings compare lexicographically.

**Implementation**: `src/code_generator/mod.rs`

---

## Appendix: Function Quick Reference

| Function | Parameters | Returns | Category |
|----------|-----------|---------|----------|
| `euclidean` | (v1, v2) | Float64 | Distance |
| `cosine` | (v1, v2) | Float64 | Distance |
| `dot` | (v1, v2) | Float64 | Distance |
| `manhattan` | (v1, v2) | Float64 | Distance |
| `normalize` | (v) | Vector | Vector Ops |
| `vec_dim` | (v) | Int64 | Vector Ops |
| `vec_add` | (v1, v2) | Vector | Vector Ops |
| `vec_scale` | (v, s) | Vector | Vector Ops |
| `lsh_bucket` | (v, table, hp) | Int64 | LSH |
| `lsh_probes` | (bucket, hp, n) | Vector | LSH |
| `lsh_multi_probe` | (v, table, hp, n) | Vector | LSH |
| `quantize_linear` | (v) | VectorInt8 | Quantization |
| `quantize_symmetric` | (v) | VectorInt8 | Quantization |
| `dequantize` | (qv) | Vector | Quantization |
| `dequantize_scaled` | (qv, s) | Vector | Quantization |
| `euclidean_int8` | (qv1, qv2) | Float64 | Int8 Distance |
| `cosine_int8` | (qv1, qv2) | Float64 | Int8 Distance |
| `dot_int8` | (qv1, qv2) | Float64 | Int8 Distance |
| `manhattan_int8` | (qv1, qv2) | Float64 | Int8 Distance |
| `time_now` | () | Int64 | Temporal |
| `time_diff` | (t1, t2) | Int64 | Temporal |
| `time_add` | (ts, dur) | Int64 | Temporal |
| `time_sub` | (ts, dur) | Int64 | Temporal |
| `time_decay` | (ts, now, hl) | Float64 | Temporal |
| `time_decay_linear` | (ts, now, max) | Float64 | Temporal |
| `time_before` | (t1, t2) | Bool | Temporal |
| `time_after` | (t1, t2) | Bool | Temporal |
| `time_between` | (ts, s, e) | Bool | Temporal |
| `within_last` | (ts, now, dur) | Bool | Temporal |
| `intervals_overlap` | (s1, e1, s2, e2) | Bool | Temporal |
| `interval_contains` | (s1, e1, s2, e2) | Bool | Temporal |
| `interval_duration` | (s, e) | Int64 | Temporal |
| `point_in_interval` | (ts, s, e) | Bool | Temporal |
| `abs` | (x) | same type | Math |
| `abs_int64` | (x) | Int64 | Math |
| `abs_float64` | (x) | Float64 | Math |
| `sqrt` | (x) | Float64 | Math |
| `pow` | (base, exp) | Float64 | Math |
| `log` | (x) | Float64 | Math |
| `exp` | (x) | Float64 | Math |
| `sin` | (x) | Float64 | Math |
| `cos` | (x) | Float64 | Math |
| `tan` | (x) | Float64 | Math |
| `floor` | (x) | Int64 | Math |
| `ceil` | (x) | Int64 | Math |
| `sign` | (x) | Int64 | Math |
| `len` | (s) | Int64 | String |
| `upper` | (s) | String | String |
| `lower` | (s) | String | String |
| `trim` | (s) | String | String |
| `substr` | (s, start, len) | String | String |
| `replace` | (s, find, repl) | String | String |
| `concat` | (s1, s2, ...) | String | String |
| `min_val` | (a, b) | same type | Min/Max |
| `max_val` | (a, b) | same type | Min/Max |

---

## Implementation Files

| File | Functions |
|------|-----------|
| `src/ast/mod.rs` | AST definitions (`BuiltinFunc` enum), parsing, arity |
| `src/ir/mod.rs` | IR-level function definitions (`BuiltinFunction` enum) |
| `src/ir_builder/mod.rs` | AST to IR function conversion |
| `src/code_generator/mod.rs` | Function execution dispatch (`evaluate_function`) |
| `src/vector_ops.rs` | Distance, vector ops, quantization, LSH, abs |
| `src/temporal_ops.rs` | All temporal functions |

---

## Test Coverage

| Directory | Tests | Functions Covered |
|-----------|-------|-------------------|
| `examples/datalog/16_vectors/` | 17 files | Distance, vector ops |
| `examples/datalog/29_temporal/` | 19 files | All temporal functions |
| `examples/datalog/30_quantization/` | 8 files | Quantization, int8 distance |
| `examples/datalog/31_lsh/` | 5 files | LSH functions |
| `examples/datalog/31_strings/` | 6+ files | String functions |
| `examples/datalog/32_math/` | 9+ files | Math functions |
| `src/vector_ops.rs` | 127+ tests | Unit tests |
| `src/temporal_ops.rs` | 60+ tests | Unit tests |
