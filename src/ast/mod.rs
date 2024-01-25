//! # Datalog AST - Abstract Syntax Tree Types
//!
//! Abstract Syntax Tree types for Datalog programs.
//! Used across multiple modules for parsing and semantic analysis.
//!
//! ## Builders
//!
//! For programmatic construction of AST nodes, see the [`builders`] module
//! which provides fluent APIs like `AtomBuilder` and `RuleBuilder`.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub mod builders;

// Core AST Types
/// Aggregation function types for Datalog
///
/// Note: Does not implement Hash or Eq because `TopKThreshold` and `WithinRadius`
/// contain f64 fields which don't implement these traits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunc {
    Count,
    CountDistinct,
    Sum,
    Min,
    Max,
    Avg,
    /// Top-K aggregate: select top k results ordered by a variable
    /// Syntax: `top_k`<10, score> or `top_k`<10, score, desc>
    TopK {
        k: usize,
        order_var: String,
        descending: bool,
    },
    /// Top-K with threshold: only return results if score meets threshold
    /// Syntax: `top_k_threshold`<10, score, 0.5> or `top_k_threshold`<10, score, 0.5, desc>
    TopKThreshold {
        k: usize,
        order_var: String,
        threshold: f64,
        descending: bool,
    },
    /// Within radius: all results within a distance threshold (range query)
    /// Syntax: `within_radius`<dist, 0.5>
    WithinRadius {
        distance_var: String,
        max_distance: f64,
    },
}

/// Built-in function for vector/scalar operations
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BuiltinFunc {
    // Distance functions
    /// Euclidean distance: euclidean(v1, v2) -> Float64
    Euclidean,
    /// Cosine distance: cosine(v1, v2) -> Float64 (1 - similarity)
    Cosine,
    /// Dot product: dot(v1, v2) -> Float64
    DotProduct,
    /// Manhattan distance: manhattan(v1, v2) -> Float64
    Manhattan,

    // LSH functions
    /// LSH bucket: `lsh_bucket(v`, `table_idx`, `num_hyperplanes`) -> Int64
    /// `num_hyperplanes` controls precision vs recall tradeoff
    LshBucket,

    // Vector operations
    /// Normalize vector: normalize(v) -> Vector
    VecNormalize,
    /// Get vector dimension: `vec_dim(v)` -> Int64
    VecDim,
    /// Add vectors: `vec_add(v1`, v2) -> Vector
    VecAdd,
    /// Scale vector: `vec_scale(v`, scalar) -> Vector
    VecScale,

    // Temporal functions
    /// Get current time: `time_now()` -> Timestamp (Unix milliseconds)
    TimeNow,
    /// Time difference: `time_diff(t1`, t2) -> Int64 (milliseconds)
    TimeDiff,
    /// Add duration to timestamp: `time_add(ts`, `duration_ms`) -> Timestamp
    TimeAdd,
    /// Subtract duration from timestamp: `time_sub(ts`, `duration_ms`) -> Timestamp
    TimeSub,
    /// Exponential time decay: `time_decay(ts`, now, `half_life_ms`) -> Float64 \[0,1\]
    TimeDecay,
    /// Linear time decay: `time_decay_linear(ts`, now, `max_age_ms`) -> Float64 \[0,1\]
    TimeDecayLinear,
    /// Check if t1 < t2: `time_before(t1`, t2) -> Bool
    TimeBefore,
    /// Check if t1 > t2: `time_after(t1`, t2) -> Bool
    TimeAfter,
    /// Check if ts in [start, end]: `time_between(ts`, start, end) -> Bool
    TimeBetween,
    /// Check if ts is within duration of now: `within_last(ts`, now, `duration_ms`) -> Bool
    WithinLast,
    /// Check if intervals overlap: `intervals_overlap(s1`, e1, s2, e2) -> Bool
    IntervalsOverlap,
    /// Check if interval 1 contains interval 2: `interval_contains(s1`, e1, s2, e2) -> Bool
    IntervalContains,
    /// Get interval duration: `interval_duration(start`, end) -> Int64
    IntervalDuration,
    /// Check if point is in interval: `point_in_interval(ts`, start, end) -> Bool
    PointInInterval,

    // Int8 quantization functions
    /// Linear quantization: `quantize_linear(v)` -> `VectorInt8`
    QuantizeLinear,
    /// Symmetric quantization: `quantize_symmetric(v)` -> `VectorInt8`
    QuantizeSymmetric,
    /// Dequantize int8 to f32: dequantize(v) -> Vector
    Dequantize,
    /// Dequantize with scale: `dequantize_scaled(v`, scale) -> Vector
    DequantizeScaled,

    // Int8 distance functions
    /// Euclidean distance for int8: `euclidean_int8(v1`, v2) -> Float64
    EuclideanInt8,
    /// Cosine distance for int8: `cosine_int8(v1`, v2) -> Float64
    CosineInt8,
    /// Dot product for int8: `dot_int8(v1`, v2) -> Float64
    DotProductInt8,
    /// Manhattan distance for int8: `manhattan_int8(v1`, v2) -> Float64
    ManhattanInt8,

    // Multi-probe LSH functions
    /// Generate probe sequence: `lsh_probes(bucket`, `num_hp`, `num_probes`) -> \[`Int64`\]
    LshProbes,
    /// Multi-probe in one call: `lsh_multi_probe(v`, `table_idx`, `num_hp`, `num_probes`) -> \[`Int64`\]
    LshMultiProbe,

    // Math utility functions
    /// Absolute value of integer: `abs_int64(x)` -> Int64
    AbsInt64,
    /// Absolute value of float: `abs_float64(x)` -> Float64
    AbsFloat64,
    /// Generic absolute value: `abs(x)` -> same type
    Abs,
    /// Square root: `sqrt(x)` -> Float64
    Sqrt,
    /// Power: `pow(base, exp)` -> Float64
    Pow,
    /// Natural logarithm: `log(x)` -> Float64
    Log,
    /// Exponential: `exp(x)` -> Float64
    Exp,
    /// Sine: `sin(x)` -> Float64
    Sin,
    /// Cosine: `cos(x)` -> Float64
    Cos,
    /// Tangent: `tan(x)` -> Float64
    Tan,
    /// Floor: `floor(x)` -> Int64
    Floor,
    /// Ceiling: `ceil(x)` -> Int64
    Ceil,
    /// Sign: `sign(x)` -> Int64 (-1, 0, or 1)
    Sign,

    // String functions
    /// String length: `len(s)` -> Int64
    Len,
    /// Convert to uppercase: `upper(s)` -> String
    Upper,
    /// Convert to lowercase: `lower(s)` -> String
    Lower,
    /// Trim whitespace: `trim(s)` -> String
    Trim,
    /// Substring: `substr(s, start, len)` -> String
    Substr,
    /// Replace: `replace(s, find, replacement)` -> String
    Replace,
    /// Concatenate strings: `concat(s1, s2, ...)` -> String
    Concat,

    // Scalar min/max functions
    /// Scalar minimum: `min_val(a, b)` -> same type
    MinVal,
    /// Scalar maximum: `max_val(a, b)` -> same type
    MaxVal,
}

