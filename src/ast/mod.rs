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
    /// Top-K aggregate: select top k tuples ordered by order_var
    /// Syntax: `top_k<2, Name, Score:desc>` — vars inside aggregate, `:desc`/`:asc` marks order var
    TopK {
        k: usize,
        order_var: String,
        /// All output variables (including order_var) in declaration order
        output_vars: Vec<String>,
        descending: bool,
    },
    /// Top-K with threshold: only return results if score meets threshold
    /// Syntax: `top_k_threshold<5, 0.5, Name, Score:desc>`
    TopKThreshold {
        k: usize,
        order_var: String,
        output_vars: Vec<String>,
        threshold: f64,
        descending: bool,
    },
    /// Within radius: all results within a distance threshold (range query)
    /// Syntax: `within_radius<10.0, Name, Distance:asc>`
    WithinRadius {
        distance_var: String,
        output_vars: Vec<String>,
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

impl BuiltinFunc {
    /// Parse a built-in function name
    pub fn parse(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "euclidean" => Some(BuiltinFunc::Euclidean),
            "cosine" => Some(BuiltinFunc::Cosine),
            "dot" => Some(BuiltinFunc::DotProduct),
            "manhattan" => Some(BuiltinFunc::Manhattan),
            "lsh_bucket" => Some(BuiltinFunc::LshBucket),
            "normalize" => Some(BuiltinFunc::VecNormalize),
            "vec_dim" => Some(BuiltinFunc::VecDim),
            "vec_add" => Some(BuiltinFunc::VecAdd),
            "vec_scale" => Some(BuiltinFunc::VecScale),
            // Temporal functions
            "time_now" => Some(BuiltinFunc::TimeNow),
            "time_diff" => Some(BuiltinFunc::TimeDiff),
            "time_add" => Some(BuiltinFunc::TimeAdd),
            "time_sub" => Some(BuiltinFunc::TimeSub),
            "time_decay" => Some(BuiltinFunc::TimeDecay),
            "time_decay_linear" => Some(BuiltinFunc::TimeDecayLinear),
            "time_before" => Some(BuiltinFunc::TimeBefore),
            "time_after" => Some(BuiltinFunc::TimeAfter),
            "time_between" => Some(BuiltinFunc::TimeBetween),
            "within_last" => Some(BuiltinFunc::WithinLast),
            "intervals_overlap" => Some(BuiltinFunc::IntervalsOverlap),
            "interval_contains" => Some(BuiltinFunc::IntervalContains),
            "interval_duration" => Some(BuiltinFunc::IntervalDuration),
            "point_in_interval" => Some(BuiltinFunc::PointInInterval),
            // Quantization functions
            "quantize_linear" => Some(BuiltinFunc::QuantizeLinear),
            "quantize_symmetric" => Some(BuiltinFunc::QuantizeSymmetric),
            "dequantize" => Some(BuiltinFunc::Dequantize),
            "dequantize_scaled" => Some(BuiltinFunc::DequantizeScaled),
            // Int8 distance functions
            "euclidean_int8" => Some(BuiltinFunc::EuclideanInt8),
            "cosine_int8" => Some(BuiltinFunc::CosineInt8),
            "dot_int8" => Some(BuiltinFunc::DotProductInt8),
            "manhattan_int8" => Some(BuiltinFunc::ManhattanInt8),
            // Multi-probe LSH
            "lsh_probes" => Some(BuiltinFunc::LshProbes),
            "lsh_multi_probe" => Some(BuiltinFunc::LshMultiProbe),
            // Math utilities
            "abs_int64" => Some(BuiltinFunc::AbsInt64),
            "abs_float64" => Some(BuiltinFunc::AbsFloat64),
            "abs" => Some(BuiltinFunc::Abs),
            "sqrt" => Some(BuiltinFunc::Sqrt),
            "pow" => Some(BuiltinFunc::Pow),
            "log" => Some(BuiltinFunc::Log),
            "exp" => Some(BuiltinFunc::Exp),
            "sin" => Some(BuiltinFunc::Sin),
            "cos" => Some(BuiltinFunc::Cos),
            "tan" => Some(BuiltinFunc::Tan),
            "floor" => Some(BuiltinFunc::Floor),
            "ceil" => Some(BuiltinFunc::Ceil),
            "sign" => Some(BuiltinFunc::Sign),
            // String functions
            "len" => Some(BuiltinFunc::Len),
            "upper" => Some(BuiltinFunc::Upper),
            "lower" => Some(BuiltinFunc::Lower),
            "trim" => Some(BuiltinFunc::Trim),
            "substr" => Some(BuiltinFunc::Substr),
            "replace" => Some(BuiltinFunc::Replace),
            "concat" => Some(BuiltinFunc::Concat),
            "min_val" => Some(BuiltinFunc::MinVal),
            "max_val" => Some(BuiltinFunc::MaxVal),
            _ => None,
        }
    }

    /// Get the expected number of arguments
    pub fn arity(&self) -> usize {
        match self {
            BuiltinFunc::Euclidean
            | BuiltinFunc::Cosine
            | BuiltinFunc::DotProduct
            | BuiltinFunc::Manhattan
            | BuiltinFunc::VecAdd
            | BuiltinFunc::VecScale => 2,
            BuiltinFunc::LshBucket => 3, // (vector, table_idx, num_hyperplanes)
            BuiltinFunc::VecNormalize | BuiltinFunc::VecDim => 1,
            // Temporal functions
            BuiltinFunc::TimeNow => 0,
            BuiltinFunc::TimeDiff
            | BuiltinFunc::TimeAdd
            | BuiltinFunc::TimeSub
            | BuiltinFunc::TimeBefore
            | BuiltinFunc::TimeAfter
            | BuiltinFunc::IntervalDuration => 2,
            BuiltinFunc::TimeDecay
            | BuiltinFunc::TimeDecayLinear
            | BuiltinFunc::TimeBetween
            | BuiltinFunc::WithinLast
            | BuiltinFunc::PointInInterval => 3,
            BuiltinFunc::IntervalsOverlap | BuiltinFunc::IntervalContains => 4,
            // Quantization functions
            BuiltinFunc::QuantizeLinear
            | BuiltinFunc::QuantizeSymmetric
            | BuiltinFunc::Dequantize => 1,
            BuiltinFunc::DequantizeScaled => 2,
            // Int8 distance functions
            BuiltinFunc::EuclideanInt8
            | BuiltinFunc::CosineInt8
            | BuiltinFunc::DotProductInt8
            | BuiltinFunc::ManhattanInt8 => 2,
            // Multi-probe LSH
            BuiltinFunc::LshProbes => 3, // (bucket, num_hp, num_probes)
            BuiltinFunc::LshMultiProbe => 4, // (v, table_idx, num_hp, num_probes)
            // Math utilities
            BuiltinFunc::AbsInt64
            | BuiltinFunc::AbsFloat64
            | BuiltinFunc::Abs
            | BuiltinFunc::Sqrt
            | BuiltinFunc::Log
            | BuiltinFunc::Exp
            | BuiltinFunc::Sin
            | BuiltinFunc::Cos
            | BuiltinFunc::Tan
            | BuiltinFunc::Floor
            | BuiltinFunc::Ceil
            | BuiltinFunc::Sign => 1,
            BuiltinFunc::Pow => 2,
            // String functions
            BuiltinFunc::Len | BuiltinFunc::Upper | BuiltinFunc::Lower | BuiltinFunc::Trim => 1,
            BuiltinFunc::Substr | BuiltinFunc::Replace | BuiltinFunc::Concat => 3, // Concat takes 2-3 args, we report 3 but allow variable
            BuiltinFunc::MinVal | BuiltinFunc::MaxVal => 2,
        }
    }

    /// Get the string representation of the function name
    pub fn as_str(&self) -> &'static str {
        match self {
            BuiltinFunc::Euclidean => "euclidean",
            BuiltinFunc::Cosine => "cosine",
            BuiltinFunc::DotProduct => "dot",
            BuiltinFunc::Manhattan => "manhattan",
            BuiltinFunc::LshBucket => "lsh_bucket",
            BuiltinFunc::VecNormalize => "normalize",
            BuiltinFunc::VecDim => "vec_dim",
            BuiltinFunc::VecAdd => "vec_add",
            BuiltinFunc::VecScale => "vec_scale",
            // Temporal functions
            BuiltinFunc::TimeNow => "time_now",
            BuiltinFunc::TimeDiff => "time_diff",
            BuiltinFunc::TimeAdd => "time_add",
            BuiltinFunc::TimeSub => "time_sub",
            BuiltinFunc::TimeDecay => "time_decay",
            BuiltinFunc::TimeDecayLinear => "time_decay_linear",
            BuiltinFunc::TimeBefore => "time_before",
            BuiltinFunc::TimeAfter => "time_after",
            BuiltinFunc::TimeBetween => "time_between",
            BuiltinFunc::WithinLast => "within_last",
            BuiltinFunc::IntervalsOverlap => "intervals_overlap",
            BuiltinFunc::IntervalContains => "interval_contains",
            BuiltinFunc::IntervalDuration => "interval_duration",
            BuiltinFunc::PointInInterval => "point_in_interval",
            // Quantization functions
            BuiltinFunc::QuantizeLinear => "quantize_linear",
            BuiltinFunc::QuantizeSymmetric => "quantize_symmetric",
            BuiltinFunc::Dequantize => "dequantize",
            BuiltinFunc::DequantizeScaled => "dequantize_scaled",
            // Int8 distance functions
            BuiltinFunc::EuclideanInt8 => "euclidean_int8",
            BuiltinFunc::CosineInt8 => "cosine_int8",
            BuiltinFunc::DotProductInt8 => "dot_int8",
            BuiltinFunc::ManhattanInt8 => "manhattan_int8",
            // Multi-probe LSH
            BuiltinFunc::LshProbes => "lsh_probes",
            BuiltinFunc::LshMultiProbe => "lsh_multi_probe",
            // Math utilities
            BuiltinFunc::AbsInt64 => "abs_int64",
            BuiltinFunc::AbsFloat64 => "abs_float64",
            BuiltinFunc::Abs => "abs",
            BuiltinFunc::Sqrt => "sqrt",
            BuiltinFunc::Pow => "pow",
            BuiltinFunc::Log => "log",
            BuiltinFunc::Exp => "exp",
            BuiltinFunc::Sin => "sin",
            BuiltinFunc::Cos => "cos",
            BuiltinFunc::Tan => "tan",
            BuiltinFunc::Floor => "floor",
            BuiltinFunc::Ceil => "ceil",
            BuiltinFunc::Sign => "sign",
            // String functions
            BuiltinFunc::Len => "len",
            BuiltinFunc::Upper => "upper",
            BuiltinFunc::Lower => "lower",
            BuiltinFunc::Trim => "trim",
            BuiltinFunc::Substr => "substr",
            BuiltinFunc::Replace => "replace",
            BuiltinFunc::Concat => "concat",
            BuiltinFunc::MinVal => "min_val",
            BuiltinFunc::MaxVal => "max_val",
        }
    }
}

/// Arithmetic operators for expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArithOp {
    /// Addition (+)
    Add,
    /// Subtraction (-)
    Sub,
    /// Multiplication (*)
    Mul,
    /// Division (/)
    Div,
    /// Modulo (%)
    Mod,
}

impl ArithOp {
    /// Parse an arithmetic operator from a string
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "+" => Some(ArithOp::Add),
            "-" => Some(ArithOp::Sub),
            "*" => Some(ArithOp::Mul),
            "/" => Some(ArithOp::Div),
            "%" => Some(ArithOp::Mod),
            _ => None,
        }
    }

    /// Get the string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            ArithOp::Add => "+",
            ArithOp::Sub => "-",
            ArithOp::Mul => "*",
            ArithOp::Div => "/",
            ArithOp::Mod => "%",
        }
    }
}

/// Arithmetic expression tree
///
/// Represents arithmetic expressions like `d + 1` or `x * y + z`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArithExpr {
    /// A variable reference
    Variable(String),
    /// A constant value
    Constant(i64),
    /// A float constant value (stored as f64 bit pattern to allow Eq/Hash)
    FloatConstant(u64),
    /// Binary operation
    Binary {
        op: ArithOp,
        left: Box<ArithExpr>,
        right: Box<ArithExpr>,
    },
}

impl ArithExpr {
    /// Create a float constant from an f64 value
    pub fn from_float(f: f64) -> Self {
        ArithExpr::FloatConstant(f.to_bits())
    }

    /// Get the f64 value from a FloatConstant
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ArithExpr::FloatConstant(bits) => Some(f64::from_bits(*bits)),
            ArithExpr::Constant(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Get all variables referenced in this expression
    pub fn variables(&self) -> std::collections::HashSet<String> {
        let mut vars = std::collections::HashSet::new();
        self.collect_variables(&mut vars);
        vars
    }

    fn collect_variables(&self, vars: &mut std::collections::HashSet<String>) {
        match self {
            ArithExpr::Variable(name) => {
                vars.insert(name.clone());
            }
            ArithExpr::Constant(_) | ArithExpr::FloatConstant(_) => {}
            ArithExpr::Binary { left, right, .. } => {
                left.collect_variables(vars);
                right.collect_variables(vars);
            }
        }
    }

    /// Check if this is a simple variable or constant
    pub fn is_simple(&self) -> bool {
        matches!(
            self,
            ArithExpr::Variable(_) | ArithExpr::Constant(_) | ArithExpr::FloatConstant(_)
        )
    }

    /// Try to evaluate as a constant if all values are known
    pub fn try_eval_constant(&self) -> Option<i64> {
        match self {
            ArithExpr::Constant(v) => Some(*v),
            ArithExpr::FloatConstant(_) => None, // Can't evaluate floats as integer
            ArithExpr::Variable(_) => None,
            ArithExpr::Binary { op, left, right } => {
                let l = left.try_eval_constant()?;
                let r = right.try_eval_constant()?;
                Some(match op {
                    ArithOp::Add => l + r,
                    ArithOp::Sub => l - r,
                    ArithOp::Mul => l * r,
                    ArithOp::Div => {
                        if r == 0 {
                            return None;
                        }
                        l / r
                    }
                    ArithOp::Mod => {
                        if r == 0 {
                            return None;
                        }
                        l % r
                    }
                })
            }
        }
    }
}

impl AggregateFunc {
    /// Parse an aggregate function name (for simple aggregates like count, sum, etc.)
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "count" => Some(AggregateFunc::Count),
            "count_distinct" | "countdistinct" => Some(AggregateFunc::CountDistinct),
            "sum" => Some(AggregateFunc::Sum),
            "min" => Some(AggregateFunc::Min),
            "max" => Some(AggregateFunc::Max),
            "avg" => Some(AggregateFunc::Avg),
            _ => None,
        }
    }

    /// Parse annotated variable specifications from ranking aggregate params.
    ///
    /// Each var is either plain (`Name`) or annotated (`Score:desc`, `Distance:asc`).
    /// Returns `(output_vars, order_var, descending)` or None on error.
    ///
    /// Rules:
    /// - 1 variable without annotation → it IS the order var (default: `default_desc`)
    /// - Multiple variables → exactly one must have `:desc` or `:asc` annotation
    /// - Multiple annotations → error (None)
    fn parse_annotated_vars(
        parts: &[&str],
        default_desc: bool,
    ) -> Option<(Vec<String>, String, bool)> {
        if parts.is_empty() {
            return None;
        }

        let mut output_vars = Vec::new();
        let mut order_var = None;
        let mut descending = default_desc;

        for part in parts {
            let trimmed = part.trim();
            if let Some(name) = trimmed.strip_suffix(":desc") {
                let name = name.trim().to_string();
                if order_var.is_some() {
                    return None; // Multiple annotations
                }
                order_var = Some(name.clone());
                descending = true;
                output_vars.push(name);
            } else if let Some(name) = trimmed.strip_suffix(":asc") {
                let name = name.trim().to_string();
                if order_var.is_some() {
                    return None; // Multiple annotations
                }
                order_var = Some(name.clone());
                descending = false;
                output_vars.push(name);
            } else {
                output_vars.push(trimmed.to_string());
            }
        }

        // If no annotation and exactly 1 variable → it's the order var
        if order_var.is_none() {
            if output_vars.len() == 1 {
                order_var = Some(output_vars[0].clone());
            } else {
                return None; // Multiple vars without annotation
            }
        }

        Some((output_vars, order_var.unwrap(), descending))
    }

    /// Parse `top_k` with parameters.
    ///
    /// New syntax: `top_k<2, Name, Score:desc>` or `top_k<2, Score>` (single var defaults desc)
    pub fn parse_top_k(params: &str) -> Option<Self> {
        let parts: Vec<&str> = params.split(',').map(str::trim).collect();
        if parts.len() < 2 {
            return None;
        }

        // First param must be k (integer)
        let k: usize = parts[0].parse().ok()?;

        // Remaining are annotated variable specs
        let (output_vars, order_var, descending) = Self::parse_annotated_vars(&parts[1..], true)?; // default desc for top_k

        Some(AggregateFunc::TopK {
            k,
            order_var,
            output_vars,
            descending,
        })
    }

    /// Parse `top_k_threshold` with parameters.
    ///
    /// New syntax: `top_k_threshold<5, 0.5, Name, Score:desc>`
    /// Numeric params: k (int), threshold (float), then annotated vars.
    pub fn parse_top_k_threshold(params: &str) -> Option<Self> {
        let parts: Vec<&str> = params.split(',').map(str::trim).collect();
        if parts.len() < 3 {
            return None;
        }

        // First param: k (integer)
        let k: usize = parts[0].parse().ok()?;
        // Second param: threshold (float)
        let threshold: f64 = parts[1].parse().ok()?;

        // Remaining are annotated variable specs
        let (output_vars, order_var, descending) = Self::parse_annotated_vars(&parts[2..], true)?; // default desc

        Some(AggregateFunc::TopKThreshold {
            k,
            order_var,
            output_vars,
            threshold,
            descending,
        })
    }

    /// Parse `within_radius` with parameters.
    ///
    /// New syntax: `within_radius<10.0, Name, Distance:asc>`
    /// Numeric params: max_distance (float), then annotated vars.
    pub fn parse_within_radius(params: &str) -> Option<Self> {
        let parts: Vec<&str> = params.split(',').map(str::trim).collect();
        if parts.len() < 2 {
            return None;
        }

        // First param: max_distance (float)
        let max_distance: f64 = parts[0].parse().ok()?;

        // Remaining are annotated variable specs
        let (output_vars, distance_var, _descending) =
            Self::parse_annotated_vars(&parts[1..], false)?; // default asc for within_radius

        Some(AggregateFunc::WithinRadius {
            distance_var,
            output_vars,
            max_distance,
        })
    }

    /// Check if this is a ranking aggregate (affects output cardinality)
    pub fn is_ranking(&self) -> bool {
        matches!(
            self,
            AggregateFunc::TopK { .. }
                | AggregateFunc::TopKThreshold { .. }
                | AggregateFunc::WithinRadius { .. }
        )
    }

    /// Check if this is a simple (scalar) aggregate — many rows → 1 value per group
    pub fn is_simple(&self) -> bool {
        matches!(
            self,
            AggregateFunc::Count
                | AggregateFunc::CountDistinct
                | AggregateFunc::Sum
                | AggregateFunc::Min
                | AggregateFunc::Max
                | AggregateFunc::Avg
        )
    }
}

/// Represents a variable or constant in Datalog
#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    Variable(String), // e.g., "x", "y", "z"
    Constant(i64),    // e.g., 42, 100
    Placeholder,      // For parser - represents "_" in Datalog
    /// Aggregation term: `count<x>`, `sum<y>`, `min<z>`, `max<z>`, `avg<z>`
    Aggregate(AggregateFunc, String), // (function, variable_name)
    /// Arithmetic expression term: `d + 1`, `x * y`, etc.
    Arithmetic(ArithExpr),
    /// Function call term: `euclidean(v1, v2)`, `normalize(v)`, etc.
    FunctionCall(BuiltinFunc, Vec<Term>),
    /// Vector literal: `[1.0, 2.0, 3.0]`
    VectorLiteral(Vec<f64>),
    /// Float constant for function arguments
    FloatConstant(f64),
    /// String constant
    StringConstant(String),
    /// Boolean constant (true / false)
    BoolConstant(bool),
    /// Field access on a record variable: `U.id`, `P.amount`
    FieldAccess(Box<Term>, String),
    /// Record pattern for destructuring in atom arguments: `{ id: x, name: y }`
    RecordPattern(Vec<(String, Term)>),
}

impl Term {
    /// Check if this term is a variable
    pub fn is_variable(&self) -> bool {
        matches!(self, Term::Variable(_))
    }

    /// Check if this term is a constant
    pub fn is_constant(&self) -> bool {
        matches!(self, Term::Constant(_))
    }

    /// Check if this term is an aggregate
    pub fn is_aggregate(&self) -> bool {
        matches!(self, Term::Aggregate(_, _))
    }

    /// Check if this term is an arithmetic expression
    pub fn is_arithmetic(&self) -> bool {
        matches!(self, Term::Arithmetic(_))
    }

    /// Check if this term is a function call
    pub fn is_function_call(&self) -> bool {
        matches!(self, Term::FunctionCall(_, _))
    }

    /// Check if this term is a vector literal
    pub fn is_vector_literal(&self) -> bool {
        matches!(self, Term::VectorLiteral(_))
    }

    /// Check if this term is a float constant
    pub fn is_float_constant(&self) -> bool {
        matches!(self, Term::FloatConstant(_))
    }

    /// Get variable name if this is a variable
    pub fn as_variable(&self) -> Option<&str> {
        if let Term::Variable(name) = self {
            Some(name)
        } else {
            None
        }
    }

    /// Get aggregate info if this is an aggregate term
    pub fn as_aggregate(&self) -> Option<(&AggregateFunc, &str)> {
        if let Term::Aggregate(func, var) = self {
            Some((func, var))
        } else {
            None
        }
    }

    /// Get arithmetic expression if this is an arithmetic term
    pub fn as_arithmetic(&self) -> Option<&ArithExpr> {
        if let Term::Arithmetic(expr) = self {
            Some(expr)
        } else {
            None
        }
    }

    /// Get function call info if this is a function call term
    pub fn as_function_call(&self) -> Option<(&BuiltinFunc, &[Term])> {
        if let Term::FunctionCall(func, args) = self {
            Some((func, args))
        } else {
            None
        }
    }

    /// Get vector literal if this is a vector literal term
    pub fn as_vector_literal(&self) -> Option<&[f64]> {
        if let Term::VectorLiteral(values) = self {
            Some(values)
        } else {
            None
        }
    }

    /// Get float constant if this is a float constant term
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Term::FloatConstant(v) => Some(*v),
            Term::Constant(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Get all variables referenced by this term
    pub fn variables(&self) -> std::collections::HashSet<String> {
        match self {
            Term::Variable(name) => {
                let mut set = std::collections::HashSet::new();
                set.insert(name.clone());
                set
            }
            Term::Aggregate(func, var) => {
                let mut set = std::collections::HashSet::new();
                // For ranking aggregates, return ALL output_vars
                match func {
                    AggregateFunc::TopK { output_vars, .. }
                    | AggregateFunc::TopKThreshold { output_vars, .. }
                    | AggregateFunc::WithinRadius { output_vars, .. } => {
                        for v in output_vars {
                            set.insert(v.clone());
                        }
                    }
                    _ => {}
                }
                if !var.is_empty() {
                    set.insert(var.clone());
                }
                set
            }
            Term::Arithmetic(expr) => expr.variables(),
            Term::FunctionCall(_, args) => args.iter().flat_map(Term::variables).collect(),
            _ => std::collections::HashSet::new(),
        }
    }
}

/// Represents an atom like edge(x, y) or reach(x)
#[derive(Debug, Clone, PartialEq)]
pub struct Atom {
    pub relation: String,
    pub args: Vec<Term>,
}

impl Atom {
    /// Create a new atom
    pub fn new(relation: String, args: Vec<Term>) -> Self {
        Atom { relation, args }
    }

    /// Get all variables in this atom (including variables inside aggregates and arithmetic)
    pub fn variables(&self) -> HashSet<String> {
        let mut vars = HashSet::new();
        for term in &self.args {
            vars.extend(term.variables());
        }
        vars
    }

    /// Check if this atom contains any aggregate terms
    pub fn has_aggregates(&self) -> bool {
        self.args.iter().any(Term::is_aggregate)
    }

    /// Check if this atom contains any arithmetic expressions
    pub fn has_arithmetic(&self) -> bool {
        self.args.iter().any(Term::is_arithmetic)
    }

    /// Get all aggregate terms in this atom
    pub fn aggregates(&self) -> Vec<(&AggregateFunc, &str)> {
        self.args.iter().filter_map(|t| t.as_aggregate()).collect()
    }

    /// Get all arithmetic expressions in this atom
    pub fn arithmetic_terms(&self) -> Vec<(usize, &ArithExpr)> {
        self.args
            .iter()
            .enumerate()
            .filter_map(|(i, t)| t.as_arithmetic().map(|e| (i, e)))
            .collect()
    }

    /// Check if this atom contains any function calls
    pub fn has_function_calls(&self) -> bool {
        self.args.iter().any(Term::is_function_call)
    }

    /// Get all function call terms in this atom
    pub fn function_calls(&self) -> Vec<(usize, &BuiltinFunc, &[Term])> {
        self.args
            .iter()
            .enumerate()
            .filter_map(|(i, t)| t.as_function_call().map(|(f, a)| (i, f, a)))
            .collect()
    }

    /// Check if this atom contains any vector literals
    pub fn has_vector_literals(&self) -> bool {
        self.args.iter().any(Term::is_vector_literal)
    }

    /// Get the arity (number of arguments) of this atom
    pub fn arity(&self) -> usize {
        self.args.len()
    }

    /// Get the effective output arity, accounting for ranking aggregates that expand
    /// into multiple output columns. For non-ranking atoms, this equals `args.len()`.
    pub fn effective_arity(&self) -> usize {
        self.args
            .iter()
            .map(|term| match term {
                Term::Aggregate(func, _) if func.is_ranking() => match func {
                    AggregateFunc::TopK { output_vars, .. }
                    | AggregateFunc::TopKThreshold { output_vars, .. }
                    | AggregateFunc::WithinRadius { output_vars, .. } => output_vars.len(),
                    _ => 1,
                },
                _ => 1,
            })
            .sum()
    }
}

/// Comparison operators for filter predicates in rule bodies
#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOp {
    Equal,          // =
    NotEqual,       // !=
    LessThan,       // <
    LessOrEqual,    // <=
    GreaterThan,    // >
    GreaterOrEqual, // >=
}

/// Represents a body predicate (positive atom, negated atom, or comparison)
/// Used in rule bodies to support stratified negation and filtering
#[derive(Debug, Clone, PartialEq)]
pub enum BodyPredicate {
    Positive(Atom),
    Negated(Atom),
    /// Comparison predicate: left op right (e.g., X = Y, X < 5)
    Comparison(Term, ComparisonOp, Term),
    /// HNSW nearest neighbor search: hnsw_nearest(index, query, k, id_var, dist_var)
    /// Example: hnsw_nearest("doc_idx", QueryVec, 10, Id, Distance)
    HnswNearest {
        /// Name of the index (string literal)
        index_name: String,
        /// Query vector (can be a variable, vector literal, or column reference)
        query: Term,
        /// Number of neighbors to return
        k: usize,
        /// Variable to bind result tuple IDs
        id_var: String,
        /// Variable to bind distances
        distance_var: String,
        /// Optional ef_search override
        ef_search: Option<usize>,
    },
}

impl BodyPredicate {
    /// Get the underlying atom (returns None for Comparison/HnswNearest predicates)
    pub fn atom(&self) -> Option<&Atom> {
        match self {
            BodyPredicate::Positive(atom) | BodyPredicate::Negated(atom) => Some(atom),
            BodyPredicate::Comparison(_, _, _) | BodyPredicate::HnswNearest { .. } => None,
        }
    }

    /// Check if this is a positive atom
    pub fn is_positive(&self) -> bool {
        matches!(self, BodyPredicate::Positive(_))
    }

    /// Check if this is a negated atom
    pub fn is_negated(&self) -> bool {
        matches!(self, BodyPredicate::Negated(_))
    }

    /// Check if this is a comparison predicate
    pub fn is_comparison(&self) -> bool {
        matches!(self, BodyPredicate::Comparison(_, _, _))
    }

    /// Check if this is an HNSW nearest neighbor predicate
    pub fn is_hnsw_nearest(&self) -> bool {
        matches!(self, BodyPredicate::HnswNearest { .. })
    }

    /// Get all variables in this predicate
    pub fn variables(&self) -> HashSet<String> {
        match self {
            BodyPredicate::Positive(atom) | BodyPredicate::Negated(atom) => atom.variables(),
            BodyPredicate::Comparison(left, _, right) => {
                let mut vars = HashSet::new();
                if let Term::Variable(v) = left {
                    vars.insert(v.clone());
                }
                if let Term::Variable(v) = right {
                    vars.insert(v.clone());
                }
                vars
            }
            BodyPredicate::HnswNearest {
                query,
                id_var,
                distance_var,
                ..
            } => {
                let mut vars = HashSet::new();
                // Query might be a variable
                if let Term::Variable(v) = query {
                    vars.insert(v.clone());
                }
                // id_var and distance_var are bound by this predicate
                vars.insert(id_var.clone());
                vars.insert(distance_var.clone());
                vars
            }
        }
    }
}

/// Represents a single Datalog rule
#[derive(Debug, Clone)]
pub struct Rule {
    pub head: Atom,
    pub body: Vec<BodyPredicate>,
}

impl Rule {
    /// Create a new rule
    pub fn new(head: Atom, body: Vec<BodyPredicate>) -> Self {
        Rule { head, body }
    }

    /// Create a rule with only positive body atoms (no negation)
    pub fn new_simple(head: Atom, body: Vec<Atom>) -> Self {
        Rule {
            head,
            body: body.into_iter().map(BodyPredicate::Positive).collect(),
        }
    }

    /// Check if this rule is safe (range-restricted)
    ///
    /// A rule is safe if:
    /// 1. All head variables appear in positive body atoms
    /// 2. All variables in negated atoms appear in positive body atoms (range restriction)
    pub fn is_safe(&self) -> bool {
        let head_vars = self.head.variables();
        let safe_vars = self.positive_body_variables();

        // Check 1: Head variables must be bound by positive atoms
        if !head_vars.is_subset(&safe_vars) {
            return false;
        }

        // Check 2: Variables in negated atoms must be bound by positive atoms
        // This is the "range restriction" requirement for negation
        for pred in &self.body {
            if let BodyPredicate::Negated(atom) = pred {
                let neg_vars = atom.variables();
                if !neg_vars.is_subset(&safe_vars) {
                    return false;
                }
            }
        }

        true
    }

    /// Get all variables in positive body atoms and function call assignments
    ///
    /// This includes:
    /// 1. Variables from positive body atoms (e.g., `test_data(Id`, X) -> {Id, X})
    /// 2. Variables bound by function call assignments (e.g., Y = `abs_int64(X)` -> {Y})
    ///
    /// Variables bound by function calls are considered "safe" because they get
    /// their values from the function result, similar to how variables in positive
    /// atoms get their values from the relation.
    pub fn positive_body_variables(&self) -> HashSet<String> {
        let mut vars: HashSet<String> = self
            .body
            .iter()
            .filter(|pred| pred.is_positive())
            .flat_map(BodyPredicate::variables)
            .collect();

        // Also include variables bound by assignments and equalities.
        // Uses fixed-point iteration since variable binding can propagate through equalities:
        // e.g., if X is bound and Y = X, then Y is bound.
        let mut changed = true;
        while changed {
            changed = false;
            for pred in &self.body {
                if let BodyPredicate::Comparison(left, op, right) = pred {
                    if matches!(op, ComparisonOp::Equal) {
                        // Y = func(X) - Y is bound by the function result
                        if let (Term::Variable(v), Term::FunctionCall(_, _)) = (left, right) {
                            changed |= vars.insert(v.clone());
                        }
                        // func(X) = Y - Y is bound by the function result
                        if let (Term::FunctionCall(_, _), Term::Variable(v)) = (left, right) {
                            changed |= vars.insert(v.clone());
                        }
                        // Y = X * 2 (or any arithmetic) - Y is bound by the arithmetic result
                        if let (Term::Variable(v), Term::Arithmetic(_)) = (left, right) {
                            changed |= vars.insert(v.clone());
                        }
                        // X * 2 = Y - Y is bound by the arithmetic result
                        if let (Term::Arithmetic(_), Term::Variable(v)) = (left, right) {
                            changed |= vars.insert(v.clone());
                        }
                        // Y = constant - Y is bound by the constant
                        if let (
                            Term::Variable(v),
                            Term::Constant(_)
                            | Term::FloatConstant(_)
                            | Term::StringConstant(_)
                            | Term::BoolConstant(_),
                        ) = (left, right)
                        {
                            changed |= vars.insert(v.clone());
                        }
                        // constant = Y - Y is bound by the constant
                        if let (
                            Term::Constant(_)
                            | Term::FloatConstant(_)
                            | Term::StringConstant(_)
                            | Term::BoolConstant(_),
                            Term::Variable(v),
                        ) = (left, right)
                        {
                            changed |= vars.insert(v.clone());
                        }
                        // Y = X (variable equality) - if one is bound, the other becomes bound
                        if let (Term::Variable(v1), Term::Variable(v2)) = (left, right) {
                            if vars.contains(v1) {
                                changed |= vars.insert(v2.clone());
                            }
                            if vars.contains(v2) {
                                changed |= vars.insert(v1.clone());
                            }
                        }
                    }
                }
            }
        }

        vars
    }

    /// Get all variables in this rule
    pub fn variables(&self) -> HashSet<String> {
        let mut vars = self.head.variables();

        for pred in &self.body {
            vars.extend(pred.variables());
        }

        vars
    }

    /// Check if this rule is recursive (head relation appears in body)
    pub fn is_recursive(&self) -> bool {
        self.body.iter().any(|pred| {
            pred.atom()
                .is_some_and(|a| a.relation == self.head.relation)
        })
    }

    /// Get all positive body atoms
    pub fn positive_body_atoms(&self) -> Vec<&Atom> {
        self.body
            .iter()
            .filter_map(|pred| match pred {
                BodyPredicate::Positive(atom) => Some(atom),
                BodyPredicate::Negated(_)
                | BodyPredicate::Comparison(_, _, _)
                | BodyPredicate::HnswNearest { .. } => None,
            })
            .collect()
    }

    /// Get all negated body atoms
    pub fn negated_body_atoms(&self) -> Vec<&Atom> {
        self.body
            .iter()
            .filter_map(|pred| match pred {
                BodyPredicate::Negated(atom) => Some(atom),
                BodyPredicate::Positive(_)
                | BodyPredicate::Comparison(_, _, _)
                | BodyPredicate::HnswNearest { .. } => None,
            })
            .collect()
    }

    /// Get all HNSW nearest neighbor predicates
    pub fn hnsw_nearest_predicates(&self) -> Vec<&BodyPredicate> {
        self.body
            .iter()
            .filter(|pred| pred.is_hnsw_nearest())
            .collect()
    }
}

/// Represents a complete Datalog program
#[derive(Debug, Clone)]
pub struct Program {
    pub rules: Vec<Rule>,
}

impl Program {
    /// Create a new empty program
    pub fn new() -> Self {
        Program { rules: Vec::new() }
    }

    /// Add a rule to the program
    pub fn add_rule(&mut self, rule: Rule) {
        self.rules.push(rule);
    }

    /// Returns all IDB relations (those that appear as heads of rules)
    pub fn idbs(&self) -> Vec<String> {
        let mut idbs: Vec<String> = self
            .rules
            .iter()
            .map(|rule| rule.head.relation.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        idbs.sort();
        idbs
    }

    /// Returns all EDB relations (those that appear in bodies but never as heads)
    pub fn edbs(&self) -> Vec<String> {
        let idb_set: HashSet<String> = self.idbs().into_iter().collect();

        let mut body_relations: HashSet<String> = HashSet::new();
        for rule in &self.rules {
            for pred in &rule.body {
                if let Some(atom) = pred.atom() {
                    body_relations.insert(atom.relation.clone());
                }
            }
        }

        let mut edbs: Vec<String> = body_relations.difference(&idb_set).cloned().collect();

        edbs.sort();
        edbs
    }

    /// Get all relation names (both EDB and IDB)
    pub fn all_relations(&self) -> Vec<String> {
        let mut all: HashSet<String> = HashSet::new();

        // Add IDBs
        for idb in self.idbs() {
            all.insert(idb);
        }

        // Add EDBs
        for edb in self.edbs() {
            all.insert(edb);
        }

        let mut result: Vec<String> = all.into_iter().collect();
        result.sort();
        result
    }

    /// Check if all rules in the program are safe
    pub fn is_safe(&self) -> bool {
        self.rules.iter().all(Rule::is_safe)
    }

    /// Get all recursive rules
    pub fn recursive_rules(&self) -> Vec<&Rule> {
        self.rules
            .iter()
            .filter(|rule| rule.is_recursive())
            .collect()
    }

    /// Get all non-recursive rules
    pub fn non_recursive_rules(&self) -> Vec<&Rule> {
        self.rules
            .iter()
            .filter(|rule| !rule.is_recursive())
            .collect()
    }
}

impl Default for Program {
    fn default() -> Self {
        Self::new()
    }
}

// Display Implementations for Datalog Formatting
impl ArithOp {
    /// Return numeric precedence: higher value = binds tighter.
    fn precedence(&self) -> u8 {
        match self {
            ArithOp::Add | ArithOp::Sub => 0,
            ArithOp::Mul | ArithOp::Div | ArithOp::Mod => 1,
        }
    }
}

impl std::fmt::Display for ArithExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArithExpr::Variable(name) => write!(f, "{name}"),
            ArithExpr::Constant(val) => write!(f, "{val}"),
            ArithExpr::FloatConstant(bits) => write!(f, "{}", f64::from_bits(*bits)),
            ArithExpr::Binary { op, left, right } => {
                let parent_prec = op.precedence();

                // Left child: parenthesize only if it has strictly lower precedence
                // (left-associative parsing handles same-precedence correctly)
                match left.as_ref() {
                    ArithExpr::Binary { op: child_op, .. }
                        if child_op.precedence() < parent_prec =>
                    {
                        write!(f, "({left})")?;
                    }
                    _ => write!(f, "{left}")?,
                }

                write!(f, "{}", op.as_str())?;

                // Right child: parenthesize if lower OR equal precedence
                // (equal because parser is left-associative, e.g. a/(b*c) != a/b*c)
                match right.as_ref() {
                    ArithExpr::Binary { op: child_op, .. }
                        if child_op.precedence() <= parent_prec =>
                    {
                        write!(f, "({right})")?;
                    }
                    _ => write!(f, "{right}")?,
                }

                Ok(())
            }
        }
    }
}

impl std::fmt::Display for AggregateFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateFunc::Count => write!(f, "count"),
            AggregateFunc::CountDistinct => write!(f, "count_distinct"),
            AggregateFunc::Sum => write!(f, "sum"),
            AggregateFunc::Min => write!(f, "min"),
            AggregateFunc::Max => write!(f, "max"),
            AggregateFunc::Avg => write!(f, "avg"),
            AggregateFunc::TopK {
                k,
                order_var,
                output_vars,
                descending,
            } => {
                write!(f, "top_k<{k}")?;
                for v in output_vars {
                    if v == order_var {
                        let dir = if *descending { "desc" } else { "asc" };
                        // Single var without annotation: omit if default (desc)
                        if output_vars.len() == 1 && *descending {
                            write!(f, ", {v}")?;
                        } else {
                            write!(f, ", {v}:{dir}")?;
                        }
                    } else {
                        write!(f, ", {v}")?;
                    }
                }
                write!(f, ">")
            }
            AggregateFunc::TopKThreshold {
                k,
                order_var,
                output_vars,
                threshold,
                descending,
            } => {
                write!(f, "top_k_threshold<{k}, {threshold}")?;
                for v in output_vars {
                    if v == order_var {
                        let dir = if *descending { "desc" } else { "asc" };
                        if output_vars.len() == 1 && *descending {
                            write!(f, ", {v}")?;
                        } else {
                            write!(f, ", {v}:{dir}")?;
                        }
                    } else {
                        write!(f, ", {v}")?;
                    }
                }
                write!(f, ">")
            }
            AggregateFunc::WithinRadius {
                distance_var,
                output_vars,
                max_distance,
            } => {
                write!(f, "within_radius<{max_distance}")?;
                for v in output_vars {
                    if v == distance_var {
                        // within_radius default is asc, so omit annotation for single var
                        if output_vars.len() == 1 {
                            write!(f, ", {v}")?;
                        } else {
                            write!(f, ", {v}:asc")?;
                        }
                    } else {
                        write!(f, ", {v}")?;
                    }
                }
                write!(f, ">")
            }
        }
    }
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Term::Variable(name) => write!(f, "{name}"),
            Term::Constant(val) => write!(f, "{val}"),
            Term::StringConstant(s) => write!(f, "\"{s}\""),
            Term::BoolConstant(b) => write!(f, "{b}"),
            Term::FloatConstant(val) => write!(f, "{val}"),
            Term::Placeholder => write!(f, "_"),
            Term::Arithmetic(expr) => write!(f, "{expr}"),
            Term::Aggregate(func, var) => {
                // For ranking aggregates, the format is different (no var)
                match func {
                    AggregateFunc::TopK { .. }
                    | AggregateFunc::TopKThreshold { .. }
                    | AggregateFunc::WithinRadius { .. } => {
                        write!(f, "{func}")
                    }
                    _ => write!(f, "{func}<{var}>"),
                }
            }
            Term::VectorLiteral(values) => {
                let vals: Vec<String> = values.iter().map(ToString::to_string).collect();
                write!(f, "[{}]", vals.join(", "))
            }
            Term::FunctionCall(func, args) => {
                let args_str: Vec<String> = args.iter().map(ToString::to_string).collect();
                write!(f, "{}({})", func.as_str(), args_str.join(", "))
            }
            Term::FieldAccess(base, field) => {
                write!(f, "{base}.{field}")
            }
            Term::RecordPattern(fields) => {
                let formatted: Vec<String> = fields
                    .iter()
                    .map(|(name, term)| format!("{name}: {term}"))
                    .collect();
                write!(f, "{{ {} }}", formatted.join(", "))
            }
        }
    }
}

impl std::fmt::Display for Atom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let args: Vec<String> = self.args.iter().map(ToString::to_string).collect();
        write!(f, "{}({})", self.relation, args.join(", "))
    }
}

impl std::fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op_str = match self {
            ComparisonOp::Equal => "=",
            ComparisonOp::NotEqual => "!=",
            ComparisonOp::LessThan => "<",
            ComparisonOp::LessOrEqual => "<=",
            ComparisonOp::GreaterThan => ">",
            ComparisonOp::GreaterOrEqual => ">=",
        };
        write!(f, "{op_str}")
    }
}

impl std::fmt::Display for BodyPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BodyPredicate::Positive(atom) => write!(f, "{atom}"),
            BodyPredicate::Negated(atom) => write!(f, "!{atom}"),
            BodyPredicate::Comparison(left, op, right) => {
                write!(f, "{left} {op} {right}")
            }
            BodyPredicate::HnswNearest {
                index_name,
                query,
                k,
                id_var,
                distance_var,
                ef_search,
            } => {
                if let Some(ef) = ef_search {
                    write!(
                        f,
                        "hnsw_nearest(\"{index_name}\", {query}, {k}, {id_var}, {distance_var}, {ef})"
                    )
                } else {
                    write!(
                        f,
                        "hnsw_nearest(\"{index_name}\", {query}, {k}, {id_var}, {distance_var})"
                    )
                }
            }
        }
    }
}

impl std::fmt::Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.body.is_empty() {
            write!(f, "{}", self.head)
        } else {
            let body_str: Vec<String> = self.body.iter().map(ToString::to_string).collect();
            write!(f, "{} <- {}", self.head, body_str.join(", "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_func_parse() {
        assert_eq!(AggregateFunc::parse("count"), Some(AggregateFunc::Count));
        assert_eq!(AggregateFunc::parse("sum"), Some(AggregateFunc::Sum));
        assert_eq!(AggregateFunc::parse("min"), Some(AggregateFunc::Min));
        assert_eq!(AggregateFunc::parse("max"), Some(AggregateFunc::Max));
        assert_eq!(AggregateFunc::parse("avg"), Some(AggregateFunc::Avg));
    }

    #[test]
    fn test_term_is_variable() {
        assert!(Term::Variable("x".to_string()).is_variable());
        assert!(!Term::Constant(42).is_variable());
    }

    #[test]
    fn test_atom_creation() {
        let atom = Atom::new(
            "edge".to_string(),
            vec![
                Term::Variable("x".to_string()),
                Term::Variable("y".to_string()),
            ],
        );

        assert_eq!(atom.relation, "edge");
        assert_eq!(atom.arity(), 2);
    }

    #[test]
    fn test_rule_safety() {
        let head = Atom::new("reach".to_string(), vec![Term::Variable("y".to_string())]);
        let body = vec![
            BodyPredicate::Positive(Atom::new(
                "reach".to_string(),
                vec![Term::Variable("x".to_string())],
            )),
            BodyPredicate::Positive(Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            )),
        ];

        let rule = Rule::new(head, body);
        assert!(rule.is_safe());
        assert!(rule.is_recursive());
    }

    #[test]
    fn test_program_edbs_idbs() {
        let mut program = Program::new();

        program.add_rule(Rule::new_simple(
            Atom::new("reach".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "source".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        ));

        program.add_rule(Rule::new_simple(
            Atom::new("reach".to_string(), vec![Term::Variable("y".to_string())]),
            vec![
                Atom::new("reach".to_string(), vec![Term::Variable("x".to_string())]),
                Atom::new(
                    "edge".to_string(),
                    vec![
                        Term::Variable("x".to_string()),
                        Term::Variable("y".to_string()),
                    ],
                ),
            ],
        ));

        let idbs = program.idbs();
        let edbs = program.edbs();

        assert_eq!(idbs, vec!["reach"]);
        assert_eq!(edbs, vec!["edge", "source"]);
    }

    // === Additional Coverage ===

    // --- BuiltinFunc ---

    #[test]
    fn test_builtin_func_parse_all_categories() {
        // Distance
        assert_eq!(
            BuiltinFunc::parse("euclidean"),
            Some(BuiltinFunc::Euclidean)
        );
        assert_eq!(BuiltinFunc::parse("cosine"), Some(BuiltinFunc::Cosine));
        assert_eq!(BuiltinFunc::parse("dot"), Some(BuiltinFunc::DotProduct));
        assert_eq!(
            BuiltinFunc::parse("manhattan"),
            Some(BuiltinFunc::Manhattan)
        );
        // Vector ops
        assert_eq!(
            BuiltinFunc::parse("normalize"),
            Some(BuiltinFunc::VecNormalize)
        );
        assert_eq!(BuiltinFunc::parse("vec_dim"), Some(BuiltinFunc::VecDim));
        assert_eq!(BuiltinFunc::parse("vec_add"), Some(BuiltinFunc::VecAdd));
        assert_eq!(BuiltinFunc::parse("vec_scale"), Some(BuiltinFunc::VecScale));
        // Temporal
        assert_eq!(BuiltinFunc::parse("time_now"), Some(BuiltinFunc::TimeNow));
        assert_eq!(BuiltinFunc::parse("time_diff"), Some(BuiltinFunc::TimeDiff));
        assert_eq!(
            BuiltinFunc::parse("time_decay"),
            Some(BuiltinFunc::TimeDecay)
        );
        // Math
        assert_eq!(BuiltinFunc::parse("sqrt"), Some(BuiltinFunc::Sqrt));
        assert_eq!(BuiltinFunc::parse("abs"), Some(BuiltinFunc::Abs));
        assert_eq!(BuiltinFunc::parse("floor"), Some(BuiltinFunc::Floor));
        assert_eq!(BuiltinFunc::parse("ceil"), Some(BuiltinFunc::Ceil));
        // String
        assert_eq!(BuiltinFunc::parse("len"), Some(BuiltinFunc::Len));
        assert_eq!(BuiltinFunc::parse("upper"), Some(BuiltinFunc::Upper));
        assert_eq!(BuiltinFunc::parse("concat"), Some(BuiltinFunc::Concat));
        // Unknown
        assert_eq!(BuiltinFunc::parse("nonexistent"), None);
    }

    #[test]
    fn test_builtin_func_arity() {
        assert_eq!(BuiltinFunc::Euclidean.arity(), 2);
        assert_eq!(BuiltinFunc::VecNormalize.arity(), 1);
        assert_eq!(BuiltinFunc::TimeNow.arity(), 0);
        assert_eq!(BuiltinFunc::LshBucket.arity(), 3);
        assert_eq!(BuiltinFunc::IntervalsOverlap.arity(), 4);
        assert_eq!(BuiltinFunc::Pow.arity(), 2);
        assert_eq!(BuiltinFunc::Len.arity(), 1);
        assert_eq!(BuiltinFunc::Substr.arity(), 3);
    }

    #[test]
    fn test_builtin_func_as_str_roundtrip() {
        let funcs = [
            BuiltinFunc::Euclidean,
            BuiltinFunc::Cosine,
            BuiltinFunc::DotProduct,
            BuiltinFunc::Sqrt,
            BuiltinFunc::Abs,
            BuiltinFunc::Len,
            BuiltinFunc::TimeNow,
            BuiltinFunc::Floor,
        ];
        for func in &funcs {
            let name = func.as_str();
            assert_eq!(BuiltinFunc::parse(name).as_ref(), Some(func));
        }
    }

    // --- ArithOp ---

    #[test]
    fn test_arith_op_parse() {
        assert_eq!(ArithOp::parse("+"), Some(ArithOp::Add));
        assert_eq!(ArithOp::parse("-"), Some(ArithOp::Sub));
        assert_eq!(ArithOp::parse("*"), Some(ArithOp::Mul));
        assert_eq!(ArithOp::parse("/"), Some(ArithOp::Div));
        assert_eq!(ArithOp::parse("%"), Some(ArithOp::Mod));
        assert_eq!(ArithOp::parse("^"), None);
    }

    #[test]
    fn test_arith_op_as_str() {
        assert_eq!(ArithOp::Add.as_str(), "+");
        assert_eq!(ArithOp::Sub.as_str(), "-");
        assert_eq!(ArithOp::Mul.as_str(), "*");
        assert_eq!(ArithOp::Div.as_str(), "/");
        assert_eq!(ArithOp::Mod.as_str(), "%");
    }

    // --- ArithExpr ---

    #[test]
    fn test_arith_expr_from_float() {
        let expr = ArithExpr::from_float(3.14);
        assert!((expr.as_f64().unwrap() - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn test_arith_expr_as_f64_constant() {
        let expr = ArithExpr::Constant(42);
        assert_eq!(expr.as_f64(), Some(42.0));
    }

    #[test]
    fn test_arith_expr_as_f64_variable() {
        let expr = ArithExpr::Variable("x".to_string());
        assert_eq!(expr.as_f64(), None);
    }

    #[test]
    fn test_arith_expr_variables() {
        let expr = ArithExpr::Binary {
            op: ArithOp::Add,
            left: Box::new(ArithExpr::Variable("x".to_string())),
            right: Box::new(ArithExpr::Variable("y".to_string())),
        };
        let vars = expr.variables();
        assert!(vars.contains("x"));
        assert!(vars.contains("y"));
        assert_eq!(vars.len(), 2);
    }

    #[test]
    fn test_arith_expr_is_simple() {
        assert!(ArithExpr::Variable("x".to_string()).is_simple());
        assert!(ArithExpr::Constant(1).is_simple());
        assert!(ArithExpr::from_float(1.0).is_simple());
        assert!(!ArithExpr::Binary {
            op: ArithOp::Add,
            left: Box::new(ArithExpr::Constant(1)),
            right: Box::new(ArithExpr::Constant(2)),
        }
        .is_simple());
    }

    #[test]
    fn test_arith_expr_try_eval_constant() {
        // Simple constant
        assert_eq!(ArithExpr::Constant(10).try_eval_constant(), Some(10));
        // Variable → None
        assert_eq!(
            ArithExpr::Variable("x".to_string()).try_eval_constant(),
            None
        );
        // Float → None
        assert_eq!(ArithExpr::from_float(1.0).try_eval_constant(), None);
        // Binary: 3 + 4 = 7
        assert_eq!(
            ArithExpr::Binary {
                op: ArithOp::Add,
                left: Box::new(ArithExpr::Constant(3)),
                right: Box::new(ArithExpr::Constant(4)),
            }
            .try_eval_constant(),
            Some(7)
        );
        // Division by zero → None
        assert_eq!(
            ArithExpr::Binary {
                op: ArithOp::Div,
                left: Box::new(ArithExpr::Constant(10)),
                right: Box::new(ArithExpr::Constant(0)),
            }
            .try_eval_constant(),
            None
        );
        // Modulo by zero → None
        assert_eq!(
            ArithExpr::Binary {
                op: ArithOp::Mod,
                left: Box::new(ArithExpr::Constant(10)),
                right: Box::new(ArithExpr::Constant(0)),
            }
            .try_eval_constant(),
            None
        );
    }

    // --- Term ---

    #[test]
    fn test_term_type_checks() {
        assert!(Term::Variable("x".to_string()).is_variable());
        assert!(Term::Constant(42).is_constant());
        assert!(Term::Aggregate(AggregateFunc::Count, "x".to_string()).is_aggregate());
        assert!(Term::Arithmetic(ArithExpr::Constant(1)).is_arithmetic());
        assert!(
            Term::FunctionCall(BuiltinFunc::Len, vec![Term::Variable("s".to_string())])
                .is_function_call()
        );
        assert!(Term::VectorLiteral(vec![1.0, 2.0]).is_vector_literal());
        assert!(Term::FloatConstant(3.14).is_float_constant());
    }

    #[test]
    fn test_term_as_variable() {
        assert_eq!(Term::Variable("x".to_string()).as_variable(), Some("x"));
        assert_eq!(Term::Constant(1).as_variable(), None);
    }

    #[test]
    fn test_term_as_aggregate() {
        let t = Term::Aggregate(AggregateFunc::Sum, "x".to_string());
        let (func, var) = t.as_aggregate().unwrap();
        assert!(matches!(func, AggregateFunc::Sum));
        assert_eq!(var, "x");
        assert!(Term::Constant(1).as_aggregate().is_none());
    }

    #[test]
    fn test_term_as_arithmetic() {
        let t = Term::Arithmetic(ArithExpr::Constant(42));
        assert!(t.as_arithmetic().is_some());
        assert!(Term::Constant(1).as_arithmetic().is_none());
    }

    #[test]
    fn test_term_as_function_call() {
        let t = Term::FunctionCall(BuiltinFunc::Len, vec![Term::Variable("s".to_string())]);
        let (func, args) = t.as_function_call().unwrap();
        assert!(matches!(func, BuiltinFunc::Len));
        assert_eq!(args.len(), 1);
        assert!(Term::Constant(1).as_function_call().is_none());
    }

    #[test]
    fn test_term_as_vector_literal() {
        let t = Term::VectorLiteral(vec![1.0, 2.0, 3.0]);
        assert_eq!(t.as_vector_literal().unwrap().len(), 3);
        assert!(Term::Constant(1).as_vector_literal().is_none());
    }

    #[test]
    fn test_term_as_float() {
        assert_eq!(Term::FloatConstant(3.14).as_float(), Some(3.14));
        assert_eq!(Term::Constant(5).as_float(), Some(5.0));
        assert_eq!(Term::Variable("x".to_string()).as_float(), None);
    }

    #[test]
    fn test_term_variables() {
        // Variable
        let vars = Term::Variable("x".to_string()).variables();
        assert!(vars.contains("x"));
        // Constant → empty
        assert!(Term::Constant(1).variables().is_empty());
        // FunctionCall → collects arg variables
        let t = Term::FunctionCall(
            BuiltinFunc::VecAdd,
            vec![
                Term::Variable("a".to_string()),
                Term::Variable("b".to_string()),
            ],
        );
        let vars = t.variables();
        assert!(vars.contains("a"));
        assert!(vars.contains("b"));
    }

    // --- AggregateFunc ---

    #[test]
    fn test_aggregate_func_is_simple() {
        assert!(AggregateFunc::Count.is_simple());
        assert!(AggregateFunc::CountDistinct.is_simple());
        assert!(AggregateFunc::Sum.is_simple());
        assert!(AggregateFunc::Min.is_simple());
        assert!(AggregateFunc::Max.is_simple());
        assert!(AggregateFunc::Avg.is_simple());
    }

    #[test]
    fn test_aggregate_func_is_ranking() {
        let top_k = AggregateFunc::TopK {
            k: 3,
            order_var: "Score".to_string(),
            output_vars: vec!["Name".to_string(), "Score".to_string()],
            descending: true,
        };
        assert!(top_k.is_ranking());
        assert!(!top_k.is_simple());
        assert!(!AggregateFunc::Count.is_ranking());
    }

    #[test]
    fn test_parse_top_k() {
        let result = AggregateFunc::parse_top_k("3, Name, Score:desc").unwrap();
        if let AggregateFunc::TopK {
            k,
            order_var,
            output_vars,
            descending,
        } = result
        {
            assert_eq!(k, 3);
            assert_eq!(order_var, "Score");
            assert_eq!(output_vars, vec!["Name", "Score"]);
            assert!(descending);
        } else {
            panic!("Expected TopK");
        }
    }

    #[test]
    fn test_parse_top_k_single_var() {
        let result = AggregateFunc::parse_top_k("5, Score").unwrap();
        if let AggregateFunc::TopK {
            k,
            order_var,
            descending,
            ..
        } = result
        {
            assert_eq!(k, 5);
            assert_eq!(order_var, "Score");
            assert!(descending); // default desc for top_k
        } else {
            panic!("Expected TopK");
        }
    }

    #[test]
    fn test_parse_top_k_asc() {
        let result = AggregateFunc::parse_top_k("2, Score:asc").unwrap();
        if let AggregateFunc::TopK { descending, .. } = result {
            assert!(!descending);
        } else {
            panic!("Expected TopK");
        }
    }

    #[test]
    fn test_parse_top_k_too_few_params() {
        assert!(AggregateFunc::parse_top_k("3").is_none());
    }

    #[test]
    fn test_parse_within_radius() {
        let result = AggregateFunc::parse_within_radius("10.0, Name, Distance:asc").unwrap();
        if let AggregateFunc::WithinRadius {
            max_distance,
            distance_var,
            output_vars,
        } = result
        {
            assert!((max_distance - 10.0).abs() < f64::EPSILON);
            assert_eq!(distance_var, "Distance");
            assert_eq!(output_vars.len(), 2);
        } else {
            panic!("Expected WithinRadius");
        }
    }

    #[test]
    fn test_parse_top_k_threshold() {
        let result = AggregateFunc::parse_top_k_threshold("5, 0.5, Name, Score:desc").unwrap();
        if let AggregateFunc::TopKThreshold {
            k,
            threshold,
            order_var,
            ..
        } = result
        {
            assert_eq!(k, 5);
            assert!((threshold - 0.5).abs() < f64::EPSILON);
            assert_eq!(order_var, "Score");
        } else {
            panic!("Expected TopKThreshold");
        }
    }

    #[test]
    fn test_parse_count_distinct() {
        assert_eq!(
            AggregateFunc::parse("count_distinct"),
            Some(AggregateFunc::CountDistinct)
        );
        assert_eq!(
            AggregateFunc::parse("countdistinct"),
            Some(AggregateFunc::CountDistinct)
        );
    }

    // --- Atom ---

    #[test]
    fn test_atom_has_aggregates() {
        let atom = Atom::new(
            "stats".to_string(),
            vec![
                Term::Variable("x".to_string()),
                Term::Aggregate(AggregateFunc::Count, "x".to_string()),
            ],
        );
        assert!(atom.has_aggregates());
        assert_eq!(atom.aggregates().len(), 1);
    }

    #[test]
    fn test_atom_has_arithmetic() {
        let atom = Atom::new(
            "calc".to_string(),
            vec![
                Term::Variable("x".to_string()),
                Term::Arithmetic(ArithExpr::Binary {
                    op: ArithOp::Add,
                    left: Box::new(ArithExpr::Variable("x".to_string())),
                    right: Box::new(ArithExpr::Constant(1)),
                }),
            ],
        );
        assert!(atom.has_arithmetic());
        assert_eq!(atom.arithmetic_terms().len(), 1);
    }

    #[test]
    fn test_atom_has_function_calls() {
        let atom = Atom::new(
            "dist".to_string(),
            vec![Term::FunctionCall(
                BuiltinFunc::Euclidean,
                vec![
                    Term::Variable("v1".to_string()),
                    Term::Variable("v2".to_string()),
                ],
            )],
        );
        assert!(atom.has_function_calls());
        assert_eq!(atom.function_calls().len(), 1);
    }

    #[test]
    fn test_atom_effective_arity_simple() {
        let atom = Atom::new(
            "edge".to_string(),
            vec![
                Term::Variable("x".to_string()),
                Term::Variable("y".to_string()),
            ],
        );
        assert_eq!(atom.arity(), 2);
        assert_eq!(atom.effective_arity(), 2);
    }

    #[test]
    fn test_atom_effective_arity_ranking() {
        let atom = Atom::new(
            "ranked".to_string(),
            vec![
                Term::Variable("group".to_string()),
                Term::Aggregate(
                    AggregateFunc::TopK {
                        k: 3,
                        order_var: "Score".to_string(),
                        output_vars: vec!["Name".to_string(), "Score".to_string()],
                        descending: true,
                    },
                    "Score".to_string(),
                ),
            ],
        );
        assert_eq!(atom.arity(), 2);
        assert_eq!(atom.effective_arity(), 3); // 1 + 2 output vars
    }

    // --- BodyPredicate ---

    #[test]
    fn test_body_predicate_type_checks() {
        let pos = BodyPredicate::Positive(Atom::new("a".to_string(), vec![]));
        assert!(pos.is_positive());
        assert!(!pos.is_negated());
        assert!(!pos.is_comparison());
        assert!(!pos.is_hnsw_nearest());
        assert!(pos.atom().is_some());

        let neg = BodyPredicate::Negated(Atom::new("b".to_string(), vec![]));
        assert!(neg.is_negated());
        assert!(neg.atom().is_some());

        let cmp = BodyPredicate::Comparison(
            Term::Variable("x".to_string()),
            ComparisonOp::LessThan,
            Term::Constant(10),
        );
        assert!(cmp.is_comparison());
        assert!(cmp.atom().is_none());
    }

    #[test]
    fn test_body_predicate_hnsw_nearest() {
        let hnsw = BodyPredicate::HnswNearest {
            index_name: "idx".to_string(),
            query: Term::Variable("q".to_string()),
            k: 10,
            id_var: "Id".to_string(),
            distance_var: "Dist".to_string(),
            ef_search: Some(64),
        };
        assert!(hnsw.is_hnsw_nearest());
        let vars = hnsw.variables();
        assert!(vars.contains("q"));
        assert!(vars.contains("Id"));
        assert!(vars.contains("Dist"));
    }

    #[test]
    fn test_body_predicate_comparison_variables() {
        let cmp = BodyPredicate::Comparison(
            Term::Variable("x".to_string()),
            ComparisonOp::Equal,
            Term::Variable("y".to_string()),
        );
        let vars = cmp.variables();
        assert!(vars.contains("x"));
        assert!(vars.contains("y"));
    }

    // --- Rule ---

    #[test]
    fn test_rule_not_recursive() {
        let rule = Rule::new_simple(
            Atom::new(
                "path".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            ),
            vec![Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            )],
        );
        assert!(!rule.is_recursive());
    }

    #[test]
    fn test_rule_unsafe() {
        // Head has variable not in body
        let rule = Rule::new_simple(
            Atom::new("out".to_string(), vec![Term::Variable("z".to_string())]),
            vec![Atom::new(
                "in".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        );
        assert!(!rule.is_safe());
    }

    #[test]
    fn test_rule_positive_and_negated_body_atoms() {
        let rule = Rule::new(
            Atom::new("safe".to_string(), vec![Term::Variable("x".to_string())]),
            vec![
                BodyPredicate::Positive(Atom::new(
                    "node".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
                BodyPredicate::Negated(Atom::new(
                    "bad".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
            ],
        );
        assert_eq!(rule.positive_body_atoms().len(), 1);
        assert_eq!(rule.negated_body_atoms().len(), 1);
        assert!(rule.is_safe());
    }

    #[test]
    fn test_rule_variables() {
        let rule = Rule::new_simple(
            Atom::new("out".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "in".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            )],
        );
        let vars = rule.variables();
        assert!(vars.contains("x"));
        assert!(vars.contains("y"));
    }

    #[test]
    fn test_rule_hnsw_nearest_predicates() {
        let rule = Rule::new(
            Atom::new("near".to_string(), vec![Term::Variable("id".to_string())]),
            vec![BodyPredicate::HnswNearest {
                index_name: "idx".to_string(),
                query: Term::Variable("q".to_string()),
                k: 5,
                id_var: "id".to_string(),
                distance_var: "d".to_string(),
                ef_search: None,
            }],
        );
        assert_eq!(rule.hnsw_nearest_predicates().len(), 1);
    }

    // --- Program ---

    #[test]
    fn test_program_empty() {
        let program = Program::new();
        assert!(program.rules.is_empty());
        assert!(program.idbs().is_empty());
        assert!(program.edbs().is_empty());
        assert!(program.all_relations().is_empty());
        assert!(program.is_safe());
    }

    #[test]
    fn test_program_all_relations() {
        let mut program = Program::new();
        program.add_rule(Rule::new_simple(
            Atom::new("reach".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "source".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        ));
        let all = program.all_relations();
        assert!(all.contains(&"reach".to_string()));
        assert!(all.contains(&"source".to_string()));
    }

    #[test]
    fn test_program_recursive_and_non_recursive_rules() {
        let mut program = Program::new();
        // Non-recursive
        program.add_rule(Rule::new_simple(
            Atom::new("reach".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "source".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        ));
        // Recursive
        program.add_rule(Rule::new_simple(
            Atom::new("reach".to_string(), vec![Term::Variable("y".to_string())]),
            vec![
                Atom::new("reach".to_string(), vec![Term::Variable("x".to_string())]),
                Atom::new(
                    "edge".to_string(),
                    vec![
                        Term::Variable("x".to_string()),
                        Term::Variable("y".to_string()),
                    ],
                ),
            ],
        ));
        assert_eq!(program.recursive_rules().len(), 1);
        assert_eq!(program.non_recursive_rules().len(), 1);
    }

    #[test]
    fn test_program_is_safe_with_unsafe_rule() {
        let mut program = Program::new();
        program.add_rule(Rule::new_simple(
            Atom::new("out".to_string(), vec![Term::Variable("z".to_string())]),
            vec![Atom::new(
                "in".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        ));
        assert!(!program.is_safe());
    }
}
