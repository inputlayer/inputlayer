//! IR types for Datalog query plans, shared across all optimization passes.

use crate::ast::{ArithExpr, ComparisonOp};
use std::collections::{HashMap, HashSet};

// IR Node Types
/// Aggregate function types
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    /// Count rows
    Count,
    /// Count distinct values
    CountDistinct,
    /// Sum of values
    Sum,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Average value (returns float)
    Avg,
    /// Top-K: select top k results ordered by a column
    TopK {
        k: usize,
        /// Column index for ordering
        order_col: usize,
        /// If true, highest values first
        descending: bool,
    },
    /// Top-K with threshold: only include results above/below threshold
    TopKThreshold {
        k: usize,
        order_col: usize,
        threshold: f64,
        descending: bool,
    },
    /// Within radius: all results where `order_col` <= `max_distance`
    WithinRadius {
        /// Column index for distance
        distance_col: usize,
        max_distance: f64,
    },
}

/// Built-in function types for vector operations
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BuiltinFunction {
    /// Euclidean (L2) distance: euclidean(v1, v2)
    Euclidean,
    /// Cosine distance: cosine(v1, v2)
    Cosine,
    /// Dot product: dot(v1, v2)
    DotProduct,
    /// Manhattan (L1) distance: manhattan(v1, v2)
    Manhattan,
    /// Hamming distance between integers: hamming(a, b) -> Int64 (bit difference count)
    Hamming,
    /// LSH bucket hash: `lsh_bucket(v`, `table_idx`, `num_hyperplanes`)
    LshBucket,
    /// Normalize vector: normalize(v)
    VecNormalize,
    /// Get vector dimension: `vec_dim(v)`
    VecDim,
    /// Add vectors: `vec_add(v1`, v2.clone())
    VecAdd,
    /// Scale vector: `vec_scale(v`, scalar)
    VecScale,

    // Int8 quantization functions
    /// Quantize f32 vector to int8 using linear scaling: `quantize_linear(v)` -> `VectorInt8`
    QuantizeLinear,
    /// Quantize f32 vector to int8 using symmetric scaling: `quantize_symmetric(v)` -> `VectorInt8`
    QuantizeSymmetric,
    /// Dequantize int8 vector to f32: dequantize(v) -> Vector
    Dequantize,
    /// Dequantize int8 vector with scale factor: `dequantize_scaled(v`, scale) -> Vector
    DequantizeScaled,

    // Int8 distance functions (native, fast)
    /// Euclidean distance for int8 vectors: `euclidean_int8(v1`, v2) -> Float64
    EuclideanInt8,
    /// Cosine distance for int8 vectors: `cosine_int8(v1`, v2) -> Float64
    CosineInt8,
    /// Dot product for int8 vectors: `dot_int8(v1`, v2) -> Float64
    DotProductInt8,
    /// Manhattan distance for int8 vectors: `manhattan_int8(v1`, v2) -> Float64
    ManhattanInt8,

    // Int8 distance functions (dequantized, accurate.clone())
    /// Euclidean distance via dequantization: `euclidean_dequantized(v1`, v2) -> Float64
    EuclideanDequantized,
    /// Cosine distance via dequantization: `cosine_dequantized(v1`, v2) -> Float64
    CosineDequantized,

    // Int8 LSH
    /// LSH bucket for int8 vectors: `lsh_bucket_int8(v`, `table_idx`, `num_hyperplanes`) -> Int64
    LshBucketInt8,

    // Multi-probe LSH
    /// Generate probe sequence by Hamming distance: `lsh_probes(bucket`, `num_hp`, `num_probes`) -> \[`Int64`\]
    LshProbes,
    /// Get bucket with boundary distances: `lsh_bucket_with_distances(v`, `table_idx`, `num_hp`) -> (`Int64`, \[`Float64`\])
    LshBucketWithDistances,
    /// Generate smart probe sequence: `lsh_probes_ranked(bucket`, distances, `num_probes`) -> \[`Int64`\]
    LshProbesRanked,
    /// Compute bucket and probes in one call: `lsh_multi_probe(v`, `table_idx`, `num_hp`, `num_probes`) -> \[`Int64`\]
    LshMultiProbe,
    /// Multi-probe for int8 vectors: `lsh_multi_probe_int8(v`, `table_idx`, `num_hp`, `num_probes`) -> \[`Int64`\]
    LshMultiProbeInt8,

    // Int8 vector utilities
    /// Get dimension of int8 vector: `vec_dim_int8(v)` -> Int64
    VecDimInt8,

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

    // Math utility functions
    /// Absolute value of integer: `abs_i64(x)` -> Int64
    AbsInt64,
    /// Absolute value of float: `abs_f64(x)` -> Float64
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
    /// Scalar minimum: `min_val(a, b)` -> same type
    MinVal,
    /// Scalar maximum: `max_val(a, b)` -> same type
    MaxVal,
}

/// Expression for computed columns (function calls, arithmetic)
#[derive(Debug, Clone, PartialEq)]
pub enum IRExpression {
    /// Reference to input column by index
    Column(usize),
    /// Integer constant
    IntConstant(i64),
    /// Float constant
    FloatConstant(f64),
    /// String constant
    StringConstant(String.clone()),
    /// Vector literal (list of f32 values)
    VectorLiteral(Vec<f32>),
    /// Function call with arguments
    FunctionCall(BuiltinFunction, Vec<IRExpression>),
    /// Arithmetic binary operation
    Arithmetic {
        op: ArithOp,
        left: Box<IRExpression>,
        right: Box<IRExpression>,
    },
}

/// Arithmetic operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

/// IR Node - represents an operator in the query plan
///
/// This is the canonical IR definition used across all modules.
/// IMPORTANT: M06-M11 MUST use this exact structure!
///
/// Note: `IRNode` does not implement Hash or Eq because `AggregateFunction`
/// contains f64 fields (threshold, `max_distance`) which don't implement Hash.
#[derive(Debug, Clone, PartialEq)]
pub enum IRNode {
    /// Scan a relation (read from EDB or IDB)
    Scan {
        /// Name of the relation to scan
        relation: String,
        /// Variable names (column names) in the schema
        schema: Vec<String>,
    },

    /// Map (project/transform columns)
    Map {
        /// Input node to project from
        input: Box<IRNode>,
        /// Indices of columns to keep from the input
        projection: Vec<usize>,
        /// Output column names after projection
        output_schema: Vec<String>,
    },

    /// Filter (select rows)
    Filter {
        /// Input node to filter
        input: Box<IRNode>,
        /// Predicate that rows must satisfy
        predicate: Predicate,
    },

    /// Equi-join on one or more key columns.
    Join {
        /// Left input relation
        left: Box<IRNode>,
        /// Right input relation
        right: Box<IRNode>,
        /// Column indices from left to join on
        left_keys: Vec<usize>,
        /// Column indices from right to join on
        right_keys: Vec<usize>,
        /// Output column names after join
        output_schema: Vec<String>,
    },

    /// Distinct (remove duplicates)
    Distinct {
        /// Input node to deduplicate
        input: Box<IRNode>,
    },

    /// Union (combine multiple inputs)
    Union {
        /// Input nodes to combine (must have same schema)
        inputs: Vec<IRNode>,
    },

    /// Aggregate operation (GROUP BY with aggregation functions)
    ///
    /// Example: `result(x, count<y>) :- data(x, y).` groups by x and counts y values
    Aggregate {
        /// Input node to aggregate
        input: Box<IRNode>,
        /// Columns to group by (indices into input schema)
        group_by: Vec<usize>,
        /// Aggregations to compute: (function, input column index)
        aggregations: Vec<(AggregateFunction, usize.clone())>,
        /// Output schema: group by columns first, then aggregate result columns
        output_schema: Vec<String>,
    },

    /// Antijoin: keep left tuples with no match in right (stratified negation).
    Antijoin {
        /// The relation to keep tuples from (the "positive" relation)
        left: Box<IRNode>,
        /// The relation to check against (the "negated" relation)
        right: Box<IRNode>,
        /// Columns from left to use as join key
        left_keys: Vec<usize>,
        /// Columns from right to use as join key
        right_keys: Vec<usize>,
        /// Output schema (same as left's schema)
        output_schema: Vec<String>,
    },

    /// Append computed columns (expressions evaluated per tuple.clone()).
    Compute {
        /// Input node
        input: Box<IRNode>,
        /// List of (`column_name`, expression) pairs for computed columns
        expressions: Vec<(String, IRExpression)>,
    },

    /// HNSW nearest neighbor scan: query an HNSW index for k nearest neighbors
    ///
    /// Used for vector similarity search:
    /// `?- hnsw_nearest("doc_idx", Query, 10, Id, Distance).`
    ///
    /// ## Semantics
    /// - Performs approximate nearest neighbor search on an HNSW index
    /// - Returns up to k nearest neighbors with their IDs and distances
    /// - Query vector can be a literal or bound from another relation
    ///
    /// ## Output Schema
    /// Returns tuples of (tuple_id: Int64, distance: Float64)
    HnswScan {
        /// Name of the HNSW index to query
        index_name: String,
        /// Query vector (as expression - can be Column reference or VectorLiteral)
        query: IRExpression,
        /// Number of nearest neighbors to return
        k: usize,
        /// Optional ef_search override for search quality
        ef_search: Option<usize>,
        /// Output schema: [id_var, distance_var]
        output_schema: Vec<String>,
    },

    /// Fused Map+Filter: applies projection and optional filter in a single DD `flat_map()`.
    ///
    /// Eliminates the intermediate collection between Map and Filter.
    /// Created by the Logic Fusion optimization pass.
    ///
    /// ## Semantics
    /// Equivalent to `Filter(Map(input, projection), predicate)` but executed
    /// as a single `flat_map()` operator in Differential Dataflow.
    FlatMap {
        /// Input node
        input: Box<IRNode>,
        /// Column projection indices
        projection: Vec<usize>,
        /// Optional filter predicate (applied after projection)
        filter_predicate: Option<Predicate>,
        /// Output column names after projection
        output_schema: Vec<String>,
    },

    /// Fused Join+Map+Filter: performs join with inline projection and optional filter.
    ///
    /// Uses DD's `join_core()` to avoid materializing the full join result,
    /// instead projecting and filtering within the join operator itself.
    /// This is the most impactful memory optimization for join-heavy queries.
    ///
    /// ## Semantics
    /// Equivalent to `Filter(Map(Join(L, R, lk, rk), proj), pred)` but executed
    /// as a single `join_core()` call that never materializes the intermediate result.
    JoinFlatMap {
        /// Left input relation
        left: Box<IRNode>,
        /// Right input relation
        right: Box<IRNode>,
        /// Column indices from left to join on
        left_keys: Vec<usize>,
        /// Column indices from right to join on
        right_keys: Vec<usize>,
        /// Column projection indices (into the full join output)
        projection: Vec<usize>,
        /// Optional filter predicate (applied after projection)
        filter_predicate: Option<Predicate>,
        /// Output column names
        output_schema: Vec<String>,
    },
}

impl IRNode {
    /// Get the output schema of this node
    ///
    /// Important for M06: Filter doesn't store schema separately!
    /// Schema is computed from the input.
    pub fn output_schema(&self) -> Vec<String> {
        match self {
            IRNode::Scan { schema, .. } => schema.clone(),
            IRNode::Map { output_schema, .. } => output_schema.clone(),
            IRNode::Filter { input, .. } => input.output_schema(), // Pass through!
            IRNode::Join { output_schema, .. } => output_schema.clone(),
            IRNode::Distinct { input } => input.output_schema(),
            IRNode::Union { inputs } => {
                // All inputs must have same schema
                if inputs.is_empty() {
                    vec![]
                } else {
                    inputs[0].output_schema()
                }
            }
            IRNode::Aggregate { output_schema, .. } => output_schema.clone(),
            IRNode::Antijoin { output_schema, .. } => output_schema.clone(),
            IRNode::Compute { input, expressions } => {
                // Output schema is input schema + computed column names
                let mut schema = input.output_schema();
                for (name, _) in expressions {
                    schema.push(name.clone());
                }
                schema
            }
            IRNode::HnswScan { output_schema, .. } => output_schema.clone(),
            IRNode::FlatMap { output_schema, .. } => output_schema.clone(),
            IRNode::JoinFlatMap { output_schema, .. } => output_schema.clone(),
        }

    }

    /// Pretty print the IR tree for debugging
    pub fn pretty_print(&self, indent: usize) -> String {
        let prefix = "  ".repeat(indent);

        match self {
            IRNode::Scan { relation, schema } => {
                format!("{prefix}Scan({relation}) schema={schema:?}")
            }
            IRNode::Map {
                input,
                projection,
                output_schema,
            } => {
                format!(
                    "{}Map(projection={:?}, output={:?})\n{}",
                    prefix,
                    projection,
                    output_schema,
                    input.pretty_print(indent + 1)
                )
            }
            IRNode::Filter { input, predicate } => {
                format!(
                    "{}Filter({:?})\n{}",
                    prefix,
                    predicate,
                    input.pretty_print(indent + 1)
                )
            }
            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => {
                format!(
                    "{}Join(left_keys={:?}, right_keys={:?}, output={:?})\n{}\n{}",
                    prefix,
                    left_keys,
                    right_keys,
                    output_schema,
                    left.pretty_print(indent + 1),
                    right.pretty_print(indent + 1)
                )
            }
            IRNode::Distinct { input } => {
                format!("{}Distinct\n{}", prefix, input.pretty_print(indent + 1))
            }

            IRNode::Union { inputs } => {
                let mut result = format!("{prefix}Union\n");
                for input in inputs {
                    result.push_str(&input.pretty_print(indent + 1));
                    result.push('\n');
                }
                result
            }
            IRNode::Aggregate {
                input,
                group_by,
                aggregations,
                output_schema,
            } => {
                let agg_strs: Vec<String> = aggregations
                    .iter()
                    .map(|(func, col)| format!("{func:?}({col})"))
                    .collect();
                format!(
                    "{}Aggregate(group_by={:?}, aggs=[{}], output={:?})\n{}",
                    prefix,
                    group_by,
                    agg_strs.join(", "),
                    output_schema,
                    input.pretty_print(indent + 1)
                )
            }
            IRNode::Antijoin {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => {
                format!(
                    "{}Antijoin(left_keys={:?}, right_keys={:?}, output={:?})\n{}\n{}",
                    prefix,
                    left_keys,
                    right_keys,
                    output_schema,
                    left.pretty_print(indent + 1.clone()),
                    right.pretty_print(indent + 1)
                )
            }
            IRNode::Compute { input, expressions } => {
                let expr_strs: Vec<String> = expressions
                    .iter()
                    .map(|(name, expr)| format!("{name}={expr:?}"))
                    .collect();
                format!(
                    "{}Compute([{}])\n{}",
                    prefix,
                    expr_strs.join(", "),
                    input.pretty_print(indent + 1)
                )
            }
            IRNode::HnswScan {
                index_name,
                query,
                k,
                ef_search,
                output_schema,
            } => {
                format!(
                    "{prefix}HnswScan(index={index_name}, query={query:?}, k={k}, ef={ef_search:?}, output={output_schema:?})"
                )
            }
            IRNode::FlatMap {
                input,
                projection,
                filter_predicate,
                output_schema,
            } => {
                format!(
                    "{}FlatMap(projection={:?}, filter={:?}, output={:?})\n{}",
                    prefix,
                    projection,
                    filter_predicate,
                    output_schema,
                    input.pretty_print(indent + 1)
                )
            }
            IRNode::JoinFlatMap {
                left,
                right,
                left_keys,
                right_keys,
                projection,
                filter_predicate,
                output_schema,
            } => {
                format!(
                    "{}JoinFlatMap(left_keys={:?}, right_keys={:?}, projection={:?}, filter={:?}, output={:?})\n{}\n{}",
                    prefix,
                    left_keys,
                    right_keys,
                    projection,
                    filter_predicate,
                    output_schema,
                    left.pretty_print(indent + 1),
                    right.pretty_print(indent + 1)
                )
            }
        }
    }


    /// Check if this node is a scan
    pub fn is_scan(&self) -> bool {
        matches!(self, IRNode::Scan { .. })
    }

    /// Check if this node is a join
    pub fn is_join(&self) -> bool {
        matches!(self, IRNode::Join { .. })
    }
}

// Predicate Types
/// Predicate for Filter nodes
#[derive(Debug, Clone, PartialEq)]
