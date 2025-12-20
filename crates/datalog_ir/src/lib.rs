//! # Datalog IR - Shared Library
//!
//! Intermediate Representation types for Datalog query plans.
//! Used across optimizer modules (M05, M06-M10, M11) for consistency.

use std::collections::HashSet;

// ============================================================================
// IR Node Types
// ============================================================================

/// Aggregate function types
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    /// Count distinct values
    Count,
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
    /// Within radius: all results where order_col <= max_distance
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
    /// LSH bucket hash: lsh_bucket(v, table_idx, num_hyperplanes)
    LshBucket,
    /// Normalize vector: normalize(v)
    VecNormalize,
    /// Get vector dimension: vec_dim(v)
    VecDim,
    /// Add vectors: vec_add(v1, v2)
    VecAdd,
    /// Scale vector: vec_scale(v, scalar)
    VecScale,

    // Int8 quantization functions
    /// Quantize f32 vector to int8 using linear scaling: quantize_linear(v) -> VectorInt8
    QuantizeLinear,
    /// Quantize f32 vector to int8 using symmetric scaling: quantize_symmetric(v) -> VectorInt8
    QuantizeSymmetric,
    /// Dequantize int8 vector to f32: dequantize(v) -> Vector
    Dequantize,
    /// Dequantize int8 vector with scale factor: dequantize_scaled(v, scale) -> Vector
    DequantizeScaled,

    // Int8 distance functions (native, fast)
    /// Euclidean distance for int8 vectors: euclidean_int8(v1, v2) -> Float64
    EuclideanInt8,
    /// Cosine distance for int8 vectors: cosine_int8(v1, v2) -> Float64
    CosineInt8,
    /// Dot product for int8 vectors: dot_int8(v1, v2) -> Float64
    DotProductInt8,
    /// Manhattan distance for int8 vectors: manhattan_int8(v1, v2) -> Float64
    ManhattanInt8,

    // Int8 distance functions (dequantized, accurate)
    /// Euclidean distance via dequantization: euclidean_dequantized(v1, v2) -> Float64
    EuclideanDequantized,
    /// Cosine distance via dequantization: cosine_dequantized(v1, v2) -> Float64
    CosineDequantized,

    // Int8 LSH
    /// LSH bucket for int8 vectors: lsh_bucket_int8(v, table_idx, num_hyperplanes) -> Int64
    LshBucketInt8,

    // Multi-probe LSH
    /// Generate probe sequence by Hamming distance: lsh_probes(bucket, num_hp, num_probes) -> [Int64]
    LshProbes,
    /// Get bucket with boundary distances: lsh_bucket_with_distances(v, table_idx, num_hp) -> (Int64, [Float64])
    LshBucketWithDistances,
    /// Generate smart probe sequence: lsh_probes_ranked(bucket, distances, num_probes) -> [Int64]
    LshProbesRanked,
    /// Compute bucket and probes in one call: lsh_multi_probe(v, table_idx, num_hp, num_probes) -> [Int64]
    LshMultiProbe,
    /// Multi-probe for int8 vectors: lsh_multi_probe_int8(v, table_idx, num_hp, num_probes) -> [Int64]
    LshMultiProbeInt8,

    // Int8 vector utilities
    /// Get dimension of int8 vector: vec_dim_int8(v) -> Int64
    VecDimInt8,

    // Temporal functions
    /// Get current time: time_now() -> Timestamp (Unix milliseconds)
    TimeNow,
    /// Time difference: time_diff(t1, t2) -> Int64 (milliseconds)
    TimeDiff,
    /// Add duration to timestamp: time_add(ts, duration_ms) -> Timestamp
    TimeAdd,
    /// Subtract duration from timestamp: time_sub(ts, duration_ms) -> Timestamp
    TimeSub,
    /// Exponential time decay: time_decay(ts, now, half_life_ms) -> Float64 [0,1]
    TimeDecay,
    /// Linear time decay: time_decay_linear(ts, now, max_age_ms) -> Float64 [0,1]
    TimeDecayLinear,
    /// Check if t1 < t2: time_before(t1, t2) -> Bool
    TimeBefore,
    /// Check if t1 > t2: time_after(t1, t2) -> Bool
    TimeAfter,
    /// Check if ts in [start, end]: time_between(ts, start, end) -> Bool
    TimeBetween,
    /// Check if ts is within duration of now: within_last(ts, now, duration_ms) -> Bool
    WithinLast,
    /// Check if intervals overlap: intervals_overlap(s1, e1, s2, e2) -> Bool
    IntervalsOverlap,
    /// Check if interval 1 contains interval 2: interval_contains(s1, e1, s2, e2) -> Bool
    IntervalContains,
    /// Get interval duration: interval_duration(start, end) -> Int64
    IntervalDuration,
    /// Check if point is in interval: point_in_interval(ts, start, end) -> Bool
    PointInInterval,

    // Math utility functions
    /// Absolute value of integer: abs_i64(x) -> Int64
    AbsInt64,
    /// Absolute value of float: abs_f64(x) -> Float64
    AbsFloat64,
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
/// **IMPORTANT**: M06-M11 MUST use this exact structure!
///
/// Note: IRNode does not implement Hash or Eq because AggregateFunction
/// contains f64 fields (threshold, max_distance) which don't implement Hash.
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

    /// Join two inputs on shared keys
    ///
    /// **Note**: Uses `Vec<usize>` for multi-column equi-joins
    /// M06 needs to adapt from single usize to `vec![usize]`
    Join {
        /// Left input relation
        left: Box<IRNode>,
        /// Right input relation
        right: Box<IRNode>,
        /// Column indices from left to join on (can be multiple)
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
        aggregations: Vec<(AggregateFunction, usize)>,
        /// Output schema: group by columns first, then aggregate result columns
        output_schema: Vec<String>,
    },

    /// Antijoin (negation): Left - (Left ⋈ Right)
    ///
    /// Returns tuples from left that do NOT have a match in right.
    /// Used for stratified negation in Datalog:
    /// `unreachable(x) :- node(x), !reach(x).`
    ///
    /// ## Semantics
    /// - For each tuple in left, check if there exists a matching tuple in right
    /// - If no match exists, include the left tuple in the output
    /// - Output schema is the same as left's schema
    ///
    /// ## Example
    /// ```text
    /// left:  [(1, "a"), (2, "b"), (3, "c")]
    /// right: [(1, _), (3, _)]  // right tuples with key 1 and 3
    /// result: [(2, "b")]       // only (2, "b") has no match
    /// ```
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

    /// Compute node: adds computed columns to input tuples
    ///
    /// Used for evaluating expressions like distance functions:
    /// `result(x, v1, v2, dist) :- data(x, v1, v2), dist = euclidean(v1, v2).`
    ///
    /// ## Semantics
    /// - Input tuples pass through unchanged
    /// - New columns are appended with computed values from expressions
    /// - Output schema is input schema + names of computed columns
    ///
    /// ## Example
    /// ```text
    /// input: [(1, [0.1, 0.2], [0.3, 0.4])]  // id, v1, v2
    /// expressions: [("dist", euclidean(col(1), col(2)))]
    /// result: [(1, [0.1, 0.2], [0.3, 0.4], 0.283)]  // id, v1, v2, dist
    /// ```
    Compute {
        /// Input node
        input: Box<IRNode>,
        /// List of (column_name, expression) pairs for computed columns
        expressions: Vec<(String, IRExpression)>,
    },
}

impl IRNode {
    /// Get the output schema of this node
    ///
    /// **Important for M06**: Filter doesn't store schema separately!
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
        }
    }

    /// Pretty print the IR tree for debugging
    pub fn pretty_print(&self, indent: usize) -> String {
        let prefix = "  ".repeat(indent);

        match self {
            IRNode::Scan { relation, schema } => {
                format!("{}Scan({}) schema={:?}", prefix, relation, schema)
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
                let mut result = format!("{}Union\n", prefix);
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
                    .map(|(func, col)| format!("{:?}({})", func, col))
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
                    left.pretty_print(indent + 1),
                    right.pretty_print(indent + 1)
                )
            }
            IRNode::Compute { input, expressions } => {
                let expr_strs: Vec<String> = expressions
                    .iter()
                    .map(|(name, expr)| format!("{}={:?}", name, expr))
                    .collect();
                format!(
                    "{}Compute([{}])\n{}",
                    prefix,
                    expr_strs.join(", "),
                    input.pretty_print(indent + 1)
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

// ============================================================================
// Predicate Types
// ============================================================================

/// Predicate for Filter nodes
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Column equals constant (integer)
    ColumnEqConst(usize, i64),
    /// Column not equals constant (integer)
    ColumnNeConst(usize, i64),
    /// Column greater than constant (integer)
    ColumnGtConst(usize, i64),
    /// Column less than constant (integer)
    ColumnLtConst(usize, i64),
    /// Column greater or equal to constant (integer)
    ColumnGeConst(usize, i64),
    /// Column less or equal to constant (integer)
    ColumnLeConst(usize, i64),
    /// Column equals string constant
    ColumnEqStr(usize, String),
    /// Column not equals string constant
    ColumnNeStr(usize, String),
    /// Column equals float constant
    ColumnEqFloat(usize, f64),
    /// Column not equals float constant
    ColumnNeFloat(usize, f64),
    /// Column greater than float constant
    ColumnGtFloat(usize, f64),
    /// Column less than float constant
    ColumnLtFloat(usize, f64),
    /// Column greater or equal to float constant
    ColumnGeFloat(usize, f64),
    /// Column less or equal to float constant
    ColumnLeFloat(usize, f64),
    /// Two columns are equal
    ColumnsEq(usize, usize),
    /// Two columns are not equal
    ColumnsNe(usize, usize),
    /// Logical AND
    And(Box<Predicate>, Box<Predicate>),
    /// Logical OR
    Or(Box<Predicate>, Box<Predicate>),
    /// Always true (for optimization)
    True,
    /// Always false (for optimization)
    False,
}

impl Predicate {
    /// Get all columns referenced by this predicate
    pub fn referenced_columns(&self) -> HashSet<usize> {
        let mut cols = HashSet::new();
        self.collect_columns(&mut cols);
        cols
    }

    fn collect_columns(&self, cols: &mut HashSet<usize>) {
        match self {
            Predicate::ColumnEqConst(col, _)
            | Predicate::ColumnNeConst(col, _)
            | Predicate::ColumnGtConst(col, _)
            | Predicate::ColumnLtConst(col, _)
            | Predicate::ColumnGeConst(col, _)
            | Predicate::ColumnLeConst(col, _)
            | Predicate::ColumnEqStr(col, _)
            | Predicate::ColumnNeStr(col, _)
            | Predicate::ColumnEqFloat(col, _)
            | Predicate::ColumnNeFloat(col, _)
            | Predicate::ColumnGtFloat(col, _)
            | Predicate::ColumnLtFloat(col, _)
            | Predicate::ColumnGeFloat(col, _)
            | Predicate::ColumnLeFloat(col, _) => {
                cols.insert(*col);
            }
            Predicate::ColumnsEq(left, right) | Predicate::ColumnsNe(left, right) => {
                cols.insert(*left);
                cols.insert(*right);
            }
            Predicate::And(p1, p2) | Predicate::Or(p1, p2) => {
                p1.collect_columns(cols);
                p2.collect_columns(cols);
            }
            Predicate::True | Predicate::False => {}
        }
    }

    /// Check if predicate is always true
    pub fn is_always_true(&self) -> bool {
        matches!(self, Predicate::True)
    }

    /// Check if predicate is always false
    pub fn is_always_false(&self) -> bool {
        matches!(self, Predicate::False)
    }

    /// Simplify predicate (basic constant folding)
    pub fn simplify(self) -> Self {
        match self {
            Predicate::And(p1, p2) => {
                let p1 = p1.simplify();
                let p2 = p2.simplify();

                if p1.is_always_true() {
                    p2
                } else if p2.is_always_true() {
                    p1
                } else if p1.is_always_false() || p2.is_always_false() {
                    Predicate::False
                } else {
                    Predicate::And(Box::new(p1), Box::new(p2))
                }
            }
            Predicate::Or(p1, p2) => {
                let p1 = p1.simplify();
                let p2 = p2.simplify();

                if p1.is_always_true() || p2.is_always_true() {
                    Predicate::True
                } else if p1.is_always_false() {
                    p2
                } else if p2.is_always_false() {
                    p1
                } else {
                    Predicate::Or(Box::new(p1), Box::new(p2))
                }
            }
            other => other,
        }
    }

    /// Adjust column indices after projection
    /// Returns None if predicate references columns not in projection
    ///
    /// **For M06 filter pushdown**: Use this when pushing filters through maps
    pub fn adjust_for_projection(&self, projection: &[usize]) -> Option<Self> {
        // Helper: find new index of old column
        let find_new_index =
            |old_idx: usize| -> Option<usize> { projection.iter().position(|&idx| idx == old_idx) };

        match self {
            Predicate::ColumnEqConst(col, val) => {
                find_new_index(*col).map(|new_col| Predicate::ColumnEqConst(new_col, *val))
            }
            Predicate::ColumnNeConst(col, val) => {
                find_new_index(*col).map(|new_col| Predicate::ColumnNeConst(new_col, *val))
            }
            Predicate::ColumnGtConst(col, val) => {
                find_new_index(*col).map(|new_col| Predicate::ColumnGtConst(new_col, *val))
            }
            Predicate::ColumnLtConst(col, val) => {
                find_new_index(*col).map(|new_col| Predicate::ColumnLtConst(new_col, *val))
            }
            Predicate::ColumnGeConst(col, val) => {
                find_new_index(*col).map(|new_col| Predicate::ColumnGeConst(new_col, *val))
            }
            Predicate::ColumnLeConst(col, val) => {
                find_new_index(*col).map(|new_col| Predicate::ColumnLeConst(new_col, *val))
            }
            // String predicates
            Predicate::ColumnEqStr(col, val) => {
                find_new_index(*col).map(|new_col| Predicate::ColumnEqStr(new_col, val.clone()))
            }
            Predicate::ColumnNeStr(col, val) => {
                find_new_index(*col).map(|new_col| Predicate::ColumnNeStr(new_col, val.clone()))
            }
            // Float predicates
            Predicate::ColumnEqFloat(col, val) => {
                find_new_index(*col).map(|new_col| Predicate::ColumnEqFloat(new_col, *val))
            }
            Predicate::ColumnNeFloat(col, val) => {
                find_new_index(*col).map(|new_col| Predicate::ColumnNeFloat(new_col, *val))
            }
            Predicate::ColumnGtFloat(col, val) => {
                find_new_index(*col).map(|new_col| Predicate::ColumnGtFloat(new_col, *val))
            }
            Predicate::ColumnLtFloat(col, val) => {
                find_new_index(*col).map(|new_col| Predicate::ColumnLtFloat(new_col, *val))
            }
            Predicate::ColumnGeFloat(col, val) => {
                find_new_index(*col).map(|new_col| Predicate::ColumnGeFloat(new_col, *val))
            }
            Predicate::ColumnLeFloat(col, val) => {
                find_new_index(*col).map(|new_col| Predicate::ColumnLeFloat(new_col, *val))
            }
            Predicate::ColumnsEq(left, right) => {
                match (find_new_index(*left), find_new_index(*right)) {
                    (Some(new_left), Some(new_right)) => {
                        Some(Predicate::ColumnsEq(new_left, new_right))
                    }
                    _ => None,
                }
            }
            Predicate::ColumnsNe(left, right) => {
                match (find_new_index(*left), find_new_index(*right)) {
                    (Some(new_left), Some(new_right)) => {
                        Some(Predicate::ColumnsNe(new_left, new_right))
                    }
                    _ => None,
                }
            }
            Predicate::And(p1, p2) => {
                match (
                    p1.adjust_for_projection(projection),
                    p2.adjust_for_projection(projection),
                ) {
                    (Some(new_p1), Some(new_p2)) => {
                        Some(Predicate::And(Box::new(new_p1), Box::new(new_p2)))
                    }
                    (Some(new_p1), None) => Some(new_p1),
                    (None, Some(new_p2)) => Some(new_p2),
                    (None, None) => None,
                }
            }
            Predicate::Or(p1, p2) => {
                // For OR, we need BOTH predicates to be adjustable
                match (
                    p1.adjust_for_projection(projection),
                    p2.adjust_for_projection(projection),
                ) {
                    (Some(new_p1), Some(new_p2)) => {
                        Some(Predicate::Or(Box::new(new_p1), Box::new(new_p2)))
                    }
                    _ => None, // Can't push OR if either side doesn't have all columns
                }
            }
            Predicate::True => Some(Predicate::True),
            Predicate::False => Some(Predicate::False),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // AggregateFunction Tests
    // ========================================================================

    #[test]
    fn test_aggregate_function_clone_eq() {
        let funcs = vec![
            AggregateFunction::Count,
            AggregateFunction::Sum,
            AggregateFunction::Min,
            AggregateFunction::Max,
            AggregateFunction::Avg,
        ];

        for func in &funcs {
            let cloned = func.clone();
            assert_eq!(func, &cloned);
        }
    }

    #[test]
    fn test_aggregate_function_debug() {
        assert_eq!(format!("{:?}", AggregateFunction::Count), "Count");
        assert_eq!(format!("{:?}", AggregateFunction::Sum), "Sum");
        assert_eq!(format!("{:?}", AggregateFunction::Min), "Min");
        assert_eq!(format!("{:?}", AggregateFunction::Max), "Max");
        assert_eq!(format!("{:?}", AggregateFunction::Avg), "Avg");
    }

    #[test]
    fn test_aggregate_function_equality() {
        // AggregateFunction no longer implements Hash (due to f64 fields in TopKThreshold, WithinRadius)
        // Test equality via PartialEq instead
        let count1 = AggregateFunction::Count;
        let count2 = AggregateFunction::Count;
        let sum = AggregateFunction::Sum;

        assert_eq!(count1, count2);
        assert_ne!(count1, sum);

        // Test new variants
        let topk = AggregateFunction::TopK { k: 5, order_col: 1, descending: true };
        let topk2 = AggregateFunction::TopK { k: 5, order_col: 1, descending: true };
        let topk3 = AggregateFunction::TopK { k: 10, order_col: 1, descending: true };
        assert_eq!(topk, topk2);
        assert_ne!(topk, topk3);
    }

    // ========================================================================
    // IRNode::Scan Tests
    // ========================================================================

    #[test]
    fn test_scan_output_schema() {
        let scan = IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };
        assert_eq!(scan.output_schema(), vec!["x", "y"]);
        assert!(scan.is_scan());
        assert!(!scan.is_join());
    }

    #[test]
    fn test_scan_empty_schema() {
        let scan = IRNode::Scan {
            relation: "empty".to_string(),
            schema: vec![],
        };
        assert_eq!(scan.output_schema(), Vec::<String>::new());
    }

    #[test]
    fn test_scan_pretty_print() {
        let scan = IRNode::Scan {
            relation: "users".to_string(),
            schema: vec!["id".to_string(), "name".to_string()],
        };
        let output = scan.pretty_print(0);
        assert!(output.contains("Scan(users)"));
        assert!(output.contains("schema="));
    }

    // ========================================================================
    // IRNode::Filter Tests
    // ========================================================================

    #[test]
    fn test_filter_passes_through_schema() {
        let scan = IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };

        let filter = IRNode::Filter {
            input: Box::new(scan),
            predicate: Predicate::ColumnGtConst(0, 5),
        };

        assert_eq!(filter.output_schema(), vec!["x", "y"]);
        assert!(!filter.is_scan());
        assert!(!filter.is_join());
    }

    #[test]
    fn test_filter_pretty_print() {
        let scan = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["x".to_string()],
        };
        let filter = IRNode::Filter {
            input: Box::new(scan),
            predicate: Predicate::ColumnGtConst(0, 10),
        };
        let output = filter.pretty_print(0);
        assert!(output.contains("Filter"));
        assert!(output.contains("Scan"));
    }

    // ========================================================================
    // IRNode::Map Tests
    // ========================================================================

    #[test]
    fn test_map_reorders_schema() {
        let scan = IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };

        let map = IRNode::Map {
            input: Box::new(scan),
            projection: vec![1, 0],
            output_schema: vec!["y".to_string(), "x".to_string()],
        };

        assert_eq!(map.output_schema(), vec!["y", "x"]);
    }

    #[test]
    fn test_map_projection_subset() {
        let scan = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };

        let map = IRNode::Map {
            input: Box::new(scan),
            projection: vec![0, 2],
            output_schema: vec!["a".to_string(), "c".to_string()],
        };

        assert_eq!(map.output_schema(), vec!["a", "c"]);
    }

    #[test]
    fn test_map_pretty_print() {
        let scan = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };
        let map = IRNode::Map {
            input: Box::new(scan),
            projection: vec![1],
            output_schema: vec!["y".to_string()],
        };
        let output = map.pretty_print(0);
        assert!(output.contains("Map"));
        assert!(output.contains("projection="));
        assert!(output.contains("output="));
    }

    // ========================================================================
    // IRNode::Join Tests
    // ========================================================================

    #[test]
    fn test_join_output_schema() {
        let left = IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["a".to_string(), "b".to_string()],
        };

        let right = IRNode::Scan {
            relation: "node".to_string(),
            schema: vec!["id".to_string(), "label".to_string()],
        };

        let join = IRNode::Join {
            left: Box::new(left),
            right: Box::new(right),
            left_keys: vec![1],
            right_keys: vec![0],
            output_schema: vec![
                "a".to_string(),
                "b".to_string(),
                "id".to_string(),
                "label".to_string(),
            ],
        };

        assert_eq!(join.output_schema(), vec!["a", "b", "id", "label"]);
        assert!(join.is_join());
        assert!(!join.is_scan());
    }

    #[test]
    fn test_join_multi_key() {
        let left = IRNode::Scan {
            relation: "orders".to_string(),
            schema: vec![
                "order_id".to_string(),
                "customer_id".to_string(),
                "product_id".to_string(),
            ],
        };

        let right = IRNode::Scan {
            relation: "order_details".to_string(),
            schema: vec![
                "detail_order_id".to_string(),
                "detail_product_id".to_string(),
                "quantity".to_string(),
            ],
        };

        let join = IRNode::Join {
            left: Box::new(left),
            right: Box::new(right),
            left_keys: vec![0, 2],
            right_keys: vec![0, 1],
            output_schema: vec![
                "order_id".to_string(),
                "customer_id".to_string(),
                "product_id".to_string(),
                "detail_order_id".to_string(),
                "detail_product_id".to_string(),
                "quantity".to_string(),
            ],
        };

        assert_eq!(join.output_schema().len(), 6);
        assert!(join.is_join());
    }

    #[test]
    fn test_join_pretty_print() {
        let left = IRNode::Scan {
            relation: "a".to_string(),
            schema: vec!["x".to_string()],
        };
        let right = IRNode::Scan {
            relation: "b".to_string(),
            schema: vec!["y".to_string()],
        };
        let join = IRNode::Join {
            left: Box::new(left),
            right: Box::new(right),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["x".to_string(), "y".to_string()],
        };
        let output = join.pretty_print(0);
        assert!(output.contains("Join"));
        assert!(output.contains("left_keys="));
        assert!(output.contains("right_keys="));
    }

    // ========================================================================
    // IRNode::Distinct Tests
    // ========================================================================

    #[test]
    fn test_distinct_passes_through_schema() {
        let scan = IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };

        let distinct = IRNode::Distinct {
            input: Box::new(scan),
        };

        assert_eq!(distinct.output_schema(), vec!["x", "y"]);
    }

    #[test]
    fn test_distinct_pretty_print() {
        let scan = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["x".to_string()],
        };
        let distinct = IRNode::Distinct {
            input: Box::new(scan),
        };
        let output = distinct.pretty_print(0);
        assert!(output.contains("Distinct"));
        assert!(output.contains("Scan"));
    }

    // ========================================================================
    // IRNode::Union Tests
    // ========================================================================

    #[test]
    fn test_union_uses_first_input_schema() {
        let scan1 = IRNode::Scan {
            relation: "edge1".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };

        let scan2 = IRNode::Scan {
            relation: "edge2".to_string(),
            schema: vec!["a".to_string(), "b".to_string()],
        };

        let union = IRNode::Union {
            inputs: vec![scan1, scan2],
        };

        assert_eq!(union.output_schema(), vec!["x", "y"]);
    }

    #[test]
    fn test_empty_union_schema() {
        let union = IRNode::Union { inputs: vec![] };
        assert_eq!(union.output_schema(), Vec::<String>::new());
    }

    #[test]
    fn test_union_single_input() {
        let scan = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["x".to_string()],
        };
        let union = IRNode::Union { inputs: vec![scan] };
        assert_eq!(union.output_schema(), vec!["x"]);
    }

    #[test]
    fn test_union_pretty_print() {
        let scan1 = IRNode::Scan {
            relation: "a".to_string(),
            schema: vec!["x".to_string()],
        };
        let scan2 = IRNode::Scan {
            relation: "b".to_string(),
            schema: vec!["x".to_string()],
        };
        let union = IRNode::Union {
            inputs: vec![scan1, scan2],
        };
        let output = union.pretty_print(0);
        assert!(output.contains("Union"));
    }

    // ========================================================================
    // IRNode::Aggregate Tests
    // ========================================================================

    #[test]
    fn test_aggregate_output_schema() {
        let scan = IRNode::Scan {
            relation: "sales".to_string(),
            schema: vec![
                "product".to_string(),
                "region".to_string(),
                "amount".to_string(),
            ],
        };

        let aggregate = IRNode::Aggregate {
            input: Box::new(scan),
            group_by: vec![0, 1],
            aggregations: vec![(AggregateFunction::Sum, 2), (AggregateFunction::Count, 2)],
            output_schema: vec![
                "product".to_string(),
                "region".to_string(),
                "total_amount".to_string(),
                "count".to_string(),
            ],
        };

        assert_eq!(
            aggregate.output_schema(),
            vec!["product", "region", "total_amount", "count"]
        );
    }

    #[test]
    fn test_aggregate_no_group_by() {
        let scan = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["value".to_string()],
        };

        let aggregate = IRNode::Aggregate {
            input: Box::new(scan),
            group_by: vec![],
            aggregations: vec![
                (AggregateFunction::Sum, 0),
                (AggregateFunction::Avg, 0),
                (AggregateFunction::Min, 0),
                (AggregateFunction::Max, 0),
            ],
            output_schema: vec![
                "sum".to_string(),
                "avg".to_string(),
                "min".to_string(),
                "max".to_string(),
            ],
        };

        assert_eq!(aggregate.output_schema().len(), 4);
    }

    #[test]
    fn test_aggregate_single_aggregation() {
        let scan = IRNode::Scan {
            relation: "users".to_string(),
            schema: vec!["id".to_string(), "name".to_string()],
        };

        let aggregate = IRNode::Aggregate {
            input: Box::new(scan),
            group_by: vec![],
            aggregations: vec![(AggregateFunction::Count, 0)],
            output_schema: vec!["user_count".to_string()],
        };

        assert_eq!(aggregate.output_schema(), vec!["user_count"]);
    }

    #[test]
    fn test_aggregate_pretty_print() {
        let scan = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };
        let aggregate = IRNode::Aggregate {
            input: Box::new(scan),
            group_by: vec![0],
            aggregations: vec![(AggregateFunction::Sum, 1)],
            output_schema: vec!["x".to_string(), "sum_y".to_string()],
        };
        let output = aggregate.pretty_print(0);
        assert!(output.contains("Aggregate"));
        assert!(output.contains("group_by="));
        assert!(output.contains("Sum(1)"));
    }

    #[test]
    fn test_aggregate_multiple_functions_pretty_print() {
        let scan = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };
        let aggregate = IRNode::Aggregate {
            input: Box::new(scan),
            group_by: vec![0],
            aggregations: vec![
                (AggregateFunction::Count, 1),
                (AggregateFunction::Max, 1),
                (AggregateFunction::Min, 1),
            ],
            output_schema: vec![
                "x".to_string(),
                "cnt".to_string(),
                "max".to_string(),
                "min".to_string(),
            ],
        };
        let output = aggregate.pretty_print(0);
        assert!(output.contains("Count(1)"));
        assert!(output.contains("Max(1)"));
        assert!(output.contains("Min(1)"));
    }

    // ========================================================================
    // IRNode::Antijoin Tests
    // ========================================================================

    #[test]
    fn test_antijoin_output_schema() {
        let nodes = IRNode::Scan {
            relation: "node".to_string(),
            schema: vec!["id".to_string(), "label".to_string()],
        };

        let reachable = IRNode::Scan {
            relation: "reachable".to_string(),
            schema: vec!["node_id".to_string()],
        };

        let antijoin = IRNode::Antijoin {
            left: Box::new(nodes),
            right: Box::new(reachable),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["id".to_string(), "label".to_string()],
        };

        assert_eq!(antijoin.output_schema(), vec!["id", "label"]);
    }

    #[test]
    fn test_antijoin_multi_key() {
        let left = IRNode::Scan {
            relation: "all_pairs".to_string(),
            schema: vec!["x".to_string(), "y".to_string(), "data".to_string()],
        };

        let right = IRNode::Scan {
            relation: "excluded_pairs".to_string(),
            schema: vec!["a".to_string(), "b".to_string()],
        };

        let antijoin = IRNode::Antijoin {
            left: Box::new(left),
            right: Box::new(right),
            left_keys: vec![0, 1],
            right_keys: vec![0, 1],
            output_schema: vec!["x".to_string(), "y".to_string(), "data".to_string()],
        };

        assert_eq!(antijoin.output_schema(), vec!["x", "y", "data"]);
    }

    #[test]
    fn test_antijoin_pretty_print() {
        let left = IRNode::Scan {
            relation: "all_nodes".to_string(),
            schema: vec!["id".to_string()],
        };
        let right = IRNode::Scan {
            relation: "visited".to_string(),
            schema: vec!["node_id".to_string()],
        };
        let antijoin = IRNode::Antijoin {
            left: Box::new(left),
            right: Box::new(right),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["id".to_string()],
        };
        let output = antijoin.pretty_print(0);
        assert!(output.contains("Antijoin"));
        assert!(output.contains("left_keys="));
        assert!(output.contains("right_keys="));
        assert!(output.contains("all_nodes"));
        assert!(output.contains("visited"));
    }

    #[test]
    fn test_antijoin_is_not_join() {
        let left = IRNode::Scan {
            relation: "a".to_string(),
            schema: vec!["x".to_string()],
        };
        let right = IRNode::Scan {
            relation: "b".to_string(),
            schema: vec!["y".to_string()],
        };
        let antijoin = IRNode::Antijoin {
            left: Box::new(left),
            right: Box::new(right),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["x".to_string()],
        };
        assert!(!antijoin.is_join());
        assert!(!antijoin.is_scan());
    }

    // ========================================================================
    // IRNode Clone and Eq Tests
    // ========================================================================

    #[test]
    fn test_irnode_clone_and_eq() {
        let scan = IRNode::Scan {
            relation: "test".to_string(),
            schema: vec!["a".to_string()],
        };

        let scan_clone = scan.clone();
        assert_eq!(scan, scan_clone);
    }

    #[test]
    fn test_irnode_equality() {
        // IRNode no longer implements Hash (due to AggregateFunction having f64 fields)
        // Test equality via PartialEq instead
        let scan1 = IRNode::Scan {
            relation: "test".to_string(),
            schema: vec!["a".to_string()],
        };
        let scan2 = IRNode::Scan {
            relation: "test".to_string(),
            schema: vec!["a".to_string()],
        };
        let scan3 = IRNode::Scan {
            relation: "other".to_string(),
            schema: vec!["a".to_string()],
        };

        assert_eq!(scan1, scan2);
        assert_ne!(scan1, scan3);
    }

    // ========================================================================
    // Nested IRNode Tests
    // ========================================================================

    #[test]
    fn test_nested_operations() {
        let scan = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["x".to_string(), "y".to_string(), "z".to_string()],
        };

        let filter = IRNode::Filter {
            input: Box::new(scan),
            predicate: Predicate::ColumnGtConst(0, 10),
        };

        let map = IRNode::Map {
            input: Box::new(filter),
            projection: vec![0, 2],
            output_schema: vec!["x".to_string(), "z".to_string()],
        };

        let distinct = IRNode::Distinct {
            input: Box::new(map),
        };

        assert_eq!(distinct.output_schema(), vec!["x", "z"]);
    }

    #[test]
    fn test_complex_query_plan() {
        // SELECT DISTINCT x, SUM(y) FROM data WHERE z > 5 GROUP BY x
        let scan = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["x".to_string(), "y".to_string(), "z".to_string()],
        };

        let filter = IRNode::Filter {
            input: Box::new(scan),
            predicate: Predicate::ColumnGtConst(2, 5),
        };

        let aggregate = IRNode::Aggregate {
            input: Box::new(filter),
            group_by: vec![0],
            aggregations: vec![(AggregateFunction::Sum, 1)],
            output_schema: vec!["x".to_string(), "sum_y".to_string()],
        };

        let distinct = IRNode::Distinct {
            input: Box::new(aggregate),
        };

        assert_eq!(distinct.output_schema(), vec!["x", "sum_y"]);
    }

    // ========================================================================
    // Predicate Tests
    // ========================================================================

    #[test]
    fn test_predicate_referenced_columns_simple() {
        let pred = Predicate::ColumnEqConst(2, 42);
        let cols = pred.referenced_columns();
        assert_eq!(cols.len(), 1);
        assert!(cols.contains(&2));
    }

    #[test]
    fn test_predicate_referenced_columns_compound() {
        let pred = Predicate::And(
            Box::new(Predicate::ColumnGtConst(0, 5)),
            Box::new(Predicate::ColumnsEq(1, 2)),
        );

        let cols = pred.referenced_columns();
        assert_eq!(cols.len(), 3);
        assert!(cols.contains(&0));
        assert!(cols.contains(&1));
        assert!(cols.contains(&2));
    }

    #[test]
    fn test_predicate_referenced_columns_or() {
        let pred = Predicate::Or(
            Box::new(Predicate::ColumnLtConst(3, 10)),
            Box::new(Predicate::ColumnsNe(4, 5)),
        );

        let cols = pred.referenced_columns();
        assert_eq!(cols.len(), 3);
        assert!(cols.contains(&3));
        assert!(cols.contains(&4));
        assert!(cols.contains(&5));
    }

    #[test]
    fn test_predicate_referenced_columns_true_false() {
        assert!(Predicate::True.referenced_columns().is_empty());
        assert!(Predicate::False.referenced_columns().is_empty());
    }

    #[test]
    fn test_predicate_referenced_columns_all_types() {
        let predicates = vec![
            Predicate::ColumnEqConst(0, 1),
            Predicate::ColumnNeConst(1, 2),
            Predicate::ColumnGtConst(2, 3),
            Predicate::ColumnLtConst(3, 4),
            Predicate::ColumnGeConst(4, 5),
            Predicate::ColumnLeConst(5, 6),
        ];

        for (i, pred) in predicates.iter().enumerate() {
            let cols = pred.referenced_columns();
            assert_eq!(cols.len(), 1);
            assert!(cols.contains(&i));
        }
    }

    #[test]
    fn test_predicate_is_always_true_false() {
        assert!(Predicate::True.is_always_true());
        assert!(!Predicate::True.is_always_false());
        assert!(Predicate::False.is_always_false());
        assert!(!Predicate::False.is_always_true());

        let pred = Predicate::ColumnEqConst(0, 1);
        assert!(!pred.is_always_true());
        assert!(!pred.is_always_false());
    }

    #[test]
    fn test_predicate_simplify_and_true() {
        let pred = Predicate::And(
            Box::new(Predicate::True),
            Box::new(Predicate::ColumnGtConst(0, 5)),
        );
        assert_eq!(pred.simplify(), Predicate::ColumnGtConst(0, 5));

        let pred2 = Predicate::And(
            Box::new(Predicate::ColumnGtConst(0, 5)),
            Box::new(Predicate::True),
        );
        assert_eq!(pred2.simplify(), Predicate::ColumnGtConst(0, 5));
    }

    #[test]
    fn test_predicate_simplify_and_false() {
        let pred = Predicate::And(
            Box::new(Predicate::False),
            Box::new(Predicate::ColumnGtConst(0, 5)),
        );
        assert_eq!(pred.simplify(), Predicate::False);

        let pred2 = Predicate::And(
            Box::new(Predicate::ColumnGtConst(0, 5)),
            Box::new(Predicate::False),
        );
        assert_eq!(pred2.simplify(), Predicate::False);
    }

    #[test]
    fn test_predicate_simplify_or_true() {
        let pred = Predicate::Or(
            Box::new(Predicate::True),
            Box::new(Predicate::ColumnGtConst(0, 5)),
        );
        assert_eq!(pred.simplify(), Predicate::True);

        let pred2 = Predicate::Or(
            Box::new(Predicate::ColumnGtConst(0, 5)),
            Box::new(Predicate::True),
        );
        assert_eq!(pred2.simplify(), Predicate::True);
    }

    #[test]
    fn test_predicate_simplify_or_false() {
        let pred = Predicate::Or(
            Box::new(Predicate::False),
            Box::new(Predicate::ColumnGtConst(0, 5)),
        );
        assert_eq!(pred.simplify(), Predicate::ColumnGtConst(0, 5));

        let pred2 = Predicate::Or(
            Box::new(Predicate::ColumnGtConst(0, 5)),
            Box::new(Predicate::False),
        );
        assert_eq!(pred2.simplify(), Predicate::ColumnGtConst(0, 5));
    }

    #[test]
    fn test_predicate_simplify_nested() {
        let pred = Predicate::And(
            Box::new(Predicate::Or(
                Box::new(Predicate::False),
                Box::new(Predicate::ColumnEqConst(0, 1)),
            )),
            Box::new(Predicate::True),
        );
        assert_eq!(pred.simplify(), Predicate::ColumnEqConst(0, 1));
    }

    #[test]
    fn test_predicate_simplify_no_change() {
        let pred = Predicate::And(
            Box::new(Predicate::ColumnGtConst(0, 5)),
            Box::new(Predicate::ColumnLtConst(0, 10)),
        );
        let simplified = pred.clone().simplify();
        assert_eq!(simplified, pred);
    }

    #[test]
    fn test_predicate_adjust_for_projection_simple() {
        let projection = vec![1, 0, 2];
        let pred = Predicate::ColumnGtConst(1, 5);

        let adjusted = pred.adjust_for_projection(&projection);
        assert_eq!(adjusted, Some(Predicate::ColumnGtConst(0, 5)));
    }

    #[test]
    fn test_predicate_adjust_for_projection_missing_column() {
        let projection = vec![0, 2];
        let pred = Predicate::ColumnGtConst(1, 5);

        let adjusted = pred.adjust_for_projection(&projection);
        assert_eq!(adjusted, None);
    }

    #[test]
    fn test_predicate_adjust_for_projection_columns_eq() {
        let projection = vec![2, 0, 1];
        let pred = Predicate::ColumnsEq(0, 1);

        let adjusted = pred.adjust_for_projection(&projection);
        assert_eq!(adjusted, Some(Predicate::ColumnsEq(1, 2)));
    }

    #[test]
    fn test_predicate_adjust_for_projection_columns_ne() {
        let projection = vec![2, 0, 1];
        let pred = Predicate::ColumnsNe(0, 1);

        let adjusted = pred.adjust_for_projection(&projection);
        assert_eq!(adjusted, Some(Predicate::ColumnsNe(1, 2)));
    }

    #[test]
    fn test_predicate_adjust_for_projection_and() {
        let projection = vec![1, 0];
        let pred = Predicate::And(
            Box::new(Predicate::ColumnEqConst(0, 5)),
            Box::new(Predicate::ColumnGtConst(1, 10)),
        );

        let adjusted = pred.adjust_for_projection(&projection);
        assert_eq!(
            adjusted,
            Some(Predicate::And(
                Box::new(Predicate::ColumnEqConst(1, 5)),
                Box::new(Predicate::ColumnGtConst(0, 10)),
            ))
        );
    }

    #[test]
    fn test_predicate_adjust_for_projection_and_partial() {
        let projection = vec![0];
        let pred = Predicate::And(
            Box::new(Predicate::ColumnEqConst(0, 5)),
            Box::new(Predicate::ColumnGtConst(1, 10)),
        );

        let adjusted = pred.adjust_for_projection(&projection);
        assert_eq!(adjusted, Some(Predicate::ColumnEqConst(0, 5)));
    }

    #[test]
    fn test_predicate_adjust_for_projection_and_both_missing() {
        let projection = vec![2];
        let pred = Predicate::And(
            Box::new(Predicate::ColumnEqConst(0, 5)),
            Box::new(Predicate::ColumnGtConst(1, 10)),
        );

        let adjusted = pred.adjust_for_projection(&projection);
        assert_eq!(adjusted, None);
    }

    #[test]
    fn test_predicate_adjust_for_projection_or_requires_both() {
        let projection = vec![0];
        let pred = Predicate::Or(
            Box::new(Predicate::ColumnEqConst(0, 5)),
            Box::new(Predicate::ColumnGtConst(1, 10)),
        );

        let adjusted = pred.adjust_for_projection(&projection);
        assert_eq!(adjusted, None);
    }

    #[test]
    fn test_predicate_adjust_for_projection_or_both_present() {
        let projection = vec![1, 0];
        let pred = Predicate::Or(
            Box::new(Predicate::ColumnEqConst(0, 5)),
            Box::new(Predicate::ColumnGtConst(1, 10)),
        );

        let adjusted = pred.adjust_for_projection(&projection);
        assert_eq!(
            adjusted,
            Some(Predicate::Or(
                Box::new(Predicate::ColumnEqConst(1, 5)),
                Box::new(Predicate::ColumnGtConst(0, 10)),
            ))
        );
    }

    #[test]
    fn test_predicate_adjust_true_false() {
        let projection = vec![0];
        assert_eq!(
            Predicate::True.adjust_for_projection(&projection),
            Some(Predicate::True)
        );
        assert_eq!(
            Predicate::False.adjust_for_projection(&projection),
            Some(Predicate::False)
        );
    }

    #[test]
    fn test_predicate_adjust_all_comparison_types() {
        let projection = vec![1, 0];

        let predicates = vec![
            (
                Predicate::ColumnEqConst(0, 1),
                Predicate::ColumnEqConst(1, 1),
            ),
            (
                Predicate::ColumnNeConst(0, 1),
                Predicate::ColumnNeConst(1, 1),
            ),
            (
                Predicate::ColumnGtConst(0, 1),
                Predicate::ColumnGtConst(1, 1),
            ),
            (
                Predicate::ColumnLtConst(0, 1),
                Predicate::ColumnLtConst(1, 1),
            ),
            (
                Predicate::ColumnGeConst(0, 1),
                Predicate::ColumnGeConst(1, 1),
            ),
            (
                Predicate::ColumnLeConst(0, 1),
                Predicate::ColumnLeConst(1, 1),
            ),
        ];

        for (input, expected) in predicates {
            let adjusted = input.adjust_for_projection(&projection);
            assert_eq!(adjusted, Some(expected));
        }
    }

    #[test]
    fn test_predicate_clone_and_eq() {
        let pred = Predicate::And(
            Box::new(Predicate::ColumnEqConst(0, 1)),
            Box::new(Predicate::True),
        );

        let pred_clone = pred.clone();
        assert_eq!(pred, pred_clone);
    }

    // Note: Predicate does not implement Hash due to f64 fields in float predicates.
    // Use Vec with dedup or other approaches instead of HashSet when needed.

    #[test]
    fn test_predicate_debug() {
        let pred = Predicate::ColumnEqConst(0, 42);
        let debug_str = format!("{:?}", pred);
        assert!(debug_str.contains("ColumnEqConst"));
        assert!(debug_str.contains("42"));
    }

    // ========================================================================
    // Integration / Complex Scenario Tests
    // ========================================================================

    #[test]
    fn test_datalog_negation_pattern() {
        // Pattern: unreachable(x) :- node(x), !reachable(x).
        let nodes = IRNode::Scan {
            relation: "node".to_string(),
            schema: vec!["x".to_string()],
        };

        let reachable = IRNode::Scan {
            relation: "reachable".to_string(),
            schema: vec!["x".to_string()],
        };

        let unreachable = IRNode::Antijoin {
            left: Box::new(nodes),
            right: Box::new(reachable),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["x".to_string()],
        };

        assert_eq!(unreachable.output_schema(), vec!["x"]);
        let output = unreachable.pretty_print(0);
        assert!(output.contains("Antijoin"));
    }

    #[test]
    fn test_datalog_aggregation_pattern() {
        // Pattern: result(x, count<y>) :- data(x, y).
        let data = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };

        let result = IRNode::Aggregate {
            input: Box::new(data),
            group_by: vec![0],
            aggregations: vec![(AggregateFunction::Count, 1)],
            output_schema: vec!["x".to_string(), "count_y".to_string()],
        };

        assert_eq!(result.output_schema(), vec!["x", "count_y"]);
    }

    #[test]
    fn test_join_then_aggregate() {
        let orders = IRNode::Scan {
            relation: "orders".to_string(),
            schema: vec!["order_id".to_string(), "customer_id".to_string()],
        };

        let items = IRNode::Scan {
            relation: "items".to_string(),
            schema: vec!["item_order_id".to_string(), "price".to_string()],
        };

        let joined = IRNode::Join {
            left: Box::new(orders),
            right: Box::new(items),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec![
                "order_id".to_string(),
                "customer_id".to_string(),
                "item_order_id".to_string(),
                "price".to_string(),
            ],
        };

        let aggregated = IRNode::Aggregate {
            input: Box::new(joined),
            group_by: vec![1],                               // group by customer_id
            aggregations: vec![(AggregateFunction::Sum, 3)], // sum(price)
            output_schema: vec!["customer_id".to_string(), "total_spent".to_string()],
        };

        assert_eq!(
            aggregated.output_schema(),
            vec!["customer_id", "total_spent"]
        );
    }

    #[test]
    fn test_pretty_print_indentation() {
        let scan = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["x".to_string()],
        };

        let output_0 = scan.pretty_print(0);
        let output_2 = scan.pretty_print(2);

        assert!(!output_0.starts_with("  "));
        assert!(output_2.starts_with("    ")); // 2 * 2 spaces
    }
}
