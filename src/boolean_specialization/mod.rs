//! # Boolean Specialization
//!
//! Selects the minimal semiring for each query: Boolean (set semantics, 1 byte)
//! when only existence matters, Counting (bag semantics, 8 bytes) when duplicates
//! or counts are needed, Min/Max for recursive min/max aggregation.
//!
//! Walks the IR tree, propagates constraints upward, and annotates each node.
//! For Boolean semiring, wraps Join/JoinFlatMap in Distinct to enforce set semantics.
//!
//! ```text
//! IRNode -> [Boolean Spec] -> Annotated IRNode -> Code Gen (with semiring info)
//! ```

use crate::ir::{IRNode, Predicate};
use std::collections::{HashMap, HashSet};

/// Semiring type for query execution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SemiringType {
    /// Set semantics (presence only)
    #[default]
    Boolean,

    /// Bag semantics (tracks multiplicities)
    Counting,

    /// Finds minimum values (e.g. shortest path)
    Min,

    /// Finds maximum values (e.g. widest path)
    Max,
}

impl SemiringType {
    /// Check if this semiring is more general than another
    /// More general means it can express everything the other can
    pub fn is_more_general_than(&self, other: &SemiringType) -> bool {
        match (self, other) {
            // Everything is more general than Boolean
            (SemiringType::Counting, SemiringType::Boolean) => true,
            // Same type is equal, not more general
            (a, b) if a == b => false,
            // Default: not more general
            _ => false,
        }
    }

    /// Get the minimal semiring that satisfies both constraints
    pub fn meet(&self, other: &SemiringType) -> SemiringType {
        if self == other {
            return *self;
        }

        // If one is boolean and other is counting, need counting
        match (self, other) {
            (SemiringType::Boolean, SemiringType::Counting)
            | (SemiringType::Counting, SemiringType::Boolean) => SemiringType::Counting,
            // Min/Max don't combine well - default to counting
            (SemiringType::Min, _) | (_, SemiringType::Min) => SemiringType::Min,
            (SemiringType::Max, _) | (_, SemiringType::Max) => SemiringType::Max,
            _ => SemiringType::Counting,
        }
    }
}

/// Analysis result for a single IR node
#[derive(Debug, Clone)]
pub struct SemiringAnnotation {
    /// The selected semiring for this node
    pub semiring: SemiringType,
    /// Whether this node requires duplicate tracking
    pub needs_duplicates: bool,
    /// Whether this node is part of a recursive computation
    pub is_recursive: bool,
    /// Reason for semiring selection (for debugging)
    pub reason: String,
}

impl Default for SemiringAnnotation {
    fn default() -> Self {
        SemiringAnnotation {
            semiring: SemiringType::Boolean,
            needs_duplicates: false,
            is_recursive: false,
            reason: "default".to_string(),
        }
    }
}

/// Statistics about boolean specialization
#[derive(Debug, Clone, Default)]
pub struct SpecializationStats {
    /// Total nodes analyzed
    pub total_nodes: usize,
    /// Nodes that can use boolean semiring
    pub boolean_nodes: usize,
    /// Nodes that require counting semiring
    pub counting_nodes: usize,
    /// Nodes using min semiring
    pub min_nodes: usize,
    /// Nodes using max semiring
    pub max_nodes: usize,
    /// Estimated performance improvement factor
    pub estimated_speedup: f64,
}

/// Picks the minimal semiring for each IR node.
pub struct BooleanSpecializer {
    /// Whether to enable boolean specialization
    enable_specialization: bool,
    /// Relations known to be recursive
    recursive_relations: HashSet<String>,
    /// Cache of annotations for IR nodes
    annotations: HashMap<usize, SemiringAnnotation>,
    /// Counter for generating unique node IDs
    node_counter: usize,
}

impl BooleanSpecializer {
    /// Create a new boolean specializer
    pub fn new() -> Self {
        BooleanSpecializer {
            enable_specialization: true,
            recursive_relations: HashSet::new(),
            annotations: HashMap::new(),
            node_counter: 0,
        }
    }

    /// Enable or disable boolean specialization
    pub fn set_specialization(&mut self, enable: bool) {
        self.enable_specialization = enable;
    }

    /// Mark a relation as recursive
    pub fn mark_recursive(&mut self, relation: &str) {
        self.recursive_relations.insert(relation.to_string());
    }

    /// Walk the IR bottom-up, pick semiring per node, add/remove Distinct as needed.
    pub fn specialize(&mut self, ir: IRNode) -> (IRNode, SemiringAnnotation) {
        if !self.enable_specialization {
            return (ir.clone(), SemiringAnnotation::default());
        }

        self.node_counter = 0;
        self.annotations.clear();

        // First pass: analyze semiring requirements
        let annotation = self.analyze_node(&ir);

        // Second pass: transform IR based on analysis
        let optimized_ir = self.transform_for_semiring(ir, &annotation);

        (optimized_ir, annotation)
    }

    /// Transform IR based on semiring analysis
    ///
    /// For boolean semiring:
    /// - Ensure Distinct is applied at appropriate points
    /// - Remove redundant Distinct operations
    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
    fn transform_for_semiring(&self, ir: IRNode, annotation: &SemiringAnnotation) -> IRNode {
        match ir {
            // If boolean semiring and join without distinct, wrap in distinct
            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } if annotation.semiring == SemiringType::Boolean => {
                let left_ann = SemiringAnnotation {
                    semiring: SemiringType::Boolean,
                    ..Default::default()
                };
                let right_ann = SemiringAnnotation {
                    semiring: SemiringType::Boolean,
                    ..Default::default()
                };

                let transformed = IRNode::Join {
                    left: Box::new(self.transform_for_semiring(*left, &left_ann)),
                    right: Box::new(self.transform_for_semiring(*right, &right_ann)),
                    left_keys,
                    right_keys,
                    output_schema,
                };

                // For boolean semiring, wrap join in distinct to enforce set semantics
                IRNode::Distinct {
                    input: Box::new(transformed),
                }
            }

            // Remove redundant distinct on scan (scans are already unique by key)
            IRNode::Distinct { input } => {
                let inner = self.transform_for_semiring(*input, annotation);
                // If inner is already a distinct or scan, don't double-wrap
                match &inner {
                    IRNode::Distinct { .. } => inner,
                    IRNode::Scan { .. } if annotation.semiring == SemiringType::Boolean => inner,
                    _ => IRNode::Distinct {
                        input: Box::new(inner),
                    },
                }
            }

            // Recursively transform other nodes
            IRNode::Map {
                input,
                projection,
                output_schema,
            } => IRNode::Map {
                input: Box::new(self.transform_for_semiring(*input, annotation)),
                projection,
                output_schema,
            },

            IRNode::Filter { input, predicate } => IRNode::Filter {
                input: Box::new(self.transform_for_semiring(*input, annotation)),
                predicate,
            },

            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => {
                // Non-boolean semiring join
                IRNode::Join {
                    left: Box::new(self.transform_for_semiring(*left, annotation)),
                    right: Box::new(self.transform_for_semiring(*right, annotation)),
                    left_keys,
                    right_keys,
                    output_schema,
                }
            }

            IRNode::Union { inputs } => IRNode::Union {
                inputs: inputs
                    .into_iter()
                    .map(|i| self.transform_for_semiring(i, annotation))
                    .collect(),
            },

            IRNode::Aggregate {
                input,
                group_by,
                aggregations,
                output_schema,
            } => IRNode::Aggregate {
                input: Box::new(self.transform_for_semiring(*input, annotation)),
                group_by,
                aggregations,
                output_schema,
            },

            // For boolean semiring, ensure inputs to antijoin are distinct
            // (antijoin requires proper set semantics for correct negation)
            IRNode::Antijoin {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } if annotation.semiring == SemiringType::Boolean => {
                let left_ann = SemiringAnnotation {
                    semiring: SemiringType::Boolean,
                    ..Default::default()
                };
                let right_ann = SemiringAnnotation {
                    semiring: SemiringType::Boolean,
                    ..Default::default()
                };
                let left_transformed = self.transform_for_semiring(*left, &left_ann);
                let right_transformed = self.transform_for_semiring(*right, &right_ann);

                // Wrap left in Distinct if it's not already distinct/scan
                let left_final = match &left_transformed {
                    IRNode::Distinct { .. } | IRNode::Scan { .. } => left_transformed,
                    _ => IRNode::Distinct {
                        input: Box::new(left_transformed),
                    },
                };

                IRNode::Antijoin {
                    left: Box::new(left_final),
                    right: Box::new(right_transformed),
                    left_keys,
                    right_keys,
                    output_schema,
                }
            }

            IRNode::Antijoin {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => IRNode::Antijoin {
                left: Box::new(self.transform_for_semiring(*left, annotation)),
                right: Box::new(self.transform_for_semiring(*right, annotation)),
                left_keys,
                right_keys,
                output_schema,
            },

            // Scans don't need transformation
            IRNode::Scan { .. } => ir,

            // HnswScan doesn't need transformation (terminal node)
            IRNode::HnswScan { .. } => ir,

            IRNode::Compute { input, expressions } => IRNode::Compute {
                input: Box::new(self.transform_for_semiring(*input, annotation)),
                expressions,
            },

            IRNode::FlatMap {
                input,
                projection,
                filter_predicate,
                output_schema,
            } => IRNode::FlatMap {
                input: Box::new(self.transform_for_semiring(*input, annotation)),
                projection,
                filter_predicate,
                output_schema,
            },

            // For boolean semiring, wrap JoinFlatMap in Distinct (like Join)
            IRNode::JoinFlatMap {
                left,
                right,
                left_keys,
                right_keys,
                projection,
                filter_predicate,
                output_schema,
            } if annotation.semiring == SemiringType::Boolean => {
                let left_ann = SemiringAnnotation {
                    semiring: SemiringType::Boolean,
                    ..Default::default()
                };
                let right_ann = SemiringAnnotation {
                    semiring: SemiringType::Boolean,
                    ..Default::default()
                };
                let transformed = IRNode::JoinFlatMap {
                    left: Box::new(self.transform_for_semiring(*left, &left_ann)),
                    right: Box::new(self.transform_for_semiring(*right, &right_ann)),
                    left_keys,
                    right_keys,
                    projection,
                    filter_predicate,
                    output_schema,
                };
                IRNode::Distinct {
                    input: Box::new(transformed),
                }
            }

            IRNode::JoinFlatMap {
                left,
                right,
                left_keys,
                right_keys,
                projection,
                filter_predicate,
                output_schema,
            } => IRNode::JoinFlatMap {
                left: Box::new(self.transform_for_semiring(*left, annotation)),
                right: Box::new(self.transform_for_semiring(*right, annotation)),
                left_keys,
                right_keys,
                projection,
                filter_predicate,
                output_schema,
            },
        }
    }

    /// Analyze semiring requirements for a node
    fn analyze_node(&mut self, ir: &IRNode) -> SemiringAnnotation {
        let node_id = self.node_counter;
        self.node_counter += 1;

        let annotation = match ir {
            IRNode::Scan { relation, .. } => {
                // Base case: scans can use boolean semiring
                // unless the relation is known to need counting
                let is_recursive = self.recursive_relations.contains(relation);
                SemiringAnnotation {
                    semiring: SemiringType::Boolean,
                    needs_duplicates: false,
                    is_recursive,
                    reason: format!("scan of {relation}"),
                }
            }

            IRNode::Map { input, .. } => {
                // Map preserves the semiring of its input
                let child = self.analyze_node(input);
                SemiringAnnotation {
                    semiring: child.semiring,
                    needs_duplicates: child.needs_duplicates,
                    is_recursive: child.is_recursive,
                    reason: format!("map inherits from child: {:?}", child.semiring),
                }
            }

            IRNode::Filter { input, predicate } => {
                // Filter preserves the semiring of its input
                let child = self.analyze_node(input);

                // Check if predicate uses aggregation functions
                let needs_counting = self.predicate_needs_counting(predicate);

                let semiring = if needs_counting {
                    SemiringType::Counting
                } else {
                    child.semiring
                };

                SemiringAnnotation {
                    semiring,
                    needs_duplicates: child.needs_duplicates || needs_counting,
                    is_recursive: child.is_recursive,
                    reason: format!("filter: {semiring:?}"),
                }
            }

            IRNode::Join { left, right, .. } => {
                // Join may introduce multiplicities
                let left_ann = self.analyze_node(left);
                let right_ann = self.analyze_node(right);

                // If both sides are boolean, result can be boolean
                // unless we need to track how many ways tuples can join
                let semiring = if left_ann.semiring == SemiringType::Boolean
                    && right_ann.semiring == SemiringType::Boolean
                {
                    // For most queries, boolean is sufficient
                    // Only need counting if tracking path counts, etc.
                    SemiringType::Boolean
                } else {
                    left_ann.semiring.meet(&right_ann.semiring)
                };

                SemiringAnnotation {
                    semiring,
                    needs_duplicates: left_ann.needs_duplicates || right_ann.needs_duplicates,
                    is_recursive: left_ann.is_recursive || right_ann.is_recursive,
                    reason: format!(
                        "join of {:?} and {:?}",
                        left_ann.semiring, right_ann.semiring
                    ),
                }
            }

            IRNode::Distinct { input } => {
                // Distinct explicitly removes duplicates -> boolean semiring
                let child = self.analyze_node(input);
                SemiringAnnotation {
                    semiring: SemiringType::Boolean,
                    needs_duplicates: false,
                    is_recursive: child.is_recursive,
                    reason: "distinct forces boolean".to_string(),
                }
            }

            IRNode::Union { inputs } => {
                // Union combines results
                // TODO: verify this condition
                if inputs.is_empty() {
                    return SemiringAnnotation::default();
                }

                let mut combined = self.analyze_node(&inputs[0]);
                for input in inputs.iter().skip(1) {
                    let child = self.analyze_node(input);
                    combined.semiring = combined.semiring.meet(&child.semiring);
                    combined.needs_duplicates = combined.needs_duplicates || child.needs_duplicates;
                    combined.is_recursive = combined.is_recursive || child.is_recursive;
                }
                combined.reason = format!("union of {} inputs", inputs.len());
                combined
            }

            IRNode::Aggregate {
                input,
                aggregations,
                ..
            } => {
                // Determine semiring based on aggregation functions:
                // - Min-only -> Min semiring
                // - Max-only -> Max semiring
                // - Mixed or Count/Sum/Avg -> Counting semiring
                let child = self.analyze_node(input);
                let all_min = !aggregations.is_empty()
                    && aggregations
                        .iter()
                        .all(|(f, _)| matches!(f, crate::ir::AggregateFunction::Min));
                let all_max = !aggregations.is_empty()
                    && aggregations
                        .iter()
                        .all(|(f, _)| matches!(f, crate::ir::AggregateFunction::Max));
                let semiring = if all_min {
                    SemiringType::Min
                } else if all_max {
                    SemiringType::Max
                } else {
                    SemiringType::Counting
                };
                SemiringAnnotation {
                    semiring,
                    needs_duplicates: true,
                    is_recursive: child.is_recursive,
                    reason: format!("aggregation: {semiring:?}"),
                }
            }

            IRNode::Antijoin { left, right, .. } => {
                // Antijoin filters left based on right, similar to Join
                let left_ann = self.analyze_node(left);
                let right_ann = self.analyze_node(right);

                // Antijoin can use boolean semiring if both sides are boolean
                let semiring = if left_ann.semiring == SemiringType::Boolean
                    && right_ann.semiring == SemiringType::Boolean
                {
                    SemiringType::Boolean
                } else {
                    left_ann.semiring.meet(&right_ann.semiring)
                };

                SemiringAnnotation {
                    semiring,
                    needs_duplicates: left_ann.needs_duplicates || right_ann.needs_duplicates,
                    is_recursive: left_ann.is_recursive || right_ann.is_recursive,
                    reason: format!(
                        "antijoin of {:?} and {:?}",
                        left_ann.semiring, right_ann.semiring
                    ),
                }
            }

            IRNode::Compute { input, .. } => {
                // Compute preserves the semiring of its input
                let child = self.analyze_node(input);
                SemiringAnnotation {
                    semiring: child.semiring,
                    needs_duplicates: child.needs_duplicates,
                    is_recursive: child.is_recursive,
                    reason: format!("compute inherits from child: {:?}", child.semiring),
                }
            }

            IRNode::HnswScan { .. } => {
                // HnswScan is a terminal node, like Scan - uses boolean semiring
                SemiringAnnotation {
                    semiring: SemiringType::Boolean,
                    needs_duplicates: false,
                    is_recursive: false,
                    reason: "hnsw_scan terminal node".to_string(),
                }
            }

            IRNode::FlatMap { input, .. } => {
                // FlatMap preserves the semiring of its input (like Map)
                let child = self.analyze_node(input);
                SemiringAnnotation {
                    semiring: child.semiring,
                    needs_duplicates: child.needs_duplicates,
                    is_recursive: child.is_recursive,
                    reason: format!("flatmap inherits from child: {:?}", child.semiring),
                }
            }

            IRNode::JoinFlatMap { left, right, .. } => {
                // JoinFlatMap inherits from both children (like Join)
                let left_ann = self.analyze_node(left);
                let right_ann = self.analyze_node(right);

                let semiring = if left_ann.semiring == SemiringType::Boolean
                    && right_ann.semiring == SemiringType::Boolean
                {
                    SemiringType::Boolean
                } else {
                    left_ann.semiring.meet(&right_ann.semiring)
                };

                SemiringAnnotation {
                    semiring,
                    needs_duplicates: left_ann.needs_duplicates || right_ann.needs_duplicates,
                    is_recursive: left_ann.is_recursive || right_ann.is_recursive,
                    reason: format!(
                        "join_flatmap of {:?} and {:?}",
                        left_ann.semiring, right_ann.semiring
                    ),
                }
            }
        };

        self.annotations.insert(node_id, annotation.clone());
        annotation
    }

    /// Check if a predicate requires counting semantics
    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
    fn predicate_needs_counting(&self, predicate: &Predicate) -> bool {
        match predicate {
            // Basic comparisons don't need counting
            Predicate::ColumnEqConst(_, _)
            | Predicate::ColumnNeConst(_, _)
            | Predicate::ColumnGtConst(_, _)
            | Predicate::ColumnLtConst(_, _)
            | Predicate::ColumnGeConst(_, _)
            | Predicate::ColumnLeConst(_, _)
            | Predicate::ColumnEqStr(_, _)
            | Predicate::ColumnNeStr(_, _)
            | Predicate::ColumnLtStr(_, _)
            | Predicate::ColumnGtStr(_, _)
            | Predicate::ColumnLeStr(_, _)
            | Predicate::ColumnGeStr(_, _)
            | Predicate::ColumnEqFloat(_, _)
            | Predicate::ColumnNeFloat(_, _)
            | Predicate::ColumnGtFloat(_, _)
            | Predicate::ColumnLtFloat(_, _)
            | Predicate::ColumnGeFloat(_, _)
            | Predicate::ColumnLeFloat(_, _)
            | Predicate::ColumnsEq(_, _)
            | Predicate::ColumnsNe(_, _)
            | Predicate::ColumnsLt(_, _)
            | Predicate::ColumnsGt(_, _)
            | Predicate::ColumnsLe(_, _)
            | Predicate::ColumnsGe(_, _)
            | Predicate::ColumnCompareArith(_, _, _, _)
            | Predicate::ArithCompareConst(_, _, _, _)
            | Predicate::True
            | Predicate::False => false,

            // Compound predicates inherit from children
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                self.predicate_needs_counting(left) || self.predicate_needs_counting(right)
            }
        }
    }

    /// Compute statistics about specialization opportunities
    pub fn compute_stats(&mut self, irs: &[IRNode]) -> SpecializationStats {
        let mut stats = SpecializationStats::default();

        for ir in irs {
            self.count_nodes_recursive(ir, &mut stats);
        }

        // Estimate speedup: boolean operations are ~2-3x faster
        if stats.total_nodes > 0 {
            let boolean_ratio = stats.boolean_nodes as f64 / stats.total_nodes as f64;
            // Conservative estimate: 2x speedup for boolean nodes
            stats.estimated_speedup = 1.0 + boolean_ratio;
        }

        stats
    }

    /// Count nodes by semiring type
    fn count_nodes_recursive(&mut self, ir: &IRNode, stats: &mut SpecializationStats) {
        stats.total_nodes += 1;

        let annotation = self.analyze_node(ir);
        match annotation.semiring {
            SemiringType::Boolean => stats.boolean_nodes += 1,
            SemiringType::Counting => stats.counting_nodes += 1,
            SemiringType::Min => stats.min_nodes += 1,
            SemiringType::Max => stats.max_nodes += 1,
        }

        // Recurse into children
        match ir {
            IRNode::Scan { .. } => {}
            IRNode::Map { input, .. } => self.count_nodes_recursive(input, stats),
            IRNode::Filter { input, .. } => self.count_nodes_recursive(input, stats),
            IRNode::Join { left, right, .. } => {
                self.count_nodes_recursive(left, stats);
                self.count_nodes_recursive(right, stats);
            }
            IRNode::Distinct { input } => self.count_nodes_recursive(input, stats),
            IRNode::Union { inputs } => {
                for input in inputs {
                    self.count_nodes_recursive(input, stats);
                }
            }
            IRNode::Aggregate { input, .. } => self.count_nodes_recursive(input, stats),
            IRNode::Antijoin { left, right, .. } => {
                self.count_nodes_recursive(left, stats);
                self.count_nodes_recursive(right, stats);
            }
            IRNode::Compute { input, .. } => self.count_nodes_recursive(input, stats),
            IRNode::HnswScan { .. } => {} // Terminal node
            IRNode::FlatMap { input, .. } => self.count_nodes_recursive(input, stats),
            IRNode::JoinFlatMap { left, right, .. } => {
                self.count_nodes_recursive(left, stats);
                self.count_nodes_recursive(right, stats);
            }
        }
    }

    /// Get the semiring annotation for a specific relation
    pub fn get_relation_semiring(&self, relation: &str) -> SemiringType {
        // Default to boolean semiring (set semantics) for all relations
        if self.recursive_relations.contains(relation) {
            SemiringType::Boolean // Recursive relations typically use set semantics
        } else {
            SemiringType::Boolean
        }
    }

    /// Suggest optimal semiring for a query pattern
    pub fn suggest_semiring(&self, ir: &IRNode) -> SemiringType {
        // Heuristics for semiring selection:
        // 1. If there's a Distinct, use Boolean
        // 2. If all operations are joins and scans, use Boolean
        // 3. If there are aggregations, check the aggregation type
        // 4. Default to Boolean (most common case)

        self.analyze_ir_pattern(ir)
    }

    /// Analyze IR pattern to determine semiring
    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
    fn analyze_ir_pattern(&self, ir: &IRNode) -> SemiringType {
        match ir {
            IRNode::Distinct { .. } => SemiringType::Boolean,
            IRNode::Scan { .. } => SemiringType::Boolean,
            IRNode::Map { input, .. } => self.analyze_ir_pattern(input),
            IRNode::Filter { input, .. } => self.analyze_ir_pattern(input),
            IRNode::Join { left, right, .. } => {
                let left_sem = self.analyze_ir_pattern(left);
                let right_sem = self.analyze_ir_pattern(right);
                left_sem.meet(&right_sem)
            }
            IRNode::Union { inputs } => {
                if inputs.is_empty() {
                    return SemiringType::Boolean;
                }
                inputs
                    .iter()
                    .map(|i| self.analyze_ir_pattern(i))
                    .fold(SemiringType::Boolean, |acc, s| acc.meet(&s))
            }
            IRNode::Aggregate { .. } => SemiringType::Counting, // Aggregation needs counting
            IRNode::Antijoin { left, right, .. } => {
                let left_sem = self.analyze_ir_pattern(left);
                let right_sem = self.analyze_ir_pattern(right);
                left_sem.meet(&right_sem)
            }
            IRNode::Compute { input, .. } => self.analyze_ir_pattern(input),
            IRNode::HnswScan { .. } => SemiringType::Boolean, // Terminal node like Scan
            IRNode::FlatMap { input, .. } => self.analyze_ir_pattern(input),
            IRNode::JoinFlatMap { left, right, .. } => {
                let left_sem = self.analyze_ir_pattern(left);
                let right_sem = self.analyze_ir_pattern(right);
                left_sem.meet(&right_sem)
            }
        }
    }

    /// Check if an IR can be executed with boolean semiring
    pub fn can_use_boolean(&self, ir: &IRNode) -> bool {
        self.analyze_ir_pattern(ir) == SemiringType::Boolean
    }
}

/// Compute the most restrictive semiring across all annotations.
/// If ANY annotation requires Counting, the result is Counting.
/// If all are Boolean, the result is Boolean.
pub fn compute_global_semiring(annotations: &[SemiringAnnotation]) -> SemiringType {
    annotations
        .iter()
        .map(|a| a.semiring)
        .fold(SemiringType::Boolean, |acc, s| acc.meet(&s))
}

impl Default for BooleanSpecializer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scan(relation: &str) -> IRNode {
        IRNode::Scan {
            relation: relation.to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        }
    }

    fn make_join(left: IRNode, right: IRNode) -> IRNode {
        IRNode::Join {
            left: Box::new(left),
            right: Box::new(right),
            left_keys: vec![1],
            right_keys: vec![0],
            output_schema: vec!["x".to_string(), "y".to_string(), "z".to_string()],
        }
    }

    fn make_distinct(input: IRNode) -> IRNode {
        IRNode::Distinct {
            input: Box::new(input),
        }
    }

    fn make_filter(input: IRNode) -> IRNode {
        IRNode::Filter {
            input: Box::new(input),
            predicate: Predicate::ColumnsEq(0, 1),
        }
    }

    #[test]
    fn test_scan_uses_boolean() {
        let mut specializer = BooleanSpecializer::new();
        let ir = make_scan("edge");

        let (_, annotation) = specializer.specialize(ir);
        assert_eq!(annotation.semiring, SemiringType::Boolean);
    }

    #[test]
    fn test_distinct_forces_boolean() {
        let mut specializer = BooleanSpecializer::new();
        let ir = make_distinct(make_join(make_scan("R"), make_scan("S")));

        let (_, annotation) = specializer.specialize(ir);
        assert_eq!(annotation.semiring, SemiringType::Boolean);
        assert!(!annotation.needs_duplicates);
    }

    #[test]
    fn test_join_preserves_boolean() {
        let mut specializer = BooleanSpecializer::new();
        let ir = make_join(make_scan("R"), make_scan("S"));

        let (_, annotation) = specializer.specialize(ir);
        assert_eq!(annotation.semiring, SemiringType::Boolean);
    }

    #[test]
    fn test_filter_preserves_semiring() {
        let mut specializer = BooleanSpecializer::new();
        let ir = make_filter(make_scan("R"));

        let (_, annotation) = specializer.specialize(ir);
        assert_eq!(annotation.semiring, SemiringType::Boolean);
    }

    #[test]
    fn test_union_combines_semirings() {
        let mut specializer = BooleanSpecializer::new();
        let ir = IRNode::Union {
            inputs: vec![make_scan("R"), make_scan("S")],
        };

        let (_, annotation) = specializer.specialize(ir);
        assert_eq!(annotation.semiring, SemiringType::Boolean);
    }

    #[test]
    fn test_semiring_meet() {
        assert_eq!(
            SemiringType::Boolean.meet(&SemiringType::Boolean),
            SemiringType::Boolean
        );
        assert_eq!(
            SemiringType::Boolean.meet(&SemiringType::Counting),
            SemiringType::Counting
        );
        assert_eq!(
            SemiringType::Counting.meet(&SemiringType::Counting),
            SemiringType::Counting
        );
    }

    #[test]
    fn test_compute_stats() {
        let mut specializer = BooleanSpecializer::new();
        let ir = make_distinct(make_join(make_scan("R"), make_scan("S")));

        let stats = specializer.compute_stats(&[ir]);

        assert!(stats.total_nodes > 0);
        assert!(stats.boolean_nodes > 0);
        assert!(stats.estimated_speedup >= 1.0);
    }

    #[test]
    fn test_disabled_specialization() {
        let mut specializer = BooleanSpecializer::new();
        specializer.set_specialization(false);

        let ir = make_scan("edge");
        let (_, annotation) = specializer.specialize(ir);

        // Default annotation when disabled
        assert_eq!(annotation.semiring, SemiringType::Boolean);
    }

    #[test]
    fn test_recursive_relation_marking() {
        let mut specializer = BooleanSpecializer::new();
        specializer.mark_recursive("reachable");

        assert_eq!(
            specializer.get_relation_semiring("reachable"),
            SemiringType::Boolean
        );
    }

    #[test]
    fn test_can_use_boolean() {
        let specializer = BooleanSpecializer::new();

        // Scan can use boolean
        assert!(specializer.can_use_boolean(&make_scan("R")));

        // Join can use boolean
        let join = make_join(make_scan("R"), make_scan("S"));
        assert!(specializer.can_use_boolean(&join));

        // Distinct forces boolean
        assert!(specializer.can_use_boolean(&make_distinct(join)));
    }

