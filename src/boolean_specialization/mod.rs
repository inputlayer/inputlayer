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

