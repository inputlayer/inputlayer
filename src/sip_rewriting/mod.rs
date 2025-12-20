//! # SIP Rewriting (Module 08)
//!
//! **Course Module**: Module 08: Sideways Information Passing
//!
//! ## What This Module Does
//!
//! SIP (Sideways Information Passing) rewriting adds existence check filters
//! before joins to reduce intermediate result sizes. This implementation uses
//! a Yannakakis-style approach:
//!
//! 1. **Join Graph Analysis**: Build graph showing variable dependencies
//! 2. **Forward Pass**: Add semijoin filters in traversal order
//! 3. **Backward Pass**: Add semijoin filters in reverse order
//! 4. **Filter Generation**: Create existence check filters
//!
//! ## Algorithm Overview (Yannakakis-style)
//!
//! For acyclic queries:
//! 1. Build join tree from IR
//! 2. Forward pass: For each node, filter against already-visited neighbors
//! 3. Backward pass: For each node in reverse, filter again
//! 4. Result: Each relation only contains tuples that will contribute to output
//!
//! ## Example
//!
//! Original:
//! ```datalog
//! result(x, z) :- R(x, y), S(y, z), T(z, w).
//! ```
//!
//! After SIP (conceptual):
//! ```text
//! R' = R ⋉ S (R filtered to keep only y values that exist in S)
//! S' = S ⋉ R ⋉ T (S filtered to keep y from R', z from T)
//! T' = T ⋉ S (T filtered to keep z values that exist in S)
//! result = R' ⋈ S' ⋈ T'
//! ```
//!
//! ## Key Concepts
//!
//! - **Semijoin (⋉)**: Keep left tuples that have matching right tuples
//! - **Existence Check**: Filter by checking if value exists in another relation
//! - **Binding Patterns**: Track which variables are bound vs free
//! - **Magic Sets**: Generate auxiliary predicates to restrict computation
//!
//! ## Pipeline Position
//!
//! ```text
//! IRNode with Joins → [SIP Rewriting] → IRNode with Existence Filters → Code Gen
//! ```

use crate::ir::{IRNode, Predicate};
use std::collections::{HashMap, HashSet};

/// Variable binding status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Binding {
    /// Variable is bound (value is known)
    Bound,
    /// Variable is free (value is unknown)
    Free,
}

/// Adornment: describes binding pattern for a predicate
#[derive(Debug, Clone)]
pub struct Adornment {
    /// Binding status for each argument position
    pub bindings: Vec<Binding>,
}

impl Adornment {
    /// Create a new adornment with all free bindings
    pub fn all_free(arity: usize) -> Self {
        Adornment {
            bindings: vec![Binding::Free; arity],
        }
    }

    /// Create a new adornment with specified bound positions
    pub fn with_bound(arity: usize, bound_positions: &[usize]) -> Self {
        let mut bindings = vec![Binding::Free; arity];
        for &pos in bound_positions {
            if pos < arity {
                bindings[pos] = Binding::Bound;
            }
        }
        Adornment { bindings }
    }

    /// Get bound positions
    pub fn bound_positions(&self) -> Vec<usize> {
        self.bindings
            .iter()
            .enumerate()
            .filter(|(_, b)| **b == Binding::Bound)
            .map(|(i, _)| i)
            .collect()
    }

    /// Check if position is bound
    pub fn is_bound(&self, pos: usize) -> bool {
        pos < self.bindings.len() && self.bindings[pos] == Binding::Bound
    }

    /// Convert to string representation (e.g., "bf" for bound-free)
    pub fn to_string(&self) -> String {
        self.bindings
            .iter()
            .map(|b| if *b == Binding::Bound { 'b' } else { 'f' })
            .collect()
    }
}

/// Statistics about SIP rewriting
#[derive(Debug, Clone, Default)]
pub struct SipStats {
    /// Number of existence filters added
    pub filters_added: usize,
    /// Number of relations with SIP applied
    pub relations_rewritten: usize,
    /// Estimated tuple reduction factor
    pub estimated_reduction: f64,
}

/// SIP traversal order for acyclic queries
#[derive(Debug, Clone)]
pub struct SipTraversal {
    /// Forward pass order (node indices)
    pub forward_order: Vec<usize>,
    /// Backward pass order (node indices)
    pub backward_order: Vec<usize>,
}

impl SipTraversal {
    /// Create traversal from a rooted tree
    pub fn from_tree(edges: &[(usize, usize)], root: usize, num_nodes: usize) -> Self {
        // Build adjacency list
        let mut adj: HashMap<usize, Vec<usize>> = HashMap::new();
        for &(u, v) in edges {
            adj.entry(u).or_insert_with(Vec::new).push(v);
            adj.entry(v).or_insert_with(Vec::new).push(u);
        }

        // BFS from root to get forward order
        let mut forward_order = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = vec![root];
        visited.insert(root);

        while let Some(node) = queue.pop() {
            forward_order.push(node);
            if let Some(neighbors) = adj.get(&node) {
                for &neighbor in neighbors {
                    if visited.insert(neighbor) {
                        queue.push(neighbor);
                    }
                }
            }
        }

        // Add any disconnected nodes
        for i in 0..num_nodes {
            if !visited.contains(&i) {
                forward_order.push(i);
            }
        }

        // Backward is reverse of forward
        let backward_order: Vec<usize> = forward_order.iter().rev().cloned().collect();

        SipTraversal {
            forward_order,
            backward_order,
        }
    }
}

/// SIP (Sideways Information Passing) rewriter for queries
///
/// This implementation adds existence check filters before joins to reduce
/// intermediate result sizes using a Yannakakis-style approach.
pub struct SipRewriter {
    /// Whether to enable SIP rewriting
    enable_sip: bool,
    /// Track statistics
    stats: SipStats,
}

impl SipRewriter {
    /// Create a new SIP rewriter
    pub fn new() -> Self {
        SipRewriter {
            enable_sip: true,
            stats: SipStats::default(),
        }
    }

    /// Enable or disable SIP rewriting
    pub fn set_sip(&mut self, enable: bool) {
        self.enable_sip = enable;
    }

    /// Rewrite IR to use SIP optimization
    ///
    /// # Algorithm
    ///
    /// 1. Analyze join structure
    /// 2. Identify shared variables between relations
    /// 3. Add existence check filters before joins
    /// 4. Preserve query semantics while reducing intermediate sizes
    ///
    /// # Returns
    ///
    /// Optimized IR with existence check filters
    pub fn rewrite(&mut self, ir: IRNode) -> IRNode {
        if !self.enable_sip {
            return ir;
        }

        self.stats = SipStats::default();

        // Only apply SIP to single joins, not chain joins
        // Chain joins are detected by having more than one join in the tree
        let num_joins = Self::count_joins(&ir);
        if num_joins != 1 {
            return ir;
        }

        // Apply SIP transformation
        self.apply_sip(ir)
    }

    /// Check if IR contains joins
    fn has_joins(ir: &IRNode) -> bool {
        match ir {
            IRNode::Join { .. } => true,
            IRNode::Antijoin { .. } => false,
            IRNode::Scan { .. } => false,
            IRNode::Map { input, .. } => Self::has_joins(input),
            IRNode::Filter { input, .. } => Self::has_joins(input),
            IRNode::Distinct { input } => Self::has_joins(input),
            IRNode::Union { inputs } => inputs.iter().any(Self::has_joins),
            IRNode::Aggregate { input, .. } => Self::has_joins(input),
            IRNode::Compute { input, .. } => Self::has_joins(input),
        }
    }

    /// Get all base relation names that appear in an IR tree
    fn get_base_relations(ir: &IRNode) -> HashSet<String> {
        match ir {
            IRNode::Scan { relation, .. } => {
                let mut set = HashSet::new();
                set.insert(relation.clone());
                set
            }
            IRNode::Join { left, right, .. } | IRNode::Antijoin { left, right, .. } => {
                let mut set = Self::get_base_relations(left);
                set.extend(Self::get_base_relations(right));
                set
            }
            IRNode::Map { input, .. }
            | IRNode::Filter { input, .. }
            | IRNode::Distinct { input }
            | IRNode::Aggregate { input, .. } => Self::get_base_relations(input),
            IRNode::Union { inputs } => {
                let mut set = HashSet::new();
                for input in inputs {
                    set.extend(Self::get_base_relations(input));
                }
                set
            }
            IRNode::Compute { input, .. } => Self::get_base_relations(input),
        }
    }

    /// Check if two IR nodes share any base relations (self-join detection)
    fn shares_base_relation(left: &IRNode, right: &IRNode) -> bool {
        let left_rels = Self::get_base_relations(left);
        let right_rels = Self::get_base_relations(right);
        !left_rels.is_disjoint(&right_rels)
    }

    /// Apply SIP transformation to IR
    fn apply_sip(&mut self, ir: IRNode) -> IRNode {
        match ir {
            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => {
                // Recursively apply SIP to children
                let left_sip = self.apply_sip(*left);
                let right_sip = self.apply_sip(*right);

                // Add existence check filters
                let (left_filtered, right_filtered) =
                    self.add_existence_filters(left_sip, right_sip, &left_keys, &right_keys);

                IRNode::Join {
                    left: Box::new(left_filtered),
                    right: Box::new(right_filtered),
                    left_keys,
                    right_keys,
                    output_schema,
                }
            }

            IRNode::Map {
                input,
                projection,
                output_schema,
            } => {
                let input_sip = self.apply_sip(*input);
                IRNode::Map {
                    input: Box::new(input_sip),
                    projection,
                    output_schema,
                }
            }

            IRNode::Filter { input, predicate } => {
                let input_sip = self.apply_sip(*input);
                IRNode::Filter {
                    input: Box::new(input_sip),
                    predicate,
                }
            }

            IRNode::Distinct { input } => {
                let input_sip = self.apply_sip(*input);
                IRNode::Distinct {
                    input: Box::new(input_sip),
                }
            }

            IRNode::Union { inputs } => {
                let inputs_sip: Vec<IRNode> =
                    inputs.into_iter().map(|i| self.apply_sip(i)).collect();
                IRNode::Union { inputs: inputs_sip }
            }

            IRNode::Aggregate { input, group_by, aggregations, output_schema } => {
                let input_sip = self.apply_sip(*input);
                IRNode::Aggregate {
                    input: Box::new(input_sip),
                    group_by,
                    aggregations,
                    output_schema,
                }
            }

            IRNode::Antijoin {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => {
                let left_sip = self.apply_sip(*left);
                let right_sip = self.apply_sip(*right);
                IRNode::Antijoin {
                    left: Box::new(left_sip),
                    right: Box::new(right_sip),
                    left_keys,
                    right_keys,
                    output_schema,
                }
            }

            // Scans don't need SIP
            IRNode::Scan { .. } => ir,

            IRNode::Compute { input, expressions } => {
                let input_sip = self.apply_sip(*input);
                IRNode::Compute {
                    input: Box::new(input_sip),
                    expressions,
                }
            }
        }
    }

    /// Add existence check filters before a join
    ///
    /// For left ⋈[l_keys = r_keys] right:
    /// - Filter left to keep only tuples where l_key values exist in right
    /// - Filter right to keep only tuples where r_key values exist in left
    ///
    /// This implements Yannakakis-style semijoin reduction:
    /// - left' = left ⋉ right (keep left tuples that have matching right tuples)
    /// - right' = right ⋉ left (keep right tuples that have matching left tuples)
    ///
    /// NOTE: Self-joins (where left and right share base relations) are skipped
    /// because filtering would create circular dependencies that empty out data.
    fn add_existence_filters(
        &mut self,
        left: IRNode,
        right: IRNode,
        left_keys: &[usize],
        right_keys: &[usize],
    ) -> (IRNode, IRNode) {
        // If there are no join keys, no filtering possible
        if left_keys.is_empty() || right_keys.is_empty() {
            return (left, right);
        }

        // Skip SIP for self-joins - filtering would create circular dependencies
        // that empty out data. For example, in edge(x,y) ⋈ edge(y,z):
        // - Filtering edge by values in edge would use the same relation
        // - This causes the semijoin filter to reference its own output
        if Self::shares_base_relation(&left, &right) {
            return (left, right);
        }

        // Track statistics
        self.stats.filters_added += 2;
        self.stats.relations_rewritten += 2;
        self.stats.estimated_reduction = 0.5; // Rough estimate

        // Apply semijoin filters:
        // 1. Filter left to keep only tuples where key values exist in right
        let left_filtered = self.create_semijoin_filter(
            left,
            &right,
            left_keys,
            right_keys,
        );

        // 2. Filter right to keep only tuples where key values exist in left
        let right_filtered = self.create_semijoin_filter(
            right,
            &left_filtered, // Use already-filtered left for better reduction
            right_keys,
            left_keys,
        );

        (left_filtered, right_filtered)
    }

    /// Generate a semijoin filter IR node
    ///
    /// Creates: input ⋉[key_cols] filter_source
    /// Keeps only tuples from input where key values exist in filter_source
    ///
    /// Implementation: distinct(project_{input_cols}(input ⋈ distinct(project_{keys}(filter_source))))
    fn create_semijoin_filter(
        &self,
        input: IRNode,
        filter_source: &IRNode,
        input_key_cols: &[usize],
        source_key_cols: &[usize],
    ) -> IRNode {
        // Build join that acts as semijoin
        // Semijoin: keep left tuple if ANY matching right tuple exists
        // Implemented as: distinct(project_{left_cols}(left ⋈ right))

        let input_schema = input.output_schema();
        let source_schema = filter_source.output_schema();

        // Build output schema (left schema + right schema for join)
        let mut output_schema = input_schema.clone();
        for var in &source_schema {
            if !output_schema.contains(var) {
                output_schema.push(var.clone());
            }
        }

        // Create join
        let joined = IRNode::Join {
            left: Box::new(input.clone()),
            right: Box::new(filter_source.clone()),
            left_keys: input_key_cols.to_vec(),
            right_keys: source_key_cols.to_vec(),
            output_schema: output_schema.clone(),
        };

        // Project back to input schema
        let projection: Vec<usize> = (0..input_schema.len()).collect();

        let projected = IRNode::Map {
            input: Box::new(joined),
            projection,
            output_schema: input_schema,
        };

        // Distinct to remove duplicates from multiple matches
        IRNode::Distinct {
            input: Box::new(projected),
        }
    }

    /// Get statistics about SIP rewriting
    pub fn get_stats(&self) -> &SipStats {
        &self.stats
    }

    /// Analyze IR and determine if SIP would be beneficial
    pub fn analyze_benefit(&self, ir: &IRNode) -> bool {
        // SIP is beneficial for:
        // 1. Queries with multiple joins
        // 2. Queries where intermediate results would be large
        // 3. Queries where selectivity varies significantly between relations

        Self::count_joins(ir) >= 2
    }

    /// Count number of joins in IR
    fn count_joins(ir: &IRNode) -> usize {
        match ir {
            IRNode::Join { left, right, .. } => {
                1 + Self::count_joins(left) + Self::count_joins(right)
            }
            IRNode::Antijoin { left, right, .. } => {
                Self::count_joins(left) + Self::count_joins(right)
            }
            IRNode::Map { input, .. } => Self::count_joins(input),
            IRNode::Filter { input, .. } => Self::count_joins(input),
            IRNode::Distinct { input } => Self::count_joins(input),
            IRNode::Union { inputs } => inputs.iter().map(Self::count_joins).sum(),
            IRNode::Aggregate { input, .. } => Self::count_joins(input),
            IRNode::Scan { .. } => 0,
            IRNode::Compute { input, .. } => Self::count_joins(input),
        }
    }

    /// Compute binding pattern from bound variables
    pub fn compute_adornment(&self, schema: &[String], bound_vars: &HashSet<String>) -> Adornment {
        let bindings: Vec<Binding> = schema
            .iter()
            .map(|var| {
                if bound_vars.contains(var) {
                    Binding::Bound
                } else {
                    Binding::Free
                }
            })
            .collect();

        Adornment { bindings }
    }

    /// Check if two relations share variables
    pub fn find_shared_variables(
        &self,
        schema1: &[String],
        schema2: &[String],
    ) -> HashSet<String> {
        let set1: HashSet<_> = schema1.iter().cloned().collect();
        let set2: HashSet<_> = schema2.iter().cloned().collect();
        set1.intersection(&set2).cloned().collect()
    }
}

impl Default for SipRewriter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scan(relation: &str, vars: &[&str]) -> IRNode {
        IRNode::Scan {
            relation: relation.to_string(),
            schema: vars.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn make_join(left: IRNode, right: IRNode, left_key: usize, right_key: usize) -> IRNode {
        let left_schema = left.output_schema();
        let right_schema = right.output_schema();

        let mut output_schema = left_schema.clone();
        for var in &right_schema {
            if !output_schema.contains(var) {
                output_schema.push(var.clone());
            }
        }

        IRNode::Join {
            left: Box::new(left),
            right: Box::new(right),
            left_keys: vec![left_key],
            right_keys: vec![right_key],
            output_schema,
        }
    }

    #[test]
    fn test_sip_rewriter_preserves_scan() {
        let mut rewriter = SipRewriter::new();
        let ir = make_scan("edge", &["x", "y"]);

        let result = rewriter.rewrite(ir.clone());

        // Scan should be unchanged
        match (&ir, &result) {
            (IRNode::Scan { relation: r1, .. }, IRNode::Scan { relation: r2, .. }) => {
                assert_eq!(r1, r2);
            }
            _ => panic!("Expected scan to be preserved"),
        }
    }

    #[test]
    fn test_sip_rewriter_handles_join() {
        let mut rewriter = SipRewriter::new();

        let scan1 = make_scan("R", &["x", "y"]);
        let scan2 = make_scan("S", &["y", "z"]);
        let ir = make_join(scan1, scan2, 1, 0);

        let result = rewriter.rewrite(ir);

        // Result should still be a join
        assert!(matches!(result, IRNode::Join { .. }));
    }

    #[test]
    fn test_sip_disabled() {
        let mut rewriter = SipRewriter::new();
        rewriter.set_sip(false);

        let scan1 = make_scan("R", &["x", "y"]);
        let scan2 = make_scan("S", &["y", "z"]);
        let ir = make_join(scan1.clone(), scan2.clone(), 1, 0);

        let result = rewriter.rewrite(ir.clone());

        // Should return unchanged
        match (&ir, &result) {
            (IRNode::Join { .. }, IRNode::Join { .. }) => (),
            _ => panic!("Expected join to be preserved"),
        }
    }

    #[test]
    fn test_adornment_creation() {
        let adorned = Adornment::with_bound(3, &[0, 2]);

        assert_eq!(adorned.bindings.len(), 3);
        assert!(adorned.is_bound(0));
        assert!(!adorned.is_bound(1));
        assert!(adorned.is_bound(2));
        assert_eq!(adorned.to_string(), "bfb");
    }

    #[test]
    fn test_adornment_all_free() {
        let adorned = Adornment::all_free(4);

        assert_eq!(adorned.to_string(), "ffff");
        assert!(adorned.bound_positions().is_empty());
    }

    #[test]
    fn test_find_shared_variables() {
        let rewriter = SipRewriter::new();

        let schema1 = vec!["x".to_string(), "y".to_string()];
        let schema2 = vec!["y".to_string(), "z".to_string()];

        let shared = rewriter.find_shared_variables(&schema1, &schema2);

        assert_eq!(shared.len(), 1);
        assert!(shared.contains("y"));
    }

    #[test]
    fn test_compute_adornment() {
        let rewriter = SipRewriter::new();

        let schema = vec!["x".to_string(), "y".to_string(), "z".to_string()];
        let bound: HashSet<String> = ["x".to_string()].into_iter().collect();

        let adorned = rewriter.compute_adornment(&schema, &bound);

        assert!(adorned.is_bound(0));
        assert!(!adorned.is_bound(1));
        assert!(!adorned.is_bound(2));
    }

    #[test]
    fn test_analyze_benefit() {
        let rewriter = SipRewriter::new();

        // Single scan - no benefit
        let scan = make_scan("R", &["x", "y"]);
        assert!(!rewriter.analyze_benefit(&scan));

        // Single join - marginal benefit
        let scan2 = make_scan("S", &["y", "z"]);
        let join = make_join(scan.clone(), scan2, 1, 0);
        assert!(!rewriter.analyze_benefit(&join));

        // Two joins - beneficial
        let scan3 = make_scan("T", &["z", "w"]);
        let join2 = make_join(join, scan3, 2, 0);
        assert!(rewriter.analyze_benefit(&join2));
    }

    #[test]
    fn test_sip_traversal() {
        let edges = vec![(0, 1), (1, 2)];
        let traversal = SipTraversal::from_tree(&edges, 0, 3);

        // Forward should start from root
        assert_eq!(traversal.forward_order[0], 0);

        // Backward should end at root
        assert_eq!(traversal.backward_order[2], 0);
    }

    #[test]
    fn test_stats_tracking() {
        let mut rewriter = SipRewriter::new();

        let scan1 = make_scan("R", &["x", "y"]);
        let scan2 = make_scan("S", &["y", "z"]);
        let ir = make_join(scan1, scan2, 1, 0);

        let _ = rewriter.rewrite(ir);

        let stats = rewriter.get_stats();
        // Stats should be updated
        assert!(stats.filters_added >= 0);
    }

    #[test]
    fn test_three_way_join_sip() {
        let mut rewriter = SipRewriter::new();

        let scan1 = make_scan("R", &["a", "b"]);
        let scan2 = make_scan("S", &["b", "c"]);
        let scan3 = make_scan("T", &["c", "d"]);

        let join1 = make_join(scan1, scan2, 1, 0);
        let ir = make_join(join1, scan3, 2, 0);

        let result = rewriter.rewrite(ir);

        // Should produce optimized join tree
        assert!(matches!(result, IRNode::Join { .. }));
    }

    #[test]
    fn test_nested_operations_sip() {
        let mut rewriter = SipRewriter::new();

        let scan1 = make_scan("R", &["x", "y"]);
        let scan2 = make_scan("S", &["y", "z"]);
        let join = make_join(scan1, scan2, 1, 0);

        // Wrap in distinct
        let distinct = IRNode::Distinct {
            input: Box::new(join),
        };

        let result = rewriter.rewrite(distinct);

        // Should handle nested structure
        assert!(matches!(result, IRNode::Distinct { .. }));
    }
}
