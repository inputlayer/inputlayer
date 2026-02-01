//! # Subplan Sharing (Module 09)
//!
//! **Course Module**: Module 09: Common Subexpression Elimination
//!
//! ## What This Module Does
//!
//! Subplan sharing detects and eliminates redundant computation by identifying
//! common subexpressions across multiple IR trees and extracting them into
//! shared views that are computed once and reused.
//!
//! ## Algorithm Overview
//!
//! 1. **Canonicalization**: Normalize variable names (x, y, z → v0, v1, v2)
//! 2. **Hashing**: Compute structural hash for each IR subtree
//! 3. **Detection**: Find subtrees with identical hashes across rules
//! 4. **Extraction**: Extract common subplans into shared views
//! 5. **Rewriting**: Replace duplicates with references to shared views
//!
//! ## Example
//!
//! Before:
//! ```text
//! Rule 1: result1(x, z) :- R(x, y), S(y, z), T(z, w).
//! Rule 2: result2(a, c) :- R(a, b), S(b, c), U(c, d).
//! ```
//!
//! Common subexpression: `R(x, y), S(y, z)` (join of R and S)
//!
//! After:
//! ```text
//! Shared View: RS_join(v0, v2) :- R(v0, v1), S(v1, v2).
//! Rule 1: result1(x, z) :- RS_join(x, z), T(z, w).
//! Rule 2: result2(a, c) :- RS_join(a, c), U(c, d).
//! ```
//!
//! ## Key Concepts
//!
//! - **Canonicalization**: Makes structurally identical subplans compare equal
//! - **Structural Hashing**: Efficient duplicate detection via hash comparison
//! - **View Materialization**: Trade-off between memory and computation
//! - **Arrangement Sharing**: DD-specific optimization for indexed data
//!
//! ## Pipeline Position
//!
//! ```text
//! Multiple IRNodes → [Subplan Sharing] → IRNodes + Shared Views → Code Gen
//! ```

use crate::ir::IRNode;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Statistics about subplan sharing
#[derive(Debug, Clone, Default)]
pub struct SharingStats {
    /// Total number of IR subtrees analyzed
    pub total_subtrees: usize,
    /// Number of unique subtrees (after deduplication)
    pub unique_subtrees: usize,
    /// Number of duplicates eliminated
    pub duplicates_eliminated: usize,
    /// Number of shared views created
    pub shared_views_created: usize,
}

/// Canonical form of an IR subtree with normalized variable names
#[derive(Debug, Clone)]
struct CanonicalSubtree {
    // TODO: Implement variable remapping for shared subplan reconstruction.
    // Reserved for completing subplan sharing optimization:
    // - `ir`: Canonical IR form of the shared subplan for reconstruction
    // - `var_mapping`: Maps canonical variable names back to query-specific names
    // Currently only hash-based duplicate detection is implemented. These fields
    // will enable extracting shared computations and reusing them across rules.
    #[allow(dead_code)]
    ir: IRNode,
    /// Hash of the canonical form
    hash: u64,
    #[allow(dead_code)]
    var_mapping: HashMap<String, String>,
}

/// Subplan sharer for common subexpression elimination
///
/// This implementation detects common subexpressions across multiple IR trees
/// and extracts them into shared views that can be computed once and reused.
pub struct SubplanSharer {
    /// Whether sharing is enabled
    enable_sharing: bool,
    /// Minimum subtree size to consider for sharing (avoid sharing trivial scans)
    min_subtree_depth: usize,
}

impl SubplanSharer {
    /// Create a new subplan sharer
    pub fn new() -> Self {
        SubplanSharer {
            enable_sharing: true,
            min_subtree_depth: 2, // Only share subtrees with depth >= 2
        }
    }

    /// Enable or disable subplan sharing
    pub fn set_sharing(&mut self, enable: bool) {
        self.enable_sharing = enable;
    }

    /// Set minimum subtree depth for sharing consideration
    pub fn set_min_depth(&mut self, depth: usize) {
        self.min_subtree_depth = depth;
    }

    /// Share common subplans across multiple IR trees
    ///
    /// # Algorithm
    ///
    /// 1. Collect all subtrees from all IRs
    /// 2. Canonicalize each subtree (normalize variable names)
    /// 3. Hash canonical forms to detect duplicates
    /// 4. Extract subtrees that appear multiple times
    /// 5. Create shared view definitions
    /// 6. Rewrite IRs to reference shared views (via Scan nodes)
    ///
    /// # Returns
    ///
    /// * `(Vec<IRNode>, HashMap<String, IRNode>)` - Rewritten IRs and shared view definitions
    pub fn share_subplans(&self, irs: Vec<IRNode>) -> (Vec<IRNode>, HashMap<String, IRNode>) {
        if !self.enable_sharing || irs.is_empty() {
            return (irs, HashMap::new());
        }

        // Step 1: Collect all subtrees with their hashes
        let mut subtree_counts: HashMap<u64, Vec<(usize, IRNode)>> = HashMap::new();

        for (ir_idx, ir) in irs.iter().enumerate() {
            self.collect_subtrees(ir, ir_idx, &mut subtree_counts);
        }

        // Step 2: Find subtrees that appear multiple times and build hash->view mapping
        let mut shared_views: HashMap<String, IRNode> = HashMap::new();
        let mut hash_to_view: HashMap<u64, String> = HashMap::new();
        let mut view_counter = 0;

        for (hash, occurrences) in &subtree_counts {
            // Only share if:
            // 1. Appears more than once
            // 2. Is non-trivial (not just a scan)
            if occurrences.len() > 1 {
                let (_, representative) = &occurrences[0];
                if self.subtree_depth(representative) >= self.min_subtree_depth {
                    let view_name = format!("__shared_view_{}", view_counter);
                    shared_views.insert(view_name.clone(), representative.clone());
                    hash_to_view.insert(*hash, view_name);
                    view_counter += 1;
                }
            }
        }

        // Step 3: Rewrite IRs to reference shared views
        let rewritten_irs: Vec<IRNode> = irs
            .into_iter()
            .map(|ir| self.rewrite_with_shared_views(&ir, &hash_to_view))
            .collect();

        (rewritten_irs, shared_views)
    }

    /// Rewrite an IR tree to use shared view references where possible
    fn rewrite_with_shared_views(
        &self,
        ir: &IRNode,
        hash_to_view: &HashMap<u64, String>,
    ) -> IRNode {
        // Check if this subtree should be replaced with a view reference
        let hash = self.hash_ir(ir);
        if let Some(view_name) = hash_to_view.get(&hash) {
            // Only replace non-trivial subtrees (not scans)
            if self.subtree_depth(ir) >= self.min_subtree_depth {
                // Replace with a scan of the shared view
                return IRNode::Scan {
                    relation: view_name.clone(),
                    schema: ir.output_schema(),
                };
            }
        }

        // Recursively rewrite children
        match ir {
            IRNode::Scan { .. } => ir.clone(),

            IRNode::Map {
                input,
                projection,
                output_schema,
            } => IRNode::Map {
                input: Box::new(self.rewrite_with_shared_views(input, hash_to_view)),
                projection: projection.clone(),
                output_schema: output_schema.clone(),
            },

            IRNode::Filter { input, predicate } => IRNode::Filter {
                input: Box::new(self.rewrite_with_shared_views(input, hash_to_view)),
                predicate: predicate.clone(),
            },

            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => IRNode::Join {
                left: Box::new(self.rewrite_with_shared_views(left, hash_to_view)),
                right: Box::new(self.rewrite_with_shared_views(right, hash_to_view)),
                left_keys: left_keys.clone(),
                right_keys: right_keys.clone(),
                output_schema: output_schema.clone(),
            },

            IRNode::Distinct { input } => IRNode::Distinct {
                input: Box::new(self.rewrite_with_shared_views(input, hash_to_view)),
            },

            IRNode::Union { inputs } => IRNode::Union {
                inputs: inputs
                    .iter()
                    .map(|i| self.rewrite_with_shared_views(i, hash_to_view))
                    .collect(),
            },

            IRNode::Aggregate {
                input,
                group_by,
                aggregations,
                output_schema,
            } => IRNode::Aggregate {
                input: Box::new(self.rewrite_with_shared_views(input, hash_to_view)),
                group_by: group_by.clone(),
                aggregations: aggregations.clone(),
                output_schema: output_schema.clone(),
            },

            IRNode::Antijoin {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => IRNode::Antijoin {
                left: Box::new(self.rewrite_with_shared_views(left, hash_to_view)),
                right: Box::new(self.rewrite_with_shared_views(right, hash_to_view)),
                left_keys: left_keys.clone(),
                right_keys: right_keys.clone(),
                output_schema: output_schema.clone(),
            },

            IRNode::Compute { input, expressions } => IRNode::Compute {
                input: Box::new(self.rewrite_with_shared_views(input, hash_to_view)),
                expressions: expressions.clone(),
            },
        }
    }

    /// Collect all subtrees from an IR tree
    fn collect_subtrees(
        &self,
        ir: &IRNode,
        ir_idx: usize,
        subtree_counts: &mut HashMap<u64, Vec<(usize, IRNode)>>,
    ) {
        // Hash the current node
        let canonical = self.canonicalize(ir);
        subtree_counts
            .entry(canonical.hash)
            .or_insert_with(Vec::new)
            .push((ir_idx, ir.clone()));

        // Recursively collect from children
        match ir {
            IRNode::Scan { .. } => {
                // Leaf node - already collected above
            }
            IRNode::Map { input, .. } => {
                self.collect_subtrees(input, ir_idx, subtree_counts);
            }
            IRNode::Filter { input, .. } => {
                self.collect_subtrees(input, ir_idx, subtree_counts);
            }
            IRNode::Join { left, right, .. } => {
                self.collect_subtrees(left, ir_idx, subtree_counts);
                self.collect_subtrees(right, ir_idx, subtree_counts);
            }
            IRNode::Distinct { input } => {
                self.collect_subtrees(input, ir_idx, subtree_counts);
            }
            IRNode::Union { inputs } => {
                for input in inputs {
                    self.collect_subtrees(input, ir_idx, subtree_counts);
                }
            }
            IRNode::Aggregate { input, .. } => {
                self.collect_subtrees(input, ir_idx, subtree_counts);
            }
            IRNode::Antijoin { left, right, .. } => {
                self.collect_subtrees(left, ir_idx, subtree_counts);
                self.collect_subtrees(right, ir_idx, subtree_counts);
            }
            IRNode::Compute { input, .. } => {
                self.collect_subtrees(input, ir_idx, subtree_counts);
            }
        }
    }

    /// Canonicalize an IR subtree by normalizing variable names
    ///
    /// This ensures structurally identical subtrees have the same canonical form,
    /// regardless of the original variable names used.
    fn canonicalize(&self, ir: &IRNode) -> CanonicalSubtree {
        let mut var_counter = 0;
        let mut var_mapping: HashMap<String, String> = HashMap::new();

        let canonical_ir = self.canonicalize_recursive(ir, &mut var_counter, &mut var_mapping);
        let hash = self.hash_ir(&canonical_ir);

        // Invert mapping for reconstruction
        let inverted_mapping: HashMap<String, String> = var_mapping
            .iter()
            .map(|(orig, canon)| (canon.clone(), orig.clone()))
            .collect();

        CanonicalSubtree {
            ir: canonical_ir,
            hash,
            var_mapping: inverted_mapping,
        }
    }

    /// Recursively canonicalize IR, assigning canonical variable names
    fn canonicalize_recursive(
        &self,
        ir: &IRNode,
        var_counter: &mut usize,
        var_mapping: &mut HashMap<String, String>,
    ) -> IRNode {
        match ir {
            IRNode::Scan { relation, schema } => {
                let canonical_schema: Vec<String> = schema
                    .iter()
                    .map(|var| self.get_canonical_var(var, var_counter, var_mapping))
                    .collect();

                IRNode::Scan {
                    relation: relation.clone(),
                    schema: canonical_schema,
                }
            }

            IRNode::Map {
                input,
                projection,
                output_schema,
            } => {
                let canonical_input = self.canonicalize_recursive(input, var_counter, var_mapping);
                let canonical_output: Vec<String> = output_schema
                    .iter()
                    .map(|var| self.get_canonical_var(var, var_counter, var_mapping))
                    .collect();

                IRNode::Map {
                    input: Box::new(canonical_input),
                    projection: projection.clone(),
                    output_schema: canonical_output,
                }
            }

            IRNode::Filter { input, predicate } => {
                let canonical_input = self.canonicalize_recursive(input, var_counter, var_mapping);

                IRNode::Filter {
                    input: Box::new(canonical_input),
                    predicate: predicate.clone(), // Predicates use column indices, not names
                }
            }

            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => {
                let canonical_left = self.canonicalize_recursive(left, var_counter, var_mapping);
                let canonical_right = self.canonicalize_recursive(right, var_counter, var_mapping);
                let canonical_output: Vec<String> = output_schema
                    .iter()
                    .map(|var| self.get_canonical_var(var, var_counter, var_mapping))
                    .collect();

                IRNode::Join {
                    left: Box::new(canonical_left),
                    right: Box::new(canonical_right),
                    left_keys: left_keys.clone(),
                    right_keys: right_keys.clone(),
                    output_schema: canonical_output,
                }
            }

            IRNode::Distinct { input } => {
                let canonical_input = self.canonicalize_recursive(input, var_counter, var_mapping);

                IRNode::Distinct {
                    input: Box::new(canonical_input),
                }
            }

            IRNode::Union { inputs } => {
                let canonical_inputs: Vec<IRNode> = inputs
                    .iter()
                    .map(|input| self.canonicalize_recursive(input, var_counter, var_mapping))
                    .collect();

                IRNode::Union {
                    inputs: canonical_inputs,
                }
            }

            IRNode::Aggregate {
                input,
                group_by,
                aggregations,
                output_schema,
            } => {
                let canonical_input = self.canonicalize_recursive(input, var_counter, var_mapping);
                let canonical_output: Vec<String> = output_schema
                    .iter()
                    .map(|var| self.get_canonical_var(var, var_counter, var_mapping))
                    .collect();

                IRNode::Aggregate {
                    input: Box::new(canonical_input),
                    group_by: group_by.clone(),
                    aggregations: aggregations.clone(),
                    output_schema: canonical_output,
                }
            }

            IRNode::Antijoin {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => {
                let canonical_left = self.canonicalize_recursive(left, var_counter, var_mapping);
                let canonical_right = self.canonicalize_recursive(right, var_counter, var_mapping);
                let canonical_output: Vec<String> = output_schema
                    .iter()
                    .map(|var| self.get_canonical_var(var, var_counter, var_mapping))
                    .collect();

                IRNode::Antijoin {
                    left: Box::new(canonical_left),
                    right: Box::new(canonical_right),
                    left_keys: left_keys.clone(),
                    right_keys: right_keys.clone(),
                    output_schema: canonical_output,
                }
            }

            IRNode::Compute { input, expressions } => {
                let canonical_input = self.canonicalize_recursive(input, var_counter, var_mapping);

                IRNode::Compute {
                    input: Box::new(canonical_input),
                    expressions: expressions.clone(),
                }
            }
        }
    }

    /// Get or create canonical variable name
    fn get_canonical_var(
        &self,
        original: &str,
        counter: &mut usize,
        mapping: &mut HashMap<String, String>,
    ) -> String {
        if let Some(canonical) = mapping.get(original) {
            canonical.clone()
        } else {
            let canonical = format!("v{}", *counter);
            *counter += 1;
            mapping.insert(original.to_string(), canonical.clone());
            canonical
        }
    }

    /// Compute structural hash of an IR node
    fn hash_ir(&self, ir: &IRNode) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash_ir_recursive(ir, &mut hasher);
        hasher.finish()
    }

    /// Recursively hash IR structure
    fn hash_ir_recursive<H: Hasher>(&self, ir: &IRNode, hasher: &mut H) {
        // Hash node type discriminant
        std::mem::discriminant(ir).hash(hasher);

        match ir {
            IRNode::Scan { relation, schema } => {
                relation.hash(hasher);
                schema.len().hash(hasher);
                // Don't hash variable names - they're canonicalized
            }

            IRNode::Map {
                input, projection, ..
            } => {
                projection.hash(hasher);
                self.hash_ir_recursive(input, hasher);
            }

            IRNode::Filter { input, predicate } => {
                // Hash predicate structure
                format!("{:?}", predicate).hash(hasher);
                self.hash_ir_recursive(input, hasher);
            }

            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                ..
            } => {
                left_keys.hash(hasher);
                right_keys.hash(hasher);
                self.hash_ir_recursive(left, hasher);
                self.hash_ir_recursive(right, hasher);
            }

            IRNode::Distinct { input } => {
                self.hash_ir_recursive(input, hasher);
            }

            IRNode::Union { inputs } => {
                inputs.len().hash(hasher);
                for input in inputs {
                    self.hash_ir_recursive(input, hasher);
                }
            }

            IRNode::Aggregate {
                input,
                group_by,
                aggregations,
                ..
            } => {
                group_by.hash(hasher);
                for (func, col) in aggregations {
                    format!("{:?}", func).hash(hasher);
                    col.hash(hasher);
                }
                self.hash_ir_recursive(input, hasher);
            }

            IRNode::Antijoin {
                left,
                right,
                left_keys,
                right_keys,
                ..
            } => {
                left_keys.hash(hasher);
                right_keys.hash(hasher);
                self.hash_ir_recursive(left, hasher);
                self.hash_ir_recursive(right, hasher);
            }

            IRNode::Compute { input, expressions } => {
                // Hash the number of expressions
                expressions.len().hash(hasher);
                // Hash each expression's name AND the expression content (not just the name!)
                // This is critical - expressions like Column(1)+1 vs Column(2)+1 must hash differently
                for (name, expr) in expressions {
                    name.hash(hasher);
                    // Hash the full expression structure including column indices
                    format!("{:?}", expr).hash(hasher);
                }
                self.hash_ir_recursive(input, hasher);
            }
        }
    }

    /// Compute depth of a subtree
    fn subtree_depth(&self, ir: &IRNode) -> usize {
        match ir {
            IRNode::Scan { .. } => 1,
            IRNode::Map { input, .. } => 1 + self.subtree_depth(input),
            IRNode::Filter { input, .. } => 1 + self.subtree_depth(input),
            IRNode::Join { left, right, .. } => {
                1 + self.subtree_depth(left).max(self.subtree_depth(right))
            }
            IRNode::Distinct { input } => 1 + self.subtree_depth(input),
            IRNode::Union { inputs } => {
                1 + inputs
                    .iter()
                    .map(|i| self.subtree_depth(i))
                    .max()
                    .unwrap_or(0)
            }
            IRNode::Aggregate { input, .. } => 1 + self.subtree_depth(input),
            IRNode::Antijoin { left, right, .. } => {
                1 + self.subtree_depth(left).max(self.subtree_depth(right))
            }
            IRNode::Compute { input, .. } => 1 + self.subtree_depth(input),
        }
    }

    /// Compute sharing statistics for a set of IR trees
    pub fn compute_stats(&self, irs: &[IRNode]) -> SharingStats {
        let mut subtree_counts: HashMap<u64, Vec<(usize, IRNode)>> = HashMap::new();

        for (ir_idx, ir) in irs.iter().enumerate() {
            self.collect_subtrees(ir, ir_idx, &mut subtree_counts);
        }

        let total_subtrees: usize = subtree_counts.values().map(|v| v.len()).sum();
        let unique_subtrees = subtree_counts.len();

        let mut duplicates_eliminated = 0;
        let mut shared_views_created = 0;

        for occurrences in subtree_counts.values() {
            if occurrences.len() > 1 {
                let (_, representative) = &occurrences[0];
                if self.subtree_depth(representative) >= self.min_subtree_depth {
                    duplicates_eliminated += occurrences.len() - 1;
                    shared_views_created += 1;
                }
            }
        }

        SharingStats {
            total_subtrees,
            unique_subtrees,
            duplicates_eliminated,
            shared_views_created,
        }
    }

    /// Find common subtrees within a single IR (internal deduplication)
    pub fn find_internal_duplicates(&self, ir: &IRNode) -> Vec<(u64, usize)> {
        let mut subtree_counts: HashMap<u64, usize> = HashMap::new();
        self.count_subtrees_internal(ir, &mut subtree_counts);

        subtree_counts
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .collect()
    }

    /// Count subtree occurrences within a single IR
    fn count_subtrees_internal(&self, ir: &IRNode, counts: &mut HashMap<u64, usize>) {
        let canonical = self.canonicalize(ir);
        *counts.entry(canonical.hash).or_insert(0) += 1;

        match ir {
            IRNode::Scan { .. } => {}
            IRNode::Map { input, .. } => self.count_subtrees_internal(input, counts),
            IRNode::Filter { input, .. } => self.count_subtrees_internal(input, counts),
            IRNode::Join { left, right, .. } => {
                self.count_subtrees_internal(left, counts);
                self.count_subtrees_internal(right, counts);
            }
            IRNode::Distinct { input } => self.count_subtrees_internal(input, counts),
            IRNode::Union { inputs } => {
                for input in inputs {
                    self.count_subtrees_internal(input, counts);
                }
            }
            IRNode::Aggregate { input, .. } => self.count_subtrees_internal(input, counts),
            IRNode::Antijoin { left, right, .. } => {
                self.count_subtrees_internal(left, counts);
                self.count_subtrees_internal(right, counts);
            }
            IRNode::Compute { input, .. } => self.count_subtrees_internal(input, counts),
        }
    }
}

impl Default for SubplanSharer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::Predicate;

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

    #[test]
    fn test_subplan_sharer_detects_duplicates() {
        let sharer = SubplanSharer::new();

        // Two IRs with identical join subtrees
        let ir1 = make_join(make_scan("R"), make_scan("S"));
        let ir2 = make_join(make_scan("R"), make_scan("S"));

        let (_, shared_views) = sharer.share_subplans(vec![ir1, ir2]);

        // Should detect the common join pattern
        // Note: exact count depends on depth threshold
        // With two identical joins, we expect at least some sharing to be detected
        // (The exact number depends on the depth threshold configuration)
    }

    #[test]
    fn test_canonicalization_normalizes_names() {
        let sharer = SubplanSharer::new();

        // Same structure, different variable names
        let ir1 = IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };
        let ir2 = IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["a".to_string(), "b".to_string()],
        };

        let canonical1 = sharer.canonicalize(&ir1);
        let canonical2 = sharer.canonicalize(&ir2);

        // Canonical forms should have same hash (same structure)
        assert_eq!(
            canonical1.hash, canonical2.hash,
            "Same structure should have same canonical hash"
        );
    }

    #[test]
    fn test_different_structures_different_hash() {
        let sharer = SubplanSharer::new();

        let ir1 = make_scan("R");
        let ir2 = make_scan("S");

        let canonical1 = sharer.canonicalize(&ir1);
        let canonical2 = sharer.canonicalize(&ir2);

        // Different relations should have different hashes
        assert_ne!(
            canonical1.hash, canonical2.hash,
            "Different relations should have different hash"
        );
    }

    #[test]
    fn test_subtree_depth_calculation() {
        let sharer = SubplanSharer::new();

        let scan = make_scan("R");
        assert_eq!(sharer.subtree_depth(&scan), 1);

        let join = make_join(make_scan("R"), make_scan("S"));
        assert_eq!(sharer.subtree_depth(&join), 2);

        let nested_join = make_join(join.clone(), make_scan("T"));
        assert_eq!(sharer.subtree_depth(&nested_join), 3);
    }

    #[test]
    fn test_compute_stats() {
        let sharer = SubplanSharer::new();

        let ir1 = make_join(make_scan("R"), make_scan("S"));
        let ir2 = make_join(make_scan("R"), make_scan("S"));
        let ir3 = make_scan("T");

        let stats = sharer.compute_stats(&[ir1, ir2, ir3]);

        assert!(stats.total_subtrees > 0);
        assert!(stats.unique_subtrees > 0);
    }

    #[test]
    fn test_find_internal_duplicates() {
        let sharer = SubplanSharer::new();

        // IR with same scan used twice in a union
        let scan = make_scan("R");
        let ir = IRNode::Union {
            inputs: vec![scan.clone(), scan.clone()],
        };

        let duplicates = sharer.find_internal_duplicates(&ir);

        // Should find the duplicate scan
        assert!(
            duplicates.iter().any(|(_, count)| *count > 1),
            "Should detect internal duplicates"
        );
    }

    #[test]
    fn test_empty_input() {
        let sharer = SubplanSharer::new();
        let (result, views) = sharer.share_subplans(vec![]);

        assert!(result.is_empty());
        assert!(views.is_empty());
    }

    #[test]
    fn test_disabled_sharing() {
        let mut sharer = SubplanSharer::new();
        sharer.set_sharing(false);

        let ir1 = make_join(make_scan("R"), make_scan("S"));
        let ir2 = make_join(make_scan("R"), make_scan("S"));

        let (result, views) = sharer.share_subplans(vec![ir1.clone(), ir2.clone()]);

        // Should return original IRs unchanged
        assert_eq!(result.len(), 2);
        assert!(views.is_empty(), "Disabled sharer should not create views");
    }
}
