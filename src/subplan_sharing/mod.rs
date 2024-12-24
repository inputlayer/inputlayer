//! # Subplan Sharing (CSE)
//!
//! Detects common subexpressions across IR trees and extracts them into
//! shared views computed once and reused.
//!
//! Algorithm: canonicalize variable names -> structural hash -> detect duplicates
//! -> extract shared views -> rewrite rules to reference them.
//!
//! Example: if `R(x,y) JOIN S(y,z)` appears in two rules, extract it as a
//! shared view `RS_join(v0, v2)` that both rules scan instead.
//!
//! ```text
//! Multiple IRNodes -> [Subplan Sharing] -> IRNodes + Shared Views -> Code Gen
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
    /// Canonical IR form with normalized variable names (v0, v1, v2, ...)
    /// Used for structural comparison. The hash is computed from this form.
    #[allow(dead_code)]
    ir: IRNode,
    /// Hash of the canonical form (ignores variable names, captures structure only)
    hash: u64,
    /// Column name -> canonical name (debug only; code gen uses column indices).
    #[allow(dead_code)]
    var_mapping: HashMap<String, String>,
}

/// Detects common subexpressions across IR trees and extracts shared views.
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

    /// Find common subtrees, extract as shared views, rewrite IRs to scan them.
    ///
    /// `derived_relations` are excluded - shared views execute before rule results
    /// are populated, so scanning a derived relation would read stale/empty data.
    pub fn share_subplans(
        &self,
        irs: Vec<IRNode>,
        derived_relations: &std::collections::HashSet<String>,
    ) -> (Vec<IRNode>, HashMap<String, IRNode>) {
        if !self.enable_sharing || irs.is_empty() {
            return (irs, HashMap::new());
        }

        // Collect all subtrees with their hashes
        let mut subtree_counts: HashMap<u64, Vec<(usize, IRNode)>> = HashMap::new();

        for (ir_idx, ir) in irs.iter().enumerate() {
            self.collect_subtrees(ir, ir_idx, &mut subtree_counts);
        }

        // Find subtrees that appear multiple times and build hash->view mapping
        let mut shared_views: HashMap<String, IRNode> = HashMap::new();
        let mut hash_to_view: HashMap<u64, String> = HashMap::new();
        let mut view_counter = 0;

        for (hash, occurrences) in &subtree_counts {
            // Only share if:
            // 1. Appears more than once
            // 2. Is non-trivial (not just a scan)
            // 3. Does NOT reference any derived relation (which would be empty at shared view execution time)
            if occurrences.len() > 1 {
                let (_, representative) = &occurrences[0];
                if self.subtree_depth(representative) >= self.min_subtree_depth
                    && !Self::references_derived_relation(representative, derived_relations)
                {
                    let view_name = format!("__shared_view_{view_counter}");
                    shared_views.insert(view_name.clone(), representative.clone());
                    hash_to_view.insert(*hash, view_name);
                    view_counter += 1;
                }
            }
        }

        // Rewrite shared views to reference each other (cascading sharing).
        // A deep view may contain a subtree that's itself a shared view.
        // Process shallowest-first so deep views can replace their subtrees with
        // scans of already-finalized shallow views. Each view excludes its own hash
        // to prevent self-references.
        let mut view_names_by_depth: Vec<(String, u64, usize)> = shared_views
            .iter()
            .map(|(name, ir)| (name.clone(), self.hash_ir(ir), self.subtree_depth(ir)))
            .collect();
        view_names_by_depth.sort_by_key(|(_, _, depth)| *depth);

        for (name, own_hash, _) in &view_names_by_depth {
            if let Some(ir) = shared_views.remove(name) {
                // Create a filtered hash_to_view that excludes this view's own hash
                let filtered: HashMap<u64, String> = hash_to_view
                    .iter()
                    .filter(|(h, _)| *h != own_hash)
                    .map(|(h, v)| (*h, v.clone()))
                    .collect();
                let rewritten = self.rewrite_with_shared_views(&ir, &filtered);
                shared_views.insert(name.clone(), rewritten);
            }
        }

        // Rewrite original IRs to reference shared views
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

            IRNode::HnswScan { .. } => ir.clone(), // Terminal node

            IRNode::FlatMap {
                input,
                projection,
                filter_predicate,
                output_schema,
            } => IRNode::FlatMap {
                input: Box::new(self.rewrite_with_shared_views(input, hash_to_view)),
                projection: projection.clone(),
                filter_predicate: filter_predicate.clone(),
                output_schema: output_schema.clone(),
            },

            IRNode::JoinFlatMap {
                left,
                right,
                left_keys,
                right_keys,
                projection,
                filter_predicate,
                output_schema,
            } => IRNode::JoinFlatMap {
                left: Box::new(self.rewrite_with_shared_views(left, hash_to_view)),
                right: Box::new(self.rewrite_with_shared_views(right, hash_to_view)),
                left_keys: left_keys.clone(),
                right_keys: right_keys.clone(),
                projection: projection.clone(),
                filter_predicate: filter_predicate.clone(),
                output_schema: output_schema.clone(),
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
            .or_default()
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
            IRNode::HnswScan { .. } => {
                // Terminal node - already collected above
            }
            IRNode::FlatMap { input, .. } => {
                self.collect_subtrees(input, ir_idx, subtree_counts);
            }
            IRNode::JoinFlatMap { left, right, .. } => {
                self.collect_subtrees(left, ir_idx, subtree_counts);
                self.collect_subtrees(right, ir_idx, subtree_counts);
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

            IRNode::HnswScan {
                index_name,
                query,
                k,
                ef_search,
                output_schema,
            } => {
                let canonical_output: Vec<String> = output_schema
                    .iter()
                    .map(|var| self.get_canonical_var(var, var_counter, var_mapping))
                    .collect();

                IRNode::HnswScan {
                    index_name: index_name.clone(),
                    query: query.clone(),
                    k: *k,
                    ef_search: *ef_search,
                    output_schema: canonical_output,
                }
            }

            IRNode::FlatMap {
                input,
                projection,
                filter_predicate,
                output_schema,
            } => {
                let canonical_input = self.canonicalize_recursive(input, var_counter, var_mapping);
                let canonical_output: Vec<String> = output_schema
                    .iter()
                    .map(|var| self.get_canonical_var(var, var_counter, var_mapping))
                    .collect();

                IRNode::FlatMap {
                    input: Box::new(canonical_input),
                    projection: projection.clone(),
                    filter_predicate: filter_predicate.clone(),
                    output_schema: canonical_output,
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
            } => {
                let canonical_left = self.canonicalize_recursive(left, var_counter, var_mapping);
                let canonical_right = self.canonicalize_recursive(right, var_counter, var_mapping);
                let canonical_output: Vec<String> = output_schema
                    .iter()
                    .map(|var| self.get_canonical_var(var, var_counter, var_mapping))
                    .collect();

                IRNode::JoinFlatMap {
                    left: Box::new(canonical_left),
                    right: Box::new(canonical_right),
                    left_keys: left_keys.clone(),
                    right_keys: right_keys.clone(),
                    projection: projection.clone(),
                    filter_predicate: filter_predicate.clone(),
                    output_schema: canonical_output,
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
    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
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
                format!("{predicate:?}").hash(hasher);
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
                    format!("{func:?}").hash(hasher);
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
                    format!("{expr:?}").hash(hasher);
                }
                self.hash_ir_recursive(input, hasher);
            }

            IRNode::HnswScan {
                index_name,
                k,
                ef_search,
                ..
            } => {
                index_name.hash(hasher);
                k.hash(hasher);
                ef_search.hash(hasher);
            }

            IRNode::FlatMap {
                input,
                projection,
                filter_predicate,
                ..
            } => {
                projection.hash(hasher);
                format!("{filter_predicate:?}").hash(hasher);
                self.hash_ir_recursive(input, hasher);
            }

            IRNode::JoinFlatMap {
                left,
                right,
                left_keys,
                right_keys,
                projection,
                filter_predicate,
                ..
            } => {
                left_keys.hash(hasher);
                right_keys.hash(hasher);
                projection.hash(hasher);
                format!("{filter_predicate:?}").hash(hasher);
                self.hash_ir_recursive(left, hasher);
                self.hash_ir_recursive(right, hasher);
            }
        }
    }

    /// Check if an IR subtree references any derived relation.
    /// Derived relations are populated by rule execution, which happens AFTER
    /// shared views are executed. Extracting subtrees that scan derived relations
    /// into shared views would produce empty results.
    fn references_derived_relation(
        ir: &IRNode,
        derived_relations: &std::collections::HashSet<String>,
    ) -> bool {
        match ir {
            IRNode::Scan { relation, .. } => derived_relations.contains(relation),
            IRNode::Map { input, .. }
            | IRNode::Filter { input, .. }
            | IRNode::Distinct { input }
            | IRNode::Aggregate { input, .. }
            | IRNode::Compute { input, .. }
            | IRNode::FlatMap { input, .. } => {
                Self::references_derived_relation(input, derived_relations)
            }
            IRNode::Join { left, right, .. }
            | IRNode::Antijoin { left, right, .. }
            | IRNode::JoinFlatMap { left, right, .. } => {
                Self::references_derived_relation(left, derived_relations)
                    || Self::references_derived_relation(right, derived_relations)
            }
            IRNode::Union { inputs } => inputs
                .iter()
                .any(|i| Self::references_derived_relation(i, derived_relations)),
            IRNode::HnswScan { .. } => false,
        }
    }

    /// Compute depth of a subtree
    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
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
            IRNode::HnswScan { .. } => 1, // Terminal node like Scan
            IRNode::FlatMap { input, .. } => 1 + self.subtree_depth(input),
            IRNode::JoinFlatMap { left, right, .. } => {
                1 + self.subtree_depth(left).max(self.subtree_depth(right))
            }
        }
    }

    /// Compute sharing statistics for a set of IR trees
    pub fn compute_stats(&self, irs: &[IRNode]) -> SharingStats {
        let mut subtree_counts: HashMap<u64, Vec<(usize, IRNode)>> = HashMap::new();

        for (ir_idx, ir) in irs.iter().enumerate() {
            self.collect_subtrees(ir, ir_idx, &mut subtree_counts);
        }

        let total_subtrees: usize = subtree_counts.values().map(std::vec::Vec::len).sum();
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
            IRNode::HnswScan { .. } => {} // Terminal node
            IRNode::FlatMap { input, .. } => self.count_subtrees_internal(input, counts),
            IRNode::JoinFlatMap { left, right, .. } => {
                self.count_subtrees_internal(left, counts);
                self.count_subtrees_internal(right, counts);
            }
        }
    }
}

