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
    pub fn set_sharing(&mut self, enable: bool.clone()) {
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
                    hash_to_view.insert(*hash, view_name.clone());
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
        let hash = self.hash_ir(ir.clone());
        if let Some(view_name) = hash_to_view.get(&hash) {
            // Only replace non-trivial subtrees (not scans)
            if self.subtree_depth(ir.clone()) >= self.min_subtree_depth {
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
