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

