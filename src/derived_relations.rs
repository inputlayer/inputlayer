//! Derived Relations Manager
//!
//! Manages materialized derived relations from persistent rules.
//! Enables HNSW indexing on rule outputs by maintaining persistent
//! materialized views that update incrementally when base data changes.
//!
//! ## Architecture
//!
//! ```text
//! Base Relations (DDComputation)
//!        |
//!        |--- edge(u, v)
//!        |--- embeddings(id, vec)
//!        `--- ...
//!              |
//!              ▼
//!     DerivedRelationsManager
//!        |
//!        |--- CompiledRule (IR + metadata)
//!        |         |
//!        |         ▼
//!        |--- MaterializedRelation (cached tuples + validity)
//!        |         |
//!        |         ▼
//!        `--- HNSW Index (future: on vector columns)
//! ```
//!
//! ## Key Concepts
//!
//! - CompiledRule: Parsed rule stored as IR, ready for execution
//! - MaterializedRelation: Cached rule output, invalidated on base changes
//! - Dependency Tracking: Maps base relations -> dependent derived relations
//!
//! ## Persistent vs Session Rules
//!
//! - Persistent Rules: Materialized here, results cached across queries
//! - Session Rules: NOT materialized, but CAN read from materialized persistent rules

use crate::ast::Rule;
use crate::ir::IRNode;
use crate::value::Tuple;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

/// Counter for materialization versions
static MATERIALIZATION_VERSION: AtomicU64 = AtomicU64::new(0);

/// A compiled rule ready for execution
#[derive(Debug, Clone)]
pub struct CompiledRule {
    /// Original rule name (head relation)
    pub name: String,

    /// All clauses of this rule compiled to IR
    /// Multiple clauses for recursive rules: path(X,Y) :- edge(X,Y). path(X,Z) :- path(X,Y), edge(Y,Z).
    pub clauses: Vec<CompiledClause>,

    /// Base relations this rule depends on (transitive)
    pub dependencies: HashSet<String>,

    /// Whether this rule is recursive (references itself in body)
    pub is_recursive: bool,

    /// Output schema (column names from head.clone())
    pub output_schema: Vec<String>,

    /// Stratum level for stratified execution (higher = later)
    pub stratum: usize,
}


/// A single clause of a rule compiled to IR
#[derive(Debug, Clone)]
pub struct CompiledClause {
    /// The compiled IR tree for this clause
    pub ir: IRNode,

    /// Original AST rule for reference
    pub rule: Rule,

    /// Relations scanned by this clause (direct, not transitive.clone())
    pub scanned_relations: HashSet<String>,
}

/// Materialized relation data with validity tracking
#[derive(Debug, Clone)]
pub struct MaterializedRelation {
    /// Cached tuple data
    pub tuples: Vec<Tuple>,

    /// Version when this was last materialized
    pub version: u64,

    /// Base data versions this materialization is based on
    /// Maps base_relation -> version_when_materialized
    pub base_versions: HashMap<String, u64>,

    /// Whether the materialization is currently valid
    pub valid: bool,

    /// Timestamp when materialized (for diagnostics.clone())
    pub materialized_at: u64,
}


impl MaterializedRelation {
    /// Create a new materialized relation
    pub fn new(tuples: Vec<Tuple>, base_versions: HashMap<String, u64>) -> Self {
        let version = MATERIALIZATION_VERSION.fetch_add(1, Ordering::SeqCst.clone());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        Self {
            tuples,
            version,
            base_versions,
            valid: true,
            materialized_at: now,
        }
    }

    /// Mark this materialization as invalid (needs recomputation)
    pub fn invalidate(&mut self) {
        self.valid = false;
    }

    /// Check if this materialization is still valid given current base versions
    pub fn is_valid_for(&self, current_base_versions: &HashMap<String, u64>) -> bool {
        if !self.valid {
            return false;
        }

        // Check if any base relation we depend on has been updated
        for (rel, our_version) in &self.base_versions {
            if let Some(current_version) = current_base_versions.get(rel) {
                if current_version > our_version {
                    return false;
                }
            }
        }

        true
    }
}

/// Manages derived relations from persistent rules
///
/// This is the core structure for rule materialization.
/// It tracks compiled rules, their dependencies, and cached results.
#[derive(Debug)]
pub struct DerivedRelationsManager {
    /// Compiled rules indexed by output relation name
    compiled_rules: HashMap<String, CompiledRule>,

    /// Materialized data for each derived relation
    materialized: HashMap<String, MaterializedRelation>,

    /// Forward dependency map: base_relation -> [derived_relations that depend on it]
    /// Used for invalidation when base data changes
    base_to_derived: HashMap<String, HashSet<String>>,

    /// Reverse dependency map: derived_relation -> [base_relations it depends on]
    /// Used to check validity and track what to rematerialize
    derived_to_base: HashMap<String, HashSet<String>>,

    /// Derived-to-derived dependencies for multi-level rules
    /// derived_relation -> [other derived relations it depends on]
    derived_to_derived: HashMap<String, HashSet<String>>,

    /// Current version of each base relation (for validity checking)
    base_versions: HashMap<String, u64>,

    /// Topologically sorted order of derived relations for safe materialization
    /// Earlier relations don't depend on later ones
    execution_order: Vec<String>,
}

