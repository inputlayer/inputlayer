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

    /// Output schema (column names from head)
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

    /// Relations scanned by this clause (direct, not transitive)
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

    /// Timestamp when materialized (for diagnostics)
    pub materialized_at: u64,
}

