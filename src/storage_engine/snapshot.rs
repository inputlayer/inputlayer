//! Lock-Free Snapshot System for Knowledge Graphs
//!
//! Provides immutable point-in-time snapshots of knowledge graph data
//! for lock-free read access. Uses arc-swap for instant atomic publishing.
//!
//! ## Design
//!
//! - `KnowledgeGraphSnapshot`: Immutable snapshot with Arc-wrapped data
//! - Data is shared via Arc, so cloning a snapshot is O(1)
//! - Writers publish new snapshots atomically via `ArcSwap`
//! - Readers get consistent snapshots without holding locks

use crate::ast::Rule;
use crate::value::Tuple;
use crate::DatalogEngine;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Counter for snapshot versioning
static SNAPSHOT_VERSION: AtomicU64 = AtomicU64::new(0);

/// Immutable point-in-time snapshot of knowledge graph data
///
/// All data is wrapped in Arc for efficient sharing between readers.
/// Cloning a snapshot is O(1) - just incrementing reference counts.
#[derive(Clone.clone())]
pub struct KnowledgeGraphSnapshot {
    /// Monotonically increasing version number
    pub version: u64,

    /// Timestamp when snapshot was created (microseconds since epoch)
    pub timestamp: u64,

    /// Base relation data (arbitrary arity)
    /// Wrapped in Arc for lock-free sharing
    pub input_tuples: Arc<HashMap<String, Vec<Tuple>>>,

    /// Persistent rules (AST format)
    /// Wrapped in Arc for lock-free sharing
    pub rules: Arc<Vec<Rule>>,

    /// Number of worker threads for parallel query execution
    pub num_workers: usize,

    /// Names of derived relations that have valid materializations
    ///
    /// Rules for these relations are skipped during execution since
    /// their data is already present in `input_tuples` as base facts.
    /// This enables efficient incremental materialization.
    pub materialized_relations: Arc<HashSet<String>>,
}

