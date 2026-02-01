//! Lock-Free Snapshot System for Knowledge Graphs
//!
//! Provides immutable point-in-time snapshots of knowledge graph data
//! for lock-free read access. Uses arc-swap for instant atomic publishing.
//!
//! ## Design
//!
//! - `KnowledgeGraphSnapshot`: Immutable snapshot with Arc-wrapped data
//! - Data is shared via Arc, so cloning a snapshot is O(1)
//! - Writers publish new snapshots atomically via ArcSwap
//! - Readers get consistent snapshots without holding locks

use crate::ast::Rule;
use crate::value::{Tuple, Tuple2};
use crate::DatalogEngine;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Counter for snapshot versioning
static SNAPSHOT_VERSION: AtomicU64 = AtomicU64::new(0);

/// Immutable point-in-time snapshot of knowledge graph data
///
/// All data is wrapped in Arc for efficient sharing between readers.
/// Cloning a snapshot is O(1) - just incrementing reference counts.
#[derive(Clone)]
pub struct KnowledgeGraphSnapshot {
    /// Monotonically increasing version number
    pub version: u64,

    /// Timestamp when snapshot was created (microseconds since epoch)
    pub timestamp: u64,

    /// Base relation data (legacy binary format)
    /// Wrapped in Arc for lock-free sharing
    pub input_data: Arc<HashMap<String, Vec<Tuple2>>>,

    /// Base relation data (production format - arbitrary arity)
    /// Wrapped in Arc for lock-free sharing
    pub input_tuples: Arc<HashMap<String, Vec<Tuple>>>,

    /// Persistent rules (AST format)
    /// Wrapped in Arc for lock-free sharing
    pub rules: Arc<Vec<Rule>>,
}

impl KnowledgeGraphSnapshot {
    /// Create a new snapshot from knowledge graph data
    pub fn new(
        input_data: HashMap<String, Vec<Tuple2>>,
        input_tuples: HashMap<String, Vec<Tuple>>,
        rules: Vec<Rule>,
    ) -> Self {
        let version = SNAPSHOT_VERSION.fetch_add(1, Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        Self {
            version,
            timestamp,
            input_data: Arc::new(input_data),
            input_tuples: Arc::new(input_tuples),
            rules: Arc::new(rules),
        }
    }

    /// Create an empty snapshot
    pub fn empty() -> Self {
        Self::new(HashMap::new(), HashMap::new(), Vec::new())
    }

    /// Execute a query against this snapshot
    ///
    /// Creates a fresh DatalogEngine with the snapshot's data.
    /// The snapshot is immutable so this is thread-safe without locks.
    pub fn execute(&self, program: &str) -> Result<Vec<Tuple2>, String> {
        let mut engine = DatalogEngine::new();
        // Clone from Arc - efficient because underlying data is shared
        engine.input_data = (*self.input_data).clone();
        engine.input_tuples = (*self.input_tuples).clone();
        engine.execute(program)
    }

    /// Execute a query with rules prepended against this snapshot
    pub fn execute_with_rules(&self, program: &str) -> Result<Vec<Tuple2>, String> {
        if self.rules.is_empty() {
            return self.execute(program);
        }

        // Build combined program: rules + query
        let mut combined = String::new();
        for rule in self.rules.iter() {
            combined.push_str(&super::format_rule(rule));
            combined.push('\n');
        }
        combined.push_str(program);

        self.execute(&combined)
    }

    /// Execute a query returning arbitrary-arity tuples
    pub fn execute_tuples(&self, program: &str) -> Result<Vec<Tuple>, String> {
        let mut engine = DatalogEngine::new();
        engine.input_data = (*self.input_data).clone();
        engine.input_tuples = (*self.input_tuples).clone();
        engine.execute_tuples(program)
    }

    /// Execute a query with rules, returning arbitrary-arity tuples
    pub fn execute_with_rules_tuples(&self, program: &str) -> Result<Vec<Tuple>, String> {
        if self.rules.is_empty() {
            return self.execute_tuples(program);
        }

        // Build combined program: rules + query
        let mut combined = String::new();
        for rule in self.rules.iter() {
            combined.push_str(&super::format_rule(rule));
            combined.push('\n');
        }
        combined.push_str(program);

        self.execute_tuples(&combined)
    }

    /// Get the number of relations in this snapshot
    pub fn relation_count(&self) -> usize {
        self.input_data
            .len()
            .max(self.input_tuples.len())
    }

    /// Get the total number of tuples across all relations
    pub fn tuple_count(&self) -> usize {
        let legacy_count: usize = self.input_data.values().map(|v| v.len()).sum();
        let prod_count: usize = self.input_tuples.values().map(|v| v.len()).sum();
        legacy_count.max(prod_count)
    }

    /// Check if this snapshot is empty (no data)
    pub fn is_empty(&self) -> bool {
        self.input_data.is_empty() && self.input_tuples.is_empty()
    }
}

impl std::fmt::Debug for KnowledgeGraphSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KnowledgeGraphSnapshot")
            .field("version", &self.version)
            .field("timestamp", &self.timestamp)
            .field("relations", &self.relation_count())
            .field("tuples", &self.tuple_count())
            .field("rules", &self.rules.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_creation() {
        let mut input_data = HashMap::new();
        input_data.insert("edge".to_string(), vec![(1, 2), (2, 3)]);

        let snapshot = KnowledgeGraphSnapshot::new(input_data, HashMap::new(), Vec::new());

        assert_eq!(snapshot.relation_count(), 1);
        assert_eq!(snapshot.tuple_count(), 2);
        assert!(!snapshot.is_empty());
    }

    #[test]
    fn test_snapshot_execute() {
        let mut input_data = HashMap::new();
        input_data.insert("edge".to_string(), vec![(1, 2), (2, 3), (3, 4)]);

        let snapshot = KnowledgeGraphSnapshot::new(input_data, HashMap::new(), Vec::new());

        let results = snapshot.execute("result(X,Y) :- edge(X,Y).").unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_snapshot_clone_is_cheap() {
        let mut input_data = HashMap::new();
        input_data.insert("edge".to_string(), vec![(1, 2), (2, 3)]);

        let snapshot1 = KnowledgeGraphSnapshot::new(input_data, HashMap::new(), Vec::new());
        let snapshot2 = snapshot1.clone();

        // Both snapshots share the same underlying data (Arc)
        assert!(Arc::ptr_eq(&snapshot1.input_data, &snapshot2.input_data));
    }

    #[test]
    fn test_empty_snapshot() {
        let snapshot = KnowledgeGraphSnapshot::empty();
        assert!(snapshot.is_empty());
        assert_eq!(snapshot.relation_count(), 0);
        assert_eq!(snapshot.tuple_count(), 0);
    }
}
