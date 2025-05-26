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
#[derive(Clone)]
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

impl KnowledgeGraphSnapshot {
    /// Create a new snapshot from knowledge graph data
    pub fn new(input_tuples: HashMap<String, Vec<Tuple>>, rules: Vec<Rule>) -> Self {
        Self::new_with_workers(input_tuples, rules, 1)
    }

    /// Create a new snapshot with configurable worker count
    pub fn new_with_workers(
        input_tuples: HashMap<String, Vec<Tuple>>,
        rules: Vec<Rule>,
        num_workers: usize,
    ) -> Self {
        Self::new_with_materializations(input_tuples, rules, num_workers, HashSet::new())
    }

    /// Create a new snapshot with materialized relations
    ///
    /// `materialized_tuples` contains tuples from derived relations that have
    /// valid materializations. These are merged into `input_tuples` so they
    /// appear as base facts. The corresponding rule execution is skipped.
    ///
    /// `materialized_names` identifies which relations are materialized.
    /// Rules with head relation in this set are not prepended to queries.
    pub fn new_with_materializations(
        input_tuples: HashMap<String, Vec<Tuple>>,
        rules: Vec<Rule>,
        num_workers: usize,
        materialized_names: HashSet<String>,
    ) -> Self {
        let version = SNAPSHOT_VERSION.fetch_add(1, Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        // Note: materialized tuples should be merged into input_tuples by the caller
        // (KnowledgeGraph::publish_snapshot) before calling this constructor.
        // This keeps the snapshot constructor simple and allows the caller to
        // decide how to handle conflicts (though typically there shouldn't be any).

        Self {
            version,
            timestamp,
            input_tuples: Arc::new(input_tuples),
            rules: Arc::new(rules),
            num_workers,
            materialized_relations: Arc::new(materialized_names),
        }
    }

    /// Create an empty snapshot
    pub fn empty() -> Self {
        Self::new(HashMap::new(), Vec::new())
    }

    /// Execute a query against this snapshot
    ///
    /// Creates a fresh `DatalogEngine` with the snapshot's data.
    /// The snapshot is immutable so this is thread-safe without locks.
    pub fn execute(&self, program: &str) -> Result<Vec<(i32, i32)>, String> {
        let mut engine = DatalogEngine::new();
        // Clone from Arc - efficient because underlying data is shared
        engine.input_tuples.clone_from(&self.input_tuples);
        engine.execute(program)
    }

    /// Execute a query with rules prepended against this snapshot
    ///
    /// Rules for materialized relations are skipped - their data is already
    /// present in `input_tuples` as base facts (injected at snapshot creation).
    pub fn execute_with_rules(&self, program: &str) -> Result<Vec<(i32, i32)>, String> {
        if self.rules.is_empty() {
            return self.execute(program);
        }

        // Build combined program: rules + query
        // Skip rules for relations that are already materialized
        let mut combined = String::new();
        for rule in self.rules.iter() {
            // Skip rules whose head relation is materialized
            if self.materialized_relations.contains(&rule.head.relation) {
                continue;
            }
            combined.push_str(&super::format_rule(rule));
            combined.push('\n');
        }
        combined.push_str(program);

        self.execute(&combined)
    }

    /// Execute a query returning arbitrary-arity tuples
    pub fn execute_tuples(&self, program: &str) -> Result<Vec<Tuple>, String> {
        let mut engine = DatalogEngine::new();
        engine.input_tuples.clone_from(&self.input_tuples);
        engine.set_num_workers(self.num_workers);
        engine.execute_tuples(program)
    }

    /// Execute a query with rules, returning arbitrary-arity tuples
    ///
    /// Rules for materialized relations are skipped - their data is already
    /// present in `input_tuples` as base facts (injected at snapshot creation).
    pub fn execute_with_rules_tuples(&self, program: &str) -> Result<Vec<Tuple>, String> {
        if self.rules.is_empty() {
            return self.execute_tuples(program);
        }

        // Build combined program: rules + query
        // Skip rules for relations that are already materialized
        let mut combined = String::new();
        for rule in self.rules.iter() {
            // Skip rules whose head relation is materialized
            // (their data is already in input_tuples as base facts)
            if self.materialized_relations.contains(&rule.head.relation) {
                continue;
            }
            combined.push_str(&super::format_rule(rule));
            combined.push('\n');
        }
        combined.push_str(program);

        self.execute_tuples(&combined)
    }

    /// Execute a query with temporary session facts that don't affect the shared store
    ///
    /// This provides request-scoped isolation: session facts are added to a CLONE
    /// of the snapshot's data, not the shared store. This prevents race conditions
    /// where concurrent queries could see each other's session facts.
    ///
    /// # Arguments
    /// * `program` - The query/rules to execute
    /// * `session_facts` - Vec of (relation_name, tuple) pairs to add temporarily
    ///
    /// # Example
    /// ```ignore
    /// let snapshot = kg.snapshot();
    /// let result = snapshot.execute_with_session_facts(
    ///     "result(X) :- edge(X, Y), session_filter(Y).",
    ///     vec![("session_filter".to_string(), Tuple::from_pair(3, 0))],
    /// )?;
    /// // session_filter is only visible to THIS query, not other concurrent queries
    /// ```
    pub fn execute_with_session_facts(
        &self,
        program: &str,
        session_facts: Vec<(String, Tuple)>,
    ) -> Result<Vec<Tuple>, String> {
        // Create a fresh engine with cloned data
        let mut engine = DatalogEngine::new();
        engine.set_num_workers(self.num_workers);

        // Clone the snapshot's input_tuples (this is a shallow clone of the HashMap,
        // but each Vec<Tuple> is cloned - this is necessary for isolation)
        let mut isolated_tuples = (*self.input_tuples).clone();

        // Add session facts to the isolated copy (NOT the shared store!)
        for (relation, tuple) in session_facts {
            isolated_tuples.entry(relation).or_default().push(tuple);
        }

        // Set the isolated tuples on the engine
        engine.input_tuples = isolated_tuples;

        // Build combined program with rules (skip materialized)
        let mut combined = String::new();
        for rule in self.rules.iter() {
            if self.materialized_relations.contains(&rule.head.relation) {
                continue;
            }
            combined.push_str(&super::format_rule(rule));
            combined.push('\n');
        }
        combined.push_str(program);

        // Execute against isolated state
        engine.execute_tuples(&combined)
    }

    /// Get the number of relations in this snapshot
    pub fn relation_count(&self) -> usize {
        self.input_tuples.len()
    }

    /// Get the total number of tuples across all relations
    pub fn tuple_count(&self) -> usize {
        self.input_tuples.values().map(std::vec::Vec::len).sum()
    }

    /// Check if this snapshot is empty (no data)
    pub fn is_empty(&self) -> bool {
        self.input_tuples.is_empty()
    }

    /// Get the number of materialized relations in this snapshot
    pub fn materialized_count(&self) -> usize {
        self.materialized_relations.len()
    }

    /// Check if a relation is materialized in this snapshot
    pub fn is_materialized(&self, relation: &str) -> bool {
        self.materialized_relations.contains(relation)
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
            .field("materialized", &self.materialized_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn test_snapshot_creation() {
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
            ],
        );

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());

        assert_eq!(snapshot.relation_count(), 1);
        assert_eq!(snapshot.tuple_count(), 2);
        assert!(!snapshot.is_empty());
    }

    #[test]
    fn test_snapshot_execute() {
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                Tuple::new(vec![Value::Int32(3), Value::Int32(4)]),
            ],
        );

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());

        let results = snapshot.execute("result(X,Y) :- edge(X,Y).").unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_snapshot_clone_is_cheap() {
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
            ],
        );

        let snapshot1 = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());
        let snapshot2 = snapshot1.clone();

        // Both snapshots share the same underlying data (Arc)
        assert!(Arc::ptr_eq(
            &snapshot1.input_tuples,
            &snapshot2.input_tuples
        ));
    }

    #[test]
    fn test_empty_snapshot() {
        let snapshot = KnowledgeGraphSnapshot::empty();
        assert!(snapshot.is_empty());
        assert_eq!(snapshot.relation_count(), 0);
        assert_eq!(snapshot.tuple_count(), 0);
    }

    // === Materialization Tests ===

    #[test]
    fn test_snapshot_with_materializations() {
        // Base relation
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
            ],
        );

        // Simulate materialized derived relation
        let mut materialized_names = HashSet::new();
        materialized_names.insert("path".to_string());

        // Add "path" tuples as if they were materialized
        input_tuples.insert(
            "path".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                Tuple::new(vec![Value::Int32(1), Value::Int32(3)]), // transitive closure
            ],
        );

        let snapshot = KnowledgeGraphSnapshot::new_with_materializations(
            input_tuples,
            Vec::new(), // No rules needed since path is materialized
            1,
            materialized_names,
        );

        assert_eq!(snapshot.materialized_count(), 1);
        assert!(snapshot.is_materialized("path"));
        assert!(!snapshot.is_materialized("edge"));
    }

