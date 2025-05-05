//! Storage Engine - Multi-Knowledge-Graph Persistent Storage
//!
//! Provides:
//! - Multiple isolated knowledge graphs (namespace isolation like PostgreSQL/MySQL)
//! - Filesystem persistence with configurable path
//! - Knowledge graph lifecycle management (create, drop, list, switch)
//! - Knowledge-graph-scoped CRUD operations
//! - Parquet-based storage for efficiency
//! - Lock-free read path via snapshots
//!
//! ## Example
//!
//! ```rust,no_run
//! use inputlayer::{StorageEngine, Config};
//!
//! let config = Config::default();
//! let mut storage = StorageEngine::new(config).unwrap();
//!
//! // Create and use knowledge graph
//! storage.create_knowledge_graph("analytics").unwrap();
//! storage.use_knowledge_graph("analytics").unwrap();
//!
//! // Insert data
//! storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();
//!
//! // Execute query (variables must be uppercase)
//! let results = storage.execute_query("path(X,Y) :- edge(X,Y).").unwrap();
//!
//! // Persist to disk
//! storage.save_knowledge_graph("analytics").unwrap();
//! ```

mod snapshot;
pub use snapshot::KnowledgeGraphSnapshot;

use crate::config::Config;
use crate::dd_computation::DDComputation;
use crate::derived_relations::CompiledRule;
use crate::rule_catalog::RuleCatalog;
use crate::schema::{RelationSchema, SchemaCatalog, ValidationEngine};
use crate::statement::{RuleDef, SerializableBodyPred};
use crate::storage::persist::{
    consolidate_to_current, to_tuples, FilePersist, PersistBackend, PersistConfig, Update,
};
use crate::storage::{
    KnowledgeGraphMetadata, KnowledgeGraphsMetadata, StorageError, StorageResult,
};
use crate::value::Tuple;
use crate::DatalogEngine;
use arc_swap::ArcSwap;
use chrono::Utc;
use dashmap::DashMap;
use parking_lot::RwLock;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Storage Engine - manages multiple knowledge graphs
///
/// Uses `DashMap` for concurrent access to knowledge graphs without global locks.
pub struct StorageEngine {
    config: Config,
    /// Knowledge graphs with lock-free concurrent access
    knowledge_graphs: DashMap<String, Arc<RwLock<KnowledgeGraph>>>,
    current_kg: Option<String>,
    /// DD-native persist backend
    persist: Arc<FilePersist>,
    /// Logical timestamp for DD updates (monotonically increasing)
    logical_time: AtomicU64,
}

/// Single knowledge graph instance
pub struct KnowledgeGraph {
    name: String,
    engine: DatalogEngine,
    metadata: KnowledgeGraphMetadata,
    /// Data directory for this knowledge graph (used for rule and schema persistence)
    data_dir: PathBuf,
    /// Rule catalog for persistent derived relations
    rule_catalog: RuleCatalog,
    /// Schema catalog for relation type definitions (per-KG isolation)
    schema_catalog: SchemaCatalog,
    /// Current snapshot for lock-free reads (updated atomically on writes)
    snapshot: ArcSwap<KnowledgeGraphSnapshot>,
    /// Persistent DD computation for incremental updates (shadow writes)
    dd_computation: Option<DDComputation>,
    /// Number of workers for parallel query execution
    num_workers: usize,
}

impl StorageEngine {
    /// Create new storage engine from configuration
    pub fn new(config: Config) -> StorageResult<Self> {
        // Configure thread pool for parallel execution (if not already initialized)
        let num_threads = config.storage.performance.num_threads;
        if num_threads > 0 {
            // Ignore error if thread pool is already initialized (e.g., in tests)
            let _ = rayon::ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .build_global();
        }

        // Create base data directory
        fs::create_dir_all(&config.storage.data_dir)?;
        fs::create_dir_all(config.storage.data_dir.join("metadata"))?;

        // Initialize DD-native persist backend
        let persist_config = PersistConfig {
            path: config.storage.data_dir.join("persist"),
            buffer_size: config.storage.persist.buffer_size,
            immediate_sync: config.storage.persist.immediate_sync,
            durability_mode: config.storage.persist.durability_mode,
        };
        let persist = Arc::new(FilePersist::new(persist_config)?);

        let mut engine = StorageEngine {
            config,
            knowledge_graphs: DashMap::new(),
            current_kg: None,
            persist,
            logical_time: AtomicU64::new(1),
        };

        // Load existing knowledge graphs from persist layer
        engine.load_all_knowledge_graphs()?;

        // Create default knowledge graph if it doesn't exist
        let default_db = engine.config.storage.default_knowledge_graph.clone();
        if !engine.knowledge_graphs.contains_key(&default_db) {
            engine.create_knowledge_graph(&default_db)?;
        }

        // Set current knowledge graph to default
        engine.current_kg = Some(default_db);

        Ok(engine)
    }

    /// Create a new knowledge graph
    pub fn create_knowledge_graph(&self, name: &str) -> StorageResult<()> {
        if self.knowledge_graphs.contains_key(name) {
            return Err(StorageError::KnowledgeGraphExists(name.to_string()));
        }

        // Validate knowledge graph name
        if name.is_empty() || name.contains('/') || name.contains('\\') {
            return Err(StorageError::InvalidRelationName(name.to_string()));
        }

        // Create knowledge graph directory structure
        let db_dir = self.config.storage.data_dir.join(name);
        fs::create_dir_all(&db_dir)?;
        fs::create_dir_all(db_dir.join("relations"))?;

        // Create knowledge graph instance (uses persist layer for durability)
        let num_workers = self.config.storage.performance.num_threads;
        let kg = KnowledgeGraph::new_with_workers(name.to_string(), db_dir, num_workers);

        // Store in memory
        self.knowledge_graphs
            .insert(name.to_string(), Arc::new(RwLock::new(kg)));

        // Update system metadata
        self.save_knowledge_graphs_metadata()?;

        Ok(())
    }

    /// Drop a knowledge graph (delete all data)
    pub fn drop_knowledge_graph(&mut self, name: &str) -> StorageResult<()> {
        // Cannot drop default knowledge graph
        if name == self.config.storage.default_knowledge_graph {
            return Err(StorageError::CannotDropDefault);
        }

        // Cannot drop current knowledge graph
        if let Some(current) = &self.current_kg {
            if current == name {
                return Err(StorageError::CannotDropCurrentKnowledgeGraph);
            }
        }

        // Check if knowledge graph exists
        if !self.knowledge_graphs.contains_key(name) {
            return Err(StorageError::KnowledgeGraphNotFound(name.to_string()));
        }

        // Remove from memory
        self.knowledge_graphs.remove(name);

        // Delete from disk
        let db_dir = self.config.storage.data_dir.join(name);
        if db_dir.exists() {
            fs::remove_dir_all(db_dir)?;
        }

        // Update system metadata
        self.save_knowledge_graphs_metadata()?;

        Ok(())
    }

    /// Switch to a different knowledge graph
    pub fn use_knowledge_graph(&mut self, name: &str) -> StorageResult<()> {
        if !self.knowledge_graphs.contains_key(name) {
            if self.config.storage.auto_create_knowledge_graphs {
                self.create_knowledge_graph(name)?;
            } else {
                return Err(StorageError::KnowledgeGraphNotFound(name.to_string()));
            }
        }

        // Update access timestamp (reusing created_at as last-accessed tracker)
        if let Some(db) = self.knowledge_graphs.get(name) {
            let mut db = db.write();
            db.metadata.created_at = Utc::now().to_rfc3339();
        }

        self.current_kg = Some(name.to_string());
        Ok(())
    }

    /// Ensure a knowledge graph exists, creating it if auto-create is enabled.
    /// This is a `&self` method suitable for use from read-lock contexts.
    pub fn ensure_knowledge_graph(&self, name: &str) -> StorageResult<()> {
        if self.knowledge_graphs.contains_key(name) {
            return Ok(());
        }
        if self.config.storage.auto_create_knowledge_graphs {
            self.create_knowledge_graph(name)
        } else {
            Err(StorageError::KnowledgeGraphNotFound(name.to_string()))
        }
    }

    /// List all knowledge graphs
    pub fn list_knowledge_graphs(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .knowledge_graphs
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        names.sort();
        names
    }

    /// Get current knowledge graph name
    pub fn current_knowledge_graph(&self) -> Option<&str> {
        self.current_kg.as_deref()
    }

    /// Insert binary tuples into a relation in the current knowledge graph
    ///
    /// This is a convenience API for binary (i32, i32) tuples.
    /// For arbitrary arity tuples, use `insert_tuples` instead.
    /// Returns (`new_count`, `duplicate_count`) for reporting to user
    pub fn insert(&self, relation: &str, tuples: Vec<(i32, i32)>) -> StorageResult<(usize, usize)> {
        // Convert to Tuple format
        let tuples: Vec<Tuple> = tuples
            .iter()
            .map(|&(a, b)| Tuple::from_pair(a, b))
            .collect();
        self.insert_tuples(relation, tuples)
    }

    /// Insert binary tuples into a specific knowledge graph (explicit API)
    ///
    /// This is a convenience API for binary (i32, i32) tuples.
    /// For arbitrary arity tuples, use `insert_tuples_into` instead.
    /// Returns (`new_count`, `duplicate_count`) for reporting to user
    pub fn insert_into(
        &self,
        kg: &str,
        relation: &str,
        tuples: Vec<(i32, i32)>,
    ) -> StorageResult<(usize, usize)> {
        // Convert to Tuple format
        let tuples: Vec<Tuple> = tuples
            .iter()
            .map(|&(a, b)| Tuple::from_pair(a, b))
            .collect();
        self.insert_tuples_into(kg, relation, tuples)
    }

    /// Insert arbitrary-arity tuples into a relation in the current knowledge graph
    /// This is the production API that supports vectors and mixed types.
    /// Returns (`new_count`, `duplicate_count`) for reporting to user
    pub fn insert_tuples(
        &self,
        relation: &str,
        tuples: Vec<Tuple>,
    ) -> StorageResult<(usize, usize)> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.insert_tuples_into(&db_name, relation, tuples)
    }

    /// Insert arbitrary-arity tuples into a specific knowledge graph (explicit API)
    /// Returns (`new_count`, `duplicate_count`) for reporting to user
    ///
    /// Uses `&self` instead of `&mut self` to enable concurrent writes to different KGs.
    pub fn insert_tuples_into(
        &self,
        kg: &str,
        relation: &str,
        tuples: Vec<Tuple>,
    ) -> StorageResult<(usize, usize)> {
        if tuples.is_empty() {
            return Ok((0, 0));
        }

        // Check if relation is a view (derived relation) - cannot insert into views
        {
            let db = self
                .knowledge_graphs
                .get(kg)
                .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;
            let db = db.read();
            if db.rule_exists(relation) {
                return Err(StorageError::Other(format!(
                    "Cannot insert into '{relation}': it is a derived relation (view). \
                     Use a base relation or drop the rule first with '.rule drop {relation}'."
                )));
            }
        }

        // Check arity consistency
        let new_arity = tuples.first().map_or(0, super::value::Tuple::arity);

        // Verify all tuples in this batch have the same arity
        for tuple in &tuples {
            if tuple.arity() != new_arity {
                return Err(StorageError::Other(format!(
                    "Arity mismatch in insert batch: expected {}, got {}",
                    new_arity,
                    tuple.arity()
                )));
            }
        }

        // Check if relation already exists with a different arity
        if let Some((existing_schema, _)) = self.get_relation_metadata_in(kg, relation)? {
            let existing_arity = existing_schema.len();
            if existing_arity != new_arity {
                return Err(StorageError::Other(format!(
                    "Arity mismatch for relation '{relation}': existing arity is {existing_arity}, but trying to insert tuples with arity {new_arity}"
                )));
            }
        }

        // Generate shard name and logical time
        let shard = format!("{kg}:{relation}");
        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);

        // Create DD-style updates (+1 diff for insert)
        let updates: Vec<Update> = tuples
            .iter().cloned()
            .map(|data| Update::insert(data.clone(), time))
            .collect();

        // Persist first (durability guarantee via WAL + batches)
        self.persist.ensure_shard(&shard)?;
        self.persist.append(&shard, &updates)?;

        // Update in-memory state
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        db.insert_in_memory(relation, tuples, time)
    }

    /// Delete binary tuples from a relation in the current knowledge graph
    ///
    /// This is a convenience API for binary (i32, i32) tuples.
    /// For arbitrary arity tuples, use `delete_tuples_from` instead.
    /// Returns the count of actually deleted tuples
    pub fn delete(&self, relation: &str, tuples: Vec<(i32, i32)>) -> StorageResult<usize> {
        // Convert to Tuple format
        let tuples: Vec<Tuple> = tuples
            .iter()
            .map(|&(a, b)| Tuple::from_pair(a, b))
            .collect();
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.delete_tuples_from(&db_name, relation, tuples)
    }

    /// Delete binary tuples from a specific knowledge graph (explicit API)
    ///
    /// This is a convenience API for binary (i32, i32) tuples.
    /// For arbitrary arity tuples, use `delete_tuples_from` instead.
    /// Returns the count of actually deleted tuples
    pub fn delete_from(
        &self,
        kg: &str,
        relation: &str,
        tuples: Vec<(i32, i32)>,
    ) -> StorageResult<usize> {
        // Convert to Tuple format
        let tuples: Vec<Tuple> = tuples
            .iter()
            .map(|&(a, b)| Tuple::from_pair(a, b))
            .collect();
        self.delete_tuples_from(kg, relation, tuples)
    }

    /// Delete a single tuple (Tuple type) from a relation in the current knowledge graph
    ///
    /// This is the production API that supports arbitrary-arity tuples.
    /// Returns the count of actually deleted tuples (0 or 1).
    pub fn delete_tuple(&self, relation: &str, tuple: &Tuple) -> StorageResult<usize> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.delete_tuples_from(&db_name, relation, vec![tuple.clone()])
    }

    /// Delete tuples (Tuple type) from a specific knowledge graph
    ///
    /// This is the production API that supports arbitrary-arity tuples.
    /// Returns the count of actually deleted tuples.
    ///
    /// Uses `&self` instead of `&mut self` to enable concurrent writes to different KGs.
    pub fn delete_tuples_from(
        &self,
        kg: &str,
        relation: &str,
        tuples: Vec<Tuple>,
    ) -> StorageResult<usize> {
        if tuples.is_empty() {
            return Ok(0);
        }

        // Generate shard name and logical time
        let shard = format!("{kg}:{relation}");
        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);

        // Create DD-style updates (-1 diff for delete)
        let updates: Vec<Update> = tuples
            .iter()
            .map(|data| Update::delete(data.clone(), time))
            .collect();

        // Persist first (durability guarantee via WAL + batches)
        self.persist.ensure_shard(&shard)?;
        self.persist.append(&shard, &updates)?;

        // Update in-memory state
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        db.delete_in_memory(relation, &tuples, time)
    }

    /// Execute a Datalog query on the current knowledge graph
    ///
    /// Returns binary tuples (i32, i32) for backward compatibility.
    /// For arbitrary arity results, use `execute_query_tuples` instead.
    pub fn execute_query(&self, program: &str) -> StorageResult<Vec<(i32, i32)>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.execute_query_on(&db_name, program)
    }

    /// Execute a Datalog query on a specific knowledge graph (explicit API)
    ///
    /// Uses a completely lock-free read path via snapshots.
    /// The snapshot is obtained atomically (O(1)) and execution proceeds
    /// without holding any locks. Concurrent reads never block.
    ///
    /// Returns binary tuples (i32, i32) for backward compatibility.
    /// For arbitrary arity results, use `execute_query_tuples_on` instead.
    pub fn execute_query_on(&self, kg: &str, program: &str) -> StorageResult<Vec<(i32, i32)>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        // Get snapshot atomically - O(1), no lock needed
        let snapshot = {
            let db_guard = db.read();
            db_guard.snapshot()
        };

        // Execute on snapshot - completely lock-free
        snapshot
            .execute(program)
            .map_err(|e| StorageError::Other(format!("Query execution failed: {e}")))
    }

    /// Execute a Datalog query on the current knowledge graph, returning arbitrary arity tuples
    pub fn execute_query_tuples(&self, program: &str) -> StorageResult<Vec<Tuple>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.execute_query_tuples_on(&db_name, program)
    }

    /// Execute a Datalog query on a specific knowledge graph, returning arbitrary arity tuples
    pub fn execute_query_tuples_on(&self, kg: &str, program: &str) -> StorageResult<Vec<Tuple>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        // Get snapshot atomically - O(1), no lock needed
        let snapshot = {
            let db_guard = db.read();
            db_guard.snapshot()
        };

        // Execute on snapshot - completely lock-free
        snapshot
            .execute_tuples(program)
            .map_err(|e| StorageError::Other(format!("Query execution failed: {e}")))
    }

    /// Explain a query plan without executing it.
    ///
    /// Runs parse → IR → optimize on the query and returns the pipeline trace.
    pub fn explain_query_on(
        &self,
        kg: &str,
        program: &str,
    ) -> StorageResult<crate::pipeline_trace::PipelineTrace> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let snapshot = {
            let db_guard = db.read();
            db_guard.snapshot()
        };

        let mut engine = crate::DatalogEngine::new();
        engine.input_tuples_mut().clone_from(&snapshot.input_tuples);

        engine
            .explain(program)
            .map_err(|e| StorageError::Other(format!("Query explain failed: {e}")))
    }

    /// Save a specific knowledge graph to disk (flush persist buffers)
    pub fn save_knowledge_graph(&self, name: &str) -> StorageResult<()> {
        // Check knowledge graph exists
        if !self.knowledge_graphs.contains_key(name) {
            return Err(StorageError::KnowledgeGraphNotFound(name.to_string()));
        }

        // Flush all shards for this knowledge graph
        let prefix = format!("{name}:");
        for shard_name in self.persist.list_shards()? {
            if shard_name.starts_with(&prefix) {
                self.persist.flush(&shard_name)?;
            }
        }

        // Sync to disk
        self.persist.sync()?;

        Ok(())
    }

    /// Compact all shards - flush WAL buffers, consolidate updates, and write optimized batch files.
    /// This is an optimization operation, not required for durability (WAL provides that).
    /// Compaction:
    /// 1. Flushes in-memory buffers to batch files
    /// 2. Consolidates all (data, time, diff) triples (cancels out +1/-1 pairs)
    /// 3. Rewrites as a single optimized batch file per shard
    /// 4. Clears the WAL
    pub fn compact_all(&self) -> StorageResult<()> {
        // Compact all shards
        for shard_name in self.persist.list_shards()? {
            self.persist.compact(&shard_name, 0)?; // Compact from time 0 (full compaction)
        }

        // Sync to disk
        self.persist.sync()?;

        // Save metadata
        self.save_knowledge_graphs_metadata()?;

        Ok(())
    }

    /// Flush all buffers to disk without full compaction (legacy compatibility)
    pub fn save_all(&self) -> StorageResult<()> {
        // Flush all shards
        for shard_name in self.persist.list_shards()? {
            self.persist.flush(&shard_name)?;
        }

        // Sync to disk
        self.persist.sync()?;

        self.save_knowledge_graphs_metadata()?;

        Ok(())
    }

    // Rule Management (Persistent Derived Relations)
    /// Register a persistent rule in the current knowledge graph
    pub fn register_rule(
        &self,
        rule_def: &RuleDef,
    ) -> StorageResult<crate::rule_catalog::RuleRegisterResult> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.register_rule_in(&db_name, rule_def)
    }

    /// Register a persistent rule in a specific knowledge graph
    ///
    /// Uses `&self` instead of `&mut self` to enable concurrent writes to different KGs.
    pub fn register_rule_in(
        &self,
        kg: &str,
        rule_def: &RuleDef,
    ) -> StorageResult<crate::rule_catalog::RuleRegisterResult> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        db.register_rule(rule_def)
            .map_err(|e| StorageError::Other(format!("Failed to register rule: {e}")))
    }

    /// Drop a rule from the current knowledge graph
    pub fn drop_rule(&self, name: &str) -> StorageResult<()> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.drop_rule_in(&db_name, name)
    }

    /// Drop a rule from a specific knowledge graph
    ///
    /// Uses `&self` instead of `&mut self` to enable concurrent writes to different KGs.
    pub fn drop_rule_in(&self, kg: &str, name: &str) -> StorageResult<()> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        db.drop_rule(name)
            .map_err(|e| StorageError::Other(format!("Failed to drop rule: {e}")))
    }

    /// List all rules in the current knowledge graph
    pub fn list_rules(&self) -> StorageResult<Vec<String>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?;

        self.list_rules_in(db_name)
    }

    /// List all rules in a specific knowledge graph
    pub fn list_rules_in(&self, kg: &str) -> StorageResult<Vec<String>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let db = db.read();
        Ok(db.list_rules())
    }

    /// Describe a rule in the current knowledge graph
    pub fn describe_rule(&self, name: &str) -> StorageResult<Option<String>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?;

        self.describe_rule_in(db_name, name)
    }

    /// Describe a rule in a specific knowledge graph
    pub fn describe_rule_in(&self, kg: &str, name: &str) -> StorageResult<Option<String>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let db = db.read();
        Ok(db.describe_rule(name))
    }

    /// Clear all clauses from a rule for editing/redefining (current knowledge graph)
    /// The rule remains registered but with no clauses, ready for new clause registration
    pub fn clear_rule(&mut self, name: &str) -> StorageResult<()> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.clear_rule_in(&db_name, name)
    }

    /// Clear all clauses from a rule for editing/redefining (specific knowledge graph)
    pub fn clear_rule_in(&mut self, kg: &str, name: &str) -> StorageResult<()> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        db.clear_rule(name)
            .map_err(|e| StorageError::Other(format!("Failed to clear rule: {e}")))
    }

    /// Replace a specific clause in a rule (current knowledge graph)
    pub fn replace_rule(
        &mut self,
        name: &str,
        index: usize,
        new_rule: crate::statement::SerializableRule,
    ) -> StorageResult<()> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.replace_rule_in(&db_name, name, index, new_rule)
    }

    /// Replace a specific clause in a rule (specific knowledge graph)
    pub fn replace_rule_in(
        &mut self,
        kg: &str,
        name: &str,
        index: usize,
        new_rule: crate::statement::SerializableRule,
    ) -> StorageResult<()> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        db.replace_rule(name, index, new_rule)
            .map_err(|e| StorageError::Other(format!("Failed to replace rule clause: {e}")))
    }

    /// Remove a specific clause from a rule (current knowledge graph)
    /// Returns true if the entire rule was deleted (last clause removed)
    pub fn remove_rule_clause(&self, name: &str, index: usize) -> StorageResult<bool> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.remove_rule_clause_in(&db_name, name, index)
    }

    /// Remove a specific clause from a rule (specific knowledge graph)
    /// Returns true if the entire rule was deleted (last clause removed)
    pub fn remove_rule_clause_in(&self, kg: &str, name: &str, index: usize) -> StorageResult<bool> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        db.remove_rule_clause(name, index)
            .map_err(|e| StorageError::Other(format!("Failed to remove rule clause: {e}")))
    }

    /// Get the number of clauses in a rule (current knowledge graph)
    pub fn rule_count(&self, name: &str) -> StorageResult<Option<usize>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?;

        self.rule_count_in(db_name, name)
    }

    /// Get the number of clauses in a rule (specific knowledge graph)
    pub fn rule_count_in(&self, kg: &str, name: &str) -> StorageResult<Option<usize>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let db = db.read();
        Ok(db.rule_count(name))
    }

    /// Get the arity (number of arguments) of a rule/view (current knowledge graph)
    pub fn rule_arity(&self, name: &str) -> StorageResult<Option<usize>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.rule_arity_in(&db_name, name)
    }

    /// Get the arity (number of arguments) of a rule/view (specific knowledge graph)
    pub fn rule_arity_in(&self, kg: &str, name: &str) -> StorageResult<Option<usize>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let db = db.read();
        Ok(db.rule_arity(name))
    }

    // Schema Catalog (per-KG type validation)
    /// Register a schema for a relation in the current knowledge graph
    pub fn register_schema(&self, schema: RelationSchema) -> StorageResult<()> {
        let kg_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();
        self.register_schema_in(&kg_name, schema)
    }

    /// Register a schema for a relation in a specific knowledge graph
    pub fn register_schema_in(&self, kg: &str, schema: RelationSchema) -> StorageResult<()> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        db.register_schema(schema).map_err(StorageError::Other)
    }

    /// Get schema for a relation in the current knowledge graph
    pub fn get_schema(&self, relation: &str) -> StorageResult<Option<RelationSchema>> {
        let kg_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();
        self.get_schema_in(&kg_name, relation)
    }

    /// Get schema for a relation in a specific knowledge graph
    pub fn get_schema_in(&self, kg: &str, relation: &str) -> StorageResult<Option<RelationSchema>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let db = db.read();
        Ok(db.get_schema(relation).cloned())
    }

    /// Check if a schema exists for a relation in the current knowledge graph
    pub fn has_schema(&self, relation: &str) -> StorageResult<bool> {
        let kg_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();
        self.has_schema_in(&kg_name, relation)
    }

    /// Check if a schema exists for a relation in a specific knowledge graph
    pub fn has_schema_in(&self, kg: &str, relation: &str) -> StorageResult<bool> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let db = db.read();
        Ok(db.has_schema(relation))
    }

    /// Remove schema for a relation in the current knowledge graph
    pub fn remove_schema(&self, relation: &str) -> StorageResult<Option<RelationSchema>> {
        let kg_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();
        self.remove_schema_in(&kg_name, relation)
    }

    /// Remove schema for a relation in a specific knowledge graph
    pub fn remove_schema_in(
        &self,
        kg: &str,
        relation: &str,
    ) -> StorageResult<Option<RelationSchema>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        db.remove_schema(relation).map_err(StorageError::Other)
    }

    /// List all schemas in the current knowledge graph
