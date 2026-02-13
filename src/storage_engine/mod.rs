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
//! let results = storage.execute_query("path(X,Y) <- edge(X,Y)").unwrap();
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
            .iter()
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
    pub fn list_schemas(&self) -> StorageResult<Vec<String>> {
        let kg_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();
        self.list_schemas_in(&kg_name)
    }

    /// List all schemas in a specific knowledge graph
    pub fn list_schemas_in(&self, kg: &str) -> StorageResult<Vec<String>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let db = db.read();
        Ok(db
            .list_schemas()
            .iter()
            .map(std::string::ToString::to_string)
            .collect())
    }

    /// Validate tuples against schema in a specific knowledge graph
    ///
    /// Returns Ok(()) if no schema exists or validation passes.
    /// Returns Err with message if validation fails.
    pub fn validate_tuples_in(
        &self,
        kg: &str,
        relation: &str,
        tuples: &[Tuple],
    ) -> StorageResult<()> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let db = db.read();
        db.validate_tuples(relation, tuples)
            .map_err(StorageError::Other)
    }

    /// Register or update a persistent schema in a specific knowledge graph
    pub fn register_or_update_schema_in(
        &self,
        kg: &str,
        schema: RelationSchema,
    ) -> StorageResult<()> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        db.register_or_update_schema(schema)
            .map_err(StorageError::Other)
    }

    /// Register or update a session schema in a specific knowledge graph (not persisted)
    pub fn register_or_update_session_schema_in(
        &self,
        kg: &str,
        schema: RelationSchema,
    ) -> StorageResult<()> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        db.register_or_update_session_schema(schema)
            .map_err(StorageError::Other)
    }

    /// Execute a query with rules prepended (current knowledge graph)
    ///
    /// Returns binary tuples (i32, i32) for backward compatibility.
    /// For arbitrary arity results, use `execute_query_with_rules_tuples` instead.
    pub fn execute_query_with_rules(&self, program: &str) -> StorageResult<Vec<(i32, i32)>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.execute_query_with_rules_on(&db_name, program)
    }

    /// Execute a query with rules prepended (specific knowledge graph)
    ///
    /// Uses a completely lock-free read path via snapshots.
    ///
    /// Returns binary tuples (i32, i32) for backward compatibility.
    /// For arbitrary arity results, use `execute_query_with_rules_tuples_on` instead.
    pub fn execute_query_with_rules_on(
        &self,
        kg: &str,
        program: &str,
    ) -> StorageResult<Vec<(i32, i32)>> {
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
            .execute_with_rules(program)
            .map_err(|e| StorageError::Other(format!("Query execution failed: {e}")))
    }

    /// Execute a query with rules prepended, returning tuples of arbitrary arity (current knowledge graph)
    pub fn execute_query_with_rules_tuples(&self, program: &str) -> StorageResult<Vec<Tuple>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.execute_query_with_rules_tuples_on(&db_name, program)
    }

    /// Execute a query with rules prepended, returning tuples of arbitrary arity (specific knowledge graph)
    ///
    /// Uses a completely lock-free read path via snapshots.
    pub fn execute_query_with_rules_tuples_on(
        &self,
        kg: &str,
        program: &str,
    ) -> StorageResult<Vec<Tuple>> {
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
            .execute_with_rules_tuples(program)
            .map_err(|e| StorageError::Other(format!("Query execution failed: {e}")))
    }

    /// Execute a query with session facts on a specific knowledge graph
    ///
    /// Session facts are added to an ISOLATED COPY of the snapshot's data,
    /// providing request-scoped isolation. Concurrent queries cannot see
    /// each other's session facts.
    ///
    /// This fixes the race condition where the old approach of:
    /// 1. Insert session facts to shared store
    /// 2. Execute query
    /// 3. Delete session facts
    /// could expose session facts to concurrent queries.
    pub fn execute_query_with_session_facts_on(
        &self,
        kg: &str,
        program: &str,
        session_facts: Vec<(String, Tuple)>,
    ) -> StorageResult<Vec<Tuple>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        // Get snapshot atomically - O(1), no lock needed
        let snapshot = {
            let db_guard = db.read();
            db_guard.snapshot()
        };

        // Execute with session facts on isolated snapshot copy - completely lock-free
        // The session facts are added to a CLONE of the data, not the shared store
        snapshot
            .execute_with_session_facts(program, session_facts)
            .map_err(|e| StorageError::Other(format!("Query execution failed: {e}")))
    }

    /// List all relations (base facts) in the current knowledge graph
    pub fn list_relations(&self) -> StorageResult<Vec<String>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?;

        self.list_relations_in(db_name)
    }

    /// List all relations in a specific knowledge graph
    pub fn list_relations_in(&self, kg: &str) -> StorageResult<Vec<String>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let db = db.read();
        let relations: Vec<String> = db.metadata.relations.keys().cloned().collect();
        Ok(relations)
    }

    /// Describe a relation in the current knowledge graph
    pub fn describe_relation(&self, name: &str) -> StorageResult<Option<String>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?;

        self.describe_relation_in(db_name, name)
    }

    /// Describe a relation in a specific knowledge graph
    pub fn describe_relation_in(&self, kg: &str, name: &str) -> StorageResult<Option<String>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let db = db.read();
        if let Some(rel_meta) = db.metadata.relations.get(name) {
            let desc = format!(
                "Relation: {}\nSchema: {:?}\nTuple count: {}",
                name, rel_meta.schema, rel_meta.tuple_count
            );
            Ok(Some(desc))
        } else {
            Ok(None)
        }
    }

    /// Get relation metadata (schema, tuple count) for the current knowledge graph
    pub fn get_relation_metadata(&self, name: &str) -> StorageResult<Option<(Vec<String>, usize)>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?;

        self.get_relation_metadata_in(db_name, name)
    }

    /// Get relation metadata for a specific knowledge graph
    pub fn get_relation_metadata_in(
        &self,
        kg: &str,
        name: &str,
    ) -> StorageResult<Option<(Vec<String>, usize)>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let db = db.read();
        if let Some(rel_meta) = db.metadata.relations.get(name) {
            Ok(Some((rel_meta.schema.clone(), rel_meta.tuple_count)))
        } else {
            Ok(None)
        }
    }

    /// List relations with metadata for a specific knowledge graph
    pub fn list_relations_with_metadata(
        &self,
        kg: &str,
    ) -> StorageResult<Vec<(String, Vec<String>, usize)>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let db = db.read();
        let relations: Vec<(String, Vec<String>, usize)> = db
            .metadata
            .relations
            .iter()
            .map(|(name, meta)| (name.clone(), meta.schema.clone(), meta.tuple_count))
            .collect();
        Ok(relations)
    }

    /// Load all knowledge graphs from persist layer
    ///
    /// Recovery process:
    /// 1. Discover knowledge graphs from persist shards
    /// 2. For each knowledge graph, read all shards
    /// 3. Consolidate updates to get current state
    /// 4. Populate in-memory `DatalogEngine`
    fn load_all_knowledge_graphs(&mut self) -> StorageResult<()> {
        // Discover knowledge graphs from persist shards
        let shard_names = self.persist.list_shards()?;
        let mut kg_names: std::collections::HashSet<String> = std::collections::HashSet::new();

        for shard in &shard_names {
            if let Some(kg_name) = shard.split(':').next() {
                kg_names.insert(kg_name.to_string());
            }
        }

        // Also check metadata file for knowledge graphs without data yet
        let metadata_path = self
            .config
            .storage
            .data_dir
            .join("metadata/knowledge_graphs.json");
        if metadata_path.exists() {
            if let Ok(metadata) = KnowledgeGraphsMetadata::load(&metadata_path) {
                for kg_info in metadata.knowledge_graphs {
                    kg_names.insert(kg_info.name);
                }
            }
        }

        // Load each knowledge graph
        for kg_name in kg_names {
            let kg_dir = self.config.storage.data_dir.join(&kg_name);
            fs::create_dir_all(&kg_dir)?;

            let kg = self.load_knowledge_graph_from_persist(&kg_name, kg_dir)?;
            self.knowledge_graphs
                .insert(kg_name, Arc::new(RwLock::new(kg)));
        }

        // Update logical time to be after all loaded data
        let max_time = self.find_max_logical_time()?;
        self.logical_time.store(max_time + 1, Ordering::SeqCst);

        Ok(())
    }

    /// Load a single knowledge graph from persist layer
    fn load_knowledge_graph_from_persist(
        &self,
        name: &str,
        data_dir: PathBuf,
    ) -> StorageResult<KnowledgeGraph> {
        let prefix = format!("{name}:");
        let mut engine = DatalogEngine::new();
        let mut metadata = KnowledgeGraphMetadata::new(name.to_string());

        // Find all shards for this knowledge graph
        for shard_name in self.persist.list_shards()? {
            if shard_name.starts_with(&prefix) {
                let relation = match shard_name.strip_prefix(&prefix) {
                    Some(r) => r,
                    None => continue, // Skip malformed shard names
                };

                // Get shard info to determine since frontier
                let info = self.persist.shard_info(&shard_name)?;

                // Read and consolidate updates
                let mut updates = self.persist.read(&shard_name, info.since)?;
                consolidate_to_current(&mut updates);

                // Extract current tuples (positive multiplicities only)
                let tuples = to_tuples(&updates);

                if !tuples.is_empty() {
                    // Infer schema from first tuple
                    let arity = tuples.first().map_or(2, super::value::Tuple::arity);
                    let schema: Vec<String> = (0..arity).map(|i| format!("col{i}")).collect();
                    let tuple_count = tuples.len();

                    // Update metadata with relation info
                    metadata.add_relation(relation.to_string(), schema, tuple_count);

                    engine.add_tuples(relation, tuples);
                }
            }
        }

        // Load view catalog (will load existing views if present)
        let rule_catalog = RuleCatalog::new(data_dir.clone())
            .map_err(|e| StorageError::Other(format!("Failed to load view catalog: {e}")))?;

        // Load schema catalog (will load existing schemas if present)
        let schema_path = data_dir.join("schema.json");
        let schema_catalog = if schema_path.exists() {
            SchemaCatalog::load(&schema_path).unwrap_or_else(|e| {
                eprintln!(
                    "Warning: Failed to load schema catalog for '{name}': {e}. Creating empty catalog."
                );
                SchemaCatalog::new()
            })
        } else {
            SchemaCatalog::new()
        };

        // Create initial snapshot from loaded data
        let num_workers = self.config.storage.performance.num_threads;
        let snapshot = ArcSwap::from_pointee(KnowledgeGraphSnapshot::new_with_workers(
            engine.input_tuples.clone(),
            rule_catalog.all_rules(),
            num_workers,
        ));

        Ok(KnowledgeGraph {
            name: name.to_string(),
            engine,
            metadata,
            data_dir,
            rule_catalog,
            schema_catalog,
            snapshot,
            dd_computation: None,
            num_workers,
        })
    }

    /// Find the maximum logical time across all shards
    fn find_max_logical_time(&self) -> StorageResult<u64> {
        let mut max_time = 0u64;

        for shard_name in self.persist.list_shards()? {
            let info = self.persist.shard_info(&shard_name)?;
            if info.upper > max_time {
                max_time = info.upper;
            }
        }

        Ok(max_time)
    }

    /// Save system-wide knowledge graphs metadata
    fn save_knowledge_graphs_metadata(&self) -> StorageResult<()> {
        let metadata_dir = self.config.storage.data_dir.join("metadata");
        fs::create_dir_all(&metadata_dir)?;

        let knowledge_graphs: Vec<_> = self
            .knowledge_graphs
            .iter()
            .map(|entry| {
                let name = entry.key();
                let kg_lock = entry.value();
                let kg = kg_lock.read();
                crate::storage::metadata::KnowledgeGraphInfo {
                    name: name.clone(),
                    created_at: kg.metadata.created_at.clone(),
                    last_accessed: Utc::now().to_rfc3339(),
                    relations_count: kg.metadata.relations.len(),
                    total_tuples: kg.metadata.total_tuples(),
                }
            })
            .collect();

        let metadata = KnowledgeGraphsMetadata {
            version: "1.0".to_string(),
            knowledge_graphs,
        };

        metadata.save(&metadata_dir.join("knowledge_graphs.json"))?;

        Ok(())
    }

    /// Get reference to the configuration
    pub fn config(&self) -> &Config {
        &self.config
    }

    // Parallel Query Execution API
    /// Execute multiple queries in parallel across different knowledge graphs
    ///
    /// This method leverages Rayon's thread pool to execute queries concurrently,
    /// utilizing all available CPU cores efficiently.
    ///
    /// # Example
    /// ```text
    /// let queries = vec![
    ///     ("kg1", "result(X,Y) <- edge(X,Y)"),
    ///     ("kg2", "result(X,Y) <- person(X,Y)"),
    ///     ("kg3", "result(X,Y) <- data(X,Y)"),
    /// ];
    ///
    /// let results = storage.execute_parallel_queries_on_knowledge_graphs(queries)?;
    /// ```
    pub fn execute_parallel_queries_on_knowledge_graphs(
        &self,
        queries: Vec<(&str, &str)>,
    ) -> StorageResult<Vec<(String, Vec<(i32, i32)>)>> {
        // Use Rayon to execute queries in parallel with lock-free snapshot reads
        let results: Result<Vec<_>, StorageError> = queries
            .par_iter()
            .map(|(kg, program)| {
                // Get knowledge graph
                let kg_lock = self
                    .knowledge_graphs
                    .get(*kg)
                    .ok_or_else(|| StorageError::KnowledgeGraphNotFound((*kg).to_string()))?;

                // Get snapshot atomically - O(1)
                let snapshot = {
                    let kg_guard = kg_lock.read();
                    kg_guard.snapshot()
                };

                // Execute on snapshot - completely lock-free
                let results = snapshot
                    .execute(program)
                    .map_err(|e| StorageError::Other(format!("Query execution failed: {e}")))?;

                Ok(((*kg).to_string(), results))
            })
            .collect();

        results
    }

    /// Execute the same query on multiple knowledge graphs in parallel
    ///
    /// Useful for federated queries or comparing results across knowledge graphs.
    ///
    /// # Example
    /// ```text
    /// let knowledge_graphs = vec!["kg1", "kg2", "kg3"];
    /// let query = "result(X,Y) <- edge(X,Y), X > 5";
    ///
    /// let results = storage.execute_query_on_multiple_knowledge_graphs(knowledge_graphs, query)?;
    /// ```
    pub fn execute_query_on_multiple_knowledge_graphs(
        &self,
        knowledge_graphs: Vec<&str>,
        program: &str,
    ) -> StorageResult<Vec<(String, Vec<(i32, i32)>)>> {
        let queries: Vec<(&str, &str)> = knowledge_graphs.iter().map(|kg| (*kg, program)).collect();

        self.execute_parallel_queries_on_knowledge_graphs(queries)
    }

    /// Execute multiple queries on the same knowledge graph in parallel
    ///
    /// Uses a completely lock-free read path via snapshots. Gets the snapshot once
    /// and shares it across all parallel queries - data is already Arc-wrapped.
    ///
    /// # Example
    /// ```text
    /// let queries = vec![
    ///     "q1(X,Y) <- edge(X,Y)",
    ///     "q2(X,Z) <- path(X,Y), path(Y,Z)",
    ///     "q3(X) <- person(X,_), edge(X,_)",
    /// ];
    ///
    /// let results = storage.execute_parallel_queries_on_knowledge_graph("kg1", queries)?;
    /// ```
    pub fn execute_parallel_queries_on_knowledge_graph(
        &self,
        kg: &str,
        programs: Vec<&str>,
    ) -> StorageResult<Vec<Vec<(i32, i32)>>> {
        // Get knowledge graph
        let kg_lock = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        // Get snapshot atomically - O(1), data is already Arc-wrapped for sharing
        let snapshot = {
            let kg_guard = kg_lock.read();
            kg_guard.snapshot()
        };

        // Execute queries in parallel on the snapshot - completely lock-free
        let results: Result<Vec<_>, StorageError> = programs
            .par_iter()
            .map(|program| {
                snapshot
                    .execute(program)
                    .map_err(|e| StorageError::Other(format!("Query execution failed: {e}")))
            })
            .collect();

        results
    }

    /// Get number of available CPU cores for parallel execution
    pub fn num_cpus(&self) -> usize {
        rayon::current_num_threads()
    }

    /// Configure the Rayon thread pool size
    ///
    /// Must be called before any parallel operations.
    /// Returns Ok(()) if configured successfully, or if already configured (ignored).
    ///
    /// # Note
    /// Rayon's thread pool can only be configured once globally. Subsequent calls
    /// will silently succeed but not change the configuration.
    pub fn set_num_threads(num_threads: usize) -> Result<(), String> {
        match rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
        {
            Ok(()) => Ok(()),
            Err(e) => {
                // Check if it's because pool is already initialized (common case)
                let err_str = format!("{e}");
                if err_str.contains("already initialized") {
                    // Silently succeed - pool was already configured
                    Ok(())
                } else {
                    Err(format!("Failed to configure thread pool: {e}"))
                }
            }
        }
    }
}

impl KnowledgeGraph {
    /// Create a new empty knowledge graph with configurable worker count
    fn new_with_workers(name: String, data_dir: PathBuf, num_workers: usize) -> Self {
        // Create rule catalog (will load existing rules if present)
        let rule_catalog = RuleCatalog::new(data_dir.clone()).unwrap_or_else(|e| {
            // Log warning but create empty catalog to avoid panic
            eprintln!(
                "Warning: Failed to load rule catalog for '{name}': {e}. Creating empty catalog."
            );
            RuleCatalog::empty()
        });

        // Create schema catalog (will load existing schemas if present)
        let schema_path = data_dir.join("schema.json");
        let schema_catalog = if schema_path.exists() {
            SchemaCatalog::load(&schema_path).unwrap_or_else(|e| {
                eprintln!(
                    "Warning: Failed to load schema catalog for '{name}': {e}. Creating empty catalog."
                );
                SchemaCatalog::new()
            })
        } else {
            SchemaCatalog::new()
        };

        // Create initial empty snapshot
        let snapshot = ArcSwap::from_pointee(KnowledgeGraphSnapshot::empty());

        KnowledgeGraph {
            name: name.clone(),
            engine: DatalogEngine::new(),
            metadata: KnowledgeGraphMetadata::new(name),
            data_dir,
            rule_catalog,
            schema_catalog,
            snapshot,
            dd_computation: None,
            num_workers,
        }
    }

    /// Enable the DDComputation for incremental updates.
    ///
    /// Creates a persistent DD computation worker thread for this knowledge graph.
    /// Once enabled, all inserts and deletes are shadow-written to DD.
    /// This is required for reading from arrangements and HNSW indexing.
    ///
    /// # Errors
    /// Returns error if worker thread fails to spawn or replaying existing data fails.
    pub fn enable_dd_computation(&mut self) -> StorageResult<()> {
        if self.dd_computation.is_none() {
            let dd = DDComputation::new(vec![]).map_err(StorageError::DDComputationError)?;

            // Replay existing data into DDComputation so arrangements are
            // populated immediately. This handles the case where data was
            // loaded from persistence before DDComputation was enabled.
            for (relation, tuples) in &self.engine.input_tuples {
                if !tuples.is_empty() {
                    dd.insert(relation, tuples.clone(), 0)
                        .map_err(StorageError::DDComputationError)?;
                }
            }

            self.dd_computation = Some(dd);
        }
        Ok(())
    }

    /// Get a reference to the DDComputation (if enabled).
    ///
    /// Used for reading from DD arrangements and verifying consistency.
    pub fn dd_computation(&self) -> Option<&DDComputation> {
        self.dd_computation.as_ref()
    }

    /// Publish a new snapshot atomically
    ///
    /// Called after data modifications to make changes visible to readers.
    /// This is O(1) - just an atomic pointer swap.
    ///
    /// If DDComputation has valid materializations, includes them
    /// in the snapshot. Materialized tuples are merged into input_tuples,
    /// and their rules are skipped during query execution.
    ///
    /// IMPORTANT: This method holds the DerivedRelationsManager lock through
    /// the entire snapshot creation AND publication to prevent TOCTOU races.
    /// Without this, another thread could invalidate materializations between
    /// reading them and publishing the snapshot.
    fn publish_snapshot(&self) {
        // Start with base relation data
        let mut input_tuples = self.engine.input_tuples.clone();
        let rules = self.rule_catalog.all_rules();

        // Gather valid materializations from DDComputation
        // CRITICAL: Hold the lock through snapshot creation AND publication
        // to prevent TOCTOU race conditions.
        if let Some(ref dd) = self.dd_computation {
            let manager = dd.derived_relations();
            let manager_guard = manager.lock();

            // Get all valid materializations
            let materializations = manager_guard.get_all_valid_materializations();

            // Merge materialized tuples into input_tuples
            // They appear as base facts so the rules don't need to recompute them
            for (rel_name, tuples) in materializations {
                input_tuples
                    .entry(rel_name.clone())
                    .or_default()
                    .extend(tuples);
            }

            // Get names of materialized relations
            let materialized_names = manager_guard.get_materialized_relation_names();

            // Create AND publish snapshot while still holding the lock
            // This ensures no concurrent invalidation can occur between
            // reading materializations and making them visible to readers.
            let new_snapshot = KnowledgeGraphSnapshot::new_with_materializations(
                input_tuples,
                rules,
                self.num_workers,
                materialized_names,
            );
            self.snapshot.store(Arc::new(new_snapshot));

            // Lock drops here AFTER publication - this is the fix for TOCTOU
        } else {
            // No DD computation - publish without materializations
            let new_snapshot = KnowledgeGraphSnapshot::new_with_materializations(
                input_tuples,
                rules,
                self.num_workers,
                HashSet::new(),
            );
            self.snapshot.store(Arc::new(new_snapshot));
        }
    }

    /// Get the current snapshot for lock-free reads
    ///
    /// Returns an Arc to the current snapshot. This is O(1) and lock-free.
    pub fn snapshot(&self) -> Arc<KnowledgeGraphSnapshot> {
        self.snapshot.load_full()
    }

    /// Materialize a derived relation and publish a new snapshot
    ///
    /// This is the proper way to store materialized data:
    /// 1. Stores the tuples in DDComputation's DerivedRelationsManager
    /// 2. Publishes a new snapshot that includes the materialized data
    ///
    /// After this call, queries via `snapshot()` will see the materialized data.
    pub fn materialize_derived_relation(
        &self,
        relation: &str,
        tuples: Vec<Tuple>,
    ) -> Result<(), String> {
        if let Some(ref dd) = self.dd_computation {
            dd.set_materialized(relation, tuples)?;
            self.publish_snapshot();
            Ok(())
        } else {
            Err("DDComputation not enabled".to_string())
        }
    }

    /// Insert tuples into in-memory state only
    ///
    /// Persistence is handled by `StorageEngine` via the persist layer.
    /// Returns (`new_count`, `duplicate_count`) for caller to report.
    ///
    /// # Errors
    /// Returns error if DD shadow write fails.
    fn insert_in_memory(
        &mut self,
        relation: &str,
        tuples: Vec<Tuple>,
        time: u64,
    ) -> StorageResult<(usize, usize)> {
        // Infer schema from first tuple if available
        let schema = if let Some(first) = tuples.first() {
            (0..first.arity())
                .map(|i| format!("col{i}"))
                .collect::<Vec<_>>()
        } else {
            vec!["col0".to_string(), "col1".to_string()]
        };

        // Update in-memory production format (input_tuples)
        let mut new_count = 0;
        let mut dup_count = 0;
        let mut new_tuples_for_dd = Vec::new();

        // Get or create the relation's tuple storage
        let existing_tuples = self
            .engine
            .input_tuples
            .entry(relation.to_string())
            .or_default();

        for tuple in tuples {
            if existing_tuples.contains(&tuple) {
                dup_count += 1;
            } else {
                new_tuples_for_dd.push(tuple.clone());
                existing_tuples.push(tuple);
                new_count += 1;
            }
        }
        let tuple_count = existing_tuples.len();

        // Update metadata
        self.metadata
            .add_relation(relation.to_string(), schema, tuple_count);

        // Shadow write new tuples to DDComputation (if enabled).
        // Uses the logical timestamp from StorageEngine for proper time tracking.
        // Time advancement is lazy  -  only happens when a consistent read is requested.
        if !new_tuples_for_dd.is_empty() {
            if let Some(dd) = &self.dd_computation {
                dd.insert(relation, new_tuples_for_dd, time)
                    .map_err(StorageError::DDComputationError)?;
                // Invalidate derived relations that depend on this base
                dd.notify_base_update(relation)
                    .map_err(StorageError::DDComputationError)?;
                // Invalidate indexes that depend on this base relation
                dd.notify_indexes_base_update(relation)
                    .map_err(StorageError::DDComputationError)?;
                // Auto-rematerialize invalidated rules
                self.auto_rematerialize_invalid_rules();
            }
        }

        // Publish new snapshot for lock-free reads
        self.publish_snapshot();

        Ok((new_count, dup_count))
    }

    /// Delete tuples from in-memory state only
    ///
    /// Persistence is handled by `StorageEngine` via the persist layer.
    /// Returns the count of actually deleted tuples.
    ///
    /// # Errors
    /// Returns error if DD shadow write fails.
    fn delete_in_memory(
        &mut self,
        relation: &str,
        tuples_to_remove: &[Tuple],
        time: u64,
    ) -> StorageResult<usize> {
        // Get schema from metadata (which has the correct arity from insert time)
        // Avoid using catalog which may not have the schema for base facts
        let schema = self.metadata.relations.get(relation).map_or_else(
            || vec!["col0".to_string(), "col1".to_string()],
            |r| r.schema.clone(),
        );

        let mut found = false;
        let mut final_count = 0;
        let mut deleted_count = 0;

        // Collect actually-deleted tuples for DD shadow write
        let mut deleted_tuples_for_dd = Vec::new();

        // Update production format (input_tuples)
        if let Some(existing) = self.engine.input_tuples.get_mut(relation) {
            // Find which tuples will actually be deleted
            for t in tuples_to_remove {
                if existing.contains(t) {
                    deleted_tuples_for_dd.push(t.clone());
                }
            }
            let count_before = existing.len();
            // Remove tuples
            existing.retain(|tuple| !tuples_to_remove.contains(tuple));
            final_count = existing.len();
            deleted_count = count_before - final_count;
            found = true;
        }

        // Update metadata if we found and modified data
        if found {
            self.metadata
                .add_relation(relation.to_string(), schema, final_count);

            // Shadow write deletes to DDComputation (only if DD exists).
            // Uses the logical timestamp from StorageEngine.
            if !deleted_tuples_for_dd.is_empty() {
                if let Some(dd) = &self.dd_computation {
                    dd.delete(relation, deleted_tuples_for_dd, time)
                        .map_err(StorageError::DDComputationError)?;
                    // Invalidate derived relations that depend on this base
                    dd.notify_base_update(relation)
                        .map_err(StorageError::DDComputationError)?;
                    // Invalidate indexes that depend on this base relation
                    dd.notify_indexes_base_update(relation)
                        .map_err(StorageError::DDComputationError)?;
                    // Auto-rematerialize invalidated rules
                    self.auto_rematerialize_invalid_rules();
                }
            }

            // Publish new snapshot for lock-free reads
            self.publish_snapshot();
        }

        Ok(deleted_count)
    }

    /// Get knowledge graph name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get knowledge graph metadata
    pub fn metadata(&self) -> &KnowledgeGraphMetadata {
        &self.metadata
    }

    // View Management
    /// Register a persistent view
    /// Returns whether view was created or rule was added
    ///
    /// When a persistent rule is registered, we automatically:
    /// 1. Register it with DDComputation for dependency tracking
    /// 2. Execute the rule against current base data
    /// 3. Store the results as materialized data
    /// This enables session rules to immediately use the materialized output.
    pub fn register_rule(
        &mut self,
        rule_def: &RuleDef,
    ) -> Result<crate::rule_catalog::RuleRegisterResult, String> {
        let result = self.rule_catalog.register_rule(rule_def)?;

        // Register with DDComputation for materialization
        if let Some(ref dd) = self.dd_computation {
            let compiled_rule = self.compile_rule_for_dd(rule_def);
            if let Err(e) = dd.register_rule(compiled_rule) {
                eprintln!("Warning: failed to register rule with DDComputation: {e}");
            }

            // Auto-materialize the rule
            // Execute the rule against current base data and store results
            if let Err(e) = self.auto_materialize_rule(&rule_def.name) {
                eprintln!(
                    "Warning: failed to auto-materialize rule '{}': {e}",
                    rule_def.name
                );
            }
        }

        self.publish_snapshot();
        Ok(result)
    }

    /// Auto-materialize a single rule by executing it and storing results
    ///
    /// This is called when a rule is registered to ensure session rules
    /// can immediately use the materialized output.
    fn auto_materialize_rule(&self, rule_name: &str) -> Result<(), String> {
        // Get the rule definition
        let rule = self
            .rule_catalog
            .get(rule_name)
            .ok_or_else(|| format!("Rule '{rule_name}' not found"))?;

        // Build program from all rule clauses
        let clauses = rule.to_rules();
        if clauses.is_empty() {
            return Ok(());
        }

        // Build the query program
        let mut program = String::new();
        for clause in &clauses {
            program.push_str(&format_rule(clause));
            program.push('\n');
        }

        // Query for all results: ?rule_name(X, Y, ...)
        // We need to figure out the arity from the head
        let first_clause = &clauses[0];
        let arity = first_clause.head.effective_arity();
        let vars: Vec<String> = (0..arity).map(|i| format!("V{i}")).collect();
        let query = format!("?{}({})", rule_name, vars.join(", "));
        program.push_str(&query);

        // Execute using a fresh engine with cloned data (like snapshot execution)
        // This avoids needing &mut self
        let mut temp_engine = crate::DatalogEngine::new();
        temp_engine
            .input_tuples
            .clone_from(&self.engine.input_tuples);
        temp_engine.set_num_workers(self.num_workers);
        let tuples = temp_engine.execute_tuples(&program)?;

        // Store as materialized
        if let Some(ref dd) = self.dd_computation {
            dd.set_materialized(rule_name, tuples)?;
        }

        Ok(())
    }

    /// Auto-rematerialize all invalid derived relations
    ///
    /// Called after base data changes to ensure materializations stay current.
    /// This enables session rules to always see up-to-date materialized data.
    fn auto_rematerialize_invalid_rules(&self) {
        let dd = match &self.dd_computation {
            Some(dd) => dd,
            None => return,
        };

        // Get list of invalid (needs rematerialization) relations
        let invalid_relations = {
            let manager = dd.derived_relations();
            let guard = manager.lock();
            guard.get_invalid_relations()
        };

        // Rematerialize each invalid relation
        for relation in invalid_relations {
            if let Err(e) = self.auto_materialize_rule(&relation) {
                // Log warning but don't fail - best effort rematerialization
                eprintln!("Warning: failed to rematerialize '{relation}': {e}");
            }
        }
    }

    /// Compile a RuleDef into a CompiledRule for DDComputation
    fn compile_rule_for_dd(&self, rule_def: &RuleDef) -> CompiledRule {
        use std::collections::HashSet;

        let name = rule_def.name.clone();

        // Extract dependencies from rule body
        let mut dependencies: HashSet<String> = HashSet::new();
        for body_pred in &rule_def.rule.body {
            if let SerializableBodyPred::Atom { relation, .. } = body_pred {
                // Don't count the rule's own head as a dependency (for recursive rules)
                if relation != &name {
                    dependencies.insert(relation.clone());
                }
            }
        }

        // Check if rule is recursive (references itself in body)
        let is_recursive = rule_def.rule.body.iter().any(|p| {
            if let SerializableBodyPred::Atom { relation, .. } = p {
                relation == &name
            } else {
                false
            }
        });

        // Extract output schema from head args
        let output_schema: Vec<String> = rule_def
            .rule
            .head_args
            .iter()
            .enumerate()
            .map(|(i, _)| format!("col{i}"))
            .collect();

        CompiledRule {
            name,
            clauses: vec![], // IR compilation deferred to execution time
            dependencies,
            is_recursive,
            output_schema,
            stratum: 0, // Stratum computed by RuleCatalog
        }
    }

    /// Drop a view
    pub fn drop_rule(&mut self, name: &str) -> Result<(), String> {
        self.rule_catalog.drop(name)?;

        // Remove from DDComputation
        if let Some(ref dd) = self.dd_computation {
            if let Err(e) = dd.remove_rule(name) {
                eprintln!("Warning: failed to remove rule from DDComputation: {e}");
            }
        }

        self.publish_snapshot();
        Ok(())
    }

    /// List all views
    pub fn list_rules(&self) -> Vec<String> {
        self.rule_catalog.list()
    }

    /// Describe a view
    pub fn describe_rule(&self, name: &str) -> Option<String> {
        self.rule_catalog.describe(name)
    }

    /// Check if a view exists
    pub fn rule_exists(&self, name: &str) -> bool {
        self.rule_catalog.exists(name)
    }

    /// Clear all rules from a view for editing/redefining
    /// The view remains registered but with no rules, ready for new rule registration
    pub fn clear_rule(&mut self, name: &str) -> Result<(), String> {
        self.rule_catalog.clear_rules(name)?;
        self.publish_snapshot();
        Ok(())
    }

    /// Replace a specific rule in a view by index (0-based)
    pub fn replace_rule(
        &mut self,
        name: &str,
        index: usize,
        new_rule: crate::statement::SerializableRule,
    ) -> Result<(), String> {
        self.rule_catalog.replace_rule(name, index, new_rule)?;
        self.publish_snapshot();
        Ok(())
    }

    /// Remove a specific clause from a rule by index (0-based)
    /// Returns true if the entire rule was deleted (last clause removed)
    pub fn remove_rule_clause(&mut self, name: &str, index: usize) -> Result<bool, String> {
        let result = self.rule_catalog.remove_rule_clause(name, index)?;
        self.publish_snapshot();
        Ok(result)
    }

    /// Get the number of rules in a view
    pub fn rule_count(&self, name: &str) -> Option<usize> {
        self.rule_catalog.rule_count(name)
    }

    /// Get the arity (number of arguments) of a rule/view
    pub fn rule_arity(&self, name: &str) -> Option<usize> {
        self.rule_catalog.rule_arity(name)
    }

    /// Execute a query with views prepended
    ///
    /// This prepends all view rules to the query, allowing DD to incrementally
    /// compute view results based on base facts.
    ///
    /// Rules for materialized relations are skipped - their data is already
    /// present in the snapshot as base facts.
    pub fn execute_with_rules(&mut self, program: &str) -> Result<Vec<(i32, i32)>, String> {
        // Get all view rules
        let rule_defs = self.rule_catalog.all_rules();

        if rule_defs.is_empty() {
            // No views, just execute normally
            return self.engine.execute(program);
        }

        // Get materialized relation names (skip their rules)
        let materialized: HashSet<String> = if let Some(ref dd) = self.dd_computation {
            let manager = dd.derived_relations();
            let guard = manager.lock();
            guard.get_materialized_relation_names()
        } else {
            HashSet::new()
        };

        // Build the combined program: view rules + query
        // Skip rules whose head relation is materialized
        let mut combined = String::new();

        // Add view rules (skip materialized)
        for rule in &rule_defs {
            if materialized.contains(&rule.head.relation) {
                continue; // Data already available as base facts
            }
            combined.push_str(&format_rule(rule));
            combined.push('\n');
        }

        // Add the query
        combined.push_str(program);

        // Execute combined program
        self.engine.execute(&combined)
    }

    /// Execute a query with views prepended, returning tuples of arbitrary arity
    ///
    /// This prepends all view rules to the query, allowing DD to incrementally
    /// compute view results based on base facts.
    ///
    /// Rules for materialized relations are skipped - their data is already
    /// present in the snapshot as base facts.
    pub fn execute_with_rules_tuples(&mut self, program: &str) -> Result<Vec<Tuple>, String> {
        // Get all view rules
        let rule_defs = self.rule_catalog.all_rules();

        if rule_defs.is_empty() {
            // No views, just execute normally
            return self.engine.execute_tuples(program);
        }

        // Get materialized relation names (skip their rules)
        let materialized: HashSet<String> = if let Some(ref dd) = self.dd_computation {
            let manager = dd.derived_relations();
            let guard = manager.lock();
            guard.get_materialized_relation_names()
        } else {
            HashSet::new()
        };

        // Build the combined program: view rules + query
        // Skip rules whose head relation is materialized
        let mut combined = String::new();
        let mut skipped_count = 0;

        // Add view rules (skip materialized)
        for rule in &rule_defs {
            if materialized.contains(&rule.head.relation) {
                skipped_count += 1;
                continue; // Data already available as base facts
            }
            combined.push_str(&format_rule(rule));
            combined.push('\n');
        }

        // Add the query
        combined.push_str(program);

        if std::env::var("IL_DEBUG").is_ok() {
            eprintln!(
                "DEBUG execute_with_rules_tuples: {} view rules ({} skipped as materialized), program = {}",
                rule_defs.len(),
                skipped_count,
                combined.replace('\n', " | ")
            );
        }

        // Execute combined program
        self.engine.execute_tuples(&combined)
    }

    /// Get reference to view catalog
    pub fn rule_catalog(&self) -> &RuleCatalog {
        &self.rule_catalog
    }

    /// Get mutable reference to view catalog
    pub fn rule_catalog_mut(&mut self) -> &mut RuleCatalog {
        &mut self.rule_catalog
    }

    // Schema Catalog (per-KG type validation)
    /// Get reference to schema catalog
    pub fn schema_catalog(&self) -> &SchemaCatalog {
        &self.schema_catalog
    }

    /// Get mutable reference to schema catalog
    pub fn schema_catalog_mut(&mut self) -> &mut SchemaCatalog {
        &mut self.schema_catalog
    }

    /// Register a persistent schema for a relation
    ///
    /// Returns error if schema already exists or is invalid.
    /// Saves the catalog to disk on success.
    pub fn register_schema(&mut self, schema: RelationSchema) -> Result<(), String> {
        self.schema_catalog
            .register(schema)
            .map_err(|e| format!("{e}"))?;
        self.save_schema_catalog()?;
        Ok(())
    }

    /// Register or update a persistent schema for a relation
    ///
    /// Overwrites any existing schema. Saves to disk on success.
    pub fn register_or_update_schema(&mut self, schema: RelationSchema) -> Result<(), String> {
        self.schema_catalog
            .register_or_update(schema)
            .map_err(|e| format!("{e}"))?;
        self.save_schema_catalog()?;
        Ok(())
    }

    /// Register a session schema for a relation (not persisted)
    ///
    /// Session schemas are cleared when the knowledge graph is reloaded.
    pub fn register_session_schema(&mut self, schema: RelationSchema) -> Result<(), String> {
        self.schema_catalog
            .register_session(schema)
            .map_err(|e| format!("{e}"))
    }

    /// Register or update a session schema for a relation (not persisted)
    pub fn register_or_update_session_schema(
        &mut self,
        schema: RelationSchema,
    ) -> Result<(), String> {
        self.schema_catalog
            .register_or_update_session(schema)
            .map_err(|e| format!("{e}"))
    }

    /// Clear all session schemas (called on disconnect/session end)
    pub fn clear_session_schemas(&mut self) {
        self.schema_catalog.clear_session();
    }

    /// Get schema for a relation (if registered)
    pub fn get_schema(&self, relation: &str) -> Option<&RelationSchema> {
        self.schema_catalog.get(relation)
    }

    /// Check if a schema exists for a relation
    pub fn has_schema(&self, relation: &str) -> bool {
        self.schema_catalog.has_schema(relation)
    }

    /// Remove schema for a relation
    ///
    /// Saves the catalog to disk on success.
    pub fn remove_schema(&mut self, relation: &str) -> Result<Option<RelationSchema>, String> {
        let removed = self.schema_catalog.remove(relation);
        if removed.is_some() {
            self.save_schema_catalog()?;
        }
        Ok(removed)
    }

    /// List all registered schemas
    pub fn list_schemas(&self) -> Vec<&str> {
        self.schema_catalog.relations()
    }

    /// Validate tuples against schema (if one exists)
    ///
    /// Returns Ok(()) if no schema exists or validation passes.
    /// Returns Err with message if validation fails.
    pub fn validate_tuples(&self, relation: &str, tuples: &[Tuple]) -> Result<(), String> {
        if let Some(schema) = self.schema_catalog.get(relation) {
            let mut engine = ValidationEngine::new();
            engine
                .validate_batch(schema, tuples)
                .map_err(|e| format!("{e}"))?;
        }
        Ok(())
    }

    /// Save schema catalog to disk
    fn save_schema_catalog(&self) -> Result<(), String> {
        let schema_path = self.data_dir.join("schema.json");
        self.schema_catalog
            .save(&schema_path)
            .map_err(|e| format!("Failed to save schema catalog: {e}"))
    }
}

/// Format a Rule as a Datalog string (uses Rule's Display impl)
fn format_rule(rule: &crate::ast::Rule) -> String {
    rule.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tempfile::TempDir;

    fn create_test_config(data_dir: PathBuf) -> Config {
        let mut config = Config::default();
        config.storage.data_dir = data_dir;
        config
    }

    #[test]
    fn test_create_and_list_knowledge_graphs() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let storage = StorageEngine::new(config).unwrap();

        // Should have default knowledge graph
        assert!(storage
            .list_knowledge_graphs()
            .contains(&"default".to_string()));

        // Create new knowledge graphs
        storage.create_knowledge_graph("kg1").unwrap();
        storage.create_knowledge_graph("kg2").unwrap();

        let knowledge_graphs = storage.list_knowledge_graphs();
        assert_eq!(knowledge_graphs.len(), 3);
        assert!(knowledge_graphs.contains(&"default".to_string()));
        assert!(knowledge_graphs.contains(&"kg1".to_string()));
        assert!(knowledge_graphs.contains(&"kg2".to_string()));
    }

    #[test]
    fn test_use_knowledge_graph() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("test_kg").unwrap();
        storage.use_knowledge_graph("test_kg").unwrap();

        assert_eq!(storage.current_knowledge_graph(), Some("test_kg"));
    }

    #[test]
    fn test_knowledge_graph_isolation() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();

        // KG1: Insert edge data
        storage.create_knowledge_graph("kg1").unwrap();
        storage.use_knowledge_graph("kg1").unwrap();
        storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();

        // KG2: Should not see edge data
        storage.create_knowledge_graph("kg2").unwrap();
        storage.use_knowledge_graph("kg2").unwrap();

        // Query for edge in kg2 - should return empty results (knowledge graph isolation)
        let result = storage.execute_query("result(X,Y) <- edge(X,Y)").unwrap();
        assert_eq!(result.len(), 0); // No edge relation in kg2 - empty result
    }

    #[test]
    fn test_persistence_roundtrip() {
        let temp = TempDir::new().unwrap();

        // Create and populate
        {
            let config = create_test_config(temp.path().to_path_buf());
            let mut storage = StorageEngine::new(config).unwrap();

            storage.create_knowledge_graph("persist_test").unwrap();
            storage.use_knowledge_graph("persist_test").unwrap();
            storage
                .insert("edge", vec![(1, 2), (2, 3), (3, 4)])
                .unwrap();
            storage.save_all().unwrap();
        }

        // Reload
        {
            let config = create_test_config(temp.path().to_path_buf());
            let mut storage = StorageEngine::new(config).unwrap();

            storage.use_knowledge_graph("persist_test").unwrap();

            let result = storage.execute_query("result(X,Y) <- edge(X,Y)").unwrap();
            assert_eq!(result.len(), 3);
            assert!(result.contains(&(1, 2)));
            assert!(result.contains(&(2, 3)));
            assert!(result.contains(&(3, 4)));
        }
    }

    #[test]
    fn test_cannot_drop_default() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();

        let result = storage.drop_knowledge_graph("default");
        assert!(matches!(result, Err(StorageError::CannotDropDefault)));
    }

    #[test]
    fn test_cannot_drop_current() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("test").unwrap();
        storage.use_knowledge_graph("test").unwrap();

        let result = storage.drop_knowledge_graph("test");
        assert!(matches!(
            result,
            Err(StorageError::CannotDropCurrentKnowledgeGraph)
        ));
    }

    #[test]
    fn test_recursive_view_transitive_closure() {
        use crate::ast::{Atom, BodyPredicate, Rule, Term};
        use crate::statement::RuleDef;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Insert edge data: 1->2->3->4
        storage
            .insert("edge", vec![(1, 2), (2, 3), (3, 4)])
            .unwrap();

        // Define first rule: connected(X, Y) <- edge(X, Y)
        let rule1 = Rule::new(
            Atom::new(
                "connected".to_string(),
                vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Y".to_string()),
                ],
            ),
            vec![BodyPredicate::Positive(Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Y".to_string()),
                ],
            ))],
        );
        let rule_def1 = RuleDef {
            name: "connected".to_string(),
            rule: crate::statement::SerializableRule::from_rule(&rule1),
        };
        storage.register_rule(&rule_def1).unwrap();

        // Define second rule: connected(X, Z) <- edge(X, Y), connected(Y, Z)
        let rule2 = Rule::new(
            Atom::new(
                "connected".to_string(),
                vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Z".to_string()),
                ],
            ),
            vec![
                BodyPredicate::Positive(Atom::new(
                    "edge".to_string(),
                    vec![
                        Term::Variable("X".to_string()),
                        Term::Variable("Y".to_string()),
                    ],
                )),
                BodyPredicate::Positive(Atom::new(
                    "connected".to_string(),
                    vec![
                        Term::Variable("Y".to_string()),
                        Term::Variable("Z".to_string()),
                    ],
                )),
            ],
        );
        let rule_def2 = RuleDef {
            name: "connected".to_string(),
            rule: crate::statement::SerializableRule::from_rule(&rule2),
        };
        storage.register_rule(&rule_def2).unwrap();

        // Check views are registered
        let views = storage.list_rules().unwrap();
        println!("Views: {:?}", views);
        assert!(
            views.contains(&"connected".to_string()),
            "View 'connected' should exist"
        );

        // Check describe_rule shows both rules
        let desc = storage.describe_rule("connected").unwrap();
        println!("View description:\n{}", desc.as_ref().unwrap());

        // Debug: print the combined program
        {
            let kg = storage
                .knowledge_graphs
                .get("default")
                .expect("default KG should exist");
            let kg = kg.read();
            let rule_defs = kg.rule_catalog.all_rules();
            println!("Number of view rules: {}", rule_defs.len());
            for (i, rule) in rule_defs.iter().enumerate() {
                println!("Rule {}: {}", i, format_rule(rule));
            }
        }

        // Query all connected pairs
        eprintln!("\n=== Executing query with views ===");
        let result = storage
            .execute_query_with_rules("result(X,Y) <- connected(X,Y)")
            .unwrap();
        println!("All connected pairs: {:?}", result);

        // Expected transitive closure: (1,2), (2,3), (3,4), (1,3), (2,4), (1,4)
        assert!(
            result.len() >= 6,
            "Should have at least 6 connected pairs, got {}",
            result.len()
        );
        assert!(result.contains(&(1, 2)), "Should contain (1, 2)");
        assert!(result.contains(&(2, 3)), "Should contain (2, 3)");
        assert!(result.contains(&(3, 4)), "Should contain (3, 4)");
        assert!(
            result.contains(&(1, 3)),
            "Should contain (1, 3) - transitive"
        );
        assert!(
            result.contains(&(2, 4)),
            "Should contain (2, 4) - transitive"
        );
        assert!(
            result.contains(&(1, 4)),
            "Should contain (1, 4) - transitive"
        );

        // Query specific: connected(1, 3) - should return 1 row
        // Use constants directly in the atom instead of constraint syntax
        let specific_result = storage
            .execute_query_with_rules("result(1, 3) <- connected(1, 3)")
            .unwrap();
        println!("connected(1, 3): {:?}", specific_result);
        assert_eq!(
            specific_result.len(),
            1,
            "Should find exactly one (1, 3) connection"
        );
        assert_eq!(specific_result[0], (1, 3));
    }

    #[test]
    fn test_dd_shadow_writes_receive_inserts() {
        use crate::value::Value;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Enable DDComputation for shadow writes
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        // Insert tuples through StorageEngine
        storage
            .insert_tuples(
                "edge",
                vec![
                    Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                    Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                    Tuple::new(vec![Value::Int32(3), Value::Int32(4)]),
                ],
            )
            .unwrap();

        // Access the KG's DDComputation and verify it received the data
        let kg = storage
            .knowledge_graphs
            .get("default")
            .expect("default KG should exist");
        let kg = kg.read();
        let dd = kg
            .dd_computation()
            .expect("DDComputation should be enabled");

        // Use consistent read  -  lazily advances time and waits
        let dd_tuples = dd.read_relation_consistent("edge").unwrap();
        assert_eq!(
            dd_tuples.len(),
            3,
            "DDComputation should have received all 3 tuples"
        );
    }

    #[test]
    fn test_dd_shadow_writes_skip_duplicates() {
        use crate::value::Value;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Enable DDComputation
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        let t1 = Tuple::new(vec![Value::Int32(1), Value::Int32(2)]);
        let t2 = Tuple::new(vec![Value::Int32(2), Value::Int32(3)]);

        // Insert first batch
        storage
            .insert_tuples("data", vec![t1.clone(), t2.clone()])
            .unwrap();

        // Insert again  -  t1 is duplicate, t3 is new
        let t3 = Tuple::new(vec![Value::Int32(3), Value::Int32(4)]);
        storage
            .insert_tuples("data", vec![t1.clone(), t3.clone()])
            .unwrap();

        // Verify DDComputation has exactly 3 tuples (not 4 with duplicate)
        let kg = storage.knowledge_graphs.get("default").expect("default KG");
        let kg = kg.read();
        let dd = kg.dd_computation().unwrap();

        let dd_tuples = dd.read_relation_consistent("data").unwrap();
        assert_eq!(
            dd_tuples.len(),
            3,
            "DDComputation should have 3 unique tuples (duplicates filtered)"
        );
    }

    #[test]
    fn test_dd_shadow_writes_handle_deletes() {
        use crate::value::Value;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Enable DDComputation
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        let t1 = Tuple::new(vec![Value::Int32(1), Value::Int32(2)]);
        let t2 = Tuple::new(vec![Value::Int32(2), Value::Int32(3)]);
        let t3 = Tuple::new(vec![Value::Int32(3), Value::Int32(4)]);

        // Insert 3 tuples
        storage
            .insert_tuples("rel", vec![t1.clone(), t2.clone(), t3.clone()])
            .unwrap();

        // Delete one tuple
        storage.delete_tuple("rel", &t2).unwrap();

        // Verify DDComputation reflects the delete
        let kg = storage.knowledge_graphs.get("default").expect("default KG");
        let kg = kg.read();
        let dd = kg.dd_computation().unwrap();

        let dd_tuples = dd.read_relation_consistent("rel").unwrap();
        assert_eq!(
            dd_tuples.len(),
            2,
            "DDComputation should have 2 tuples after delete"
        );
        assert!(dd_tuples.contains(&t1));
        assert!(dd_tuples.contains(&t3));
        assert!(!dd_tuples.contains(&t2));
    }

    #[test]
    fn test_dd_shadow_writes_legacy_tuple2() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Enable DDComputation
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        // Insert via binary tuple API
        storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();

        // Verify DDComputation received the data
        let kg = storage.knowledge_graphs.get("default").expect("default KG");
        let kg = kg.read();
        let dd = kg
            .dd_computation()
            .expect("DDComputation should be enabled");

        let dd_tuples = dd.read_relation_consistent("edge").unwrap();
        assert_eq!(dd_tuples.len(), 2, "DDComputation should have 2 tuples");
    }

    // Arrangement Read Consistency Verification Tests
    //
    // These tests verify that DD arrangement reads produce exactly the same
    // data as the HashMap in-memory state, proving the arrangement read path
    // is correct and ready for HNSW indexing.

    #[test]
    fn test_dd_arrangement_read_parity_with_hashmap() {
        use crate::value::Value;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        // Insert data
        storage
            .insert_tuples(
                "edge",
                vec![
                    Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                    Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                    Tuple::new(vec![Value::Int32(3), Value::Int32(4)]),
                ],
            )
            .unwrap();

        // Read from HashMap (via snapshot) and from DD arrangement
        let kg = storage.knowledge_graphs.get("default").unwrap();
        let kg = kg.read();

        // HashMap state
        let hashmap_tuples = kg.engine.input_tuples.get("edge").unwrap();

        // DD arrangement state
        let dd = kg.dd_computation().unwrap();
        let mut dd_tuples = dd.read_relation_consistent("edge").unwrap();
        dd_tuples.sort();

        let mut hashmap_sorted: Vec<_> = hashmap_tuples.clone();
        hashmap_sorted.sort();

        // Verify exact parity
        assert_eq!(
            hashmap_sorted, dd_tuples,
            "DD arrangement should contain exactly the same tuples as HashMap"
        );
    }

    #[test]
    fn test_dd_arrangement_parity_after_multi_batch_inserts() {
        use crate::value::Value;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        // Batch 1
        storage
            .insert_tuples(
                "data",
                vec![
                    Tuple::new(vec![Value::Int32(1)]),
                    Tuple::new(vec![Value::Int32(2)]),
                ],
            )
            .unwrap();

        // Batch 2 (includes a duplicate)
        storage
            .insert_tuples(
                "data",
                vec![
                    Tuple::new(vec![Value::Int32(2)]), // duplicate
                    Tuple::new(vec![Value::Int32(3)]),
                ],
            )
            .unwrap();

        // Batch 3
        storage
            .insert_tuples(
                "data",
                vec![
                    Tuple::new(vec![Value::Int32(4)]),
                    Tuple::new(vec![Value::Int32(5)]),
                ],
            )
            .unwrap();

        // Verify parity
        let kg = storage.knowledge_graphs.get("default").unwrap();
        let kg = kg.read();
        let hashmap_tuples = kg.engine.input_tuples.get("data").unwrap();
        let dd = kg.dd_computation().unwrap();
        let mut dd_tuples = dd.read_relation_consistent("data").unwrap();
        dd_tuples.sort();

        let mut hashmap_sorted: Vec<_> = hashmap_tuples.clone();
        hashmap_sorted.sort();

        assert_eq!(
            hashmap_sorted.len(),
            5,
            "Should have 5 unique tuples in HashMap"
        );
        assert_eq!(
            hashmap_sorted, dd_tuples,
            "DD arrangement should match HashMap after multi-batch inserts with duplicates"
        );
    }

    #[test]
    fn test_dd_arrangement_parity_after_inserts_and_deletes() {
        use crate::value::Value;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        let t1 = Tuple::new(vec![Value::Int32(1), Value::string("a")]);
        let t2 = Tuple::new(vec![Value::Int32(2), Value::string("b")]);
        let t3 = Tuple::new(vec![Value::Int32(3), Value::string("c")]);
        let t4 = Tuple::new(vec![Value::Int32(4), Value::string("d")]);

        // Insert 4 tuples
        storage
            .insert_tuples(
                "mixed",
                vec![t1.clone(), t2.clone(), t3.clone(), t4.clone()],
            )
            .unwrap();

        // Delete 2 tuples
        storage.delete_tuple("mixed", &t2).unwrap();
        storage.delete_tuple("mixed", &t4).unwrap();

        // Insert one more
        let t5 = Tuple::new(vec![Value::Int32(5), Value::string("e")]);
        storage.insert_tuples("mixed", vec![t5.clone()]).unwrap();

        // Verify parity
        let kg = storage.knowledge_graphs.get("default").unwrap();
        let kg = kg.read();
        let hashmap_tuples = kg.engine.input_tuples.get("mixed").unwrap();
        let dd = kg.dd_computation().unwrap();
        let mut dd_tuples = dd.read_relation_consistent("mixed").unwrap();
        dd_tuples.sort();

        let mut hashmap_sorted: Vec<_> = hashmap_tuples.clone();
        hashmap_sorted.sort();

        assert_eq!(hashmap_sorted.len(), 3, "Should have t1, t3, t5");
        assert_eq!(
            hashmap_sorted, dd_tuples,
            "DD arrangement should match HashMap after mixed inserts and deletes"
        );
    }

    #[test]
    fn test_dd_arrangement_parity_multi_relation() {
        use crate::value::Value;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        // Insert into multiple relations
        storage
            .insert_tuples(
                "edges",
                vec![
                    Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                    Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                ],
            )
            .unwrap();

        storage
            .insert_tuples(
                "nodes",
                vec![
                    Tuple::new(vec![Value::string("a"), Value::Float64(1.0)]),
                    Tuple::new(vec![Value::string("b"), Value::Float64(2.0)]),
                    Tuple::new(vec![Value::string("c"), Value::Float64(3.0)]),
                ],
            )
            .unwrap();

        // Verify parity for each relation
        let kg = storage.knowledge_graphs.get("default").unwrap();
        let kg = kg.read();
        let dd = kg.dd_computation().unwrap();

        for rel_name in &["edges", "nodes"] {
            let hashmap_tuples = kg.engine.input_tuples.get(*rel_name).unwrap();
            let mut dd_tuples = dd.read_relation_consistent(rel_name).unwrap();
            dd_tuples.sort();

            let mut hashmap_sorted: Vec<_> = hashmap_tuples.clone();
            hashmap_sorted.sort();

            assert_eq!(
                hashmap_sorted, dd_tuples,
                "DD arrangement for '{rel_name}' should match HashMap"
            );
        }
    }

    #[test]
    fn test_dd_arrangement_max_write_time_tracks_logical_time() {
        use crate::value::Value;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        // Record logical time before insert
        let time_before = storage.logical_time.load(Ordering::SeqCst);

        // Insert triggers logical_time.fetch_add(1)
        storage
            .insert_tuples("data", vec![Tuple::new(vec![Value::Int32(42)])])
            .unwrap();

        // DD's max_write_time should be >= time_before (the time used for this insert)
        let kg = storage.knowledge_graphs.get("default").unwrap();
        let kg = kg.read();
        let dd = kg.dd_computation().unwrap();

        assert!(
            dd.max_write_time() >= time_before,
            "DD max_write_time ({}) should be >= logical time at insert ({})",
            dd.max_write_time(),
            time_before
        );
    }

    // WAL Replay into DDComputation Tests
    #[test]
    fn test_dd_replay_existing_data_on_enable() {
        use crate::value::Value;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Insert data BEFORE enabling DDComputation
        storage
            .insert_tuples(
                "edge",
                vec![
                    Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                    Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                    Tuple::new(vec![Value::Int32(3), Value::Int32(4)]),
                ],
            )
            .unwrap();

        storage
            .insert_tuples(
                "node",
                vec![
                    Tuple::new(vec![Value::string("a")]),
                    Tuple::new(vec![Value::string("b")]),
                ],
            )
            .unwrap();

        // NOW enable DDComputation  -  should replay existing data
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        // Verify DDComputation has all pre-existing data
        let kg = storage.knowledge_graphs.get("default").unwrap();
        let kg = kg.read();
        let dd = kg.dd_computation().unwrap();

        let edges = dd.read_relation_consistent("edge").unwrap();
        assert_eq!(edges.len(), 3, "DD should have 3 edges from replay");

        let nodes = dd.read_relation_consistent("node").unwrap();
        assert_eq!(nodes.len(), 2, "DD should have 2 nodes from replay");
    }

    #[test]
    fn test_dd_replay_parity_with_hashmap() {
        use crate::value::Value;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Insert data before enabling DD
        storage
            .insert_tuples(
                "data",
                vec![
                    Tuple::new(vec![Value::Int32(10), Value::string("x")]),
                    Tuple::new(vec![Value::Int32(20), Value::string("y")]),
                    Tuple::new(vec![Value::Int32(30), Value::string("z")]),
                ],
            )
            .unwrap();

        // Enable DD (triggers replay)
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        // Verify exact parity between DD and HashMap
        let kg = storage.knowledge_graphs.get("default").unwrap();
        let kg = kg.read();
        let hashmap_tuples = kg.engine.input_tuples.get("data").unwrap();
        let dd = kg.dd_computation().unwrap();
        let mut dd_tuples = dd.read_relation_consistent("data").unwrap();
        dd_tuples.sort();

        let mut hashmap_sorted: Vec<_> = hashmap_tuples.clone();
        hashmap_sorted.sort();

        assert_eq!(
            hashmap_sorted, dd_tuples,
            "DD arrangement after replay should match HashMap exactly"
        );
    }

    #[test]
    fn test_dd_replay_then_new_writes() {
        use crate::value::Value;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Insert data before enabling DD
        storage
            .insert_tuples(
                "items",
                vec![
                    Tuple::new(vec![Value::Int32(1)]),
                    Tuple::new(vec![Value::Int32(2)]),
                ],
            )
            .unwrap();

        // Enable DD (triggers replay)
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        // Now insert MORE data (after DD is enabled)
        storage
            .insert_tuples(
                "items",
                vec![
                    Tuple::new(vec![Value::Int32(3)]),
                    Tuple::new(vec![Value::Int32(4)]),
                ],
            )
            .unwrap();

        // Verify DD has ALL data (replayed + new)
        let kg = storage.knowledge_graphs.get("default").unwrap();
        let kg = kg.read();
        let dd = kg.dd_computation().unwrap();
        let mut dd_tuples = dd.read_relation_consistent("items").unwrap();
        dd_tuples.sort();

        assert_eq!(
            dd_tuples.len(),
            4,
            "DD should have 4 items (2 replayed + 2 new)"
        );

        // Verify parity with HashMap
        let mut hashmap_sorted: Vec<_> = kg.engine.input_tuples.get("items").unwrap().clone();
        hashmap_sorted.sort();

        assert_eq!(
            hashmap_sorted, dd_tuples,
            "DD should match HashMap after replay + new writes"
        );
    }

    #[test]
    fn test_dd_replay_legacy_tuple2_data() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Insert data before enabling DD
        storage
            .insert("edge", vec![(1, 2), (2, 3), (3, 4)])
            .unwrap();

        // Enable DD (should replay legacy data)
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        // Verify DD has the replayed data
        let kg = storage.knowledge_graphs.get("default").unwrap();
        let kg = kg.read();
        let dd = kg.dd_computation().unwrap();
        let dd_tuples = dd.read_relation_consistent("edge").unwrap();
        assert_eq!(
            dd_tuples.len(),
            3,
            "DD should have 3 edges from legacy replay"
        );
    }

    #[test]
    fn test_dd_replay_from_persistence() {
        let temp = TempDir::new().unwrap();

        // Create and populate, then save
        {
            let config = create_test_config(temp.path().to_path_buf());
            let mut storage = StorageEngine::new(config).unwrap();
            storage.use_knowledge_graph("default").unwrap();
            storage
                .insert("edge", vec![(10, 20), (20, 30), (30, 40)])
                .unwrap();
            storage.save_all().unwrap();
        }

        // Reload from persistence
        {
            let config = create_test_config(temp.path().to_path_buf());
            let mut storage = StorageEngine::new(config).unwrap();
            storage.use_knowledge_graph("default").unwrap();

            // Enable DDComputation  -  should replay persisted data
            {
                let kg = storage.knowledge_graphs.get("default").unwrap();
                let mut kg = kg.write();
                kg.enable_dd_computation().unwrap();
            }

            // Verify DD has the persisted data
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let kg = kg.read();
            let dd = kg.dd_computation().unwrap();
            let dd_tuples = dd.read_relation_consistent("edge").unwrap();
            assert_eq!(
                dd_tuples.len(),
                3,
                "DD should have 3 edges from persistence replay"
            );
        }
    }

    // === Materialization Pipeline Integration Tests ===

    use crate::statement::{RuleDef, SerializableBodyPred, SerializableRule, SerializableTerm};
    use crate::value::Value;

    fn make_path_rule_def() -> RuleDef {
        RuleDef {
            name: "path".to_string(),
            rule: SerializableRule {
                head_relation: "path".to_string(),
                head_args: vec![
                    SerializableTerm::Variable("X".to_string()),
                    SerializableTerm::Variable("Y".to_string()),
                ],
                body: vec![SerializableBodyPred::Atom {
                    relation: "edge".to_string(),
                    args: vec![
                        SerializableTerm::Variable("X".to_string()),
                        SerializableTerm::Variable("Y".to_string()),
                    ],
                    negated: false,
                }],
            },
        }
    }

    fn make_simple_rule_def(name: &str, base_relation: &str) -> RuleDef {
        RuleDef {
            name: name.to_string(),
            rule: SerializableRule {
                head_relation: name.to_string(),
                head_args: vec![SerializableTerm::Variable("X".to_string())],
                body: vec![SerializableBodyPred::Atom {
                    relation: base_relation.to_string(),
                    args: vec![SerializableTerm::Variable("X".to_string())],
                    negated: false,
                }],
            },
        }
    }

    #[test]
    fn test_materialization_snapshot_includes_valid_materializations() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Enable DDComputation
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        // Insert base data
        storage
            .insert_tuples(
                "edge",
                vec![
                    Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                    Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                ],
            )
            .unwrap();

        // Register a rule
        let rule_def = make_path_rule_def();
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.register_rule(&rule_def).unwrap();
        }

        // Materialize the derived relation (uses the new method that also publishes snapshot)
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let kg = kg.read();

            // Simulate materializing path with some tuples
            let path_tuples = vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                Tuple::new(vec![Value::Int32(99), Value::Int32(100)]), // Extra tuple
            ];
            kg.materialize_derived_relation("path", path_tuples)
                .unwrap();
        }

        // Get the updated snapshot
        let kg = storage.knowledge_graphs.get("default").unwrap();
        let kg = kg.read();
        let snapshot = kg.snapshot();

        // Verify snapshot has the materialized relation
        assert!(
            snapshot.is_materialized("path"),
            "path should be marked as materialized"
        );
        assert_eq!(snapshot.materialized_count(), 1);

        // Verify the materialized tuples are in input_tuples
        let path_tuples = snapshot.input_tuples.get("path");
        assert!(path_tuples.is_some(), "path tuples should be in snapshot");
        assert_eq!(
            path_tuples.unwrap().len(),
            3,
            "should have 3 materialized tuples"
        );
    }

    #[test]
    fn test_materialization_invalidation_removes_from_snapshot() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Enable DDComputation
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        // Insert base data and register rule
        storage
            .insert_tuples(
                "edge",
                vec![Tuple::new(vec![Value::Int32(1), Value::Int32(2)])],
            )
            .unwrap();

        let rule_def = make_path_rule_def();
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.register_rule(&rule_def).unwrap();
        }

        // Materialize (uses the new method that also publishes snapshot)
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let kg = kg.read();
            kg.materialize_derived_relation(
                "path",
                vec![Tuple::new(vec![Value::Int32(1), Value::Int32(2)])],
            )
            .unwrap();
        }

        // Verify materialized
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let kg = kg.read();
            let snapshot = kg.snapshot();
            assert!(snapshot.is_materialized("path"));
        }

        // Insert more data - this should invalidate the materialization
        // (insert triggers notify_base_update which invalidates derived relations)
        storage
            .insert_tuples(
                "edge",
                vec![Tuple::new(vec![Value::Int32(2), Value::Int32(3)])],
            )
            .unwrap();

        // Verify no longer materialized (invalidated)
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let kg = kg.read();
            let snapshot = kg.snapshot();
            assert!(
                !snapshot.is_materialized("path"),
                "path should be invalidated after base data change"
            );
        }
    }

    #[test]
    fn test_materialization_query_uses_cached_data() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Enable DDComputation
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        // Insert base data
        storage
            .insert_tuples(
                "edge",
                vec![
                    Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                    Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                ],
            )
            .unwrap();

        // Register rule
        let rule_def = make_path_rule_def();
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.register_rule(&rule_def).unwrap();
        }

        // Materialize with DIFFERENT data than what the rule would produce
        // This proves the query uses cached data, not the rule
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let kg = kg.read();
            kg.materialize_derived_relation(
                "path",
                vec![
                    Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                    Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                    Tuple::new(vec![Value::Int32(99), Value::Int32(100)]), // Extra!
                ],
            )
            .unwrap();
        }

        // Query path - should get 3 results (from materialized), not 2 (from rule)
        let results = storage
            .execute_query_with_rules_tuples("result(X, Y) <- path(X, Y)")
            .unwrap();
        assert_eq!(
            results.len(),
            3,
            "Should use materialized data (3 tuples), not rule evaluation (2 tuples)"
        );
    }

    #[test]
    fn test_materialization_stats() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Enable DDComputation
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_dd_computation().unwrap();
        }

        // Register two rules
        let rule1 = make_simple_rule_def("derived1", "base");
        let rule2 = make_simple_rule_def("derived2", "base");

        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.register_rule(&rule1).unwrap();
            kg.register_rule(&rule2).unwrap();
        }

        // Check stats - 2 rules, 0 materialized, 2 invalid
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let kg = kg.read();
            let dd = kg.dd_computation().unwrap();
            let (total, materialized, invalid) = dd.get_derived_stats().unwrap();
            assert_eq!(total, 2, "should have 2 rules");
            assert_eq!(materialized, 0, "nothing materialized yet");
            assert_eq!(invalid, 2, "both should be invalid (not materialized)");
        }

        // Materialize one
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let kg = kg.read();
            let dd = kg.dd_computation().unwrap();
            dd.set_materialized("derived1", vec![]).unwrap();
        }

        // Check stats - 2 rules, 1 materialized, 1 invalid
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let kg = kg.read();
            let dd = kg.dd_computation().unwrap();
            let (total, materialized, invalid) = dd.get_derived_stats().unwrap();
            assert_eq!(total, 2);
            assert_eq!(materialized, 1);
            assert_eq!(invalid, 1);
        }
    }
}
