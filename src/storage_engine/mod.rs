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

use arc_swap::ArcSwap;
use crate::config::Config;
use crate::rule_catalog::RuleCatalog;
use crate::statement::RuleDef;
use crate::storage::persist::{
    consolidate_to_current, to_tuple2s, FilePersist, PersistBackend, PersistConfig, Update,
};
use crate::storage::{
    KnowledgeGraphMetadata, KnowledgeGraphsMetadata, StorageError, StorageResult,
};
use crate::value::Tuple;
use crate::value::Tuple2;
use crate::DatalogEngine;
use chrono::Utc;
use dashmap::DashMap;
use rayon::prelude::*;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;

/// Storage Engine - manages multiple knowledge graphs
///
/// Uses DashMap for concurrent access to knowledge graphs without global locks.
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
    // TODO: Implement knowledge graph backup, migration, and multi-knowledge-graph management.
    // Reserved for future storage operations:
    // - Knowledge graph export/import to different locations
    // - Knowledge graph migration between storage formats
    // - Diagnostic tools that report knowledge graph location
    // Currently the path is only used during initialization for RuleCatalog.
    #[allow(dead_code)]
    data_dir: PathBuf,
    /// Rule catalog for persistent derived relations
    rule_catalog: RuleCatalog,
    /// Current snapshot for lock-free reads (updated atomically on writes)
    snapshot: ArcSwap<KnowledgeGraphSnapshot>,
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
    pub fn create_knowledge_graph(&mut self, name: &str) -> StorageResult<()> {
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
        let kg = KnowledgeGraph::new(name.to_string(), db_dir);

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

        // Note: Could add last_accessed field to DatabaseMetadata for tracking
        // For now, we reuse created_at field (simplified implementation)
        if let Some(db) = self.knowledge_graphs.get(name) {
            let mut db = db.write();
            db.metadata.created_at = Utc::now().to_rfc3339();
        }

        self.current_kg = Some(name.to_string());
        Ok(())
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

    /// Insert tuples into a relation in the current knowledge graph
    /// Returns (new_count, duplicate_count) for reporting to user
    pub fn insert(&self, relation: &str, tuples: Vec<Tuple2>) -> StorageResult<(usize, usize)> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .to_string();

        self.insert_into(&db_name, relation, tuples)
    }

    /// Insert tuples into a specific knowledge graph (explicit API)
    /// Returns (new_count, duplicate_count) for reporting to user
    ///
    /// Uses `&self` instead of `&mut self` to enable concurrent writes to different KGs.
    /// Thread safety is ensured via internal per-KG locking.
    pub fn insert_into(
        &self,
        kg: &str,
        relation: &str,
        tuples: Vec<Tuple2>,
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
                    "Cannot insert into '{}': it is a derived relation (view). \
                     Use a base relation or drop the rule first with '.rule drop {}'.",
                    relation, relation
                )));
            }
        }

        // Tuple2 is always arity 2 - check if relation exists with different arity
        if let Some((existing_schema, _)) = self.get_relation_metadata_in(kg, relation)? {
            let existing_arity = existing_schema.len();
            if existing_arity != 2 {
                return Err(StorageError::Other(format!(
                    "Arity mismatch for relation '{}': existing arity is {}, but trying to insert tuples with arity 2",
                    relation, existing_arity
                )));
            }
        }

        // Generate shard name and logical time
        let shard = format!("{}:{}", kg, relation);
        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);

        // Create DD-style updates (+1 diff for insert)
        let updates: Vec<Update> = tuples
            .iter()
            .map(|&data| Update::insert_tuple2(data, time))
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
        let (new_count, dup_count) = db.insert_in_memory(relation, tuples);

        Ok((new_count, dup_count))
    }

    /// Insert arbitrary-arity tuples into a relation in the current knowledge graph
    /// This is the production API that supports vectors and mixed types.
    /// Returns (new_count, duplicate_count) for reporting to user
    pub fn insert_tuples(
        &self,
        relation: &str,
        tuples: Vec<Tuple>,
    ) -> StorageResult<(usize, usize)> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .to_string();

        self.insert_tuples_into(&db_name, relation, tuples)
    }

    /// Insert arbitrary-arity tuples into a specific knowledge graph (explicit API)
    /// Returns (new_count, duplicate_count) for reporting to user
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
                    "Cannot insert into '{}': it is a derived relation (view). \
                     Use a base relation or drop the rule first with '.rule drop {}'.",
                    relation, relation
                )));
            }
        }

        // Check arity consistency
        let new_arity = tuples.first().map(|t| t.arity()).unwrap_or(0);

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
                    "Arity mismatch for relation '{}': existing arity is {}, but trying to insert tuples with arity {}",
                    relation, existing_arity, new_arity
                )));
            }
        }

        // Generate shard name and logical time
        let shard = format!("{}:{}", kg, relation);
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
        let (new_count, dup_count) = db.insert_tuples_in_memory(relation, tuples);

        Ok((new_count, dup_count))
    }

    /// Delete tuples from a relation in the current knowledge graph
    /// Returns the count of actually deleted tuples
    pub fn delete(&self, relation: &str, tuples: Vec<Tuple2>) -> StorageResult<usize> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .to_string();

        self.delete_from(&db_name, relation, tuples)
    }

    /// Delete tuples from a specific knowledge graph (explicit API)
    /// Returns the count of actually deleted tuples
    ///
    /// Uses `&self` instead of `&mut self` to enable concurrent writes to different KGs.
    pub fn delete_from(
        &self,
        kg: &str,
        relation: &str,
        tuples: Vec<Tuple2>,
    ) -> StorageResult<usize> {
        if tuples.is_empty() {
            return Ok(0);
        }

        // Generate shard name and logical time
        let shard = format!("{}:{}", kg, relation);
        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);

        // Create DD-style updates (-1 diff for delete)
        let updates: Vec<Update> = tuples
            .iter()
            .map(|&data| Update::delete_tuple2(data, time))
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
        let deleted_count = db.delete_in_memory(relation, &tuples);

        Ok(deleted_count)
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
            .to_string();

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
        let shard = format!("{}:{}", kg, relation);
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
        let deleted_count = db.delete_tuples_in_memory(relation, &tuples);

        Ok(deleted_count)
    }

    /// Execute a Datalog query on the current knowledge graph
    pub fn execute_query(&mut self, program: &str) -> StorageResult<Vec<Tuple2>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .to_string();

        self.execute_query_on(&db_name, program)
    }

    /// Execute a Datalog query on a specific knowledge graph (explicit API)
    ///
    /// Uses a completely lock-free read path via snapshots.
    /// The snapshot is obtained atomically (O(1)) and execution proceeds
    /// without holding any locks. Concurrent reads never block.
    pub fn execute_query_on(&mut self, kg: &str, program: &str) -> StorageResult<Vec<Tuple2>> {
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
            .map_err(|e| StorageError::Other(format!("Query execution failed: {}", e)))
    }

    /// Save a specific knowledge graph to disk (flush persist buffers)
    pub fn save_knowledge_graph(&self, name: &str) -> StorageResult<()> {
        // Check knowledge graph exists
        if !self.knowledge_graphs.contains_key(name) {
            return Err(StorageError::KnowledgeGraphNotFound(name.to_string()));
        }

        // Flush all shards for this knowledge graph
        let prefix = format!("{}:", name);
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

    // ========================================================================
    // Rule Management (Persistent Derived Relations)
    // ========================================================================

    /// Register a persistent rule in the current knowledge graph
    pub fn register_rule(
        &self,
        rule_def: &RuleDef,
    ) -> StorageResult<crate::rule_catalog::RuleRegisterResult> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .to_string();

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
            .map_err(|e| StorageError::Other(format!("Failed to register rule: {}", e)))
    }

    /// Drop a rule from the current knowledge graph
    pub fn drop_rule(&self, name: &str) -> StorageResult<()> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .to_string();

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
            .map_err(|e| StorageError::Other(format!("Failed to drop rule: {}", e)))
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
            .to_string();

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
            .map_err(|e| StorageError::Other(format!("Failed to clear rule: {}", e)))
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
            .to_string();

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
            .map_err(|e| StorageError::Other(format!("Failed to replace rule clause: {}", e)))
    }

    /// Remove a specific clause from a rule (current knowledge graph)
    /// Returns true if the entire rule was deleted (last clause removed)
    pub fn remove_rule_clause(&mut self, name: &str, index: usize) -> StorageResult<bool> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .to_string();

        self.remove_rule_clause_in(&db_name, name, index)
    }

    /// Remove a specific clause from a rule (specific knowledge graph)
    /// Returns true if the entire rule was deleted (last clause removed)
    pub fn remove_rule_clause_in(
        &mut self,
        kg: &str,
        name: &str,
        index: usize,
    ) -> StorageResult<bool> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        db.remove_rule_clause(name, index)
            .map_err(|e| StorageError::Other(format!("Failed to remove rule clause: {}", e)))
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
            .to_string();

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

    /// Execute a query with rules prepended (current knowledge graph)
    pub fn execute_query_with_rules(&mut self, program: &str) -> StorageResult<Vec<Tuple2>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .to_string();

        self.execute_query_with_rules_on(&db_name, program)
    }

    /// Execute a query with rules prepended (specific knowledge graph)
    ///
    /// Uses a completely lock-free read path via snapshots.
    pub fn execute_query_with_rules_on(
        &mut self,
        kg: &str,
        program: &str,
    ) -> StorageResult<Vec<Tuple2>> {
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
            .map_err(|e| StorageError::Other(format!("Query execution failed: {}", e)))
    }

    /// Execute a query with rules prepended, returning tuples of arbitrary arity (current knowledge graph)
    pub fn execute_query_with_rules_tuples(&mut self, program: &str) -> StorageResult<Vec<Tuple>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .to_string();

        self.execute_query_with_rules_tuples_on(&db_name, program)
    }

    /// Execute a query with rules prepended, returning tuples of arbitrary arity (specific knowledge graph)
    ///
    /// Uses a completely lock-free read path via snapshots.
    pub fn execute_query_with_rules_tuples_on(
        &mut self,
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
            .map_err(|e| StorageError::Other(format!("Query execution failed: {}", e)))
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
    /// 4. Populate in-memory DatalogEngine
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
        let prefix = format!("{}:", name);
        let mut engine = DatalogEngine::new();
        let mut metadata = KnowledgeGraphMetadata::new(name.to_string());

        // Find all shards for this knowledge graph
        for shard_name in self.persist.list_shards()? {
            if shard_name.starts_with(&prefix) {
                let relation = shard_name.strip_prefix(&prefix).unwrap();

                // Get shard info to determine since frontier
                let info = self.persist.shard_info(&shard_name)?;

                // Read and consolidate updates
                let mut updates = self.persist.read(&shard_name, info.since)?;
                consolidate_to_current(&mut updates);

                // Extract current tuples (positive multiplicities only)
                // Convert to Tuple2 for DatalogEngine compatibility
                let tuples = to_tuple2s(&updates);

                if !tuples.is_empty() {
                    // Infer schema from tuples (Tuple2 is always arity 2)
                    let schema = vec!["col0".to_string(), "col1".to_string()];
                    let tuple_count = tuples.len();

                    // Update metadata with relation info
                    metadata.add_relation(relation.to_string(), schema, tuple_count);

                    engine.add_fact(relation, tuples);
                }
            }
        }

        // Load view catalog (will load existing views if present)
        let rule_catalog = RuleCatalog::new(data_dir.clone())
            .map_err(|e| StorageError::Other(format!("Failed to load view catalog: {}", e)))?;

        // Create initial snapshot from loaded data
        let snapshot = ArcSwap::from_pointee(KnowledgeGraphSnapshot::new(
            engine.input_data.clone(),
            engine.input_tuples.clone(),
            rule_catalog.all_rules(),
        ));

        Ok(KnowledgeGraph {
            name: name.to_string(),
            engine,
            metadata,
            data_dir,
            rule_catalog,
            snapshot,
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

    // ========================================================================
    // Parallel Query Execution API
    // ========================================================================

    /// Execute multiple queries in parallel across different knowledge graphs
    ///
    /// This method leverages Rayon's thread pool to execute queries concurrently,
    /// utilizing all available CPU cores efficiently.
    ///
    /// # Example
    /// ```text
    /// let queries = vec![
    ///     ("kg1", "result(X,Y) :- edge(X,Y)."),
    ///     ("kg2", "result(X,Y) :- person(X,Y)."),
    ///     ("kg3", "result(X,Y) :- data(X,Y)."),
    /// ];
    ///
    /// let results = storage.execute_parallel_queries_on_knowledge_graphs(queries)?;
    /// ```
    pub fn execute_parallel_queries_on_knowledge_graphs(
        &self,
        queries: Vec<(&str, &str)>,
    ) -> StorageResult<Vec<(String, Vec<Tuple2>)>> {
        // Use Rayon to execute queries in parallel with lock-free snapshot reads
        let results: Result<Vec<_>, StorageError> = queries
            .par_iter()
            .map(|(kg, program)| {
                // Get knowledge graph
                let kg_lock = self
                    .knowledge_graphs
                    .get(*kg)
                    .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

                // Get snapshot atomically - O(1)
                let snapshot = {
                    let kg_guard = kg_lock.read();
                    kg_guard.snapshot()
                };

                // Execute on snapshot - completely lock-free
                let results = snapshot
                    .execute(program)
                    .map_err(|e| StorageError::Other(format!("Query execution failed: {}", e)))?;

                Ok((kg.to_string(), results))
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
    /// let query = "result(X,Y) :- edge(X,Y), X > 5.";
    ///
    /// let results = storage.execute_query_on_multiple_knowledge_graphs(knowledge_graphs, query)?;
    /// ```
    pub fn execute_query_on_multiple_knowledge_graphs(
        &self,
        knowledge_graphs: Vec<&str>,
        program: &str,
    ) -> StorageResult<Vec<(String, Vec<Tuple2>)>> {
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
    ///     "q1(X,Y) :- edge(X,Y).",
    ///     "q2(X,Z) :- path(X,Y), path(Y,Z).",
    ///     "q3(X) :- person(X,_), edge(X,_).",
    /// ];
    ///
    /// let results = storage.execute_parallel_queries_on_knowledge_graph("kg1", queries)?;
    /// ```
    pub fn execute_parallel_queries_on_knowledge_graph(
        &self,
        kg: &str,
        programs: Vec<&str>,
    ) -> StorageResult<Vec<Vec<Tuple2>>> {
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
                    .map_err(|e| StorageError::Other(format!("Query execution failed: {}", e)))
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
    pub fn set_num_threads(num_threads: usize) {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
            .expect("Failed to configure thread pool");
    }
}

impl KnowledgeGraph {
    /// Create a new empty knowledge graph
    fn new(name: String, data_dir: PathBuf) -> Self {
        // Create view catalog (will load existing views if present)
        let rule_catalog = RuleCatalog::new(data_dir.clone()).unwrap_or_else(|_| {
            // If loading fails, create empty catalog
            RuleCatalog::new(data_dir.clone()).unwrap()
        });

        // Create initial empty snapshot
        let snapshot = ArcSwap::from_pointee(KnowledgeGraphSnapshot::empty());

        KnowledgeGraph {
            name: name.clone(),
            engine: DatalogEngine::new(),
            metadata: KnowledgeGraphMetadata::new(name),
            data_dir,
            rule_catalog,
            snapshot,
        }
    }

    /// Publish a new snapshot atomically
    ///
    /// Called after data modifications to make changes visible to readers.
    /// This is O(1) - just an atomic pointer swap.
    fn publish_snapshot(&self) {
        let new_snapshot = KnowledgeGraphSnapshot::new(
            self.engine.input_data.clone(),
            self.engine.input_tuples.clone(),
            self.rule_catalog.all_rules(),
        );
        self.snapshot.store(Arc::new(new_snapshot));
    }

    /// Get the current snapshot for lock-free reads
    ///
    /// Returns an Arc to the current snapshot. This is O(1) and lock-free.
    pub fn snapshot(&self) -> Arc<KnowledgeGraphSnapshot> {
        self.snapshot.load_full()
    }

    /// Insert tuples into in-memory state only
    ///
    /// Persistence is handled by StorageEngine via the persist layer.
    /// Returns (new_count, duplicate_count) for caller to report.
    fn insert_in_memory(&mut self, relation: &str, tuples: Vec<Tuple2>) -> (usize, usize) {
        // Get schema (immutable borrow)
        let schema = self
            .engine
            .catalog()
            .get_schema(relation)
            .map(|s| s.to_vec())
            .unwrap_or_else(|| vec!["col0".to_string(), "col1".to_string()]);

        // Update in-memory state, tracking new vs duplicate
        let mut new_count = 0;
        let mut dup_count = 0;
        let existing = self
            .engine
            .input_data
            .entry(relation.to_string())
            .or_insert_with(Vec::new);
        for tuple in tuples {
            if !existing.contains(&tuple) {
                existing.push(tuple);
                new_count += 1;
            } else {
                dup_count += 1;
            }
        }
        let tuple_count = existing.len();

        // Update metadata
        self.metadata
            .add_relation(relation.to_string(), schema, tuple_count);

        // Publish new snapshot for lock-free reads
        self.publish_snapshot();

        (new_count, dup_count)
    }

    /// Insert arbitrary-arity tuples into in-memory state only
    ///
    /// Persistence is handled by StorageEngine via the persist layer.
    /// Returns (new_count, duplicate_count) for caller to report.
    fn insert_tuples_in_memory(&mut self, relation: &str, tuples: Vec<Tuple>) -> (usize, usize) {
        // Infer schema from first tuple if available
        let schema = if let Some(first) = tuples.first() {
            (0..first.arity())
                .map(|i| format!("col{}", i))
                .collect::<Vec<_>>()
        } else {
            vec!["col0".to_string(), "col1".to_string()]
        };

        // Update in-memory production format (input_tuples)
        let mut new_count = 0;
        let mut dup_count = 0;

        // Get or create the relation's tuple storage
        let existing_tuples = self
            .engine
            .input_tuples
            .entry(relation.to_string())
            .or_insert_with(Vec::new);

        for tuple in tuples {
            if !existing_tuples.contains(&tuple) {
                existing_tuples.push(tuple);
                new_count += 1;
            } else {
                dup_count += 1;
            }
        }
        let tuple_count = existing_tuples.len();

        // Update metadata
        self.metadata
            .add_relation(relation.to_string(), schema, tuple_count);

        // Publish new snapshot for lock-free reads
        self.publish_snapshot();

        (new_count, dup_count)
    }

    /// Delete tuples from in-memory state only
    ///
    /// Persistence is handled by StorageEngine via the persist layer.
    /// Delete tuples from in-memory state and return the count of actually deleted tuples
    fn delete_in_memory(&mut self, relation: &str, tuples_to_remove: &[Tuple2]) -> usize {
        // Get schema from metadata (which has the correct arity from insert time)
        // Avoid using catalog which may not have the schema for base facts
        let schema = self
            .metadata
            .relations
            .get(relation)
            .map(|r| r.schema.clone())
            .unwrap_or_else(|| vec!["col0".to_string(), "col1".to_string()]);

        let mut found = false;
        let mut final_count = 0;
        let mut deleted_count = 0;

        // Update in-memory state (legacy format - input_data)
        if let Some(existing) = self.engine.input_data.get_mut(relation) {
            let count_before = existing.len();
            // Remove tuples
            existing.retain(|tuple| !tuples_to_remove.contains(tuple));
            final_count = existing.len();
            deleted_count = count_before - final_count;
            found = true;
        }

        // Also update production format (input_tuples)
        if let Some(existing) = self.engine.input_tuples.get_mut(relation) {
            // Convert Tuple2 to Tuple for comparison
            let tuples_as_tuple: Vec<crate::value::Tuple> = tuples_to_remove
                .iter()
                .map(|&(a, b)| crate::value::Tuple::from_pair(a, b))
                .collect();

            let count_before = existing.len();
            // Remove tuples
            existing.retain(|tuple| !tuples_as_tuple.contains(tuple));
            final_count = existing.len();
            // Use the larger of the two deleted counts (in case only one format has data)
            deleted_count = deleted_count.max(count_before - final_count);
            found = true;
        }

        // Update metadata if we found and modified data
        if found {
            self.metadata
                .add_relation(relation.to_string(), schema, final_count);
            // Publish new snapshot for lock-free reads
            self.publish_snapshot();
        }

        deleted_count
    }

    /// Delete tuples (Tuple type) from in-memory state only
    ///
    /// This is the production API for deleting arbitrary-arity tuples.
    /// Persistence is handled by StorageEngine via the persist layer.
    /// Returns the count of actually deleted tuples.
    fn delete_tuples_in_memory(&mut self, relation: &str, tuples_to_remove: &[Tuple]) -> usize {
        // Get schema from metadata (which has the correct arity from insert time)
        // Avoid using catalog which may not have the schema for base facts
        let schema = self
            .metadata
            .relations
            .get(relation)
            .map(|r| r.schema.clone())
            .unwrap_or_else(|| vec!["col0".to_string(), "col1".to_string()]);

        let mut found = false;
        let mut final_count = 0;
        let mut deleted_count = 0;

        // Update production format (input_tuples)
        if let Some(existing) = self.engine.input_tuples.get_mut(relation) {
            let count_before = existing.len();
            // Remove tuples
            existing.retain(|tuple| !tuples_to_remove.contains(tuple));
            final_count = existing.len();
            deleted_count = count_before - final_count;
            found = true;
        }

        // Also update legacy format (input_data) if tuples are convertible
        if let Some(existing) = self.engine.input_data.get_mut(relation) {
            // Convert Tuple to Tuple2 for comparison where possible
            let tuples_as_tuple2: Vec<Tuple2> = tuples_to_remove
                .iter()
                .filter_map(|t| t.to_pair())
                .collect();

            if !tuples_as_tuple2.is_empty() {
                let count_before = existing.len();
                // Remove tuples
                existing.retain(|tuple| !tuples_as_tuple2.contains(tuple));
                final_count = existing.len();
                // Use the larger of the two deleted counts (in case only one format has data)
                deleted_count = deleted_count.max(count_before - final_count);
                found = true;
            }
        }

        // Update metadata if we found and modified data
        if found {
            self.metadata
                .add_relation(relation.to_string(), schema, final_count);
            // Publish new snapshot for lock-free reads
            self.publish_snapshot();
        }

        deleted_count
    }

    /// Get knowledge graph name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get knowledge graph metadata
    pub fn metadata(&self) -> &KnowledgeGraphMetadata {
        &self.metadata
    }

    // ========================================================================
    // View Management
    // ========================================================================

    /// Register a persistent view
    /// Returns whether view was created or rule was added
    pub fn register_rule(
        &mut self,
        rule_def: &RuleDef,
    ) -> Result<crate::rule_catalog::RuleRegisterResult, String> {
        let result = self.rule_catalog.register_rule(rule_def)?;
        self.publish_snapshot();
        Ok(result)
    }

    /// Drop a view
    pub fn drop_rule(&mut self, name: &str) -> Result<(), String> {
        self.rule_catalog.drop(name)?;
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
    pub fn execute_with_rules(&mut self, program: &str) -> Result<Vec<Tuple2>, String> {
        // Get all view rules
        let rule_defs = self.rule_catalog.all_rules();

        if rule_defs.is_empty() {
            // No views, just execute normally
            return self.engine.execute(program);
        }

        // Build the combined program: view rules + query
        let mut combined = String::new();

        // Add view rules
        for rule in &rule_defs {
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
    pub fn execute_with_rules_tuples(&mut self, program: &str) -> Result<Vec<Tuple>, String> {
        // Get all view rules
        let rule_defs = self.rule_catalog.all_rules();

        if rule_defs.is_empty() {
            // No views, just execute normally
            return self.engine.execute_tuples(program);
        }

        // Build the combined program: view rules + query
        let mut combined = String::new();

        // Add view rules
        for rule in &rule_defs {
            combined.push_str(&format_rule(rule));
            combined.push('\n');
        }

        // Add the query
        combined.push_str(program);

        if std::env::var("DATALOG_DEBUG").is_ok() {
            eprintln!(
                "DEBUG execute_with_rules_tuples: {} view rules, program = {}",
                rule_defs.len(),
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
}

/// Format a Rule as a Datalog string
fn format_rule(rule: &crate::ast::Rule) -> String {
    let head = format_atom(&rule.head);

    if rule.body.is_empty() {
        return format!("{}.", head);
    }

    let mut body_parts = Vec::new();

    for pred in &rule.body {
        match pred {
            crate::ast::BodyPredicate::Positive(atom) => {
                body_parts.push(format_atom(atom));
            }
            crate::ast::BodyPredicate::Negated(atom) => {
                body_parts.push(format!("!{}", format_atom(atom)));
            }
            crate::ast::BodyPredicate::Comparison(left, op, right) => {
                let op_str = match op {
                    crate::ast::ComparisonOp::Equal => "=",
                    crate::ast::ComparisonOp::NotEqual => "!=",
                    crate::ast::ComparisonOp::LessThan => "<",
                    crate::ast::ComparisonOp::LessOrEqual => "<=",
                    crate::ast::ComparisonOp::GreaterThan => ">",
                    crate::ast::ComparisonOp::GreaterOrEqual => ">=",
                };
                body_parts.push(format!("{} {} {}", format_term(left), op_str, format_term(right)));
            }
        }
    }

    format!("{} :- {}.", head, body_parts.join(", "))
}

/// Format an Atom as a Datalog string
fn format_atom(atom: &crate::ast::Atom) -> String {
    let args: Vec<String> = atom.args.iter().map(format_term).collect();
    format!("{}({})", atom.relation, args.join(", "))
}

/// Format a Term as a Datalog string
fn format_term(term: &crate::ast::Term) -> String {
    match term {
        crate::ast::Term::Variable(name) => name.clone(),
        crate::ast::Term::Constant(val) => val.to_string(),
        crate::ast::Term::StringConstant(s) => format!("\"{}\"", s),
        crate::ast::Term::FloatConstant(f) => f.to_string(),
        crate::ast::Term::Placeholder => "_".to_string(),
        crate::ast::Term::Arithmetic(expr) => format_arith_expr(expr),
        crate::ast::Term::Aggregate(func, var) => format_aggregate(func, var),
        crate::ast::Term::FunctionCall(func, args) => {
            let formatted_args: Vec<String> = args.iter().map(format_term).collect();
            format!("{}({})", func.as_str(), formatted_args.join(", "))
        }
        crate::ast::Term::VectorLiteral(vals) => {
            let formatted: Vec<String> = vals.iter().map(|v| v.to_string()).collect();
            format!("[{}]", formatted.join(", "))
        }
        crate::ast::Term::FieldAccess(base, field) => {
            format!("{}.{}", format_term(base), field)
        }
        crate::ast::Term::RecordPattern(fields) => {
            let formatted: Vec<String> = fields
                .iter()
                .map(|(name, term)| format!("{}: {}", name, format_term(term)))
                .collect();
            format!("{{ {} }}", formatted.join(", "))
        }
    }
}

/// Format an ArithExpr as a Datalog string
fn format_arith_expr(expr: &crate::ast::ArithExpr) -> String {
    match expr {
        crate::ast::ArithExpr::Variable(name) => name.clone(),
        crate::ast::ArithExpr::Constant(val) => val.to_string(),
        crate::ast::ArithExpr::Binary { op, left, right } => {
            format!(
                "{}{}{}",
                format_arith_expr(left),
                op.as_str(),
                format_arith_expr(right)
            )
        }
    }
}

/// Format an AggregateFunc as a Datalog string
fn format_aggregate(func: &crate::ast::AggregateFunc, var: &str) -> String {
    match func {
        crate::ast::AggregateFunc::Count => format!("count<{}>", var),
        crate::ast::AggregateFunc::Sum => format!("sum<{}>", var),
        crate::ast::AggregateFunc::Min => format!("min<{}>", var),
        crate::ast::AggregateFunc::Max => format!("max<{}>", var),
        crate::ast::AggregateFunc::Avg => format!("avg<{}>", var),
        crate::ast::AggregateFunc::TopK {
            k,
            order_var,
            descending,
        } => {
            if *descending {
                format!("top_k<{}, {}, desc>", k, order_var)
            } else {
                format!("top_k<{}, {}>", k, order_var)
            }
        }
        crate::ast::AggregateFunc::TopKThreshold {
            k,
            order_var,
            threshold,
            descending,
        } => {
            if *descending {
                format!("top_k_threshold<{}, {}, {}, desc>", k, order_var, threshold)
            } else {
                format!("top_k_threshold<{}, {}, {}>", k, order_var, threshold)
            }
        }
        crate::ast::AggregateFunc::WithinRadius {
            distance_var,
            max_distance,
        } => {
            format!("within_radius<{}, {}>", distance_var, max_distance)
        }
    }
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

        let mut storage = StorageEngine::new(config).unwrap();

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
        let result = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
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

            let result = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
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

        // Define first rule: connected(X, Y) :- edge(X, Y).
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

        // Define second rule: connected(X, Z) :- edge(X, Y), connected(Y, Z).
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
            let kg = storage.knowledge_graphs.get("default").expect("default KG should exist");
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
            .execute_query_with_rules("result(X,Y) :- connected(X,Y).")
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
            .execute_query_with_rules("result(1, 3) :- connected(1, 3).")
            .unwrap();
        println!("connected(1, 3): {:?}", specific_result);
        assert_eq!(
            specific_result.len(),
            1,
            "Should find exactly one (1, 3) connection"
        );
        assert_eq!(specific_result[0], (1, 3));
    }
}
