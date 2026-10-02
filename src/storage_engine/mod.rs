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

#[cfg(test)]
mod materialize_tests;
mod relation_store;
mod snapshot;
mod vector_index;
pub use relation_store::RelationStore;
pub use snapshot::KnowledgeGraphSnapshot;

use crate::config::Config;
use crate::derived_relations::CompiledRule;
use crate::incremental::IncrementalEngine;
use crate::index_manager::IndexManager;
use crate::naming;
use crate::rule_catalog::RuleCatalog;
use crate::schema::{RelationSchema, SchemaCatalog, ValidationEngine};
use crate::statement::{RuleDef, SerializableBodyPred};
use crate::storage::persist::{
    consolidate_to_current, set_semantics_corrections, to_tuples, FilePersist, PersistBackend,
    PersistConfig, Update,
};
use crate::storage::{
    DropTombstones, KnowledgeGraphMetadata, KnowledgeGraphsMetadata, RelationTombstone,
    StorageError, StorageResult,
};
use crate::value::relation::to_vec_map;
use crate::value::{Relation, Tuple};
use arc_swap::ArcSwap;
use chrono::Utc;
use dashmap::DashMap;
use parking_lot::RwLock;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

/// Cleanup token returned by Phase 1 of KG drop.
/// Carries the data needed for Phase 2 (slow file I/O cleanup).
pub struct KgDropCleanup {
    /// Name of the knowledge graph being dropped
    pub name: String,
    /// Path to the KG's data directory
    data_dir: PathBuf,
    /// Reference to the persist backend for shard cleanup
    persist: Arc<FilePersist>,
}

fn validate_names(kg: &str, relation: &str) -> StorageResult<()> {
    naming::validate_kg_name(kg)
        .and_then(|()| naming::validate_relation_name(relation))
        .map_err(StorageError::InvalidName)
}

/// Warn about names loaded from disk that the grammar now rejects. Writes to
/// them fail; drop them with `.kg drop <kg>` or `.kg use <kg>` + `.rel drop <rel>`.
fn report_invalid_names(
    kg_names: &HashSet<String>,
    kg_shards: &std::collections::HashMap<String, Vec<(String, String)>>,
) {
    let mut kgs: Vec<&String> = kg_names
        .iter()
        .filter(|kg| naming::validate_kg_name(kg).is_err())
        .collect();
    kgs.sort();
    for kg in kgs {
        tracing::warn!(kg = %kg, "invalid_kg_name: writes are rejected; drop with `.kg drop {kg}`");
    }
    let mut relations: Vec<(&String, &String)> = kg_shards
        .iter()
        .filter(|(kg, _)| naming::validate_kg_name(kg).is_ok())
        .flat_map(|(kg, shards)| shards.iter().map(move |(_, rel)| (kg, rel)))
        .filter(|(_, rel)| naming::validate_relation_name(rel).is_err())
        .collect();
    relations.sort();
    for (kg, rel) in relations {
        tracing::warn!(
            kg = %kg,
            relation = %rel,
            "invalid_relation_name: writes are rejected; drop with `.kg use {kg}` then `.rel drop {rel}`"
        );
    }
}

/// Delete the persist shards and data directory of dropped KG `name`.
/// `kgs` are the other names that may own shards. Returns whether every
/// step succeeded.
fn delete_kg_files(persist: &FilePersist, name: &str, data_dir: &Path, kgs: &[String]) -> bool {
    let mut clean = true;
    match persist.list_shards() {
        Ok(shards) => {
            for shard in &shards {
                let owner = naming::shard_owner(shard, kgs.iter().map(String::as_str));
                if owner.is_some_and(|(kg, _)| kg == name) {
                    if let Err(e) = persist.delete_shard(shard) {
                        warn!(kg = %name, shard = %shard, error = %e, "kg_drop_shard_delete_failed");
                        clean = false;
                    }
                }
            }
        }
        Err(e) => {
            warn!(kg = %name, error = %e, "kg_drop_list_shards_failed");
            clean = false;
        }
    }
    if data_dir.exists() {
        if let Err(e) = fs::remove_dir_all(data_dir) {
            warn!(kg = %name, error = %e, "kg_drop_dir_delete_failed");
            clean = false;
        }
        // Sync parent directory to ensure directory deletion is durable
        if let Some(parent) = data_dir.parent() {
            if let Ok(dir) = fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
    }
    clean
}

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
    /// Committed drops whose cleanup is pending, mirrored on disk. A listed
    /// KG name cannot be recreated until its cleanup finishes.
    tombstones: parking_lot::Mutex<DropTombstones>,
}

/// Single knowledge graph instance
pub struct KnowledgeGraph {
    name: String,
    /// Base relations with dedup indexes; snapshots share its tuples.
    store: RelationStore,
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
    incremental: Option<IncrementalEngine>,
    /// Recompute every rule into the incremental engine on each publish (off by default)
    auto_materialize: bool,
    /// Vector indexes, kept in sync with base relations on every write
    indexes: IndexManager,
    /// Number of workers for parallel query execution
    num_workers: usize,
    /// Maximum result rows per query (0 = unlimited)
    max_result_rows: usize,
    /// Maximum query cost score (0 = unlimited)
    max_query_cost: u64,
    /// Optimizer passes for every engine this KG builds
    optimization: crate::OptimizationConfig,
    /// Set by drop under the write lock; writers holding a stale handle bail
    dropped: bool,
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
            durability_mode: config.storage.persist.durability_mode,
            max_wal_size_bytes: config.storage.persist.max_wal_size_bytes,
        };
        let persist = Arc::new(FilePersist::new(persist_config)?);

        let mut engine = StorageEngine {
            config,
            knowledge_graphs: DashMap::new(),
            current_kg: None,
            persist,
            logical_time: AtomicU64::new(1),
            tombstones: parking_lot::Mutex::new(DropTombstones::default()),
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

    /// Maximum allowed byte length for a knowledge graph name.
    pub const MAX_KG_NAME_BYTES: usize = naming::MAX_KG_NAME_BYTES;

    /// Create a new knowledge graph
    pub fn create_knowledge_graph(&self, name: &str) -> StorageResult<()> {
        let start = Instant::now();
        naming::validate_kg_name(name).map_err(StorageError::InvalidName)?;

        // Check max knowledge graph limit
        let max_kgs = self.config.storage.max_knowledge_graphs;
        if max_kgs > 0 && self.knowledge_graphs.len() >= max_kgs {
            return Err(StorageError::Other(format!(
                "Maximum number of knowledge graphs ({max_kgs}) exceeded"
            )));
        }

        // Block creation if a same-name KG is being dropped (prevents RC-2)
        if self.tombstones.lock().knowledge_graphs.contains(name) {
            return Err(StorageError::Other(format!(
                "Knowledge graph '{name}' is being dropped, cannot create"
            )));
        }

        // Atomic check-and-insert to prevent TOCTOU race
        use dashmap::mapref::entry::Entry;
        let entry = self.knowledge_graphs.entry(name.to_string());
        match entry {
            Entry::Occupied(_) => {
                return Err(StorageError::KnowledgeGraphExists(name.to_string()));
            }
            Entry::Vacant(vacant) => {
                // Create knowledge graph directory structure
                let db_dir = self.config.storage.data_dir.join(name);
                fs::create_dir_all(&db_dir)?;
                fs::create_dir_all(db_dir.join("relations"))?;

                // Create knowledge graph instance (uses persist layer for durability)
                let num_workers = self.config.storage.performance.num_threads;
                let mut kg =
                    KnowledgeGraph::new_with_workers(name.to_string(), db_dir, num_workers);
                kg.max_result_rows = self.config.storage.performance.max_result_rows;
                kg.max_query_cost = self.config.storage.performance.max_query_cost;
                kg.set_optimization(self.config.optimization.clone());

                vacant.insert(Arc::new(RwLock::new(kg)));
            }
        }

        if let Err(e) = self.save_knowledge_graphs_metadata() {
            self.knowledge_graphs.remove(name);
            return Err(e);
        }

        let elapsed_ms = start.elapsed().as_millis() as u64;
        info!(kg = %name, elapsed_ms, "kg_create_complete");

        Ok(())
    }

    /// Phase 1 of KG drop: Fast in-memory removal (~microseconds).
    /// Returns a `KgDropCleanup` token for Phase 2.
    /// Uses interior mutability (DashMap + RwLock) so only needs `&self`.
    pub fn prepare_drop_knowledge_graph(&self, name: &str) -> StorageResult<KgDropCleanup> {
        let start = Instant::now();
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

        // The durable tombstone commits the drop: startup finishes it, and
        // it blocks same-name recreation until cleanup is done (RC-2).
        self.update_tombstones(|t| {
            t.knowledge_graphs.insert(name.to_string());
        })?;

        // Remove from the DashMap, then mark dropped under the KG lock: this
        // waits for in-flight writes, and writers still holding a handle bail.
        if let Some((_, db)) = self.knowledge_graphs.remove(name) {
            db.write().dropped = true;
        }

        // The tombstone outranks a stale listing, so a failure here is benign.
        if let Err(e) = self.save_knowledge_graphs_metadata() {
            warn!(kg = %name, error = %e, "kg_drop_metadata_save_failed");
        }

        let elapsed_ms = start.elapsed().as_millis() as u64;
        info!(kg = %name, elapsed_ms, "kg_drop_prepare_complete");

        Ok(KgDropCleanup {
            name: name.to_string(),
            data_dir: self.config.storage.data_dir.join(name),
            persist: Arc::clone(&self.persist),
        })
    }

    /// Phase 2 of KG drop: Slow file I/O cleanup (NO storage write lock needed).
    /// Deletes persist shards and data directory, then removes the tombstone.
    /// If any cleanup step fails the tombstone stays, and startup retries.
    pub fn finish_drop_knowledge_graph(&self, cleanup: KgDropCleanup) {
        let start = Instant::now();
        let mut kgs = self.list_knowledge_graphs();
        kgs.extend(self.tombstones.lock().knowledge_graphs.iter().cloned());
        if delete_kg_files(&cleanup.persist, &cleanup.name, &cleanup.data_dir, &kgs) {
            let result = self.update_tombstones(|t| {
                t.knowledge_graphs.remove(&cleanup.name);
                t.relations.retain(|r| r.kg != cleanup.name);
            });
            if let Err(e) = result {
                warn!(kg = %cleanup.name, error = %e, "kg_drop_tombstone_clear_failed");
            }
        } else {
            warn!(kg = %cleanup.name, "kg_drop_cleanup_incomplete: retried on restart");
        }
        let elapsed_ms = start.elapsed().as_millis() as u64;
        info!(kg = %cleanup.name, elapsed_ms, "kg_drop_finish_complete");
    }

    /// Drop a knowledge graph (delete all data).
    /// Convenience method that combines prepare + finish phases.
    pub fn drop_knowledge_graph(&self, name: &str) -> StorageResult<()> {
        let cleanup = self.prepare_drop_knowledge_graph(name)?;
        self.finish_drop_knowledge_graph(cleanup);
        Ok(())
    }

    /// Switch to a different knowledge graph
    pub fn use_knowledge_graph(&mut self, name: &str) -> StorageResult<()> {
        let start = Instant::now();
        if !self.knowledge_graphs.contains_key(name) {
            if self.config.storage.auto_create_knowledge_graphs {
                self.create_knowledge_graph(name)?;
            } else {
                return Err(StorageError::KnowledgeGraphNotFound(name.to_string()));
            }
        }

        // Note: Access tracking is handled at save time via KnowledgeGraphInfo::last_accessed.
        // We do NOT overwrite created_at here to preserve the original creation timestamp.

        self.current_kg = Some(name.to_string());
        let elapsed_ms = start.elapsed().as_millis() as u64;
        info!(kg = %name, elapsed_ms, "kg_use_complete");
        Ok(())
    }

    /// Ensure a knowledge graph exists, creating it if auto-create is enabled.
    /// This is a `&self` method suitable for use from read-lock contexts.
    pub fn ensure_knowledge_graph(&self, name: &str) -> StorageResult<()> {
        let start = Instant::now();
        if self.knowledge_graphs.contains_key(name) {
            let elapsed_ms = start.elapsed().as_millis() as u64;
            info!(kg = %name, elapsed_ms, "kg_ensure_exists");
            return Ok(());
        }
        if self.config.storage.auto_create_knowledge_graphs {
            let result = self.create_knowledge_graph(name);
            let elapsed_ms = start.elapsed().as_millis() as u64;
            info!(kg = %name, elapsed_ms, "kg_ensure_created");
            result
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
        validate_names(kg, relation)?;
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
            db.validate_index_rows(relation, &tuples)
                .map_err(StorageError::Other)?;
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

        // Under the KG write lock: compute the effective delta, assign time,
        // persist, apply. Disk and memory see writes in the same order.
        let db = self.kg_handle(kg)?;
        let mut db = Self::lock_live(&db, kg)?;
        let total = tuples.len();
        let new_tuples = db.absent_tuples(relation, tuples);
        if new_tuples.is_empty() {
            return Ok((0, total));
        }

        self.settle_relation_drop(&mut db, kg, relation)?;
        let shard = format!("{kg}:{relation}");
        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);
        let updates: Vec<Update> = new_tuples
            .iter()
            .map(|data| Update::insert(data.clone(), time))
            .collect();

        let persist_start = Instant::now();
        self.persist.ensure_shard(&shard)?;
        self.persist.append(&shard, &updates)?;
        let persist_ms = persist_start.elapsed().as_millis() as u64;
        info!(
            kg = %kg,
            relation = %relation,
            tuples = updates.len(),
            persist_ms,
            "persist_append_complete"
        );

        let new_count = new_tuples.len();
        db.insert_in_memory(relation, new_tuples, time)?;
        Ok((new_count, total - new_count))
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
        validate_names(kg, relation)?;
        if tuples.is_empty() {
            return Ok(0);
        }

        let db = self.kg_handle(kg)?;
        let mut db = Self::lock_live(&db, kg)?;
        let present = db.present_tuples(relation, &tuples);
        if present.is_empty() {
            return Ok(0);
        }

        self.settle_relation_drop(&mut db, kg, relation)?;
        let shard = format!("{kg}:{relation}");
        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);
        let updates: Vec<Update> = present
            .iter()
            .map(|data| Update::delete(data.clone(), time))
            .collect();

        self.persist.ensure_shard(&shard)?;
        self.persist.append(&shard, &updates)?;

        db.delete_in_memory(relation, present, time)
    }

    /// Clone a KG handle without holding the `DashMap` shard lock.
    fn kg_handle(&self, kg: &str) -> StorageResult<Arc<RwLock<KnowledgeGraph>>> {
        self.knowledge_graphs
            .get(kg)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))
    }

    /// Write-lock a KG handle, failing if the KG was dropped while waiting.
    fn lock_live<'a>(
        db: &'a RwLock<KnowledgeGraph>,
        kg: &str,
    ) -> StorageResult<parking_lot::RwLockWriteGuard<'a, KnowledgeGraph>> {
        let db = db.write();
        if db.dropped {
            return Err(StorageError::KnowledgeGraphNotFound(kg.to_string()));
        }
        Ok(db)
    }

    fn tombstones_path(&self) -> PathBuf {
        self.config.storage.data_dir.join("metadata/dropping.json")
    }

    /// Apply `f` to the drop tombstones and persist them. Memory only
    /// changes once the save succeeds.
    fn update_tombstones(&self, f: impl FnOnce(&mut DropTombstones)) -> StorageResult<()> {
        let mut current = self.tombstones.lock();
        let mut next = current.clone();
        f(&mut next);
        if next != *current {
            next.save(&self.tombstones_path())?;
            *current = next;
        }
        Ok(())
    }

    /// Finish a committed drop of `relation` whose cleanup failed, before
    /// anything writes to that name again.
    fn settle_relation_drop(
        &self,
        db: &mut KnowledgeGraph,
        kg: &str,
        relation: &str,
    ) -> StorageResult<()> {
        let pending = {
            let tombstones = self.tombstones.lock();
            tombstones
                .relations
                .iter()
                .any(|t| t.kg == kg && t.relation == relation)
        };
        if !pending {
            return Ok(());
        }
        self.persist.delete_shard(&format!("{kg}:{relation}"))?;
        db.save_schema_catalog().map_err(StorageError::Other)?;
        let tombstone = RelationTombstone::new(kg, relation);
        self.update_tombstones(|t| {
            t.relations.remove(&tombstone);
        })
    }

    /// Execute an IQL query on the current knowledge graph
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

    /// Execute an IQL query on a specific knowledge graph (explicit API)
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

    /// Execute an IQL query on the current knowledge graph, returning arbitrary arity tuples
    pub fn execute_query_tuples(&self, program: &str) -> StorageResult<Vec<Tuple>> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.execute_query_tuples_on(&db_name, program)
    }

    /// Execute an IQL query on a specific knowledge graph, returning arbitrary arity tuples
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

    /// Debug a query plan without executing it.
    ///
    /// Runs parse → IR → optimize on the query and returns the pipeline trace.
    pub fn debug_query_on(
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

        // Prepend persistent rules (same as execute_with_rules) so the debug
        // plan matches actual execution behavior.
        let combined = if snapshot.rule_prefix().is_empty() {
            program.to_string()
        } else {
            format!("{}{}", snapshot.rule_prefix(), program)
        };

        let mut engine = snapshot.new_engine();
        engine.set_inputs(snapshot.input_tuples.as_ref().clone());

        engine
            .debug(&combined)
            .map_err(|e| StorageError::Other(format!("Query debug failed: {e}")))
    }

    /// Execute a query with rules and return both results and proof context.
    ///
    /// Uses a single snapshot for consistency between the query results
    /// and the rules/data used for proof tree construction.
    pub fn execute_and_get_context(
        &self,
        kg: &str,
        program: &str,
    ) -> StorageResult<(
        Vec<Tuple>,
        Vec<crate::ast::Rule>,
        std::collections::HashMap<String, Vec<Tuple>>,
        std::collections::HashMap<String, Vec<Tuple>>, // derived relation data
        std::collections::HashMap<String, String>,     // index_name -> metric
    )> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let (snapshot, index_metrics) = {
            let db_guard = db.read();
            let snap = db_guard.snapshot();
            // Index metrics for HNSW proof enrichment
            let metrics = db_guard.index_metrics();
            (snap, metrics)
        };

        let (result_tuples, derived_data) = snapshot
            .execute_with_rules_tuples_and_derived(program)
            .map_err(|e| StorageError::Other(format!("Query execution failed: {e}")))?;

        let rules = snapshot.rules.as_ref().clone();
        let base_data = to_vec_map(&snapshot.input_tuples);
        let derived_data = to_vec_map(&derived_data);

        Ok((result_tuples, rules, base_data, derived_data, index_metrics))
    }

    /// Get rules and base data for a knowledge graph (for provenance queries).
    pub fn get_rules_and_data(
        &self,
        kg: &str,
    ) -> StorageResult<(
        Vec<crate::ast::Rule>,
        std::collections::HashMap<String, Vec<crate::value::Tuple>>,
    )> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let snapshot = {
            let db_guard = db.read();
            db_guard.snapshot()
        };

        let rules = snapshot.rules.as_ref().clone();
        let base_data = to_vec_map(&snapshot.input_tuples);
        Ok((rules, base_data))
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

    /// Compact only shards that exceed the batch file threshold.
    /// Returns the number of shards compacted.
    pub fn compact_if_needed(&self, threshold: usize) -> StorageResult<usize> {
        if threshold == 0 {
            return Ok(0);
        }
        let mut compacted = 0;
        for shard_name in self.persist.list_shards()? {
            let info = self.persist.shard_info(&shard_name)?;
            if info.batch_count >= threshold {
                tracing::info!(
                    shard = %shard_name,
                    batch_count = info.batch_count,
                    threshold,
                    "auto_compact_shard"
                );
                self.persist.compact(&shard_name, 0)?;
                compacted += 1;
            }
        }
        if compacted > 0 {
            self.persist.sync()?;
            self.save_knowledge_graphs_metadata()?;
        }
        Ok(compacted)
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

    /// Drop a relation entirely from a specific knowledge graph.
    ///
    /// Removes all data, metadata, schema, and any associated rules, and
    /// deletes the persist shard. A durable tombstone is written first and the
    /// KG write lock is held throughout, so neither a crash nor a concurrent
    /// write brings the relation back. If the shard cannot be deleted and
    /// nothing changed, returns the error and the relation stays.
    pub fn drop_relation_in(&self, kg: &str, name: &str) -> StorageResult<()> {
        let db = self.kg_handle(kg)?;
        let mut db = Self::lock_live(&db, kg)?;
        if !db.has_relation(name) {
            return Err(StorageError::Other(format!(
                "Failed to drop relation: Relation '{name}' not found."
            )));
        }

        let tombstone = RelationTombstone::new(kg, name);
        self.update_tombstones(|t| {
            t.relations.insert(tombstone.clone());
        })?;

        let shard = format!("{kg}:{name}");
        let had_shard = self.persist.shard_info(&shard).is_ok();
        let mut settled = true;
        if let Err(e) = self.persist.delete_shard(&shard) {
            let intact = self.persist.shard_info(&shard).is_ok() == had_shard;
            let rolled_back = intact
                && self
                    .update_tombstones(|t| {
                        t.relations.remove(&tombstone);
                    })
                    .is_ok();
            if rolled_back {
                return Err(e);
            }
            // Committed: pending cleanup runs before the next write or on restart.
            warn!(kg = %kg, relation = %name, error = %e, "relation_drop_cleanup_deferred");
            settled = false;
        }

        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);
        if let Err(e) = db.drop_relation(name, time) {
            warn!(kg = %kg, relation = %name, error = %e, "relation_drop_cleanup_deferred");
            settled = false;
        }

        if settled {
            if let Err(e) = self.update_tombstones(|t| {
                t.relations.remove(&tombstone);
            }) {
                warn!(kg = %kg, relation = %name, error = %e, "relation_drop_tombstone_clear_failed");
            }
        }
        Ok(())
    }

    /// Drop all rules matching a prefix from a specific knowledge graph.
    /// Returns the list of dropped rule names.
    pub fn drop_rules_by_prefix_in(&self, kg: &str, prefix: &str) -> StorageResult<Vec<String>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        db.drop_rules_by_prefix(prefix)
            .map_err(|e| StorageError::Other(format!("Failed to drop rules by prefix: {e}")))
    }

    /// Clear all facts from relations matching a prefix in a knowledge graph.
    ///
    /// Returns list of (relation_name, count_deleted) for each affected relation.
    /// See [`KnowledgeGraph::clear_relations_by_prefix`] for partial failure.
    pub fn clear_relations_by_prefix_in(
        &self,
        kg: &str,
        prefix: &str,
    ) -> StorageResult<Vec<(String, usize)>> {
        let db = self.kg_handle(kg)?;
        let mut db = Self::lock_live(&db, kg)?;
        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);
        db.clear_relations_by_prefix(prefix, time, &self.persist, kg)
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
    pub fn clear_rule(&self, name: &str) -> StorageResult<()> {
        let db_name = self
            .current_kg
            .as_ref()
            .ok_or(StorageError::NoCurrentKnowledgeGraph)?
            .clone();

        self.clear_rule_in(&db_name, name)
    }

    /// Clear all clauses from a rule for editing/redefining (specific knowledge graph)
    pub fn clear_rule_in(&self, kg: &str, name: &str) -> StorageResult<()> {
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
        validate_names(kg, &schema.name)?;
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        self.settle_relation_drop(&mut db, kg, &schema.name)?;
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

    /// Execute a closure with read access to a specific KnowledgeGraph.
    ///
    /// This helper provides a convenient way to access KG internals
    /// (like the IncrementalEngine) from Handler methods.
    pub fn with_kg_read<T, F>(&self, kg: &str, f: F) -> StorageResult<T>
    where
        F: FnOnce(&KnowledgeGraph) -> Result<T, String>,
    {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;
        let db = db.read();
        f(&db).map_err(StorageError::Other)
    }

    /// Execute a closure with write access to a specific KnowledgeGraph.
    pub fn with_kg_mut<T, F>(&self, kg: &str, f: F) -> StorageResult<T>
    where
        F: FnOnce(&mut KnowledgeGraph) -> Result<T, String>,
    {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;
        let mut db = db.write();
        f(&mut db).map_err(StorageError::Other)
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
        validate_names(kg, &schema.name)?;
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let mut db = db.write();
        self.settle_relation_drop(&mut db, kg, &schema.name)?;
        db.register_or_update_schema(schema)
            .map_err(StorageError::Other)
    }

    /// Register or update a session schema in a specific knowledge graph (not persisted)
    pub fn register_or_update_session_schema_in(
        &self,
        kg: &str,
        schema: RelationSchema,
    ) -> StorageResult<()> {
        validate_names(kg, &schema.name)?;
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

    /// Get a snapshot for a specific knowledge graph.
    ///
    /// Returns an `Arc<KnowledgeGraphSnapshot>` that can be used for lock-free
    /// query execution. Callers can release the storage lock after obtaining
    /// the snapshot and run DD computations without holding any locks.
    pub fn get_snapshot_for(&self, kg: &str) -> StorageResult<Arc<KnowledgeGraphSnapshot>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let db_guard = db.read();
        Ok(db_guard.snapshot())
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

    /// Get relation metadata for a specific knowledge graph.
    /// Prefers column names from the schema catalog when available.
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
            let columns = if let Some(schema) = db.schema_catalog.get(name) {
                schema.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                rel_meta.schema.clone()
            };
            Ok(Some((columns, rel_meta.tuple_count)))
        } else {
            Ok(None)
        }
    }

    /// List relations with metadata for a specific knowledge graph.
    /// Prefers column names from the schema catalog when available.
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
            .map(|(name, meta)| {
                // Use schema catalog column names if a typed schema was declared
                let columns = if let Some(schema) = db.schema_catalog.get(name) {
                    schema.columns.iter().map(|c| c.name.clone()).collect()
                } else {
                    meta.schema.clone()
                };
                (name.clone(), columns, meta.tuple_count)
            })
            .collect();
        Ok(relations)
    }

    /// List relations with typed metadata for a specific knowledge graph.
    /// Returns (name, Vec<(column_name, type_name)>, tuple_count).
    /// When a typed schema is declared, uses the declared type; otherwise defaults to "any".
    pub fn list_relations_with_typed_metadata_in(
        &self,
        kg: &str,
    ) -> StorageResult<Vec<(String, Vec<(String, String)>, usize)>> {
        let db = self
            .knowledge_graphs
            .get(kg)
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))?;

        let db = db.read();
        let relations: Vec<(String, Vec<(String, String)>, usize)> = db
            .metadata
            .relations
            .iter()
            .map(|(name, meta)| {
                let typed_columns = if let Some(schema) = db.schema_catalog.get(name) {
                    schema
                        .columns
                        .iter()
                        .map(|c| (c.name.clone(), c.data_type.to_string()))
                        .collect()
                } else {
                    meta.schema
                        .iter()
                        .map(|col_name| (col_name.clone(), "any".to_string()))
                        .collect()
                };
                (name.clone(), typed_columns, meta.tuple_count)
            })
            .collect();
        Ok(relations)
    }

    /// Load all knowledge graphs from persist layer
    ///
    /// Recovery process:
    /// 1. Discover knowledge graphs from metadata
    /// 2. Finish tombstoned drops; their KGs and relations are never loaded
    /// 3. Assign each shard to the longest KG name prefixing it
    /// 4. Consolidate each shard's updates to get current state
    /// 5. Populate in-memory `IQLEngine`
    fn load_all_knowledge_graphs(&mut self) -> StorageResult<()> {
        let mut kg_names: HashSet<String> = HashSet::new();
        let metadata_path = self
            .config
            .storage
            .data_dir
            .join("metadata/knowledge_graphs.json");
        if metadata_path.exists() {
            if let Ok(metadata) = KnowledgeGraphsMetadata::load(&metadata_path) {
                kg_names.extend(metadata.knowledge_graphs.into_iter().map(|kg| kg.name));
            }
        }

        let tombstones = DropTombstones::load(&self.tombstones_path())?;
        let mut pending = tombstones.clone();
        let listed_dropped = tombstones
            .knowledge_graphs
            .iter()
            .any(|kg| kg_names.contains(kg));
        let known: Vec<String> = kg_names
            .iter()
            .chain(&tombstones.knowledge_graphs)
            .cloned()
            .collect();
        for kg in &tombstones.knowledge_graphs {
            kg_names.remove(kg);
            let data_dir = self.config.storage.data_dir.join(kg);
            if delete_kg_files(&self.persist, kg, &data_dir, &known) {
                pending.knowledge_graphs.remove(kg);
                pending.relations.retain(|r| &r.kg != kg);
            }
        }
        let mut deleted = Vec::new();
        for t in &tombstones.relations {
            match self
                .persist
                .delete_shard(&format!("{}:{}", t.kg, t.relation))
            {
                Ok(()) => deleted.push(t.clone()),
                Err(e) => {
                    warn!(kg = %t.kg, relation = %t.relation, error = %e, "relation_drop_recovery_failed");
                }
            }
        }
        let shard_names = self.persist.list_shards()?;

        // Metadata is missing or stale when no listed KG claims a shard, or the
        // claimed relation contains ':' (a legacy colon KG). Infer the KG as
        // everything before the last ':'.
        let inferred: HashSet<String> = shard_names
            .iter()
            .filter(|shard| {
                naming::shard_owner(shard, kg_names.iter().map(String::as_str))
                    .is_none_or(|(_, rel)| rel.contains(':'))
            })
            .filter_map(|shard| shard.rsplit_once(':').map(|(kg, _)| kg.to_string()))
            .filter(|kg| !kg_names.contains(kg) && !tombstones.knowledge_graphs.contains(kg))
            .collect();
        for kg in &inferred {
            tracing::warn!(kg = %kg, "kg_missing_from_metadata: inferred from persist shards");
        }
        kg_names.extend(inferred);

        let mut kg_shards: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();
        for shard in &shard_names {
            if let Some((kg, relation)) =
                naming::shard_owner(shard, kg_names.iter().map(String::as_str))
            {
                if tombstones
                    .relations
                    .contains(&RelationTombstone::new(kg, relation))
                {
                    continue;
                }
                kg_shards
                    .entry(kg.to_string())
                    .or_default()
                    .push((shard.clone(), relation.to_string()));
            }
        }
        report_invalid_names(&kg_names, &kg_shards);

        // Load each knowledge graph
        let total_kgs = kg_names.len();
        let load_start = std::time::Instant::now();
        let mut corrections = Vec::new();
        for (i, kg_name) in kg_names.into_iter().enumerate() {
            let kg_dir = self.config.storage.data_dir.join(&kg_name);
            fs::create_dir_all(&kg_dir)?;

            let shards = kg_shards.remove(&kg_name).unwrap_or_default();
            let kg = self.load_knowledge_graph_from_persist(
                &kg_name,
                kg_dir,
                &shards,
                &mut corrections,
            )?;
            self.knowledge_graphs
                .insert(kg_name, Arc::new(RwLock::new(kg)));

            if total_kgs >= 10 && (i + 1) % 10 == 0 {
                tracing::info!(loaded = i + 1, total = total_kgs, "kg_load_progress");
            }
        }
        tracing::info!(
            knowledge_graphs = total_kgs,
            elapsed_ms = load_start.elapsed().as_millis() as u64,
            "kg_load_complete"
        );

        // Update logical time to be after all loaded data
        let max_time = self.find_max_logical_time()?;
        self.logical_time.store(max_time + 1, Ordering::SeqCst);

        // Clamp shards whose multiplicities drifted from set membership
        if !corrections.is_empty() {
            let time = self.logical_time.fetch_add(1, Ordering::SeqCst);
            for (shard, mut fixes) in corrections {
                fixes.iter_mut().for_each(|u| u.time = time);
                tracing::warn!(shard = %shard, tuples = fixes.len(), "persist_multiplicity_clamped");
                self.persist.append(&shard, &fixes)?;
            }
        }

        // A drop may have crashed before its schema removal was saved.
        for t in deleted {
            let scrubbed = match self.knowledge_graphs.get(&t.kg) {
                Some(db) => db.write().remove_schema(&t.relation).map(|_| ()),
                None => Ok(()),
            };
            match scrubbed {
                Ok(()) => {
                    pending.relations.remove(&t);
                }
                Err(e) => {
                    warn!(kg = %t.kg, relation = %t.relation, error = %e, "relation_drop_recovery_failed");
                }
            }
        }
        if listed_dropped {
            if let Err(e) = self.save_knowledge_graphs_metadata() {
                warn!(error = %e, "kg_drop_recovery_metadata_save_failed");
                pending
                    .knowledge_graphs
                    .clone_from(&tombstones.knowledge_graphs);
            }
        }
        *self.tombstones.get_mut() = tombstones;
        if let Err(e) = self.update_tombstones(|t| *t = pending) {
            warn!(error = %e, "drop_tombstones_save_failed");
        }

        Ok(())
    }

    /// Load a single knowledge graph from persist layer
    fn load_knowledge_graph_from_persist(
        &self,
        name: &str,
        data_dir: PathBuf,
        shards: &[(String, String)],
        corrections: &mut Vec<(String, Vec<Update>)>,
    ) -> StorageResult<KnowledgeGraph> {
        let mut store = RelationStore::new();
        let mut metadata = KnowledgeGraphMetadata::new(name.to_string());

        for (shard_name, relation) in shards {
            // Get shard info to determine since frontier
            let info = self.persist.shard_info(shard_name)?;

            // Read and consolidate updates
            let mut updates = self.persist.read(shard_name, info.since)?;
            consolidate_to_current(&mut updates);

            // Extract current tuples (positive multiplicities only)
            let tuples = to_tuples(&updates);
            let fixes = set_semantics_corrections(&updates, 0);
            if !fixes.is_empty() {
                corrections.push((shard_name.clone(), fixes));
            }

            if !tuples.is_empty() {
                // Infer schema from first tuple
                let arity = tuples.first().map_or(2, super::value::Tuple::arity);
                let schema: Vec<String> = (0..arity).map(|i| format!("col{i}")).collect();
                let tuple_count = tuples.len();

                // Update metadata with relation info
                metadata.add_relation(relation.clone(), schema, tuple_count);

                store.set(relation, tuples);
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
        let mut initial = KnowledgeGraphSnapshot::new_with_workers(
            store.relations().clone(),
            rule_catalog.all_rules(),
            num_workers,
        );
        initial.optimization = self.config.optimization.clone();
        let snapshot = ArcSwap::from_pointee(initial);

        let mut kg = KnowledgeGraph {
            name: name.to_string(),
            store,
            metadata,
            data_dir,
            rule_catalog,
            schema_catalog,
            snapshot,
            incremental: None,
            auto_materialize: false,
            indexes: IndexManager::new(),
            num_workers,
            max_result_rows: self.config.storage.performance.max_result_rows,
            max_query_cost: self.config.storage.performance.max_query_cost,
            optimization: self.config.optimization.clone(),
            dropped: false,
        };
        kg.restore_indexes();
        if !kg.indexes.is_empty() {
            kg.publish_snapshot();
        }
        Ok(kg)
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
        let start = Instant::now();
        let metadata_dir = self.config.storage.data_dir.join("metadata");
        if let Err(e) = fs::create_dir_all(&metadata_dir) {
            eprintln!(
                "[storage] ERROR create metadata dir: path={}, error={}",
                metadata_dir.display(),
                e
            );
            return Err(e.into());
        }

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

        let elapsed_ms = start.elapsed().as_millis() as u64;
        info!(elapsed_ms, "save_metadata_complete");
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
            store: RelationStore::new(),
            metadata: KnowledgeGraphMetadata::new(name),
            data_dir,
            rule_catalog,
            schema_catalog,
            snapshot,
            incremental: None,
            auto_materialize: false,
            indexes: IndexManager::new(),
            num_workers,
            max_result_rows: 0,
            max_query_cost: 0,
            optimization: crate::OptimizationConfig::default(),
            dropped: false,
        }
    }

    /// Set the optimizer passes for this KG's engines and published snapshots.
    fn set_optimization(&mut self, config: crate::OptimizationConfig) {
        let mut snapshot = (**self.snapshot.load()).clone();
        snapshot.optimization = config.clone();
        self.snapshot.store(Arc::new(snapshot));
        self.optimization = config;
    }

    /// Enable the IncrementalEngine for incremental updates.
    ///
    /// Creates a persistent DD computation worker thread for this knowledge graph.
    /// Once enabled, all inserts and deletes are shadow-written to DD.
    /// This is required for reading from arrangements.
    ///
    /// # Errors
    /// Returns error if worker thread fails to spawn or replaying existing data fails.
    pub fn enable_incremental(&mut self) -> StorageResult<()> {
        if self.incremental.is_none() {
            let dd =
                IncrementalEngine::new(vec![]).map_err(StorageError::IncrementalEngineError)?;

            // Replay existing data into IncrementalEngine so arrangements are
            // populated immediately. This handles the case where data was
            // loaded from persistence before IncrementalEngine was enabled.
            for (relation, tuples) in self.store.relations() {
                if !tuples.is_empty() {
                    dd.insert(relation, tuples.to_vec(), 0)
                        .map_err(StorageError::IncrementalEngineError)?;
                }
            }

            self.incremental = Some(dd);
        }
        Ok(())
    }

    /// Serve rules from materializations recomputed on every publish.
    ///
    /// Takes effect only with the incremental engine enabled. Turning it off
    /// drops every materialization, since nothing keeps them fresh.
    pub fn set_auto_materialize(&mut self, enabled: bool) {
        self.auto_materialize = enabled;
        if !enabled {
            if let Some(dd) = &self.incremental {
                dd.derived_relations().lock().clear_all_materialized();
            }
        }
        self.publish_snapshot();
    }

    /// Get a reference to the IncrementalEngine (if enabled).
    ///
    /// Used for reading from DD arrangements and verifying consistency.
    pub fn incremental(&self) -> Option<&IncrementalEngine> {
        self.incremental.as_ref()
    }

    /// Publish a new snapshot atomically
    ///
    /// Called after data modifications to make changes visible to readers.
    /// Relations are shared with the store, so this copies no tuples (apart
    /// from merging valid materializations).
    ///
    /// If IncrementalEngine has valid materializations, includes them
    /// in the snapshot. Materialized tuples are merged into input_tuples,
    /// and their rules are skipped during query execution.
    ///
    /// IMPORTANT: This method holds the DerivedRelationsManager lock through
    /// the entire snapshot creation AND publication to prevent TOCTOU races.
    /// Without this, another thread could invalidate materializations between
    /// reading them and publishing the snapshot.
    fn publish_snapshot(&self) {
        let snapshot_start = Instant::now();
        if self.auto_materialize {
            self.refresh_materializations();
        }
        // Start with base relation data
        let mut input_tuples = self.store.relations().clone();
        let rules = self.rule_catalog.all_rules();

        // Gather valid materializations from IncrementalEngine
        // CRITICAL: Hold the lock through snapshot creation AND publication
        // to prevent TOCTOU race conditions.
        if let Some(ref dd) = self.incremental {
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
            let mut new_snapshot = KnowledgeGraphSnapshot::new_with_materializations(
                input_tuples,
                rules,
                self.num_workers,
                materialized_names,
            );
            new_snapshot.max_result_rows = self.max_result_rows;
            new_snapshot.max_query_cost = self.max_query_cost;
            new_snapshot.optimization = self.optimization.clone();
            new_snapshot.hnsw_search_fn = self.hnsw_search_fn();
            self.snapshot.store(Arc::new(new_snapshot));

            // Lock drops here AFTER publication - this is the fix for TOCTOU
        } else {
            // No DD computation - publish without materializations
            let mut new_snapshot = KnowledgeGraphSnapshot::new_with_materializations(
                input_tuples,
                rules,
                self.num_workers,
                HashSet::new(),
            );
            new_snapshot.max_result_rows = self.max_result_rows;
            new_snapshot.max_query_cost = self.max_query_cost;
            new_snapshot.optimization = self.optimization.clone();
            new_snapshot.hnsw_search_fn = self.hnsw_search_fn();
            self.snapshot.store(Arc::new(new_snapshot));
        }

        info!(
            relations = self.store.relations().len(),
            rules = self.rule_catalog.all_rules().len(),
            snapshot_ms = snapshot_start.elapsed().as_millis() as u64,
            "snapshot_publish_complete"
        );
    }

    /// HNSW search over the indexes as of now (None without indexes).
    ///
    /// Captures each index at its current epoch, which matches the base data
    /// being published: both change only under this KG's write lock.
    fn hnsw_search_fn(&self) -> Option<crate::index_manager::HnswSearchFn> {
        if self.indexes.is_empty() {
            None
        } else {
            Some(crate::index_manager::search_fn(self.indexes.views()))
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
    /// 1. Stores the tuples in IncrementalEngine's DerivedRelationsManager
    /// 2. Publishes a new snapshot that includes the materialized data
    ///
    /// After this call, queries via `snapshot()` will see the materialized data.
    pub fn materialize_derived_relation(
        &self,
        relation: &str,
        tuples: Vec<Tuple>,
    ) -> Result<(), String> {
        if let Some(ref dd) = self.incremental {
            dd.set_materialized(relation, tuples)?;
            self.publish_snapshot();
            Ok(())
        } else {
            Err("IncrementalEngine not enabled".to_string())
        }
    }

    /// Distinct tuples from `tuples` not yet in `relation`, in input order.
    fn absent_tuples(&self, relation: &str, tuples: Vec<Tuple>) -> Vec<Tuple> {
        self.store.absent(relation, tuples)
    }

    /// Distinct tuples from `tuples` currently in `relation`.
    fn present_tuples(&self, relation: &str, tuples: &[Tuple]) -> Vec<Tuple> {
        self.store.present(relation, tuples)
    }

    /// Insert tuples into in-memory state only
    ///
    /// `tuples` should be the effective delta (see `absent_tuples`); the store
    /// still dedups. Persistence is handled by `StorageEngine`.
    ///
    /// # Errors
    /// Returns error if DD shadow write fails.
    fn insert_in_memory(
        &mut self,
        relation: &str,
        tuples: Vec<Tuple>,
        time: u64,
    ) -> StorageResult<()> {
        // Infer schema from first tuple if available
        let schema = if let Some(first) = tuples.first() {
            (0..first.arity())
                .map(|i| format!("col{i}"))
                .collect::<Vec<_>>()
        } else {
            vec!["col0".to_string(), "col1".to_string()]
        };

        let (added, _) = self.store.insert(relation, tuples);
        let new_count = added.len();
        let tuple_count = self.store.get(relation).map_or(0, Relation::len);

        // Update metadata
        self.metadata
            .add_relation(relation.to_string(), schema, tuple_count);

        // Shadow write to IncrementalEngine (if enabled).
        if new_count > 0 {
            self.index_inserted(relation, &added);
            if let Some(dd) = &self.incremental {
                dd.insert(relation, added, time)
                    .map_err(StorageError::IncrementalEngineError)?;
                // Invalidate derived relations that depend on this base
                dd.notify_base_update(relation)
                    .map_err(StorageError::IncrementalEngineError)?;
            }
            // Publish new snapshot for lock-free reads
            self.publish_snapshot();
        }

        info!(
            relation = %relation,
            new_count,
            time,
            "insert_in_memory_complete"
        );

        Ok(())
    }

    /// Delete tuples from in-memory state only
    ///
    /// `tuples` should be the effective delta (see `present_tuples`).
    /// Persistence is handled by `StorageEngine`. Returns the count of
    /// deleted tuples.
    ///
    /// # Errors
    /// Returns error if DD shadow write fails.
    fn delete_in_memory(
        &mut self,
        relation: &str,
        tuples: Vec<Tuple>,
        time: u64,
    ) -> StorageResult<usize> {
        let removed = self.store.delete(relation, &tuples);
        if removed.is_empty() {
            return Ok(0);
        }
        let final_count = self.store.get(relation).map_or(0, Relation::len);

        // Get schema from metadata (which has the correct arity from insert time)
        // Avoid using catalog which may not have the schema for base facts
        let schema = self.metadata.relations.get(relation).map_or_else(
            || vec!["col0".to_string(), "col1".to_string()],
            |r| r.schema.clone(),
        );
        self.metadata
            .add_relation(relation.to_string(), schema, final_count);

        let deleted_count = removed.len();
        self.index_deleted(relation, &removed);
        if let Some(dd) = &self.incremental {
            dd.delete(relation, removed, time)
                .map_err(StorageError::IncrementalEngineError)?;
            // Invalidate derived relations that depend on this base
            dd.notify_base_update(relation)
                .map_err(StorageError::IncrementalEngineError)?;
        }

        // Publish new snapshot for lock-free reads
        self.publish_snapshot();

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
    /// With the incremental engine on, the rule is also registered there for
    /// dependency tracking.
    pub fn register_rule(
        &mut self,
        rule_def: &RuleDef,
    ) -> Result<crate::rule_catalog::RuleRegisterResult, String> {
        let result = self.rule_catalog.register_rule(rule_def)?;

        // Register with IncrementalEngine for materialization
        if let Some(ref dd) = self.incremental {
            let compiled_rule = self.compile_rule_for_dd(rule_def);
            if let Err(e) = dd.register_rule(compiled_rule) {
                warn!(rule = %rule_def.name, error = %e, "incremental_register_rule_failed");
            }
        }

        self.publish_snapshot();
        Ok(result)
    }

    /// Recompute every rule from base data into the incremental engine.
    ///
    /// A rule that fails to evaluate loses its materialization, so queries
    /// fall back to evaluating it.
    fn refresh_materializations(&self) {
        let Some(dd) = &self.incremental else {
            return;
        };
        for name in self.rule_catalog.list() {
            match self.evaluate_rule(&name) {
                Ok(tuples) => {
                    let _ = dd.set_materialized(&name, tuples);
                }
                Err(e) => {
                    dd.derived_relations().lock().clear_materialized(&name);
                    warn!(rule = %name, error = %e, "auto_materialize_failed");
                }
            }
        }
    }

    /// Evaluate one rule over base data with every registered rule in scope.
    fn evaluate_rule(&self, rule_name: &str) -> Result<Vec<Tuple>, String> {
        let arity = self
            .rule_catalog
            .rule_arity(rule_name)
            .ok_or_else(|| format!("Rule '{rule_name}' not found"))?;

        let mut program = String::new();
        for clause in self.rule_catalog.all_rules() {
            program.push_str(&format_rule(&clause));
            program.push('\n');
        }
        let vars = (0..arity)
            .map(|i| format!("V{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        program.push_str(&format!("__materialize({vars}) <- {rule_name}({vars})"));

        let mut temp_engine = crate::IQLEngine::with_config(self.optimization.clone());
        temp_engine.set_inputs(self.store.relations().clone());
        temp_engine.set_num_workers(self.num_workers);
        temp_engine.execute_tuples(&program)
    }

    /// Compile a RuleDef into a CompiledRule for IncrementalEngine
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

        // Remove from IncrementalEngine
        if let Some(ref dd) = self.incremental {
            if let Err(e) = dd.remove_rule(name) {
                warn!(rule = %name, error = %e, "incremental_remove_rule_failed");
            }
        }

        self.publish_snapshot();
        Ok(())
    }

    /// Whether `name` exists as data, metadata, a rule, or a schema.
    fn has_relation(&self, name: &str) -> bool {
        self.metadata.relations.contains_key(name)
            || self.store.contains_relation(name)
            || self.rule_catalog.exists(name)
            || self.schema_catalog.get(name).is_some()
    }

    /// Drop a relation entirely: data, metadata, schema, any associated rules
    /// and indexes, and its DD base data; invalidates dependents.
    ///
    /// # Errors
    /// Returns an error if the relation is missing, or if persisting the
    /// schema or rule removal fails (memory is dropped regardless).
    pub fn drop_relation(&mut self, name: &str, time: u64) -> Result<(), String> {
        if !self.has_relation(name) {
            return Err(format!("Relation '{name}' not found."));
        }

        let tuples = self
            .store
            .get(name)
            .map(Relation::to_vec)
            .unwrap_or_default();
        self.store.remove(name);
        self.metadata.relations.remove(name);
        self.schema_catalog.remove_session(name);
        let schema = self.remove_schema(name).map(|_| ());
        let rule = if self.rule_catalog.exists(name) {
            self.rule_catalog.drop(name)
        } else {
            Ok(())
        };

        if let Some(ref dd) = self.incremental {
            let retracted = if tuples.is_empty() {
                Ok(())
            } else {
                dd.delete(name, tuples, time)
            };
            let result = retracted
                .and_then(|()| dd.remove_rule(name))
                .and_then(|()| dd.notify_base_update(name).map(|_| ()));
            if let Err(e) = result {
                warn!(relation = %name, error = %e, "incremental_drop_relation_failed");
            }
        }

        self.drop_indexes_for(name);
        self.publish_snapshot();
        schema.and(rule)
    }

    /// Drop all rules matching a prefix.
    /// Returns the list of dropped rule names.
    pub fn drop_rules_by_prefix(&mut self, prefix: &str) -> Result<Vec<String>, String> {
        let dropped = self.rule_catalog.drop_by_prefix(prefix)?;

        // Remove each from IncrementalEngine
        if let Some(ref dd) = self.incremental {
            for name in &dropped {
                if let Err(e) = dd.remove_rule(name) {
                    warn!(rule = %name, error = %e, "incremental_remove_rule_failed");
                }
            }
        }

        if !dropped.is_empty() {
            self.publish_snapshot();
        }
        Ok(dropped)
    }

    /// Clear all facts from relations whose name starts with the given prefix.
    ///
    /// Returns a list of (relation_name, deleted_count) for each affected relation.
    /// Does NOT remove the relations themselves or their schemas - only clears data.
    ///
    /// # Errors
    /// Relations are cleared in name order, each persisted before memory is
    /// touched. A persist failure returns the error; relations before the
    /// failing one stay cleared, it and later ones stay intact.
    pub fn clear_relations_by_prefix(
        &mut self,
        prefix: &str,
        time: u64,
        persist: &FilePersist,
        kg_name: &str,
    ) -> StorageResult<Vec<(String, usize)>> {
        // Find all relations matching the prefix (sorted for deterministic output)
        let mut matching: Vec<String> = self
            .store
            .names()
            .filter(|name| name.starts_with(prefix))
            .cloned()
            .collect();
        matching.sort();

        if matching.is_empty() {
            return Ok(vec![]);
        }

        let mut results = Vec::new();

        for relation in &matching {
            if let Some(tuples) = self.store.get(relation) {
                let count = tuples.len();
                if count == 0 {
                    continue;
                }

                // Persist before touching memory so a failed write leaves both intact
                let shard = format!("{kg_name}:{relation}");
                let updates: Vec<Update> = tuples
                    .iter()
                    .map(|t| Update::delete(t.clone(), time))
                    .collect();
                if let Err(e) = persist
                    .ensure_shard(&shard)
                    .and_then(|()| persist.append(&shard, &updates))
                {
                    if !results.is_empty() {
                        self.publish_snapshot();
                    }
                    return Err(e);
                }

                if let Some(ref dd) = self.incremental {
                    let _ = dd.delete(relation, tuples.to_vec(), time);
                    let _ = dd.notify_base_update(relation);
                }

                self.store.clear(relation);
                self.rebuild_indexes_for(relation);

                // Update metadata
                let schema = self
                    .metadata
                    .relations
                    .get(relation)
                    .map(|m| m.schema.clone())
                    .unwrap_or_default();
                self.metadata.add_relation(relation.clone(), schema, 0);

                results.push((relation.clone(), count));
            }
        }

        if !results.is_empty() {
            self.publish_snapshot();
        }

        Ok(results)
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

    /// Execute a query with persistent rules on the current snapshot.
    ///
    /// Rules for materialized relations are skipped - their data is already
    /// present in the snapshot as base facts.
    pub fn execute_with_rules(&self, program: &str) -> Result<Vec<(i32, i32)>, String> {
        self.snapshot().execute_with_rules(program)
    }

    /// Execute a query with persistent rules on the current snapshot,
    /// returning tuples of arbitrary arity.
    pub fn execute_with_rules_tuples(&self, program: &str) -> Result<Vec<Tuple>, String> {
        self.snapshot().execute_with_rules_tuples(program)
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

/// Format a Rule as an IQL string (uses Rule's Display impl)
fn format_rule(rule: &crate::ast::Rule) -> String {
    rule.to_string()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tempfile::TempDir;

    #[test]
    fn test_clear_by_prefix_persist_failure_keeps_memory() {
        let temp = TempDir::new().unwrap();
        let persist = FilePersist::new(PersistConfig {
            path: temp.path().join("persist"),
            ..PersistConfig::default()
        })
        .unwrap();
        let mut kg = KnowledgeGraph::new_with_workers("kg".into(), temp.path().join("kg"), 1);
        for rel in ["p_a", "p_b"] {
            kg.insert_in_memory(rel, vec![Tuple::from_pair(1, 2), Tuple::from_pair(3, 4)], 1)
                .unwrap();
        }
        // p_a's shard exists; creating p_b's fails because shards/ is a file.
        persist.ensure_shard("kg:p_a").unwrap();
        let shards = temp.path().join("persist").join("shards");
        fs::remove_dir_all(&shards).unwrap();
        fs::write(&shards, b"").unwrap();

        assert!(kg
            .clear_relations_by_prefix("p_", 2, &persist, "kg")
            .is_err());
        assert!(kg.store.get("p_a").unwrap().is_empty());
        assert_eq!(kg.store.get("p_b").unwrap().len(), 2);
        let snapshot = kg.snapshot.load();
        assert!(snapshot.input_tuples["p_a"].is_empty());
        assert_eq!(snapshot.input_tuples["p_b"].len(), 2);
    }

    #[test]
    fn test_stale_handle_rejected_after_drop() {
        let temp = TempDir::new().unwrap();
        let storage = StorageEngine::new(create_test_config(temp.path().to_path_buf())).unwrap();
        storage.create_knowledge_graph("x").unwrap();
        let stale = storage.kg_handle("x").unwrap();
        storage.drop_knowledge_graph("x").unwrap();
        storage.create_knowledge_graph("x").unwrap();

        assert!(StorageEngine::lock_live(&stale, "x").is_err());
        let live = storage.kg_handle("x").unwrap();
        assert!(StorageEngine::lock_live(&live, "x").is_ok());
    }

    fn relation_tuples(storage: &StorageEngine, kg: &str, rel: &str) -> Option<HashSet<Tuple>> {
        let db = storage.kg_handle(kg).unwrap();
        let db = db.read();
        db.store
            .get(rel)
            .filter(|t| !t.is_empty())
            .map(|t| t.iter().cloned().collect())
    }

    #[test]
    fn test_prepare_drop_without_finish_stays_dropped_after_restart() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config.clone()).unwrap();
        storage.create_knowledge_graph("gone").unwrap();
        storage
            .insert_into("gone", "edge", vec![(1, 2), (3, 4)])
            .unwrap();
        let _cleanup = storage.prepare_drop_knowledge_graph("gone").unwrap();
        drop(storage);

        let storage = StorageEngine::new(config.clone()).unwrap();
        assert_eq!(storage.list_knowledge_graphs(), vec!["default".to_string()]);
        let shards = storage.persist.list_shards().unwrap();
        assert!(!shards.iter().any(|s| s.starts_with("gone:")), "{shards:?}");
        assert!(storage.tombstones.lock().knowledge_graphs.is_empty());

        storage.create_knowledge_graph("gone").unwrap();
        assert_eq!(relation_tuples(&storage, "gone", "edge"), None);
        drop(storage);
        let storage = StorageEngine::new(config).unwrap();
        assert!(storage
            .list_knowledge_graphs()
            .contains(&"gone".to_string()));
    }

    #[test]
    fn test_rel_drop_wal_failure_keeps_relation() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config.clone()).unwrap();
        storage
            .insert_into("default", "doomed", vec![(1, 2)])
            .unwrap();
        storage
            .insert_into("default", "other", vec![(5, 6)])
            .unwrap();
        // The WAL rewrite fails: its temp path is a directory.
        let blocker = temp.path().join("persist/wal/current.wal.new");
        fs::create_dir_all(&blocker).unwrap();

        assert!(storage.drop_relation_in("default", "doomed").is_err());
        assert!(storage
            .list_relations_in("default")
            .unwrap()
            .contains(&"doomed".to_string()));
        assert!(storage.tombstones.lock().relations.is_empty());

        fs::remove_dir(&blocker).unwrap();
        drop(storage);
        let storage = StorageEngine::new(config).unwrap();
        let expected: HashSet<Tuple> = [Tuple::from_pair(1, 2)].into_iter().collect();
        assert_eq!(
            relation_tuples(&storage, "default", "doomed"),
            Some(expected)
        );
    }

    #[test]
    fn test_rel_drop_persists_schema_removal_and_allows_new_arity() {
        use crate::schema::{ColumnSchema, SchemaType};
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config.clone()).unwrap();
        let schema = RelationSchema::new("r")
            .with_column(ColumnSchema::new("a", SchemaType::Int))
            .with_column(ColumnSchema::new("b", SchemaType::Int));
        storage.register_schema_in("default", schema).unwrap();
        storage.insert_into("default", "r", vec![(1, 2)]).unwrap();
        storage.drop_relation_in("default", "r").unwrap();
        assert!(storage.tombstones.lock().relations.is_empty());
        drop(storage);

        let storage = StorageEngine::new(config.clone()).unwrap();
        assert!(!storage.has_schema_in("default", "r").unwrap());
        assert!(!storage
            .list_relations_in("default")
            .unwrap()
            .contains(&"r".to_string()));
        let wide = Tuple::new(vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)]);
        storage
            .insert_tuples_into("default", "r", vec![wide.clone()])
            .unwrap();
        drop(storage);

        let storage = StorageEngine::new(config).unwrap();
        let expected: HashSet<Tuple> = [wide].into_iter().collect();
        assert_eq!(relation_tuples(&storage, "default", "r"), Some(expected));
    }

    #[test]
    fn test_rel_drop_tombstone_finished_on_restart() {
        use crate::schema::{ColumnSchema, SchemaType};
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config.clone()).unwrap();
        let schema = RelationSchema::new("r").with_column(ColumnSchema::new("a", SchemaType::Int));
        storage.register_schema_in("default", schema).unwrap();
        storage
            .insert_tuples_into("default", "r", vec![Tuple::new(vec![Value::Int64(1)])])
            .unwrap();
        // Crash right after the tombstone was saved.
        storage
            .update_tombstones(|t| {
                t.relations.insert(RelationTombstone::new("default", "r"));
            })
            .unwrap();
        drop(storage);

        let storage = StorageEngine::new(config).unwrap();
        assert!(!storage
            .list_relations_in("default")
            .unwrap()
            .contains(&"r".to_string()));
        assert!(!storage.has_schema_in("default", "r").unwrap());
        assert!(storage.tombstones.lock().relations.is_empty());
        assert!(storage.persist.shard_info("default:r").is_err());
    }

    #[test]
    fn test_rel_drop_racing_inserts_match_after_restart() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config.clone()).unwrap();
        storage.insert_into("default", "r", vec![(0, 0)]).unwrap();
        std::thread::scope(|scope| {
            let storage = &storage;
            scope.spawn(move || {
                for i in 1..200 {
                    storage.insert_into("default", "r", vec![(i, i)]).unwrap();
                }
            });
            scope.spawn(move || {
                for _ in 0..20 {
                    let _ = storage.drop_relation_in("default", "r");
                    std::thread::yield_now();
                }
            });
        });
        let before = relation_tuples(&storage, "default", "r");
        drop(storage);

        let storage = StorageEngine::new(config).unwrap();
        assert_eq!(relation_tuples(&storage, "default", "r"), before);
    }

    #[test]
    fn test_rel_drop_retracts_incremental_base_data() {
        let temp = TempDir::new().unwrap();
        let storage = StorageEngine::new(create_test_config(temp.path().to_path_buf())).unwrap();
        storage.insert_into("default", "r", vec![(1, 2)]).unwrap();
        storage
            .with_kg_mut("default", |kg| {
                kg.enable_incremental().map_err(|e| e.to_string())
            })
            .unwrap();
        storage.drop_relation_in("default", "r").unwrap();
        storage.insert_into("default", "r", vec![(3, 4)]).unwrap();

        let db = storage.kg_handle("default").unwrap();
        let db = db.read();
        let dd = db.incremental().unwrap();
        assert_eq!(
            dd.read_relation_consistent("r").unwrap(),
            vec![Tuple::from_pair(3, 4)]
        );
    }

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

        let storage = StorageEngine::new(config).unwrap();

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
        println!("Views: {views:?}");
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
        println!("All connected pairs: {result:?}");

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
        println!("connected(1, 3): {specific_result:?}");
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

        // Enable IncrementalEngine for shadow writes
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_incremental().unwrap();
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

        // Access the KG's IncrementalEngine and verify it received the data
        let kg = storage
            .knowledge_graphs
            .get("default")
            .expect("default KG should exist");
        let kg = kg.read();
        let dd = kg
            .incremental()
            .expect("IncrementalEngine should be enabled");

        // Use consistent read  -  lazily advances time and waits
        let dd_tuples = dd.read_relation_consistent("edge").unwrap();
        assert_eq!(
            dd_tuples.len(),
            3,
            "IncrementalEngine should have received all 3 tuples"
        );
    }

    #[test]
    fn test_dd_shadow_writes_skip_duplicates() {
        use crate::value::Value;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Enable IncrementalEngine
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_incremental().unwrap();
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

        // Verify IncrementalEngine has exactly 3 tuples (not 4 with duplicate)
        let kg = storage.knowledge_graphs.get("default").expect("default KG");
        let kg = kg.read();
        let dd = kg.incremental().unwrap();

        let dd_tuples = dd.read_relation_consistent("data").unwrap();
        assert_eq!(
            dd_tuples.len(),
            3,
            "IncrementalEngine should have 3 unique tuples (duplicates filtered)"
        );
    }

    #[test]
    fn test_dd_shadow_writes_handle_deletes() {
        use crate::value::Value;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Enable IncrementalEngine
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_incremental().unwrap();
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

        // Verify IncrementalEngine reflects the delete
        let kg = storage.knowledge_graphs.get("default").expect("default KG");
        let kg = kg.read();
        let dd = kg.incremental().unwrap();

        let dd_tuples = dd.read_relation_consistent("rel").unwrap();
        assert_eq!(
            dd_tuples.len(),
            2,
            "IncrementalEngine should have 2 tuples after delete"
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

        // Enable IncrementalEngine
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_incremental().unwrap();
        }

        // Insert via binary tuple API
        storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();

        // Verify IncrementalEngine received the data
        let kg = storage.knowledge_graphs.get("default").expect("default KG");
        let kg = kg.read();
        let dd = kg
            .incremental()
            .expect("IncrementalEngine should be enabled");

        let dd_tuples = dd.read_relation_consistent("edge").unwrap();
        assert_eq!(dd_tuples.len(), 2, "IncrementalEngine should have 2 tuples");
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
            kg.enable_incremental().unwrap();
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
        let hashmap_tuples = &kg.store.get("edge").unwrap().to_vec();

        // DD arrangement state
        let dd = kg.incremental().unwrap();
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
            kg.enable_incremental().unwrap();
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
        let hashmap_tuples = &kg.store.get("data").unwrap().to_vec();
        let dd = kg.incremental().unwrap();
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
            kg.enable_incremental().unwrap();
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
        let hashmap_tuples = &kg.store.get("mixed").unwrap().to_vec();
        let dd = kg.incremental().unwrap();
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
            kg.enable_incremental().unwrap();
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
        let dd = kg.incremental().unwrap();

        for rel_name in &["edges", "nodes"] {
            let hashmap_tuples = &kg.store.get(rel_name).unwrap().to_vec();
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
            kg.enable_incremental().unwrap();
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
        let dd = kg.incremental().unwrap();

        assert!(
            dd.max_write_time() >= time_before,
            "DD max_write_time ({}) should be >= logical time at insert ({})",
            dd.max_write_time(),
            time_before
        );
    }

    // WAL Replay into IncrementalEngine Tests
    #[test]
    fn test_dd_replay_existing_data_on_enable() {
        use crate::value::Value;

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Insert data BEFORE enabling IncrementalEngine
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

        // NOW enable IncrementalEngine  -  should replay existing data
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_incremental().unwrap();
        }

        // Verify IncrementalEngine has all pre-existing data
        let kg = storage.knowledge_graphs.get("default").unwrap();
        let kg = kg.read();
        let dd = kg.incremental().unwrap();

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
            kg.enable_incremental().unwrap();
        }

        // Verify exact parity between DD and HashMap
        let kg = storage.knowledge_graphs.get("default").unwrap();
        let kg = kg.read();
        let hashmap_tuples = &kg.store.get("data").unwrap().to_vec();
        let dd = kg.incremental().unwrap();
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
            kg.enable_incremental().unwrap();
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
        let dd = kg.incremental().unwrap();
        let mut dd_tuples = dd.read_relation_consistent("items").unwrap();
        dd_tuples.sort();

        assert_eq!(
            dd_tuples.len(),
            4,
            "DD should have 4 items (2 replayed + 2 new)"
        );

        // Verify parity with HashMap
        let mut hashmap_sorted: Vec<_> = kg.store.get("items").unwrap().to_vec();
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
            kg.enable_incremental().unwrap();
        }

        // Verify DD has the replayed data
        let kg = storage.knowledge_graphs.get("default").unwrap();
        let kg = kg.read();
        let dd = kg.incremental().unwrap();
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

            // Enable IncrementalEngine  -  should replay persisted data
            {
                let kg = storage.knowledge_graphs.get("default").unwrap();
                let mut kg = kg.write();
                kg.enable_incremental().unwrap();
            }

            // Verify DD has the persisted data
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let kg = kg.read();
            let dd = kg.incremental().unwrap();
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

        // Enable IncrementalEngine
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_incremental().unwrap();
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

        // Enable IncrementalEngine
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_incremental().unwrap();
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

        // Enable IncrementalEngine
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_incremental().unwrap();
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

        // Enable IncrementalEngine
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let mut kg = kg.write();
            kg.enable_incremental().unwrap();
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
            let dd = kg.incremental().unwrap();
            let (total, materialized, invalid) = dd.get_derived_stats().unwrap();
            assert_eq!(total, 2, "should have 2 rules");
            assert_eq!(materialized, 0, "nothing materialized yet");
            assert_eq!(invalid, 2, "both should be invalid (not materialized)");
        }

        // Materialize one
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let kg = kg.read();
            let dd = kg.incremental().unwrap();
            dd.set_materialized("derived1", vec![]).unwrap();
        }

        // Check stats - 2 rules, 1 materialized, 1 invalid
        {
            let kg = storage.knowledge_graphs.get("default").unwrap();
            let kg = kg.read();
            let dd = kg.incremental().unwrap();
            let (total, materialized, invalid) = dd.get_derived_stats().unwrap();
            assert_eq!(total, 2);
            assert_eq!(materialized, 1);
            assert_eq!(invalid, 1);
        }
    }

    // =========================================================================
    // Storage Engine API Coverage Tests
    // =========================================================================

    #[test]
    fn test_create_duplicate_knowledge_graph_error() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("dup_test").unwrap();
        let result = storage.create_knowledge_graph("dup_test");
        assert!(matches!(result, Err(StorageError::KnowledgeGraphExists(_))));
    }

    #[test]
    fn test_create_knowledge_graph_invalid_name() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        // Empty name
        assert!(storage.create_knowledge_graph("").is_err());
        // Slash in name
        assert!(storage.create_knowledge_graph("a/b").is_err());
        // Backslash in name
        assert!(storage.create_knowledge_graph("a\\b").is_err());
        // Path traversal with ..
        assert!(storage.create_knowledge_graph("..").is_err());
        assert!(storage.create_knowledge_graph("foo/..").is_err());
        assert!(storage.create_knowledge_graph("a..b").is_err());
        // Current directory
        assert!(storage.create_knowledge_graph(".").is_err());
        // Null byte
        assert!(storage.create_knowledge_graph("a\0b").is_err());
    }

    #[test]
    fn test_drop_nonexistent_knowledge_graph() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        let result = storage.drop_knowledge_graph("nonexistent");
        assert!(matches!(
            result,
            Err(StorageError::KnowledgeGraphNotFound(_))
        ));
    }

    #[test]
    fn test_drop_knowledge_graph_success() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("to_drop").unwrap();
        assert!(storage
            .list_knowledge_graphs()
            .contains(&"to_drop".to_string()));

        // Drop it (not the current kg)
        storage.drop_knowledge_graph("to_drop").unwrap();
        assert!(!storage
            .list_knowledge_graphs()
            .contains(&"to_drop".to_string()));
    }

    #[test]
    fn test_drop_knowledge_graph_cleans_persist_shards() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config.clone()).unwrap();

        // Create a KG and insert some data to create persist shards
        storage.create_knowledge_graph("shard_test").unwrap();
        storage
            .insert_tuples_into(
                "shard_test",
                "rel1",
                vec![Tuple::new(vec![Value::Int64(1), Value::Int64(2)])],
            )
            .unwrap();

        // Verify shard exists
        let shards = storage.persist.list_shards().unwrap();
        assert!(
            shards.iter().any(|s| s.starts_with("shard_test:")),
            "Expected persist shard for shard_test, got: {shards:?}"
        );

        // Drop the KG
        storage.drop_knowledge_graph("shard_test").unwrap();

        // Verify persist shards are cleaned up
        let shards_after = storage.persist.list_shards().unwrap();
        assert!(
            !shards_after.iter().any(|s| s.starts_with("shard_test:")),
            "Persist shards should be cleaned up after drop, got: {shards_after:?}"
        );

        // Verify KG doesn't resurrect on restart
        drop(storage);
        let storage2 = StorageEngine::new(config).unwrap();
        assert!(
            !storage2
                .list_knowledge_graphs()
                .contains(&"shard_test".to_string()),
            "Dropped KG should not resurrect on restart"
        );
    }

    #[test]
    fn test_use_nonexistent_knowledge_graph_no_auto_create() {
        let temp = TempDir::new().unwrap();
        let mut config = create_test_config(temp.path().to_path_buf());
        config.storage.auto_create_knowledge_graphs = false;
        let mut storage = StorageEngine::new(config).unwrap();

        let result = storage.use_knowledge_graph("nonexistent");
        assert!(matches!(
            result,
            Err(StorageError::KnowledgeGraphNotFound(_))
        ));
    }

    #[test]
    fn test_use_knowledge_graph_auto_create() {
        let temp = TempDir::new().unwrap();
        let mut config = create_test_config(temp.path().to_path_buf());
        config.storage.auto_create_knowledge_graphs = true;
        let mut storage = StorageEngine::new(config).unwrap();

        // Auto-create should succeed
        storage.use_knowledge_graph("auto_created").unwrap();
        assert_eq!(storage.current_knowledge_graph(), Some("auto_created"));
        assert!(storage
            .list_knowledge_graphs()
            .contains(&"auto_created".to_string()));
    }

    #[test]
    fn test_insert_empty_tuples() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        let (new_count, dup_count) = storage.insert_tuples("empty_rel", vec![]).unwrap();
        assert_eq!(new_count, 0);
        assert_eq!(dup_count, 0);
    }

    #[test]
    fn test_insert_arity_mismatch_in_batch() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        let tuples = vec![
            Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
            Tuple::new(vec![Value::Int32(3)]), // Different arity
        ];
        let result = storage.insert_tuples("rel", tuples);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Arity mismatch"));
    }

    #[test]
    fn test_insert_into_view_error() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Register a rule to make "path" a derived relation
        let rule_def = make_path_rule_def();
        storage.register_rule(&rule_def).unwrap();

        // Try to insert into the view
        let result = storage.insert_tuples(
            "path",
            vec![Tuple::new(vec![Value::Int32(1), Value::Int32(2)])],
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("derived relation"));
    }

    #[test]
    fn test_insert_no_current_kg() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.current_kg = None;

        let result = storage.insert_tuples("rel", vec![Tuple::new(vec![Value::Int32(1)])]);
        assert!(matches!(result, Err(StorageError::NoCurrentKnowledgeGraph)));
    }

    #[test]
    fn test_delete_empty_tuples() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        let count = storage
            .delete_tuples_from("default", "rel", vec![])
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_execute_query_no_current_kg() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.current_kg = None;

        let result = storage.execute_query("result(X,Y) <- edge(X,Y)");
        assert!(matches!(result, Err(StorageError::NoCurrentKnowledgeGraph)));
    }

    #[test]
    fn test_execute_query_on_nonexistent_kg() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        let result = storage.execute_query_on("nonexistent", "result(X,Y) <- edge(X,Y)");
        assert!(matches!(
            result,
            Err(StorageError::KnowledgeGraphNotFound(_))
        ));
    }

    #[test]
    fn test_list_relations_empty() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        let relations = storage.list_relations().unwrap();
        assert!(relations.is_empty());
    }

    #[test]
    fn test_list_relations_after_insert() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        storage.insert("edge", vec![(1, 2)]).unwrap();
        storage.insert("node", vec![(1, 1)]).unwrap();

        let relations = storage.list_relations().unwrap();
        assert!(relations.contains(&"edge".to_string()));
        assert!(relations.contains(&"node".to_string()));
    }

    #[test]
    fn test_describe_relation_exists() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        storage.insert("edge", vec![(1, 2), (3, 4)]).unwrap();

        let desc = storage.describe_relation("edge").unwrap();
        assert!(desc.is_some());
        let desc = desc.unwrap();
        assert!(desc.contains("edge"));
        assert!(desc.contains('2')); // tuple count
    }

    #[test]
    fn test_describe_relation_nonexistent() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        let desc = storage.describe_relation("nonexistent").unwrap();
        assert!(desc.is_none());
    }

    #[test]
    fn test_get_relation_metadata() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        storage
            .insert("edge", vec![(1, 2), (3, 4), (5, 6)])
            .unwrap();

        let meta = storage.get_relation_metadata("edge").unwrap();
        assert!(meta.is_some());
        let (schema, count) = meta.unwrap();
        assert_eq!(schema.len(), 2); // binary relation
        assert_eq!(count, 3);
    }

    #[test]
    fn test_get_relation_metadata_nonexistent() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        let meta = storage.get_relation_metadata("nonexistent").unwrap();
        assert!(meta.is_none());
    }

    #[test]
    fn test_list_relations_with_metadata() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        storage.insert("alpha", vec![(1, 2)]).unwrap();
        storage.insert("beta", vec![(3, 4), (5, 6)]).unwrap();

        let rels = storage.list_relations_with_metadata("default").unwrap();
        assert_eq!(rels.len(), 2);
    }

    #[test]
    fn test_rule_management_lifecycle() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        // Register
        let rule_def = make_simple_rule_def("derived1", "base");
        storage.register_rule(&rule_def).unwrap();

        // List
        let rules = storage.list_rules().unwrap();
        assert!(rules.contains(&"derived1".to_string()));

        // Describe
        let desc = storage.describe_rule("derived1").unwrap();
        assert!(desc.is_some());

        // Count
        let count = storage.rule_count("derived1").unwrap();
        assert_eq!(count, Some(1));

        // Arity
        let arity = storage.rule_arity("derived1").unwrap();
        assert_eq!(arity, Some(1));

        // Drop
        storage.drop_rule("derived1").unwrap();
        let rules = storage.list_rules().unwrap();
        assert!(!rules.contains(&"derived1".to_string()));
    }

    #[test]
    fn test_save_and_compact() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        storage.insert("data", vec![(1, 2), (3, 4)]).unwrap();

        // Save should not error
        storage.save_knowledge_graph("default").unwrap();
        storage.save_all().unwrap();

        // Compact should not error
        storage.compact_all().unwrap();
    }

    #[test]
    fn test_save_nonexistent_kg() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        let result = storage.save_knowledge_graph("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_ensure_knowledge_graph_existing() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        // Default exists, should be no-op
        storage.ensure_knowledge_graph("default").unwrap();
    }

    #[test]
    fn test_ensure_knowledge_graph_no_auto_create() {
        let temp = TempDir::new().unwrap();
        let mut config = create_test_config(temp.path().to_path_buf());
        config.storage.auto_create_knowledge_graphs = false;
        let storage = StorageEngine::new(config).unwrap();

        let result = storage.ensure_knowledge_graph("nonexistent");
        assert!(matches!(
            result,
            Err(StorageError::KnowledgeGraphNotFound(_))
        ));
    }

    #[test]
    fn test_execute_query_tuples() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        storage
            .insert_tuples(
                "data",
                vec![
                    Tuple::new(vec![Value::Int32(1), Value::string("hello")]),
                    Tuple::new(vec![Value::Int32(2), Value::string("world")]),
                ],
            )
            .unwrap();

        let results = storage
            .execute_query_tuples("result(X, Y) <- data(X, Y)")
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_insert_and_query_binary_tuples() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();

        let results = storage.execute_query("result(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&(1, 2)));
        assert!(results.contains(&(2, 3)));
    }

    #[test]
    fn test_delete_binary_tuples() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        storage
            .insert("edge", vec![(1, 2), (2, 3), (3, 4)])
            .unwrap();
        let deleted = storage.delete("edge", vec![(2, 3)]).unwrap();
        assert_eq!(deleted, 1);

        let results = storage.execute_query("result(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&(1, 2)));
        assert!(results.contains(&(3, 4)));
    }

    #[test]
    fn test_insert_into_specific_kg() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("test_kg").unwrap();
        storage
            .insert_into("test_kg", "data", vec![(10, 20)])
            .unwrap();

        let results = storage
            .execute_query_on("test_kg", "result(X, Y) <- data(X, Y)")
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (10, 20));
    }

    #[test]
    fn test_schema_register_and_get() {
        use crate::schema::{ColumnSchema, SchemaType};
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        let schema = RelationSchema::new("typed_rel")
            .with_column(ColumnSchema::new("name", SchemaType::String))
            .with_column(ColumnSchema::new("age", SchemaType::Int));
        storage.register_schema(schema).unwrap();

        assert!(storage.has_schema("typed_rel").unwrap());
        let retrieved = storage.get_schema("typed_rel").unwrap();
        assert!(retrieved.is_some());

        let schemas = storage.list_schemas().unwrap();
        assert!(schemas.contains(&"typed_rel".to_string()));
    }

    #[test]
    fn test_schema_remove() {
        use crate::schema::{ColumnSchema, SchemaType};
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        let schema =
            RelationSchema::new("to_remove").with_column(ColumnSchema::new("x", SchemaType::Int));
        storage.register_schema(schema).unwrap();
        assert!(storage.has_schema("to_remove").unwrap());

        storage.remove_schema("to_remove").unwrap();
        assert!(!storage.has_schema("to_remove").unwrap());
    }

    #[test]
    fn test_remove_rule_clause_from_storage() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        let rule_def = make_simple_rule_def("test_view", "base");
        storage.register_rule(&rule_def).unwrap();

        // Remove the only clause (0-based index) → should delete the rule
        let deleted = storage.remove_rule_clause("test_view", 0).unwrap();
        assert!(deleted, "Should have deleted the entire rule");
        assert!(storage.list_rules().unwrap().is_empty());
    }

    // =========================================================================
    // Additional Storage Engine Coverage Tests
    // =========================================================================

    #[test]
    fn test_current_knowledge_graph_after_use() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();
        assert_eq!(storage.current_knowledge_graph(), Some("default"));
    }

    #[test]
    fn test_list_knowledge_graphs() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("kg1").unwrap();
        storage.create_knowledge_graph("kg2").unwrap();

        let kgs = storage.list_knowledge_graphs();
        assert!(kgs.contains(&"kg1".to_string()));
        assert!(kgs.contains(&"kg2".to_string()));
    }

    #[test]
    fn test_insert_and_delete_binary() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        let (inserted, _) = storage
            .insert("pairs", vec![(1, 2), (3, 4), (5, 6)])
            .unwrap();
        assert_eq!(inserted, 3);

        let deleted = storage.delete("pairs", vec![(1, 2)]).unwrap();
        assert_eq!(deleted, 1);
    }

    #[test]
    fn test_delete_tuple_single() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        storage
            .insert_tuples(
                "items",
                vec![
                    Tuple::new(vec![Value::Int64(1)]),
                    Tuple::new(vec![Value::Int64(2)]),
                ],
            )
            .unwrap();

        let deleted = storage
            .delete_tuple("items", &Tuple::new(vec![Value::Int64(1)]))
            .unwrap();
        assert_eq!(deleted, 1);
    }

    #[test]
    fn test_execute_query_binary() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();

        storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();
        let results = storage.execute_query("result(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_execute_query_on_specific_kg() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("target_kg").unwrap();
        storage
            .insert_into("target_kg", "data", vec![(10, 20)])
            .unwrap();

        let results = storage
            .execute_query_on("target_kg", "result(X, Y) <- data(X, Y)")
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_insert_tuples_into_cross_kg() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("xkg").unwrap();
        storage
            .insert_tuples_into(
                "xkg",
                "nodes",
                vec![
                    Tuple::new(vec![Value::Int64(1)]),
                    Tuple::new(vec![Value::Int64(2)]),
                ],
            )
            .unwrap();

        let results = storage
            .execute_query_tuples_on("xkg", "result(X) <- nodes(X)")
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_describe_relation_in() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("desc_kg").unwrap();
        storage
            .insert_into("desc_kg", "info", vec![(1, 2)])
            .unwrap();

        let desc = storage.describe_relation_in("desc_kg", "info").unwrap();
        assert!(desc.is_some());
    }

    #[test]
    fn test_describe_relation_nonexistent_in_kg() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("empty_kg").unwrap();
        let desc = storage.describe_relation_in("empty_kg", "missing").unwrap();
        assert!(desc.is_none());
    }

    #[test]
    fn test_save_all() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("s1").unwrap();
        storage.create_knowledge_graph("s2").unwrap();

        // save_all should not fail even with empty KGs
        storage.save_all().unwrap();
    }

    #[test]
    fn test_compact_all() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("c1").unwrap();
        storage.compact_all().unwrap();
    }

    #[test]
    fn test_num_cpus() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();
        assert!(storage.num_cpus() >= 1);
    }

    #[test]
    fn test_config_accessor() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();
        let cfg = storage.config();
        assert!(!cfg.storage.data_dir.as_os_str().is_empty());
    }

    #[test]
    fn test_rule_management_cross_kg() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("rule_kg").unwrap();

        let rule_def = make_simple_rule_def("view_r", "base_r");
        storage.register_rule_in("rule_kg", &rule_def).unwrap();

        let rules = storage.list_rules_in("rule_kg").unwrap();
        assert!(rules.contains(&"view_r".to_string()));

        let desc = storage.describe_rule_in("rule_kg", "view_r").unwrap();
        assert!(desc.is_some());

        let count = storage.rule_count_in("rule_kg", "view_r").unwrap();
        assert_eq!(count, Some(1));

        storage.drop_rule_in("rule_kg", "view_r").unwrap();
        let rules_after = storage.list_rules_in("rule_kg").unwrap();
        assert!(rules_after.is_empty());
    }

    #[test]
    fn test_schema_management_cross_kg() {
        use crate::schema::{ColumnSchema, SchemaType};
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("schema_kg").unwrap();

        let schema =
            RelationSchema::new("my_rel").with_column(ColumnSchema::new("id", SchemaType::Int));
        storage.register_schema_in("schema_kg", schema).unwrap();

        assert!(storage.has_schema_in("schema_kg", "my_rel").unwrap());
        let retrieved = storage.get_schema_in("schema_kg", "my_rel").unwrap();
        assert!(retrieved.is_some());

        let schemas = storage.list_schemas_in("schema_kg").unwrap();
        assert!(schemas.contains(&"my_rel".to_string()));

        storage.remove_schema_in("schema_kg", "my_rel").unwrap();
        assert!(!storage.has_schema_in("schema_kg", "my_rel").unwrap());
    }

    #[test]
    fn test_delete_from_cross_kg() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("del_kg").unwrap();
        storage
            .insert_into("del_kg", "data", vec![(1, 2), (3, 4)])
            .unwrap();

        let deleted = storage.delete_from("del_kg", "data", vec![(1, 2)]).unwrap();
        assert_eq!(deleted, 1);
    }

    #[test]
    fn test_debug_query() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("debug_kg").unwrap();
        storage
            .insert_into("debug_kg", "edge", vec![(1, 2)])
            .unwrap();

        let trace = storage
            .debug_query_on("debug_kg", "result(X, Y) <- edge(X, Y)")
            .unwrap();
        assert!(trace.ast.is_some());
    }

    #[test]
    fn test_create_knowledge_graph_duplicate() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("dup_kg").unwrap();
        let result = storage.create_knowledge_graph("dup_kg");
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_knowledge_graph() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("drop_kg").unwrap();
        storage.drop_knowledge_graph("drop_kg").unwrap();
        let kgs = storage.list_knowledge_graphs();
        assert!(!kgs.contains(&"drop_kg".to_string()));
    }

    #[test]
    fn test_drop_nonexistent_kg() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        let result = storage.drop_knowledge_graph("nope_kg");
        assert!(result.is_err());
    }

    #[test]
    fn test_use_knowledge_graph_nonexistent() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        let result = storage.use_knowledge_graph("no_such_kg");
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_and_query_strings() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("str_kg").unwrap();
        storage
            .insert_tuples_into(
                "str_kg",
                "names",
                vec![
                    Tuple::new(vec![Value::String("alice".into()), Value::Int32(30)]),
                    Tuple::new(vec![Value::String("bob".into()), Value::Int32(25)]),
                ],
            )
            .unwrap();

        let result = storage
            .execute_query_tuples_on("str_kg", "result(N, A) <- names(N, A)")
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_list_relations_in() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("rel_kg").unwrap();
        storage.insert_into("rel_kg", "edge", vec![(1, 2)]).unwrap();
        storage.insert_into("rel_kg", "node", vec![(1, 0)]).unwrap();

        let relations = storage.list_relations_in("rel_kg").unwrap();
        assert!(relations.contains(&"edge".to_string()));
        assert!(relations.contains(&"node".to_string()));
    }

    #[test]
    fn test_query_with_filter() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("filt_kg").unwrap();
        storage
            .insert_into("filt_kg", "scores", vec![(1, 90), (2, 50), (3, 80)])
            .unwrap();

        let result = storage
            .execute_query_on("filt_kg", "result(Id, S) <- scores(Id, S), S > 60")
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_multiple_kg_isolation() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("kg_a").unwrap();
        storage.create_knowledge_graph("kg_b").unwrap();

        storage.insert_into("kg_a", "data", vec![(1, 2)]).unwrap();
        storage
            .insert_into("kg_b", "data", vec![(3, 4), (5, 6)])
            .unwrap();

        let result_a = storage
            .execute_query_on("kg_a", "result(X, Y) <- data(X, Y)")
            .unwrap();
        let result_b = storage
            .execute_query_on("kg_b", "result(X, Y) <- data(X, Y)")
            .unwrap();
        assert_eq!(result_a.len(), 1);
        assert_eq!(result_b.len(), 2);
    }

    #[test]
    fn test_describe_relation_format() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("desc_kg").unwrap();
        storage
            .insert_into("desc_kg", "edge", vec![(1, 2), (3, 4)])
            .unwrap();

        let desc = storage.describe_relation_in("desc_kg", "edge").unwrap();
        assert!(desc.is_some());
    }

    #[test]
    fn test_rule_arity_in() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("arity_kg").unwrap();
        let rule_def = make_simple_rule_def("path", "edge");
        storage.register_rule_in("arity_kg", &rule_def).unwrap();

        let arity = storage.rule_arity_in("arity_kg", "path").unwrap();
        assert!(arity.is_some());
    }

    #[test]
    fn test_get_relation_metadata_in() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("meta_kg").unwrap();
        storage
            .insert_into("meta_kg", "edge", vec![(1, 2), (3, 4)])
            .unwrap();

        let meta = storage.get_relation_metadata_in("meta_kg", "edge").unwrap();
        assert!(meta.is_some());
        let (_, count) = meta.unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_execute_query_tuples_on() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("tuples_kg").unwrap();
        storage
            .insert_into("tuples_kg", "data", vec![(10, 20)])
            .unwrap();

        let results = storage
            .execute_query_tuples_on("tuples_kg", "result(X, Y) <- data(X, Y)")
            .unwrap();
        assert_eq!(results.len(), 1);
        // insert_into uses (i32, i32) but query pipeline produces Int64
        assert_eq!(results[0].get(0), Some(&Value::Int64(10)));
    }

    #[test]
    fn test_insert_tuples_into() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("tup_kg").unwrap();
        let tuples = vec![
            Tuple::new(vec![Value::Int64(1), Value::string("a")]),
            Tuple::new(vec![Value::Int64(2), Value::string("b")]),
        ];
        let (new, dup) = storage
            .insert_tuples_into("tup_kg", "mixed", tuples)
            .unwrap();
        assert_eq!(new, 2);
        assert_eq!(dup, 0);
    }

    #[test]
    fn test_insert_tuples_empty() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("emp_tup_kg").unwrap();
        let (new, dup) = storage
            .insert_tuples_into("emp_tup_kg", "rel", vec![])
            .unwrap();
        assert_eq!(new, 0);
        assert_eq!(dup, 0);
    }

    #[test]
    fn test_delete_tuples_from() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("del_tup_kg").unwrap();
        let tuples = vec![
            Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
            Tuple::new(vec![Value::Int32(3), Value::Int32(4)]),
        ];
        storage
            .insert_tuples_into("del_tup_kg", "data", tuples.clone())
            .unwrap();

        let deleted = storage
            .delete_tuples_from("del_tup_kg", "data", vec![tuples[0].clone()])
            .unwrap();
        assert_eq!(deleted, 1);
    }

    #[test]
    fn test_delete_tuples_empty() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("del_emp_kg").unwrap();
        let deleted = storage
            .delete_tuples_from("del_emp_kg", "data", vec![])
            .unwrap();
        assert_eq!(deleted, 0);
    }

    #[test]
    fn test_ensure_knowledge_graph() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("ensure_kg").unwrap();
        // Should succeed (already exists)
        assert!(storage.ensure_knowledge_graph("ensure_kg").is_ok());
        // Should fail (doesn't exist)
        assert!(storage.ensure_knowledge_graph("missing_kg").is_err());
    }

    #[test]
    fn test_debug_query_on() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("debug_kg").unwrap();
        storage
            .insert_into("debug_kg", "edge", vec![(1, 2)])
            .unwrap();

        let trace = storage
            .debug_query_on("debug_kg", "result(X, Y) <- edge(X, Y)")
            .unwrap();
        assert!(trace.ast.is_some());
    }

    #[test]
    fn test_list_relations_with_metadata_counts() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("meta_count_kg").unwrap();
        storage
            .insert_into("meta_count_kg", "alpha", vec![(1, 2)])
            .unwrap();
        storage
            .insert_into("meta_count_kg", "beta", vec![(3, 4), (5, 6)])
            .unwrap();

        let meta = storage
            .list_relations_with_metadata("meta_count_kg")
            .unwrap();
        assert_eq!(meta.len(), 2);
        // One relation has 1 tuple, the other has 2
        let total: usize = meta.iter().map(|(_, _, count)| count).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn test_drop_rule_in() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("drop_rule_kg").unwrap();
        let rule_def = make_simple_rule_def("path", "edge");
        storage.register_rule_in("drop_rule_kg", &rule_def).unwrap();
        assert!(storage
            .list_rules_in("drop_rule_kg")
            .unwrap()
            .contains(&"path".to_string()));

        storage.drop_rule_in("drop_rule_kg", "path").unwrap();
        assert!(!storage
            .list_rules_in("drop_rule_kg")
            .unwrap()
            .contains(&"path".to_string()));
    }

    #[test]
    fn test_describe_rule_in() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("desc_rule_kg").unwrap();
        let rule_def = make_simple_rule_def("path", "edge");
        storage.register_rule_in("desc_rule_kg", &rule_def).unwrap();

        let desc = storage.describe_rule_in("desc_rule_kg", "path").unwrap();
        assert!(desc.is_some());
    }

    #[test]
    fn test_rule_count_in() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("cnt_rule_kg").unwrap();
        let rule_def = make_simple_rule_def("path", "edge");
        storage.register_rule_in("cnt_rule_kg", &rule_def).unwrap();

        let count = storage.rule_count_in("cnt_rule_kg", "path").unwrap();
        assert!(count.is_some());
        assert!(count.unwrap() >= 1);
    }

    #[test]
    fn test_validate_tuples_in_no_schema() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("val_tup_kg").unwrap();
        let tuples = vec![Tuple::new(vec![Value::Int32(1)])];
        // No schema → validation should pass
        let result = storage.validate_tuples_in("val_tup_kg", "noschema", &tuples);
        assert!(result.is_ok());
    }

    #[test]
    fn test_execute_with_rules_on() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("ewr_kg").unwrap();
        storage
            .insert_into("ewr_kg", "edge", vec![(1, 2), (2, 3)])
            .unwrap();

        let rule_def = make_simple_rule_def("path", "edge");
        storage.register_rule_in("ewr_kg", &rule_def).unwrap();

        let results = storage
            .execute_query_with_rules_tuples_on("ewr_kg", "result(X, Y) <- path(X, Y)")
            .unwrap();
        assert!(!results.is_empty());
    }

    #[test]
    fn test_set_num_threads_already_initialized() {
        // Rayon global pool is already initialized in test context.
        // set_num_threads should still return Ok (silently succeeds).
        let result = StorageEngine::set_num_threads(4);
        // In test context, rayon pool is already built by other tests
        // so this may succeed silently or error depending on error message format
        // Either way, it should not panic
        let _ = result;
    }

    // === Batch 26: Untested public method coverage ===

    #[test]
    fn test_clear_rule_in() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("clear_rule_kg").unwrap();
        let rule_def = make_simple_rule_def("reach", "edge");
        storage
            .register_rule_in("clear_rule_kg", &rule_def)
            .unwrap();
        assert_eq!(
            storage.rule_count_in("clear_rule_kg", "reach").unwrap(),
            Some(1)
        );

        storage.clear_rule_in("clear_rule_kg", "reach").unwrap();
        // After clear, the rule count should be 0 (no clauses)
        let count = storage.rule_count_in("clear_rule_kg", "reach").unwrap();
        assert!(count.is_none() || count == Some(0));
    }

    #[test]
    fn test_execute_query_with_rules_binary_on() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("qwrb_kg").unwrap();
        storage
            .insert_into("qwrb_kg", "edge", vec![(1, 2), (2, 3)])
            .unwrap();

        let rule_def = RuleDef {
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
        };
        storage.register_rule_in("qwrb_kg", &rule_def).unwrap();

        let results = storage
            .execute_query_with_rules_on("qwrb_kg", "result(X, Y) <- path(X, Y)")
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_execute_query_with_session_facts_on() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("sessf_kg").unwrap();
        storage
            .insert_into("sessf_kg", "edge", vec![(1, 2), (2, 3)])
            .unwrap();

        // from_pair stores as Int64, so session facts must also use Int64
        let session_facts = vec![("filter".to_string(), Tuple::new(vec![Value::Int64(1)]))];

        let results = storage
            .execute_query_with_session_facts_on(
                "sessf_kg",
                "result(X, Y) <- edge(X, Y), filter(X)",
                session_facts,
            )
            .unwrap();
        // Only edge(1, 2) matches because only 1 is in filter
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_register_or_update_schema_in() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("schema_kg").unwrap();

        let schema = RelationSchema::new("users");
        let result = storage.register_or_update_schema_in("schema_kg", schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_register_or_update_session_schema_in() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("ssschema_kg").unwrap();

        let schema = RelationSchema::new("temp_data");
        let result = storage.register_or_update_session_schema_in("ssschema_kg", schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_execute_query_with_rules_on_nonexistent_kg() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        let result = storage.execute_query_with_rules_on("no_such_kg", "result(X) <- a(X)");
        assert!(result.is_err());
    }

    #[test]
    fn test_execute_query_with_session_facts_nonexistent_kg() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        let result = storage.execute_query_with_session_facts_on(
            "no_kg_at_all",
            "result(X) <- a(X)",
            vec![],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_clear_relations_by_prefix_basic() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();
        storage.create_knowledge_graph("clear_pfx").unwrap();

        // Insert data into relations with and without prefix
        storage
            .insert_tuples_into(
                "clear_pfx",
                "env_a",
                vec![Tuple::new(vec![Value::Int32(1)])],
            )
            .unwrap();
        storage
            .insert_tuples_into(
                "clear_pfx",
                "env_b",
                vec![Tuple::new(vec![Value::Int32(2)])],
            )
            .unwrap();
        storage
            .insert_tuples_into("clear_pfx", "keep", vec![Tuple::new(vec![Value::Int32(3)])])
            .unwrap();

        let results = storage
            .clear_relations_by_prefix_in("clear_pfx", "env_")
            .unwrap();

        // Should have cleared 2 relations
        assert_eq!(results.len(), 2);
        let total: usize = results.iter().map(|(_, c)| c).sum();
        assert_eq!(total, 2);

        // "keep" should still have its data
        let keep = storage
            .execute_query_tuples_on("clear_pfx", "result(X) <- keep(X)")
            .unwrap();
        assert_eq!(keep.len(), 1);

        // env_a should be empty
        let env_a = storage
            .execute_query_tuples_on("clear_pfx", "result(X) <- env_a(X)")
            .unwrap();
        assert!(env_a.is_empty());
    }

    #[test]
    fn test_clear_relations_by_prefix_no_match() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();
        storage.create_knowledge_graph("clear_none").unwrap();

        storage
            .insert_tuples_into(
                "clear_none",
                "alpha",
                vec![Tuple::new(vec![Value::Int32(1)])],
            )
            .unwrap();

        let results = storage
            .clear_relations_by_prefix_in("clear_none", "zzz_")
            .unwrap();

        assert!(results.is_empty());

        // Original data untouched
        let data = storage
            .execute_query_tuples_on("clear_none", "result(X) <- alpha(X)")
            .unwrap();
        assert_eq!(data.len(), 1);
    }

    #[test]
    fn test_clear_relations_by_prefix_kg_not_found() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        let result = storage.clear_relations_by_prefix_in("no_such_kg", "env_");
        assert!(result.is_err());
    }

    // === Regression tests for production readiness fixes ===

    /// Regression: max_knowledge_graphs limit prevents unbounded KG creation (DoS).
    #[test]
    fn test_max_knowledge_graphs_limit_enforced() {
        let temp = TempDir::new().unwrap();
        let mut config = create_test_config(temp.path().to_path_buf());
        config.storage.max_knowledge_graphs = 3;
        let storage = StorageEngine::new(config).unwrap();

        // Default KG exists, so we can create 2 more
        storage.create_knowledge_graph("kg1").unwrap();
        storage.create_knowledge_graph("kg2").unwrap();

        // Third should fail (default + kg1 + kg2 = 3)
        let result = storage.create_knowledge_graph("kg3");
        assert!(result.is_err(), "Should reject KG creation at limit");
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("Maximum number of knowledge graphs"),
            "Error should mention limit: {err}"
        );
    }

    /// Regression: max_knowledge_graphs=0 means unlimited.
    #[test]
    fn test_max_knowledge_graphs_zero_means_unlimited() {
        let temp = TempDir::new().unwrap();
        let mut config = create_test_config(temp.path().to_path_buf());
        config.storage.max_knowledge_graphs = 0;
        let storage = StorageEngine::new(config).unwrap();

        // Should be able to create many KGs
        for i in 0..20 {
            storage
                .create_knowledge_graph(&format!("unlimited_kg_{i}"))
                .unwrap();
        }
    }

    /// Regression: after dropping a KG, creation should succeed again.
    #[test]
    fn test_max_knowledge_graphs_drop_frees_slot() {
        let temp = TempDir::new().unwrap();
        let mut config = create_test_config(temp.path().to_path_buf());
        config.storage.max_knowledge_graphs = 2;
        let storage = StorageEngine::new(config).unwrap();

        // default + kg1 = 2 (at limit)
        storage.create_knowledge_graph("kg1").unwrap();
        assert!(storage.create_knowledge_graph("kg2").is_err());

        // Drop kg1, should free a slot
        storage.drop_knowledge_graph("kg1").unwrap();
        storage
            .create_knowledge_graph("kg2")
            .expect("Should succeed after dropping a KG");
    }

    /// Regression: max_result_rows propagates through Config → StorageEngine → KG → Snapshot → Engine.
    /// Verifies the entire pipeline caps results at the configured limit.
    #[test]
    fn test_max_result_rows_end_to_end() {
        let temp = TempDir::new().unwrap();
        let mut config = create_test_config(temp.path().to_path_buf());
        config.storage.performance.max_result_rows = 3;
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("limit_kg").unwrap();
        storage
            .insert_into(
                "limit_kg",
                "data",
                vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)],
            )
            .unwrap();

        let result = storage
            .execute_query_tuples_on("limit_kg", "result(X, Y) <- data(X, Y)")
            .unwrap();
        assert!(
            result.len() <= 3,
            "max_result_rows=3 should cap output at 3, got {}",
            result.len()
        );
    }

    /// Regression: max_result_rows=0 means unlimited (no cap).
    #[test]
    fn test_max_result_rows_zero_means_unlimited() {
        let temp = TempDir::new().unwrap();
        let mut config = create_test_config(temp.path().to_path_buf());
        config.storage.performance.max_result_rows = 0;
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("unlim_kg").unwrap();
        storage
            .insert_into(
                "unlim_kg",
                "data",
                vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)],
            )
            .unwrap();

        let result = storage
            .execute_query_tuples_on("unlim_kg", "result(X, Y) <- data(X, Y)")
            .unwrap();
        assert_eq!(result.len(), 5, "max_result_rows=0 should return all rows");
    }

    // === Query cost scoring tests (#47) ===

    #[test]
    fn test_max_query_cost_rejects_expensive_query() {
        let temp = TempDir::new().unwrap();
        let mut config = create_test_config(temp.path().to_path_buf());
        config.storage.performance.max_query_cost = 5; // Very low threshold
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("cost_kg").unwrap();
        storage
            .insert_into("cost_kg", "edge", vec![(1, 2), (2, 3)])
            .unwrap();

        // A simple query should cost 10+ (scan alone costs 10), so it will be rejected
        let result = storage.execute_query_tuples_on("cost_kg", "result(X, Y) <- edge(X, Y)");
        assert!(
            result.is_err(),
            "Query should be rejected when cost exceeds threshold"
        );
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("Query too complex"),
            "Error should mention complexity: {err}"
        );
    }

    #[test]
    fn test_max_query_cost_zero_means_unlimited() {
        let temp = TempDir::new().unwrap();
        let mut config = create_test_config(temp.path().to_path_buf());
        config.storage.performance.max_query_cost = 0; // Unlimited
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("nocost_kg").unwrap();
        storage.insert_into("nocost_kg", "a", vec![(1, 2)]).unwrap();
        storage.insert_into("nocost_kg", "b", vec![(2, 3)]).unwrap();

        // Join query - should succeed with unlimited cost
        let result =
            storage.execute_query_tuples_on("nocost_kg", "result(X, Y, Z) <- a(X, Y), b(Y, Z)");
        assert!(
            result.is_ok(),
            "Query should succeed with max_query_cost=0 (unlimited)"
        );
    }

    #[test]
    fn test_max_query_cost_allows_simple_query() {
        let temp = TempDir::new().unwrap();
        let mut config = create_test_config(temp.path().to_path_buf());
        config.storage.performance.max_query_cost = 1_000_000; // High threshold
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("simple_kg").unwrap();
        storage
            .insert_into("simple_kg", "data", vec![(1, 10), (2, 20)])
            .unwrap();

        let result = storage.execute_query_tuples_on("simple_kg", "result(X, Y) <- data(X, Y)");
        assert!(
            result.is_ok(),
            "Simple query should be under high cost threshold"
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod incremental_scc_tests {
    use super::*;
    use crate::config::Config;
    use crate::value::Value;
    use tempfile::TempDir;

    fn storage_with_incremental(temp: &TempDir) -> StorageEngine {
        let mut config = Config::default();
        config.storage.data_dir = temp.path().to_path_buf();
        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_knowledge_graph("default").unwrap();
        let kg = storage.knowledge_graphs.get("default").unwrap();
        kg.write().enable_incremental().unwrap();
        drop(kg);
        storage
    }

    fn ints(values: &[i64]) -> Vec<Tuple> {
        values
            .iter()
            .map(|&v| Tuple::new(vec![Value::Int64(v)]))
            .collect()
    }

    fn pairs(values: &[(i64, i64)]) -> Vec<Tuple> {
        values
            .iter()
            .map(|&(a, b)| Tuple::new(vec![Value::Int64(a), Value::Int64(b)]))
            .collect()
    }

    fn register(storage: &StorageEngine, text: &str) {
        let def = crate::statement::parse_rule_definition(text).unwrap();
        storage.register_rule(&def).unwrap();
    }

    fn query(storage: &StorageEngine, q: &str) -> Vec<i64> {
        let mut out: Vec<i64> = storage
            .execute_query_with_rules_tuples(q)
            .unwrap()
            .iter()
            .map(|t| match t.get(0) {
                Some(Value::Int64(v)) => *v,
                Some(Value::Int32(v)) => i64::from(*v),
                other => panic!("unexpected value {other:?}"),
            })
            .collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn test_incremental_kg_mutual_recursion_is_full_fixpoint() {
        let temp = TempDir::new().unwrap();
        let storage = storage_with_incremental(&temp);
        storage
            .insert_tuples("succ", pairs(&[(0, 1), (1, 2), (2, 3), (3, 4)]))
            .unwrap();
        storage.insert_tuples("zero", ints(&[0])).unwrap();
        register(&storage, "is_even(N) <- zero(N)");
        register(&storage, "is_even(N) <- succ(M, N), is_odd(M)");
        register(&storage, "is_odd(N) <- succ(M, N), is_even(M)");

        assert_eq!(query(&storage, "q(X) <- is_even(X)"), vec![0, 2, 4]);
        assert_eq!(query(&storage, "q(X) <- is_odd(X)"), vec![1, 3]);

        storage
            .delete_tuples_from("default", "succ", pairs(&[(2, 3)]))
            .unwrap();
        assert_eq!(query(&storage, "q(X) <- is_even(X)"), vec![0, 2]);
        assert_eq!(query(&storage, "q(X) <- is_odd(X)"), vec![1]);
    }

    #[test]
    fn test_incremental_kg_rule_chain_sees_upstream_rules() {
        let temp = TempDir::new().unwrap();
        let storage = storage_with_incremental(&temp);
        storage.insert_tuples("base", ints(&[1, 2])).unwrap();
        register(&storage, "a(X) <- base(X)");
        register(&storage, "b(X) <- a(X)");
        register(&storage, "c(X) <- b(X)");
        assert_eq!(query(&storage, "q(X) <- c(X)"), vec![1, 2]);

        storage.insert_tuples("base", ints(&[3])).unwrap();
        assert_eq!(query(&storage, "q(X) <- c(X)"), vec![1, 2, 3]);
    }
}
