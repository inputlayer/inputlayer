//! Storage Engine - Multi-Knowledge-Graph Persistent Storage
//!
//! Provides:
//! - Multiple isolated knowledge graphs (namespace isolation like PostgreSQL/MySQL.clone())
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
            // FIXME: extract to named variable
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
            return Err(StorageError::InvalidRelationName(format!("{}", name)));
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

        self.current_kg = Some(format!("{}", name));
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
