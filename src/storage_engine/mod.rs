//! Storage Engine - Multi-Database Persistent Storage
//!
//! Provides:
//! - Multiple isolated databases (namespace isolation like PostgreSQL/MySQL)
//! - Filesystem persistence with configurable path
//! - Database lifecycle management (create, drop, list, switch)
//! - Database-scoped CRUD operations
//! - Parquet-based storage for efficiency
//!
//! ## Example
//!
//! ```rust,ignore
//! use datalog_engine::{StorageEngine, Config};
//!
//! let config = Config::load()?;
//! let mut storage = StorageEngine::new(config)?;
//!
//! // Create and use database
//! storage.create_database("analytics")?;
//! storage.use_database("analytics")?;
//!
//! // Insert data
//! storage.insert("edge", vec![(1, 2), (2, 3)])?;
//!
//! // Execute query
//! let results = storage.execute_query("path(x,y) :- edge(x,y).")?;
//!
//! // Persist to disk
//! storage.save_database("analytics")?;
//! ```

use crate::config::Config;
use crate::storage::{DatabaseMetadata, DatabasesMetadata, StorageError, StorageResult};
use crate::storage::parquet::{load_from_parquet, save_to_parquet};
use crate::storage::persist::{
    consolidate_to_current, to_tuples, to_tuple2s, FilePersist, PersistBackend, PersistConfig, Update,
};
use crate::value::Tuple2;
use crate::value::Tuple;
use crate::rule_catalog::RuleCatalog;
use crate::statement::RuleDef;
use crate::DatalogEngine;
use chrono::Utc;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Storage Engine - manages multiple databases
pub struct StorageEngine {
    config: Config,
    databases: HashMap<String, Arc<RwLock<Database>>>,
    current_db: Option<String>,
    /// DD-native persist backend
    persist: Arc<FilePersist>,
    /// Logical timestamp for DD updates (monotonically increasing)
    logical_time: AtomicU64,
}

/// Single database instance
pub struct Database {
    name: String,
    engine: DatalogEngine,
    metadata: DatabaseMetadata,
    data_dir: PathBuf,
    /// Rule catalog for persistent derived relations
    rule_catalog: RuleCatalog,
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
        };
        let persist = Arc::new(FilePersist::new(persist_config)?);

        let mut engine = StorageEngine {
            config,
            databases: HashMap::new(),
            current_db: None,
            persist,
            logical_time: AtomicU64::new(1),
        };

        // Load existing databases from persist layer
        engine.load_all_databases()?;

        // Create default database if it doesn't exist
        let default_db = engine.config.storage.default_database.clone();
        if !engine.databases.contains_key(&default_db) {
            engine.create_database(&default_db)?;
        }

        // Set current database to default
        engine.current_db = Some(default_db);

        Ok(engine)
    }

    /// Create a new database
    pub fn create_database(&mut self, name: &str) -> StorageResult<()> {
        if self.databases.contains_key(name) {
            return Err(StorageError::DatabaseExists(name.to_string()));
        }

        // Validate database name
        if name.is_empty() || name.contains('/') || name.contains('\\') {
            return Err(StorageError::InvalidRelationName(name.to_string()));
        }

        // Create database directory structure
        let db_dir = self.config.storage.data_dir.join(name);
        fs::create_dir_all(&db_dir)?;
        fs::create_dir_all(db_dir.join("relations"))?;

        // Create database instance (uses persist layer for durability)
        let database = Database::new(name.to_string(), db_dir);

        // Store in memory
        self.databases.insert(name.to_string(), Arc::new(RwLock::new(database)));

        // Update system metadata
        self.save_databases_metadata()?;

        Ok(())
    }

    /// Drop a database (delete all data)
    pub fn drop_database(&mut self, name: &str) -> StorageResult<()> {
        // Cannot drop default database
        if name == self.config.storage.default_database {
            return Err(StorageError::CannotDropDefault);
        }

        // Cannot drop current database
        if let Some(current) = &self.current_db {
            if current == name {
                return Err(StorageError::CannotDropCurrentDatabase);
            }
        }

        // Check if database exists
        if !self.databases.contains_key(name) {
            return Err(StorageError::DatabaseNotFound(name.to_string()));
        }

        // Remove from memory
        self.databases.remove(name);

        // Delete from disk
        let db_dir = self.config.storage.data_dir.join(name);
        if db_dir.exists() {
            fs::remove_dir_all(db_dir)?;
        }

        // Update system metadata
        self.save_databases_metadata()?;

        Ok(())
    }

    /// Switch to a different database
    pub fn use_database(&mut self, name: &str) -> StorageResult<()> {
        if !self.databases.contains_key(name) {
            if self.config.storage.auto_create_databases {
                self.create_database(name)?;
            } else {
                return Err(StorageError::DatabaseNotFound(name.to_string()));
            }
        }

        // Note: Could add last_accessed field to DatabaseMetadata for tracking
        // For now, we reuse created_at field (simplified implementation)
        if let Some(db) = self.databases.get(name) {
            let mut db = db.write().unwrap();
            db.metadata.created_at = Utc::now().to_rfc3339();
        }

        self.current_db = Some(name.to_string());
        Ok(())
    }

    /// List all databases
    pub fn list_databases(&self) -> Vec<String> {
        let mut names: Vec<String> = self.databases.keys().cloned().collect();
        names.sort();
        names
    }

    /// Get current database name
    pub fn current_database(&self) -> Option<&str> {
        self.current_db.as_deref()
    }

    /// Insert tuples into a relation in the current database
    /// Returns (new_count, duplicate_count) for reporting to user
    pub fn insert(&mut self, relation: &str, tuples: Vec<Tuple2>) -> StorageResult<(usize, usize)> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?
            .to_string();

        self.insert_into(&db_name, relation, tuples)
    }

    /// Insert tuples into a specific database (explicit API)
    /// Returns (new_count, duplicate_count) for reporting to user
    pub fn insert_into(&mut self, database: &str, relation: &str, tuples: Vec<Tuple2>) -> StorageResult<(usize, usize)> {
        if tuples.is_empty() {
            return Ok((0, 0));
        }

        // Generate shard name and logical time
        let shard = format!("{}:{}", database, relation);
        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);

        // Create DD-style updates (+1 diff for insert)
        let updates: Vec<Update> = tuples.iter()
            .map(|&data| Update::insert_tuple2(data, time))
            .collect();

        // Persist first (durability guarantee via WAL + batches)
        self.persist.ensure_shard(&shard)?;
        self.persist.append(&shard, &updates)?;

        // Update in-memory state
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let mut db = db.write().unwrap();
        let (new_count, dup_count) = db.insert_in_memory(relation, tuples);

        Ok((new_count, dup_count))
    }

    /// Insert arbitrary-arity tuples into a relation in the current database
    /// This is the production API that supports vectors and mixed types.
    /// Returns (new_count, duplicate_count) for reporting to user
    pub fn insert_tuples(&mut self, relation: &str, tuples: Vec<Tuple>) -> StorageResult<(usize, usize)> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?
            .to_string();

        self.insert_tuples_into(&db_name, relation, tuples)
    }

    /// Insert arbitrary-arity tuples into a specific database (explicit API)
    /// Returns (new_count, duplicate_count) for reporting to user
    pub fn insert_tuples_into(&mut self, database: &str, relation: &str, tuples: Vec<Tuple>) -> StorageResult<(usize, usize)> {
        if tuples.is_empty() {
            return Ok((0, 0));
        }

        // Generate shard name and logical time
        let shard = format!("{}:{}", database, relation);
        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);

        // Create DD-style updates (+1 diff for insert)
        let updates: Vec<Update> = tuples.iter()
            .map(|data| Update::insert(data.clone(), time))
            .collect();

        // Persist first (durability guarantee via WAL + batches)
        self.persist.ensure_shard(&shard)?;
        self.persist.append(&shard, &updates)?;

        // Update in-memory state
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let mut db = db.write().unwrap();
        let (new_count, dup_count) = db.insert_tuples_in_memory(relation, tuples);

        Ok((new_count, dup_count))
    }

    /// Delete tuples from a relation in the current database
    pub fn delete(&mut self, relation: &str, tuples: Vec<Tuple2>) -> StorageResult<()> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?
            .to_string();

        self.delete_from(&db_name, relation, tuples)
    }

    /// Delete tuples from a specific database (explicit API)
    pub fn delete_from(&mut self, database: &str, relation: &str, tuples: Vec<Tuple2>) -> StorageResult<()> {
        if tuples.is_empty() {
            return Ok(());
        }

        // Generate shard name and logical time
        let shard = format!("{}:{}", database, relation);
        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);

        // Create DD-style updates (-1 diff for delete)
        let updates: Vec<Update> = tuples.iter()
            .map(|&data| Update::delete_tuple2(data, time))
            .collect();

        // Persist first (durability guarantee via WAL + batches)
        self.persist.ensure_shard(&shard)?;
        self.persist.append(&shard, &updates)?;

        // Update in-memory state
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let mut db = db.write().unwrap();
        db.delete_in_memory(relation, &tuples);

        Ok(())
    }

    /// Execute a Datalog query on the current database
    pub fn execute_query(&mut self, program: &str) -> StorageResult<Vec<Tuple2>> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?
            .to_string();

        self.execute_query_on(&db_name, program)
    }

    /// Execute a Datalog query on a specific database (explicit API)
    pub fn execute_query_on(&mut self, database: &str, program: &str) -> StorageResult<Vec<Tuple2>> {
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let mut db = db.write().unwrap();
        let results = db.execute(program)
            .map_err(|e| StorageError::Other(format!("Query execution failed: {}", e)))?;

        Ok(results)
    }

    /// Save a specific database to disk (flush persist buffers)
    pub fn save_database(&self, name: &str) -> StorageResult<()> {
        // Check database exists
        if !self.databases.contains_key(name) {
            return Err(StorageError::DatabaseNotFound(name.to_string()));
        }

        // Flush all shards for this database
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
        self.save_databases_metadata()?;

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

        self.save_databases_metadata()?;

        Ok(())
    }

    // ========================================================================
    // Rule Management (Persistent Derived Relations)
    // ========================================================================

    /// Register a persistent rule in the current database
    pub fn register_rule(&mut self, rule_def: &RuleDef) -> StorageResult<crate::rule_catalog::RuleRegisterResult> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?
            .to_string();

        self.register_rule_in(&db_name, rule_def)
    }

    /// Register a persistent rule in a specific database
    pub fn register_rule_in(&mut self, database: &str, rule_def: &RuleDef) -> StorageResult<crate::rule_catalog::RuleRegisterResult> {
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let mut db = db.write().unwrap();
        db.register_rule(rule_def)
            .map_err(|e| StorageError::Other(format!("Failed to register rule: {}", e)))
    }

    /// Drop a rule from the current database
    pub fn drop_rule(&mut self, name: &str) -> StorageResult<()> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?
            .to_string();

        self.drop_rule_in(&db_name, name)
    }

    /// Drop a rule from a specific database
    pub fn drop_rule_in(&mut self, database: &str, name: &str) -> StorageResult<()> {
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let mut db = db.write().unwrap();
        db.drop_rule(name)
            .map_err(|e| StorageError::Other(format!("Failed to drop rule: {}", e)))
    }

    /// List all rules in the current database
    pub fn list_rules(&self) -> StorageResult<Vec<String>> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?;

        self.list_rules_in(db_name)
    }

    /// List all rules in a specific database
    pub fn list_rules_in(&self, database: &str) -> StorageResult<Vec<String>> {
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let db = db.read().unwrap();
        Ok(db.list_rules())
    }

    /// Describe a rule in the current database
    pub fn describe_rule(&self, name: &str) -> StorageResult<Option<String>> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?;

        self.describe_rule_in(db_name, name)
    }

    /// Describe a rule in a specific database
    pub fn describe_rule_in(&self, database: &str, name: &str) -> StorageResult<Option<String>> {
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let db = db.read().unwrap();
        Ok(db.describe_rule(name))
    }

    /// Clear all clauses from a rule for editing/redefining (current database)
    /// The rule remains registered but with no clauses, ready for new clause registration
    pub fn clear_rule(&mut self, name: &str) -> StorageResult<()> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?
            .to_string();

        self.clear_rule_in(&db_name, name)
    }

    /// Clear all clauses from a rule for editing/redefining (specific database)
    pub fn clear_rule_in(&mut self, database: &str, name: &str) -> StorageResult<()> {
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let mut db = db.write().unwrap();
        db.clear_rule(name)
            .map_err(|e| StorageError::Other(format!("Failed to clear rule: {}", e)))
    }

    /// Replace a specific clause in a rule (current database)
    pub fn replace_rule(&mut self, name: &str, index: usize, new_rule: crate::statement::SerializableRule) -> StorageResult<()> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?
            .to_string();

        self.replace_rule_in(&db_name, name, index, new_rule)
    }

    /// Replace a specific clause in a rule (specific database)
    pub fn replace_rule_in(&mut self, database: &str, name: &str, index: usize, new_rule: crate::statement::SerializableRule) -> StorageResult<()> {
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let mut db = db.write().unwrap();
        db.replace_rule(name, index, new_rule)
            .map_err(|e| StorageError::Other(format!("Failed to replace rule clause: {}", e)))
    }

    /// Get the number of clauses in a rule (current database)
    pub fn rule_count(&self, name: &str) -> StorageResult<Option<usize>> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?;

        self.rule_count_in(db_name, name)
    }

    /// Get the number of clauses in a rule (specific database)
    pub fn rule_count_in(&self, database: &str, name: &str) -> StorageResult<Option<usize>> {
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let db = db.read().unwrap();
        Ok(db.rule_count(name))
    }

    /// Execute a query with rules prepended (current database)
    pub fn execute_query_with_rules(&mut self, program: &str) -> StorageResult<Vec<Tuple2>> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?
            .to_string();

        self.execute_query_with_rules_on(&db_name, program)
    }

    /// Execute a query with rules prepended (specific database)
    pub fn execute_query_with_rules_on(&mut self, database: &str, program: &str) -> StorageResult<Vec<Tuple2>> {
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let mut db = db.write().unwrap();
        db.execute_with_rules(program)
            .map_err(|e| StorageError::Other(format!("Query execution failed: {}", e)))
    }

    /// Execute a query with rules prepended, returning tuples of arbitrary arity (current database)
    pub fn execute_query_with_rules_tuples(&mut self, program: &str) -> StorageResult<Vec<Tuple>> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?
            .to_string();

        self.execute_query_with_rules_tuples_on(&db_name, program)
    }

    /// Execute a query with rules prepended, returning tuples of arbitrary arity (specific database)
    pub fn execute_query_with_rules_tuples_on(&mut self, database: &str, program: &str) -> StorageResult<Vec<Tuple>> {
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let mut db = db.write().unwrap();
        db.execute_with_rules_tuples(program)
            .map_err(|e| StorageError::Other(format!("Query execution failed: {}", e)))
    }

    /// List all relations (base facts) in the current database
    pub fn list_relations(&self) -> StorageResult<Vec<String>> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?;

        self.list_relations_in(db_name)
    }

    /// List all relations in a specific database
    pub fn list_relations_in(&self, database: &str) -> StorageResult<Vec<String>> {
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let db = db.read().unwrap();
        let relations: Vec<String> = db.metadata.relations.keys().cloned().collect();
        Ok(relations)
    }

    /// Describe a relation in the current database
    pub fn describe_relation(&self, name: &str) -> StorageResult<Option<String>> {
        let db_name = self.current_db.as_ref()
            .ok_or(StorageError::NoCurrentDatabase)?;

        self.describe_relation_in(db_name, name)
    }

    /// Describe a relation in a specific database
    pub fn describe_relation_in(&self, database: &str, name: &str) -> StorageResult<Option<String>> {
        let db = self.databases.get(database)
            .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

        let db = db.read().unwrap();
        if let Some(rel_meta) = db.metadata.relations.get(name) {
            let desc = format!(
                "Relation: {}\nSchema: {:?}\nTuple count: {}",
                name,
                rel_meta.schema,
                rel_meta.tuple_count
            );
            Ok(Some(desc))
        } else {
            Ok(None)
        }
    }

    /// Load all databases from persist layer
    ///
    /// Recovery process:
    /// 1. Discover databases from persist shards
    /// 2. For each database, read all shards
    /// 3. Consolidate updates to get current state
    /// 4. Populate in-memory DatalogEngine
    fn load_all_databases(&mut self) -> StorageResult<()> {
        // Discover databases from persist shards
        let shard_names = self.persist.list_shards()?;
        let mut db_names: std::collections::HashSet<String> = std::collections::HashSet::new();

        for shard in &shard_names {
            if let Some(db_name) = shard.split(':').next() {
                db_names.insert(db_name.to_string());
            }
        }

        // Also check metadata file for databases without data yet
        let metadata_path = self.config.storage.data_dir.join("metadata/databases.json");
        if metadata_path.exists() {
            if let Ok(metadata) = DatabasesMetadata::load(&metadata_path) {
                for db_info in metadata.databases {
                    db_names.insert(db_info.name);
                }
            }
        }

        // Load each database
        for db_name in db_names {
            let db_dir = self.config.storage.data_dir.join(&db_name);
            fs::create_dir_all(&db_dir)?;

            let database = self.load_database_from_persist(&db_name, db_dir)?;
            self.databases.insert(db_name, Arc::new(RwLock::new(database)));
        }

        // Update logical time to be after all loaded data
        let max_time = self.find_max_logical_time()?;
        self.logical_time.store(max_time + 1, Ordering::SeqCst);

        Ok(())
    }

    /// Load a single database from persist layer
    fn load_database_from_persist(&self, name: &str, data_dir: PathBuf) -> StorageResult<Database> {
        let prefix = format!("{}:", name);
        let mut engine = DatalogEngine::new();

        // Find all shards for this database
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
                    engine.add_fact(relation, tuples);
                }
            }
        }

        let metadata = DatabaseMetadata::new(name.to_string());

        // Load view catalog (will load existing views if present)
        let rule_catalog = RuleCatalog::new(data_dir.clone())
            .map_err(|e| StorageError::Other(format!("Failed to load view catalog: {}", e)))?;

        Ok(Database {
            name: name.to_string(),
            engine,
            metadata,
            data_dir,
            rule_catalog,
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

    /// Save system-wide databases metadata
    fn save_databases_metadata(&self) -> StorageResult<()> {
        let metadata_dir = self.config.storage.data_dir.join("metadata");
        fs::create_dir_all(&metadata_dir)?;

        let databases: Vec<_> = self.databases.iter()
            .map(|(name, db)| {
                let db = db.read().unwrap();
                crate::storage::metadata::DatabaseInfo {
                    name: name.clone(),
                    created_at: db.metadata.created_at.clone(),
                    last_accessed: Utc::now().to_rfc3339(),
                    relations_count: db.metadata.relations.len(),
                    total_tuples: db.metadata.total_tuples(),
                }
            })
            .collect();

        let metadata = DatabasesMetadata {
            version: "1.0".to_string(),
            databases,
        };

        metadata.save(&metadata_dir.join("databases.json"))?;

        Ok(())
    }

    /// Get reference to the configuration
    pub fn config(&self) -> &Config {
        &self.config
    }

    // ========================================================================
    // Parallel Query Execution API
    // ========================================================================

    /// Execute multiple queries in parallel across different databases
    ///
    /// This method leverages Rayon's thread pool to execute queries concurrently,
    /// utilizing all available CPU cores efficiently.
    ///
    /// # Example
    /// ```rust,ignore
    /// let queries = vec![
    ///     ("db1", "result(x,y) :- edge(x,y)."),
    ///     ("db2", "result(x,y) :- person(x,y)."),
    ///     ("db3", "result(x,y) :- data(x,y)."),
    /// ];
    ///
    /// let results = storage.execute_parallel_queries_on_databases(queries)?;
    /// ```
    pub fn execute_parallel_queries_on_databases(
        &self,
        queries: Vec<(&str, &str)>,
    ) -> StorageResult<Vec<(String, Vec<Tuple2>)>> {
        // Use Rayon to execute queries in parallel
        let results: Result<Vec<_>, StorageError> = queries
            .par_iter()
            .map(|(database, program)| {
                // Get database with read lock
                let db = self.databases.get(*database)
                    .ok_or_else(|| StorageError::DatabaseNotFound(database.to_string()))?;

                // Execute query (RwLock allows multiple concurrent readers)
                let mut db = db.write().unwrap();
                let results = db.execute(program)
                    .map_err(|e| StorageError::Other(format!("Query execution failed: {}", e)))?;

                Ok((database.to_string(), results))
            })
            .collect();

        results
    }

    /// Execute the same query on multiple databases in parallel
    ///
    /// Useful for federated queries or comparing results across databases.
    ///
    /// # Example
    /// ```rust,ignore
    /// let databases = vec!["db1", "db2", "db3"];
    /// let query = "result(x,y) :- edge(x,y), x > 5.";
    ///
    /// let results = storage.execute_query_on_multiple_databases(databases, query)?;
    /// ```
    pub fn execute_query_on_multiple_databases(
        &self,
        databases: Vec<&str>,
        program: &str,
    ) -> StorageResult<Vec<(String, Vec<Tuple2>)>> {
        let queries: Vec<(&str, &str)> = databases.iter()
            .map(|db| (*db, program))
            .collect();

        self.execute_parallel_queries_on_databases(queries)
    }

    /// Execute multiple queries on the same database in parallel
    ///
    /// Note: Since Datalog queries read from the same database, this uses
    /// RwLock::read() to allow concurrent read access.
    ///
    /// # Example
    /// ```rust,ignore
    /// let queries = vec![
    ///     "q1(x,y) :- edge(x,y).",
    ///     "q2(x,z) :- path(x,y), path(y,z).",
    ///     "q3(x) :- person(x,_), edge(x,_).",
    /// ];
    ///
    /// let results = storage.execute_parallel_queries_on_database("db1", queries)?;
    /// ```
    pub fn execute_parallel_queries_on_database(
        &self,
        database: &str,
        programs: Vec<&str>,
    ) -> StorageResult<Vec<Vec<Tuple2>>> {
        // Verify database exists
        if !self.databases.contains_key(database) {
            return Err(StorageError::DatabaseNotFound(database.to_string()));
        }

        // Execute queries in parallel
        let results: Result<Vec<_>, StorageError> = programs
            .par_iter()
            .map(|program| {
                let db = self.databases.get(database).unwrap();
                let mut db = db.write().unwrap();

                db.execute(program)
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

impl Database {
    /// Create a new empty database
    fn new(name: String, data_dir: PathBuf) -> Self {
        // Create view catalog (will load existing views if present)
        let rule_catalog = RuleCatalog::new(data_dir.clone())
            .unwrap_or_else(|_| {
                // If loading fails, create empty catalog
                RuleCatalog::new(data_dir.clone()).unwrap()
            });

        Database {
            name: name.clone(),
            engine: DatalogEngine::new(),
            metadata: DatabaseMetadata::new(name),
            data_dir,
            rule_catalog,
        }
    }

    /// Insert tuples into in-memory state only
    ///
    /// Persistence is handled by StorageEngine via the persist layer.
    /// Returns (new_count, duplicate_count) for caller to report.
    fn insert_in_memory(&mut self, relation: &str, tuples: Vec<Tuple2>) -> (usize, usize) {
        // Get schema (immutable borrow)
        let schema = self.engine.catalog()
            .get_schema(relation)
            .map(|s| s.to_vec())
            .unwrap_or_else(|| vec!["col0".to_string(), "col1".to_string()]);

        // Update in-memory state, tracking new vs duplicate
        let mut new_count = 0;
        let mut dup_count = 0;
        let existing = self.engine.input_data.entry(relation.to_string()).or_insert_with(Vec::new);
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
        self.metadata.add_relation(relation.to_string(), schema, tuple_count);

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
        let existing_tuples = self.engine.input_tuples
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
        self.metadata.add_relation(relation.to_string(), schema, tuple_count);

        (new_count, dup_count)
    }

    /// Delete tuples from in-memory state only
    ///
    /// Persistence is handled by StorageEngine via the persist layer.
    fn delete_in_memory(&mut self, relation: &str, tuples_to_remove: &[Tuple2]) {
        // Get schema (immutable borrow)
        let schema = self.engine.catalog()
            .get_schema(relation)
            .map(|s| s.to_vec())
            .unwrap_or_else(|| vec!["col0".to_string(), "col1".to_string()]);

        // Update in-memory state
        if let Some(existing) = self.engine.input_data.get_mut(relation) {
            // Remove tuples
            existing.retain(|tuple| !tuples_to_remove.contains(tuple));
            let tuple_count = existing.len();

            // Update metadata
            self.metadata.add_relation(relation.to_string(), schema, tuple_count);
        }
    }

    /// Execute a Datalog program
    fn execute(&mut self, program: &str) -> Result<Vec<Tuple2>, String> {
        self.engine.execute(program)
    }

    /// Get database name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get database metadata
    pub fn metadata(&self) -> &DatabaseMetadata {
        &self.metadata
    }

    // ========================================================================
    // View Management
    // ========================================================================

    /// Register a persistent view
    /// Returns whether view was created or rule was added
    pub fn register_rule(&mut self, rule_def: &RuleDef) -> Result<crate::rule_catalog::RuleRegisterResult, String> {
        self.rule_catalog.register_rule(rule_def)
    }

    /// Drop a view
    pub fn drop_rule(&mut self, name: &str) -> Result<(), String> {
        self.rule_catalog.drop(name)
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
        self.rule_catalog.clear_rules(name)
    }

    /// Replace a specific rule in a view by index (0-based)
    pub fn replace_rule(&mut self, name: &str, index: usize, new_rule: crate::statement::SerializableRule) -> Result<(), String> {
        self.rule_catalog.replace_rule(name, index, new_rule)
    }

    /// Get the number of rules in a view
    pub fn rule_count(&self, name: &str) -> Option<usize> {
        self.rule_catalog.rule_count(name)
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
            eprintln!("DEBUG execute_with_rules_tuples: {} view rules, program = {}", rule_defs.len(), combined.replace('\n', " | "));
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

    if rule.body.is_empty() && rule.constraints.is_empty() {
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
        }
    }

    for constraint in &rule.constraints {
        body_parts.push(format_constraint(constraint));
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
            format!("{}{}{}", format_arith_expr(left), op.as_str(), format_arith_expr(right))
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
        crate::ast::AggregateFunc::TopK { k, order_var, descending } => {
            if *descending {
                format!("top_k<{}, {}, desc>", k, order_var)
            } else {
                format!("top_k<{}, {}>", k, order_var)
            }
        }
        crate::ast::AggregateFunc::TopKThreshold { k, order_var, threshold, descending } => {
            if *descending {
                format!("top_k_threshold<{}, {}, {}, desc>", k, order_var, threshold)
            } else {
                format!("top_k_threshold<{}, {}, {}>", k, order_var, threshold)
            }
        }
        crate::ast::AggregateFunc::WithinRadius { distance_var, max_distance } => {
            format!("within_radius<{}, {}>", distance_var, max_distance)
        }
    }
}

/// Format a Constraint as a Datalog string
fn format_constraint(constraint: &crate::ast::Constraint) -> String {
    match constraint {
        crate::ast::Constraint::Equal(l, r) => format!("{} = {}", format_term(l), format_term(r)),
        crate::ast::Constraint::NotEqual(l, r) => format!("{} != {}", format_term(l), format_term(r)),
        crate::ast::Constraint::LessThan(l, r) => format!("{} < {}", format_term(l), format_term(r)),
        crate::ast::Constraint::LessOrEqual(l, r) => format!("{} <= {}", format_term(l), format_term(r)),
        crate::ast::Constraint::GreaterThan(l, r) => format!("{} > {}", format_term(l), format_term(r)),
        crate::ast::Constraint::GreaterOrEqual(l, r) => format!("{} >= {}", format_term(l), format_term(r)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::config::Config;

    fn create_test_config(data_dir: PathBuf) -> Config {
        let mut config = Config::default();
        config.storage.data_dir = data_dir;
        config
    }

    #[test]
    fn test_create_and_list_databases() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();

        // Should have default database
        assert!(storage.list_databases().contains(&"default".to_string()));

        // Create new databases
        storage.create_database("db1").unwrap();
        storage.create_database("db2").unwrap();

        let databases = storage.list_databases();
        assert_eq!(databases.len(), 3);
        assert!(databases.contains(&"default".to_string()));
        assert!(databases.contains(&"db1".to_string()));
        assert!(databases.contains(&"db2".to_string()));
    }

    #[test]
    fn test_use_database() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();

        storage.create_database("test_db").unwrap();
        storage.use_database("test_db").unwrap();

        assert_eq!(storage.current_database(), Some("test_db"));
    }

    #[test]
    fn test_database_isolation() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();

        // DB1: Insert edge data
        storage.create_database("db1").unwrap();
        storage.use_database("db1").unwrap();
        storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();

        // DB2: Should not see edge data
        storage.create_database("db2").unwrap();
        storage.use_database("db2").unwrap();

        // Query for edge in db2 - should return empty results (database isolation)
        let result = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
        assert_eq!(result.len(), 0); // No edge relation in db2 - empty result
    }

    #[test]
    fn test_persistence_roundtrip() {
        let temp = TempDir::new().unwrap();

        // Create and populate
        {
            let config = create_test_config(temp.path().to_path_buf());
            let mut storage = StorageEngine::new(config).unwrap();

            storage.create_database("persist_test").unwrap();
            storage.use_database("persist_test").unwrap();
            storage.insert("edge", vec![(1, 2), (2, 3), (3, 4)]).unwrap();
            storage.save_all().unwrap();
        }

        // Reload
        {
            let config = create_test_config(temp.path().to_path_buf());
            let mut storage = StorageEngine::new(config).unwrap();

            storage.use_database("persist_test").unwrap();

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

        let result = storage.drop_database("default");
        assert!(matches!(result, Err(StorageError::CannotDropDefault)));
    }

    #[test]
    fn test_cannot_drop_current() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();

        storage.create_database("test").unwrap();
        storage.use_database("test").unwrap();

        let result = storage.drop_database("test");
        assert!(matches!(result, Err(StorageError::CannotDropCurrentDatabase)));
    }

    #[test]
    fn test_recursive_view_transitive_closure() {
        use crate::statement::RuleDef;
        use crate::ast::{Atom, BodyPredicate, Rule, Term};

        let temp = TempDir::new().unwrap();
        let config = create_test_config(temp.path().to_path_buf());

        let mut storage = StorageEngine::new(config).unwrap();
        storage.use_database("default").unwrap();

        // Insert edge data: 1->2->3->4
        storage.insert("edge", vec![(1, 2), (2, 3), (3, 4)]).unwrap();

        // Define first rule: connected(X, Y) :- edge(X, Y).
        let rule1 = Rule::new(
            Atom::new(
                "connected".to_string(),
                vec![Term::Variable("X".to_string()), Term::Variable("Y".to_string())],
            ),
            vec![BodyPredicate::Positive(Atom::new(
                "edge".to_string(),
                vec![Term::Variable("X".to_string()), Term::Variable("Y".to_string())],
            ))],
            vec![],
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
                vec![Term::Variable("X".to_string()), Term::Variable("Z".to_string())],
            ),
            vec![
                BodyPredicate::Positive(Atom::new(
                    "edge".to_string(),
                    vec![Term::Variable("X".to_string()), Term::Variable("Y".to_string())],
                )),
                BodyPredicate::Positive(Atom::new(
                    "connected".to_string(),
                    vec![Term::Variable("Y".to_string()), Term::Variable("Z".to_string())],
                )),
            ],
            vec![],
        );
        let rule_def2 = RuleDef {
            name: "connected".to_string(),
            rule: crate::statement::SerializableRule::from_rule(&rule2),
        };
        storage.register_rule(&rule_def2).unwrap();

        // Check views are registered
        let views = storage.list_rules().unwrap();
        println!("Views: {:?}", views);
        assert!(views.contains(&"connected".to_string()), "View 'connected' should exist");

        // Check describe_rule shows both rules
        let desc = storage.describe_rule("connected").unwrap();
        println!("View description:\n{}", desc.as_ref().unwrap());

        // Debug: print the combined program
        {
            let db = storage.databases.get("default").unwrap();
            let db = db.read().unwrap();
            let rule_defs = db.rule_catalog.all_rules();
            println!("Number of view rules: {}", rule_defs.len());
            for (i, rule) in rule_defs.iter().enumerate() {
                println!("Rule {}: {}", i, format_rule(rule));
            }
        }

        // Query all connected pairs
        eprintln!("\n=== Executing query with views ===");
        let result = storage.execute_query_with_rules("result(X,Y) :- connected(X,Y).").unwrap();
        println!("All connected pairs: {:?}", result);

        // Expected transitive closure: (1,2), (2,3), (3,4), (1,3), (2,4), (1,4)
        assert!(result.len() >= 6, "Should have at least 6 connected pairs, got {}", result.len());
        assert!(result.contains(&(1, 2)), "Should contain (1, 2)");
        assert!(result.contains(&(2, 3)), "Should contain (2, 3)");
        assert!(result.contains(&(3, 4)), "Should contain (3, 4)");
        assert!(result.contains(&(1, 3)), "Should contain (1, 3) - transitive");
        assert!(result.contains(&(2, 4)), "Should contain (2, 4) - transitive");
        assert!(result.contains(&(1, 4)), "Should contain (1, 4) - transitive");

        // Query specific: connected(1, 3) - should return 1 row
        let specific_result = storage.execute_query_with_rules(
            "result(X,Y) :- connected(X,Y), X = 1, Y = 3."
        ).unwrap();
        println!("connected(1, 3): {:?}", specific_result);
        assert_eq!(specific_result.len(), 1, "Should find exactly one (1, 3) connection");
        assert_eq!(specific_result[0], (1, 3));
    }
}
