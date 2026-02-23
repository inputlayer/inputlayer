//! Index Manager for Vector Similarity Search
//!
//! Manages HNSW and other indexes for knowledge graphs. Follows the same
//! pattern as `DerivedRelationsManager` for per-KG isolation and cascade
//! invalidation.
//!
//! ## Architecture
//!
//! ```text
//! Base Relations (IncrementalEngine)
//!        |
//!        |--- documents(id, title, embedding)
//!        `--- ...
//!              |
//!              ▼
//!     IndexManager
//!        |
//!        |--- RegisteredIndex (metadata)
//!        |         |
//!        |         ▼
//!        `--- MaterializedIndex (HNSW structure + validity)
//! ```
//!
//! ## Key Concepts
//!
//! - RegisteredIndex: Index definition (relation, column, config)
//! - MaterializedIndex: Built index structure with validity tracking
//! - Dependency Tracking: Maps base relations -> dependent indexes
//! - Cascade Invalidation: Base updates invalidate dependent indexes

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Counter for index versions
static INDEX_VERSION: AtomicU64 = AtomicU64::new(0);

/// Tuple ID type - position in the relation's `Vec<Tuple>`
pub type TupleId = usize;

/// Distance metric for similarity search
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum DistanceMetric {
    /// Cosine similarity (normalized dot product)
    #[default]
    Cosine,
    /// Euclidean distance (L2 norm)
    Euclidean,
    /// Dot product similarity
    DotProduct,
    /// Manhattan distance (L1 norm)
    Manhattan,
}

impl std::fmt::Display for DistanceMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cosine => write!(f, "cosine"),
            Self::Euclidean => write!(f, "l2"),
            Self::DotProduct => write!(f, "dot"),
            Self::Manhattan => write!(f, "l1"),
        }
    }
}

impl std::str::FromStr for DistanceMetric {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "cosine" | "cos" => Ok(Self::Cosine),
            "euclidean" | "l2" | "euclid" => Ok(Self::Euclidean),
            "dot" | "dotproduct" | "dot_product" | "inner" => Ok(Self::DotProduct),
            "manhattan" | "l1" | "taxicab" => Ok(Self::Manhattan),
            _ => Err(format!(
                "Unknown distance metric: '{s}'. Valid options: cosine, l2, dot, l1"
            )),
        }
    }
}

/// HNSW-specific configuration
#[derive(Clone, Debug, PartialEq)]
pub struct HnswConfig {
    /// Maximum number of connections per layer (default: 16)
    pub m: usize,
    /// Construction-time ef parameter (default: 200)
    pub ef_construction: usize,
    /// Default search ef parameter (default: 50)
    pub ef_search: usize,
    /// Distance metric for similarity calculation
    pub metric: DistanceMetric,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            metric: DistanceMetric::Cosine,
        }
    }
}

/// Index type enumeration
#[derive(Clone, Debug, PartialEq)]
pub enum IndexType {
    /// HNSW index for approximate nearest neighbor search
    Hnsw(HnswConfig),
    // Future index types:
    // BTree(BTreeConfig),
    // Hash(HashConfig),
    // Bloom(BloomConfig),
}

impl IndexType {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Hnsw(_) => "hnsw",
        }
    }
}

/// Common interface for all index types
pub trait Index: Send + Sync {
    /// Search for k nearest neighbors to the query vector
    ///
    /// Returns a vector of (tuple_id, distance) pairs, sorted by distance
    /// (ascending for distance metrics, descending for similarity metrics)
    fn search(&self, query: &[f32], k: usize, ef: Option<usize>) -> Vec<(TupleId, f64)>;

    /// Insert a vector with the given tuple ID
    fn insert(&mut self, id: TupleId, vector: &[f32]) -> Result<(), String>;

    /// Mark a tuple ID as deleted (tombstone)
    ///
    /// The actual data is not removed immediately - it's marked with a tombstone.
    /// Call `rebuild()` to compact the index and remove tombstones.
    fn delete(&mut self, id: TupleId);

    /// Get the ratio of tombstones to total entries
    ///
    /// Used to decide when to trigger a rebuild. A high ratio (e.g., > 0.3)
    /// indicates the index should be rebuilt.
    fn tombstone_ratio(&self) -> f64;

    /// Rebuild the index from scratch, removing tombstones
    ///
    /// `vectors` contains the current valid (id, vector) pairs to index.
    fn rebuild(&mut self, vectors: &[(TupleId, Vec<f32>)]) -> Result<(), String>;

    /// Get the number of vectors in the index (including tombstones)
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn index_type(&self) -> &str;

    fn metric(&self) -> DistanceMetric;

    /// Get the number of tombstones (deleted entries)
    fn tombstone_count(&self) -> usize;

    /// Get the vector dimension (0 if empty)
    fn dimension(&self) -> usize;

    /// Insert multiple vectors at once (#17).
    /// Default implementation calls `insert()` for each. HNSW overrides this
    /// to defer the graph rebuild until after all inserts, avoiding O(N^2 log N) cost.
    fn insert_batch(&mut self, entries: &[(TupleId, Vec<f32>)]) -> Result<(), String> {
        for (id, vec) in entries {
            self.insert(*id, vec)?;
        }
        Ok(())
    }

    /// Downcast to concrete type for persistence operations.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Index registration metadata
#[derive(Clone, Debug)]
pub struct RegisteredIndex {
    /// Unique name for this index
    pub name: String,
    /// Relation this index is built on
    pub relation: String,
    /// Column index containing the vector data
    pub column_idx: usize,
    /// Column name (for display)
    pub column_name: String,
    /// Type of index and its configuration
    pub index_type: IndexType,
}

/// Materialized index with validity tracking
pub struct MaterializedIndex {
    /// The actual index structure
    pub index: Box<dyn Index + Send + Sync>,

    /// Arc-wrapped index for sharing with snapshots
    index_arc: Arc<dyn Index + Send + Sync>,

    /// Version when this was last built
    pub version: u64,

    /// Base relation versions this index was built from
    pub base_versions: HashMap<String, u64>,

    /// Whether the materialization is currently valid
    pub valid: bool,

    /// Timestamp when built (microseconds since epoch)
    pub built_at: u64,

    /// Number of vectors in the index
    pub tuple_count: usize,
}

impl MaterializedIndex {
    pub fn new(
        index: Box<dyn Index + Send + Sync>,
        base_versions: HashMap<String, u64>,
        tuple_count: usize,
    ) -> Self {
        let version = INDEX_VERSION.fetch_add(1, Ordering::SeqCst);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        // Create Arc wrapper for the index
        // Box<dyn Index> and Arc<dyn Index> require different ownership,
        // so we use an IndexWrapper to bridge between them.
        let index_arc = Arc::from(index);

        Self {
            index: Box::new(IndexWrapper(Arc::clone(&index_arc))),
            index_arc,
            version,
            base_versions,
            valid: true,
            built_at: now,
            tuple_count,
        }
    }

    /// Mark this materialization as invalid
    pub fn invalidate(&mut self) {
        self.valid = false;
    }

    /// Get an Arc reference to the index for snapshot sharing
    pub fn arc(&self) -> Arc<dyn Index + Send + Sync> {
        Arc::clone(&self.index_arc)
    }
}

/// Wrapper to allow Arc<dyn Index> to be used as Box<dyn Index>
struct IndexWrapper(Arc<dyn Index + Send + Sync>);

impl Index for IndexWrapper {
    fn search(&self, query: &[f32], k: usize, ef: Option<usize>) -> Vec<(TupleId, f64)> {
        self.0.search(query, k, ef)
    }

    fn insert(&mut self, _id: TupleId, _vector: &[f32]) -> Result<(), String> {
        Err("Cannot insert into Arc-wrapped index".to_string())
    }

    fn delete(&mut self, _id: TupleId) {
        // No-op for Arc-wrapped index
    }

    fn tombstone_ratio(&self) -> f64 {
        self.0.tombstone_ratio()
    }

    fn rebuild(&mut self, _vectors: &[(TupleId, Vec<f32>)]) -> Result<(), String> {
        Err("Cannot rebuild Arc-wrapped index".to_string())
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn index_type(&self) -> &str {
        self.0.index_type()
    }

    fn metric(&self) -> DistanceMetric {
        self.0.metric()
    }

    fn tombstone_count(&self) -> usize {
        self.0.tombstone_count()
    }

    fn dimension(&self) -> usize {
        self.0.dimension()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Statistics about an index for reporting
#[derive(Clone, Debug)]
pub struct IndexStats {
    /// Index name
    pub name: String,
    /// Relation the index is built on
    pub relation: String,
    /// Column name
    pub column: String,
    /// Index type name
    pub index_type: String,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Number of vectors
    pub tuple_count: usize,
    /// Number of tombstones
    pub tombstone_count: usize,
    /// Whether the index is valid
    pub valid: bool,
    /// When the index was built
    pub built_at: u64,
    /// Vector dimension
    pub dimension: usize,
}

/// Manages indexes for a single KnowledgeGraph
///
/// Follows the same pattern as `DerivedRelationsManager`:
/// - Per-KG isolation (no global state)
/// - Dependency tracking for cascade invalidation
/// - Validity tracking with version numbers
#[derive(Default)]
pub struct IndexManager {
    /// Registered indexes by name
    indexes: HashMap<String, RegisteredIndex>,

    /// Materialized index data
    materialized: HashMap<String, MaterializedIndex>,

    /// Forward dependency map: base_relation -> [index_names]
    base_to_indexes: HashMap<String, HashSet<String>>,

    /// Current version of each base relation
    base_versions: HashMap<String, u64>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new index (does not build it)
    ///
    /// Returns an error if an index with the same name already exists.
    pub fn register_index(&mut self, index: RegisteredIndex) -> Result<(), String> {
        let name = index.name.clone();
        let relation = index.relation.clone();

        if self.indexes.contains_key(&name) {
            return Err(format!("Index '{name}' already exists"));
        }

        // Track dependency: base_relation -> index
        self.base_to_indexes
            .entry(relation)
            .or_default()
            .insert(name.clone());

        self.indexes.insert(name, index);
        Ok(())
    }

    /// Remove an index by name
    pub fn remove_index(&mut self, name: &str) -> Result<(), String> {
        if let Some(index) = self.indexes.remove(name) {
            // Clean up dependency tracking
            if let Some(deps) = self.base_to_indexes.get_mut(&index.relation) {
                deps.remove(name);
            }
            self.materialized.remove(name);
            Ok(())
        } else {
            Err(format!("Index '{name}' not found"))
        }
    }

    /// Get a registered index by name
    pub fn get_registered(&self, name: &str) -> Option<&RegisteredIndex> {
        self.indexes.get(name)
    }

    pub fn has_index(&self, name: &str) -> bool {
        self.indexes.contains_key(name)
    }

    /// Store a built index
    pub fn set_materialized(
        &mut self,
        name: &str,
        index: Box<dyn Index + Send + Sync>,
        tuple_count: usize,
    ) {
        let base_versions = self.base_versions.clone();

        self.materialized.insert(
            name.to_string(),
            MaterializedIndex::new(index, base_versions, tuple_count),
        );
    }

    /// Get a materialized index if valid
    pub fn get_materialized(&self, name: &str) -> Option<&MaterializedIndex> {
        self.materialized.get(name).filter(|m| m.valid)
    }

    /// Get a mutable reference to a materialized index
    pub fn get_materialized_mut(&mut self, name: &str) -> Option<&mut MaterializedIndex> {
        self.materialized.get_mut(name)
    }

    /// Notify that a base relation was updated
    ///
    /// This invalidates all indexes that depend on the relation.
    /// Returns the names of indexes that were invalidated.
    pub fn notify_base_update(&mut self, base_relation: &str) -> Vec<String> {
        let mut invalidated = Vec::new();

        // Bump base version
        let version = self
            .base_versions
            .entry(base_relation.to_string())
            .or_insert(0);
        *version += 1;

        // Invalidate all indexes on this relation
        if let Some(index_names) = self.base_to_indexes.get(base_relation) {
            for name in index_names {
                if let Some(mat) = self.materialized.get_mut(name) {
                    if mat.valid {
                        mat.invalidate();
                        invalidated.push(name.clone());
                    }
                }
            }
        }

        invalidated
    }

    /// Get all valid indexes for snapshot publication
    pub fn get_all_valid_indexes(&self) -> HashMap<String, Arc<dyn Index + Send + Sync>> {
        self.materialized
            .iter()
            .filter(|(_, m)| m.valid)
            .map(|(name, m)| (name.clone(), m.arc()))
            .collect()
    }

    /// Get the names of all invalid indexes (need rebuild)
    pub fn get_invalid_indexes(&self) -> Vec<String> {
        self.indexes
            .keys()
            .filter(|name| self.materialized.get(*name).is_none_or(|m| !m.valid))
            .cloned()
            .collect()
    }

    /// Get all indexes for a relation
    pub fn get_indexes_for_relation(&self, relation: &str) -> Vec<&RegisteredIndex> {
        self.base_to_indexes
            .get(relation)
            .map(|names| {
                names
                    .iter()
                    .filter_map(|name| self.indexes.get(name))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get index statistics
    pub fn get_stats(&self, name: &str) -> Option<IndexStats> {
        let registered = self.indexes.get(name)?;
        let materialized = self.materialized.get(name);

        let (tuple_count, tombstone_count, valid, built_at, dimension) =
            materialized.map_or((0, 0, false, 0, 0), |m| {
                (
                    m.tuple_count,
                    m.index.tombstone_count(),
                    m.valid,
                    m.built_at,
                    m.index.dimension(),
                )
            });

        Some(IndexStats {
            name: registered.name.clone(),
            relation: registered.relation.clone(),
            column: registered.column_name.clone(),
            index_type: registered.index_type.type_name().to_string(),
            metric: match &registered.index_type {
                IndexType::Hnsw(config) => config.metric,
            },
            tuple_count,
            tombstone_count,
            valid,
            built_at,
            dimension,
        })
    }

    /// Get all index statistics
    pub fn get_all_stats(&self) -> Vec<IndexStats> {
        self.indexes
            .keys()
            .filter_map(|name| self.get_stats(name))
            .collect()
    }

    /// Get the number of registered indexes
    pub fn index_count(&self) -> usize {
        self.indexes.len()
    }

    /// Get the number of valid materialized indexes
    pub fn valid_count(&self) -> usize {
        self.materialized.values().filter(|m| m.valid).count()
    }

    // ── Index Persistence ───────────────────────────────────────────────

    /// Save all registered and materialized HNSW indexes to disk.
    /// Each index is saved as a subdirectory under `base_dir/indexes/`.
    pub fn save_indexes(&self, base_dir: &std::path::Path) -> Result<(), String> {
        use crate::hnsw_index::HnswIndex;

        let indexes_dir = base_dir.join("indexes");

        // Save registered index metadata
        let mut registrations: Vec<serde_json::Value> = Vec::new();
        for (name, reg) in &self.indexes {
            let index_type_str = match &reg.index_type {
                IndexType::Hnsw(config) => serde_json::json!({
                    "type": "hnsw",
                    "m": config.m,
                    "ef_construction": config.ef_construction,
                    "ef_search": config.ef_search,
                    "metric": format!("{:?}", config.metric).to_lowercase()
                }),
            };
            registrations.push(serde_json::json!({
                "name": name,
                "relation": reg.relation,
                "column_idx": reg.column_idx,
                "column_name": reg.column_name,
                "index_type": index_type_str
            }));
        }

        if registrations.is_empty() {
            // No indexes to save — clean up any stale index dir
            if indexes_dir.exists() {
                let _ = std::fs::remove_dir_all(&indexes_dir);
            }
            return Ok(());
        }

        std::fs::create_dir_all(&indexes_dir)
            .map_err(|e| format!("Failed to create indexes dir: {e}"))?;

        // Save registration metadata
        let meta_json = serde_json::to_string_pretty(&registrations)
            .map_err(|e| format!("Failed to serialize index metadata: {e}"))?;
        std::fs::write(indexes_dir.join("registrations.json"), meta_json)
            .map_err(|e| format!("Failed to write index metadata: {e}"))?;

        // Save materialized index data
        for (name, mat) in &self.materialized {
            if !mat.valid {
                continue; // Skip invalid indexes
            }
            // Downcast to HnswIndex for save
            if let Some(hnsw) = mat.index_arc.as_any().downcast_ref::<HnswIndex>() {
                let index_dir = indexes_dir.join(name);
                hnsw.save(&index_dir)?;
            }
        }

        Ok(())
    }

    /// Load registered indexes from disk and rebuild materialized HNSW structures.
    /// Returns the number of indexes loaded.
    pub fn load_indexes(&mut self, base_dir: &std::path::Path) -> Result<usize, String> {
        use crate::hnsw_index::HnswIndex;

        let indexes_dir = base_dir.join("indexes");
        let reg_path = indexes_dir.join("registrations.json");

        if !reg_path.exists() {
            return Ok(0);
        }

        let meta_json = std::fs::read_to_string(&reg_path)
            .map_err(|e| format!("Failed to read index metadata: {e}"))?;
        let registrations: Vec<serde_json::Value> = serde_json::from_str(&meta_json)
            .map_err(|e| format!("Failed to parse index metadata: {e}"))?;

        let mut loaded = 0;
        for reg in &registrations {
            let name = reg["name"]
                .as_str()
                .ok_or("Missing index name")?
                .to_string();
            let relation = reg["relation"]
                .as_str()
                .ok_or("Missing relation")?
                .to_string();
            let column_idx = reg["column_idx"].as_u64().ok_or("Missing column_idx")? as usize;
            let column_name = reg["column_name"]
                .as_str()
                .ok_or("Missing column_name")?
                .to_string();

            let it = &reg["index_type"];
            let metric = match it["metric"].as_str().unwrap_or("euclidean") {
                "cosine" => DistanceMetric::Cosine,
                "dotproduct" | "dot_product" => DistanceMetric::DotProduct,
                "manhattan" => DistanceMetric::Manhattan,
                _ => DistanceMetric::Euclidean,
            };
            let config = HnswConfig {
                m: it["m"].as_u64().unwrap_or(16) as usize,
                ef_construction: it["ef_construction"].as_u64().unwrap_or(100) as usize,
                ef_search: it["ef_search"].as_u64().unwrap_or(32) as usize,
                metric,
            };

            let index_type = IndexType::Hnsw(config);

            // Register the index
            let registered = RegisteredIndex {
                name: name.clone(),
                relation: relation.clone(),
                column_idx,
                column_name,
                index_type,
            };
            if self.register_index(registered).is_err() {
                continue; // Already registered (e.g., duplicate)
            }

            // Load materialized data if available
            let index_dir = indexes_dir.join(&name);
            if HnswIndex::persisted_exists(&index_dir) {
                match HnswIndex::load(&index_dir) {
                    Ok(hnsw) => {
                        let tuple_count = hnsw.len();
                        let mat = MaterializedIndex::new(
                            Box::new(hnsw),
                            std::collections::HashMap::new(),
                            tuple_count,
                        );
                        self.materialized.insert(name.clone(), mat);
                        loaded += 1;
                        tracing::info!(
                            index = name,
                            relation,
                            tuple_count,
                            "hnsw_index_loaded_from_persist"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            index = name,
                            error = %e,
                            "hnsw_index_load_failed"
                        );
                    }
                }
            }
        }

        Ok(loaded)
    }

    /// Get all registered indexes (for persistence).
    pub fn registered_indexes(&self) -> &HashMap<String, RegisteredIndex> {
        &self.indexes
    }
}

impl std::fmt::Debug for IndexManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexManager")
            .field("registered", &self.indexes.len())
            .field("materialized", &self.materialized.len())
            .field("valid", &self.valid_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock index for testing
    struct MockIndex {
        vectors: HashMap<TupleId, Vec<f32>>,
        tombstones: HashSet<TupleId>,
        metric: DistanceMetric,
    }

    impl MockIndex {
        fn new(metric: DistanceMetric) -> Self {
            Self {
                vectors: HashMap::new(),
                tombstones: HashSet::new(),
                metric,
            }
        }
    }

    impl Index for MockIndex {
        fn search(&self, query: &[f32], k: usize, _ef: Option<usize>) -> Vec<(TupleId, f64)> {
            // Simple linear search for testing
            let mut results: Vec<_> = self
                .vectors
                .iter()
                .filter(|(id, _)| !self.tombstones.contains(id))
                .map(|(id, vec)| {
                    let dist = vec
                        .iter()
                        .zip(query.iter())
                        .map(|(a, b)| (a - b).powi(2))
                        .sum::<f32>()
                        .sqrt() as f64;
                    (*id, dist)
                })
                .collect();
            results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            results.truncate(k);
            results
        }

        fn insert(&mut self, id: TupleId, vector: &[f32]) -> Result<(), String> {
            self.vectors.insert(id, vector.to_vec());
            Ok(())
        }

        fn delete(&mut self, id: TupleId) {
            self.tombstones.insert(id);
        }

        fn tombstone_ratio(&self) -> f64 {
            if self.vectors.is_empty() {
                0.0
            } else {
                self.tombstones.len() as f64 / self.vectors.len() as f64
            }
        }

        fn rebuild(&mut self, vectors: &[(TupleId, Vec<f32>)]) -> Result<(), String> {
            self.vectors.clear();
            self.tombstones.clear();
            for (id, vec) in vectors {
                self.vectors.insert(*id, vec.clone());
            }
            Ok(())
        }

        fn len(&self) -> usize {
            self.vectors.len()
        }

        fn index_type(&self) -> &str {
            "mock"
        }

        fn metric(&self) -> DistanceMetric {
            self.metric
        }

        fn tombstone_count(&self) -> usize {
            self.tombstones.len()
        }

        fn dimension(&self) -> usize {
            self.vectors.values().next().map(|v| v.len()).unwrap_or(0)
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    fn make_registered_index(name: &str, relation: &str, column_idx: usize) -> RegisteredIndex {
        RegisteredIndex {
            name: name.to_string(),
            relation: relation.to_string(),
            column_idx,
            column_name: format!("col{}", column_idx),
            index_type: IndexType::Hnsw(HnswConfig::default()),
        }
    }

    #[test]
    fn test_register_index() {
        let mut manager = IndexManager::new();

        let idx = make_registered_index("doc_emb_hnsw", "documents", 2);
        manager.register_index(idx).unwrap();

        assert!(manager.has_index("doc_emb_hnsw"));
        assert!(!manager.has_index("nonexistent"));
        assert!(manager.get_registered("doc_emb_hnsw").is_some());
    }

    #[test]
    fn test_register_duplicate_fails() {
        let mut manager = IndexManager::new();

        let idx = make_registered_index("my_index", "documents", 2);
        manager.register_index(idx.clone()).unwrap();

        let result = manager.register_index(idx);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already exists"));
    }

    #[test]
    fn test_remove_index() {
        let mut manager = IndexManager::new();

        let idx = make_registered_index("to_remove", "documents", 2);
        manager.register_index(idx).unwrap();

        assert!(manager.has_index("to_remove"));

        manager.remove_index("to_remove").unwrap();

        assert!(!manager.has_index("to_remove"));
    }

    #[test]
    fn test_remove_nonexistent_fails() {
        let mut manager = IndexManager::new();

        let result = manager.remove_index("nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn test_materialization() {
        let mut manager = IndexManager::new();

        let idx = make_registered_index("test_idx", "documents", 2);
        manager.register_index(idx).unwrap();

        // Initially not materialized
        assert!(manager.get_materialized("test_idx").is_none());

        // Materialize
        let mut mock = MockIndex::new(DistanceMetric::Cosine);
        mock.insert(0, &[1.0, 2.0, 3.0]).unwrap();
        mock.insert(1, &[4.0, 5.0, 6.0]).unwrap();

        manager.set_materialized("test_idx", Box::new(mock), 2);

        // Now available
        let mat = manager.get_materialized("test_idx").unwrap();
        assert!(mat.valid);
        assert_eq!(mat.tuple_count, 2);
    }

    #[test]
    fn test_invalidation() {
        let mut manager = IndexManager::new();

        let idx = make_registered_index("test_idx", "documents", 2);
        manager.register_index(idx).unwrap();

        // Materialize
        let mock = MockIndex::new(DistanceMetric::Cosine);
        manager.set_materialized("test_idx", Box::new(mock), 0);

        assert!(manager.get_materialized("test_idx").is_some());

        // Notify base update
        let invalidated = manager.notify_base_update("documents");
        assert_eq!(invalidated, vec!["test_idx"]);

        // Now invalid (get_materialized returns None for invalid)
        assert!(manager.get_materialized("test_idx").is_none());
    }

    #[test]
    fn test_multiple_indexes_same_relation() {
        let mut manager = IndexManager::new();

        let idx1 = make_registered_index("idx1", "documents", 2);
        let idx2 = make_registered_index("idx2", "documents", 3);
        manager.register_index(idx1).unwrap();
        manager.register_index(idx2).unwrap();

        // Materialize both
        manager.set_materialized("idx1", Box::new(MockIndex::new(DistanceMetric::Cosine)), 0);
        manager.set_materialized(
            "idx2",
            Box::new(MockIndex::new(DistanceMetric::Euclidean)),
            0,
        );

        // Update base relation - both should be invalidated
        let invalidated = manager.notify_base_update("documents");
        assert_eq!(invalidated.len(), 2);
        assert!(invalidated.contains(&"idx1".to_string()));
        assert!(invalidated.contains(&"idx2".to_string()));
    }

    #[test]
    fn test_get_indexes_for_relation() {
        let mut manager = IndexManager::new();

        manager
            .register_index(make_registered_index("idx1", "docs", 2))
            .unwrap();
        manager
            .register_index(make_registered_index("idx2", "docs", 3))
            .unwrap();
        manager
            .register_index(make_registered_index("idx3", "other", 1))
            .unwrap();

        let doc_indexes = manager.get_indexes_for_relation("docs");
        assert_eq!(doc_indexes.len(), 2);

        let other_indexes = manager.get_indexes_for_relation("other");
        assert_eq!(other_indexes.len(), 1);

        let empty = manager.get_indexes_for_relation("nonexistent");
        assert!(empty.is_empty());
    }

    #[test]
    fn test_stats() {
        let mut manager = IndexManager::new();

        let idx = make_registered_index("test_idx", "documents", 2);
        manager.register_index(idx).unwrap();

        // Before materialization
        let stats = manager.get_stats("test_idx").unwrap();
        assert_eq!(stats.name, "test_idx");
        assert_eq!(stats.relation, "documents");
        assert!(!stats.valid);
        assert_eq!(stats.tuple_count, 0);

        // After materialization
        let mut mock = MockIndex::new(DistanceMetric::Euclidean);
        mock.insert(0, &[1.0, 2.0]).unwrap();
        mock.insert(1, &[3.0, 4.0]).unwrap();
        manager.set_materialized("test_idx", Box::new(mock), 2);

        let stats = manager.get_stats("test_idx").unwrap();
        assert!(stats.valid);
        assert_eq!(stats.tuple_count, 2);
        assert_eq!(stats.dimension, 2);
    }

    #[test]
    fn test_get_all_valid_indexes() {
        let mut manager = IndexManager::new();

        manager
            .register_index(make_registered_index("idx1", "docs", 2))
            .unwrap();
        manager
            .register_index(make_registered_index("idx2", "docs", 3))
            .unwrap();

        // Materialize both
        manager.set_materialized("idx1", Box::new(MockIndex::new(DistanceMetric::Cosine)), 0);
        manager.set_materialized("idx2", Box::new(MockIndex::new(DistanceMetric::Cosine)), 0);

        let valid = manager.get_all_valid_indexes();
        assert_eq!(valid.len(), 2);

        // Invalidate one
        manager.notify_base_update("docs");

        let valid = manager.get_all_valid_indexes();
        assert_eq!(valid.len(), 0);
    }

    #[test]
    fn test_invalid_indexes_list() {
        let mut manager = IndexManager::new();

        manager
            .register_index(make_registered_index("idx1", "docs", 2))
            .unwrap();
        manager
            .register_index(make_registered_index("idx2", "other", 3))
            .unwrap();

        // Both are invalid (not materialized)
        let invalid = manager.get_invalid_indexes();
        assert_eq!(invalid.len(), 2);

        // Materialize one
        manager.set_materialized("idx1", Box::new(MockIndex::new(DistanceMetric::Cosine)), 0);

        let invalid = manager.get_invalid_indexes();
        assert_eq!(invalid.len(), 1);
        assert!(invalid.contains(&"idx2".to_string()));
    }

    #[test]
    fn test_distance_metric_parsing() {
        assert_eq!(
            "cosine".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::Cosine
        );
        assert_eq!(
            "l2".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::Euclidean
        );
        assert_eq!(
            "euclidean".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::Euclidean
        );
        assert_eq!(
            "dot".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::DotProduct
        );
        assert_eq!(
            "l1".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::Manhattan
        );
        assert_eq!(
            "manhattan".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::Manhattan
        );

        assert!("invalid".parse::<DistanceMetric>().is_err());
    }

    #[test]
    fn test_mock_index_search() {
        let mut index = MockIndex::new(DistanceMetric::Euclidean);
        index.insert(0, &[0.0, 0.0]).unwrap();
        index.insert(1, &[1.0, 0.0]).unwrap();
        index.insert(2, &[0.0, 1.0]).unwrap();
        index.insert(3, &[1.0, 1.0]).unwrap();

        // Search near origin
        let results = index.search(&[0.1, 0.1], 2, None);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 0); // Closest is origin
    }

    #[test]
    fn test_mock_index_tombstones() {
        let mut index = MockIndex::new(DistanceMetric::Euclidean);
        index.insert(0, &[0.0, 0.0]).unwrap();
        index.insert(1, &[1.0, 0.0]).unwrap();

        assert_eq!(index.len(), 2);
        assert_eq!(index.tombstone_count(), 0);
        assert_eq!(index.tombstone_ratio(), 0.0);

        index.delete(0);

        assert_eq!(index.len(), 2); // Still 2 (tombstone doesn't remove)
        assert_eq!(index.tombstone_count(), 1);
        assert_eq!(index.tombstone_ratio(), 0.5);

        // Search should skip tombstoned entry
        let results = index.search(&[0.0, 0.0], 2, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
    }

    // === Additional Coverage ===

    #[test]
    fn test_distance_metric_display() {
        assert_eq!(DistanceMetric::Cosine.to_string(), "cosine");
        assert_eq!(DistanceMetric::Euclidean.to_string(), "l2");
        assert_eq!(DistanceMetric::DotProduct.to_string(), "dot");
        assert_eq!(DistanceMetric::Manhattan.to_string(), "l1");
    }

    #[test]
    fn test_distance_metric_default() {
        assert_eq!(DistanceMetric::default(), DistanceMetric::Cosine);
    }

    #[test]
    fn test_distance_metric_all_aliases() {
        // Test all parsing aliases
        assert_eq!(
            "cos".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::Cosine
        );
        assert_eq!(
            "euclid".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::Euclidean
        );
        assert_eq!(
            "dotproduct".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::DotProduct
        );
        assert_eq!(
            "dot_product".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::DotProduct
        );
        assert_eq!(
            "inner".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::DotProduct
        );
        assert_eq!(
            "taxicab".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::Manhattan
        );
    }

    #[test]
    fn test_distance_metric_case_insensitive() {
        assert_eq!(
            "COSINE".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::Cosine
        );
        assert_eq!(
            "L2".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::Euclidean
        );
    }

    #[test]
    fn test_hnsw_config_default() {
        let config = HnswConfig::default();
        assert_eq!(config.m, 16);
        assert_eq!(config.ef_construction, 200);
        assert_eq!(config.ef_search, 50);
        assert_eq!(config.metric, DistanceMetric::Cosine);
    }

    #[test]
    fn test_index_type_name() {
        let idx_type = IndexType::Hnsw(HnswConfig::default());
        assert_eq!(idx_type.type_name(), "hnsw");
    }

    #[test]
    fn test_index_manager_new_is_empty() {
        let manager = IndexManager::new();
        assert!(manager.get_all_valid_indexes().is_empty());
        assert!(manager.get_invalid_indexes().is_empty());
    }

    #[test]
    fn test_notify_base_update_no_indexes() {
        let mut manager = IndexManager::new();
        let invalidated = manager.notify_base_update("nonexistent");
        assert!(invalidated.is_empty());
    }

    #[test]
    fn test_mock_index_is_empty() {
        let index = MockIndex::new(DistanceMetric::Cosine);
        assert!(index.is_empty());
        assert_eq!(index.index_type(), "mock");
        assert_eq!(index.dimension(), 0);
    }

    #[test]
    fn test_mock_index_dimension() {
        let mut index = MockIndex::new(DistanceMetric::Cosine);
        index.insert(0, &[1.0, 2.0, 3.0]).unwrap();
        assert_eq!(index.dimension(), 3);
    }

    #[test]
    fn test_stats_nonexistent() {
        let manager = IndexManager::new();
        assert!(manager.get_stats("nonexistent").is_none());
    }

    #[test]
    fn test_mock_index_rebuild() {
        let mut index = MockIndex::new(DistanceMetric::Euclidean);
        index.insert(0, &[0.0, 0.0]).unwrap();
        index.insert(1, &[1.0, 0.0]).unwrap();
        index.delete(0);

        assert_eq!(index.tombstone_count(), 1);

        // Rebuild with only the non-deleted entry
        index.rebuild(&[(1, vec![1.0, 0.0])]).unwrap();

        assert_eq!(index.len(), 1);
        assert_eq!(index.tombstone_count(), 0);
    }
}
