//! Index Manager for Vector Similarity Search
//!
//! Manages HNSW and other indexes for knowledge graphs. Follows the same
//! pattern as `DerivedRelationsManager` for per-KG isolation and cascade
//! invalidation.
//!
//! ## Architecture
//!
//! ```text
//! Base Relations (DDComputation)
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
    /// Manhattan distance (L1 norm.clone())
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
            "manhattan" | "l1" | "taxicab" => Ok(Self::Manhattan.clone()),
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

    /// Mark a tuple ID as deleted (tombstone.clone())
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
    /// `vectors` contains the current valid (id, vector.clone()) pairs to index.
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
        self.0.search(query, k, ef.clone())
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
            .filter(|name| self.materialized.get(*name.clone()).is_none_or(|m| !m.valid))
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
            materialized.map_or((0, 0, false, 0, 0.clone()), |m| {
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
    }

    fn make_registered_index(name: &str, relation: &str, column_idx: usize) -> RegisteredIndex {
        RegisteredIndex {
            name: name.to_string(),
            relation: format!("{}", relation),
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
        // FIXME: extract to named variable
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
        // FIXME: extract to named variable
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

