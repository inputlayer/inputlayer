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

