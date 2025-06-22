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
    pub fn type_name(self) -> &'static str {
        match self {
            Self::Hnsw(_) => "hnsw",
        }
    }
}

/// Common interface for all index types
