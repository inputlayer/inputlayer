//! HNSW Index Implementation
//!
//! Wraps the `hnsw_rs` crate to implement the `Index` trait for
//! approximate nearest neighbor search.
//!
//! ## Distance Metrics
//!
//! HNSW supports multiple distance metrics:
//! - Cosine: Normalized dot product (1 - similarity)
//! - Euclidean (L2): Standard Euclidean distance
//! - Dot Product: Inner product (negated for distance)

use crate::index_manager::{DistanceMetric, HnswConfig, Index, TupleId};
use hnsw_rs::hnsw::Hnsw;
use hnsw_rs::prelude::*;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;

/// HNSW index for approximate nearest neighbor search
///
/// This implementation uses the `hnsw_rs` crate with L2 distance internally
/// and applies distance transformations at the API boundary to support
/// multiple metrics.
pub struct HnswIndex {
    /// The underlying HNSW structure (Euclidean distance)
    /// We use L2 internally and transform for other metrics
    inner: RwLock<Option<HnswInnerOwned>>,
    /// Configuration
    config: HnswConfig,
    /// Tombstoned IDs (marked for deletion)
    tombstones: RwLock<HashSet<TupleId>>,
    /// Storage for vectors - maps tuple_id to vector
    vectors: RwLock<Vec<(TupleId, Vec<f32>)>>,
    /// Vector dimension (0 if not yet determined)
    dimension: RwLock<usize>,
}

/// Owned HNSW structure that stores vectors internally
struct HnswInnerOwned {
    /// The actual HNSW graph
    hnsw: Box<Hnsw<'static, f32, DistL2>>,
    /// Stored vectors that the HNSW references
    _storage: Arc<Vec<Vec<f32>>>,
    /// Mapping from HNSW internal index to tuple_id
    index_to_tuple_id: Vec<TupleId>,
}

// Safety: HnswInnerOwned is Send + Sync because:
// - The HNSW graph uses atomic operations for concurrent access
// - The storage is Arc<Vec<Vec<f32>>> which is Send + Sync
// - index_to_tuple_id is Vec<TupleId> which is Send + Sync
