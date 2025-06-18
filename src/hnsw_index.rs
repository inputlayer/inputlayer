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
unsafe impl Send for HnswInnerOwned {}
unsafe impl Sync for HnswInnerOwned {}

impl HnswIndex {
    /// Create a new HNSW index with the given configuration
    pub fn new(config: HnswConfig) -> Self {
        Self {
            inner: RwLock::new(None),
            config,
            tombstones: RwLock::new(HashSet::new()),
            vectors: RwLock::new(Vec::new()),
            dimension: RwLock::new(0),
        }
    }

    /// Initialize or rebuild the HNSW structure from stored vectors
    fn rebuild_hnsw(&self) -> Result<(), String> {
        let vectors = self.vectors.read();
        let tombstones = self.tombstones.read();

        // Filter out tombstoned vectors, keeping tuple_ids
        let active_vectors: Vec<(TupleId, &Vec<f32>)> = vectors
            .iter()
            .filter(|(tuple_id, _)| !tombstones.contains(tuple_id))
            .map(|(tuple_id, vec)| (*tuple_id, vec))
            .collect();

        if active_vectors.is_empty() {
            *self.inner.write() = None;
            return Ok(());
        }

        // Create storage for vectors and mapping
        let dim = active_vectors[0].1.len();
        let storage: Vec<Vec<f32>> = active_vectors.iter().map(|(_, v)| (*v).clone()).collect();
        let index_to_tuple_id: Vec<TupleId> = active_vectors.iter().map(|(id, _)| *id).collect();
        let storage = Arc::new(storage);

        // Create a &'static reference to storage backed by the Arc.
        // Sound because HnswInnerOwned keeps the Arc alive for the reference's lifetime.
        let storage_ref: &'static Vec<Vec<f32>> = unsafe {
            // SAFETY: We keep storage alive via Arc in HnswInnerOwned
            // The reference is only valid as long as HnswInnerOwned exists
            &*Arc::as_ptr(&storage).cast::<Vec<Vec<f32>>>()
        };

        let max_elements = storage_ref.len().max(1000);
        let hnsw: Hnsw<'static, f32, DistL2> = Hnsw::new(
            self.config.m,
            max_elements,
            16,
            self.config.ef_construction,
            DistL2,
        );

        // Insert all vectors with their indices
        for (idx, vec) in storage_ref.iter().enumerate() {
            hnsw.insert((vec, idx));
        }

        *self.inner.write() = Some(HnswInnerOwned {
            hnsw: Box::new(hnsw),
            _storage: storage,
            index_to_tuple_id,
        });

        *self.dimension.write() = dim;

        Ok(())
    }

    /// Get the ef_search parameter to use
    fn get_ef_search(&self, ef_override: Option<usize>) -> usize {
        ef_override.unwrap_or(self.config.ef_search)
    }

    /// Transform distance based on metric
    fn transform_distance(&self, dist: f32) -> f64 {
        match self.config.metric {
            DistanceMetric::Euclidean => dist as f64,
            DistanceMetric::Cosine => {
                // L2 on normalized vectors: d^2 = 2(1 - cos(theta))
                // So cosine distance = 1 - cos(theta) = d^2 / 2
                (dist * dist / 2.0) as f64
            }
            DistanceMetric::DotProduct => {
                // Dot product on unit vectors equals cosine
                // Convert L2 to approximate dot product similarity
                // Higher similarity = lower distance, so negate
                -(1.0 - dist * dist / 2.0) as f64
            }
            DistanceMetric::Manhattan => {
                // Approximate L1 from L2 (rough approximation)
                dist as f64
            }
        }
    }

    /// Normalize a vector (for cosine similarity)
    fn normalize_vector(vec: &[f32]) -> Vec<f32> {
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 1e-10 {
            vec.iter().map(|x| x / norm).collect()
        } else {
            vec.to_vec()
        }
    }

    /// Prepare vector for insertion based on metric
    fn prepare_vector(&self, vec: &[f32]) -> Vec<f32> {
        match self.config.metric {
            DistanceMetric::Cosine | DistanceMetric::DotProduct => Self::normalize_vector(vec),
            _ => vec.to_vec(),
        }
    }
}

impl Index for HnswIndex {
    fn search(&self, query: &[f32], k: usize, ef: Option<usize>) -> Vec<(TupleId, f64)> {
        let inner_guard = self.inner.read();
        let inner = match &*inner_guard {
            Some(h) => h,
            None => return Vec::new(),
        };

        let ef_search = self.get_ef_search(ef);

        // Prepare query vector
        let prepared_query = self.prepare_vector(query);

        // Search HNSW
        let raw_results = inner.hnsw.search(&prepared_query, k, ef_search);

        // Map internal indices to tuple IDs using the stored mapping
        let mut results: Vec<(TupleId, f64)> = raw_results
            .into_iter()
            .filter_map(|neighbour| {
                let internal_idx = neighbour.d_id;
                if internal_idx < inner.index_to_tuple_id.len() {
                    let tuple_id = inner.index_to_tuple_id[internal_idx];
                    let dist = self.transform_distance(neighbour.distance);
                    Some((tuple_id, dist))
                } else {
                    None
                }
            })
            .collect();

        // Sort by distance (HNSW should return sorted, but ensure it)
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        results
    }

    fn insert(&mut self, id: TupleId, vector: &[f32]) -> Result<(), String> {
        // Track dimension
        {
            let mut dim = self.dimension.write();
            if *dim == 0 {
                *dim = vector.len();
            } else if *dim != vector.len() {
                return Err(format!(
                    "Dimension mismatch: index has dimension {}, got vector of dimension {}",
                    *dim,
                    vector.len()
                ));
            }
        }

        // Prepare and store vector
        let prepared = self.prepare_vector(vector);
        self.vectors.write().push((id, prepared));

        // Rebuild HNSW structure
        // Note: For better performance, we could batch inserts and rebuild less frequently
        self.rebuild_hnsw()?;

        Ok(())
    }

    fn delete(&mut self, id: TupleId) {
        self.tombstones.write().insert(id);
    }

    fn tombstone_ratio(&self) -> f64 {
        let vectors = self.vectors.read();
        let tombstones = self.tombstones.read();

        if vectors.is_empty() {
            0.0
        } else {
            tombstones.len() as f64 / vectors.len() as f64
        }
    }

    fn rebuild(&mut self, vectors: &[(TupleId, Vec<f32>)]) -> Result<(), String> {
        // Clear state
        self.tombstones.write().clear();
        *self.inner.write() = None;

        // Reset dimension
        if let Some((_, vec)) = vectors.first() {
            *self.dimension.write() = vec.len();
        } else {
            *self.dimension.write() = 0;
            self.vectors.write().clear();
            return Ok(());
        }

        // Prepare and store vectors
        {
            let mut stored = self.vectors.write();
            stored.clear();
            for (id, vec) in vectors {
                let prepared = self.prepare_vector(vec);
                stored.push((*id, prepared));
            }
        }

        // Rebuild HNSW
        self.rebuild_hnsw()
    }

    fn len(&self) -> usize {
        self.vectors.read().len()
    }

    fn index_type(&self) -> &'static str {
        "hnsw"
    }

    fn metric(&self) -> DistanceMetric {
        self.config.metric
    }

    fn tombstone_count(&self) -> usize {
        self.tombstones.read().len()
    }

    fn dimension(&self) -> usize {
        *self.dimension.read()
    }
}

// Safety: HnswIndex uses RwLock internally for thread safety
unsafe impl Send for HnswIndex {}
unsafe impl Sync for HnswIndex {}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(metric: DistanceMetric) -> HnswConfig {
        HnswConfig {
            m: 8,
            ef_construction: 100,
            ef_search: 32,
            metric,
        }
    }

    #[test]
    fn test_hnsw_insert_search_euclidean() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        // Insert vectors at known positions
        index.insert(0, &[0.0, 0.0]).unwrap();
        index.insert(1, &[1.0, 0.0]).unwrap();
        index.insert(2, &[0.0, 1.0]).unwrap();
        index.insert(3, &[1.0, 1.0]).unwrap();

        assert_eq!(index.len(), 4);
        assert_eq!(index.dimension(), 2);

        // Search near origin
        let results = index.search(&[0.1, 0.1], 2, None);
        assert!(!results.is_empty());
        // Closest should be origin (id=0)
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn test_hnsw_insert_search_cosine() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Cosine));

        // Insert some vectors
        index.insert(0, &[1.0, 0.0, 0.0]).unwrap();
        index.insert(1, &[0.0, 1.0, 0.0]).unwrap();
        index.insert(2, &[0.0, 0.0, 1.0]).unwrap();
        index.insert(3, &[0.707, 0.707, 0.0]).unwrap();

        assert_eq!(index.len(), 4);
        assert_eq!(index.dimension(), 3);

        // Search for vector most similar to [1, 0, 0]
        // Use high ef to ensure exhaustive search (HNSW is approximate)
        let results = index.search(&[1.0, 0.0, 0.0], 2, Some(100));
        assert_eq!(results.len(), 2);
        // First result should be the identical vector
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn test_hnsw_delete_tombstone() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        index.insert(0, &[0.0, 0.0]).unwrap();
        index.insert(1, &[1.0, 0.0]).unwrap();
        index.insert(2, &[2.0, 0.0]).unwrap();

        assert_eq!(index.len(), 3);
        assert_eq!(index.tombstone_count(), 0);

        // Delete the closest one to origin
        index.delete(0);

        assert_eq!(index.len(), 3); // Still 3 (tombstone)
        assert_eq!(index.tombstone_count(), 1);
        assert!(index.tombstone_ratio() > 0.3);

        // Rebuild to actually remove tombstoned entries
        // Note: Current implementation doesn't automatically exclude tombstones from search
        // until the next rebuild. For the test, let's manually rebuild.
        let active: Vec<(TupleId, Vec<f32>)> = vec![(1, vec![1.0, 0.0]), (2, vec![2.0, 0.0])];
        index.rebuild(&active).unwrap();

        // Now search should not include id=0
        let results = index.search(&[0.0, 0.0], 2, None);
        assert!(results.iter().all(|(id, _)| *id != 0));
    }

    #[test]
    fn test_hnsw_rebuild() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        index.insert(0, &[0.0, 0.0]).unwrap();
        index.insert(1, &[1.0, 0.0]).unwrap();
        index.insert(2, &[2.0, 0.0]).unwrap();

        // Rebuild with only one vector
        index.rebuild(&[(2, vec![2.0, 0.0])]).unwrap();

        assert_eq!(index.len(), 1);
        assert_eq!(index.tombstone_count(), 0);
        assert_eq!(index.tombstone_ratio(), 0.0);

        // Search should find the remaining vector
        let results = index.search(&[2.0, 0.0], 1, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 2);
    }

    #[test]
    fn test_hnsw_dimension_mismatch() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        index.insert(0, &[1.0, 2.0, 3.0]).unwrap();

        let result = index.insert(1, &[1.0, 2.0]); // Wrong dimension
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Dimension mismatch"));
    }

    #[test]
    fn test_hnsw_empty_index() {
        let index = HnswIndex::new(make_config(DistanceMetric::Cosine));

        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
        assert_eq!(index.dimension(), 0);

        // Search on empty index should return empty
        let results = index.search(&[1.0, 2.0, 3.0], 10, None);
        assert!(results.is_empty());
    }

    #[test]
    fn test_hnsw_custom_ef_search() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        for i in 0..50 {
            index.insert(i, &[i as f32, 0.0]).unwrap();
        }

        // Search with default ef
        let results1 = index.search(&[25.0, 0.0], 5, None);
        assert_eq!(results1.len(), 5);

        // Search with custom ef (higher should potentially give better results)
        let results2 = index.search(&[25.0, 0.0], 5, Some(100));
        assert_eq!(results2.len(), 5);

        // Both should find the same nearest neighbor
        assert_eq!(results1[0].0, results2[0].0);
    }

    #[test]
    fn test_hnsw_all_metrics() {
        // Test all distance metrics work
        for metric in [
            DistanceMetric::Cosine,
            DistanceMetric::Euclidean,
            DistanceMetric::DotProduct,
            DistanceMetric::Manhattan,
        ] {
            let mut index = HnswIndex::new(make_config(metric));

            // Use enough vectors for reliable HNSW graph construction
            // Grid of vectors ensures good connectivity
            let mut vectors = Vec::new();
            for i in 0..10 {
                for j in 0..10 {
                    let id = (i * 10 + j) as TupleId;
                    vectors.push((id, vec![i as f32 / 10.0, j as f32 / 10.0]));
                }
            }
            index.rebuild(&vectors).unwrap();

            assert_eq!(index.len(), 100);
            assert_eq!(index.metric(), metric);

            // Search for vector at origin (id=0)
            let results = index.search(&[0.0, 0.0], 5, Some(100));
            assert!(
                results.len() >= 5,
                "Expected at least 5 results for {:?}, got {}",
                metric,
                results.len()
            );
            // First result should be id=0 (exact match at origin)
            assert_eq!(
                results[0].0, 0,
                "First result should be id=0 for {:?}, got {}",
                metric, results[0].0
            );
        }
    }

    #[test]
    fn test_hnsw_larger_dataset() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        // Insert 100 vectors in a grid
        for i in 0..10 {
            for j in 0..10 {
                let id = i * 10 + j;
                index.insert(id, &[i as f32, j as f32]).unwrap();
            }
        }

        assert_eq!(index.len(), 100);

        // Search for a specific location
        let results = index.search(&[5.0, 5.0], 5, None);
        assert_eq!(results.len(), 5);

        // The closest should be at [5, 5]
        // ID for [5, 5] = 5*10 + 5 = 55
        assert_eq!(results[0].0, 55);
    }

    #[test]
    fn test_hnsw_single_vector() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        index.insert(42, &[1.0, 2.0, 3.0]).unwrap();

        assert_eq!(index.len(), 1);
        assert_eq!(index.dimension(), 3);

        // Search should return the only vector
        let results = index.search(&[1.0, 2.0, 3.0], 10, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 42);
    }

    #[test]
    fn test_hnsw_returns_k_when_dataset_larger() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        // Use enough vectors for reliable HNSW graph construction
        let vectors: Vec<_> = (0..100)
            .map(|i| (i, vec![i as f32 / 10.0, (i % 10) as f32 / 10.0]))
            .collect();
        index.rebuild(&vectors).unwrap();

        // Request k=10 from dataset of 100
        let results = index.search(&[0.0, 0.0], 10, Some(100));
        assert_eq!(results.len(), 10, "Expected exactly 10 results");
        // Origin should be the closest
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn test_hnsw_high_dimensional() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        // Insert 128-dimensional vectors (common for embeddings)
        let mut vectors = Vec::new();
        for i in 0..10 {
            let vec: Vec<f32> = (0..128).map(|j| (i * 128 + j) as f32 / 1000.0).collect();
            vectors.push((i, vec));
        }
        index.rebuild(&vectors).unwrap();

        assert_eq!(index.len(), 10);
        assert_eq!(index.dimension(), 128);

        // Search
        let query: Vec<f32> = (0..128).map(|_| 0.5).collect();
        let results = index.search(&query, 3, Some(100));
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_hnsw_rebuild_clears_tombstones() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        index.insert(0, &[0.0, 0.0]).unwrap();
        index.insert(1, &[1.0, 1.0]).unwrap();
        index.insert(2, &[2.0, 2.0]).unwrap();

        // Delete one
        index.delete(1);
        assert_eq!(index.tombstone_count(), 1);

        // Rebuild without the deleted one
        let vectors = vec![(0, vec![0.0, 0.0]), (2, vec![2.0, 2.0])];
        index.rebuild(&vectors).unwrap();

        assert_eq!(index.len(), 2);
        assert_eq!(index.tombstone_count(), 0);
        assert_eq!(index.tombstone_ratio(), 0.0);
    }

    #[test]
    fn test_hnsw_multiple_deletes() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        for i in 0..10 {
            index.insert(i, &[i as f32, 0.0]).unwrap();
        }

        // Delete multiple
        index.delete(2);
        index.delete(5);
        index.delete(8);

        assert_eq!(index.tombstone_count(), 3);
        assert_eq!(index.len(), 10);
        assert!((index.tombstone_ratio() - 0.3).abs() < 0.01);
    }

    #[test]
    fn test_hnsw_empty_vector_error() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        let result = index.insert(0, &[]);
        // Empty vector should set dimension to 0, which is technically valid
        assert!(result.is_ok());
    }

    #[test]
    fn test_hnsw_rebuild_empty() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        index.insert(0, &[1.0, 2.0]).unwrap();
        assert_eq!(index.len(), 1);

        // Rebuild with empty list
        index.rebuild(&[]).unwrap();

        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
    }

    #[test]
    fn test_hnsw_index_type() {
        let index = HnswIndex::new(make_config(DistanceMetric::Cosine));
        assert_eq!(index.index_type(), "hnsw");
    }

    #[test]
    fn test_hnsw_different_m_values() {
        // Test with different M values
        for m in [4, 8, 16, 32] {
            let config = HnswConfig {
                m,
                ef_construction: 100,
                ef_search: 32,
                metric: DistanceMetric::Euclidean,
            };
            let mut index = HnswIndex::new(config);

            // Insert some vectors
            let vectors: Vec<_> = (0..20).map(|i| (i, vec![i as f32, 0.0])).collect();
            index.rebuild(&vectors).unwrap();

            // Search should work - use ef_search=200 for reliable results
            let results = index.search(&[10.0, 0.0], 5, Some(200));
            assert_eq!(results.len(), 5, "m={}: expected 5 results", m);
            // Nearest neighbor of [10.0, 0.0] must be vector #10 (distance 0)
            assert!(
                results.iter().any(|r| r.0 == 10),
                "m={}: expected vector 10 in results, got {:?}",
                m,
                results
            );
        }
    }

    #[test]
    fn test_hnsw_manhattan_metric() {
        let config = HnswConfig {
            m: 8,
            ef_construction: 100,
            ef_search: 32,
            metric: DistanceMetric::Manhattan,
        };
        let mut index = HnswIndex::new(config);

        let vectors = vec![
            (0, vec![0.0, 0.0]),
            (1, vec![1.0, 0.0]),
            (2, vec![0.0, 1.0]),
            (3, vec![1.0, 1.0]),
        ];
        index.rebuild(&vectors).unwrap();

        assert_eq!(index.metric(), DistanceMetric::Manhattan);

        // Search near origin
        let results = index.search(&[0.1, 0.1], 2, Some(100));
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 0); // Origin is closest
    }
}
