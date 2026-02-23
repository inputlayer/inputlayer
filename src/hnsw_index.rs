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
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
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
        //
        // SAFETY: This transmute extends the borrow lifetime to 'static. This is sound because:
        // 1. `storage` (the Arc) is moved into `HnswInnerOwned._storage`, keeping refcount >= 1
        // 2. `HnswInnerOwned` is stored inside `self.inner` (behind a RwLock)
        // 3. The HNSW graph (which holds this reference) is dropped BEFORE `_storage` because
        //    Rust drops struct fields in declaration order: `hnsw` (uses ref) then `_storage` (owns data)
        // 4. No code path clones or moves `_storage` out of HnswInnerOwned
        // 5. The RwLock write guard in rebuild_hnsw() replaces the entire Option<HnswInnerOwned>,
        //    dropping old graph + storage atomically
        let storage_ref: &'static Vec<Vec<f32>> =
            unsafe { &*Arc::as_ptr(&storage).cast::<Vec<Vec<f32>>>() };

        let max_elements = storage_ref.len();
        // Scale max_layer to dataset size: log_M(N), clamped to [4, 16]
        let max_layer = if storage_ref.len() <= 1 {
            4
        } else {
            let m = (self.config.m as f64).max(2.0);
            let n = storage_ref.len() as f64;
            let layers = (n.ln() / m.ln()).ceil() as usize;
            layers.clamp(4, 16)
        };
        let mut hnsw: Hnsw<'static, f32, DistL2> = Hnsw::new(
            self.config.m,
            max_elements,
            max_layer,
            self.config.ef_construction,
            DistL2,
        );
        // Keep pruned connections to prevent graph disconnection.
        // Without this, Navarro's heuristic can over-prune small-to-medium
        // datasets, causing search to return fewer than k results.
        hnsw.set_keeping_pruned(true);
        hnsw.set_extend_candidates(true);
        // Flatten the graph: reduce level scale to keep more nodes at lower
        // layers, improving connectivity for small-to-medium datasets.
        hnsw.modify_level_scale(0.2);

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

    /// Transform raw L2 distance from hnsw_rs into the user-facing metric.
    ///
    /// hnsw_rs `DistL2.eval()` returns actual L2 distance (not squared):
    ///   dist = sqrt(sum((a_i - b_i)^2))
    fn transform_distance(&self, dist: f32) -> f64 {
        match self.config.metric {
            DistanceMetric::Euclidean => dist as f64,
            DistanceMetric::Cosine => {
                // Vectors are pre-normalized to unit length before insertion.
                // For unit vectors: L2^2 = |a-b|^2 = 2(1 - cos(θ))
                // Therefore: cosine_distance = 1 - cos(θ) = L2^2 / 2 = dist^2 / 2
                (dist * dist / 2.0) as f64
            }
            DistanceMetric::DotProduct => {
                // For unit vectors: dot(a,b) = cos(θ) = 1 - L2^2/2 = 1 - dist^2/2
                // Negate so lower = more similar (consistent with distance semantics)
                -(1.0 - dist * dist / 2.0) as f64
            }
            DistanceMetric::Manhattan => {
                // Manhattan distance is recomputed from stored vectors in search(),
                // so this branch is only a fallback (should not be reached).
                dist as f64
            }
        }
    }

    /// Compute actual L1 (Manhattan) distance between two vectors.
    fn manhattan_distance(a: &[f32], b: &[f32]) -> f64 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (*x as f64 - *y as f64).abs())
            .sum()
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
        let is_manhattan = matches!(self.config.metric, DistanceMetric::Manhattan);

        // Prepare query vector
        let prepared_query = self.prepare_vector(query);

        // For Manhattan, request more candidates since L2 ordering != L1 ordering.
        // Reranking from a larger candidate set improves recall.
        let search_k = if is_manhattan { k * 4 } else { k };
        let raw_results = inner.hnsw.search(&prepared_query, search_k, ef_search);

        // Map internal indices to tuple IDs using the stored mapping
        let mut results: Vec<(TupleId, f64)> = if is_manhattan {
            // Recompute actual L1 distance from stored vectors
            let vectors = self.vectors.read();
            raw_results
                .into_iter()
                .filter_map(|neighbour| {
                    let internal_idx = neighbour.d_id;
                    if internal_idx < inner.index_to_tuple_id.len() {
                        let tuple_id = inner.index_to_tuple_id[internal_idx];
                        // Find the stored vector for this tuple_id
                        if let Some((_, stored_vec)) =
                            vectors.iter().find(|(id, _)| *id == tuple_id)
                        {
                            let dist = Self::manhattan_distance(&prepared_query, stored_vec);
                            Some((tuple_id, dist))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            raw_results
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
                .collect()
        };

        // Sort by distance and take top-k (important for Manhattan reranking)
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(k);

        results
    }

    fn insert(&mut self, id: TupleId, vector: &[f32]) -> Result<(), String> {
        // Reject empty vectors
        if vector.is_empty() {
            return Err("Cannot insert empty vector into HNSW index".to_string());
        }

        // Reject zero-norm vectors for metrics that require normalization
        if matches!(
            self.config.metric,
            DistanceMetric::Cosine | DistanceMetric::DotProduct
        ) {
            let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm <= 1e-10 {
                return Err(
                    "Cannot insert zero-norm vector for cosine/dot product metric".to_string(),
                );
            }
        }

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

        // Check for duplicate ID and update in place if found
        {
            let mut vectors = self.vectors.write();
            if let Some(pos) = vectors
                .iter()
                .position(|(existing_id, _)| *existing_id == id)
            {
                let prepared = self.prepare_vector(vector);
                vectors[pos] = (id, prepared);
            } else {
                let prepared = self.prepare_vector(vector);
                vectors.push((id, prepared));
            }
        }

        // Rebuild HNSW structure
        // Note: For better performance, we could batch inserts and rebuild less frequently
        self.rebuild_hnsw()?;

        Ok(())
    }

    fn insert_batch(&mut self, entries: &[(TupleId, Vec<f32>)]) -> Result<(), String> {
        for (id, vector) in entries {
            if vector.is_empty() {
                return Err("Cannot insert empty vector into HNSW index".to_string());
            }
            if matches!(
                self.config.metric,
                DistanceMetric::Cosine | DistanceMetric::DotProduct
            ) {
                let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm <= 1e-10 {
                    return Err(
                        "Cannot insert zero-norm vector for cosine/dot product metric".to_string(),
                    );
                }
            }
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
            {
                let mut vectors = self.vectors.write();
                if let Some(pos) = vectors
                    .iter()
                    .position(|(existing_id, _)| *existing_id == *id)
                {
                    let prepared = self.prepare_vector(vector);
                    vectors[pos] = (*id, prepared);
                } else {
                    let prepared = self.prepare_vector(vector);
                    vectors.push((*id, prepared));
                }
            }
        }
        // Single rebuild after all inserts (key optimization)
        self.rebuild_hnsw()
    }

    fn delete(&mut self, id: TupleId) {
        self.tombstones.write().insert(id);

        // Auto-compact when tombstone ratio exceeds 30% (#49)
        if self.tombstone_ratio() > 0.3 {
            let active: Vec<(TupleId, Vec<f32>)> = {
                let vectors = self.vectors.read();
                let tombstones = self.tombstones.read();
                vectors
                    .iter()
                    .filter(|(id, _)| !tombstones.contains(id))
                    .cloned()
                    .collect()
            };
            let _ = self.rebuild(&active);
        }
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// Safety: HnswIndex uses RwLock internally for thread safety
unsafe impl Send for HnswIndex {}
unsafe impl Sync for HnswIndex {}

// ── Index Persistence ──────────────────────────────────────────────────────

/// Serializable representation of an HNSW index for persistence.
/// The HNSW graph topology is NOT saved — it's rebuilt from vectors on load.
#[derive(Serialize, Deserialize)]
struct PersistedHnswIndex {
    /// Index configuration
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    metric: String,
    /// Vector dimension
    dimension: usize,
    /// All stored vectors (including tombstoned)
    vectors: Vec<(TupleId, Vec<f32>)>,
    /// Tombstoned (deleted) IDs
    tombstones: Vec<TupleId>,
}

impl HnswIndex {
    /// Save the index state to a directory.
    /// Creates `{dir}/index.json` containing config, vectors, and tombstones.
    /// The HNSW graph is reconstructed on load from the persisted vectors.
    pub fn save(&self, dir: &Path) -> Result<(), String> {
        std::fs::create_dir_all(dir).map_err(|e| format!("Failed to create index dir: {e}"))?;

        let persisted = PersistedHnswIndex {
            m: self.config.m,
            ef_construction: self.config.ef_construction,
            ef_search: self.config.ef_search,
            metric: format!("{:?}", self.config.metric).to_lowercase(),
            dimension: *self.dimension.read(),
            vectors: self.vectors.read().clone(),
            tombstones: self.tombstones.read().iter().copied().collect(),
        };

        let json = serde_json::to_string(&persisted)
            .map_err(|e| format!("Failed to serialize index: {e}"))?;

        // Atomic write: write to temp then rename
        let tmp_path = dir.join("index.json.tmp");
        let final_path = dir.join("index.json");

        std::fs::write(&tmp_path, &json).map_err(|e| format!("Failed to write index file: {e}"))?;
        std::fs::rename(&tmp_path, &final_path)
            .map_err(|e| format!("Failed to finalize index file: {e}"))?;

        tracing::debug!(
            vectors = persisted.vectors.len(),
            tombstones = persisted.tombstones.len(),
            dim = persisted.dimension,
            "hnsw_index_saved"
        );

        Ok(())
    }

    /// Load an index from a directory.
    /// Reads `{dir}/index.json` and rebuilds the HNSW graph from persisted vectors.
    pub fn load(dir: &Path) -> Result<Self, String> {
        let path = dir.join("index.json");
        let json =
            std::fs::read_to_string(&path).map_err(|e| format!("Failed to read index: {e}"))?;

        let persisted: PersistedHnswIndex =
            serde_json::from_str(&json).map_err(|e| format!("Failed to parse index: {e}"))?;

        let metric = match persisted.metric.as_str() {
            "euclidean" => DistanceMetric::Euclidean,
            "cosine" => DistanceMetric::Cosine,
            "dotproduct" | "dot_product" => DistanceMetric::DotProduct,
            "manhattan" => DistanceMetric::Manhattan,
            other => return Err(format!("Unknown distance metric: {other}")),
        };

        let config = HnswConfig {
            m: persisted.m,
            ef_construction: persisted.ef_construction,
            ef_search: persisted.ef_search,
            metric,
        };

        let index = Self {
            inner: RwLock::new(None),
            config,
            tombstones: RwLock::new(persisted.tombstones.into_iter().collect()),
            vectors: RwLock::new(persisted.vectors),
            dimension: RwLock::new(persisted.dimension),
        };

        // Rebuild the HNSW graph from persisted vectors
        index.rebuild_hnsw()?;

        tracing::debug!(
            vectors = index.vectors.read().len(),
            tombstones = index.tombstones.read().len(),
            dim = persisted.dimension,
            "hnsw_index_loaded"
        );

        Ok(index)
    }

    /// Check if a persisted index exists at the given directory.
    pub fn persisted_exists(dir: &Path) -> bool {
        dir.join("index.json").exists()
    }

    /// Get the index configuration (for saving with registered index metadata).
    pub fn config(&self) -> &HnswConfig {
        &self.config
    }
}

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

        // Use enough vectors so that deleting one doesn't exceed the 30% auto-compact threshold
        index.insert(0, &[0.0, 0.0]).unwrap();
        index.insert(1, &[1.0, 0.0]).unwrap();
        index.insert(2, &[2.0, 0.0]).unwrap();
        index.insert(3, &[3.0, 0.0]).unwrap();

        assert_eq!(index.len(), 4);
        assert_eq!(index.tombstone_count(), 0);

        // Delete one (ratio 0.25 < 0.3 → no auto-compact)
        index.delete(0);

        assert_eq!(index.len(), 4); // Still 4 (tombstone)
        assert_eq!(index.tombstone_count(), 1);

        // Now search should not include id=0 after manual rebuild
        let active: Vec<(TupleId, Vec<f32>)> = vec![
            (1, vec![1.0, 0.0]),
            (2, vec![2.0, 0.0]),
            (3, vec![3.0, 0.0]),
        ];
        index.rebuild(&active).unwrap();

        let results = index.search(&[0.0, 0.0], 3, None);
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
        // Use vectors with distinct angular directions (important for cosine/dot-product).
        // Evenly spaced angles on a unit circle ensure no two vectors collapse after normalization.
        for metric in [
            DistanceMetric::Euclidean,
            DistanceMetric::Cosine,
            DistanceMetric::DotProduct,
            DistanceMetric::Manhattan,
        ] {
            let mut index = HnswIndex::new(make_config(metric));

            let n = 50;
            let mut vectors = Vec::new();
            for i in 0..n {
                let angle = (i as f32) * std::f32::consts::TAU / (n as f32);
                let id = i as TupleId;
                vectors.push((id, vec![angle.cos(), angle.sin()]));
            }
            index.rebuild(&vectors).unwrap();

            assert_eq!(index.len(), n);
            assert_eq!(index.metric(), metric);

            // Search for vector at angle=0 → (1.0, 0.0) which is id=0
            let results = index.search(&[1.0, 0.0], 5, Some(100));
            assert!(
                results.len() >= 5,
                "Expected at least 5 results for {:?}, got {}",
                metric,
                results.len()
            );
            // id=0 is an exact match for the query — must be in top-5
            assert!(
                results.iter().any(|(id, _)| *id == 0),
                "id=0 should be in top-5 for {:?}, got {:?}",
                metric,
                results,
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

        // Use 4 vectors so deleting 1 (25%) stays below auto-compact threshold (30%)
        index.insert(0, &[0.0, 0.0]).unwrap();
        index.insert(1, &[1.0, 1.0]).unwrap();
        index.insert(2, &[2.0, 2.0]).unwrap();
        index.insert(3, &[3.0, 3.0]).unwrap();

        // Delete one (ratio 0.25 < 0.3 → no auto-compact)
        index.delete(1);
        assert_eq!(index.tombstone_count(), 1);

        // Rebuild without the deleted one
        let vectors = vec![
            (0, vec![0.0, 0.0]),
            (2, vec![2.0, 2.0]),
            (3, vec![3.0, 3.0]),
        ];
        index.rebuild(&vectors).unwrap();

        assert_eq!(index.len(), 3);
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
        // Empty vector should be rejected (#34)
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("empty vector"));
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
    fn test_hnsw_auto_compaction_on_delete() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        // Insert 3 vectors
        index.insert(0, &[0.0, 0.0]).unwrap();
        index.insert(1, &[1.0, 0.0]).unwrap();
        index.insert(2, &[2.0, 0.0]).unwrap();

        // Delete 1 of 3 (ratio 0.33 > 0.3) → should auto-compact
        index.delete(0);

        // After auto-compaction: tombstones cleared, only active vectors remain
        assert_eq!(index.tombstone_count(), 0);
        assert_eq!(index.len(), 2);

        // Deleted vector should not appear in search
        let results = index.search(&[0.0, 0.0], 2, Some(100));
        assert!(results.iter().all(|(id, _)| *id != 0));
    }

    #[test]
    fn test_hnsw_batch_insert() {
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));

        let entries: Vec<(TupleId, Vec<f32>)> = (0..20).map(|i| (i, vec![i as f32, 0.0])).collect();
        index.insert_batch(&entries).unwrap();

        assert_eq!(index.len(), 20);

        // Search should work
        let results = index.search(&[10.0, 0.0], 5, Some(100));
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0, 10);
    }

    #[test]
    fn test_hnsw_index_type() {
        let index = HnswIndex::new(make_config(DistanceMetric::Cosine));
        assert_eq!(index.index_type(), "hnsw");
    }

    #[test]
    fn test_hnsw_different_m_values() {
        // Test with different M values (M=4 excluded: too sparse for reliable ANN)
        for m in [8, 16, 32] {
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

        // Search near origin — L1 distance from (0.1, 0.1):
        // to (0,0) = 0.2, to (1,0) = 1.0, to (0,1) = 1.0, to (1,1) = 1.8
        let results = index.search(&[0.1, 0.1], 4, Some(100));
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].0, 0); // Origin is closest

        // Verify actual L1 distances (within floating point tolerance)
        assert!(
            (results[0].1 - 0.2).abs() < 1e-6,
            "L1 to origin should be 0.2, got {}",
            results[0].1
        );
        assert!(
            (results[1].1 - 1.0).abs() < 1e-4,
            "L1 to (1,0) should be ~1.0, got {}",
            results[1].1
        );
    }

    // ── Persistence tests ──────────────────────────────────────────────

    #[test]
    fn test_hnsw_save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let index_dir = dir.path().join("test_index");

        // Create and populate an index
        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));
        index.insert(0, &[0.0, 0.0]).unwrap();
        index.insert(1, &[1.0, 0.0]).unwrap();
        index.insert(2, &[0.0, 1.0]).unwrap();
        index.insert(3, &[1.0, 1.0]).unwrap();

        // Save
        index.save(&index_dir).unwrap();
        assert!(HnswIndex::persisted_exists(&index_dir));

        // Load
        let loaded = HnswIndex::load(&index_dir).unwrap();

        // Verify properties
        assert_eq!(loaded.len(), 4);
        assert_eq!(loaded.dimension(), 2);
        assert_eq!(loaded.metric(), DistanceMetric::Euclidean);

        // Verify search produces same results
        let orig_results = index.search(&[0.1, 0.1], 2, Some(100));
        let loaded_results = loaded.search(&[0.1, 0.1], 2, Some(100));
        assert_eq!(orig_results.len(), loaded_results.len());
        assert_eq!(orig_results[0].0, loaded_results[0].0);
    }

    #[test]
    fn test_hnsw_save_and_load_with_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let index_dir = dir.path().join("test_index_tombstones");

        let mut index = HnswIndex::new(make_config(DistanceMetric::Euclidean));
        // Use 4 vectors so delete ratio stays < 30% (no auto-compact)
        index.insert(0, &[0.0, 0.0]).unwrap();
        index.insert(1, &[1.0, 0.0]).unwrap();
        index.insert(2, &[0.0, 1.0]).unwrap();
        index.insert(3, &[1.0, 1.0]).unwrap();
        index.delete(1); // tombstone id=1

        index.save(&index_dir).unwrap();
        let loaded = HnswIndex::load(&index_dir).unwrap();

        assert_eq!(loaded.len(), 4); // includes tombstoned
        assert_eq!(loaded.tombstone_count(), 1);
    }

    #[test]
    fn test_hnsw_save_and_load_cosine_metric() {
        let dir = tempfile::tempdir().unwrap();
        let index_dir = dir.path().join("test_cosine");

        let mut index = HnswIndex::new(make_config(DistanceMetric::Cosine));
        index.insert(0, &[1.0, 0.0, 0.0]).unwrap();
        index.insert(1, &[0.0, 1.0, 0.0]).unwrap();
        index.insert(2, &[0.0, 0.0, 1.0]).unwrap();

        index.save(&index_dir).unwrap();
        let loaded = HnswIndex::load(&index_dir).unwrap();

        assert_eq!(loaded.metric(), DistanceMetric::Cosine);
        assert_eq!(loaded.dimension(), 3);
        assert_eq!(loaded.len(), 3);

        // Verify search works on loaded index
        let results = loaded.search(&[1.0, 0.0, 0.0], 1, Some(100));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn test_hnsw_save_and_load_empty_index() {
        let dir = tempfile::tempdir().unwrap();
        let index_dir = dir.path().join("test_empty");

        let index = HnswIndex::new(make_config(DistanceMetric::Euclidean));
        index.save(&index_dir).unwrap();

        let loaded = HnswIndex::load(&index_dir).unwrap();
        assert_eq!(loaded.len(), 0);
        assert!(loaded.is_empty());
        assert_eq!(loaded.dimension(), 0);
    }

    #[test]
    fn test_hnsw_load_nonexistent_dir() {
        let result = HnswIndex::load(Path::new("/nonexistent/path"));
        assert!(result.is_err());
    }

    #[test]
    fn test_hnsw_persisted_exists_false() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!HnswIndex::persisted_exists(dir.path()));
    }

    #[test]
    fn test_hnsw_config_accessor() {
        let config = make_config(DistanceMetric::DotProduct);
        let index = HnswIndex::new(config);
        assert_eq!(index.config().metric, DistanceMetric::DotProduct);
        assert_eq!(index.config().m, 8);
    }
}
