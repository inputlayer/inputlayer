//! HNSW index with epoch-based visibility.
//!
//! Wraps `hnsw_rs` (L2 internally; other metrics are derived at the API
//! boundary). Every entry records the epoch it became visible (`born`) and the
//! epoch it stopped being visible (`died`). A knowledge-graph snapshot captures
//! the index epoch at publish time and searches at that epoch, so one shared
//! graph serves all snapshots consistently while the writer keeps inserting.
//!
//! Deletes and replacements are tombstones: the old entry stays in the graph
//! as a navigation node but is filtered from results. The owner rebuilds the
//! index once `dead_ratio()` crosses its threshold.
//!
//! Indexes of up to `EXACT_SEARCH_MAX` entries are scanned exhaustively, so
//! small datasets get exact top-k.

use crate::index_manager::{DistanceMetric, HnswConfig, TupleId};
use crate::size_limits::MAX_EF_CONSTRUCTION;
use hnsw_rs::prelude::{DistL2, Hnsw};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// `died` value of an entry that is still visible.
const ALIVE: u64 = u64::MAX;

/// Upper bound on HNSW layers (the `hnsw_rs` maximum).
const MAX_LAYER: usize = 16;

/// Initial capacity hint for an empty index.
const MIN_CAPACITY: usize = 1024;

/// Indexes with at most this many graph entries are searched exhaustively:
/// exact results, and cheaper than a graph walk at this size.
pub const EXACT_SEARCH_MAX: usize = 1024;

struct Entry {
    id: TupleId,
    born: u64,
    died: u64,
}

impl Entry {
    fn visible_at(&self, epoch: u64) -> bool {
        self.born <= epoch && self.died > epoch
    }
}

/// Graph plus per-slot metadata. A slot is the `hnsw_rs` data id.
struct Inner {
    graph: Hnsw<'static, f32, DistL2>,
    entries: Vec<Entry>,
    dimension: usize,
}

/// HNSW index for approximate nearest neighbor search.
pub struct HnswIndex {
    config: HnswConfig,
    inner: RwLock<Inner>,
    /// Writer state: tuple id -> live slot. Also serializes writers.
    live: Mutex<HashMap<TupleId, usize>>,
    /// Latest committed epoch.
    epoch: AtomicU64,
}

impl HnswIndex {
    /// Create an empty index.
    pub fn new(config: HnswConfig) -> Self {
        let graph = new_graph(&config, MIN_CAPACITY);
        Self {
            config,
            inner: RwLock::new(Inner {
                graph,
                entries: Vec::new(),
                dimension: 0,
            }),
            live: Mutex::new(HashMap::new()),
            epoch: AtomicU64::new(0),
        }
    }

    /// Build an index from `(id, vector)` rows in one pass (parallel insert).
    ///
    /// Later rows win when an id repeats. Fails on the first invalid vector.
    pub fn build(config: HnswConfig, rows: Vec<(TupleId, Vec<f32>)>) -> Result<Self, String> {
        let mut slot_of: HashMap<TupleId, usize> = HashMap::with_capacity(rows.len());
        let mut unique: Vec<(TupleId, Vec<f32>)> = Vec::with_capacity(rows.len());
        let mut dimension = 0;
        for (id, vector) in rows {
            validate_vector(&config, dimension, &vector)
                .map_err(|e| format!("row id={id}: {e}"))?;
            dimension = vector.len();
            let prepared = prepare(&config, &vector);
            match slot_of.get(&id) {
                Some(&slot) => unique[slot].1 = prepared,
                None => {
                    slot_of.insert(id, unique.len());
                    unique.push((id, prepared));
                }
            }
        }

        let graph = new_graph(&config, unique.len().max(MIN_CAPACITY));
        let batch: Vec<(&Vec<f32>, usize)> = unique
            .iter()
            .enumerate()
            .map(|(slot, (_, v))| (v, slot))
            .collect();
        graph.parallel_insert(&batch);

        let entries = unique
            .iter()
            .map(|(id, _)| Entry {
                id: *id,
                born: 0,
                died: ALIVE,
            })
            .collect();

        Ok(Self {
            config,
            inner: RwLock::new(Inner {
                graph,
                entries,
                dimension,
            }),
            live: Mutex::new(slot_of),
            epoch: AtomicU64::new(0),
        })
    }

    /// Apply one batch of changes and commit it as a new epoch.
    ///
    /// Deletes run before upserts, so an id present in both ends up live with
    /// the upserted vector. Upserting an existing id replaces its vector.
    /// The whole batch is validated before anything changes.
    ///
    /// Returns the committed epoch.
    pub fn apply(
        &self,
        upserts: &[(TupleId, Vec<f32>)],
        deletes: &[TupleId],
    ) -> Result<u64, String> {
        let mut live = self.live.lock();

        let mut dimension = self.inner.read().dimension;
        for (id, vector) in upserts {
            validate_vector(&self.config, dimension, vector)
                .map_err(|e| format!("row id={id}: {e}"))?;
            dimension = vector.len();
        }

        let epoch = self.epoch.load(Ordering::Acquire) + 1;

        {
            let mut inner = self.inner.write();
            for id in deletes {
                if let Some(slot) = live.remove(id) {
                    inner.entries[slot].died = epoch;
                }
            }
            if !upserts.is_empty() {
                inner.dimension = dimension;
            }
        }

        for (id, vector) in upserts {
            let prepared = prepare(&self.config, vector);
            // One write lock per vector keeps readers interleaved with a long batch.
            let mut inner = self.inner.write();
            if let Some(old) = live.get(id) {
                inner.entries[*old].died = epoch;
            }
            let slot = inner.entries.len();
            inner.entries.push(Entry {
                id: *id,
                born: epoch,
                died: ALIVE,
            });
            inner.graph.insert((&prepared, slot));
            live.insert(*id, slot);
        }

        self.epoch.store(epoch, Ordering::Release);
        Ok(epoch)
    }

    /// Latest committed epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Search the `k` nearest entries visible at `epoch`.
    ///
    /// Results are `(id, distance)` sorted by ascending distance in the
    /// configured metric, ties by id. `ef` overrides the configured `ef_search`.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
        epoch: u64,
    ) -> Result<Vec<(TupleId, f64)>, String> {
        let inner = self.inner.read();
        if inner.entries.is_empty() || k == 0 {
            return Ok(Vec::new());
        }
        if query.len() != inner.dimension {
            return Err(format!(
                "query vector has dimension {}, but the index has dimension {}",
                query.len(),
                inner.dimension
            ));
        }
        if query.iter().any(|x| !x.is_finite()) {
            return Err("query vector contains NaN or infinite values".to_string());
        }

        let prepared = prepare(&self.config, query);
        if inner.entries.len() <= EXACT_SEARCH_MAX {
            return Ok(self.exact_search(&inner, &prepared, k, epoch));
        }
        let manhattan = self.config.metric == DistanceMetric::Manhattan;
        let total = inner.entries.len();
        // `hnsw_rs` sizes its candidate heaps by `ef`; a wider beam than the
        // graph has entries finds nothing more.
        let ef = ef.unwrap_or(self.config.ef_search).min(total);
        // Manhattan reranks L2 candidates, so it needs a wider candidate set.
        let mut fetch = if manhattan { k.saturating_mul(4) } else { k };

        // Tombstones and entries newer than `epoch` are filtered after the
        // graph search; widen the search until k visible hits are found.
        loop {
            let fetch_n = fetch.min(total);
            let raw = inner.graph.search(&prepared, fetch_n, ef.max(fetch_n));
            let mut hits: Vec<(TupleId, f64)> = raw
                .into_iter()
                .filter_map(|n| {
                    let entry = inner.entries.get(n.d_id)?;
                    if !entry.visible_at(epoch) {
                        return None;
                    }
                    let distance = if manhattan {
                        let point = inner.graph.get_point_indexation().get_point_data(&n.p_id)?;
                        manhattan_distance(&prepared, &point)
                    } else {
                        self.transform_distance(n.distance)
                    };
                    Some((entry.id, distance))
                })
                .collect();
            if hits.len() >= k || fetch_n >= total {
                hits.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
                hits.truncate(k);
                return Ok(hits);
            }
            fetch = fetch_n.saturating_mul(2);
        }
    }

    fn exact_search(
        &self,
        inner: &Inner,
        prepared: &[f32],
        k: usize,
        epoch: u64,
    ) -> Vec<(TupleId, f64)> {
        let mut hits: Vec<(TupleId, f64)> = inner
            .graph
            .get_point_indexation()
            .into_iter()
            .filter_map(|point| {
                let entry = inner.entries.get(point.get_origin_id())?;
                if !entry.visible_at(epoch) {
                    return None;
                }
                let distance = match self.config.metric {
                    DistanceMetric::Manhattan => manhattan_distance(prepared, point.get_v()),
                    _ => self.transform_distance(l2_distance(prepared, point.get_v())),
                };
                Some((entry.id, distance))
            })
            .collect();
        hits.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        hits.truncate(k);
        hits
    }

    /// Number of live entries.
    pub fn len(&self) -> usize {
        self.live.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of tombstoned entries still in the graph.
    pub fn dead_count(&self) -> usize {
        let total = self.inner.read().entries.len();
        total - self.len()
    }

    /// Fraction of graph entries that are tombstones (0.0 when empty).
    pub fn dead_ratio(&self) -> f64 {
        let total = self.inner.read().entries.len();
        if total == 0 {
            0.0
        } else {
            self.dead_count() as f64 / total as f64
        }
    }

    /// Vector dimension (0 until the first vector arrives; fixed until rebuild).
    pub fn dimension(&self) -> usize {
        self.inner.read().dimension
    }

    pub fn metric(&self) -> DistanceMetric {
        self.config.metric
    }

    pub fn config(&self) -> &HnswConfig {
        &self.config
    }

    /// Check that `vector` could be inserted (dimension, finiteness, norm).
    pub fn validate(&self, vector: &[f32]) -> Result<(), String> {
        validate_vector(&self.config, self.dimension(), vector)
    }

    /// Map an `hnsw_rs` L2 distance to the configured metric.
    ///
    /// Cosine and dot vectors are unit-normalized on insert, so
    /// `L2^2 = 2(1 - cos)`.
    fn transform_distance(&self, l2: f32) -> f64 {
        let l2 = f64::from(l2);
        match self.config.metric {
            DistanceMetric::Euclidean | DistanceMetric::Manhattan => l2,
            DistanceMetric::Cosine => l2 * l2 / 2.0,
            DistanceMetric::DotProduct => -(1.0 - l2 * l2 / 2.0),
        }
    }
}

fn new_graph(config: &HnswConfig, capacity: usize) -> Hnsw<'static, f32, DistL2> {
    // `hnsw_rs` exits the process when m is over 256 and sizes a heap by
    // ef_construction on every insert; definitions saved before these were
    // bounded may hold anything.
    let mut graph = Hnsw::new(
        config.m.clamp(2, 256),
        capacity,
        MAX_LAYER,
        config.ef_construction.clamp(1, MAX_EF_CONSTRUCTION),
        DistL2,
    );
    // Keep pruned links and extend candidates: Navarro pruning alone can
    // disconnect small-to-medium graphs and make search return < k results.
    graph.set_keeping_pruned(true);
    graph.set_extend_candidates(true);
    graph
}

/// Check that `vector` can go into an index with `config` whose dimension is
/// `dimension` (0 = not fixed yet).
pub fn validate_vector(
    config: &HnswConfig,
    dimension: usize,
    vector: &[f32],
) -> Result<(), String> {
    if vector.is_empty() {
        return Err("cannot index an empty vector".to_string());
    }
    if dimension != 0 && vector.len() != dimension {
        return Err(format!(
            "vector has dimension {}, but the index has dimension {dimension}",
            vector.len()
        ));
    }
    if vector.iter().any(|x| !x.is_finite()) {
        return Err("vector contains NaN or infinite values".to_string());
    }
    if matches!(
        config.metric,
        DistanceMetric::Cosine | DistanceMetric::DotProduct
    ) && norm(vector) <= 1e-10
    {
        return Err(format!(
            "zero-norm vector cannot be indexed with metric {}",
            config.metric
        ));
    }
    Ok(())
}

fn norm(vector: &[f32]) -> f32 {
    vector.iter().map(|x| x * x).sum::<f32>().sqrt()
}

/// Normalize for cosine/dot; other metrics index raw vectors.
fn prepare(config: &HnswConfig, vector: &[f32]) -> Vec<f32> {
    match config.metric {
        DistanceMetric::Cosine | DistanceMetric::DotProduct => {
            let n = norm(vector);
            if n > 1e-10 {
                vector.iter().map(|x| x / n).collect()
            } else {
                vector.to_vec()
            }
        }
        DistanceMetric::Euclidean | DistanceMetric::Manhattan => vector.to_vec(),
    }
}

fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

fn manhattan_distance(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (f64::from(*x) - f64::from(*y)).abs())
        .sum()
}

#[cfg(test)]
#[path = "hnsw_index_tests.rs"]
mod tests;
