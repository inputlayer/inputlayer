//! Vector index definitions and per-KG index registry.
//!
//! A `KnowledgeGraph` owns one `IndexManager`. Each `ManagedIndex` pairs a
//! definition with a live `HnswIndex` that the storage layer keeps in sync
//! with the base relation on every insert and delete.
//!
//! Snapshots never touch the manager: at publish time they capture an
//! `IndexView` (shared index + committed epoch) per index, so a query sees
//! exactly the index state that matches its snapshot's base data.

use crate::hnsw_index::HnswIndex;
use crate::value::Value;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

/// Row identifier stored in an index: the integer in the relation's first column.
pub type TupleId = i64;

/// Distance metric for similarity search
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DistanceMetric {
    /// Cosine distance (1 - cosine similarity)
    #[default]
    Cosine,
    /// Euclidean distance (L2 norm)
    Euclidean,
    /// Negated cosine similarity (vectors are normalized)
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum IndexType {
    /// HNSW index for approximate nearest neighbor search
    Hnsw(HnswConfig),
}

impl IndexType {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Hnsw(_) => "hnsw",
        }
    }

    pub fn hnsw_config(&self) -> &HnswConfig {
        match self {
            Self::Hnsw(config) => config,
        }
    }
}

/// Index definition (what `.index create` declares; persisted per KG)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

/// Integer type of the indexed relation's id column, used to return ids
/// that join with the relation (`Int32(1) != Int64(1)`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum IdType {
    Int32,
    #[default]
    Int64,
}

impl IdType {
    /// Split an id value into its integer and type (None for non-integers).
    pub fn of(value: &Value) -> Option<(TupleId, Self)> {
        match value {
            Value::Int32(v) => Some((TupleId::from(*v), Self::Int32)),
            Value::Int64(v) => Some((*v, Self::Int64)),
            _ => None,
        }
    }

    pub fn to_value(self, id: TupleId) -> Value {
        match self {
            // Ids typed Int32 were stored from Int32 values, so they fit.
            Self::Int32 => i32::try_from(id).map_or(Value::Int64(id), Value::Int32),
            Self::Int64 => Value::Int64(id),
        }
    }
}

/// A definition plus its live index.
pub struct ManagedIndex {
    pub definition: RegisteredIndex,
    pub index: Arc<HnswIndex>,
    pub id_type: IdType,
    /// Build time (microseconds since epoch)
    pub built_at: u64,
}

impl ManagedIndex {
    pub fn new(definition: RegisteredIndex, index: HnswIndex, id_type: IdType) -> Self {
        let built_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as u64);
        Self {
            definition,
            index: Arc::new(index),
            id_type,
            built_at,
        }
    }

    /// Point-in-time view for a snapshot.
    pub fn view(&self) -> IndexView {
        IndexView {
            index: Arc::clone(&self.index),
            epoch: self.index.epoch(),
            id_type: self.id_type,
        }
    }

    pub fn stats(&self) -> IndexStats {
        IndexStats {
            name: self.definition.name.clone(),
            relation: self.definition.relation.clone(),
            column: self.definition.column_name.clone(),
            index_type: self.definition.index_type.type_name().to_string(),
            metric: self.index.metric(),
            tuple_count: self.index.len(),
            tombstone_count: self.index.dead_count(),
            built_at: self.built_at,
            dimension: self.index.dimension(),
        }
    }
}

/// An index frozen at one epoch. Cheap to clone; shared by snapshots.
#[derive(Clone)]
pub struct IndexView {
    index: Arc<HnswIndex>,
    epoch: u64,
    id_type: IdType,
}

impl IndexView {
    /// k-NN search returning `(id value, distance)` pairs.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<(Value, f64)>, String> {
        Ok(self
            .index
            .search(query, k, ef, self.epoch)?
            .into_iter()
            .map(|(id, dist)| (self.id_type.to_value(id), dist))
            .collect())
    }
}

/// Search callback the query engine uses for `hnsw_nearest`.
/// Signature: (index_name, query_vector, k, ef_search) -> [(id, distance)]
pub type HnswSearchFn = Arc<
    dyn Fn(&str, &[f32], usize, Option<usize>) -> Result<Vec<(Value, f64)>, String> + Send + Sync,
>;

/// Build a search callback over a fixed set of index views.
pub fn search_fn(views: HashMap<String, IndexView>) -> HnswSearchFn {
    Arc::new(move |name, query, k, ef| {
        let view = views.get(name).ok_or_else(|| {
            let mut known: Vec<&str> = views.keys().map(String::as_str).collect();
            known.sort_unstable();
            format!(
                "hnsw_nearest: index '{name}' does not exist. Available indexes: [{}]. \
                 Create one with `.index create {name} on <relation>(<column>)`.",
                known.join(", ")
            )
        })?;
        view.search(query, k, ef)
            .map_err(|e| format!("hnsw_nearest on index '{name}': {e}"))
    })
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
    /// Number of live vectors
    pub tuple_count: usize,
    /// Number of tombstoned entries awaiting compaction
    pub tombstone_count: usize,
    /// When the index was (re)built (microseconds since epoch)
    pub built_at: u64,
    /// Vector dimension (0 while empty)
    pub dimension: usize,
}

/// Indexes of one knowledge graph, keyed by name.
#[derive(Default)]
pub struct IndexManager {
    indexes: BTreeMap<String, ManagedIndex>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an index. Fails if the name is taken.
    pub fn insert(&mut self, managed: ManagedIndex) -> Result<(), String> {
        let name = managed.definition.name.clone();
        if self.indexes.contains_key(&name) {
            return Err(format!(
                "Index '{name}' already exists. Drop it first with `.index drop {name}`."
            ));
        }
        self.indexes.insert(name, managed);
        Ok(())
    }

    /// Replace the live index of an existing entry (rebuild).
    pub fn replace(&mut self, managed: ManagedIndex) {
        self.indexes
            .insert(managed.definition.name.clone(), managed);
    }

    pub fn remove(&mut self, name: &str) -> Result<ManagedIndex, String> {
        self.indexes
            .remove(name)
            .ok_or_else(|| self.not_found(name))
    }

    pub fn get(&self, name: &str) -> Option<&ManagedIndex> {
        self.indexes.get(name)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut ManagedIndex> {
        self.indexes.get_mut(name)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.indexes.contains_key(name)
    }

    /// Names of indexes built on `relation`.
    pub fn names_for_relation(&self, relation: &str) -> Vec<String> {
        self.indexes
            .values()
            .filter(|m| m.definition.relation == relation)
            .map(|m| m.definition.name.clone())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &ManagedIndex> {
        self.indexes.values()
    }

    pub fn stats(&self, name: &str) -> Result<IndexStats, String> {
        self.get(name)
            .map(ManagedIndex::stats)
            .ok_or_else(|| self.not_found(name))
    }

    pub fn all_stats(&self) -> Vec<IndexStats> {
        self.indexes.values().map(ManagedIndex::stats).collect()
    }

    /// Point-in-time views of every index (for snapshot publication).
    pub fn views(&self) -> HashMap<String, IndexView> {
        self.indexes
            .iter()
            .map(|(name, m)| (name.clone(), m.view()))
            .collect()
    }

    /// Error for a missing index, listing the ones that exist.
    pub fn not_found(&self, name: &str) -> String {
        let known: Vec<&str> = self.indexes.keys().map(String::as_str).collect();
        format!(
            "Index '{name}' not found. Available indexes: [{}]",
            known.join(", ")
        )
    }

    /// Persist all definitions to `path` (atomic replace; removes the file when empty).
    pub fn save_definitions(&self, path: &Path) -> Result<(), String> {
        if self.indexes.is_empty() {
            return match std::fs::remove_file(path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(format!("Failed to remove {}: {e}", path.display())),
            };
        }
        let definitions: Vec<&RegisteredIndex> =
            self.indexes.values().map(|m| &m.definition).collect();
        let json = serde_json::to_string_pretty(&definitions)
            .map_err(|e| format!("Failed to serialize index definitions: {e}"))?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, json)
            .map_err(|e| format!("Failed to write {}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, path).map_err(|e| format!("Failed to write {}: {e}", path.display()))
    }

    /// Read definitions saved by `save_definitions` (empty if the file is absent).
    pub fn load_definitions(path: &Path) -> Result<Vec<RegisteredIndex>, String> {
        let json = match std::fs::read_to_string(path) {
            Ok(json) => json,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(format!("Failed to read {}: {e}", path.display())),
        };
        serde_json::from_str(&json).map_err(|e| format!("Failed to parse {}: {e}", path.display()))
    }
}

impl std::fmt::Debug for IndexManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexManager")
            .field("indexes", &self.indexes.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn definition(name: &str, relation: &str) -> RegisteredIndex {
        RegisteredIndex {
            name: name.to_string(),
            relation: relation.to_string(),
            column_idx: 1,
            column_name: "embedding".to_string(),
            index_type: IndexType::Hnsw(HnswConfig::default()),
        }
    }

    fn managed(name: &str, relation: &str, rows: Vec<(TupleId, Vec<f32>)>) -> ManagedIndex {
        let def = definition(name, relation);
        let index = HnswIndex::build(def.index_type.hnsw_config().clone(), rows).unwrap();
        ManagedIndex::new(def, index, IdType::Int64)
    }

    #[test]
    fn test_distance_metric_parse_and_display() {
        assert_eq!(
            "cos".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::Cosine
        );
        assert_eq!(
            "L2".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::Euclidean
        );
        assert_eq!(
            "inner".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::DotProduct
        );
        assert_eq!(
            "taxicab".parse::<DistanceMetric>().unwrap(),
            DistanceMetric::Manhattan
        );
        assert!("bogus"
            .parse::<DistanceMetric>()
            .unwrap_err()
            .contains("Valid options"));
        assert_eq!(DistanceMetric::Euclidean.to_string(), "l2");
    }

    #[test]
    fn test_manager_insert_duplicate_errors() {
        let mut mgr = IndexManager::new();
        mgr.insert(managed("a", "docs", vec![])).unwrap();
        let err = mgr.insert(managed("a", "docs", vec![])).unwrap_err();
        assert!(err.contains("already exists"), "{err}");
    }

    #[test]
    fn test_manager_remove_missing_lists_available() {
        let mut mgr = IndexManager::new();
        mgr.insert(managed("a", "docs", vec![])).unwrap();
        let err = mgr.remove("zzz").err().unwrap();
        assert!(
            err.contains("'zzz' not found") && err.contains("[a]"),
            "{err}"
        );
    }

    #[test]
    fn test_manager_names_for_relation() {
        let mut mgr = IndexManager::new();
        mgr.insert(managed("a", "docs", vec![])).unwrap();
        mgr.insert(managed("b", "other", vec![])).unwrap();
        assert_eq!(mgr.names_for_relation("docs"), vec!["a".to_string()]);
        assert_eq!(mgr.names_for_relation("none").len(), 0);
    }

    #[test]
    fn test_manager_stats_reports_counts() {
        let mut mgr = IndexManager::new();
        mgr.insert(managed(
            "a",
            "docs",
            vec![(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])],
        ))
        .unwrap();
        mgr.get("a").unwrap().index.apply(&[], &[1]).unwrap();
        let stats = mgr.stats("a").unwrap();
        assert_eq!(stats.tuple_count, 1);
        assert_eq!(stats.tombstone_count, 1);
        assert_eq!(stats.dimension, 2);
        assert_eq!(stats.metric, DistanceMetric::Cosine);
    }

    #[test]
    fn test_view_is_frozen_at_publish_epoch() {
        let mut mgr = IndexManager::new();
        mgr.insert(managed("a", "docs", vec![(1, vec![1.0, 0.0])]))
            .unwrap();
        let before = mgr.views();
        mgr.get("a")
            .unwrap()
            .index
            .apply(&[(2, vec![0.9, 0.1])], &[1])
            .unwrap();
        let after = mgr.views();

        let old: Vec<Value> = before["a"]
            .search(&[1.0, 0.0], 5, None)
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let new: Vec<Value> = after["a"]
            .search(&[1.0, 0.0], 5, None)
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(old, vec![Value::Int64(1)]);
        assert_eq!(new, vec![Value::Int64(2)]);
    }

    #[test]
    fn test_search_fn_unknown_index_error() {
        let f = search_fn(HashMap::new());
        let err = f("missing", &[1.0], 1, None).unwrap_err();
        assert!(err.contains("index 'missing' does not exist"), "{err}");
        assert!(err.contains(".index create"), "{err}");
    }

    #[test]
    fn test_search_fn_dimension_mismatch_error() {
        let mut mgr = IndexManager::new();
        mgr.insert(managed("a", "docs", vec![(1, vec![1.0, 0.0])]))
            .unwrap();
        let f = search_fn(mgr.views());
        let err = f("a", &[1.0, 0.0, 0.0], 1, None).unwrap_err();
        assert!(
            err.contains("index 'a'") && err.contains("dimension 3"),
            "{err}"
        );
    }

    #[test]
    fn test_id_type_roundtrip() {
        assert_eq!(IdType::of(&Value::Int32(7)), Some((7, IdType::Int32)));
        assert_eq!(IdType::of(&Value::Int64(7)), Some((7, IdType::Int64)));
        assert_eq!(IdType::of(&Value::string("x")), None);
        assert_eq!(IdType::Int32.to_value(7), Value::Int32(7));
        assert_eq!(IdType::Int64.to_value(7), Value::Int64(7));
    }

    #[test]
    fn test_definitions_save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("indexes.json");
        assert_eq!(IndexManager::load_definitions(&path).unwrap().len(), 0);

        let mut mgr = IndexManager::new();
        let mut def = definition("a", "docs");
        def.index_type = IndexType::Hnsw(HnswConfig {
            m: 8,
            ef_construction: 64,
            ef_search: 20,
            metric: DistanceMetric::Manhattan,
        });
        let index = HnswIndex::build(def.index_type.hnsw_config().clone(), vec![]).unwrap();
        mgr.insert(ManagedIndex::new(def.clone(), index, IdType::Int64))
            .unwrap();
        mgr.save_definitions(&path).unwrap();
        assert_eq!(IndexManager::load_definitions(&path).unwrap(), vec![def]);

        mgr.remove("a").unwrap();
        mgr.save_definitions(&path).unwrap();
        assert!(!path.exists());
    }
}
