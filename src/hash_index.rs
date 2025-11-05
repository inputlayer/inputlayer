//! Hash-based indexing for relations. O(1) tuple lookup by join key,
//! with a Bloom filter per index for fast negative lookups.
//!
//! # Architecture
//!
//! ```text
//! HashIndexManager
//!   `-- HashMap<JoinKeySpec, HashIndex>
//!         `-- HashIndex
//!               |-- HashMap<Tuple, Vec<Tuple>>  (key -> tuples)
//!               |-- BloomFilter                 (for fast negatives)
//!               `-- HashIndexStats              (for optimization)
//! ```
//!
//! # Example
//!
//! ```
//! use inputlayer::hash_index::{HashIndex, JoinKeySpec, HashIndexManager, HashIndexConfig};
//! use inputlayer::value::{Tuple, Value};
//!
//! fn make_tuple(values: Vec<i64>) -> Tuple {
//!     Tuple::new(values.into_iter().map(Value::Int64).collect())
//! }
//!
//! // Create an index for edge(src, dst) on column 0 (src)
//! let spec = JoinKeySpec::new("edge", vec![0]);
//! let mut index = HashIndex::new(spec, 1000);
//!
//! // Insert tuples
//! index.insert(make_tuple(vec![1, 2]));  // edge(1, 2)
//! index.insert(make_tuple(vec![1, 3]));  // edge(1, 3)
//! index.insert(make_tuple(vec![2, 4]));  // edge(2, 4)
//!
//! // Lookup by key
//! let key = make_tuple(vec![1]);
//! let results: Vec<_> = index.probe(&key).collect();
//! assert_eq!(results.len(), 2);  // Found edge(1,2) and edge(1,3)
//! ```

use crate::bloom_filter::BloomFilter;
use crate::value::Tuple;
#[cfg(test)]
use crate::value::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Identifies a specific join key configuration.
///
/// A join key spec uniquely identifies an index by:
/// 1. The relation name
/// 2. The column indices that form the key
///
/// # Example
///
/// ```
/// use inputlayer::hash_index::JoinKeySpec;
///
/// // Index on edge.src (column 0)
/// let spec1 = JoinKeySpec::new("edge", vec![0]);
///
/// // Index on edge.(src, type) (columns 0 and 2)
/// let spec2 = JoinKeySpec::new("edge", vec![0, 2]);
/// ```
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct JoinKeySpec {
    /// Relation name being indexed
    pub relation: String,
    /// Column indices that form the join key (0-based)
    pub key_columns: Vec<usize>,
}

impl JoinKeySpec {
    pub fn new(relation: &str, key_columns: Vec<usize>) -> Self {
        Self {
            relation: relation.to_string(),
            key_columns,
        }
    }

    /// Get a string representation for logging/display.
    pub fn display_name(&self) -> String {
        let cols: Vec<String> = self
            .key_columns
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        format!("{}[{}]", self.relation, cols.join(","))
    }
}

/// Hash index for a specific join key.
///
/// Provides O(1) lookup of tuples by their join key value.
/// Internally uses a Bloom filter to accelerate negative lookups.
pub struct HashIndex {
    /// The join key specification
    pub spec: JoinKeySpec,
    /// Hash map: key tuple -> list of full tuples with that key
    index: HashMap<Tuple, Vec<Tuple>>,
    /// Bloom filter for quick "definitely not present" checks
    bloom: BloomFilter,
    /// Statistics about the index
    stats: HashIndexStats,
    /// Version number (incremented on each modification)
    version: u64,
}

/// Statistics about a hash index.
///
/// Used for query optimization and monitoring.
#[derive(Clone, Debug, Default)]
pub struct HashIndexStats {
    /// Number of unique key values
    pub num_keys: usize,
    /// Total number of tuples indexed
    pub num_tuples: usize,
    /// Average number of tuples per key
    pub avg_tuples_per_key: f64,
    /// Maximum tuples for any single key
    pub max_tuples_per_key: usize,
    /// Current estimated Bloom filter false positive rate
    pub bloom_fp_rate: f64,
}

impl HashIndex {
    /// Create a new hash index.
    ///
    /// # Arguments
    ///
    /// * `spec` - The join key specification
    /// * `expected_keys` - Expected number of unique key values
    ///   (used to size the Bloom filter)
    ///
    /// # Example
    ///
    /// ```
    /// use inputlayer::hash_index::{HashIndex, JoinKeySpec};
    ///
    /// let spec = JoinKeySpec::new("edge", vec![0]);
    /// let index = HashIndex::new(spec, 1000);
    /// ```
    pub fn new(spec: JoinKeySpec, expected_keys: usize) -> Self {
        Self {
            spec,
            index: HashMap::with_capacity(expected_keys),
            bloom: BloomFilter::new(expected_keys.max(100), 0.01),
            stats: HashIndexStats::default(),
            version: 0,
        }
    }

    /// Build the index from a collection of tuples.
    ///
    /// This is more efficient than inserting tuples one by one
    /// because it can batch operations and compute statistics once.
    ///
    /// # Arguments
    ///
    /// * `tuples` - Iterator of tuples to index
    ///
    /// # Note
    ///
    /// This clears any existing index contents.
    pub fn build_from_tuples(&mut self, tuples: impl IntoIterator<Item = Tuple>) {
        // Clear existing data
        self.index.clear();
        self.bloom.clear();

        let mut max_per_key = 0usize;
        let mut total_tuples = 0usize;

        for tuple in tuples {
            let key = self.extract_key(&tuple);
            self.bloom.insert(&key);

            let entry = self.index.entry(key).or_default();
            entry.push(tuple);
            max_per_key = max_per_key.max(entry.len());
            total_tuples += 1;
        }

        // Update statistics
        self.stats = HashIndexStats {
            num_keys: self.index.len(),
            num_tuples: total_tuples,
            avg_tuples_per_key: if self.index.is_empty() {
                0.0
            } else {
                total_tuples as f64 / self.index.len() as f64
            },
            max_tuples_per_key: max_per_key,
            bloom_fp_rate: self.bloom.estimated_false_positive_rate(),
        };

        self.version += 1;
    }

    /// Insert a single tuple into the index.
    ///
    /// # Arguments
    ///
    /// * `tuple` - The tuple to insert
    ///
    /// # Note
    ///
    /// Duplicate tuples are allowed; the same tuple can be
    /// inserted multiple times.
    pub fn insert(&mut self, tuple: Tuple) {
        let key = self.extract_key(&tuple);
        self.bloom.insert(&key);

        let entry = self.index.entry(key).or_default();
        let is_new_key = entry.is_empty();
        entry.push(tuple);

        // Update statistics
        self.stats.num_tuples += 1;
        if is_new_key {
            self.stats.num_keys += 1;
        }
        self.stats.avg_tuples_per_key =
            self.stats.num_tuples as f64 / self.stats.num_keys.max(1) as f64;
        self.stats.max_tuples_per_key = self.stats.max_tuples_per_key.max(entry.len());

        self.version += 1;
    }

    /// Remove a tuple from the index.
    ///
    /// # Arguments
    ///
    /// * `tuple` - The exact tuple to remove (must match all columns)
    ///
    /// # Returns
    ///
    /// `true` if the tuple was found and removed, `false` otherwise.
    ///
    /// # Note
    ///
    /// Bloom filters don't support removal, so the filter may still
    /// report the key as "might contain" after removal. This is safe
    /// (just a potential false positive) but slightly less efficient.
    pub fn remove(&mut self, tuple: &Tuple) -> bool {
        let key = self.extract_key(tuple);

        if let Some(tuples) = self.index.get_mut(&key) {
            if let Some(pos) = tuples.iter().position(|t| t == tuple) {
                tuples.remove(pos);
                self.stats.num_tuples -= 1;

                if tuples.is_empty() {
                    self.index.remove(&key);
                    self.stats.num_keys -= 1;
                    // Note: Can't remove from Bloom filter
                }

                // Update avg (recalculate)
                self.stats.avg_tuples_per_key = if self.stats.num_keys > 0 {
                    self.stats.num_tuples as f64 / self.stats.num_keys as f64
                } else {
                    0.0
                };

                self.version += 1;
                return true;
            }
        }

        false
    }

    /// Check if a key might exist in the index (fast Bloom filter check).
    ///
    /// # Returns
    ///
    /// - `false` - The key definitely does NOT exist
    /// - `true` - The key MIGHT exist (need to call `probe` to verify)
    ///
    /// # Performance
    ///
    /// This is O(k) where k is typically 7, much faster than
    /// the HashMap lookup for negative cases.
    pub fn might_contain_key(&self, key: &Tuple) -> bool {
        self.bloom.might_contain(key)
    }

    /// Get all tuples for a key (direct HashMap lookup).
    ///
    /// # Returns
    ///
    /// `Some(&Vec<Tuple>)` if the key exists, `None` otherwise.
    pub fn get(&self, key: &Tuple) -> Option<&Vec<Tuple>> {
        self.index.get(key)
    }

    /// Get all tuples for a key with Bloom filter pre-check.
    ///
    /// This first checks the Bloom filter and returns `None`
    /// immediately if the key definitely doesn't exist.
    ///
    /// # Returns
    ///
    /// `Some(&Vec<Tuple>)` if the key exists, `None` otherwise.
    pub fn get_with_bloom(&self, key: &Tuple) -> Option<&Vec<Tuple>> {
        if !self.bloom.might_contain(key) {
            return None;
        }
        self.index.get(key)
    }

    /// Probe the index for all tuples matching a key.
    ///
    /// This is the primary lookup method for join execution.
    ///
    /// # Arguments
    ///
    /// * `key` - The join key value to look up
    ///
    /// # Returns
    ///
    /// Iterator over matching tuples (empty if no match).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let key = make_tuple(vec![42]);
    /// for tuple in index.probe(&key) {
    ///     println!("Found: {:?}", tuple);
    /// }
    /// ```
    pub fn probe(&self, key: &Tuple) -> impl Iterator<Item = &Tuple> {
        self.get_with_bloom(key)
            .map(|v| v.iter())
            .into_iter()
            .flatten()
    }

    pub fn stats(&self) -> &HashIndexStats {
        &self.stats
    }

    /// Version is incremented on every modification.
    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn len(&self) -> usize {
        self.stats.num_tuples
    }

    pub fn is_empty(&self) -> bool {
        self.stats.num_tuples == 0
    }

    /// Extract the key tuple from a full tuple.
    ///
    /// # Example
    ///
    /// If key_columns = [0, 2] and tuple = (A, B, C, D),
    /// returns (A, C).
    fn extract_key(&self, tuple: &Tuple) -> Tuple {
        tuple.from_indices(&self.spec.key_columns)
    }
}

/// Manager for hash indexes across all relations.
///
/// Handles:
/// - Index creation and lifecycle
/// - Automatic index creation for frequently-used join keys
/// - LRU eviction when memory limits are reached
/// - Invalidation when relations are modified
///
/// # Thread Safety
///
/// The manager itself is not thread-safe. Each index is wrapped
/// in `Arc<RwLock<>>` for shared access.
pub struct HashIndexManager {
    /// All indexes: (relation, key_columns) -> index
    indexes: HashMap<JoinKeySpec, Arc<RwLock<HashIndex>>>,
    /// Configuration
    config: HashIndexConfig,
    /// Usage statistics for LRU eviction
    usage_stats: HashMap<JoinKeySpec, IndexUsageStats>,
}

/// Configuration for the hash index manager.
#[derive(Clone, Debug)]
pub struct HashIndexConfig {
    /// Maximum number of indexes to maintain
    pub max_indexes: usize,
    /// Automatically create indexes for frequently-used join keys
    pub auto_create: bool,
    /// Number of uses before auto-creating an index
    pub auto_create_threshold: usize,
    /// Default expected keys for new indexes
    pub default_expected_keys: usize,
}

impl Default for HashIndexConfig {
    fn default() -> Self {
        Self {
            max_indexes: 100,
            auto_create: true,
            auto_create_threshold: 3,
            default_expected_keys: 10000,
        }
    }
}

/// Internal: tracks usage of each index for LRU eviction.
#[derive(Clone, Debug, Default)]
struct IndexUsageStats {
    /// Number of times this join key was used in queries
    lookup_count: usize,
    /// Number of probe operations
    #[allow(dead_code)]
    probe_count: usize,
    /// Last access timestamp (for LRU)
    last_used: u64,
}

impl HashIndexManager {
    pub fn new(config: HashIndexConfig) -> Self {
        Self {
            indexes: HashMap::new(),
            config,
            usage_stats: HashMap::new(),
        }
    }

    /// Create an index for a join key specification.
    ///
    /// If an index already exists for this spec, returns the existing one.
    pub fn create_index(&mut self, spec: JoinKeySpec) -> Arc<RwLock<HashIndex>> {
        if let Some(existing) = self.indexes.get(&spec) {
            return Arc::clone(existing);
        }

        let index = HashIndex::new(spec.clone(), self.config.default_expected_keys);
        let arc = Arc::new(RwLock::new(index));
        self.indexes.insert(spec, Arc::clone(&arc));
        arc
    }

    pub fn get_index(&self, spec: &JoinKeySpec) -> Option<Arc<RwLock<HashIndex>>> {
        self.indexes.get(spec).map(Arc::clone)
    }

    /// Get an existing index or create a new one.
    pub fn get_or_create(&mut self, spec: JoinKeySpec) -> Arc<RwLock<HashIndex>> {
        if let Some(existing) = self.indexes.get(&spec) {
            return Arc::clone(existing);
        }
        self.create_index(spec)
    }

    /// Record that a join key was used in a query.
    ///
    /// This is used for:
    /// 1. Auto-creation of indexes for frequently-used keys
    /// 2. LRU eviction decisions
    pub fn record_usage(&mut self, spec: &JoinKeySpec) {
        let stats = self.usage_stats.entry(spec.clone()).or_default();
        stats.lookup_count += 1;
        stats.last_used = current_timestamp();

        // Auto-create if threshold reached
        if self.config.auto_create
            && stats.lookup_count >= self.config.auto_create_threshold
            && !self.indexes.contains_key(spec)
        {
            self.create_index(spec.clone());
        }
    }

    /// Drop an index.
    ///
    /// # Returns
    ///
    /// `true` if the index existed and was dropped, `false` otherwise.
    pub fn drop_index(&mut self, spec: &JoinKeySpec) -> bool {
        self.usage_stats.remove(spec);
        self.indexes.remove(spec).is_some()
    }

    /// Invalidate all indexes for a relation.
    ///
    /// Call this when a relation's data changes to mark indexes
    /// as needing rebuild.
    pub fn invalidate_relation(&mut self, relation: &str) {
        let specs: Vec<_> = self
            .indexes
            .keys()
            .filter(|spec| spec.relation == relation)
            .cloned()
            .collect();

        for spec in specs {
            if let Some(index) = self.indexes.get(&spec) {
                // Increment version to signal staleness
                if let Ok(mut idx) = index.write() {
                    idx.version += 1;
                }
            }
        }
    }

    /// Get all indexes for a specific relation.
    pub fn indexes_for_relation(&self, relation: &str) -> Vec<Arc<RwLock<HashIndex>>> {
        self.indexes
            .iter()
            .filter(|(spec, _)| spec.relation == relation)
            .map(|(_, idx)| Arc::clone(idx))
            .collect()
    }

    /// Evict least-recently-used indexes if over the limit.
    pub fn evict_if_needed(&mut self) {
        while self.indexes.len() > self.config.max_indexes {
            // Find LRU index
            let lru_spec = self
                .usage_stats
                .iter()
                .min_by_key(|(_, stats)| stats.last_used)
                .map(|(spec, _)| spec.clone());

            if let Some(spec) = lru_spec {
                self.indexes.remove(&spec);
                self.usage_stats.remove(&spec);
            } else {
                break;
            }
        }
    }

    pub fn index_count(&self) -> usize {
        self.indexes.len()
    }
}

impl Default for HashIndexManager {
    fn default() -> Self {
        Self::new(HashIndexConfig::default())
    }
}

/// Get current timestamp in milliseconds.
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tuple(values: Vec<i64>) -> Tuple {
        Tuple::new(values.into_iter().map(Value::Int64).collect())
    }

    // HAPPY PATH TESTS
    #[test]
    fn test_hash_index_insert_single_lookup() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 100);

        let tuple = make_tuple(vec![1, 2, 3]);
        index.insert(tuple.clone());

        let key = make_tuple(vec![1]);
        let results: Vec<_> = index.probe(&key).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], &tuple);
    }

    #[test]
    fn test_hash_index_multiple_tuples_same_key() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 100);

        // Insert 3 tuples with key=1
        index.insert(make_tuple(vec![1, 10]));
        index.insert(make_tuple(vec![1, 20]));
        index.insert(make_tuple(vec![1, 30]));

        let key = make_tuple(vec![1]);
        let results: Vec<_> = index.probe(&key).collect();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_hash_index_different_keys_lookup_each() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 100);

        index.insert(make_tuple(vec![1, 100]));
        index.insert(make_tuple(vec![2, 200]));
        index.insert(make_tuple(vec![3, 300]));

        assert_eq!(index.probe(&make_tuple(vec![1])).count(), 1);
        assert_eq!(index.probe(&make_tuple(vec![2])).count(), 1);
        assert_eq!(index.probe(&make_tuple(vec![3])).count(), 1);
    }

    #[test]
    fn test_hash_index_remove_tuple() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 100);

        let tuple = make_tuple(vec![1, 2]);
        index.insert(tuple.clone());
        assert_eq!(index.probe(&make_tuple(vec![1])).count(), 1);

        let removed = index.remove(&tuple);
        assert!(removed);
        assert_eq!(index.probe(&make_tuple(vec![1])).count(), 0);
    }

    #[test]
    fn test_hash_index_build_from_tuples() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 100);

        let tuples = vec![
            make_tuple(vec![1, 10]),
            make_tuple(vec![2, 20]),
            make_tuple(vec![1, 11]),
        ];
        index.build_from_tuples(tuples);

        assert_eq!(index.stats().num_tuples, 3);
        assert_eq!(index.stats().num_keys, 2);
    }

    #[test]
    fn test_hash_index_bloom_accelerates_negative() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 1000);

        // Insert keys 0-999
        for i in 0..1000 {
            index.insert(make_tuple(vec![i, i * 10]));
        }

        // Key 9999 should fail Bloom check quickly
        let key = make_tuple(vec![9999]);
        assert!(!index.might_contain_key(&key));

        // Probe should return empty
        assert_eq!(index.probe(&key).count(), 0);
    }

    // EDGE CASE TESTS
    #[test]
    fn test_hash_index_empty_returns_empty() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let index = HashIndex::new(spec, 100);

        let key = make_tuple(vec![1]);
        assert_eq!(index.probe(&key).count(), 0);
    }

    #[test]
    fn test_hash_index_single_tuple() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 1);

        index.insert(make_tuple(vec![42, 100]));

        assert_eq!(index.probe(&make_tuple(vec![42])).count(), 1);
        assert_eq!(index.probe(&make_tuple(vec![43])).count(), 0);
    }

    #[test]
    fn test_hash_index_key_not_found_returns_empty() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 100);

        index.insert(make_tuple(vec![1, 10]));
        index.insert(make_tuple(vec![2, 20]));

        // Key 999 doesn't exist
        assert_eq!(index.probe(&make_tuple(vec![999])).count(), 0);
    }

    #[test]
    fn test_hash_index_remove_nonexistent_returns_false() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 100);

        index.insert(make_tuple(vec![1, 10]));

        // Try to remove tuple that doesn't exist
        let removed = index.remove(&make_tuple(vec![999, 999]));
        assert!(!removed);
    }

    #[test]
    fn test_hash_index_remove_last_tuple_removes_key() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 100);

        let tuple = make_tuple(vec![1, 10]);
        index.insert(tuple.clone());
        assert_eq!(index.stats().num_keys, 1);

        index.remove(&tuple);
        assert_eq!(index.stats().num_keys, 0);
        assert_eq!(index.stats().num_tuples, 0);
    }

    #[test]
    fn test_hash_index_multi_column_key() {
        // Key on columns 0 AND 1
        let spec = JoinKeySpec::new("rel", vec![0, 1]);
        let mut index = HashIndex::new(spec, 100);

        index.insert(make_tuple(vec![1, 2, 100]));
        index.insert(make_tuple(vec![1, 2, 200])); // Same key
        index.insert(make_tuple(vec![1, 3, 300])); // Different key

        // Key (1, 2) should return 2 tuples
        let results: Vec<_> = index.probe(&make_tuple(vec![1, 2])).collect();
        assert_eq!(results.len(), 2);

        // Key (1, 3) should return 1 tuple
        let results: Vec<_> = index.probe(&make_tuple(vec![1, 3])).collect();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_hash_index_null_in_key() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 100);

        // Tuple with null key
        let tuple_with_null = Tuple::new(vec![Value::Null, Value::Int64(10)]);
        index.insert(tuple_with_null.clone());

        // Should be able to look up by null key
        let key = Tuple::new(vec![Value::Null]);
        let results: Vec<_> = index.probe(&key).collect();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_hash_index_wide_tuple_100_columns() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 100);

        // Create tuple with 100 columns
        let values: Vec<Value> = (0..100).map(Value::Int64).collect();
        let wide_tuple = Tuple::new(values);
        index.insert(wide_tuple.clone());

        let key = make_tuple(vec![0]);
        let results: Vec<_> = index.probe(&key).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].arity(), 100);
    }

    #[test]
    fn test_hash_index_version_increments_on_modification() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 100);

        let v0 = index.version();

        index.insert(make_tuple(vec![1, 10]));
        let v1 = index.version();
        assert!(v1 > v0);

        index.insert(make_tuple(vec![2, 20]));
        let v2 = index.version();
        assert!(v2 > v1);

        index.remove(&make_tuple(vec![1, 10]));
        let v3 = index.version();
        assert!(v3 > v2);
    }

    #[test]
    fn test_hash_index_stats_accurate() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 100);

        // Insert 5 tuples with 3 distinct keys
        index.insert(make_tuple(vec![1, 10]));
        index.insert(make_tuple(vec![1, 11]));
        index.insert(make_tuple(vec![2, 20]));
        index.insert(make_tuple(vec![3, 30]));
        index.insert(make_tuple(vec![3, 31]));

        let stats = index.stats();
        assert_eq!(stats.num_tuples, 5);
        assert_eq!(stats.num_keys, 3);
        assert!((stats.avg_tuples_per_key - 5.0 / 3.0).abs() < 0.01);
        assert_eq!(stats.max_tuples_per_key, 2);
    }

    #[test]
    fn test_hash_index_len_and_is_empty() {
        let spec = JoinKeySpec::new("rel", vec![0]);
        let mut index = HashIndex::new(spec, 100);

        assert!(index.is_empty());
        assert_eq!(index.len(), 0);

        index.insert(make_tuple(vec![1, 10]));
        assert!(!index.is_empty());
        assert_eq!(index.len(), 1);
    }

    // HASH INDEX MANAGER TESTS
    #[test]
    fn test_hash_index_manager_get_or_create() {
        let mut manager = HashIndexManager::new(HashIndexConfig::default());

        let spec = JoinKeySpec::new("edge", vec![0]);
        let index1 = manager.get_or_create(spec.clone());
        let index2 = manager.get_or_create(spec.clone());

        // Should return same index (Arc pointer equality)
        assert!(Arc::ptr_eq(&index1, &index2));
    }

    #[test]
    fn test_hash_index_manager_auto_create_threshold() {
        let config = HashIndexConfig {
            auto_create: true,
            auto_create_threshold: 3,
            ..Default::default()
        };
        let mut manager = HashIndexManager::new(config);

        let spec = JoinKeySpec::new("edge", vec![0]);

        // Record usage below threshold
        manager.record_usage(&spec);
        manager.record_usage(&spec);
        assert!(manager.get_index(&spec).is_none());

        // Third usage triggers auto-creation
        manager.record_usage(&spec);
        assert!(manager.get_index(&spec).is_some());
    }

    #[test]
    fn test_hash_index_manager_drop_index() {
        let mut manager = HashIndexManager::new(HashIndexConfig::default());

        let spec = JoinKeySpec::new("rel", vec![0]);
        manager.create_index(spec.clone());
        assert!(manager.get_index(&spec).is_some());

        let dropped = manager.drop_index(&spec);
        assert!(dropped);
        assert!(manager.get_index(&spec).is_none());
    }

    #[test]
    fn test_hash_index_manager_invalidate_relation() {
        let mut manager = HashIndexManager::new(HashIndexConfig::default());

        // Create two indexes for same relation
        let spec1 = JoinKeySpec::new("edge", vec![0]);
        let spec2 = JoinKeySpec::new("edge", vec![1]);

        let idx1 = manager.create_index(spec1.clone());
        let idx2 = manager.create_index(spec2.clone());

        let v1_before = idx1.read().unwrap().version();
        let v2_before = idx2.read().unwrap().version();

        // Invalidate relation
        manager.invalidate_relation("edge");

        let v1_after = idx1.read().unwrap().version();
        let v2_after = idx2.read().unwrap().version();

        // Versions should have changed
        assert!(v1_after > v1_before);
        assert!(v2_after > v2_before);
    }

    #[test]
    fn test_hash_index_manager_index_count() {
        let mut manager = HashIndexManager::new(HashIndexConfig::default());

        assert_eq!(manager.index_count(), 0);

        manager.create_index(JoinKeySpec::new("a", vec![0]));
        assert_eq!(manager.index_count(), 1);

        manager.create_index(JoinKeySpec::new("b", vec![0]));
        assert_eq!(manager.index_count(), 2);

        // Same spec doesn't create new index
        manager.create_index(JoinKeySpec::new("a", vec![0]));
        assert_eq!(manager.index_count(), 2);
    }

    #[test]
    fn test_join_key_spec_display_name() {
        let spec = JoinKeySpec::new("edge", vec![0, 1]);
        assert_eq!(spec.display_name(), "edge[0,1]");

        let spec2 = JoinKeySpec::new("node", vec![0]);
        assert_eq!(spec2.display_name(), "node[0]");
    }
}
