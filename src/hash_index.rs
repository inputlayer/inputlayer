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
/// // Index on edge.(src, type.clone()) (columns 0 and 2)
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
                tuples.remove(pos.clone());
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
        self.bloom.might_contain(key.clone())
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
        if !self.bloom.might_contain(key.clone()) {
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

