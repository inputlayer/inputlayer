//! Query Cache Module
//!
//! Provides caching for:
//! - Compiled queries (IR nodes)
//! - Query results
//!
//! ## Design
//!
//! Uses LRU (Least Recently Used) eviction with configurable size limits.
//! Cache entries have TTL (time-to-live) for result caching.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::ir::IRNode;

/// Cache entry for compiled queries
#[derive(Clone)]
pub struct CacheEntry<T> {
    /// The cached value
    pub value: T,

    /// When the entry was created
    pub created_at: Instant,

    /// When the entry was last accessed
    pub last_accessed: Instant,

    /// Number of times this entry has been accessed
    pub access_count: usize,

    /// Time-to-live (None = no expiration)
    pub ttl: Option<Duration>,
}

impl<T> CacheEntry<T> {
    /// Create a new cache entry
    pub fn new(value: T, ttl: Option<Duration>) -> Self {
        let now = Instant::now();
        CacheEntry {
            value,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            ttl,
        }
    }

    /// Check if the entry has expired
    pub fn is_expired(&self) -> bool {
        if let Some(ttl) = self.ttl {
            self.created_at.elapsed() > ttl
        } else {
            false
        }
    }

    /// Mark the entry as accessed
    pub fn touch(&mut self) {
        self.last_accessed = Instant::now();
        self.access_count += 1;
    }

    /// Get the age of this entry
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Get time since last access
    pub fn idle_time(&self) -> Duration {
        self.last_accessed.elapsed()
    }
}

/// Cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of cache hits
    pub hits: usize,

    /// Number of cache misses
    pub misses: usize,

    /// Number of entries currently in cache
    pub size: usize,

    /// Number of entries evicted
    pub evictions: usize,

    /// Number of entries expired
    pub expirations: usize,
}

impl CacheStats {
    /// Calculate hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Reset statistics
    pub fn reset(&mut self) {
        self.hits = 0;
        self.misses = 0;
        self.evictions = 0;
        self.expirations = 0;
    }
}

/// Query cache for storing compiled IR and results
///
/// Thread-safe LRU cache with TTL support.
pub struct QueryCache {
    /// Compiled query cache (query string hash → IR)
    compiled: Arc<RwLock<HashMap<u64, CacheEntry<Vec<IRNode>>>>>,

    /// Result cache (query + data hash → results)
    results: Arc<RwLock<HashMap<u64, CacheEntry<Vec<(i32, i32)>>>>>,

    /// Maximum entries in compiled cache
    max_compiled_entries: usize,

    /// Maximum entries in result cache
    max_result_entries: usize,

    /// Default TTL for result cache entries
    result_ttl: Duration,

    /// Statistics
    stats: Arc<RwLock<CacheStats>>,
}

impl QueryCache {
    /// Create a new query cache with specified limits
    pub fn new(max_compiled: usize, max_results: usize, result_ttl: Duration) -> Self {
        QueryCache {
            compiled: Arc::new(RwLock::new(HashMap::new())),
            results: Arc::new(RwLock::new(HashMap::new())),
            max_compiled_entries: max_compiled,
            max_result_entries: max_results,
            result_ttl,
            stats: Arc::new(RwLock::new(CacheStats::default())),
        }
    }

    /// Create a cache with default settings
    pub fn with_defaults() -> Self {
        QueryCache::new(1000, 1000, Duration::from_secs(300))
    }

    /// Compute a hash for a query string
    fn query_hash(query: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        query.hash(&mut hasher);
        hasher.finish()
    }

    /// Compute a hash for query + data combination
    fn query_data_hash(query: &str, data_fingerprint: u64) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        query.hash(&mut hasher);
        data_fingerprint.hash(&mut hasher);
        hasher.finish()
    }

    /// Get compiled IR from cache
    pub fn get_compiled(&self, query: &str) -> Option<Vec<IRNode>> {
        let hash = Self::query_hash(query);
        let mut cache = self.compiled.write().unwrap();

        if let Some(entry) = cache.get_mut(&hash) {
            if entry.is_expired() {
                cache.remove(&hash);
                let mut stats = self.stats.write().unwrap();
                stats.misses += 1;
                stats.expirations += 1;
                return None;
            }

            entry.touch();
            let mut stats = self.stats.write().unwrap();
            stats.hits += 1;
            return Some(entry.value.clone());
        }

        let mut stats = self.stats.write().unwrap();
        stats.misses += 1;
        None
    }

    /// Store compiled IR in cache
    pub fn put_compiled(&self, query: &str, ir: Vec<IRNode>) {
        let hash = Self::query_hash(query);
        let mut cache = self.compiled.write().unwrap();

        // Evict if at capacity
        if cache.len() >= self.max_compiled_entries {
            self.evict_lru(&mut cache);
        }

        cache.insert(hash, CacheEntry::new(ir, None)); // Compiled queries don't expire

        let mut stats = self.stats.write().unwrap();
        stats.size = cache.len();
    }

    /// Get cached results
    pub fn get_results(&self, query: &str, data_fingerprint: u64) -> Option<Vec<(i32, i32)>> {
        let hash = Self::query_data_hash(query, data_fingerprint);
        let mut cache = self.results.write().unwrap();

        if let Some(entry) = cache.get_mut(&hash) {
            if entry.is_expired() {
                cache.remove(&hash);
                let mut stats = self.stats.write().unwrap();
                stats.misses += 1;
                stats.expirations += 1;
                return None;
            }

            entry.touch();
            let mut stats = self.stats.write().unwrap();
            stats.hits += 1;
            return Some(entry.value.clone());
        }

        let mut stats = self.stats.write().unwrap();
        stats.misses += 1;
        None
    }

    /// Store results in cache
    pub fn put_results(&self, query: &str, data_fingerprint: u64, results: Vec<(i32, i32)>) {
        let hash = Self::query_data_hash(query, data_fingerprint);
        let mut cache = self.results.write().unwrap();

        // Evict if at capacity
        if cache.len() >= self.max_result_entries {
            self.evict_lru_results(&mut cache);
        }

        cache.insert(hash, CacheEntry::new(results, Some(self.result_ttl)));
    }

    /// Evict the least recently used entry from compiled cache
    fn evict_lru<T: Clone>(&self, cache: &mut HashMap<u64, CacheEntry<T>>) {
        // Find entry with oldest last_accessed time
        if let Some((&key_to_remove, _)) = cache
            .iter()
            .min_by_key(|(_, entry)| entry.last_accessed)
        {
            cache.remove(&key_to_remove);
            let mut stats = self.stats.write().unwrap();
            stats.evictions += 1;
        }
    }

    /// Evict the least recently used entry from results cache
    fn evict_lru_results(&self, cache: &mut HashMap<u64, CacheEntry<Vec<(i32, i32)>>>) {
        // First try to evict expired entries
        let expired: Vec<_> = cache
            .iter()
            .filter(|(_, entry)| entry.is_expired())
            .map(|(&k, _)| k)
            .collect();

        if !expired.is_empty() {
            for key in expired {
                cache.remove(&key);
                let mut stats = self.stats.write().unwrap();
                stats.expirations += 1;
            }
            return;
        }

        // Otherwise evict LRU
        if let Some((&key_to_remove, _)) = cache
            .iter()
            .min_by_key(|(_, entry)| entry.last_accessed)
        {
            cache.remove(&key_to_remove);
            let mut stats = self.stats.write().unwrap();
            stats.evictions += 1;
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        self.stats.read().unwrap().clone()
    }

    /// Reset statistics
    pub fn reset_stats(&self) {
        self.stats.write().unwrap().reset();
    }

    /// Clear all caches
    pub fn clear(&self) {
        self.compiled.write().unwrap().clear();
        self.results.write().unwrap().clear();

        let mut stats = self.stats.write().unwrap();
        stats.size = 0;
    }

    /// Clear only result cache (useful when data changes)
    pub fn clear_results(&self) {
        self.results.write().unwrap().clear();
    }

    /// Invalidate cache entries for a specific query
    pub fn invalidate_query(&self, query: &str) {
        let hash = Self::query_hash(query);
        self.compiled.write().unwrap().remove(&hash);
        // Note: Result cache entries with this query are harder to invalidate
        // since they also depend on data fingerprint. For full invalidation,
        // use clear_results().
    }

    /// Get the number of entries in compiled cache
    pub fn compiled_size(&self) -> usize {
        self.compiled.read().unwrap().len()
    }

    /// Get the number of entries in results cache
    pub fn results_size(&self) -> usize {
        self.results.read().unwrap().len()
    }
}

impl Default for QueryCache {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl Clone for QueryCache {
    fn clone(&self) -> Self {
        QueryCache {
            compiled: Arc::clone(&self.compiled),
            results: Arc::clone(&self.results),
            max_compiled_entries: self.max_compiled_entries,
            max_result_entries: self.max_result_entries,
            result_ttl: self.result_ttl,
            stats: Arc::clone(&self.stats),
        }
    }
}

/// Compute a fingerprint for input data
///
/// Used to detect when cached results may be stale.
pub fn compute_data_fingerprint(data: &HashMap<String, Vec<(i32, i32)>>) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();

    // Sort keys for deterministic ordering
    let mut keys: Vec<_> = data.keys().collect();
    keys.sort();

    for key in keys {
        key.hash(&mut hasher);
        if let Some(tuples) = data.get(key) {
            tuples.len().hash(&mut hasher);
            // Hash first and last tuples as a quick fingerprint
            if let Some(first) = tuples.first() {
                first.hash(&mut hasher);
            }
            if let Some(last) = tuples.last() {
                last.hash(&mut hasher);
            }
        }
    }

    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_entry_expiration() {
        let entry: CacheEntry<i32> = CacheEntry::new(42, Some(Duration::from_millis(10)));

        assert!(!entry.is_expired());

        std::thread::sleep(Duration::from_millis(20));

        assert!(entry.is_expired());
    }

    #[test]
    fn test_cache_entry_no_expiration() {
        let entry: CacheEntry<i32> = CacheEntry::new(42, None);

        assert!(!entry.is_expired());

        std::thread::sleep(Duration::from_millis(10));

        assert!(!entry.is_expired());
    }

    #[test]
    fn test_cache_hit_miss() {
        let cache = QueryCache::new(100, 100, Duration::from_secs(60));

        // Miss
        assert!(cache.get_compiled("test query").is_none());
        assert_eq!(cache.stats().misses, 1);

        // Insert
        cache.put_compiled("test query", vec![]);

        // Hit
        assert!(cache.get_compiled("test query").is_some());
        assert_eq!(cache.stats().hits, 1);
    }

    #[test]
    fn test_cache_eviction() {
        let cache = QueryCache::new(2, 2, Duration::from_secs(60));

        // Fill cache
        cache.put_compiled("query1", vec![]);
        cache.put_compiled("query2", vec![]);

        // Access query1 to make it more recent
        cache.get_compiled("query1");

        // Add third entry - should evict query2 (LRU)
        cache.put_compiled("query3", vec![]);

        assert_eq!(cache.compiled_size(), 2);
        assert!(cache.get_compiled("query1").is_some()); // Still there
        assert!(cache.get_compiled("query3").is_some()); // Still there
    }

    #[test]
    fn test_result_cache_with_fingerprint() {
        let cache = QueryCache::new(100, 100, Duration::from_secs(60));

        let query = "test query";
        let results = vec![(1, 2), (3, 4)];

        // Different fingerprints should be different cache entries
        cache.put_results(query, 123, results.clone());
        cache.put_results(query, 456, vec![(5, 6)]);

        assert_eq!(cache.get_results(query, 123), Some(results));
        assert_eq!(cache.get_results(query, 456), Some(vec![(5, 6)]));
        assert!(cache.get_results(query, 789).is_none());
    }

    #[test]
    fn test_hit_rate() {
        let stats = CacheStats {
            hits: 75,
            misses: 25,
            ..Default::default()
        };

        assert!((stats.hit_rate() - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_data_fingerprint() {
        let mut data1 = HashMap::new();
        data1.insert("edge".to_string(), vec![(1, 2), (3, 4)]);

        let mut data2 = HashMap::new();
        data2.insert("edge".to_string(), vec![(1, 2), (3, 4)]);

        let mut data3 = HashMap::new();
        data3.insert("edge".to_string(), vec![(1, 2), (5, 6)]);

        // Same data should have same fingerprint
        assert_eq!(compute_data_fingerprint(&data1), compute_data_fingerprint(&data2));

        // Different data should have different fingerprint
        assert_ne!(compute_data_fingerprint(&data1), compute_data_fingerprint(&data3));
    }

    #[test]
    fn test_clear_caches() {
        let cache = QueryCache::new(100, 100, Duration::from_secs(60));

        cache.put_compiled("query", vec![]);
        cache.put_results("query", 123, vec![(1, 2)]);

        assert_eq!(cache.compiled_size(), 1);
        assert_eq!(cache.results_size(), 1);

        cache.clear();

        assert_eq!(cache.compiled_size(), 0);
        assert_eq!(cache.results_size(), 0);
    }
}
