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
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

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
    /// Compiled query cache (query string hash -> IR)
    compiled: Arc<RwLock<HashMap<u64, CacheEntry<Vec<IRNode>>>>>,

    /// Result cache (query + data hash -> results)
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
        let mut cache = self.compiled.write();

        if let Some(entry) = cache.get_mut(&hash) {
            if entry.is_expired() {
                cache.remove(&hash);
                let mut stats = self.stats.write();
                stats.misses += 1;
                stats.expirations += 1;
                return None;
            }

            entry.touch();
            let mut stats = self.stats.write();
            stats.hits += 1;
            return Some(entry.value.clone());
        }

        let mut stats = self.stats.write();
        stats.misses += 1;
        None
    }

    /// Store compiled IR in cache
    pub fn put_compiled(&self, query: &str, ir: Vec<IRNode>) {
        let hash = Self::query_hash(query);
        let mut cache = self.compiled.write();

        // Evict if at capacity
        if cache.len() >= self.max_compiled_entries {
            self.evict_lru(&mut cache);
        }

        cache.insert(hash, CacheEntry::new(ir, None)); // Compiled queries don't expire

        let mut stats = self.stats.write();
        stats.size = cache.len();
    }

    /// Get cached results
    pub fn get_results(&self, query: &str, data_fingerprint: u64) -> Option<Vec<(i32, i32)>> {
        let hash = Self::query_data_hash(query, data_fingerprint);
        let mut cache = self.results.write();

        if let Some(entry) = cache.get_mut(&hash) {
            if entry.is_expired() {
                cache.remove(&hash);
                let mut stats = self.stats.write();
                stats.misses += 1;
                stats.expirations += 1;
                return None;
            }

            entry.touch();
            let mut stats = self.stats.write();
            stats.hits += 1;
            return Some(entry.value.clone());
        }

        let mut stats = self.stats.write();
        stats.misses += 1;
        None
    }

    /// Store results in cache
    pub fn put_results(&self, query: &str, data_fingerprint: u64, results: Vec<(i32, i32)>) {
        let hash = Self::query_data_hash(query, data_fingerprint);
        let mut cache = self.results.write();

        // Evict if at capacity
        if cache.len() >= self.max_result_entries {
            self.evict_lru_results(&mut cache);
        }

        cache.insert(hash, CacheEntry::new(results, Some(self.result_ttl)));
    }

    /// Evict the LRU entry (oldest last_accessed).
    fn evict_lru<T: Clone>(&self, cache: &mut HashMap<u64, CacheEntry<T>>) {
        if let Some((&key_to_remove, _)) = cache.iter().min_by_key(|(_, entry)| entry.last_accessed)
        {
            cache.remove(&key_to_remove);
            let mut stats = self.stats.write();
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
                let mut stats = self.stats.write();
                stats.expirations += 1;
            }
            return;
        }

        // Otherwise evict LRU
        if let Some((&key_to_remove, _)) = cache.iter().min_by_key(|(_, entry)| entry.last_accessed)
        {
            cache.remove(&key_to_remove);
            let mut stats = self.stats.write();
            stats.evictions += 1;
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        self.stats.read().clone()
    }

    /// Reset statistics
    pub fn reset_stats(&self) {
        self.stats.write().reset();
    }

    /// Clear all caches
    pub fn clear(&self) {
        self.compiled.write().clear();
        self.results.write().clear();

        let mut stats = self.stats.write();
        stats.size = 0;
    }

    /// Clear only result cache (useful when data changes)
    pub fn clear_results(&self) {
        self.results.write().clear();
    }

    /// Invalidate cache entries for a specific query
    pub fn invalidate_query(&self, query: &str) {
        let hash = Self::query_hash(query);
        self.compiled.write().remove(&hash);
        // Note: Result cache entries with this query are harder to invalidate
        // since they also depend on data fingerprint. For full invalidation,
        // use clear_results().
    }

    /// Get the number of entries in compiled cache
    pub fn compiled_size(&self) -> usize {
        self.compiled.read().len()
    }

    /// Get the number of entries in results cache
    pub fn results_size(&self) -> usize {
        self.results.read().len()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_entry_expiration() {
        let entry: CacheEntry<i32> = CacheEntry::new(42, Some(Duration::from_millis(50)));

        assert!(!entry.is_expired());

        std::thread::sleep(Duration::from_millis(150));

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

    // === Additional Coverage ===

    #[test]
    fn test_cache_entry_touch() {
        let mut entry: CacheEntry<i32> = CacheEntry::new(42, None);
        assert_eq!(entry.access_count, 1);
        entry.touch();
        assert_eq!(entry.access_count, 2);
        entry.touch();
        assert_eq!(entry.access_count, 3);
    }

    #[test]
    fn test_cache_entry_age_and_idle() {
        let entry: CacheEntry<i32> = CacheEntry::new(42, None);
        std::thread::sleep(Duration::from_millis(50));
        assert!(entry.age() >= Duration::from_millis(50));
        assert!(entry.idle_time() >= Duration::from_millis(50));
    }

    #[test]
    fn test_hit_rate_zero_total() {
        let stats = CacheStats::default();
        assert_eq!(stats.hit_rate(), 0.0);
    }

    #[test]
    fn test_hit_rate_all_hits() {
        let stats = CacheStats {
            hits: 100,
            misses: 0,
            ..Default::default()
        };
        assert!((stats.hit_rate() - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_reset_stats() {
        let mut stats = CacheStats {
            hits: 10,
            misses: 5,
            evictions: 3,
            expirations: 1,
            size: 20,
        };
        stats.reset();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.evictions, 0);
        assert_eq!(stats.expirations, 0);
        // size is NOT reset by reset()
    }

    #[test]
    fn test_cache_default() {
        let cache = QueryCache::default();
        assert_eq!(cache.compiled_size(), 0);
        assert_eq!(cache.results_size(), 0);
    }

    #[test]
    fn test_cache_with_defaults() {
        let cache = QueryCache::with_defaults();
        assert_eq!(cache.max_compiled_entries, 1000);
        assert_eq!(cache.max_result_entries, 1000);
    }

    #[test]
    fn test_clear_results_only() {
        let cache = QueryCache::new(100, 100, Duration::from_secs(60));
        cache.put_compiled("query", vec![]);
        cache.put_results("query", 123, vec![(1, 2)]);

        cache.clear_results();

        assert_eq!(cache.compiled_size(), 1); // compiled not cleared
        assert_eq!(cache.results_size(), 0); // results cleared
    }

    #[test]
    fn test_invalidate_query() {
        let cache = QueryCache::new(100, 100, Duration::from_secs(60));
        cache.put_compiled("query1", vec![]);
        cache.put_compiled("query2", vec![]);

        cache.invalidate_query("query1");

        assert!(cache.get_compiled("query1").is_none());
        assert!(cache.get_compiled("query2").is_some());
    }

    #[test]
    fn test_result_cache_expiration() {
        let cache = QueryCache::new(100, 100, Duration::from_millis(50));
        cache.put_results("query", 1, vec![(1, 2)]);

        assert!(cache.get_results("query", 1).is_some());

        std::thread::sleep(Duration::from_millis(150));

        assert!(cache.get_results("query", 1).is_none());
    }

    #[test]
    fn test_compiled_cache_no_expiration() {
        let cache = QueryCache::new(100, 100, Duration::from_millis(50));
        cache.put_compiled("query", vec![]);

        std::thread::sleep(Duration::from_millis(150));

        // Compiled cache entries have no TTL
        assert!(cache.get_compiled("query").is_some());
    }

    #[test]
    fn test_result_cache_eviction_prefers_expired() {
        let cache = QueryCache::new(100, 2, Duration::from_millis(50));
        cache.put_results("a", 1, vec![(1, 2)]);
        cache.put_results("b", 1, vec![(3, 4)]);

        std::thread::sleep(Duration::from_millis(150));

        // Adding a new entry should evict expired ones
        cache.put_results("c", 1, vec![(5, 6)]);
        // cache size should be 1 (only "c" survives)
        assert_eq!(cache.results_size(), 1);
    }

    #[test]
    fn test_stats_tracking() {
        let cache = QueryCache::new(100, 100, Duration::from_secs(60));

        cache.get_compiled("miss1"); // miss
        cache.get_compiled("miss2"); // miss
        cache.put_compiled("hit", vec![]);
        cache.get_compiled("hit"); // hit

        let stats = cache.stats();
        assert_eq!(stats.misses, 2);
        assert_eq!(stats.hits, 1);
    }

    #[test]
    fn test_reset_stats_on_cache() {
        let cache = QueryCache::new(100, 100, Duration::from_secs(60));
        cache.get_compiled("miss"); // miss
        assert_eq!(cache.stats().misses, 1);

        cache.reset_stats();
        assert_eq!(cache.stats().misses, 0);
        assert_eq!(cache.stats().hits, 0);
    }

    #[test]
    fn test_cache_clone_shares_state() {
        let cache1 = QueryCache::new(100, 100, Duration::from_secs(60));
        let cache2 = cache1.clone();

        cache1.put_compiled("shared", vec![]);
        assert!(cache2.get_compiled("shared").is_some());
    }
}
