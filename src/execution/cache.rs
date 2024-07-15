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
