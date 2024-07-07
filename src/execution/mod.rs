//! Query Execution Module
//!
//! Provides production-grade query execution with:
//! - Timeout enforcement
//! - Resource limits (memory, result size)
//! - Query caching (compiled queries and results)
//!
//! ## Example
//!
//! ```rust,no_run
//! use inputlayer::execution::{ExecutionConfig, ResourceLimits};
//! use std::time::Duration;
//!
//! let config = ExecutionConfig::default()
//!     .with_timeout(Duration::from_secs(30))
//!     .with_max_results(100_000);
//! ```

mod cache;
mod limits;
mod timeout;

pub use cache::{CacheEntry, CacheStats, QueryCache};
pub use limits::{MemoryTracker, ResourceError, ResourceLimits};
pub use timeout::{CancelHandle, QueryTimeout, TimeoutError};

use std::time::Duration;

/// Configuration for query execution
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    /// Query timeout duration (None = no timeout)
    pub timeout: Option<Duration>,

    /// Resource limits
    pub limits: ResourceLimits,

    /// Whether to cache compiled queries
    pub enable_query_cache: bool,

    /// Whether to cache query results
    pub enable_result_cache: bool,

    /// Maximum cache size (number of entries)
    pub max_cache_entries: usize,

    /// Result cache TTL (time-to-live)
    pub result_cache_ttl: Duration,
}

