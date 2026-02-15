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

impl Default for ExecutionConfig {
    fn default() -> Self {
        ExecutionConfig {
            timeout: Some(Duration::from_secs(60)), // 1 minute default timeout
            limits: ResourceLimits::default(),
            enable_query_cache: true,
            enable_result_cache: true,
            max_cache_entries: 1000,
            result_cache_ttl: Duration::from_secs(300), // 5 minutes
        }
    }
}

impl ExecutionConfig {
    /// Create a new configuration with no limits (for testing)
    pub fn unlimited() -> Self {
        ExecutionConfig {
            timeout: None,
            limits: ResourceLimits::unlimited(),
            enable_query_cache: false,
            enable_result_cache: false,
            max_cache_entries: 0,
            result_cache_ttl: Duration::from_secs(0),
        }
    }

    /// Set the query timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Disable timeout
    pub fn without_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    /// Set maximum results
    pub fn with_max_results(mut self, max: usize) -> Self {
        self.limits.max_result_size = Some(max);
        self
    }

    /// Set memory limit
    pub fn with_memory_limit(mut self, bytes: usize) -> Self {
        self.limits.max_memory_bytes = Some(bytes);
        self
    }

    /// Enable or disable query caching
    pub fn with_query_cache(mut self, enabled: bool) -> Self {
        self.enable_query_cache = enabled;
        self
    }

    /// Enable or disable result caching
    pub fn with_result_cache(mut self, enabled: bool) -> Self {
        self.enable_result_cache = enabled;
        self
    }
}

/// Execution error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum ExecutionError {
    /// Query timed out
    #[error("Query timeout: {0}")]
    Timeout(#[from] TimeoutError),

    /// Resource limit exceeded
    #[error("Resource limit exceeded: {0}")]
    ResourceLimit(#[from] ResourceError),

    /// Query execution error
    #[error("Query error: {0}")]
    QueryError(String),

    /// Parse error
    #[error("Parse error: {0}")]
    ParseError(String),
}

/// Result type for execution operations
pub type ExecutionResult<T> = Result<T, ExecutionError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ExecutionConfig::default();
        assert!(config.timeout.is_some());
        assert_eq!(config.timeout, Some(Duration::from_secs(60)));
        assert!(config.enable_query_cache);
        assert!(config.enable_result_cache);
        assert_eq!(config.max_cache_entries, 1000);
        assert_eq!(config.result_cache_ttl, Duration::from_secs(300));
    }

    #[test]
    fn test_unlimited_config() {
        let config = ExecutionConfig::unlimited();
        assert!(config.timeout.is_none());
        assert!(!config.enable_query_cache);
        assert!(!config.enable_result_cache);
        assert_eq!(config.max_cache_entries, 0);
    }

    #[test]
    fn test_config_builder() {
        let config = ExecutionConfig::default()
            .with_timeout(Duration::from_secs(30))
            .with_max_results(50_000)
            .with_memory_limit(1024 * 1024 * 100); // 100MB

        assert_eq!(config.timeout, Some(Duration::from_secs(30)));
        assert_eq!(config.limits.max_result_size, Some(50_000));
        assert_eq!(config.limits.max_memory_bytes, Some(100 * 1024 * 1024));
    }

    #[test]
    fn test_without_timeout() {
        let config = ExecutionConfig::default().without_timeout();
        assert!(config.timeout.is_none());
    }

    #[test]
    fn test_with_query_cache() {
        let config = ExecutionConfig::default().with_query_cache(false);
        assert!(!config.enable_query_cache);
    }

    #[test]
    fn test_with_result_cache() {
        let config = ExecutionConfig::default().with_result_cache(false);
        assert!(!config.enable_result_cache);
    }

    #[test]
    fn test_builder_chaining() {
        let config = ExecutionConfig::unlimited()
            .with_timeout(Duration::from_secs(10))
            .with_max_results(100)
            .with_memory_limit(1024)
            .with_query_cache(true)
            .with_result_cache(true);

        assert_eq!(config.timeout, Some(Duration::from_secs(10)));
        assert_eq!(config.limits.max_result_size, Some(100));
        assert_eq!(config.limits.max_memory_bytes, Some(1024));
        assert!(config.enable_query_cache);
        assert!(config.enable_result_cache);
    }

    #[test]
    fn test_execution_error_display() {
        let err = ExecutionError::QueryError("test error".to_string());
        assert_eq!(format!("{err}"), "Query error: test error");

        let err = ExecutionError::ParseError("bad syntax".to_string());
        assert_eq!(format!("{err}"), "Parse error: bad syntax");
    }
}
