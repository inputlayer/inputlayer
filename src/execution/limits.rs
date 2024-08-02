//! Resource Limits Module
//!
//! Provides resource limit enforcement for query execution:
//! - Memory usage limits
//! - Result set size limits
//! - Intermediate result limits
//!
//! ## Design
//!
//! Uses cooperative checking - query execution code should periodically
//! call `check_*` methods to verify limits are not exceeded.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Resource limit error
#[derive(Debug, Clone, thiserror::Error)]
pub enum ResourceError {
    /// Memory limit exceeded
    #[error("Memory limit exceeded: used {used} bytes, limit {limit} bytes")]
    MemoryLimitExceeded { limit: usize, used: usize },

    /// Result size limit exceeded
    #[error("Result size limit exceeded: {actual} tuples, limit {limit} tuples")]
    ResultSizeLimitExceeded { limit: usize, actual: usize },

    /// Intermediate result size exceeded
    #[error(
        "Intermediate result limit exceeded at '{stage}': {actual} tuples, limit {limit} tuples"
    )]
    IntermediateResultExceeded {
        limit: usize,
        actual: usize,
        stage: String,
    },

    /// Row width (tuple arity) exceeded
    #[error("Row width limit exceeded: {actual} columns, limit {limit} columns")]
    RowWidthExceeded { limit: usize, actual: usize },
}

/// Resource limits configuration
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum memory usage in bytes (None = unlimited)
    pub max_memory_bytes: Option<usize>,

    /// Maximum number of tuples in final result (None = unlimited)
    pub max_result_size: Option<usize>,

    /// Maximum number of tuples in intermediate results (None = unlimited)
    pub max_intermediate_size: Option<usize>,

    /// Maximum row width (number of columns per tuple)
    pub max_row_width: Option<usize>,

    /// Maximum recursion depth for fixpoint iterations
    pub max_recursion_depth: Option<usize>,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        ResourceLimits {
            max_memory_bytes: Some(1024 * 1024 * 1024), // 1 GB
            max_result_size: Some(10_000_000),          // 10 million tuples
            max_intermediate_size: Some(100_000_000),   // 100 million tuples
            max_row_width: Some(100),                   // 100 columns
            max_recursion_depth: Some(1000),            // 1000 iterations
        }
    }
}

impl ResourceLimits {
    /// Create limits with no restrictions (for testing)
    pub fn unlimited() -> Self {
        ResourceLimits {
            max_memory_bytes: None,
            max_result_size: None,
            max_intermediate_size: None,
            max_row_width: None,
            max_recursion_depth: None,
        }
    }

    /// Create strict limits for untrusted queries
    pub fn strict() -> Self {
        ResourceLimits {
            max_memory_bytes: Some(100 * 1024 * 1024), // 100 MB
            max_result_size: Some(100_000),            // 100K tuples
            max_intermediate_size: Some(1_000_000),    // 1M tuples
            max_row_width: Some(20),                   // 20 columns
            max_recursion_depth: Some(100),            // 100 iterations
        }
    }

    /// Check if result size is within limits
    pub fn check_result_size(&self, size: usize) -> Result<(), ResourceError> {
        if let Some(limit) = self.max_result_size {
            if size > limit {
                return Err(ResourceError::ResultSizeLimitExceeded {
                    limit,
                    actual: size,
                });
            }
        }
        Ok(())
    }

    /// Check if intermediate result size is within limits
    pub fn check_intermediate_size(&self, size: usize, stage: &str) -> Result<(), ResourceError> {
        if let Some(limit) = self.max_intermediate_size {
            if size > limit {
                return Err(ResourceError::IntermediateResultExceeded {
                    limit,
                    actual: size,
                    stage: stage.to_string(),
                });
            }
        }
        Ok(())
    }

    /// Check if row width is within limits
    pub fn check_row_width(&self, width: usize) -> Result<(), ResourceError> {
        if let Some(limit) = self.max_row_width {
            if width > limit {
                return Err(ResourceError::RowWidthExceeded {
                    limit,
                    actual: width,
                });
            }
        }
        Ok(())
    }

    /// Set maximum memory in bytes
    pub fn with_max_memory(mut self, bytes: usize) -> Self {
        self.max_memory_bytes = Some(bytes);
        self
    }

    /// Set maximum result size
    pub fn with_max_result_size(mut self, tuples: usize) -> Self {
        self.max_result_size = Some(tuples);
        self
    }

    /// Set maximum intermediate size
    pub fn with_max_intermediate_size(mut self, tuples: usize) -> Self {
        self.max_intermediate_size = Some(tuples);
        self
    }
}

/// Memory usage tracker
///
/// Thread-safe tracker for monitoring memory usage during query execution.
/// Uses atomic operations for efficient concurrent updates.
#[derive(Clone)]
pub struct MemoryTracker {
    /// Current memory usage in bytes
    current: Arc<AtomicUsize>,

    /// Peak memory usage in bytes
    peak: Arc<AtomicUsize>,

    /// Memory limit in bytes (if any)
    limit: Option<usize>,
}

impl MemoryTracker {
    /// Create a new memory tracker with optional limit
    pub fn new(limit: Option<usize>) -> Self {
        MemoryTracker {
            current: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
            limit,
        }
    }

    /// Create a tracker with no limit
    pub fn unlimited() -> Self {
        MemoryTracker::new(None)
    }

    /// Allocate memory and check limit
    ///
    /// Returns Ok if allocation is within limits, Err otherwise.
    pub fn allocate(&self, bytes: usize) -> Result<(), ResourceError> {
        let new_total = self.current.fetch_add(bytes, Ordering::Relaxed) + bytes;

        // Update peak
        self.peak.fetch_max(new_total, Ordering::Relaxed);

        // Check limit
        if let Some(limit) = self.limit {
            if new_total > limit {
                // Rollback allocation
                self.current.fetch_sub(bytes, Ordering::Relaxed);
                return Err(ResourceError::MemoryLimitExceeded {
                    limit,
                    used: new_total,
                });
            }
        }

        Ok(())
    }

    /// Release memory
    pub fn release(&self, bytes: usize) {
        self.current.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Get current memory usage
    pub fn current_usage(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// Get peak memory usage
    pub fn peak_usage(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Get remaining memory before limit (if any)
    pub fn remaining(&self) -> Option<usize> {
        self.limit.map(|limit| {
            let current = self.current.load(Ordering::Relaxed);
            limit.saturating_sub(current)
        })
    }

    /// Reset the tracker
    pub fn reset(&self) {
        self.current.store(0, Ordering::Relaxed);
        self.peak.store(0, Ordering::Relaxed);
    }

    /// Check if memory limit has been exceeded
    pub fn check(&self) -> Result<(), ResourceError> {
        if let Some(limit) = self.limit {
            let current = self.current.load(Ordering::Relaxed);
            if current > limit {
                return Err(ResourceError::MemoryLimitExceeded {
                    limit,
                    used: current,
                });
            }
        }
        Ok(())
    }
}

impl Default for MemoryTracker {
    fn default() -> Self {
        // Default 1GB limit
        MemoryTracker::new(Some(1024 * 1024 * 1024))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limits() {
        let limits = ResourceLimits::default();
        assert!(limits.max_memory_bytes.is_some());
        assert!(limits.max_result_size.is_some());
    }

    #[test]
    fn test_unlimited_limits() {
        let limits = ResourceLimits::unlimited();
        assert!(limits.max_memory_bytes.is_none());
        assert!(limits.max_result_size.is_none());
    }

    #[test]
    fn test_result_size_check() {
        let limits = ResourceLimits::default().with_max_result_size(100);

        assert!(limits.check_result_size(50).is_ok());
        assert!(limits.check_result_size(100).is_ok());
        assert!(limits.check_result_size(101).is_err());
    }

    #[test]
    fn test_row_width_check() {
        let limits = ResourceLimits::default();
        limits.max_row_width.map(|_| {
            let mut limits = limits.clone();
            limits.max_row_width = Some(10);

            assert!(limits.check_row_width(5).is_ok());
            assert!(limits.check_row_width(10).is_ok());
            assert!(limits.check_row_width(11).is_err());
        });
    }

    #[test]
    fn test_memory_tracker_basic() {
        let tracker = MemoryTracker::new(Some(1000));

        // Allocate within limits
        assert!(tracker.allocate(500).is_ok());
        assert_eq!(tracker.current_usage(), 500);

        // Allocate more
        assert!(tracker.allocate(300).is_ok());
        assert_eq!(tracker.current_usage(), 800);

        // Release some
        tracker.release(200);
        assert_eq!(tracker.current_usage(), 600);

        // Peak should still be 800
        assert_eq!(tracker.peak_usage(), 800);
    }

    #[test]
    fn test_memory_tracker_limit() {
        let tracker = MemoryTracker::new(Some(1000));

        // Allocate up to limit
        assert!(tracker.allocate(900).is_ok());

        // Exceed limit - should fail and rollback
        assert!(tracker.allocate(200).is_err());
        assert_eq!(tracker.current_usage(), 900); // Rolled back

        // Can still allocate within remaining space
        assert!(tracker.allocate(100).is_ok());
        assert_eq!(tracker.current_usage(), 1000);
    }

    #[test]
    fn test_memory_tracker_unlimited() {
        let tracker = MemoryTracker::unlimited();

        // Can allocate any amount
        assert!(tracker.allocate(1_000_000_000).is_ok());
        assert_eq!(tracker.remaining(), None);
    }

    #[test]
    fn test_remaining_memory() {
        let tracker = MemoryTracker::new(Some(1000));
        assert_eq!(tracker.remaining(), Some(1000));

        tracker.allocate(300).unwrap();
        assert_eq!(tracker.remaining(), Some(700));
    }

    #[test]
    fn test_strict_limits() {
        let limits = ResourceLimits::strict();

        // Strict limits are more restrictive
        assert!(limits.max_result_size.unwrap() < 1_000_000);
        assert!(limits.max_row_width.unwrap() <= 20);
    }
}
