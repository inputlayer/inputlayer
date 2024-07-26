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

