//! Query Timeout Module
//!
//! Provides timeout enforcement for query execution.
//!
//! ## Design
//!
//! Uses a combination of:
//! - Atomic flag for cooperative cancellation
//! - Timeout wrapper for blocking operations
//!
//! Differential Dataflow computations can check the cancellation flag
//! periodically to enable early termination.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Timeout error
#[derive(Debug, Clone, thiserror::Error)]
#[error("Query exceeded timeout of {timeout:?} (ran for {elapsed:?})")]
pub struct TimeoutError {
    /// The timeout duration that was exceeded
    pub timeout: Duration,
    /// How long the query actually ran
    pub elapsed: Duration,
}

/// Query timeout controller
///
/// Provides cooperative cancellation for long-running queries.
/// The controller can be shared across threads and checked periodically.
#[derive(Clone)]
pub struct QueryTimeout {
    /// Cancellation flag (shared across threads)
    cancelled: Arc<AtomicBool>,

    /// When the query started
    start_time: Instant,

    /// Maximum allowed duration
    timeout_duration: Option<Duration>,
}

