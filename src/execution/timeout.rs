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

impl QueryTimeout {
    /// Create a new timeout controller with the specified duration
    pub fn new(timeout: Option<Duration>) -> Self {
        QueryTimeout {
            cancelled: Arc::new(AtomicBool::new(false)),
            start_time: Instant::now(),
            timeout_duration: timeout,
        }
    }

    /// Create a timeout controller with no timeout (infinite)
    pub fn infinite() -> Self {
        QueryTimeout::new(None)
    }

    /// Check if the query has been cancelled or timed out
    ///
    /// This should be called periodically during query execution.
    /// Returns Ok(()) if the query can continue, or Err(TimeoutError)
    /// if it should be cancelled.
    pub fn check(&self) -> Result<(), TimeoutError> {
        // Check explicit cancellation
        if self.cancelled.load(Ordering::Relaxed) {
            return Err(TimeoutError {
                timeout: self.timeout_duration.unwrap_or(Duration::ZERO),
                elapsed: self.start_time.elapsed(),
            });
        }

        // Check timeout
        if let Some(timeout) = self.timeout_duration {
            let elapsed = self.start_time.elapsed();
            if elapsed > timeout {
                self.cancelled.store(true, Ordering::Relaxed);
                return Err(TimeoutError { timeout, elapsed });
            }
        }

        Ok(())
    }

    /// Cancel the query explicitly
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Check if the query has been cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Get the elapsed time since the query started
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get the remaining time before timeout (if any)
    pub fn remaining(&self) -> Option<Duration> {
        self.timeout_duration.map(|timeout| {
            let elapsed = self.start_time.elapsed();
            if elapsed >= timeout {
                Duration::ZERO
            } else {
                timeout.checked_sub(elapsed).unwrap_or(Duration::ZERO)
            }
        })
    }

    /// Reset the start time (for reusing the controller)
    pub fn reset(&mut self) {
        self.start_time = Instant::now();
        self.cancelled.store(false, Ordering::Relaxed);
    }

    /// Get a handle that can be used to cancel from another thread
    pub fn cancel_handle(&self) -> CancelHandle {
        CancelHandle {
            cancelled: Arc::clone(&self.cancelled),
        }
    }
}

impl Default for QueryTimeout {
    fn default() -> Self {
        // Default 60-second timeout
        QueryTimeout::new(Some(Duration::from_secs(60)))
    }
}

/// Handle for cancelling a query from another thread
#[derive(Clone)]
pub struct CancelHandle {
    cancelled: Arc<AtomicBool>,
}

impl CancelHandle {
    /// Cancel the associated query
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Check if cancellation has been requested
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}

