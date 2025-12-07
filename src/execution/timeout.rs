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
#[derive(Debug, Clone)]
pub struct TimeoutError {
    /// The timeout duration that was exceeded
    pub timeout: Duration,
    /// How long the query actually ran
    pub elapsed: Duration,
}

impl std::fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Query exceeded timeout of {:?} (ran for {:?})",
            self.timeout, self.elapsed
        )
    }
}

impl std::error::Error for TimeoutError {}

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
                timeout - elapsed
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

/// Execute a closure with timeout
///
/// This is a convenience function for running synchronous code with a timeout.
/// For async code, use tokio or async-std timeout functions.
pub fn with_timeout<T, F>(timeout: Duration, f: F) -> Result<T, TimeoutError>
where
    F: FnOnce(&QueryTimeout) -> T,
{
    let controller = QueryTimeout::new(Some(timeout));

    // Execute the function
    let result = f(&controller);

    // Final timeout check
    controller.check()?;

    Ok(result)
}

/// Execute a fallible closure with timeout
pub fn with_timeout_result<T, E, F>(timeout: Duration, f: F) -> Result<Result<T, E>, TimeoutError>
where
    F: FnOnce(&QueryTimeout) -> Result<T, E>,
{
    let controller = QueryTimeout::new(Some(timeout));

    // Execute the function
    let result = f(&controller);

    // Final timeout check
    controller.check()?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_no_timeout() {
        let timeout = QueryTimeout::new(None);
        assert!(timeout.check().is_ok());
        assert!(!timeout.is_cancelled());
    }

    #[test]
    fn test_timeout_not_exceeded() {
        let timeout = QueryTimeout::new(Some(Duration::from_secs(10)));
        assert!(timeout.check().is_ok());
        assert!(!timeout.is_cancelled());
    }

    #[test]
    fn test_explicit_cancellation() {
        let timeout = QueryTimeout::new(Some(Duration::from_secs(10)));
        timeout.cancel();
        assert!(timeout.is_cancelled());
        assert!(timeout.check().is_err());
    }

    #[test]
    fn test_cancel_handle() {
        let timeout = QueryTimeout::new(Some(Duration::from_secs(10)));
        let handle = timeout.cancel_handle();

        // Cancel from handle
        handle.cancel();

        // Original controller should reflect cancellation
        assert!(timeout.is_cancelled());
        assert!(handle.is_cancelled());
    }

    #[test]
    fn test_timeout_exceeded() {
        let timeout = QueryTimeout::new(Some(Duration::from_millis(10)));

        // Sleep to exceed timeout
        thread::sleep(Duration::from_millis(50));

        let result = timeout.check();
        assert!(result.is_err());

        if let Err(e) = result {
            assert!(e.elapsed >= Duration::from_millis(10));
        }
    }

    #[test]
    fn test_with_timeout_success() {
        let result = with_timeout(Duration::from_secs(1), |_| 42);
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_remaining_time() {
        let timeout = QueryTimeout::new(Some(Duration::from_secs(10)));
        let remaining = timeout.remaining().unwrap();
        assert!(remaining <= Duration::from_secs(10));
        assert!(remaining > Duration::from_secs(9));
    }

    #[test]
    fn test_reset() {
        let mut timeout = QueryTimeout::new(Some(Duration::from_secs(10)));
        timeout.cancel();
        assert!(timeout.is_cancelled());

        timeout.reset();
        assert!(!timeout.is_cancelled());
        assert!(timeout.check().is_ok());
    }
}
