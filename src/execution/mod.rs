//! Query Execution Module
//!
//! Provides production-grade query execution with:
//! - Deadlines and cancellation via cooperative checks ([`RequestControl`])
//! - A per-query memory limit and a server-wide query memory budget,
//!   metered per thread ([`memory`])

pub mod hnsw_resolve;
pub mod memory;
mod request_control;
pub mod timing;

pub use request_control::{Halt, QueryMemoryPool, RequestControl, Stop};
pub use timing::{
    IrBuilderTiming, OptimizerTiming, RuleTiming, TimingBreakdown, TimingCollector,
    TimingHistograms, TimingMode,
};

/// Execution error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum ExecutionError {
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
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_error_display() {
        let err = ExecutionError::QueryError("test error".to_string());
        assert_eq!(format!("{err}"), "Query error: test error");

        let err = ExecutionError::ParseError("bad syntax".to_string());
        assert_eq!(format!("{err}"), "Parse error: bad syntax");
    }
}
