//! Query Execution Module
//!
//! Provides production-grade query execution with:
//! - Timeout enforcement via cooperative cancellation

mod timeout;

pub use timeout::{CancelHandle, QueryTimeout, TimeoutError};

/// Execution error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum ExecutionError {
    /// Query timed out
    #[error("Query timeout: {0}")]
    Timeout(#[from] TimeoutError),

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
    fn test_execution_error_display() {
        let err = ExecutionError::QueryError("test error".to_string());
        assert_eq!(format!("{err}"), "Query error: test error");

        let err = ExecutionError::ParseError("bad syntax".to_string());
        assert_eq!(format!("{err}"), "Parse error: bad syntax");
    }
}
