//! InputLayer Protocol Error Types
//!
//! Error types for the InputLayer RPC protocol.

use serde::{Deserialize, Serialize};
use std::fmt;

/// InputLayer RPC error type.
///
/// Comprehensive error enum covering all possible failure modes in the protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InputLayerError {
    // ========================================================================
    // Database Errors
    // ========================================================================

    /// Database not found
    DatabaseNotFound { name: String },

    /// Database already exists
    DatabaseExists { name: String },

    /// Relation not found in database
    RelationNotFound { relation: String, database: String },

    /// Cannot drop default database
    CannotDropDefault { name: String },

    /// Cannot drop current database
    CannotDropCurrent { name: String },

    /// No current database selected
    NoCurrentDatabase,

    // ========================================================================
    // Query Errors
    // ========================================================================

    /// Parse error in Datalog program
    ParseError {
        message: String,
        line: Option<u32>,
        column: Option<u32>,
    },

    /// Query execution error
    ExecutionError { message: String },

    /// Query timeout
    Timeout { timeout_ms: u64 },

    // ========================================================================
    // Data Errors
    // ========================================================================

    /// Schema violation
    SchemaViolation { expected: String, got: String },

    /// Vector dimension mismatch
    VectorDimensionMismatch { expected: usize, got: usize },

    /// Type mismatch
    TypeMismatch { expected: String, got: String },

    /// Invalid data format
    InvalidData { message: String },

    // ========================================================================
    // Connection Errors
    // ========================================================================

    /// Connection failed
    ConnectionFailed { address: String, reason: String },

    /// Connection lost
    ConnectionLost { reason: String },

    /// Authentication failed
    AuthenticationFailed { reason: String },

    // ========================================================================
    // Server Errors
    // ========================================================================

    /// Internal server error
    InternalError { message: String },

    /// Server overloaded
    ServerOverloaded { active_queries: u32, max_queries: u32 },

    /// Server is shutting down
    ShuttingDown,

    /// Resource limit exceeded
    ResourceLimitExceeded { resource: String, limit: String },

    // ========================================================================
    // Serialization Errors
    // ========================================================================

    /// Serialization error
    SerializationError { message: String },

    /// Deserialization error
    DeserializationError { message: String },
}

impl fmt::Display for InputLayerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Database errors
            InputLayerError::DatabaseNotFound { name } => {
                write!(f, "Database not found: {}", name)
            }
            InputLayerError::DatabaseExists { name } => {
                write!(f, "Database already exists: {}", name)
            }
            InputLayerError::RelationNotFound { relation, database } => {
                write!(f, "Relation '{}' not found in database '{}'", relation, database)
            }
            InputLayerError::CannotDropDefault { name } => {
                write!(f, "Cannot drop default database: {}", name)
            }
            InputLayerError::CannotDropCurrent { name } => {
                write!(f, "Cannot drop current database: {}", name)
            }
            InputLayerError::NoCurrentDatabase => {
                write!(f, "No current database selected")
            }

            // Query errors
            InputLayerError::ParseError { message, line, column } => {
                match (line, column) {
                    (Some(l), Some(c)) => write!(f, "Parse error at {}:{}: {}", l, c, message),
                    (Some(l), None) => write!(f, "Parse error at line {}: {}", l, message),
                    _ => write!(f, "Parse error: {}", message),
                }
            }
            InputLayerError::ExecutionError { message } => {
                write!(f, "Execution error: {}", message)
            }
            InputLayerError::Timeout { timeout_ms } => {
                write!(f, "Query timeout after {}ms", timeout_ms)
            }

            // Data errors
            InputLayerError::SchemaViolation { expected, got } => {
                write!(f, "Schema violation: expected {}, got {}", expected, got)
            }
            InputLayerError::VectorDimensionMismatch { expected, got } => {
                write!(f, "Vector dimension mismatch: expected {}, got {}", expected, got)
            }
            InputLayerError::TypeMismatch { expected, got } => {
                write!(f, "Type mismatch: expected {}, got {}", expected, got)
            }
            InputLayerError::InvalidData { message } => {
                write!(f, "Invalid data: {}", message)
            }

            // Connection errors
            InputLayerError::ConnectionFailed { address, reason } => {
                write!(f, "Connection to {} failed: {}", address, reason)
            }
            InputLayerError::ConnectionLost { reason } => {
                write!(f, "Connection lost: {}", reason)
            }
            InputLayerError::AuthenticationFailed { reason } => {
                write!(f, "Authentication failed: {}", reason)
            }

            // Server errors
            InputLayerError::InternalError { message } => {
                write!(f, "Internal error: {}", message)
            }
            InputLayerError::ServerOverloaded { active_queries, max_queries } => {
                write!(f, "Server overloaded: {}/{} queries", active_queries, max_queries)
            }
            InputLayerError::ShuttingDown => {
                write!(f, "Server is shutting down")
            }
            InputLayerError::ResourceLimitExceeded { resource, limit } => {
                write!(f, "Resource limit exceeded: {} (limit: {})", resource, limit)
            }

            // Serialization errors
            InputLayerError::SerializationError { message } => {
                write!(f, "Serialization error: {}", message)
            }
            InputLayerError::DeserializationError { message } => {
                write!(f, "Deserialization error: {}", message)
            }
        }
    }
}

impl std::error::Error for InputLayerError {}

// ============================================================================
// Conversions from internal error types
// ============================================================================

impl From<crate::storage::StorageError> for InputLayerError {
    fn from(e: crate::storage::StorageError) -> Self {
        match e {
            crate::storage::StorageError::DatabaseNotFound(name) => {
                InputLayerError::DatabaseNotFound { name }
            }
            crate::storage::StorageError::DatabaseExists(name) => {
                InputLayerError::DatabaseExists { name }
            }
            crate::storage::StorageError::CannotDropDefault => {
                InputLayerError::CannotDropDefault { name: "default".to_string() }
            }
            crate::storage::StorageError::CannotDropCurrentDatabase => {
                InputLayerError::CannotDropCurrent { name: "current".to_string() }
            }
            crate::storage::StorageError::NoCurrentDatabase => {
                InputLayerError::NoCurrentDatabase
            }
            crate::storage::StorageError::RelationNotFound(relation, database) => {
                InputLayerError::RelationNotFound { relation, database }
            }
            _ => InputLayerError::InternalError {
                message: e.to_string(),
            },
        }
    }
}

impl From<bincode::Error> for InputLayerError {
    fn from(e: bincode::Error) -> Self {
        InputLayerError::SerializationError {
            message: e.to_string(),
        }
    }
}

impl From<std::io::Error> for InputLayerError {
    fn from(e: std::io::Error) -> Self {
        InputLayerError::InternalError {
            message: format!("IO error: {}", e),
        }
    }
}

impl From<String> for InputLayerError {
    fn from(message: String) -> Self {
        InputLayerError::InternalError { message }
    }
}

impl From<&str> for InputLayerError {
    fn from(message: &str) -> Self {
        InputLayerError::InternalError {
            message: message.to_string(),
        }
    }
}

// ============================================================================
// Result type alias
// ============================================================================

/// Result type for InputLayer protocol operations.
pub type InputLayerResult<T> = Result<T, InputLayerError>;

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = InputLayerError::DatabaseNotFound {
            name: "test".to_string(),
        };
        assert_eq!(err.to_string(), "Database not found: test");
    }

    #[test]
    fn test_parse_error_display() {
        let err = InputLayerError::ParseError {
            message: "unexpected token".to_string(),
            line: Some(10),
            column: Some(5),
        };
        assert_eq!(err.to_string(), "Parse error at 10:5: unexpected token");
    }

    #[test]
    fn test_error_serialization() {
        let err = InputLayerError::Timeout { timeout_ms: 5000 };
        let bytes = bincode::serialize(&err).unwrap();
        let restored: InputLayerError = bincode::deserialize(&bytes).unwrap();
        assert_eq!(err.to_string(), restored.to_string());
    }

    #[test]
    fn test_from_string() {
        let err: InputLayerError = "something went wrong".into();
        assert!(matches!(err, InputLayerError::InternalError { .. }));
    }
}
