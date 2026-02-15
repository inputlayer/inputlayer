//! Error types for the `InputLayer` RPC protocol.

use serde::{Deserialize, Serialize};

/// `InputLayer` RPC error type.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum InputLayerError {
    // Knowledge Graph Errors
    /// Knowledge graph not found
    #[error("Knowledge graph not found: {name}")]
    KnowledgeGraphNotFound { name: String },

    /// Knowledge graph already exists
    #[error("Knowledge graph already exists: {name}")]
    KnowledgeGraphExists { name: String },

    /// Relation not found in knowledge graph
    #[error("Relation '{relation}' not found in knowledge graph '{knowledge_graph}'")]
    RelationNotFound {
        relation: String,
        knowledge_graph: String,
    },

    /// Cannot drop default knowledge graph
    #[error("Cannot drop default knowledge graph: {name}")]
    CannotDropDefault { name: String },

    /// Cannot drop current knowledge graph
    #[error("Cannot drop current knowledge graph: {name}")]
    CannotDropCurrent { name: String },

    /// No current knowledge graph selected
    #[error("No current knowledge graph selected")]
    NoCurrentKnowledgeGraph,

    // Query Errors
    /// Parse error in Datalog program
    #[error("Parse error: {message}")]
    ParseError {
        message: String,
        line: Option<u32>,
        column: Option<u32>,
    },

    /// Query execution error
    #[error("Execution error: {message}")]
    ExecutionError { message: String },

    /// Query timeout
    #[error("Query timeout after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    // Data Errors
    /// Schema violation
    #[error("Schema violation: expected {expected}, got {got}")]
    SchemaViolation { expected: String, got: String },

    /// Vector dimension mismatch
    #[error("Vector dimension mismatch: expected {expected}, got {got}")]
    VectorDimensionMismatch { expected: usize, got: usize },

    /// Type mismatch
    #[error("Type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },

    /// Invalid data format
    #[error("Invalid data: {message}")]
    InvalidData { message: String },

    // Connection Errors
    /// Connection failed
    #[error("Connection to {address} failed: {reason}")]
    ConnectionFailed { address: String, reason: String },

    /// Connection lost
    #[error("Connection lost: {reason}")]
    ConnectionLost { reason: String },

    /// Authentication failed
    #[error("Authentication failed: {reason}")]
    AuthenticationFailed { reason: String },

    // Server Errors
    /// Internal server error
    #[error("Internal error: {message}")]
    InternalError { message: String },

    /// Server overloaded
    #[error("Server overloaded: {active_queries}/{max_queries} queries")]
    ServerOverloaded {
        active_queries: u32,
        max_queries: u32,
    },

    /// Server is shutting down
    #[error("Server is shutting down")]
    ShuttingDown,

    /// Resource limit exceeded
    #[error("Resource limit exceeded: {resource} (limit: {limit})")]
    ResourceLimitExceeded { resource: String, limit: String },

    // Serialization Errors
    /// Serialization error
    #[error("Serialization error: {message}")]
    SerializationError { message: String },

    /// Deserialization error
    #[error("Deserialization error: {message}")]
    DeserializationError { message: String },
}

// Conversions from internal error types
impl From<crate::storage::StorageError> for InputLayerError {
    fn from(e: crate::storage::StorageError) -> Self {
        match e {
            crate::storage::StorageError::KnowledgeGraphNotFound(name) => {
                InputLayerError::KnowledgeGraphNotFound { name }
            }
            crate::storage::StorageError::KnowledgeGraphExists(name) => {
                InputLayerError::KnowledgeGraphExists { name }
            }
            crate::storage::StorageError::CannotDropDefault => InputLayerError::CannotDropDefault {
                name: "default".to_string(),
            },
            crate::storage::StorageError::CannotDropCurrentKnowledgeGraph => {
                InputLayerError::CannotDropCurrent {
                    name: "current".to_string(),
                }
            }
            crate::storage::StorageError::NoCurrentKnowledgeGraph => {
                InputLayerError::NoCurrentKnowledgeGraph
            }
            crate::storage::StorageError::RelationNotFound(relation, knowledge_graph) => {
                InputLayerError::RelationNotFound {
                    relation,
                    knowledge_graph,
                }
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
            message: format!("IO error: {e}"),
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

// Result type alias
/// Result type for `InputLayer` protocol operations.
pub type InputLayerResult<T> = Result<T, InputLayerError>;

// Tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = InputLayerError::KnowledgeGraphNotFound {
            name: "test".to_string(),
        };
        assert_eq!(err.to_string(), "Knowledge graph not found: test");
    }

    #[test]
    fn test_parse_error_display() {
        let err = InputLayerError::ParseError {
            message: "unexpected token".to_string(),
            line: Some(10),
            column: Some(5),
        };
        // thiserror uses simplified format - line/column still stored for programmatic access
        assert_eq!(err.to_string(), "Parse error: unexpected token");
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

    #[test]
    fn test_from_string_owned() {
        let err: InputLayerError = String::from("owned error").into();
        assert!(
            matches!(err, InputLayerError::InternalError { ref message } if message == "owned error")
        );
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: InputLayerError = io_err.into();
        assert!(matches!(err, InputLayerError::InternalError { .. }));
        assert!(err.to_string().contains("IO error"));
    }

    #[test]
    fn test_error_display_all_variants() {
        let errors = vec![
            InputLayerError::KnowledgeGraphNotFound {
                name: "test".to_string(),
            },
            InputLayerError::KnowledgeGraphExists {
                name: "test".to_string(),
            },
            InputLayerError::RelationNotFound {
                relation: "edge".to_string(),
                knowledge_graph: "test".to_string(),
            },
            InputLayerError::CannotDropDefault {
                name: "default".to_string(),
            },
            InputLayerError::CannotDropCurrent {
                name: "test".to_string(),
            },
            InputLayerError::NoCurrentKnowledgeGraph,
            InputLayerError::ExecutionError {
                message: "failed".to_string(),
            },
            InputLayerError::Timeout { timeout_ms: 5000 },
            InputLayerError::SchemaViolation {
                expected: "int".to_string(),
                got: "string".to_string(),
            },
            InputLayerError::VectorDimensionMismatch {
                expected: 3,
                got: 5,
            },
            InputLayerError::TypeMismatch {
                expected: "int".to_string(),
                got: "string".to_string(),
            },
            InputLayerError::InvalidData {
                message: "bad data".to_string(),
            },
            InputLayerError::ConnectionFailed {
                address: "localhost:8080".to_string(),
                reason: "refused".to_string(),
            },
            InputLayerError::ConnectionLost {
                reason: "timeout".to_string(),
            },
            InputLayerError::AuthenticationFailed {
                reason: "bad token".to_string(),
            },
            InputLayerError::InternalError {
                message: "oops".to_string(),
            },
            InputLayerError::ServerOverloaded {
                active_queries: 100,
                max_queries: 50,
            },
            InputLayerError::ShuttingDown,
            InputLayerError::ResourceLimitExceeded {
                resource: "memory".to_string(),
                limit: "1GB".to_string(),
            },
            InputLayerError::SerializationError {
                message: "encode fail".to_string(),
            },
            InputLayerError::DeserializationError {
                message: "decode fail".to_string(),
            },
        ];

        for err in &errors {
            let display = err.to_string();
            assert!(!display.is_empty(), "Empty display for: {err:?}");
        }
    }

    #[test]
    fn test_error_json_roundtrip() {
        let err = InputLayerError::RelationNotFound {
            relation: "edge".to_string(),
            knowledge_graph: "test".to_string(),
        };
        let json = serde_json::to_string(&err).unwrap();
        let back: InputLayerError = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            back,
            InputLayerError::RelationNotFound { ref relation, ref knowledge_graph }
            if relation == "edge" && knowledge_graph == "test"
        ));
    }

    #[test]
    fn test_from_storage_error_kg_not_found() {
        let storage_err = crate::storage::StorageError::KnowledgeGraphNotFound("mykg".to_string());
        let err: InputLayerError = storage_err.into();
        assert!(
            matches!(err, InputLayerError::KnowledgeGraphNotFound { ref name } if name == "mykg")
        );
    }

    #[test]
    fn test_from_storage_error_kg_exists() {
        let storage_err = crate::storage::StorageError::KnowledgeGraphExists("mykg".to_string());
        let err: InputLayerError = storage_err.into();
        assert!(
            matches!(err, InputLayerError::KnowledgeGraphExists { ref name } if name == "mykg")
        );
    }

    #[test]
    fn test_from_storage_error_cannot_drop_default() {
        let storage_err = crate::storage::StorageError::CannotDropDefault;
        let err: InputLayerError = storage_err.into();
        assert!(matches!(err, InputLayerError::CannotDropDefault { .. }));
    }

    #[test]
    fn test_from_storage_error_no_current_kg() {
        let storage_err = crate::storage::StorageError::NoCurrentKnowledgeGraph;
        let err: InputLayerError = storage_err.into();
        assert!(matches!(err, InputLayerError::NoCurrentKnowledgeGraph));
    }

    #[test]
    fn test_from_storage_error_relation_not_found() {
        let storage_err =
            crate::storage::StorageError::RelationNotFound("edge".to_string(), "test".to_string());
        let err: InputLayerError = storage_err.into();
        assert!(matches!(err, InputLayerError::RelationNotFound { .. }));
    }
}
