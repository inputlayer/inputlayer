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

