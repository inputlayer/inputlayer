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

    // Server Errors
    /// Internal server error
    #[error("Internal error: {message}")]
    InternalError { message: String },

    // Serialization Errors
    /// Serialization error
    #[error("Serialization error: {message}")]
    SerializationError { message: String },
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
        tracing::warn!(error = %e, "IO error");
        InputLayerError::InternalError {
            message: format!("IO error: {}", e.kind()),
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
    fn test_error_serialization() {
        let err = InputLayerError::InternalError {
            message: "test error".to_string(),
        };
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

    /// Regression: IO error messages must use e.kind() to avoid leaking internal paths.
    #[test]
    fn test_io_error_does_not_leak_path() {
        let io_err = std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "failed to open /etc/shadow: permission denied",
        );
        let err: InputLayerError = io_err.into();
        let msg = err.to_string();
        // Must contain the error kind, not the detailed message with paths
        assert!(msg.contains("permission denied"), "Missing error kind");
        assert!(
            !msg.contains("/etc/shadow"),
            "IO error leaked internal path: {msg}"
        );
    }

    /// Regression: IO error message should contain kind name for all error types.
    #[test]
    fn test_io_error_contains_kind_for_various_errors() {
        let kinds = vec![
            (std::io::ErrorKind::NotFound, "not found"),
            (std::io::ErrorKind::ConnectionRefused, "connection refused"),
            (std::io::ErrorKind::AlreadyExists, "entity already exists"),
        ];
        for (kind, expected_substr) in kinds {
            let io_err = std::io::Error::new(kind, format!("sensitive path /var/data/{:?}", kind));
            let err: InputLayerError = io_err.into();
            let msg = err.to_string();
            assert!(
                msg.contains(expected_substr),
                "Missing kind '{expected_substr}' in: {msg}"
            );
            assert!(
                !msg.contains("/var/data/"),
                "Leaked path for kind {kind:?}: {msg}"
            );
        }
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
            InputLayerError::InternalError {
                message: "oops".to_string(),
            },
            InputLayerError::SerializationError {
                message: "encode fail".to_string(),
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
