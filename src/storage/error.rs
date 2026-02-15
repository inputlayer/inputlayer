//! Storage Engine Error Types

use std::io;
use thiserror::Error;

/// Storage engine errors
#[derive(Error, Debug)]
pub enum StorageError {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Parquet error
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Arrow error
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// JSON serialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Knowledge graph not found
    #[error("Knowledge graph not found: {0}")]
    KnowledgeGraphNotFound(String),

    /// Knowledge graph already exists
    #[error("Knowledge graph already exists: {0}")]
    KnowledgeGraphExists(String),

    /// No current knowledge graph selected
    #[error("No current knowledge graph selected. Use use_knowledge_graph() first.")]
    NoCurrentKnowledgeGraph,

    /// Cannot drop default knowledge graph
    #[error("Cannot drop the default knowledge graph")]
    CannotDropDefault,

    /// Cannot drop current knowledge graph
    #[error("Cannot drop the current knowledge graph. Switch to another knowledge graph first.")]
    CannotDropCurrentKnowledgeGraph,

    /// Relation not found
    #[error("Relation '{0}' not found in knowledge graph '{1}'")]
    RelationNotFound(String, String),

    /// Invalid relation name
    #[error("Invalid relation name: {0}")]
    InvalidRelationName(String),

    /// Metadata error
    #[error("Metadata error: {0}")]
    MetadataError(String),

    /// Parse error
    #[error("Parse error: {0}")]
    ParseError(String),

    /// Generic error
    #[error("{0}")]
    Other(String),

    /// Lock poisoned (another thread panicked while holding the lock)
    /// Note: No longer used after migration to `parking_lot` (which never poisons)
    /// Kept for backward compatibility with any code matching on this variant
    #[error("Lock poisoned: a thread panicked while holding this lock")]
    #[deprecated(note = "parking_lot locks never poison; this variant is no longer used")]
    LockPoisoned,

    /// Lock acquisition timeout
    #[error("Lock acquisition timed out after {0}ms")]
    LockTimeout(u64),

    /// Incremental engine error
    ///
    /// Occurs when the incremental engine fails to process an operation (insert, delete, query).
    /// This is a critical error - the in-memory and DD state may be inconsistent.
    #[error("Incremental engine error: {0}")]
    IncrementalEngineError(String),

    /// DD worker disconnected
    ///
    /// The DD background worker thread terminated unexpectedly.
    #[error("DD worker disconnected")]
    DDWorkerDisconnected,
}

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_error_display() {
        let err: StorageError = io::Error::new(io::ErrorKind::NotFound, "file not found").into();
        assert!(err.to_string().contains("I/O error"));
    }

    #[test]
    fn test_kg_not_found_display() {
        let err = StorageError::KnowledgeGraphNotFound("test".to_string());
        assert_eq!(err.to_string(), "Knowledge graph not found: test");
    }

    #[test]
    fn test_kg_exists_display() {
        let err = StorageError::KnowledgeGraphExists("test".to_string());
        assert_eq!(err.to_string(), "Knowledge graph already exists: test");
    }

    #[test]
    fn test_no_current_kg_display() {
        let err = StorageError::NoCurrentKnowledgeGraph;
        assert!(err.to_string().contains("No current knowledge graph"));
    }

    #[test]
    fn test_cannot_drop_default_display() {
        let err = StorageError::CannotDropDefault;
        assert!(err.to_string().contains("Cannot drop the default"));
    }

    #[test]
    fn test_cannot_drop_current_display() {
        let err = StorageError::CannotDropCurrentKnowledgeGraph;
        assert!(err.to_string().contains("Cannot drop the current"));
    }

    #[test]
    fn test_relation_not_found_display() {
        let err = StorageError::RelationNotFound("edge".to_string(), "test_kg".to_string());
        assert!(err.to_string().contains("edge"));
        assert!(err.to_string().contains("test_kg"));
    }

    #[test]
    fn test_invalid_relation_name_display() {
        let err = StorageError::InvalidRelationName("bad-name".to_string());
        assert!(err.to_string().contains("bad-name"));
    }

    #[test]
    fn test_metadata_error_display() {
        let err = StorageError::MetadataError("corrupt".to_string());
        assert!(err.to_string().contains("Metadata error: corrupt"));
    }

    #[test]
    fn test_parse_error_display() {
        let err = StorageError::ParseError("invalid syntax".to_string());
        assert!(err.to_string().contains("Parse error: invalid syntax"));
    }

    #[test]
    fn test_other_error_display() {
        let err = StorageError::Other("something went wrong".to_string());
        assert_eq!(err.to_string(), "something went wrong");
    }

    #[test]
    fn test_lock_timeout_display() {
        let err = StorageError::LockTimeout(5000);
        assert!(err.to_string().contains("5000ms"));
    }

    #[test]
    fn test_incremental_engine_error_display() {
        let err = StorageError::IncrementalEngineError("worker failed".to_string());
        assert!(err.to_string().contains("worker failed"));
    }

    #[test]
    fn test_dd_worker_disconnected_display() {
        let err = StorageError::DDWorkerDisconnected;
        assert!(err.to_string().contains("DD worker disconnected"));
    }

    #[test]
    fn test_from_io_error() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
        let storage_err: StorageError = io_err.into();
        assert!(matches!(storage_err, StorageError::Io(_)));
    }

    #[test]
    fn test_from_json_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let storage_err: StorageError = json_err.into();
        assert!(matches!(storage_err, StorageError::Json(_)));
    }
}
