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

    /// Database not found
    #[error("Database not found: {0}")]
    DatabaseNotFound(String),

    /// Database already exists
    #[error("Database already exists: {0}")]
    DatabaseExists(String),

    /// No current database selected
    #[error("No current database selected. Use use_database() first.")]
    NoCurrentDatabase,

    /// Cannot drop default database
    #[error("Cannot drop the default database")]
    CannotDropDefault,

    /// Cannot drop current database
    #[error("Cannot drop the current database. Switch to another database first.")]
    CannotDropCurrentDatabase,

    /// Relation not found
    #[error("Relation '{0}' not found in database '{1}'")]
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
}

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;
