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
    /// Note: No longer used after migration to parking_lot (which never poisons)
    /// Kept for backward compatibility with any code matching on this variant
    #[error("Lock poisoned: a thread panicked while holding this lock")]
    #[deprecated(note = "parking_lot locks never poison; this variant is no longer used")]
    LockPoisoned,

    /// Lock acquisition timeout
    #[error("Lock acquisition timed out after {0}ms")]
    LockTimeout(u64),
}

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;
