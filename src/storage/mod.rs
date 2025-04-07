//! Storage Module
//!
//! Provides persistent storage functionality for `InputLayer`:
//! - DD-native persistence with (data, time, diff) triples
//! - Parquet serialization (columnar, compressed, efficient for analytics)
//! - CSV serialization (human-readable, interoperable)
//! - Metadata management
//! - Error handling
//!
//! ## Persistence Model
//!
//! `InputLayer` uses Differential Dataflow-native persistence:
//! - Updates are stored as `(data, time, diff)` triples
//! - Consolidation sums diffs to compute current state
//! - WAL provides immediate durability, batches provide efficient reads
//!
//! ## Format Selection
//!
//! - Parquet: Best for large datasets, analytics workloads, and production use
//! - CSV: Best for data exchange, debugging, and human inspection

pub mod csv;
pub mod error;
pub mod metadata;
pub mod parquet;
pub mod persist;
pub mod wal;

// Re-export commonly used types
pub use csv::{
    load_from_csv, load_from_csv_with_options, save_to_csv, save_to_csv_with_options, CsvOptions,
};
pub use error::{StorageError, StorageResult};
pub use metadata::{
    KnowledgeGraphInfo, KnowledgeGraphMetadata, KnowledgeGraphsMetadata, RelationMetadata,
};
pub use parquet::{load_from_parquet, save_to_parquet};
pub use wal::{replay_wal, Wal, WalEntry, WalOp};

// Re-export persist types
pub use persist::{
    consolidate, consolidate_to_current, to_tuples, Batch, BatchRef, FilePersist, PersistBackend,
    PersistConfig, PersistWal, ShardInfo, ShardMeta, Update,
};
