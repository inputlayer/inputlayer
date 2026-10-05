//! Storage Module
//!
//! Provides persistent storage functionality for `InputLayer`:
//! - DD-native persistence with (data, time, diff) triples
//! - Parquet serialization (columnar, compressed, efficient for analytics)
//! - CSV serialization (human-readable, interoperable)
//! - Metadata management
//! - Exclusive data directory ownership
//! - Offline backup and restore of a data directory
//! - Error handling
//!
//! ## Persistence Model
//!
//! `InputLayer` uses Differential Dataflow-native persistence:
//! - Updates are stored as `(data, time, diff)` triples
//! - Consolidation sums diffs to compute current state
//! - Each write commits one `Transaction`, made durable as one WAL record
//! - WAL provides immediate durability, batches provide efficient reads
//!
//! ## Format Selection
//!
//! - Parquet: Best for large datasets, analytics workloads, and production use
//! - CSV: Best for data exchange, debugging, and human inspection

pub mod backup;
pub mod csv;
pub mod data_dir_lock;
pub mod error;
pub mod metadata;
pub mod nested_json;
pub mod parquet;
pub mod persist;

// Re-export commonly used types
pub use csv::{
    load_from_csv, load_from_csv_with_options, save_to_csv, save_to_csv_with_options, CsvOptions,
};
pub use data_dir_lock::DataDirLock;
pub use error::{StorageError, StorageResult};
pub use metadata::{
    DropTombstones, KnowledgeGraphInfo, KnowledgeGraphMetadata, KnowledgeGraphsMetadata,
    RelationMetadata, RelationTombstone,
};
pub use parquet::{load_from_parquet, save_to_parquet};

// Re-export persist types
pub use persist::{
    consolidate, consolidate_to_current, to_tuples, Batch, BatchRef, FilePersist, PersistBackend,
    PersistConfig, PersistWal, ShardInfo, ShardMeta, Transaction, TxnOp, Update,
};
