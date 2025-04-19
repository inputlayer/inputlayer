//! DD-native persistence layer.
//!
//! Persists Differential Dataflow (data, time, diff) updates,
//! following the approach used by Materialize.
//!
//! ## Architecture
//!
//! ```text
//! Insert/Delete
//!     |
//! Update{data, time, diff}
//!     |
//! WAL (immediate durability)
//!     |
//! In-memory buffer
//!     | (when buffer full)
//! Batch file (Parquet)
//! ```
//!
//! ## Recovery
//!
//! On startup:
//! 1. Load shard metadata
//! 2. Read batch files
//! 3. Replay WAL (uncommitted updates)
//! 4. Consolidate to get current state

pub mod batch;
pub mod consolidate;
pub mod wal;

pub use batch::{Batch, BatchRef, ShardInfo, ShardMeta, Update};
pub use consolidate::{
    consolidate, consolidate_to_current, filter_since, to_tuples, to_tuples_with_multiplicity,
};
pub use wal::PersistWal;

use crate::storage::{StorageError, StorageResult};
use crate::value::{record_batch_to_tuples, tuples_to_record_batch, DataType, Tuple, TupleSchema};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

// Parquet I/O for batches
use arrow::array::{ArrayRef, Int32Array, Int64Array, UInt64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

use crate::config::DurabilityMode;

/// Configuration for the persist layer
#[derive(Debug, Clone)]
pub struct PersistConfig {
    /// Base directory for persist data
    pub path: PathBuf,
    /// Buffer size before flushing to batch file
    pub buffer_size: usize,
    /// Whether to sync WAL immediately on each write (DEPRECATED)
    pub immediate_sync: bool,
    /// Durability mode for writes
    pub durability_mode: DurabilityMode,
}

impl Default for PersistConfig {
    fn default() -> Self {
        PersistConfig {
            path: PathBuf::from("./data/persist"),
            buffer_size: 10000,
            immediate_sync: true,
            durability_mode: DurabilityMode::Immediate,
        }
    }
}

/// Trait for persist backends
