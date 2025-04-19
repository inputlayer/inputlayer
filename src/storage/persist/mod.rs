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
