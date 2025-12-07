//! DD-Native Persistence Layer
//!
//! This module implements persistence for Differential Dataflow-style
//! (data, time, diff) updates, following the approach used by Materialize.
//!
//! ## Architecture
//!
//! ```text
//! Insert/Delete
//!     ↓
//! Update{data, time, diff}
//!     ↓
//! WAL (immediate durability)
//!     ↓
//! In-memory buffer
//!     ↓ (when buffer full)
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
pub use consolidate::{consolidate, consolidate_to_current, filter_since, to_tuples, to_tuple2s, to_tuples_with_multiplicity, to_tuple2s_with_multiplicity};
pub use wal::PersistWal;

use crate::code_generator::Tuple2;
use crate::storage::{StorageError, StorageResult};
use crate::value::{Tuple, Value, TupleSchema, DataType, tuples_to_record_batch, record_batch_to_tuples};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

// Parquet I/O for batches
use arrow::array::{Int32Array, Int64Array, UInt64Array, ArrayRef};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

/// Configuration for the persist layer
#[derive(Debug, Clone)]
pub struct PersistConfig {
    /// Base directory for persist data
    pub path: PathBuf,
    /// Buffer size before flushing to batch file
    pub buffer_size: usize,
    /// Whether to sync WAL immediately on each write
    pub immediate_sync: bool,
}

impl Default for PersistConfig {
    fn default() -> Self {
        PersistConfig {
            path: PathBuf::from("./data/persist"),
            buffer_size: 10000,
            immediate_sync: true,
        }
    }
}

/// Trait for persist backends
pub trait PersistBackend: Send + Sync {
    /// Append updates to a shard
    fn append(&self, shard: &str, updates: &[Update]) -> StorageResult<()>;

    /// Read all updates for a shard since a frontier
    fn read(&self, shard: &str, since: u64) -> StorageResult<Vec<Update>>;

    /// Compact a shard to a frontier (discard history before `since`)
    fn compact(&self, shard: &str, new_since: u64) -> StorageResult<()>;

    /// List all shards
    fn list_shards(&self) -> StorageResult<Vec<String>>;

    /// Get shard metadata
    fn shard_info(&self, shard: &str) -> StorageResult<ShardInfo>;

    /// Ensure a shard exists
    fn ensure_shard(&self, shard: &str) -> StorageResult<()>;

    /// Sync all pending writes to disk
    fn sync(&self) -> StorageResult<()>;

    /// Flush buffered updates for a shard to a batch file
    fn flush(&self, shard: &str) -> StorageResult<()>;
}

/// In-memory state for a shard
struct ShardState {
    meta: ShardMeta,
    buffer: Vec<Update>,
}

/// File-based persist implementation
pub struct FilePersist {
    config: PersistConfig,
    shards: RwLock<HashMap<String, ShardState>>,
    wal: Mutex<PersistWal>,
    next_batch_id: AtomicU64,
}

impl FilePersist {
    /// Create a new FilePersist instance
    pub fn new(config: PersistConfig) -> StorageResult<Self> {
        // Create directory structure
        fs::create_dir_all(&config.path)?;
        fs::create_dir_all(config.path.join("shards"))?;
        fs::create_dir_all(config.path.join("batches"))?;

        let wal = PersistWal::new(config.path.join("wal"))?;

        let mut persist = FilePersist {
            config,
            shards: RwLock::new(HashMap::new()),
            wal: Mutex::new(wal),
            next_batch_id: AtomicU64::new(1),
        };

        // Load existing shards and replay WAL
        persist.load_shards()?;
        persist.replay_wal()?;

        Ok(persist)
    }

    /// Load shard metadata from disk
    fn load_shards(&mut self) -> StorageResult<()> {
        let shards_dir = self.config.path.join("shards");
        if !shards_dir.exists() {
            return Ok(());
        }

        let mut shards = self.shards.write().unwrap();

        for entry in fs::read_dir(&shards_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let content = fs::read_to_string(&path)?;
                let meta: ShardMeta = serde_json::from_str(&content)
                    .map_err(|e| StorageError::Other(format!("Failed to parse shard metadata: {}", e)))?;

                // Update next_batch_id if needed
                for batch in &meta.batches {
                    if let Ok(id) = batch.id.parse::<u64>() {
                        let current = self.next_batch_id.load(Ordering::Relaxed);
                        if id >= current {
                            self.next_batch_id.store(id + 1, Ordering::Relaxed);
                        }
                    }
                }

                shards.insert(meta.name.clone(), ShardState {
                    meta,
                    buffer: Vec::new(),
                });
            }
        }

        Ok(())
    }

    /// Replay WAL entries into shard buffers
    fn replay_wal(&self) -> StorageResult<()> {
        let wal = self.wal.lock().unwrap();
        let entries = wal.read_all()?;

        let mut shards = self.shards.write().unwrap();

        for entry in entries {
            let state = shards.entry(entry.shard.clone()).or_insert_with(|| {
                ShardState {
                    meta: ShardMeta::new(entry.shard.clone()),
                    buffer: Vec::new(),
                }
            });
            state.buffer.push(entry.update);
        }

        Ok(())
    }

    /// Save shard metadata to disk
    fn save_shard_meta(&self, meta: &ShardMeta) -> StorageResult<()> {
        let path = self.config.path.join("shards").join(format!("{}.json", sanitize_name(&meta.name)));
        let content = serde_json::to_string_pretty(meta)
            .map_err(|e| StorageError::Other(format!("Failed to serialize shard metadata: {}", e)))?;
        fs::write(&path, content)?;
        Ok(())
    }

    /// Generate a unique batch ID
    fn generate_batch_id(&self) -> String {
        self.next_batch_id.fetch_add(1, Ordering::Relaxed).to_string()
    }

    /// Write a batch to a Parquet file
    fn write_batch(&self, updates: &[Update]) -> StorageResult<(String, PathBuf)> {
        let batch_id = self.generate_batch_id();
        let path = self.config.path.join("batches").join(format!("{}.parquet", batch_id));

        write_updates_parquet(&path, updates)?;

        Ok((batch_id, path))
    }

    /// Read updates from a batch file
    fn read_batch(&self, batch_ref: &BatchRef) -> StorageResult<Vec<Update>> {
        read_updates_parquet(&batch_ref.path)
    }
}

impl PersistBackend for FilePersist {
    fn append(&self, shard: &str, updates: &[Update]) -> StorageResult<()> {
        if updates.is_empty() {
            return Ok(());
        }

        // Write to WAL first (durability)
        {
            let mut wal = self.wal.lock().unwrap();
            wal.append_batch(shard, updates)?;
        }

        // Add to buffer
        let should_flush = {
            let mut shards = self.shards.write().unwrap();
            let state = shards.entry(shard.to_string()).or_insert_with(|| {
                ShardState {
                    meta: ShardMeta::new(shard.to_string()),
                    buffer: Vec::new(),
                }
            });

            state.buffer.extend_from_slice(updates);

            // Update upper frontier
            for update in updates {
                if update.time >= state.meta.upper {
                    state.meta.upper = update.time + 1;
                }
            }

            state.buffer.len() >= self.config.buffer_size
        };

        // Flush if buffer is full
        if should_flush {
            self.flush(shard)?;
        }

        Ok(())
    }

    fn read(&self, shard: &str, since: u64) -> StorageResult<Vec<Update>> {
        let shards = self.shards.read().unwrap();

        let state = shards.get(shard).ok_or_else(|| {
            StorageError::Other(format!("Shard not found: {}", shard))
        })?;

        let mut updates = Vec::new();

        // Read from batch files
        for batch_ref in &state.meta.batches {
            if batch_ref.upper > since {
                let batch_updates = self.read_batch(batch_ref)?;
                updates.extend(batch_updates.into_iter().filter(|u| u.time >= since));
            }
        }

        // Add buffered updates
        updates.extend(state.buffer.iter().filter(|u| u.time >= since).cloned());

        Ok(updates)
    }

    fn compact(&self, shard: &str, new_since: u64) -> StorageResult<()> {
        // Flush first to ensure all data is in batches
        self.flush(shard)?;

        let mut shards = self.shards.write().unwrap();
        let state = shards.get_mut(shard).ok_or_else(|| {
            StorageError::Other(format!("Shard not found: {}", shard))
        })?;

        // Read all updates
        let mut all_updates = Vec::new();
        for batch_ref in &state.meta.batches {
            let batch_updates = self.read_batch(batch_ref)?;
            all_updates.extend(batch_updates);
        }

        // Filter and consolidate
        let mut filtered: Vec<Update> = all_updates.into_iter()
            .filter(|u| u.time >= new_since)
            .collect();
        consolidate(&mut filtered);

        // Remove old batch files
        for batch_ref in &state.meta.batches {
            let _ = fs::remove_file(&batch_ref.path);
        }
        state.meta.batches.clear();

        // Write new compacted batch if not empty
        if !filtered.is_empty() {
            let batch = Batch::new(filtered.clone());
            let (batch_id, path) = self.write_batch(&filtered)?;

            state.meta.add_batch(BatchRef {
                id: batch_id,
                path,
                lower: batch.lower,
                upper: batch.upper,
                len: batch.len(),
            });
        }

        // Update since frontier
        state.meta.advance_since(new_since);
        self.save_shard_meta(&state.meta)?;

        Ok(())
    }

    fn list_shards(&self) -> StorageResult<Vec<String>> {
        let shards = self.shards.read().unwrap();
        Ok(shards.keys().cloned().collect())
    }

    fn shard_info(&self, shard: &str) -> StorageResult<ShardInfo> {
        let shards = self.shards.read().unwrap();
        let state = shards.get(shard).ok_or_else(|| {
            StorageError::Other(format!("Shard not found: {}", shard))
        })?;
        Ok(ShardInfo::from(&state.meta))
    }

    fn ensure_shard(&self, shard: &str) -> StorageResult<()> {
        let mut shards = self.shards.write().unwrap();
        if !shards.contains_key(shard) {
            let meta = ShardMeta::new(shard.to_string());
            self.save_shard_meta(&meta)?;
            shards.insert(shard.to_string(), ShardState {
                meta,
                buffer: Vec::new(),
            });
        }
        Ok(())
    }

    fn sync(&self) -> StorageResult<()> {
        let mut wal = self.wal.lock().unwrap();
        wal.sync()
    }

    fn flush(&self, shard: &str) -> StorageResult<()> {
        let mut shards = self.shards.write().unwrap();
        let state = shards.get_mut(shard).ok_or_else(|| {
            StorageError::Other(format!("Shard not found: {}", shard))
        })?;

        if state.buffer.is_empty() {
            return Ok(());
        }

        // Write buffer to batch file
        let batch = Batch::new(state.buffer.clone());
        let (batch_id, path) = self.write_batch(&state.buffer)?;

        state.meta.add_batch(BatchRef {
            id: batch_id,
            path,
            lower: batch.lower,
            upper: batch.upper,
            len: batch.len(),
        });

        state.buffer.clear();

        // Save metadata
        self.save_shard_meta(&state.meta)?;

        // Clear WAL (data is now durable in batch file)
        {
            let mut wal = self.wal.lock().unwrap();
            wal.clear()?;
        }

        Ok(())
    }
}

// ============================================================================
// Parquet I/O for Update batches
// ============================================================================

/// Infer schema from updates - needed because we don't have stored schema yet
fn infer_schema_from_updates(updates: &[Update]) -> TupleSchema {
    if updates.is_empty() {
        // Default to 2-column Int32 schema for backwards compatibility
        return TupleSchema::new(vec![
            ("col0".to_string(), DataType::Int32),
            ("col1".to_string(), DataType::Int32),
        ]);
    }

    let first = &updates[0].data;
    let fields: Vec<(String, DataType)> = first
        .values()
        .iter()
        .enumerate()
        .map(|(i, v)| (format!("col{}", i), v.data_type()))
        .collect();

    TupleSchema::new(fields)
}

/// Write updates to a Parquet file
///
/// The file format is:
/// - N data columns (from the Tuple)
/// - time column (UInt64)
/// - diff column (Int64)
fn write_updates_parquet(path: &PathBuf, updates: &[Update]) -> StorageResult<()> {
    if updates.is_empty() {
        // Write empty file with default schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("col0", ArrowDataType::Int32, false),
            Field::new("col1", ArrowDataType::Int32, false),
            Field::new("time", ArrowDataType::UInt64, false),
            Field::new("diff", ArrowDataType::Int64, false),
        ]));

        let file = fs::File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| StorageError::Parquet(e))?;

        let empty_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(Vec::<i32>::new())),
                Arc::new(Int32Array::from(Vec::<i32>::new())),
                Arc::new(UInt64Array::from(Vec::<u64>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            ],
        ).map_err(|e| StorageError::Arrow(e))?;

        writer.write(&empty_batch).map_err(|e| StorageError::Parquet(e))?;
        writer.close().map_err(|e| StorageError::Parquet(e))?;

        return Ok(());
    }

    // Infer schema from the data
    let tuple_schema = infer_schema_from_updates(updates);

    // Extract tuples for conversion
    let tuples: Vec<Tuple> = updates.iter().map(|u| u.data.clone()).collect();

    // Convert tuples to record batch
    let data_batch = tuples_to_record_batch(&tuples, &tuple_schema)
        .map_err(|e| StorageError::Other(format!("Arrow conversion error: {}", e)))?;

    // Build full schema with time and diff columns
    let mut fields: Vec<Field> = data_batch.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new("time", ArrowDataType::UInt64, false));
    fields.push(Field::new("diff", ArrowDataType::Int64, false));
    let full_schema = Arc::new(Schema::new(fields));

    // Build columns array
    let mut columns: Vec<ArrayRef> = data_batch.columns().to_vec();

    // Add time and diff columns
    let times: Vec<u64> = updates.iter().map(|u| u.time).collect();
    let diffs: Vec<i64> = updates.iter().map(|u| u.diff).collect();
    columns.push(Arc::new(UInt64Array::from(times)));
    columns.push(Arc::new(Int64Array::from(diffs)));

    let batch = RecordBatch::try_new(full_schema.clone(), columns)
        .map_err(|e| StorageError::Arrow(e))?;

    let file = fs::File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, full_schema, Some(props))
        .map_err(|e| StorageError::Parquet(e))?;

    writer.write(&batch).map_err(|e| StorageError::Parquet(e))?;
    writer.close().map_err(|e| StorageError::Parquet(e))?;

    Ok(())
}

/// Read updates from a Parquet file
fn read_updates_parquet(path: &PathBuf) -> StorageResult<Vec<Update>> {
    let file = fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| StorageError::Parquet(e))?;

    let reader = builder.build().map_err(|e| StorageError::Parquet(e))?;

    let mut updates = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(|e| StorageError::Arrow(e))?;
        let num_cols = batch.num_columns();

        // Last two columns are always time and diff
        // Data columns are all columns except the last two
        if num_cols < 2 {
            return Err(StorageError::Other("Invalid parquet file: not enough columns".to_string()));
        }

        let time_col_idx = num_cols - 2;
        let diff_col_idx = num_cols - 1;

        let times = batch.column(time_col_idx).as_any().downcast_ref::<UInt64Array>()
            .ok_or_else(|| StorageError::Other("Invalid time column type".to_string()))?;
        let diffs = batch.column(diff_col_idx).as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| StorageError::Other("Invalid diff column type".to_string()))?;

        // Create a sub-batch with only data columns
        let data_schema = Arc::new(Schema::new(
            batch.schema().fields()[..time_col_idx].iter().map(|f| f.as_ref().clone()).collect::<Vec<_>>()
        ));
        let data_columns: Vec<ArrayRef> = batch.columns()[..time_col_idx].to_vec();

        if data_columns.is_empty() {
            // No data columns - shouldn't happen but handle gracefully
            continue;
        }

        let data_batch = RecordBatch::try_new(data_schema, data_columns)
            .map_err(|e| StorageError::Arrow(e))?;

        // Convert data batch back to tuples
        let (tuples, _) = record_batch_to_tuples(&data_batch)
            .map_err(|e| StorageError::Other(format!("Arrow conversion error: {}", e)))?;

        // Combine with time and diff
        for (i, tuple) in tuples.into_iter().enumerate() {
            updates.push(Update {
                data: tuple,
                time: times.value(i),
                diff: diffs.value(i),
            });
        }
    }

    Ok(updates)
}

/// Sanitize a shard name for use as a filename
fn sanitize_name(name: &str) -> String {
    name.replace(':', "_").replace('/', "_")
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_persist() -> (TempDir, FilePersist) {
        let temp = TempDir::new().unwrap();
        let config = PersistConfig {
            path: temp.path().to_path_buf(),
            buffer_size: 5,
            immediate_sync: true,
        };
        let persist = FilePersist::new(config).unwrap();
        (temp, persist)
    }

    #[test]
    fn test_append_and_read() {
        let (_temp, persist) = create_test_persist();

        let updates = vec![
            Update::insert(Tuple::from_pair(1, 2), 10),
            Update::insert(Tuple::from_pair(3, 4), 20),
        ];

        persist.ensure_shard("db:edge").unwrap();
        persist.append("db:edge", &updates).unwrap();

        let read = persist.read("db:edge", 0).unwrap();
        assert_eq!(read.len(), 2);
    }

    #[test]
    fn test_flush_and_read() {
        let (_temp, persist) = create_test_persist();

        let updates = vec![
            Update::insert(Tuple::from_pair(1, 2), 10),
            Update::insert(Tuple::from_pair(3, 4), 20),
        ];

        persist.ensure_shard("db:edge").unwrap();
        persist.append("db:edge", &updates).unwrap();
        persist.flush("db:edge").unwrap();

        // After flush, data should be in batch file
        let info = persist.shard_info("db:edge").unwrap();
        assert_eq!(info.batch_count, 1);

        let read = persist.read("db:edge", 0).unwrap();
        assert_eq!(read.len(), 2);
    }

    #[test]
    fn test_auto_flush_on_buffer_full() {
        let (_temp, persist) = create_test_persist();  // buffer_size = 5

        persist.ensure_shard("db:edge").unwrap();

        // Add 6 updates (exceeds buffer of 5)
        for i in 0..6 {
            persist.append("db:edge", &[Update::insert(Tuple::from_pair(i, i), i as u64)]).unwrap();
        }

        // Should have flushed
        let info = persist.shard_info("db:edge").unwrap();
        assert!(info.batch_count >= 1);
    }

    #[test]
    fn test_consolidate_on_read() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:edge").unwrap();

        // Insert and delete the same tuple
        persist.append("db:edge", &[Update::insert(Tuple::from_pair(1, 2), 10)]).unwrap();
        persist.append("db:edge", &[Update::delete(Tuple::from_pair(1, 2), 10)]).unwrap();
        persist.append("db:edge", &[Update::insert(Tuple::from_pair(3, 4), 20)]).unwrap();

        let mut updates = persist.read("db:edge", 0).unwrap();
        consolidate(&mut updates);

        // (1,2) should cancel out
        let tuples = to_tuple2s(&updates);
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0], (3, 4));
    }

    #[test]
    fn test_compaction() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:edge").unwrap();

        // Add updates at different times
        persist.append("db:edge", &[Update::insert(Tuple::from_pair(1, 2), 10)]).unwrap();
        persist.append("db:edge", &[Update::insert(Tuple::from_pair(3, 4), 20)]).unwrap();
        persist.append("db:edge", &[Update::insert(Tuple::from_pair(5, 6), 30)]).unwrap();
        persist.flush("db:edge").unwrap();

        // Compact to time 15 (should discard time 10)
        persist.compact("db:edge", 15).unwrap();

        let info = persist.shard_info("db:edge").unwrap();
        assert_eq!(info.since, 15);

        let updates = persist.read("db:edge", 0).unwrap();
        // Only updates at time >= 15 should remain
        assert!(updates.iter().all(|u| u.time >= 15));
    }

    #[test]
    fn test_list_shards() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db1:edge").unwrap();
        persist.ensure_shard("db1:node").unwrap();
        persist.ensure_shard("db2:edge").unwrap();

        let shards = persist.list_shards().unwrap();
        assert_eq!(shards.len(), 3);
        assert!(shards.contains(&"db1:edge".to_string()));
        assert!(shards.contains(&"db1:node".to_string()));
        assert!(shards.contains(&"db2:edge".to_string()));
    }

    #[test]
    fn test_persistence_across_restarts() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().to_path_buf();

        // First instance: write data
        {
            let config = PersistConfig {
                path: path.clone(),
                buffer_size: 100,
                immediate_sync: true,
            };
            let persist = FilePersist::new(config).unwrap();

            persist.ensure_shard("db:edge").unwrap();
            persist.append("db:edge", &[
                Update::insert(Tuple::from_pair(1, 2), 10),
                Update::insert(Tuple::from_pair(3, 4), 20),
            ]).unwrap();
            persist.flush("db:edge").unwrap();
        }

        // Second instance: should see the data
        {
            let config = PersistConfig {
                path: path.clone(),
                buffer_size: 100,
                immediate_sync: true,
            };
            let persist = FilePersist::new(config).unwrap();

            let shards = persist.list_shards().unwrap();
            assert!(shards.contains(&"db:edge".to_string()));

            let updates = persist.read("db:edge", 0).unwrap();
            assert_eq!(updates.len(), 2);
        }
    }

    #[test]
    fn test_multi_arity_tuples() {
        let (_temp, persist) = create_test_persist();

        // Test with 3-arity tuples
        let updates = vec![
            Update::insert(Tuple::new(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]), 10),
            Update::insert(Tuple::new(vec![Value::Int32(4), Value::Int32(5), Value::Int32(6)]), 20),
        ];

        persist.ensure_shard("db:triple").unwrap();
        persist.append("db:triple", &updates).unwrap();
        persist.flush("db:triple").unwrap();

        let read = persist.read("db:triple", 0).unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].data.arity(), 3);
        assert_eq!(read[0].data.get(0), Some(&Value::Int32(1)));
        assert_eq!(read[0].data.get(2), Some(&Value::Int32(3)));
    }

    #[test]
    fn test_mixed_type_tuples() {
        let (_temp, persist) = create_test_persist();

        // Test with mixed types
        let updates = vec![
            Update::insert(
                Tuple::new(vec![Value::Int32(1), Value::string("hello"), Value::Float64(3.14)]),
                10
            ),
            Update::insert(
                Tuple::new(vec![Value::Int32(2), Value::string("world"), Value::Float64(2.71)]),
                20
            ),
        ];

        persist.ensure_shard("db:mixed").unwrap();
        persist.append("db:mixed", &updates).unwrap();
        persist.flush("db:mixed").unwrap();

        let read = persist.read("db:mixed", 0).unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].data.arity(), 3);
        assert_eq!(read[0].data.get(0), Some(&Value::Int32(1)));
        assert_eq!(read[0].data.get(1).and_then(|v| v.as_str()), Some("hello"));
    }

    #[test]
    fn test_legacy_tuple2_compatibility() {
        let (_temp, persist) = create_test_persist();

        // Use legacy insert_tuple2 method
        let updates = vec![
            Update::insert_tuple2((1, 2), 10),
            Update::insert_tuple2((3, 4), 20),
        ];

        persist.ensure_shard("db:legacy").unwrap();
        persist.append("db:legacy", &updates).unwrap();
        persist.flush("db:legacy").unwrap();

        let read = persist.read("db:legacy", 0).unwrap();
        assert_eq!(read.len(), 2);

        // Verify we can convert back to Tuple2
        let tuple2s = to_tuple2s(&read);
        assert_eq!(tuple2s.len(), 2);
        assert!(tuple2s.contains(&(1, 2)));
        assert!(tuple2s.contains(&(3, 4)));
    }
}
