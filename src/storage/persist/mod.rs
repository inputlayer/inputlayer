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
    /// Durability mode for writes
    pub durability_mode: DurabilityMode,
}

impl Default for PersistConfig {
    fn default() -> Self {
        PersistConfig {
            path: PathBuf::from("./data/persist"),
            buffer_size: 10000,
            durability_mode: DurabilityMode::Immediate,
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

    /// Delete a shard and all its data (metadata, batch files, in-memory state)
    fn delete_shard(&self, shard: &str) -> StorageResult<()>;
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
    /// Create a new `FilePersist` instance
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

        // Clean up stale .archived and .new WAL files from previous runs
        {
            let wal = persist.wal.lock();
            wal.cleanup_archives()?;
        }

        Ok(persist)
    }

    /// Load shard metadata from disk
    fn load_shards(&mut self) -> StorageResult<()> {
        let shards_dir = self.config.path.join("shards");
        if !shards_dir.exists() {
            return Ok(());
        }

        let mut shards = self.shards.write();

        for entry in fs::read_dir(&shards_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let content = fs::read_to_string(&path)?;
                let meta: ShardMeta = serde_json::from_str(&content).map_err(|e| {
                    StorageError::Other(format!("Failed to parse shard metadata: {e}"))
                })?;

                // Update next_batch_id if needed
                for batch in &meta.batches {
                    if let Ok(id) = batch.id.parse::<u64>() {
                        let current = self.next_batch_id.load(Ordering::Relaxed);
                        if id >= current {
                            self.next_batch_id.store(id + 1, Ordering::Relaxed);
                        }
                    }
                }

                shards.insert(
                    meta.name.clone(),
                    ShardState {
                        meta,
                        buffer: Vec::new(),
                    },
                );
            }
        }

        Ok(())
    }

    /// Replay WAL entries into shard buffers
    fn replay_wal(&self) -> StorageResult<()> {
        let wal = self.wal.lock();
        let entries = wal.read_all()?;

        let mut shards = self.shards.write();

        for entry in entries {
            let state = shards
                .entry(entry.shard.clone())
                .or_insert_with(|| ShardState {
                    meta: ShardMeta::new(entry.shard.clone()),
                    buffer: Vec::new(),
                });
            state.buffer.push(entry.update);
        }

        Ok(())
    }

    /// Save shard metadata to disk using atomic write-to-temp+rename.
    ///
    /// Writes to `{name}.json.tmp`, calls `sync_all()`, then renames to `{name}.json`.
    /// Rename is atomic on POSIX, so the metadata file is always either the old
    /// or new version — never a corrupt half-written state.
    fn save_shard_meta(&self, meta: &ShardMeta) -> StorageResult<()> {
        let dir = self.config.path.join("shards");
        let final_path = dir.join(format!("{}.json", sanitize_name(&meta.name)));
        let tmp_path = dir.join(format!("{}.json.tmp", sanitize_name(&meta.name)));
        let content = serde_json::to_string_pretty(meta)
            .map_err(|e| StorageError::Other(format!("Failed to serialize shard metadata: {e}")))?;

        // Write to temp file
        if let Err(e) = fs::write(&tmp_path, &content) {
            eprintln!(
                "[persist] ERROR save_shard_meta: path={}, parent_exists={}, error={}",
                tmp_path.display(),
                tmp_path.parent().is_some_and(std::path::Path::exists),
                e
            );
            return Err(e.into());
        }

        // Sync to disk before rename
        fs::File::open(&tmp_path)?.sync_all()?;

        // Atomic rename
        fs::rename(&tmp_path, &final_path)?;

        Ok(())
    }

    /// Generate a unique batch ID
    fn generate_batch_id(&self) -> String {
        self.next_batch_id
            .fetch_add(1, Ordering::Relaxed)
            .to_string()
    }

    /// Write a batch to a Parquet file
    fn write_batch(&self, updates: &[Update]) -> StorageResult<(String, PathBuf)> {
        let batch_id = self.generate_batch_id();
        let path = self
            .config
            .path
            .join("batches")
            .join(format!("{batch_id}.parquet"));

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

        // Handle WAL based on durability mode
        match self.config.durability_mode {
            DurabilityMode::Immediate => {
                // Write to WAL with immediate sync (safest)
                let mut wal = self.wal.lock();
                wal.append_batch(shard, updates)?;
            }
            DurabilityMode::Batched => {
                // Write to WAL without sync (faster, batched durability)
                let mut wal = self.wal.lock();
                wal.append_batch_buffered(shard, updates)?;
            }
            DurabilityMode::Async => {
                // Skip WAL entirely for maximum speed (in-memory only until flush)
                // Data may be lost on crash, but in-memory operations are fast
            }
        }

        // Add to buffer
        let should_flush = {
            let mut shards = self.shards.write();
            let state = shards
                .entry(shard.to_string())
                .or_insert_with(|| ShardState {
                    meta: ShardMeta::new(shard.to_string()),
                    buffer: Vec::new(),
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
        let shards = self.shards.read();

        let state = shards
            .get(shard)
            .ok_or_else(|| StorageError::Other(format!("Shard not found: {shard}")))?;

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

        let mut shards = self.shards.write();
        let state = shards
            .get_mut(shard)
            .ok_or_else(|| StorageError::Other(format!("Shard not found: {shard}")))?;

        // Read all updates
        let mut all_updates = Vec::new();
        for batch_ref in &state.meta.batches {
            let batch_updates = self.read_batch(batch_ref)?;
            all_updates.extend(batch_updates);
        }

        // Filter and consolidate
        let mut filtered: Vec<Update> = all_updates
            .into_iter()
            .filter(|u| u.time >= new_since)
            .collect();
        consolidate(&mut filtered);

        // Remember old batch refs for cleanup after the new batch is durable
        let old_batches: Vec<BatchRef> = state.meta.batches.drain(..).collect();

        // Step 1: Write new compacted batch FIRST (crash-safe ordering)
        // If we crash here, old batches still exist and metadata still points to them.
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

        // Step 2: Update metadata atomically (write-to-temp+rename in save_shard_meta)
        // After this succeeds, metadata points to the new batch only.
        state.meta.advance_since(new_since);
        self.save_shard_meta(&state.meta)?;

        // Step 3: Delete old batch files LAST (safe — metadata no longer references them)
        // If we crash here, we have orphaned files but no data loss.
        for batch_ref in &old_batches {
            let _ = fs::remove_file(&batch_ref.path);
        }

        // Sync batches directory to ensure deletions are durable
        if !old_batches.is_empty() {
            sync_directory(&self.config.path.join("batches"));
        }

        Ok(())
    }

    fn list_shards(&self) -> StorageResult<Vec<String>> {
        let shards = self.shards.read();
        Ok(shards.keys().cloned().collect())
    }

    fn shard_info(&self, shard: &str) -> StorageResult<ShardInfo> {
        let shards = self.shards.read();
        let state = shards
            .get(shard)
            .ok_or_else(|| StorageError::Other(format!("Shard not found: {shard}")))?;
        Ok(ShardInfo::from(&state.meta))
    }

    fn ensure_shard(&self, shard: &str) -> StorageResult<()> {
        let mut shards = self.shards.write();
        if !shards.contains_key(shard) {
            let meta = ShardMeta::new(shard.to_string());
            self.save_shard_meta(&meta)?;
            shards.insert(
                shard.to_string(),
                ShardState {
                    meta,
                    buffer: Vec::new(),
                },
            );
        }
        Ok(())
    }

    fn sync(&self) -> StorageResult<()> {
        let mut wal = self.wal.lock();
        wal.sync()
    }

    fn flush(&self, shard: &str) -> StorageResult<()> {
        let mut shards = self.shards.write();
        let state = shards
            .get_mut(shard)
            .ok_or_else(|| StorageError::Other(format!("Shard not found: {shard}")))?;

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

        // Remove only THIS shard's WAL entries (preserves other shards)
        {
            let mut wal = self.wal.lock();
            wal.remove_shard_entries(shard)?;
        }

        Ok(())
    }

    fn delete_shard(&self, shard: &str) -> StorageResult<()> {
        // Step 1: Remove from in-memory shard map (fast, under write lock)
        let removed_state = {
            let mut shards = self.shards.write();
            shards.remove(shard)
        }; // write lock released — other shards unblocked

        // Step 2: Delete shard metadata file FIRST (crash-safe ordering)
        let meta_path = self
            .config
            .path
            .join("shards")
            .join(format!("{}.json", sanitize_name(shard)));
        if meta_path.exists() {
            let _ = fs::remove_file(&meta_path);
            // Sync shards directory to ensure metadata deletion is durable
            sync_directory(&self.config.path.join("shards"));
        }

        // Step 3: Selective WAL filter — remove only this shard's entries
        // Other shards' WAL data is PRESERVED (no need to flush them)
        {
            let mut wal = self.wal.lock();
            wal.remove_shard_entries(shard)?;
        }

        // Step 4: Delete batch files (no lock needed)
        if let Some(state) = removed_state {
            let mut deleted_any = false;
            for batch_ref in &state.meta.batches {
                if batch_ref.path.exists() {
                    let _ = fs::remove_file(&batch_ref.path);
                    deleted_any = true;
                }
            }
            // Sync batches directory to ensure file deletions are durable
            if deleted_any {
                sync_directory(&self.config.path.join("batches"));
            }
        }

        Ok(())
    }
}

// Parquet I/O for Update batches
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
        .map(|(i, v)| (format!("col{i}"), v.data_type()))
        .collect();

    TupleSchema::new(fields)
}

/// Write updates to a Parquet file
///
/// The file format is:
/// - N data columns (from the Tuple)
/// - time column (`UInt64`)
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
            .map_err(StorageError::Parquet)?;

        let empty_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(Vec::<i32>::new())),
                Arc::new(Int32Array::from(Vec::<i32>::new())),
                Arc::new(UInt64Array::from(Vec::<u64>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            ],
        )
        .map_err(StorageError::Arrow)?;

        writer.write(&empty_batch).map_err(StorageError::Parquet)?;
        writer.close().map_err(StorageError::Parquet)?;

        // Ensure data is durably written to disk
        fs::File::open(path)?.sync_all()?;

        return Ok(());
    }

    // Infer schema from the data
    let tuple_schema = infer_schema_from_updates(updates);

    // Extract tuples for conversion
    let tuples: Vec<Tuple> = updates.iter().map(|u| u.data.clone()).collect();

    // Convert tuples to record batch
    let data_batch = tuples_to_record_batch(&tuples, &tuple_schema)
        .map_err(|e| StorageError::Other(format!("Arrow conversion error: {e}")))?;

    // Build full schema with time and diff columns
    let mut fields: Vec<Field> = data_batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
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

    let batch = RecordBatch::try_new(full_schema.clone(), columns).map_err(StorageError::Arrow)?;

    let file = fs::File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer =
        ArrowWriter::try_new(file, full_schema, Some(props)).map_err(StorageError::Parquet)?;

    writer.write(&batch).map_err(StorageError::Parquet)?;
    writer.close().map_err(StorageError::Parquet)?;

    // Ensure data is durably written to disk
    fs::File::open(path)?.sync_all()?;

    Ok(())
}

/// Read updates from a Parquet file
fn read_updates_parquet(path: &PathBuf) -> StorageResult<Vec<Update>> {
    let file = fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(StorageError::Parquet)?;

    let reader = builder.build().map_err(StorageError::Parquet)?;

    let mut updates = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(StorageError::Arrow)?;
        let num_cols = batch.num_columns();

        // Last two columns are always time and diff
        // Data columns are all columns except the last two
        if num_cols < 2 {
            return Err(StorageError::Other(
                "Invalid parquet file: not enough columns".to_string(),
            ));
        }

        let time_col_idx = num_cols - 2;
        let diff_col_idx = num_cols - 1;

        let times = batch
            .column(time_col_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| StorageError::Other("Invalid time column type".to_string()))?;
        let diffs = batch
            .column(diff_col_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| StorageError::Other("Invalid diff column type".to_string()))?;

        // Create a sub-batch with only data columns
        let data_schema = Arc::new(Schema::new(
            batch.schema().fields()[..time_col_idx]
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>(),
        ));
        let data_columns: Vec<ArrayRef> = batch.columns()[..time_col_idx].to_vec();

        if data_columns.is_empty() {
            // No data columns - shouldn't happen but handle gracefully
            continue;
        }

        let data_batch =
            RecordBatch::try_new(data_schema, data_columns).map_err(StorageError::Arrow)?;

        // Convert data batch back to tuples
        let (tuples, _) = record_batch_to_tuples(&data_batch)
            .map_err(|e| StorageError::Other(format!("Arrow conversion error: {e}")))?;

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

/// Sync a directory to ensure metadata operations (rename, unlink) are durable.
///
/// On POSIX systems, file deletion and rename are only guaranteed durable
/// after the parent directory inode is fsynced. Without this, a crash can
/// "resurrect" deleted files or roll back renames.
fn sync_directory(dir: &std::path::Path) {
    if let Ok(d) = fs::File::open(dir) {
        let _ = d.sync_all();
    }
}

/// Sanitize a shard name for use as a filename
fn sanitize_name(name: &str) -> String {
    name.replace([':', '/'], "_")
}

// Tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;
    use tempfile::TempDir;

    fn create_test_persist() -> (TempDir, FilePersist) {
        let temp = TempDir::new().unwrap();
        let config = PersistConfig {
            path: temp.path().to_path_buf(),
            buffer_size: 5,

            durability_mode: DurabilityMode::Immediate,
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
        let (_temp, persist) = create_test_persist(); // buffer_size = 5

        persist.ensure_shard("db:edge").unwrap();

        // Add 6 updates (exceeds buffer of 5)
        for i in 0..6 {
            persist
                .append(
                    "db:edge",
                    &[Update::insert(Tuple::from_pair(i, i), i as u64)],
                )
                .unwrap();
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
        persist
            .append("db:edge", &[Update::insert(Tuple::from_pair(1, 2), 10)])
            .unwrap();
        persist
            .append("db:edge", &[Update::delete(Tuple::from_pair(1, 2), 10)])
            .unwrap();
        persist
            .append("db:edge", &[Update::insert(Tuple::from_pair(3, 4), 20)])
            .unwrap();

        let mut updates = persist.read("db:edge", 0).unwrap();
        consolidate(&mut updates);

        // (1,2) should cancel out
        let tuples = to_tuples(&updates);
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].to_pair(), Some((3, 4)));
    }

    #[test]
    fn test_compaction() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:edge").unwrap();

        // Add updates at different times
        persist
            .append("db:edge", &[Update::insert(Tuple::from_pair(1, 2), 10)])
            .unwrap();
        persist
            .append("db:edge", &[Update::insert(Tuple::from_pair(3, 4), 20)])
            .unwrap();
        persist
            .append("db:edge", &[Update::insert(Tuple::from_pair(5, 6), 30)])
            .unwrap();
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

                durability_mode: DurabilityMode::Immediate,
            };
            let persist = FilePersist::new(config).unwrap();

            persist.ensure_shard("db:edge").unwrap();
            persist
                .append(
                    "db:edge",
                    &[
                        Update::insert(Tuple::from_pair(1, 2), 10),
                        Update::insert(Tuple::from_pair(3, 4), 20),
                    ],
                )
                .unwrap();
            persist.flush("db:edge").unwrap();
        }

        // Second instance: should see the data
        {
            let config = PersistConfig {
                path: path.clone(),
                buffer_size: 100,

                durability_mode: DurabilityMode::Immediate,
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
            Update::insert(
                Tuple::new(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]),
                10,
            ),
            Update::insert(
                Tuple::new(vec![Value::Int32(4), Value::Int32(5), Value::Int32(6)]),
                20,
            ),
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
                Tuple::new(vec![
                    Value::Int32(1),
                    Value::string("hello"),
                    Value::Float64(3.14),
                ]),
                10,
            ),
            Update::insert(
                Tuple::new(vec![
                    Value::Int32(2),
                    Value::string("world"),
                    Value::Float64(2.71),
                ]),
                20,
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

        // Use binary tuple insert
        let updates = vec![
            Update::insert(Tuple::from_pair(1, 2), 10),
            Update::insert(Tuple::from_pair(3, 4), 20),
        ];

        persist.ensure_shard("db:test").unwrap();
        persist.append("db:test", &updates).unwrap();
        persist.flush("db:test").unwrap();

        let read = persist.read("db:test", 0).unwrap();
        assert_eq!(read.len(), 2);

        // Verify we can read back the tuples
        let tuples = to_tuples(&read);
        assert_eq!(tuples.len(), 2);
        assert!(tuples.iter().any(|t| t.to_pair() == Some((1, 2))));
        assert!(tuples.iter().any(|t| t.to_pair() == Some((3, 4))));
    }

    #[test]
    fn test_persist_config_default() {
        let config = PersistConfig::default();
        assert_eq!(config.buffer_size, 10000);
        assert_eq!(config.path, PathBuf::from("./data/persist"));
        assert!(matches!(config.durability_mode, DurabilityMode::Immediate));
    }

    #[test]
    fn test_sanitize_name() {
        assert_eq!(sanitize_name("db:edge"), "db_edge");
        assert_eq!(sanitize_name("db/test/edge"), "db_test_edge");
        assert_eq!(sanitize_name("simple"), "simple");
        assert_eq!(sanitize_name("a:b/c:d"), "a_b_c_d");
    }

    #[test]
    fn test_ensure_shard_idempotent() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:test").unwrap();
        persist.ensure_shard("db:test").unwrap(); // Second call should be idempotent

        let shards = persist.list_shards().unwrap();
        assert_eq!(
            shards.iter().filter(|s| *s == "db:test").count(),
            1,
            "Shard should only appear once"
        );
    }

    #[test]
    fn test_read_nonexistent_shard() {
        let (_temp, persist) = create_test_persist();

        let result = persist.read("nonexistent", 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_shard_info_basic() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:info_test").unwrap();
        let info = persist.shard_info("db:info_test").unwrap();
        assert_eq!(info.batch_count, 0);
        assert_eq!(info.since, 0);
    }

    #[test]
    fn test_shard_info_nonexistent() {
        let (_temp, persist) = create_test_persist();
        let result = persist.shard_info("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_flush_empty_buffer() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:empty").unwrap();
        // Flushing empty buffer should be a no-op
        persist.flush("db:empty").unwrap();

        let info = persist.shard_info("db:empty").unwrap();
        assert_eq!(info.batch_count, 0);
    }

    #[test]
    fn test_append_empty_updates() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:empty_append").unwrap();
        persist.append("db:empty_append", &[]).unwrap();

        let read = persist.read("db:empty_append", 0).unwrap();
        assert!(read.is_empty());
    }

    #[test]
    fn test_sync() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:sync_test").unwrap();
        persist
            .append(
                "db:sync_test",
                &[Update::insert(Tuple::from_pair(1, 2), 10)],
            )
            .unwrap();

        // Sync should not error
        persist.sync().unwrap();
    }

    #[test]
    fn test_infer_schema_from_updates_empty() {
        let schema = infer_schema_from_updates(&[]);
        // Should return default 2-column Int32 schema
        assert_eq!(schema.arity(), 2);
    }

    #[test]
    fn test_infer_schema_from_updates_with_data() {
        let updates = vec![Update::insert(
            Tuple::new(vec![
                Value::Int32(1),
                Value::string("hello"),
                Value::Float64(3.14),
            ]),
            10,
        )];
        let schema = infer_schema_from_updates(&updates);
        assert_eq!(schema.arity(), 3);
    }

    #[test]
    fn test_read_with_since_filter() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:since_test").unwrap();
        persist
            .append(
                "db:since_test",
                &[
                    Update::insert(Tuple::from_pair(1, 2), 10),
                    Update::insert(Tuple::from_pair(3, 4), 20),
                    Update::insert(Tuple::from_pair(5, 6), 30),
                ],
            )
            .unwrap();

        // Read only updates since time 15
        let read = persist.read("db:since_test", 15).unwrap();
        assert!(read.iter().all(|u| u.time >= 15));
        assert_eq!(read.len(), 2); // Only time 20 and 30
    }

    #[test]
    fn test_multiple_shards_independent() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:a").unwrap();
        persist.ensure_shard("db:b").unwrap();

        persist
            .append("db:a", &[Update::insert(Tuple::from_pair(1, 2), 10)])
            .unwrap();
        persist
            .append("db:b", &[Update::insert(Tuple::from_pair(3, 4), 20)])
            .unwrap();

        let read_a = persist.read("db:a", 0).unwrap();
        let read_b = persist.read("db:b", 0).unwrap();

        assert_eq!(read_a.len(), 1);
        assert_eq!(read_b.len(), 1);
        assert_ne!(read_a[0].data, read_b[0].data);
    }

    #[test]
    fn test_flush_nonexistent_shard() {
        let (_temp, persist) = create_test_persist();
        let result = persist.flush("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_shard_removes_all_data() {
        let (_temp, persist) = create_test_persist();

        // Create shard and add data
        persist.ensure_shard("db:edge").unwrap();
        persist
            .append(
                "db:edge",
                &[
                    Update::insert(Tuple::from_pair(1, 2), 10),
                    Update::insert(Tuple::from_pair(3, 4), 20),
                ],
            )
            .unwrap();
        persist.flush("db:edge").unwrap();

        // Verify shard exists
        let shards = persist.list_shards().unwrap();
        assert!(shards.contains(&"db:edge".to_string()));

        // Delete the shard
        persist.delete_shard("db:edge").unwrap();

        // Shard should no longer be listed
        let shards = persist.list_shards().unwrap();
        assert!(!shards.contains(&"db:edge".to_string()));
    }

    #[test]
    fn test_delete_shard_nonexistent_is_ok() {
        let (_temp, persist) = create_test_persist();
        // Deleting a non-existent shard should succeed silently
        persist.delete_shard("nonexistent").unwrap();
    }

    #[test]
    fn test_delete_shard_not_resurrected_on_restart() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().to_path_buf();

        // First instance: create shard, flush, then delete
        {
            let config = PersistConfig {
                path: path.clone(),
                buffer_size: 100,

                durability_mode: DurabilityMode::Immediate,
            };
            let persist = FilePersist::new(config).unwrap();

            persist.ensure_shard("db:edge").unwrap();
            persist
                .append("db:edge", &[Update::insert(Tuple::from_pair(1, 2), 10)])
                .unwrap();
            persist.flush("db:edge").unwrap();
            persist.delete_shard("db:edge").unwrap();
        }

        // Second instance: deleted shard should not reappear
        {
            let config = PersistConfig {
                path,
                buffer_size: 100,

                durability_mode: DurabilityMode::Immediate,
            };
            let persist = FilePersist::new(config).unwrap();
            let shards = persist.list_shards().unwrap();
            assert!(
                !shards.contains(&"db:edge".to_string()),
                "Deleted shard should not be resurrected on restart"
            );
        }
    }

    // === Regression tests for production readiness fixes ===

    /// P0-2: Verify compaction writes new batch BEFORE deleting old ones.
    /// Simulates the crash-safe ordering: after compaction, data survives restart.
    #[test]
    fn test_compaction_crash_safe_ordering() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().to_path_buf();

        // First instance: create shard, flush, compact
        {
            let config = PersistConfig {
                path: path.clone(),
                buffer_size: 100,
                durability_mode: DurabilityMode::Immediate,
            };
            let persist = FilePersist::new(config).unwrap();

            persist.ensure_shard("db:edge").unwrap();
            persist
                .append(
                    "db:edge",
                    &[
                        Update::insert(Tuple::from_pair(1, 2), 10),
                        Update::insert(Tuple::from_pair(3, 4), 20),
                        Update::insert(Tuple::from_pair(5, 6), 30),
                    ],
                )
                .unwrap();
            persist.flush("db:edge").unwrap();

            // Compact to time 15 (keeps times >= 15)
            persist.compact("db:edge", 15).unwrap();
        }

        // Second instance: data should survive restart after compaction
        {
            let config = PersistConfig {
                path,
                buffer_size: 100,
                durability_mode: DurabilityMode::Immediate,
            };
            let persist = FilePersist::new(config).unwrap();

            let updates = persist.read("db:edge", 0).unwrap();
            // Only times >= 15 should remain
            assert!(updates.iter().all(|u| u.time >= 15));
            assert_eq!(updates.len(), 2); // time 20 and 30
        }
    }

    /// P0-3: Verify metadata uses atomic write (temp + rename).
    /// After save_shard_meta, no .json.tmp files should remain.
    #[test]
    fn test_metadata_atomic_write_no_temp_files() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:test").unwrap();
        persist
            .append("db:test", &[Update::insert(Tuple::from_pair(1, 2), 10)])
            .unwrap();
        persist.flush("db:test").unwrap();

        // Check no .json.tmp files exist in shards directory
        let shards_dir = _temp.path().join("shards");
        let tmp_files: Vec<_> = fs::read_dir(&shards_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().to_str().is_some_and(|s| s.ends_with(".json.tmp")))
            .collect();
        assert!(
            tmp_files.is_empty(),
            "No .json.tmp files should remain after atomic write"
        );
    }

    /// P0-3: Verify metadata survives restart (atomic write is durable).
    #[test]
    fn test_metadata_durable_across_restart() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().to_path_buf();

        // First instance: create shard and flush
        {
            let config = PersistConfig {
                path: path.clone(),
                buffer_size: 100,
                durability_mode: DurabilityMode::Immediate,
            };
            let persist = FilePersist::new(config).unwrap();
            persist.ensure_shard("db:meta_test").unwrap();
            persist
                .append(
                    "db:meta_test",
                    &[Update::insert(Tuple::from_pair(42, 99), 10)],
                )
                .unwrap();
            persist.flush("db:meta_test").unwrap();
        }

        // Second instance: metadata and data should be intact
        {
            let config = PersistConfig {
                path,
                buffer_size: 100,
                durability_mode: DurabilityMode::Immediate,
            };
            let persist = FilePersist::new(config).unwrap();
            let info = persist.shard_info("db:meta_test").unwrap();
            assert_eq!(info.batch_count, 1);

            let updates = persist.read("db:meta_test", 0).unwrap();
            assert_eq!(updates.len(), 1);
            assert_eq!(updates[0].data, Tuple::from_pair(42, 99));
        }
    }

    /// P1-7: Verify startup cleans up stale .archived WAL files.
    #[test]
    fn test_startup_cleans_archived_wal_files() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().to_path_buf();

        // Create WAL dir with stale archive file
        let wal_dir = path.join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        fs::write(wal_dir.join("wal_12345.archived"), "stale data").unwrap();

        // Also create required directories for FilePersist
        fs::create_dir_all(path.join("shards")).unwrap();
        fs::create_dir_all(path.join("batches")).unwrap();

        // Starting FilePersist should clean up archived files
        let config = PersistConfig {
            path: path.clone(),
            buffer_size: 100,
            durability_mode: DurabilityMode::Immediate,
        };
        let _persist = FilePersist::new(config).unwrap();

        assert!(
            !wal_dir.join("wal_12345.archived").exists(),
            "Archived WAL files should be cleaned up on startup"
        );
    }

    // === Regression tests for P0 durability: directory fsync after deletions ===

    /// Regression: After delete_shard, metadata file must be removed from disk.
    /// Verifies the shard .json file is durably deleted (not resurrectable on crash).
    #[test]
    fn test_delete_shard_metadata_file_removed() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:edge").unwrap();
        persist
            .append("db:edge", &[Update::insert(Tuple::from_pair(1, 2), 10)])
            .unwrap();
        persist.flush("db:edge").unwrap();

        // Verify shard metadata file exists
        let meta_path = _temp.path().join("shards").join("db_edge.json");
        assert!(
            meta_path.exists(),
            "Shard metadata file must exist before deletion"
        );

        persist.delete_shard("db:edge").unwrap();

        // Metadata file must be removed
        assert!(
            !meta_path.exists(),
            "Shard metadata file must be deleted after delete_shard"
        );
    }

    /// Regression: After delete_shard, batch files must be removed from disk.
    #[test]
    fn test_delete_shard_batch_files_removed() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:edge").unwrap();
        for i in 0..3 {
            persist
                .append(
                    "db:edge",
                    &[Update::insert(Tuple::from_pair(i, i + 1), i as u64)],
                )
                .unwrap();
        }
        persist.flush("db:edge").unwrap();

        // Verify batch files exist
        let batches_dir = _temp.path().join("batches");
        let batch_count_before = fs::read_dir(&batches_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .count();
        assert!(
            batch_count_before > 0,
            "Batch files must exist before deletion"
        );

        persist.delete_shard("db:edge").unwrap();

        // All batch files for this shard must be gone
        let batch_count_after = fs::read_dir(&batches_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .count();
        assert_eq!(
            batch_count_after, 0,
            "All batch files must be deleted after delete_shard"
        );
    }

    /// Regression: Compaction must delete old batch files and leave only new compacted one.
    #[test]
    fn test_compaction_deletes_old_batch_files() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:edge").unwrap();

        // Create multiple flushes to generate multiple batch files
        persist
            .append("db:edge", &[Update::insert(Tuple::from_pair(1, 2), 10)])
            .unwrap();
        persist.flush("db:edge").unwrap();

        persist
            .append("db:edge", &[Update::insert(Tuple::from_pair(3, 4), 20)])
            .unwrap();
        persist.flush("db:edge").unwrap();

        let batches_dir = _temp.path().join("batches");
        let batch_count_before = fs::read_dir(&batches_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .count();
        assert!(
            batch_count_before >= 2,
            "Should have at least 2 batch files before compaction"
        );

        // Compact
        persist.compact("db:edge", 0).unwrap();

        let batch_count_after = fs::read_dir(&batches_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .count();
        assert_eq!(
            batch_count_after, 1,
            "Only one compacted batch file should remain after compaction"
        );

        // Data must still be intact
        let updates = persist.read("db:edge", 0).unwrap();
        assert_eq!(updates.len(), 2);
    }

    /// Regression: save_shard_meta uses atomic write-to-temp-then-rename.
    /// No .tmp files should remain after save.
    #[test]
    fn test_shard_meta_atomic_write() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:atomic_test").unwrap();
        persist
            .append(
                "db:atomic_test",
                &[Update::insert(Tuple::from_pair(1, 2), 10)],
            )
            .unwrap();
        persist.flush("db:atomic_test").unwrap();

        // Verify no .tmp files in shards dir
        let shards_dir = _temp.path().join("shards");
        let tmp_files: Vec<_> = fs::read_dir(&shards_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().to_str().is_some_and(|s| s.ends_with(".json.tmp")))
            .collect();
        assert!(
            tmp_files.is_empty(),
            "No .json.tmp files should remain after atomic shard meta write"
        );

        // Verify the final metadata file is valid
        let meta_path = shards_dir.join("db_atomic_test.json");
        assert!(meta_path.exists());
        let content = fs::read_to_string(&meta_path).unwrap();
        let _: ShardMeta = serde_json::from_str(&content).unwrap();
    }
}
