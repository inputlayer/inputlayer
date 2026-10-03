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
//! Writes arrive as [`Transaction`]s: every change of one commit, across any number
//! of shards, at one revision. Each is one WAL record, so it is durable and recovered
//! as a whole or not at all.
//!
//! ## Recovery
//!
//! On startup:
//! 1. Migrate v1 data (see `migrate`) and load shard metadata
//! 2. Read batch files
//! 3. Replay the WAL's intact prefix of committed transactions
//! 4. Consolidate to get current state
//!
//! Rule and schema changes in the WAL are handed to the engine
//! ([`FilePersist::take_recovered_catalog`]), which applies them over the catalog
//! files; see the `catalog_log` module for how long the WAL keeps them.

pub mod batch;
mod catalog_log;
pub mod codec;
pub mod consolidate;
mod migrate;
pub mod transaction;
pub mod wal;
mod wal_record;

pub use batch::{Batch, BatchRef, ShardInfo, ShardMeta, Update};
pub use consolidate::{
    consolidate, consolidate_to_current, filter_since, set_semantics_corrections, to_tuples,
    to_tuples_with_multiplicity,
};
pub use transaction::{CatalogEntry, CatalogRecord, Transaction, TxnOp};
pub use wal::PersistWal;

use crate::storage::{StorageError, StorageResult};
use crate::value::record_batch_to_tuples;
use catalog_log::CatalogLog;
use parking_lot::{Mutex, RwLock};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

// Parquet I/O for batches
use arrow::array::{Array, ArrayRef, Int64Array, LargeBinaryArray, UInt64Array};
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
    /// Maximum WAL file size in bytes before forcing a flush (0 = unlimited)
    pub max_wal_size_bytes: u64,
}

impl Default for PersistConfig {
    fn default() -> Self {
        PersistConfig {
            path: PathBuf::from("./data/persist"),
            buffer_size: 10000,
            durability_mode: DurabilityMode::Immediate,
            max_wal_size_bytes: 67_108_864, // 64 MB
        }
    }
}

/// Trait for persist backends
pub trait PersistBackend: Send + Sync {
    /// Commit a transaction: make it durable per the durability mode, then visible
    /// to reads. All of it is committed, or on error none of it.
    fn commit(&self, txn: Transaction) -> StorageResult<()>;

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
///
/// Lock order everywhere: `wal`, then `shards`, then `catalog`.
pub struct FilePersist {
    config: PersistConfig,
    shards: RwLock<HashMap<String, ShardState>>,
    wal: Mutex<PersistWal>,
    /// Which catalog changes in the WAL are still needed.
    catalog: Mutex<CatalogLog>,
    /// Catalog changes replayed from the WAL at startup, until the engine takes them.
    recovered_catalog: Mutex<Vec<CatalogRecord>>,
    next_batch_id: AtomicU64,
}

impl FilePersist {
    /// Create a new `FilePersist` instance
    pub fn new(config: PersistConfig) -> StorageResult<Self> {
        // Create directory structure
        fs::create_dir_all(&config.path)?;
        fs::create_dir_all(config.path.join("shards"))?;
        fs::create_dir_all(config.path.join("batches"))?;

        let (wal, recovered) = PersistWal::open(config.path.join("wal"))?;
        migrate::migrate_v1(&config.path)?;

        let mut persist = FilePersist {
            config,
            shards: RwLock::new(HashMap::new()),
            wal: Mutex::new(wal),
            catalog: Mutex::new(CatalogLog::default()),
            recovered_catalog: Mutex::new(Vec::new()),
            next_batch_id: AtomicU64::new(1),
        };

        // Load existing shards, clean up orphans, and replay WAL
        persist.load_shards()?;
        persist.cleanup_orphaned_batches();
        let replayed = persist.replay(recovered);

        // Crash-safe WAL drain: if we replayed any entries, flush them to batch
        // files and clear the WAL immediately. This makes replay idempotent -
        // on a second crash, there are no stale WAL entries to double-apply.
        if replayed > 0 {
            let shard_names: Vec<String> = {
                let shards = persist.shards.read();
                shards
                    .iter()
                    .filter(|(_, state)| !state.buffer.is_empty())
                    .map(|(name, _)| name.clone())
                    .collect()
            };
            for shard_name in &shard_names {
                persist.flush(shard_name)?;
            }
        }

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

        let batches_dir = self.config.path.join("batches");
        let mut shards = self.shards.write();

        for entry in fs::read_dir(&shards_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let mut meta = read_shard_meta(&path)?;

                if meta.version != batch::SHARD_META_VERSION {
                    return Err(StorageError::Other(format!(
                        "Shard '{}' has format version {} but this server reads version {}. \
                         Please upgrade the server or downgrade the data.",
                        meta.name,
                        meta.version,
                        batch::SHARD_META_VERSION
                    )));
                }

                // A meta under a foreign filename means two shards may share a file.
                // Refuse to start rather than let orphan cleanup delete their batches.
                let expected = shard_meta_path(&shards_dir, &meta.name);
                if path != expected || shards.contains_key(&meta.name) {
                    return Err(StorageError::Other(format!(
                        "Shard metadata '{}' holds shard '{}' whose metadata belongs at '{}'; \
                         refusing to load to avoid deleting batch files",
                        path.display(),
                        meta.name,
                        expected.display()
                    )));
                }
                rebase_batch_paths(&mut meta, &batches_dir);

                // Update next_batch_id if needed
                for batch in &meta.batches {
                    if let Ok(id) = batch.id.parse::<u64>() {
                        let current = self.next_batch_id.load(Ordering::Relaxed);
                        if id >= current {
                            self.next_batch_id.store(id + 1, Ordering::Relaxed);
                        }
                    }
                }

                // Validate batch files exist and are readable (#6)
                let mut valid_batches = Vec::new();
                let mut removed_count = 0usize;
                for batch_ref in &meta.batches {
                    if batch_ref.path.exists() {
                        valid_batches.push(batch_ref.clone());
                    } else {
                        tracing::warn!(
                            shard = %meta.name,
                            batch_id = %batch_ref.id,
                            path = %batch_ref.path.display(),
                            "Batch file missing - removing stale reference"
                        );
                        removed_count += 1;
                    }
                }
                if removed_count > 0 {
                    meta.batches = valid_batches;
                    meta.total_updates = meta.batches.iter().map(|b| b.len).sum();
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

    /// Remove batch files in the batches/ directory that aren't referenced by any shard.
    /// These can accumulate from crashes during flush (batch written, metadata not yet saved)
    /// or during compaction/delete (metadata updated, old files not yet removed).
    fn cleanup_orphaned_batches(&self) {
        let batches_dir = self.config.path.join("batches");
        if !batches_dir.exists() {
            return;
        }

        // Collect all referenced batch file paths
        let referenced: std::collections::HashSet<PathBuf> = {
            let shards = self.shards.read();
            shards
                .values()
                .flat_map(|state| state.meta.batches.iter().map(|b| b.path.clone()))
                .collect()
        };

        // Scan batches directory and remove orphans
        let mut removed = 0usize;
        if let Ok(entries) = fs::read_dir(&batches_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("parquet")
                    && !referenced.contains(&path)
                {
                    let _ = fs::remove_file(&path);
                    removed += 1;
                }
                // Also clean up stale temp files from interrupted atomic writes
                if path
                    .extension()
                    .and_then(|s| s.to_str())
                    .is_some_and(|ext| ext == "tmp")
                {
                    let _ = fs::remove_file(&path);
                    removed += 1;
                }
            }
        }

        if removed > 0 {
            eprintln!("[persist] Cleaned up {removed} orphaned batch file(s)");
            sync_directory(&batches_dir);
        }
    }

    /// Replay recovered transactions: fact changes into shard buffers, catalog
    /// changes kept for [`Self::take_recovered_catalog`]. Returns how many
    /// transactions there were.
    fn replay(&self, txns: Vec<Transaction>) -> usize {
        let count = txns.len();
        let mut shards = self.shards.write();
        let mut log = self.catalog.lock();
        let mut recovered = self.recovered_catalog.lock();
        for txn in txns {
            log.logged(&txn);
            let (updates, catalog) = txn.split();
            for (shard, updates) in updates {
                buffer_updates(&mut shards, shard, updates);
            }
            recovered.extend(catalog);
        }
        count
    }

    /// The rule and schema changes the WAL held at startup, in commit order.
    /// The WAL keeps them until [`Self::catalog_saved`] reports them saved.
    /// Returns them once; later calls return nothing.
    pub fn take_recovered_catalog(&self) -> Vec<CatalogRecord> {
        std::mem::take(&mut *self.recovered_catalog.lock())
    }

    /// `kg`'s catalog files now reflect every change up to `revision`, so the
    /// WAL no longer needs them. They are dropped at the next WAL rewrite, or
    /// now when the WAL holds no fact changes (it is small then).
    pub fn catalog_saved(&self, kg: &str, revision: u64) -> StorageResult<()> {
        let mut wal = self.wal.lock();
        let shards = self.shards.read();
        let mut log = self.catalog.lock();
        log.saved(kg, revision);
        if log.has_redundant() && shards.values().all(|state| state.buffer.is_empty()) {
            wal.retain_ops(|revision, op| log.keeps(revision, op))?;
            log.pruned();
        }
        Ok(())
    }

    /// Remove every catalog change of `kg` from the WAL, for a dropped KG: a
    /// later KG of the same name must not replay them.
    pub fn forget_catalog(&self, kg: &str) -> StorageResult<()> {
        let mut wal = self.wal.lock();
        wal.retain_ops(|_, op| !matches!(op, TxnOp::Catalog { kg: k, .. } if k == kg))?;
        self.catalog.lock().forget(kg);
        Ok(())
    }

    /// Save shard metadata to disk; see [`write_shard_meta`].
    fn save_shard_meta(&self, meta: &ShardMeta) -> StorageResult<()> {
        write_shard_meta(&self.config.path.join("shards"), meta)
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

    /// Flush a shard's buffer to a batch. Returns `false` if the shard does not exist.
    fn flush_existing(&self, shard: &str) -> StorageResult<bool> {
        let mut wal = self.wal.lock();
        let mut shards = self.shards.write();
        let Some(state) = shards.get_mut(shard) else {
            return Ok(false);
        };

        if state.buffer.is_empty() {
            return Ok(true);
        }

        // Step 1: Write buffer to batch file (atomic via temp+rename in write_batch)
        let batch = Batch::new(state.buffer.clone());
        let (batch_id, path) = self.write_batch(&state.buffer)?;

        let batch_ref = BatchRef {
            id: batch_id,
            path: path.clone(),
            lower: batch.lower,
            upper: batch.upper,
            len: batch.len(),
        };

        // Step 2: Update metadata and save atomically
        state.meta.add_batch(batch_ref);
        state.buffer.clear();

        if let Err(e) = self.save_shard_meta(&state.meta) {
            // Metadata save failed - clean up the orphaned batch file
            let _ = fs::remove_file(&path);
            return Err(e);
        }

        // Step 3: Remove WAL entries LAST (safe - metadata already points to batch),
        // with any catalog changes the catalog files already reflect.
        let mut log = self.catalog.lock();
        wal.retain_ops(|revision, op| match op {
            TxnOp::Facts { shard: s, .. } => s != shard,
            TxnOp::Catalog { .. } => log.keeps(revision, op),
        })?;
        log.pruned();

        Ok(true)
    }

    /// Flush all dirty shards (shards with non-empty buffers).
    /// Used when WAL size exceeds the configured limit.
    fn flush_all(&self) -> StorageResult<()> {
        let dirty_shards: Vec<String> = {
            let shards = self.shards.read();
            shards
                .iter()
                .filter(|(_, state)| !state.buffer.is_empty())
                .map(|(name, _)| name.clone())
                .collect()
        };

        for shard_name in &dirty_shards {
            self.flush_existing(shard_name)?;
        }

        Ok(())
    }

    /// Make a later WAL write fail at `fault`.
    #[cfg(test)]
    pub(crate) fn inject_wal_fault(&self, fault: wal::WalFault) {
        self.wal.lock().inject_fault(fault);
    }
}

impl PersistBackend for FilePersist {
    fn commit(&self, txn: Transaction) -> StorageResult<()> {
        if txn.is_empty() {
            return Ok(());
        }
        for shard in txn.shards() {
            self.ensure_shard(shard)?;
        }

        // WAL write and buffer push share one critical section, so a concurrent flush
        // never drops a WAL record whose updates are not yet in its batch.
        // Lock order everywhere: WAL, then shards.
        let full: Vec<String> = {
            let mut wal = self.wal.lock();
            match self.config.durability_mode {
                DurabilityMode::Immediate => wal.append(&txn, true)?,
                DurabilityMode::Batched => wal.append(&txn, false)?,
                // No WAL: data is lost on crash. Only for ephemeral/reproducible data.
                DurabilityMode::Async => {}
            }

            let mut shards = self.shards.write();
            // Only rule and schema changes are tracked: plain fact commits
            // never take the catalog lock.
            if self.config.durability_mode != DurabilityMode::Async
                && txn.catalog_kgs().next().is_some()
            {
                self.catalog.lock().logged(&txn);
            }
            let (updates, _) = txn.split();
            updates
                .into_iter()
                .filter_map(|(shard, updates)| {
                    let len = buffer_updates(&mut shards, shard.clone(), updates);
                    (len >= self.config.buffer_size).then_some(shard)
                })
                .collect()
        };

        // Flush full buffers; a concurrent delete_shard may have removed the shard.
        if !full.is_empty() {
            for shard in &full {
                self.flush_existing(shard)?;
            }
        } else if self.config.max_wal_size_bytes > 0 {
            // Check WAL size - force flush all dirty shards if WAL is too large
            let wal_size = self.wal.lock().file_size();
            if wal_size > self.config.max_wal_size_bytes {
                tracing::info!(
                    wal_size_bytes = wal_size,
                    max = self.config.max_wal_size_bytes,
                    "wal_size_limit_flush"
                );
                self.flush_all()?;
            }
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
        let old_batches: Vec<BatchRef> = std::mem::take(&mut state.meta.batches);

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

        // Step 3: Delete old batch files LAST (safe - metadata no longer references them)
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
        if self.flush_existing(shard)? {
            Ok(())
        } else {
            Err(StorageError::Other(format!("Shard not found: {shard}")))
        }
    }

    fn delete_shard(&self, shard: &str) -> StorageResult<()> {
        // Lock order everywhere: WAL, then shards. Holding both until the
        // metadata is gone keeps a concurrent append from acking an entry
        // this delete then drops.
        let mut wal = self.wal.lock();
        let mut shards = self.shards.write();

        // Step 1: Drop this shard's WAL entries. A failure here changes nothing.
        wal.remove_shard_entries(shard)?;
        let removed_state = shards.remove(shard);

        // Step 2: Delete metadata so restart no longer sees the shard.
        let meta_path = shard_meta_path(&self.config.path.join("shards"), shard);
        match fs::remove_file(&meta_path) {
            Ok(()) => sync_directory(&self.config.path.join("shards")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        drop(shards);
        drop(wal);

        // Step 3: Delete batch files. Leftovers are unreferenced orphans,
        // removed on the next startup.
        if let Some(ref state) = removed_state {
            let mut deleted_any = false;
            for batch_ref in &state.meta.batches {
                if batch_ref.path.exists() {
                    let _ = fs::remove_file(&batch_ref.path);
                    deleted_any = true;
                }
            }
            if deleted_any {
                sync_directory(&self.config.path.join("batches"));
            }
        }

        Ok(())
    }
}

/// Append committed updates to a shard's buffer, creating the shard if needed, and
/// advance its upper frontier. Returns the buffer's new length.
fn buffer_updates(
    shards: &mut HashMap<String, ShardState>,
    shard: String,
    updates: Vec<Update>,
) -> usize {
    let state = shards.entry(shard).or_insert_with_key(|name| ShardState {
        meta: ShardMeta::new(name.clone()),
        buffer: Vec::new(),
    });
    if let Some(max) = updates.iter().map(|u| u.time).max() {
        state.meta.upper = state.meta.upper.max(max + 1);
    }
    state.buffer.extend(updates);
    state.buffer.len()
}

// Shard metadata files

/// Longest filename stem kept verbatim; longer names get a hashed stem.
const MAX_META_STEM: usize = 200;

/// Injective filename stem for a shard name.
///
/// Bytes outside `[a-z0-9_.-]` are percent-encoded, so ':' and '_' never alias and names
/// differing only in case stay distinct on case-insensitive filesystems. Stems over
/// [`MAX_META_STEM`] are truncated and suffixed with `~` plus a SHA-256 of the full name;
/// `~` is always escaped otherwise, so the two forms cannot collide.
fn shard_file_stem(name: &str) -> String {
    let mut stem = String::with_capacity(name.len());
    for b in name.bytes() {
        if b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'.' | b'-') {
            stem.push(b as char);
        } else {
            stem.push_str(&format!("%{b:02X}"));
        }
    }
    if stem.len() > MAX_META_STEM {
        let digest = Sha256::digest(name.as_bytes());
        stem.truncate(MAX_META_STEM - 65);
        stem.push('~');
        for b in digest {
            stem.push_str(&format!("{b:02x}"));
        }
    }
    stem
}

fn shard_meta_path(shards_dir: &Path, name: &str) -> PathBuf {
    shards_dir.join(format!("{}.json", shard_file_stem(name)))
}

fn read_shard_meta(path: &Path) -> StorageResult<ShardMeta> {
    let content = fs::read_to_string(path)?;
    serde_json::from_str(&content).map_err(|e| {
        StorageError::Other(format!(
            "Failed to parse shard metadata '{}': {e}",
            path.display()
        ))
    })
}

/// Point batch refs at `batches_dir`, so a data dir stays valid after it moves.
fn rebase_batch_paths(meta: &mut ShardMeta, batches_dir: &Path) {
    for batch_ref in &mut meta.batches {
        if let Some(file) = batch_ref.path.file_name() {
            batch_ref.path = batches_dir.join(file);
        }
    }
}

/// Save shard metadata atomically: write `{stem}.json.tmp`, fsync, rename to `{stem}.json`.
/// The file is always either the old or the new version, never half-written.
fn write_shard_meta(shards_dir: &Path, meta: &ShardMeta) -> StorageResult<()> {
    let final_path = shard_meta_path(shards_dir, &meta.name);
    let tmp_path = final_path.with_extension("json.tmp");
    let content = serde_json::to_string_pretty(meta)
        .map_err(|e| StorageError::Other(format!("Failed to serialize shard metadata: {e}")))?;

    if let Err(e) = fs::write(&tmp_path, &content) {
        eprintln!(
            "[persist] ERROR save_shard_meta: path={}, parent_exists={}, error={}",
            tmp_path.display(),
            tmp_path.parent().is_some_and(Path::exists),
            e
        );
        return Err(e.into());
    }

    if let Err(e) = fs::File::open(&tmp_path).and_then(|f| f.sync_all()) {
        let _ = fs::remove_file(&tmp_path);
        return Err(e.into());
    }

    if let Err(e) = fs::rename(&tmp_path, &final_path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(e.into());
    }
    sync_directory(shards_dir);

    Ok(())
}

// Parquet I/O for Update batches

/// Parquet key-value metadata key holding the batch format version.
const FORMAT_KEY: &str = "inputlayer.persist.format";
/// Batch format written by this server.
const BATCH_FORMAT: &str = "2";
/// v2 column holding each tuple encoded by [`codec::encode_tuple`].
const TUPLE_COLUMN: &str = "tuple";

/// Write updates to a Parquet file.
///
/// Columns: `tuple` (`LargeBinary`, losslessly encoded), `time` (`UInt64`), `diff` (`Int64`).
fn write_updates_parquet(path: &Path, updates: &[Update]) -> StorageResult<()> {
    if updates.is_empty() {
        // No data to write - skip creating the file entirely.
        // The caller handles absence of batch files gracefully.
        return Ok(());
    }

    let mut buf = Vec::new();
    let mut offsets = Vec::with_capacity(updates.len() + 1);
    offsets.push(0i64);
    for u in updates {
        codec::encode_tuple(&u.data, &mut buf);
        offsets.push(buf.len() as i64);
    }
    let tuples = LargeBinaryArray::new(
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        buf.into(),
        None,
    );

    let full_schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new(TUPLE_COLUMN, ArrowDataType::LargeBinary, false),
            Field::new("time", ArrowDataType::UInt64, false),
            Field::new("diff", ArrowDataType::Int64, false),
        ],
        HashMap::from([(FORMAT_KEY.to_string(), BATCH_FORMAT.to_string())]),
    ));
    let times: Vec<u64> = updates.iter().map(|u| u.time).collect();
    let diffs: Vec<i64> = updates.iter().map(|u| u.diff).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(tuples),
        Arc::new(UInt64Array::from(times)),
        Arc::new(Int64Array::from(diffs)),
    ];

    let batch = RecordBatch::try_new(full_schema.clone(), columns).map_err(StorageError::Arrow)?;

    // Write to temp file then rename atomically (crash-safe).
    // If we crash mid-write, the temp file is orphaned but the original path
    // is never left in a corrupt half-written state.
    let tmp_path = path.with_extension("parquet.tmp");

    let file = match fs::File::create(&tmp_path) {
        Ok(f) => f,
        Err(e) => {
            // ENOSPC or permission error - no temp file to clean up
            return Err(StorageError::Other(format!(
                "Failed to create batch file '{}': {e}",
                tmp_path.display()
            )));
        }
    };
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    // Helper: clean up temp file on any write error (ENOSPC, etc.)
    let write_result = (|| -> StorageResult<()> {
        let mut writer =
            ArrowWriter::try_new(file, full_schema, Some(props)).map_err(StorageError::Parquet)?;
        writer.write(&batch).map_err(StorageError::Parquet)?;
        writer.close().map_err(StorageError::Parquet)?;
        fs::File::open(&tmp_path)?.sync_all()?;
        Ok(())
    })();

    if let Err(e) = write_result {
        // Clean up partial temp file so it doesn't consume disk space
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    // Atomic rename (POSIX guarantees atomicity)
    fs::rename(&tmp_path, path)?;
    if let Some(dir) = path.parent() {
        sync_directory(dir);
    }

    Ok(())
}

/// Read updates from a Parquet file in either the v2 or the legacy typed-column format.
fn read_updates_parquet(path: &Path) -> StorageResult<Vec<Update>> {
    let file = fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(StorageError::Parquet)?;
    let v2 = match builder
        .schema()
        .metadata()
        .get(FORMAT_KEY)
        .map(String::as_str)
    {
        None => false,
        Some(BATCH_FORMAT) => true,
        Some(other) => {
            return Err(StorageError::Other(format!(
                "Unsupported batch format '{other}' in '{}'",
                path.display()
            )))
        }
    };

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

        if v2 {
            if time_col_idx != 1 || batch.schema().field(0).name() != TUPLE_COLUMN {
                return Err(StorageError::Other(format!(
                    "Invalid v2 batch layout in '{}'",
                    path.display()
                )));
            }
            let encoded = batch
                .column(0)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| StorageError::Other("Invalid tuple column type".to_string()))?;
            for i in 0..batch.num_rows() {
                if encoded.is_null(i) {
                    return Err(StorageError::Other(format!(
                        "Null tuple in batch '{}' row {i}",
                        path.display()
                    )));
                }
                let data = codec::decode_tuple(encoded.value(i)).map_err(|e| {
                    StorageError::Other(format!(
                        "Corrupt tuple in batch '{}' row {i}: {e}",
                        path.display()
                    ))
                })?;
                updates.push(Update {
                    data,
                    time: times.value(i),
                    diff: diffs.value(i),
                });
            }
            continue;
        }

        // Legacy (v1) batch: one typed column per tuple position
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

#[cfg(test)]
mod commit_tests;

// Tests
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{Tuple, Value};
    use tempfile::TempDir;

    /// Commit `updates` to `shard`, one transaction per run of equal times.
    fn commit(persist: &FilePersist, shard: &str, updates: &[Update]) -> StorageResult<()> {
        for run in updates.chunk_by(|a, b| a.time == b.time) {
            let mut txn = Transaction::new(run[0].time);
            txn.facts(
                shard,
                run.iter().map(|u| (u.data.clone(), u.diff)).collect(),
            );
            persist.commit(txn)?;
        }
        Ok(())
    }

    fn create_test_persist() -> (TempDir, FilePersist) {
        let temp = TempDir::new().unwrap();
        let config = PersistConfig {
            path: temp.path().to_path_buf(),
            buffer_size: 5,

            durability_mode: DurabilityMode::Immediate,
            ..Default::default()
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
        commit(&persist, "db:edge", &updates).unwrap();

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
        commit(&persist, "db:edge", &updates).unwrap();
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
            commit(
                &persist,
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
        commit(
            &persist,
            "db:edge",
            &[Update::insert(Tuple::from_pair(1, 2), 10)],
        )
        .unwrap();
        commit(
            &persist,
            "db:edge",
            &[Update::delete(Tuple::from_pair(1, 2), 10)],
        )
        .unwrap();
        commit(
            &persist,
            "db:edge",
            &[Update::insert(Tuple::from_pair(3, 4), 20)],
        )
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
        commit(
            &persist,
            "db:edge",
            &[Update::insert(Tuple::from_pair(1, 2), 10)],
        )
        .unwrap();
        commit(
            &persist,
            "db:edge",
            &[Update::insert(Tuple::from_pair(3, 4), 20)],
        )
        .unwrap();
        commit(
            &persist,
            "db:edge",
            &[Update::insert(Tuple::from_pair(5, 6), 30)],
        )
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
                ..Default::default()
            };
            let persist = FilePersist::new(config).unwrap();

            persist.ensure_shard("db:edge").unwrap();
            commit(
                &persist,
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
                ..Default::default()
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
        commit(&persist, "db:triple", &updates).unwrap();
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
        commit(&persist, "db:mixed", &updates).unwrap();
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
        commit(&persist, "db:test", &updates).unwrap();
        persist.flush("db:test").unwrap();

        let read = persist.read("db:test", 0).unwrap();
        assert_eq!(read.len(), 2);

        // Verify we can read back the tuples
        let tuples = to_tuples(&read);
        assert_eq!(tuples.len(), 2);
        assert!(tuples.iter().any(|t| t.to_pair() == Some((1, 2))));
        assert!(tuples.iter().any(|t| t.to_pair() == Some((3, 4))));
    }

    fn write_batch_with_format(path: &Path, format: Option<&str>) {
        let metadata = format
            .map(|f| HashMap::from([(FORMAT_KEY.to_string(), f.to_string())]))
            .unwrap_or_default();
        let mut buf = Vec::new();
        codec::encode_tuple(&Tuple::from_pair(1, 2), &mut buf);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new(TUPLE_COLUMN, ArrowDataType::LargeBinary, false),
                Field::new("time", ArrowDataType::UInt64, false),
                Field::new("diff", ArrowDataType::Int64, false),
            ],
            metadata,
        ));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(LargeBinaryArray::from_vec(vec![buf.as_slice()])),
            Arc::new(UInt64Array::from(vec![1u64])),
            Arc::new(Int64Array::from(vec![1i64])),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut writer =
            ArrowWriter::try_new(fs::File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn test_read_parquet_honours_format_key() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("b.parquet");

        write_batch_with_format(&path, Some(BATCH_FORMAT));
        let read = read_updates_parquet(&path).unwrap();
        assert_eq!(read[0].data, Tuple::from_pair(1, 2));

        write_batch_with_format(&path, Some("3"));
        let err = read_updates_parquet(&path).unwrap_err().to_string();
        assert!(err.contains("Unsupported batch format '3'"), "{err}");

        // Without the key the file is v1, whose columns are typed values, not encoded tuples.
        write_batch_with_format(&path, None);
        assert!(read_updates_parquet(&path).is_err());
    }

    #[test]
    fn test_persist_config_default() {
        let config = PersistConfig::default();
        assert_eq!(config.buffer_size, 10000);
        assert_eq!(config.path, PathBuf::from("./data/persist"));
        assert!(matches!(config.durability_mode, DurabilityMode::Immediate));
    }

    #[test]
    fn test_shard_file_stem() {
        assert_eq!(shard_file_stem("db:edge"), "db%3Aedge");
        assert_eq!(shard_file_stem("db/test/edge"), "db%2Ftest%2Fedge");
        assert_eq!(shard_file_stem("simple_1.x-y"), "simple_1.x-y");
        assert_eq!(shard_file_stem("Db:E"), "%44b%3A%45");
        assert_eq!(shard_file_stem("100%~"), "100%25%7E");
    }

    #[test]
    fn test_shard_file_stem_is_injective() {
        let names = [
            "a:b_c", "a_b:c", "a%3Ab_c", "kg:Edge", "kg:edge", "a:b/c", "a:b%2Fc",
        ];
        let stems: std::collections::HashSet<String> =
            names.iter().map(|n| shard_file_stem(n)).collect();
        assert_eq!(stems.len(), names.len());
    }

    #[test]
    fn test_shard_file_stem_long_names_are_bounded_and_distinct() {
        let a = format!("kg:{}a", "x".repeat(300));
        let b = format!("kg:{}b", "x".repeat(300));
        let (sa, sb) = (shard_file_stem(&a), shard_file_stem(&b));
        assert!(sa.len() <= MAX_META_STEM);
        assert_ne!(sa, sb);
        assert!(sa.contains('~'));
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
        commit(&persist, "db:empty_append", &[]).unwrap();

        let read = persist.read("db:empty_append", 0).unwrap();
        assert!(read.is_empty());
    }

    #[test]
    fn test_sync() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:sync_test").unwrap();
        commit(
            &persist,
            "db:sync_test",
            &[Update::insert(Tuple::from_pair(1, 2), 10)],
        )
        .unwrap();

        // Sync should not error
        persist.sync().unwrap();
    }

    #[test]
    fn test_read_with_since_filter() {
        let (_temp, persist) = create_test_persist();

        persist.ensure_shard("db:since_test").unwrap();
        commit(
            &persist,
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

        commit(
            &persist,
            "db:a",
            &[Update::insert(Tuple::from_pair(1, 2), 10)],
        )
        .unwrap();
        commit(
            &persist,
            "db:b",
            &[Update::insert(Tuple::from_pair(3, 4), 20)],
        )
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
        commit(
            &persist,
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
    fn test_delete_shard_wal_failure_leaves_shard_intact() {
        let temp = TempDir::new().unwrap();
        let config = PersistConfig {
            path: temp.path().to_path_buf(),
            ..PersistConfig::default()
        };
        let persist = FilePersist::new(config.clone()).unwrap();
        persist.ensure_shard("db:a").unwrap();
        persist.ensure_shard("db:b").unwrap();
        commit(
            &persist,
            "db:a",
            &[Update::insert(Tuple::from_pair(1, 2), 1)],
        )
        .unwrap();
        commit(
            &persist,
            "db:b",
            &[Update::insert(Tuple::from_pair(3, 4), 2)],
        )
        .unwrap();
        // The WAL rewrite fails: its temp path is a directory.
        fs::create_dir_all(temp.path().join("wal/current.wal.new")).unwrap();

        assert!(persist.delete_shard("db:a").is_err());
        assert_eq!(persist.read("db:a", 0).unwrap().len(), 1);
        drop(persist);
        fs::remove_dir(temp.path().join("wal/current.wal.new")).unwrap();
        let persist = FilePersist::new(config).unwrap();
        assert_eq!(persist.read("db:a", 0).unwrap().len(), 1);
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
                ..Default::default()
            };
            let persist = FilePersist::new(config).unwrap();

            persist.ensure_shard("db:edge").unwrap();
            commit(
                &persist,
                "db:edge",
                &[Update::insert(Tuple::from_pair(1, 2), 10)],
            )
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
                ..Default::default()
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
                ..Default::default()
            };
            let persist = FilePersist::new(config).unwrap();

            persist.ensure_shard("db:edge").unwrap();
            commit(
                &persist,
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
                ..Default::default()
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
        let (temp, persist) = create_test_persist();

        persist.ensure_shard("db:test").unwrap();
        commit(
            &persist,
            "db:test",
            &[Update::insert(Tuple::from_pair(1, 2), 10)],
        )
        .unwrap();
        persist.flush("db:test").unwrap();

        // Check no .json.tmp files exist in shards directory
        let shards_dir = temp.path().join("shards");
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
                ..Default::default()
            };
            let persist = FilePersist::new(config).unwrap();
            persist.ensure_shard("db:meta_test").unwrap();
            commit(
                &persist,
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
                ..Default::default()
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
            ..Default::default()
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
        let (temp, persist) = create_test_persist();

        persist.ensure_shard("db:edge").unwrap();
        commit(
            &persist,
            "db:edge",
            &[Update::insert(Tuple::from_pair(1, 2), 10)],
        )
        .unwrap();
        persist.flush("db:edge").unwrap();

        // Verify shard metadata file exists
        let meta_path = shard_meta_path(&temp.path().join("shards"), "db:edge");
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
        let (temp, persist) = create_test_persist();

        persist.ensure_shard("db:edge").unwrap();
        for i in 0..3 {
            commit(
                &persist,
                "db:edge",
                &[Update::insert(Tuple::from_pair(i, i + 1), i as u64)],
            )
            .unwrap();
        }
        persist.flush("db:edge").unwrap();

        // Verify batch files exist
        let batches_dir = temp.path().join("batches");
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
        let (temp, persist) = create_test_persist();

        persist.ensure_shard("db:edge").unwrap();

        // Create multiple flushes to generate multiple batch files
        commit(
            &persist,
            "db:edge",
            &[Update::insert(Tuple::from_pair(1, 2), 10)],
        )
        .unwrap();
        persist.flush("db:edge").unwrap();

        commit(
            &persist,
            "db:edge",
            &[Update::insert(Tuple::from_pair(3, 4), 20)],
        )
        .unwrap();
        persist.flush("db:edge").unwrap();

        let batches_dir = temp.path().join("batches");
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
        let (temp, persist) = create_test_persist();

        persist.ensure_shard("db:atomic_test").unwrap();
        commit(
            &persist,
            "db:atomic_test",
            &[Update::insert(Tuple::from_pair(1, 2), 10)],
        )
        .unwrap();
        persist.flush("db:atomic_test").unwrap();

        // Verify no .tmp files in shards dir
        let shards_dir = temp.path().join("shards");
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
        let meta_path = shard_meta_path(&shards_dir, "db:atomic_test");
        assert!(meta_path.exists());
        let content = fs::read_to_string(&meta_path).unwrap();
        let _: ShardMeta = serde_json::from_str(&content).unwrap();
    }

    /// Regression: When WAL exceeds max_wal_size_bytes, all dirty shards are flushed.
    #[test]
    fn test_wal_size_limit_triggers_flush() {
        let temp = TempDir::new().unwrap();
        let config = PersistConfig {
            path: temp.path().to_path_buf(),
            buffer_size: 1000, // High buffer so normal buffer-based flush won't trigger
            durability_mode: DurabilityMode::Immediate,
            max_wal_size_bytes: 100, // Very low limit to trigger flush quickly
        };
        let persist = FilePersist::new(config).unwrap();

        persist.ensure_shard("db:wal_test").unwrap();

        // Insert enough data to exceed 100-byte WAL limit
        for i in 0..20i32 {
            commit(
                &persist,
                "db:wal_test",
                &[Update::insert(Tuple::from_pair(i, i * 10), i as u64)],
            )
            .unwrap();
        }

        // After the WAL size limit is hit, data should have been flushed to batch files.
        // The WAL should be much smaller now (entries cleared for flushed shards).
        let _wal_size = persist.wal.lock().file_size();
        // After flush, WAL entries for this shard are removed
        // (exact size depends on implementation, but should be much less than
        // what 20 entries would produce without any flushing)
        let batches_dir = temp.path().join("batches");
        if batches_dir.exists() {
            let batch_files: Vec<_> = std::fs::read_dir(&batches_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
                .collect();
            assert!(
                !batch_files.is_empty(),
                "WAL size limit should have triggered flush, creating batch files"
            );
        }

        // Data should still be readable
        let read = persist.read("db:wal_test", 0).unwrap();
        assert_eq!(read.len(), 20);
    }

    /// Regression: max_wal_size_bytes=0 means unlimited (no auto-flush from WAL size).
    #[test]
    fn test_wal_size_limit_zero_means_unlimited() {
        let temp = TempDir::new().unwrap();
        let config = PersistConfig {
            path: temp.path().to_path_buf(),
            buffer_size: 1000,
            durability_mode: DurabilityMode::Immediate,
            max_wal_size_bytes: 0, // Unlimited
        };
        let persist = FilePersist::new(config).unwrap();

        persist.ensure_shard("db:unlimited").unwrap();

        for i in 0..20i32 {
            commit(
                &persist,
                "db:unlimited",
                &[Update::insert(Tuple::from_pair(i, i), i as u64)],
            )
            .unwrap();
        }

        // With unlimited WAL and high buffer_size, no batch files should be created
        let batches_dir = temp.path().join("batches");
        let batch_count = if batches_dir.exists() {
            std::fs::read_dir(&batches_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
                .count()
        } else {
            0
        };
        assert_eq!(
            batch_count, 0,
            "With max_wal_size_bytes=0, no WAL-triggered flush should occur"
        );

        // Data should still be readable from buffer + WAL
        let read = persist.read("db:unlimited", 0).unwrap();
        assert_eq!(read.len(), 20);
    }
}
