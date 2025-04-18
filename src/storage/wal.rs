//! Write-Ahead Log (WAL) for `InputLayer`
//!
//! Provides O(1) append-only persistence for database writes, with periodic
//! compaction to Parquet for query efficiency.
//!
//! ## Architecture
//!
//! ```text
//! Insert/Delete -> WAL (append, O(1)) -> Periodic compaction -> Parquet
//!                      |
//!                      v
//!                 Recovery on startup (replay WAL)
//! ```
//!
//! ## WAL Entry Format
//!
//! Each entry is a JSON line (for simplicity and debuggability):
//! ```json
//! {"op":"insert","relation":"edge","tuples":[[1,2],[3,4]],"ts":1234567890}
//! {"op":"delete","relation":"edge","tuples":[[1,2]],"ts":1234567891}
//! ```

use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

use super::{StorageError, StorageResult};

/// WAL operation type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum WalOp {
    Insert,
    Delete,
}

/// A single WAL entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub op: WalOp,
    pub relation: String,
    pub tuples: Vec<(i32, i32)>,
    pub ts: u64,
}

/// Write-Ahead Log for a single database
pub struct Wal {
    /// Path to the WAL directory
    wal_dir: PathBuf,
    /// Current WAL file writer
    writer: Option<BufWriter<File>>,
    /// Current WAL file path
    current_file: PathBuf,
    /// Number of entries written since last compaction
    entries_since_compaction: usize,
    /// Threshold for automatic compaction (0 = disabled)
    compaction_threshold: usize,
}

impl Wal {
    /// Create a new WAL for a database
    pub fn new(wal_dir: PathBuf) -> StorageResult<Self> {
        fs::create_dir_all(&wal_dir)?;

        let current_file = wal_dir.join("current.wal");

        Ok(Wal {
            wal_dir,
            writer: None,
            current_file,
            entries_since_compaction: 0,
            compaction_threshold: 1000, // Compact after 1000 entries
        })
    }

    /// Open the WAL file for writing (lazy initialization)
    fn ensure_writer(&mut self) -> StorageResult<&mut BufWriter<File>> {
        if self.writer.is_none() {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.current_file)?;
            self.writer = Some(BufWriter::new(file));
        }
        self.writer
            .as_mut()
            .ok_or_else(|| StorageError::Other("WAL writer not initialized".into()))
    }

    /// Append an entry to the WAL
    pub fn append(&mut self, entry: WalEntry) -> StorageResult<()> {
        let writer = self.ensure_writer()?;

        // Serialize to JSON line
        let json = serde_json::to_string(&entry)
            .map_err(|e| StorageError::Other(format!("WAL serialization failed: {e}")))?;

        writeln!(writer, "{json}")?;
        writer.flush()?; // Flush buffers to OS
                         // Ensure data is durably written to disk
        writer.get_ref().sync_all()?;

        self.entries_since_compaction += 1;

        Ok(())
    }

    /// Log an insert operation
    pub fn log_insert(&mut self, relation: &str, tuples: Vec<(i32, i32)>) -> StorageResult<()> {
        let entry = WalEntry {
            op: WalOp::Insert,
            relation: relation.to_string(),
            tuples,
            ts: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        self.append(entry)
    }

    /// Log a delete operation
    pub fn log_delete(&mut self, relation: &str, tuples: Vec<(i32, i32)>) -> StorageResult<()> {
        let entry = WalEntry {
            op: WalOp::Delete,
            relation: relation.to_string(),
            tuples,
            ts: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        self.append(entry)
    }

    /// Read all entries from the WAL for recovery
    pub fn read_all(&self) -> StorageResult<Vec<WalEntry>> {
        if !self.current_file.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.current_file)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let entry: WalEntry = serde_json::from_str(&line)
                .map_err(|e| StorageError::Other(format!("WAL parse error: {e}")))?;
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Clear the WAL after successful compaction to Parquet
    pub fn clear(&mut self) -> StorageResult<()> {
        // Close current writer
        self.writer = None;

        // Archive the old WAL (optional - for debugging)
        if self.current_file.exists() {
            let archive_name = format!(
                "wal_{}.archived",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            );
            let archive_path = self.wal_dir.join(archive_name);
            fs::rename(&self.current_file, archive_path)?;
        }

        self.entries_since_compaction = 0;

        Ok(())
    }

    /// Check if compaction is needed
    pub fn needs_compaction(&self) -> bool {
        self.compaction_threshold > 0 && self.entries_since_compaction >= self.compaction_threshold
    }

    /// Set the compaction threshold (0 = disabled)
    pub fn set_compaction_threshold(&mut self, threshold: usize) {
        self.compaction_threshold = threshold;
    }

    /// Get the number of entries since last compaction
    pub fn entries_since_compaction(&self) -> usize {
        self.entries_since_compaction
    }

    /// Get the WAL file size in bytes
    pub fn file_size(&self) -> u64 {
        fs::metadata(&self.current_file)
            .map(|m| m.len())
            .unwrap_or(0)
    }
}

/// Replay WAL entries into a data structure
