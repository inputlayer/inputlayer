//! Write-Ahead Log (WAL) for InputLayer
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

use crate::value::Tuple2;
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
        Ok(self.writer.as_mut().unwrap())
    }

    /// Append an entry to the WAL
    pub fn append(&mut self, entry: WalEntry) -> StorageResult<()> {
        let writer = self.ensure_writer()?;

        // Serialize to JSON line
        let json = serde_json::to_string(&entry)
            .map_err(|e| StorageError::Other(format!("WAL serialization failed: {}", e)))?;

        writeln!(writer, "{}", json)?;
        writer.flush()?; // Ensure durability

        self.entries_since_compaction += 1;

        Ok(())
    }

    /// Log an insert operation
    pub fn log_insert(&mut self, relation: &str, tuples: Vec<Tuple2>) -> StorageResult<()> {
        let entry = WalEntry {
            op: WalOp::Insert,
            relation: relation.to_string(),
            tuples,
            ts: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };
        self.append(entry)
    }

    /// Log a delete operation
    pub fn log_delete(&mut self, relation: &str, tuples: Vec<Tuple2>) -> StorageResult<()> {
        let entry = WalEntry {
            op: WalOp::Delete,
            relation: relation.to_string(),
            tuples,
            ts: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
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
                .map_err(|e| StorageError::Other(format!("WAL parse error: {}", e)))?;
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
                    .unwrap()
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
pub fn replay_wal(entries: &[WalEntry], data: &mut std::collections::HashMap<String, Vec<Tuple2>>) {
    for entry in entries {
        match entry.op {
            WalOp::Insert => {
                let relation_data = data.entry(entry.relation.clone()).or_insert_with(Vec::new);
                for tuple in &entry.tuples {
                    if !relation_data.contains(tuple) {
                        relation_data.push(*tuple);
                    }
                }
            }
            WalOp::Delete => {
                if let Some(relation_data) = data.get_mut(&entry.relation) {
                    relation_data.retain(|t| !entry.tuples.contains(t));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_append_and_read() {
        let temp = TempDir::new().unwrap();
        let mut wal = Wal::new(temp.path().to_path_buf()).unwrap();

        // Write some entries
        wal.log_insert("edge", vec![(1, 2), (3, 4)]).unwrap();
        wal.log_insert("edge", vec![(5, 6)]).unwrap();
        wal.log_delete("edge", vec![(1, 2)]).unwrap();

        // Read back
        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 3);

        assert_eq!(entries[0].op, WalOp::Insert);
        assert_eq!(entries[0].relation, "edge");
        assert_eq!(entries[0].tuples, vec![(1, 2), (3, 4)]);

        assert_eq!(entries[1].op, WalOp::Insert);
        assert_eq!(entries[1].tuples, vec![(5, 6)]);

        assert_eq!(entries[2].op, WalOp::Delete);
        assert_eq!(entries[2].tuples, vec![(1, 2)]);
    }

    #[test]
    fn test_wal_replay() {
        let temp = TempDir::new().unwrap();
        let mut wal = Wal::new(temp.path().to_path_buf()).unwrap();

        // Simulate operations
        wal.log_insert("edge", vec![(1, 2), (3, 4)]).unwrap();
        wal.log_insert("node", vec![(10, 20)]).unwrap();
        wal.log_delete("edge", vec![(1, 2)]).unwrap();
        wal.log_insert("edge", vec![(5, 6)]).unwrap();

        // Replay
        let entries = wal.read_all().unwrap();
        let mut data = std::collections::HashMap::new();
        replay_wal(&entries, &mut data);

        // Check results
        let edge = data.get("edge").unwrap();
        assert_eq!(edge.len(), 2);
        assert!(edge.contains(&(3, 4)));
        assert!(edge.contains(&(5, 6)));
        assert!(!edge.contains(&(1, 2))); // Was deleted

        let node = data.get("node").unwrap();
        assert_eq!(node.len(), 1);
        assert!(node.contains(&(10, 20)));
    }

    #[test]
    fn test_wal_clear() {
        let temp = TempDir::new().unwrap();
        let mut wal = Wal::new(temp.path().to_path_buf()).unwrap();

        wal.log_insert("edge", vec![(1, 2)]).unwrap();
        assert_eq!(wal.entries_since_compaction(), 1);

        wal.clear().unwrap();
        assert_eq!(wal.entries_since_compaction(), 0);

        // New entries should work
        wal.log_insert("edge", vec![(3, 4)]).unwrap();
        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].tuples, vec![(3, 4)]);
    }

    #[test]
    fn test_wal_compaction_threshold() {
        let temp = TempDir::new().unwrap();
        let mut wal = Wal::new(temp.path().to_path_buf()).unwrap();
        wal.set_compaction_threshold(3);

        assert!(!wal.needs_compaction());

        wal.log_insert("edge", vec![(1, 2)]).unwrap();
        assert!(!wal.needs_compaction());

        wal.log_insert("edge", vec![(3, 4)]).unwrap();
        assert!(!wal.needs_compaction());

        wal.log_insert("edge", vec![(5, 6)]).unwrap();
        assert!(wal.needs_compaction()); // Threshold reached
    }
}
