//! Write-Ahead Log for persist layer
//!
//! The WAL provides durability for updates that haven't been flushed to batch files yet.
//! Each entry contains the shard name and the update data.

use super::batch::Update;
use crate::storage::{StorageError, StorageResult};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

/// A WAL entry containing shard and update information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Shard name (format: "{db}:{relation}")
    pub shard: String,
    /// The update
    pub update: Update,
}

/// Write-Ahead Log writer
pub struct PersistWal {
    /// Path to WAL directory
    wal_dir: PathBuf,
    /// Current WAL file writer
    writer: Option<BufWriter<File>>,
    /// Current WAL file path
    current_file: PathBuf,
    /// Number of entries written
    entries_written: usize,
}

impl PersistWal {
    /// Create a new WAL
    pub fn new(wal_dir: PathBuf) -> StorageResult<Self> {
        fs::create_dir_all(&wal_dir)?;

        let current_file = wal_dir.join("current.wal");

        Ok(PersistWal {
            wal_dir,
            writer: None,
            current_file,
            entries_written: 0,
        })
    }

    /// Ensure writer is open
    fn ensure_writer(&mut self) -> StorageResult<&mut BufWriter<File>> {
        if self.writer.is_none() {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.current_file)
                .map_err(|e| {
                    eprintln!(
                        "[wal] ERROR ensure_writer: path={}, parent_exists={}, error={}",
                        self.current_file.display(),
                        self.current_file
                            .parent()
                            .is_some_and(std::path::Path::exists),
                        e
                    );
                    e
                })?;
            self.writer = Some(BufWriter::new(file));
        }
        Ok(self.writer.as_mut().unwrap())
    }

    /// Append an entry to the WAL with immediate flush (durable)
    pub fn append(&mut self, shard: &str, update: &Update) -> StorageResult<()> {
        self.append_inner(shard, update, true)
    }

    /// Append an entry to the WAL without immediate flush (buffered)
    pub fn append_buffered(&mut self, shard: &str, update: &Update) -> StorageResult<()> {
        self.append_inner(shard, update, false)
    }

    /// Internal append implementation
    fn append_inner(&mut self, shard: &str, update: &Update, flush: bool) -> StorageResult<()> {
        let entry = WalEntry {
            shard: shard.to_string(),
            update: update.clone(),
        };

        let writer = self.ensure_writer()?;
        let json = serde_json::to_string(&entry)
            .map_err(|e| StorageError::Other(format!("WAL serialization failed: {e}")))?;

        writeln!(writer, "{json}")?;
        if flush {
            writer.flush()?;
        }
        self.entries_written += 1;

        Ok(())
    }

    /// Append multiple entries for a shard with immediate flush (durable)
    pub fn append_batch(&mut self, shard: &str, updates: &[Update]) -> StorageResult<()> {
        self.append_batch_inner(shard, updates, true)
    }

    /// Append multiple entries for a shard without immediate flush (buffered)
    pub fn append_batch_buffered(&mut self, shard: &str, updates: &[Update]) -> StorageResult<()> {
        self.append_batch_inner(shard, updates, false)
    }

    /// Internal batch append implementation
    fn append_batch_inner(
        &mut self,
        shard: &str,
        updates: &[Update],
        flush: bool,
    ) -> StorageResult<()> {
        for update in updates {
            self.append_inner(shard, update, false)?; // Don't flush individual entries
        }
        if flush {
            // Flush once at the end for the whole batch
            if let Some(ref mut writer) = self.writer {
                writer.flush()?;
            }
        }
        Ok(())
    }

    /// Read all entries from the WAL
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

    /// Read entries for a specific shard
    pub fn read_shard(&self, shard: &str) -> StorageResult<Vec<Update>> {
        let entries = self.read_all()?;
        Ok(entries
            .into_iter()
            .filter(|e| e.shard == shard)
            .map(|e| e.update)
            .collect())
    }

    /// Clear the WAL (after successful flush to batch files)
    pub fn clear(&mut self) -> StorageResult<()> {
        // Close writer
        self.writer = None;

        // Archive old WAL
        if self.current_file.exists() {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let archive_name = format!("wal_{timestamp}.archived");
            let archive_path = self.wal_dir.join(archive_name);
            fs::rename(&self.current_file, archive_path)?;
        }

        self.entries_written = 0;
        Ok(())
    }

    /// Sync WAL to disk
    pub fn sync(&mut self) -> StorageResult<()> {
        if let Some(ref mut writer) = self.writer {
            writer.flush()?;
        }
        Ok(())
    }

    /// Get number of entries written since last clear
    pub fn entries_written(&self) -> usize {
        self.entries_written
    }

    /// Remove all WAL entries for a specific shard.
    /// Rewrites the WAL excluding those entries. Other shards' data is preserved.
    pub fn remove_shard_entries(&mut self, shard_name: &str) -> StorageResult<()> {
        let entries = self.read_all()?;

        // Close writer before manipulating the file
        self.writer = None;

        // Archive old WAL (use nanos for unique names under rapid calls)
        if self.current_file.exists() {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let archive_path = self.wal_dir.join(format!("wal_{timestamp}.archived"));
            fs::rename(&self.current_file, &archive_path)?;
        }

        // Rewrite with surviving entries only
        self.entries_written = 0;
        for entry in entries {
            if entry.shard != shard_name {
                self.append_inner(&entry.shard, &entry.update, false)?;
            }
        }
        if let Some(ref mut writer) = self.writer {
            writer.flush()?;
        }

        Ok(())
    }

    /// Get WAL file size
    pub fn file_size(&self) -> u64 {
        fs::metadata(&self.current_file)
            .map(|m| m.len())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Tuple;
    use tempfile::TempDir;

    #[test]
    fn test_wal_append_read() {
        let temp = TempDir::new().unwrap();
        let mut wal = PersistWal::new(temp.path().to_path_buf()).unwrap();

        let update1 = Update::insert(Tuple::from_pair(1, 2), 10);
        let update2 = Update::delete(Tuple::from_pair(3, 4), 20);

        wal.append("db:edge", &update1).unwrap();
        wal.append("db:edge", &update2).unwrap();
        wal.append("db:node", &update1).unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 3);

        let edge_updates = wal.read_shard("db:edge").unwrap();
        assert_eq!(edge_updates.len(), 2);

        let node_updates = wal.read_shard("db:node").unwrap();
        assert_eq!(node_updates.len(), 1);
    }

    #[test]
    fn test_wal_clear() {
        let temp = TempDir::new().unwrap();
        let mut wal = PersistWal::new(temp.path().to_path_buf()).unwrap();

        wal.append("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10))
            .unwrap();
        assert_eq!(wal.entries_written(), 1);

        wal.clear().unwrap();
        assert_eq!(wal.entries_written(), 0);

        // New writes should work
        wal.append("db:edge", &Update::insert(Tuple::from_pair(3, 4), 20))
            .unwrap();
        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].update.data, Tuple::from_pair(3, 4));
    }

    #[test]
    fn test_wal_empty_read() {
        let temp = TempDir::new().unwrap();
        let wal = PersistWal::new(temp.path().to_path_buf()).unwrap();
        let entries = wal.read_all().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_wal_read_shard_empty() {
        let temp = TempDir::new().unwrap();
        let wal = PersistWal::new(temp.path().to_path_buf()).unwrap();
        let updates = wal.read_shard("nonexistent").unwrap();
        assert!(updates.is_empty());
    }

    #[test]
    fn test_wal_file_size() {
        let temp = TempDir::new().unwrap();
        let mut wal = PersistWal::new(temp.path().to_path_buf()).unwrap();
        assert_eq!(wal.file_size(), 0);

        wal.append("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10))
            .unwrap();
        assert!(wal.file_size() > 0);
    }

    #[test]
    fn test_wal_entries_written() {
        let temp = TempDir::new().unwrap();
        let mut wal = PersistWal::new(temp.path().to_path_buf()).unwrap();
        assert_eq!(wal.entries_written(), 0);

        wal.append("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10))
            .unwrap();
        assert_eq!(wal.entries_written(), 1);

        wal.append("db:edge", &Update::insert(Tuple::from_pair(3, 4), 20))
            .unwrap();
        assert_eq!(wal.entries_written(), 2);
    }

    #[test]
    fn test_wal_append_batch() {
        let temp = TempDir::new().unwrap();
        let mut wal = PersistWal::new(temp.path().to_path_buf()).unwrap();

        let updates = vec![
            Update::insert(Tuple::from_pair(1, 2), 10),
            Update::insert(Tuple::from_pair(3, 4), 10),
            Update::delete(Tuple::from_pair(5, 6), 10),
        ];

        wal.append_batch("db:edge", &updates).unwrap();
        assert_eq!(wal.entries_written(), 3);

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_wal_append_buffered() {
        let temp = TempDir::new().unwrap();
        let mut wal = PersistWal::new(temp.path().to_path_buf()).unwrap();

        wal.append_buffered("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10))
            .unwrap();
        wal.sync().unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_wal_sync_no_writer() {
        let temp = TempDir::new().unwrap();
        let mut wal = PersistWal::new(temp.path().to_path_buf()).unwrap();
        // Sync with no writer should not error
        wal.sync().unwrap();
    }

    #[test]
    fn test_wal_entry_serde() {
        let entry = WalEntry {
            shard: "db:edge".to_string(),
            update: Update::insert(Tuple::from_pair(1, 2), 10),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: WalEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.shard, "db:edge");
    }

    #[test]
    fn test_wal_remove_shard_entries() {
        let temp = TempDir::new().unwrap();
        let mut wal = PersistWal::new(temp.path().to_path_buf()).unwrap();

        wal.append("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10))
            .unwrap();
        wal.append("db:node", &Update::insert(Tuple::from_pair(3, 4), 20))
            .unwrap();
        wal.append("db:edge", &Update::insert(Tuple::from_pair(5, 6), 30))
            .unwrap();
        wal.append("other:rel", &Update::insert(Tuple::from_pair(7, 8), 40))
            .unwrap();
        assert_eq!(wal.entries_written(), 4);

        // Remove only db:edge entries
        wal.remove_shard_entries("db:edge").unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].shard, "db:node");
        assert_eq!(entries[1].shard, "other:rel");

        // entries_written should reflect the rewritten count
        assert_eq!(wal.entries_written(), 2);
    }

    #[test]
    fn test_wal_remove_shard_entries_all() {
        let temp = TempDir::new().unwrap();
        let mut wal = PersistWal::new(temp.path().to_path_buf()).unwrap();

        wal.append("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10))
            .unwrap();
        wal.append("db:edge", &Update::insert(Tuple::from_pair(3, 4), 20))
            .unwrap();

        // Remove all entries (only shard)
        wal.remove_shard_entries("db:edge").unwrap();

        let entries = wal.read_all().unwrap();
        assert!(entries.is_empty());
        assert_eq!(wal.entries_written(), 0);
    }

    #[test]
    fn test_wal_remove_shard_entries_nonexistent() {
        let temp = TempDir::new().unwrap();
        let mut wal = PersistWal::new(temp.path().to_path_buf()).unwrap();

        wal.append("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10))
            .unwrap();

        // Remove entries for a shard that doesn't exist — should be a no-op
        wal.remove_shard_entries("nonexistent").unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_wal_remove_shard_entries_then_append() {
        let temp = TempDir::new().unwrap();
        let mut wal = PersistWal::new(temp.path().to_path_buf()).unwrap();

        wal.append("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10))
            .unwrap();
        wal.append("db:node", &Update::insert(Tuple::from_pair(3, 4), 20))
            .unwrap();

        wal.remove_shard_entries("db:edge").unwrap();

        // New appends should work after removal
        wal.append("db:new", &Update::insert(Tuple::from_pair(5, 6), 30))
            .unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].shard, "db:node");
        assert_eq!(entries[1].shard, "db:new");
    }
}
