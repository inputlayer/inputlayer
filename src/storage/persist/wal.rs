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
                .open(&self.current_file)?;
            self.writer = Some(BufWriter::new(file));
        }
        Ok(self.writer.as_mut().unwrap())
    }

    /// Append an entry to the WAL
    pub fn append(&mut self, shard: &str, update: &Update) -> StorageResult<()> {
        let entry = WalEntry {
            shard: shard.to_string(),
            update: update.clone(),
        };

        let writer = self.ensure_writer()?;
        let json = serde_json::to_string(&entry)
            .map_err(|e| StorageError::Other(format!("WAL serialization failed: {}", e)))?;

        writeln!(writer, "{}", json)?;
        writer.flush()?;
        self.entries_written += 1;

        Ok(())
    }

    /// Append multiple entries for a shard
    pub fn append_batch(&mut self, shard: &str, updates: &[Update]) -> StorageResult<()> {
        for update in updates {
            self.append(shard, update)?;
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
                .map_err(|e| StorageError::Other(format!("WAL parse error: {}", e)))?;
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
            let archive_name = format!("wal_{}.archived", timestamp);
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

        wal.append("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10)).unwrap();
        assert_eq!(wal.entries_written(), 1);

        wal.clear().unwrap();
        assert_eq!(wal.entries_written(), 0);

        // New writes should work
        wal.append("db:edge", &Update::insert(Tuple::from_pair(3, 4), 20)).unwrap();
        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].update.data, Tuple::from_pair(3, 4));
    }
}
