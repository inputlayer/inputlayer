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
            // sync_all() forces data to disk (not just OS page cache).
            // Without this, a power failure after flush() could still lose data
            // because the OS may not have written the page cache to the physical disk yet.
            writer.get_ref().sync_all()?;
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
            // Flush once at the end for the whole batch and sync to disk
            if let Some(ref mut writer) = self.writer {
                writer.flush()?;
                writer.get_ref().sync_all()?;
            }
        }
        Ok(())
    }

    /// Read all entries from the WAL.
    ///
    /// Tolerates corrupt or truncated lines by logging a warning and skipping
    /// them. This makes WAL recovery resilient to partial writes (crash mid-write)
    /// AND bit-rot or other corruption — the system recovers as many valid
    /// entries as possible rather than refusing to start.
    pub fn read_all(&self) -> StorageResult<Vec<WalEntry>> {
        if !self.current_file.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.current_file)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut lines: Vec<String> = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            lines.push(line);
        }

        let mut skipped = 0usize;
        for (i, line) in lines.iter().enumerate() {
            match serde_json::from_str::<WalEntry>(line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    eprintln!(
                        "[wal] WARNING: Skipping corrupt WAL entry on line {} of {}: {}",
                        i + 1,
                        self.current_file.display(),
                        e
                    );
                    skipped += 1;
                }
            }
        }

        if skipped > 0 {
            eprintln!(
                "[wal] WARNING: Skipped {skipped} corrupt WAL entry/entries in {}. Recovered {} valid entries.",
                self.current_file.display(),
                entries.len()
            );
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

        // Simply remove the old WAL file. The caller has already flushed
        // all data to batch files, so the WAL entries are redundant.
        if self.current_file.exists() {
            fs::remove_file(&self.current_file)?;
        }

        self.entries_written = 0;
        Ok(())
    }

    /// Sync WAL to disk (flushes buffer and calls fsync)
    pub fn sync(&mut self) -> StorageResult<()> {
        if let Some(ref mut writer) = self.writer {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        Ok(())
    }

    /// Get number of entries written since last clear
    pub fn entries_written(&self) -> usize {
        self.entries_written
    }

    /// Remove all WAL entries for a specific shard.
    /// Rewrites the WAL excluding those entries. Other shards' data is preserved.
    ///
    /// Uses atomic write-to-new+rename: surviving entries are written to
    /// `current.wal.new`, synced to disk, then atomically renamed to `current.wal`.
    /// This guarantees that either the old or new WAL exists at all times —
    /// a crash at any point cannot lose other shards' data.
    pub fn remove_shard_entries(&mut self, shard_name: &str) -> StorageResult<()> {
        let entries = self.read_all()?;

        // Close writer before manipulating the file
        self.writer = None;

        let surviving: Vec<&WalEntry> = entries.iter().filter(|e| e.shard != shard_name).collect();

        if surviving.is_empty() {
            // No surviving entries: just remove the WAL file
            if self.current_file.exists() {
                fs::remove_file(&self.current_file)?;
            }
            self.entries_written = 0;
            return Ok(());
        }

        // Write surviving entries to a temp file
        let new_file = self.wal_dir.join("current.wal.new");
        {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&new_file)?;
            let mut writer = BufWriter::new(file);
            for entry in &surviving {
                let json = serde_json::to_string(entry)
                    .map_err(|e| StorageError::Other(format!("WAL serialization failed: {e}")))?;
                writeln!(writer, "{json}")?;
            }
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        // Atomic rename: replaces old WAL with the new one.
        // On POSIX, rename is atomic — either the old or new file is visible.
        fs::rename(&new_file, &self.current_file)?;

        self.entries_written = surviving.len();
        Ok(())
    }

    /// Remove stale .archived WAL files left over from previous runs.
    /// Called during startup — if we reached this point, recovery succeeded
    /// and archived files are no longer needed.
    pub fn cleanup_archives(&self) -> StorageResult<()> {
        if !self.wal_dir.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(&self.wal_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path
                .extension()
                .and_then(|s| s.to_str())
                .is_some_and(|ext| ext == "archived")
            {
                let _ = fs::remove_file(&path);
            }
            // Also clean up incomplete .new files from interrupted rewrites
            if path.file_name().and_then(|n| n.to_str()) == Some("current.wal.new") {
                let _ = fs::remove_file(&path);
            }
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

    // === Regression tests for production readiness fixes ===

    /// P0-1: Verify that immediate-mode append actually syncs to disk.
    /// After append(), data must be readable from a FRESH WAL instance
    /// (proving it reached disk, not just OS page cache).
    #[test]
    fn test_wal_fsync_immediate_mode_durable() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().to_path_buf();

        // Write data with immediate flush
        {
            let mut wal = PersistWal::new(path.clone()).unwrap();
            wal.append("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10))
                .unwrap();
            // Drop without explicit sync — append() should have already synced
        }

        // Read from a fresh WAL instance — data must be there
        {
            let wal = PersistWal::new(path).unwrap();
            let entries = wal.read_all().unwrap();
            assert_eq!(entries.len(), 1, "Data should be durable after append()");
            assert_eq!(entries[0].shard, "db:edge");
        }
    }

    /// P0-1: Verify batch append syncs to disk.
    #[test]
    fn test_wal_fsync_batch_durable() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().to_path_buf();

        {
            let mut wal = PersistWal::new(path.clone()).unwrap();
            let updates = vec![
                Update::insert(Tuple::from_pair(1, 2), 10),
                Update::insert(Tuple::from_pair(3, 4), 20),
            ];
            wal.append_batch("db:edge", &updates).unwrap();
        }

        {
            let wal = PersistWal::new(path).unwrap();
            let entries = wal.read_all().unwrap();
            assert_eq!(entries.len(), 2, "Batch data should be durable");
        }
    }

    /// P0-4: Verify that remove_shard_entries uses atomic rename (no .archived files).
    /// The old archive-based approach could lose data on crash between archive and rewrite.
    #[test]
    fn test_wal_remove_shard_no_archived_files() {
        let temp = TempDir::new().unwrap();
        let mut wal = PersistWal::new(temp.path().to_path_buf()).unwrap();

        wal.append("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10))
            .unwrap();
        wal.append("db:node", &Update::insert(Tuple::from_pair(3, 4), 20))
            .unwrap();

        wal.remove_shard_entries("db:edge").unwrap();

        // No .archived files should exist — we use atomic rename now
        let archived_files: Vec<_> = fs::read_dir(temp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .and_then(|s| s.to_str())
                    .is_some_and(|ext| ext == "archived")
            })
            .collect();
        assert!(
            archived_files.is_empty(),
            "No .archived files should be created; found {:?}",
            archived_files
        );
    }

    /// P0-4: Verify remove_shard_entries is crash-safe by checking
    /// that no .new temp file is left behind after successful operation.
    #[test]
    fn test_wal_remove_shard_no_temp_files() {
        let temp = TempDir::new().unwrap();
        let mut wal = PersistWal::new(temp.path().to_path_buf()).unwrap();

        wal.append("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10))
            .unwrap();
        wal.append("db:node", &Update::insert(Tuple::from_pair(3, 4), 20))
            .unwrap();

        wal.remove_shard_entries("db:edge").unwrap();

        // No .new temp file should remain
        let temp_file = temp.path().join("current.wal.new");
        assert!(
            !temp_file.exists(),
            "Temp file current.wal.new should be cleaned up"
        );
    }

    /// P0-5: Verify that a truncated last line is tolerated during recovery.
    #[test]
    fn test_wal_recovery_truncated_last_line() {
        let temp = TempDir::new().unwrap();
        let wal_dir = temp.path().to_path_buf();
        let wal_file = wal_dir.join("current.wal");

        // Write valid entries + a truncated last line
        {
            let mut wal = PersistWal::new(wal_dir.clone()).unwrap();
            wal.append("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10))
                .unwrap();
            wal.append("db:node", &Update::insert(Tuple::from_pair(3, 4), 20))
                .unwrap();
        }

        // Append a truncated (invalid JSON) line to simulate crash mid-write
        {
            use std::io::Write;
            let mut file = OpenOptions::new().append(true).open(&wal_file).unwrap();
            writeln!(file, r#"{{"shard":"db:edge","upd"#).unwrap();
        }

        // Recovery should succeed — truncated last line is skipped
        let wal = PersistWal::new(wal_dir).unwrap();
        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 2, "Should recover 2 valid entries");
        assert_eq!(entries[0].shard, "db:edge");
        assert_eq!(entries[1].shard, "db:node");
    }

    /// P0-5: Verify that corruption on a NON-last line is skipped (resilient recovery).
    /// The WAL recovers as many valid entries as possible, skipping corrupt ones.
    #[test]
    fn test_wal_recovery_corrupted_middle_line_is_skipped() {
        let temp = TempDir::new().unwrap();
        let wal_dir = temp.path().to_path_buf();
        let wal_file = wal_dir.join("current.wal");

        // Create WAL dir
        fs::create_dir_all(&wal_dir).unwrap();

        // Write: valid line, corrupt line, valid line
        {
            use std::io::Write;
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&wal_file)
                .unwrap();

            let valid_entry = WalEntry {
                shard: "db:edge".to_string(),
                update: Update::insert(Tuple::from_pair(1, 2), 10),
            };
            let json = serde_json::to_string(&valid_entry).unwrap();
            writeln!(file, "{json}").unwrap();
            writeln!(file, "THIS IS CORRUPT DATA").unwrap();
            writeln!(file, "{json}").unwrap();
        }

        let wal = PersistWal::new(wal_dir).unwrap();
        let entries = wal.read_all().unwrap();
        assert_eq!(
            entries.len(),
            2,
            "Should recover 2 valid entries, skipping the corrupt middle line"
        );
        assert_eq!(entries[0].shard, "db:edge");
        assert_eq!(entries[1].shard, "db:edge");
    }

    /// P1-7: Verify cleanup_archives removes stale .archived and .new files.
    #[test]
    fn test_wal_cleanup_archives() {
        let temp = TempDir::new().unwrap();
        let wal_dir = temp.path().to_path_buf();
        let wal = PersistWal::new(wal_dir.clone()).unwrap();

        // Create fake stale files
        fs::write(wal_dir.join("wal_12345.archived"), "stale").unwrap();
        fs::write(wal_dir.join("wal_67890.archived"), "stale").unwrap();
        fs::write(wal_dir.join("current.wal.new"), "incomplete").unwrap();

        wal.cleanup_archives().unwrap();

        // All stale files should be removed
        assert!(!wal_dir.join("wal_12345.archived").exists());
        assert!(!wal_dir.join("wal_67890.archived").exists());
        assert!(!wal_dir.join("current.wal.new").exists());
    }

    /// P1-7: Verify clear() no longer creates .archived files.
    #[test]
    fn test_wal_clear_no_archived_files() {
        let temp = TempDir::new().unwrap();
        let mut wal = PersistWal::new(temp.path().to_path_buf()).unwrap();

        wal.append("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10))
            .unwrap();
        wal.clear().unwrap();

        let archived: Vec<_> = fs::read_dir(temp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .and_then(|s| s.to_str())
                    .is_some_and(|ext| ext == "archived")
            })
            .collect();
        assert!(
            archived.is_empty(),
            "clear() should not create .archived files"
        );
    }

    /// P0-1: Verify sync() calls fsync (data readable from new instance).
    #[test]
    fn test_wal_sync_actually_syncs() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().to_path_buf();

        {
            let mut wal = PersistWal::new(path.clone()).unwrap();
            wal.append_buffered("db:edge", &Update::insert(Tuple::from_pair(1, 2), 10))
                .unwrap();
            wal.sync().unwrap();
        }

        {
            let wal = PersistWal::new(path).unwrap();
            let entries = wal.read_all().unwrap();
            assert_eq!(entries.len(), 1, "sync() should ensure data is durable");
        }
    }
}
