//! Write-Ahead Log for persist layer
//!
//! The WAL makes each committed [`Transaction`] durable until its updates are flushed
//! to batch files. Every transaction is one record (see the `wal_record` module), written
//! with one append and, when durable, one fsync. A failed append is cut back off the
//! file before an ordinary failure is reported. When the cut fails it is recorded
//! durably (see the `wal_cut` module) and made before
//! the next write or at the next open; when even that record fails, the append
//! reports [`StorageError::OutcomeUnknown`]: its outcome is unknown, not failed.
//! So does an append whose file is no longer the file at the WAL's path, because
//! the file or its directory was removed, moved or replaced while the server ran,
//! whether the writer was open or closed.
//!
//! Rule and schema changes stay in the WAL until their knowledge graph's catalog
//! files are saved (see the `catalog_log` module).

use super::sync_directory;
use super::transaction::{Transaction, TxnOp};
use super::wal_cut;
use super::wal_record;
use crate::storage::{StorageError, StorageResult};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// How to bring the file back to a committed state after a failed write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Repair {
    /// Cut the file to exactly this length: the end of the last committed record.
    ToLength(u64),
    /// The committed length is unknown: cut after the last intact record.
    ToIntactPrefix,
}

/// A write failure injected by tests, consumed by the first write that reaches it.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalFault {
    /// Half the record reaches the file, then the write fails.
    Write,
    /// The whole record reaches the file, then fsync fails.
    Sync,
    /// Cutting a failed record back off the file fails.
    Restore,
    /// Saving the cut that could not be made fails.
    SaveCut,
    /// Rewriting the WAL to retire records fails.
    Rewrite,
}

/// Write-Ahead Log writer
pub struct PersistWal {
    /// Path to WAL directory
    wal_dir: PathBuf,
    /// Current WAL file writer
    writer: Option<BufWriter<File>>,
    /// Current WAL file path
    current_file: PathBuf,
    /// Bytes in the file plus bytes buffered in `writer`
    len: u64,
    /// Identity of the file `current_file` must hold, to detect it being removed or
    /// replaced; `None` while the WAL has no file
    file_id: Option<FileId>,
    /// A failed write left bytes that could not be cut off; repair before the next write
    repair: Option<Repair>,
    read_only: bool,
    #[cfg(test)]
    faults: Vec<WalFault>,
    /// Rewrites made by [`Self::retain_ops`].
    #[cfg(test)]
    pub(crate) rewrites: usize,
}

impl PersistWal {
    /// Open the WAL and recover its committed transactions, in commit order.
    ///
    /// The file is cut after its longest intact prefix of records. An unterminated
    /// last line is a write torn by a crash and is dropped. Any other damage may hide
    /// committed data, so the cut-off bytes are first saved to a `.corrupt` file in
    /// the WAL directory.
    ///
    /// # Errors
    /// I/O failures, and [`StorageError::WalUnreadable`] for an intact record this
    /// server cannot decode.
    pub fn open(wal_dir: PathBuf) -> StorageResult<(Self, Vec<Transaction>)> {
        super::create_directory(&wal_dir)?;
        let mut wal = PersistWal {
            current_file: wal_dir.join("current.wal"),
            wal_dir,
            writer: None,
            len: 0,
            file_id: None,
            repair: None,
            read_only: false,
            #[cfg(test)]
            faults: Vec::new(),
            #[cfg(test)]
            rewrites: 0,
        };
        let txns = wal.recover()?;
        wal.file_id = match fs::metadata(&wal.current_file) {
            Ok(metadata) => Some(FileId::of(&metadata)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        Ok((wal, txns))
    }

    /// Cut the file after its intact prefix and return that prefix's transactions.
    /// A cut recorded by a failed append is made first.
    fn recover(&self) -> StorageResult<Vec<Transaction>> {
        if let Some(len) = wal_cut::load(&self.wal_dir)? {
            if self.current_file.exists() {
                cut_file(&self.current_file, len)?;
            }
        }
        if self.current_file.exists() {
            File::open(&self.current_file)?.sync_all()?;
        }
        wal_cut::remove(&self.wal_dir)?;
        let Some(bytes) = self.read_file()? else {
            return Ok(Vec::new());
        };
        let scan = wal_record::scan(&self.current_file, &bytes)?;
        if let Some(damage) = &scan.damage {
            let dropped = &bytes[scan.valid_end..];
            if !scan.torn_tail {
                let saved = self.quarantine(dropped)?;
                tracing::error!(
                    file = %self.current_file.display(),
                    offset = scan.valid_end,
                    %damage,
                    intact_records_dropped = scan.stranded,
                    saved_to = %saved.display(),
                    "WAL damaged: recovering to the last intact record; \
                     the bytes from the damage on are not replayed"
                );
            } else {
                tracing::warn!(
                    file = %self.current_file.display(),
                    kept = scan.valid_end,
                    dropped = dropped.len(),
                    %damage,
                    "Truncating torn WAL tail"
                );
            }
            cut_file(&self.current_file, scan.valid_end as u64)?;
        }
        Ok(scan.txns)
    }

    /// Save bytes cut off the WAL to `<unix-ms>.corrupt` beside it, durably.
    fn quarantine(&self, bytes: &[u8]) -> StorageResult<PathBuf> {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis());
        let path = self.wal_dir.join(format!("current.wal.{millis}.corrupt"));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        sync_directory(&self.wal_dir)?;
        Ok(path)
    }

    fn read_file(&self) -> StorageResult<Option<Vec<u8>>> {
        match fs::read(&self.current_file) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Restore the file to a committed state after a write failed to.
    fn apply_repair(&mut self, repair: Repair) -> StorageResult<()> {
        match repair {
            Repair::ToLength(len) => {
                cut_file(&self.current_file, len)?;
                Ok(wal_cut::remove(&self.wal_dir)?)
            }
            Repair::ToIntactPrefix => self.recover().map(drop),
        }
    }

    /// Ensure writer is open
    fn ensure_writer(&mut self) -> StorageResult<&mut BufWriter<File>> {
        self.check_writable()?;
        if self.writer.is_none() {
            self.check_in_place()?;
            if let Some(repair) = self.repair {
                self.apply_repair(repair)?;
                self.repair = None;
            }
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.current_file)?;
            sync_directory(&self.wal_dir)?;
            let metadata = file.metadata()?;
            self.len = metadata.len();
            self.file_id = Some(FileId::of(&metadata));
            self.writer = Some(BufWriter::new(file));
        }
        Ok(self
            .writer
            .as_mut()
            .expect("writer is guaranteed Some: set on the line above when None"))
    }

    /// Append one transaction as one record. With `durable`, flush and fsync it
    /// before returning; otherwise it may sit in the buffer until [`Self::sync`].
    ///
    /// Before reporting an ordinary failure, cut the file back to its prior length
    /// or durably record that cut. An unknown outcome instead closes the WAL to
    /// further writes until restart recovery.
    ///
    /// # Errors
    /// [`StorageError::OutcomeUnknown`] when the cut can be neither made nor
    /// recorded, so a restart may recover the transaction, and when the record went
    /// to a file no longer at the WAL's path (see `check_in_place`).
    pub fn append(&mut self, txn: &Transaction, durable: bool) -> StorageResult<()> {
        self.append_record(&wal_record::encode(txn)?, durable)
    }

    /// [`Self::append`] for a transaction already encoded as one record (by
    /// `persist::encode_record`), so a caller that also ships the record
    /// encodes it once.
    ///
    /// # Errors
    /// As [`Self::append`].
    pub fn append_record(&mut self, record: &[u8], durable: bool) -> StorageResult<()> {
        self.ensure_writer()?;
        let len = self.len;
        let Err(write) = self.write_record(record, durable) else {
            return self.check_in_place();
        };
        self.discard_writer(Repair::ToLength(len));
        if self.repair.is_some() {
            if let Err(undo) = self.save_cut(len) {
                self.read_only = true;
                return Err(StorageError::OutcomeUnknown {
                    write: write.to_string(),
                    undo: undo.to_string(),
                });
            }
        }
        Err(write)
    }

    /// Fail unless the WAL's file is still the file at `current_file`.
    ///
    /// An open file outlives its path: when the data directory is removed or moved
    /// while the server runs, writes and fsyncs to the open file keep succeeding but
    /// restart recovery reads `current_file` and never sees them. Checked after each
    /// write, so a durable record that passes is in the WAL restart reads, and before
    /// the file is reopened, read or rewritten, so a file removed or replaced while
    /// the writer was closed is not taken for the WAL. A record that fails went to a
    /// file recovery cannot read unless it is put back, so its outcome is unknown,
    /// and the WAL refuses further writes until restart: the transactions
    /// acknowledged before it may be gone with the file.
    fn check_in_place(&mut self) -> StorageResult<()> {
        let Some(file_id) = self.file_id else {
            return Ok(());
        };
        let found = fs::metadata(&self.current_file);
        if found
            .as_ref()
            .is_ok_and(|metadata| FileId::of(metadata) == file_id)
        {
            return Ok(());
        }
        let now = found.map_or_else(
            |e| format!("cannot be read ({e})"),
            |_| "is a different file".to_string(),
        );
        self.writer = None;
        self.read_only = true;
        tracing::error!(
            file = %self.current_file.display(),
            now = %now,
            "WAL file removed or moved while the server ran; refusing writes until restart"
        );
        Err(StorageError::OutcomeUnknown {
            write: format!(
                "the WAL file {} was removed or moved while the server ran (the path now {now})",
                self.current_file.display()
            ),
            undo: "records written to the detached file are read by restart recovery only \
                   if it is put back"
                .to_string(),
        })
    }

    /// Record durably that the file must be cut back to `len`.
    fn save_cut(&mut self, len: u64) -> std::io::Result<()> {
        #[cfg(test)]
        if self.take_fault(WalFault::SaveCut) {
            return Err(std::io::Error::other("injected WAL fault: SaveCut"));
        }
        wal_cut::save(&self.wal_dir, len)
    }

    fn write_record(&mut self, record: &[u8], durable: bool) -> StorageResult<()> {
        #[cfg(test)]
        if self.take_fault(WalFault::Write) {
            let writer = self.ensure_writer()?;
            writer.write_all(&record[..record.len() / 2])?;
            writer.flush()?;
            return Err(injected(WalFault::Write));
        }
        #[cfg(test)]
        let fail_sync = self.take_fault(WalFault::Sync);

        let writer = self.ensure_writer()?;
        writer.write_all(record)?;
        if durable {
            writer.flush()?;
            #[cfg(test)]
            if fail_sync {
                return Err(injected(WalFault::Sync));
            }
            // sync_all() forces data to disk, not just the OS page cache.
            writer.get_ref().sync_all()?;
        }
        self.len += record.len() as u64;
        Ok(())
    }

    /// Drop the writer and bring the file back to a committed state. With
    /// [`Repair::ToLength`] earlier buffered records are kept: bytes past `len` are
    /// cut off, missing ones written from the buffer. If that fails, the repair
    /// runs before the next write instead.
    fn discard_writer(&mut self, repair: Repair) {
        let Some(writer) = self.writer.take() else {
            return;
        };
        let (mut file, unflushed) = writer.into_parts();
        let unflushed = unflushed.unwrap_or_default();
        #[cfg(test)]
        let fail_restore = self.take_fault(WalFault::Restore);
        #[cfg(not(test))]
        let fail_restore = false;
        let restored = match repair {
            Repair::ToLength(len) if !fail_restore => {
                restore_length(&mut file, len, &unflushed).is_ok()
            }
            _ => false,
        };
        self.repair = (!restored).then_some(repair);
    }

    /// Flush buffered bytes to the file, optionally fsyncing. On error the writer is
    /// discarded so a retry on drop cannot write a partial record later.
    fn flush_writer(&mut self, sync: bool) -> StorageResult<()> {
        let Some(writer) = self.writer.as_mut() else {
            return Ok(());
        };
        let result = writer.flush().and_then(|()| {
            if sync {
                writer.get_ref().sync_all()
            } else {
                Ok(())
            }
        });
        if result.is_err() {
            self.discard_writer(Repair::ToIntactPrefix);
        }
        Ok(result?)
    }

    /// Close the writer with every buffered record in the file and any pending repair
    /// applied, so the file holds exactly the committed transactions.
    fn settle(&mut self) -> StorageResult<()> {
        self.check_writable()?;
        self.flush_writer(false)?;
        self.writer = None;
        self.check_in_place()?;
        if let Some(repair) = self.repair {
            self.apply_repair(repair)?;
            self.repair = None;
        }
        Ok(())
    }

    /// Read every committed transaction, in commit order.
    ///
    /// # Errors
    /// Fails if the file holds a damaged record. Damage found at open is cut off
    /// there, so this means the file changed underneath the running server.
    pub fn read_all(&mut self) -> StorageResult<Vec<Transaction>> {
        self.settle()?;
        let Some(bytes) = self.read_file()? else {
            return Ok(Vec::new());
        };
        let scan = wal_record::scan(&self.current_file, &bytes)?;
        match scan.damage {
            None => Ok(scan.txns),
            Some(damage) => Err(StorageError::Other(format!(
                "WAL {} damaged at byte {} while the server is running: {damage}",
                self.current_file.display(),
                scan.valid_end
            ))),
        }
    }

    /// Clear the WAL (after successful flush to batch files)
    pub fn clear(&mut self) -> StorageResult<()> {
        self.settle()?;

        // The caller has already flushed all data to batch files, so the WAL
        // records are redundant.
        if self.current_file.exists() {
            fs::remove_file(&self.current_file)?;
        }
        self.file_id = None;
        self.sync_retirement()
    }

    /// Sync WAL to disk (flushes buffer and calls fsync)
    pub fn sync(&mut self) -> StorageResult<()> {
        self.check_writable()?;
        self.flush_writer(true)
    }

    /// Remove every change to `shard` from the WAL, dropping transactions left empty.
    /// Other shards' changes, and their transactions' boundaries, are preserved.
    pub fn remove_shard_entries(&mut self, shard_name: &str) -> StorageResult<()> {
        self.retain_ops(|_, op| !matches!(op, TxnOp::Facts { shard, .. } if shard == shard_name))
    }

    /// Keep only the changes `keep(revision, op)` accepts, dropping transactions
    /// left empty. Surviving changes keep their transactions' boundaries.
    ///
    /// Uses atomic write-to-new+rename: the surviving records are written to
    /// `current.wal.new`, synced, then renamed over `current.wal`. A crash at any
    /// point leaves either the old or the new WAL.
    pub fn retain_ops(&mut self, mut keep: impl FnMut(u64, &TxnOp) -> bool) -> StorageResult<()> {
        #[cfg(test)]
        if self.take_fault(WalFault::Rewrite) {
            return Err(injected(WalFault::Rewrite));
        }
        let mut txns = self.read_all()?;
        let mut changed = false;
        for txn in &mut txns {
            let revision = txn.revision();
            changed |= txn.retain(|op| keep(revision, op));
        }
        if !changed {
            return self.sync_retirement();
        }
        #[cfg(test)]
        {
            self.rewrites += 1;
        }
        txns.retain(|txn| !txn.is_empty());

        if txns.is_empty() {
            return self.clear();
        }

        let new_file = self.wal_dir.join("current.wal.new");
        let file_id = {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&new_file)?;
            let mut writer = BufWriter::new(file);
            for txn in &txns {
                writer.write_all(&wal_record::encode(txn)?)?;
            }
            writer.flush()?;
            writer.get_ref().sync_all()?;
            FileId::of(&writer.get_ref().metadata()?)
        };

        // On POSIX, rename is atomic - either the old or new file is visible.
        fs::rename(&new_file, &self.current_file)?;
        self.file_id = Some(file_id);
        self.sync_retirement()
    }

    fn sync_retirement(&self) -> StorageResult<()> {
        sync_directory(&self.wal_dir).map_err(StorageError::WalDurabilityPending)
    }

    /// Remove stale .archived WAL files left over from previous runs.
    /// Called during startup - if we reached this point, recovery succeeded
    /// and archived files are no longer needed. `.corrupt` files are kept for
    /// the operator.
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

    pub(crate) fn check_writable(&self) -> StorageResult<()> {
        if self.read_only {
            Err(StorageError::StoreReadOnly)
        } else {
            Ok(())
        }
    }

    /// Get WAL file size
    pub fn file_size(&self) -> u64 {
        fs::metadata(&self.current_file).map_or(0, |m| m.len())
    }

    /// Make a later write fail at `fault`.
    #[cfg(test)]
    pub(crate) fn inject_fault(&mut self, fault: WalFault) {
        self.faults.push(fault);
    }

    #[cfg(test)]
    fn take_fault(&mut self, fault: WalFault) -> bool {
        let found = self.faults.iter().position(|f| *f == fault);
        found.map(|i| self.faults.remove(i)).is_some()
    }
}

impl Drop for PersistWal {
    /// A failed write whose bytes could not be cut off is repaired on clean shutdown,
    /// so the next start does not replay it.
    fn drop(&mut self) {
        if let Err(e) = self.settle() {
            tracing::error!(
                file = %self.current_file.display(),
                error = %e,
                "WAL repair at shutdown failed; a failed write may be replayed"
            );
        }
    }
}

/// Which file a path or handle refers to: device and inode where the platform
/// has them, else nothing, so only a missing path is detected.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FileId {
    dev: u64,
    ino: u64,
}

impl FileId {
    #[cfg(unix)]
    fn of(metadata: &fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        FileId {
            dev: metadata.dev(),
            ino: metadata.ino(),
        }
    }

    #[cfg(not(unix))]
    fn of(_: &fs::Metadata) -> Self {
        FileId::default()
    }
}

#[cfg(test)]
fn injected(fault: WalFault) -> StorageError {
    std::io::Error::other(format!("injected WAL fault: {fault:?}")).into()
}

/// Cut `path` to at most `len` bytes and make that durable. A shorter file is
/// left alone: extending it would write zeros that read as damage.
fn cut_file(path: &Path, len: u64) -> StorageResult<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    if file.metadata()?.len() > len {
        file.set_len(len)?;
    }
    #[cfg(test)]
    super::check_sync_fault(path)?;
    file.sync_all()?;
    Ok(())
}

/// Bring `file` to exactly `len` bytes: cut off what a failed write left, or append
/// earlier buffered bytes the failure kept from reaching it.
fn restore_length(file: &mut File, len: u64, unflushed: &[u8]) -> std::io::Result<()> {
    let on_disk = file.metadata()?.len();
    if on_disk >= len {
        file.set_len(len)?;
    } else {
        let missing = usize::try_from(len - on_disk).unwrap_or(usize::MAX);
        file.write_all(
            unflushed
                .get(..missing)
                .ok_or(std::io::ErrorKind::UnexpectedEof)?,
        )?;
    }
    file.sync_all()
}

#[cfg(test)]
#[path = "wal_tests.rs"]
mod tests;
