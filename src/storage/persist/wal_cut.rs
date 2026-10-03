//! The durable record of a WAL cut that could not be made.
//!
//! When a failed append's bytes cannot be cut back off the WAL, the length to cut
//! back to is saved beside it in `current.wal.cut`. Opening the WAL makes the cut
//! first, so a crash before the cut is retried still never replays the failed
//! record. The marker is removed as soon as the cut is made, before anything is
//! appended past that length; a marker left behind would cut committed records.

use crate::storage::{StorageError, StorageResult};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const MARKER: &str = "current.wal.cut";

fn marker(wal_dir: &Path) -> PathBuf {
    wal_dir.join(MARKER)
}

/// Durably record that the WAL must be cut back to `len` bytes.
pub(super) fn save(wal_dir: &Path, len: u64) -> io::Result<()> {
    let tmp = wal_dir.join(format!("{MARKER}.tmp"));
    let mut file = File::create(&tmp)?;
    file.write_all(len.to_string().as_bytes())?;
    file.sync_all()?;
    fs::rename(&tmp, marker(wal_dir))?;
    super::sync_directory(wal_dir)
}

/// The length a recorded cut brings the WAL back to, if one is pending.
///
/// # Errors
/// An unreadable marker: guessing could replay a failed write or cut a committed one.
pub(super) fn load(wal_dir: &Path) -> StorageResult<Option<u64>> {
    let path = marker(wal_dir);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    text.trim().parse().map(Some).map_err(|e| {
        StorageError::Other(format!(
            "WAL cut marker {} is unreadable ({e}); it records the WAL length before \
             a failed write. Set it to that length or remove it, then restart",
            path.display()
        ))
    })
}

/// Durably remove the marker once its cut is made. Absent is fine.
pub(super) fn remove(wal_dir: &Path) -> io::Result<()> {
    match fs::remove_file(marker(wal_dir)) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    super::sync_directory(wal_dir)
}
