//! Offline backup and restore of a data directory.
//!
//! An offline backup is a complete copy of a stopped server's data
//! directory (metadata, WAL, shard and batch files, per-KG rule and schema
//! catalogs, the `_internal` auth KG) plus a manifest listing every
//! directory and every file with its size and SHA-256.
//!
//! Consistency comes from exclusivity, not from a snapshot protocol:
//! [`create`] takes the same [`DataDirLock`](super::DataDirLock) a server
//! holds for its lifetime, so it refuses a directory a server is running on
//! and no server can start on it until the copy is finished. Restoring the
//! copy is then equivalent to restarting the server that was stopped.
//!
//! Every operation writes only into a new directory or an empty one (a
//! mounted volume may hold `lost+found`), so it never overwrites data. A
//! failed run returns that directory to how it found it; an interrupted one
//! leaves a backup without a manifest, which [`restore`] refuses.
//!
//! The source of a backup is never modified apart from the pid recorded in
//! its `LOCK` file while the backup holds the lock.
//!
//! ## Online export
//!
//! A running server backs itself up without stopping: it captures a
//! [`Checkpoint`] of every knowledge graph, each consistent at its own
//! committed revision, then [`Export`] writes it out on a background thread
//! as a complete data directory with the same manifest, marked with the
//! newest of those revisions. [`verify`]
//! and [`restore`] treat it like any other backup.

mod checkpoint;
mod destination;
mod export;
mod manifest;
mod offline;
mod tree;

pub use checkpoint::{Checkpoint, KgCheckpoint};
pub use export::{checkpoint_name, Export};
pub use manifest::{FileEntry, Manifest, MANIFEST_FILE_NAME};
pub use offline::{create, restore, verify, Report};

use super::StorageError;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Result type for backup operations.
pub type BackupResult<T> = Result<T, BackupError>;

/// Why a backup, verify or restore was refused or failed.
#[derive(Debug, Error)]
pub enum BackupError {
    /// The source does not look like a data directory.
    #[error("{} is not an InputLayer data directory (no metadata/ or persist/ inside)", .0.display())]
    NotADataDir(PathBuf),

    /// A server holds the data directory lock.
    #[error(
        "data directory {} is in use by a running InputLayer server (pid {owner}); \
         stop the server first",
        dir.display()
    )]
    SourceInUse {
        /// The locked data directory
        dir: PathBuf,
        /// Pid recorded by the lock owner, or "unknown"
        owner: String,
    },

    /// The destination exists and is not an empty directory.
    #[error(
        "{} already exists and is not an empty directory; backup and restore write only \
         into a new or empty directory",
        .0.display()
    )]
    DestinationExists(PathBuf),

    /// The destination would be written inside the tree being read.
    #[error("{} is inside {}; choose a destination outside it", dest.display(), inside.display())]
    DestinationInsideSource {
        /// The requested destination
        dest: PathBuf,
        /// The directory being copied
        inside: PathBuf,
    },

    /// The destination path names no directory (e.g. `/` or `..`).
    #[error("{} is not a usable destination directory name", .0.display())]
    InvalidDestination(PathBuf),

    /// The backup has no manifest.
    #[error(
        "{} is not a complete backup (no {MANIFEST_FILE_NAME}): the backup was interrupted \
         or this is not a backup directory",
        .0.display()
    )]
    Incomplete(PathBuf),

    /// The manifest is unreadable, foreign or unsafe.
    #[error("invalid backup manifest: {0}")]
    InvalidManifest(String),

    /// Backup contents differ from the manifest.
    #[error("backup is corrupt: {path}: {reason}")]
    Corrupt {
        /// Manifest path of the offending entry
        path: String,
        /// What differs
        reason: String,
    },

    /// The tree holds something other than directories and regular files.
    #[error("cannot back up {}: {1} not supported", .0.display())]
    UnsupportedEntry(PathBuf, &'static str),

    /// The checkpoint name is not a plain directory name.
    #[error(
        "invalid checkpoint name '{0}': use letters, digits, '-', '_' and '.', \
         not starting with '.', at most {max} characters",
        max = export::MAX_NAME_LEN
    )]
    InvalidName(String),

    /// No directory is configured for online checkpoint exports.
    #[error(
        "no backup directory configured: set storage.backup_dir \
         (INPUTLAYER_STORAGE__BACKUP_DIR) to a directory outside the data directory"
    )]
    NoBackupDir,

    /// Another online checkpoint export is still being written.
    #[error("a checkpoint export into {} is still running; wait for it to finish", .0.display())]
    ExportRunning(PathBuf),

    /// The export stopped before it finished; nothing was left behind.
    #[error("checkpoint export cancelled before it finished (the server is shutting down)")]
    Cancelled,

    /// A storage operation failed (taking a lock, writing engine files).
    #[error(transparent)]
    Storage(StorageError),

    /// Filesystem failure on a specific path.
    #[error("{}: {source}", path.display())]
    Io {
        /// The path being accessed
        path: PathBuf,
        /// Underlying error
        source: io::Error,
    },

    /// The operation failed and its partial output could not be removed.
    #[error(
        "{cause}; the partial directory {} could not be removed ({cleanup}). \
         Delete it by hand; restore refuses it because it has no manifest",
        dir.display()
    )]
    CleanupFailed {
        /// The original failure
        cause: Box<BackupError>,
        /// The partial directory left behind
        dir: PathBuf,
        /// Why removing it failed
        cleanup: io::Error,
    },
}

impl BackupError {
    fn io(path: &Path, source: io::Error) -> Self {
        Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}

impl From<StorageError> for BackupError {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::DataDirLocked { dir, owner } => Self::SourceInUse { dir, owner },
            other => Self::Storage(other),
        }
    }
}
