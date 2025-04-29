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

