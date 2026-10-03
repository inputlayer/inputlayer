//! `create`, `verify` and `restore`: the three offline backup operations.
//!
//! Files are copied and fsynced in parallel: with many small files the
//! copy is bound by fsync latency, and concurrent fsyncs share journal
//! commits.

use super::destination::{canonical, Destination, LOST_AND_FOUND};
use super::manifest::{relative_path, FileEntry, Manifest, MANIFEST_FILE_NAME};
use super::tree::{self, copy_hashed, hash_file, sync_dir};
use super::{BackupError, BackupResult};
use crate::storage::data_dir_lock::{DataDirLock, LOCK_FILE_NAME};
use rayon::prelude::*;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// What an operation covered and how long it took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    /// The backup (create, verify) or restored data directory (restore).
    pub dir: PathBuf,
    /// Directories covered.
    pub directories: usize,
    /// Files covered.
    pub files: usize,
    /// Bytes of file content covered.
    pub bytes: u64,
    /// Wall-clock duration.
    pub elapsed: Duration,
}

/// Back up the stopped server's `data_dir` into `dest`, a new or empty
/// directory.
///
/// Holds the data directory lock for the whole copy, so it fails with
/// [`BackupError::SourceInUse`] while a server runs on `data_dir`, and a
/// server cannot start on it until the copy is done.
pub fn create(data_dir: &Path, dest: &Path) -> BackupResult<Report> {
    let start = Instant::now();
    let data_dir = existing_data_dir(data_dir)?;
    let _lock = DataDirLock::acquire(&data_dir)?;

    let (dest, manifest) = Destination::claim(dest, &data_dir)?.fill(|dest| {
        let tree = tree::walk(&data_dir, &[LOCK_FILE_NAME, LOST_AND_FOUND])?;
        for dir in &tree.directories {
            fs::create_dir(dest.join(dir)).map_err(|e| BackupError::io(&dest.join(dir), e))?;
        }
        let files = tree
            .files
            .par_iter()
            .zip(tree::manifest_paths(&tree.files)?)
            .map(|(file, path)| {
                let (size, sha256) = copy_hashed(&data_dir.join(file), &dest.join(file))?;
                Ok(FileEntry { path, size, sha256 })
            })
            .collect::<BackupResult<Vec<_>>>()?;
        sync_tree(dest, &tree.directories)?;

        let manifest = Manifest::new(
            data_dir.clone(),
            tree::manifest_paths(&tree.directories)?,
            files,
        );
        manifest.store(dest)?;
        sync_dir(dest)?;
        Ok(manifest)
    })?;

    Ok(report(dest, &manifest, start))
}

/// Check that the backup at `backup_dir` is complete and intact: a manifest,
/// exactly the listed entries, and every file's size and SHA-256 matching.
/// Read-only.
pub fn verify(backup_dir: &Path) -> BackupResult<Report> {
    let start = Instant::now();
    let manifest = Manifest::load(backup_dir)?;
    check_listing(backup_dir, &manifest)?;
    for entry in &manifest.files {
        let actual = hash_file(&backup_dir.join(relative_path(&entry.path)?))?;
        check_entry(entry, actual)?;
    }
    Ok(report(backup_dir.to_path_buf(), &manifest, start))
}

/// Restore the backup at `backup_dir` into `target`, a new or empty
/// directory, which becomes a data directory.
///
/// Refuses an incomplete or corrupt backup, checking every file against the
/// manifest as it is copied. Holds `target`'s data directory lock while
/// copying, so no server starts on a half-restored directory. On failure
/// `target` is returned to how it was found.
pub fn restore(backup_dir: &Path, target: &Path) -> BackupResult<Report> {
    let start = Instant::now();
    let manifest = Manifest::load(backup_dir)?;
    check_listing(backup_dir, &manifest)?;
    let backup_dir = canonical(backup_dir)?;

    let (target, ()) = Destination::claim(target, &backup_dir)?.fill(|target| {
        let _lock = DataDirLock::acquire(target)?;
        let directories = manifest
            .directories
            .iter()
            .map(|d| relative_path(d))
            .collect::<BackupResult<Vec<_>>>()?;
        for dir in &directories {
            let abs = target.join(dir);
            fs::create_dir_all(&abs).map_err(|e| BackupError::io(&abs, e))?;
        }
        manifest.files.par_iter().try_for_each(|entry| {
            let relative = relative_path(&entry.path)?;
            let actual = copy_hashed(&backup_dir.join(&relative), &target.join(&relative))?;
            check_entry(entry, actual)
        })?;
        sync_tree(target, &directories)
    })?;

    Ok(report(target, &manifest, start))
}

/// Canonical `data_dir`, refusing anything that is not a data directory.
///
/// Checked before locking because taking the lock creates the directory.
fn existing_data_dir(data_dir: &Path) -> BackupResult<PathBuf> {
    let data_dir = canonical(data_dir).map_err(|_| BackupError::NotADataDir(data_dir.into()))?;
    let looks_like_data_dir = ["metadata", "persist"]
        .iter()
        .any(|d| data_dir.join(d).is_dir());
    if !data_dir.is_dir() || !looks_like_data_dir {
        return Err(BackupError::NotADataDir(data_dir));
    }
    Ok(data_dir)
}

/// The backup holds exactly the manifest's entries, no more and no fewer.
fn check_listing(backup_dir: &Path, manifest: &Manifest) -> BackupResult<()> {
    let tree = tree::walk(backup_dir, &[MANIFEST_FILE_NAME, LOST_AND_FOUND])?;
    let found_dirs: BTreeSet<String> = tree::manifest_paths(&tree.directories)?
        .into_iter()
        .collect();
    let found_files: BTreeSet<String> = tree::manifest_paths(&tree.files)?.into_iter().collect();
    let listed_dirs: BTreeSet<&str> = manifest.directories.iter().map(String::as_str).collect();
    let listed_files: BTreeSet<&str> = manifest.files.iter().map(|f| f.path.as_str()).collect();

    for (found, listed) in [(&found_dirs, &listed_dirs), (&found_files, &listed_files)] {
        if let Some(missing) = listed.iter().find(|p| !found.contains(**p)) {
            return Err(corrupt(missing, "listed in the manifest but missing"));
        }
        if let Some(extra) = found.iter().find(|p| !listed.contains(p.as_str())) {
            return Err(corrupt(extra, "present but not listed in the manifest"));
        }
    }
    Ok(())
}

fn check_entry(entry: &FileEntry, (size, sha256): (u64, String)) -> BackupResult<()> {
    if size != entry.size {
        return Err(corrupt(
            &entry.path,
            &format!("size {size} bytes, manifest says {}", entry.size),
        ));
    }
    if sha256 != entry.sha256 {
        return Err(corrupt(&entry.path, "SHA-256 differs from the manifest"));
    }
    Ok(())
}

fn corrupt(path: &str, reason: &str) -> BackupError {
    BackupError::Corrupt {
        path: path.to_string(),
        reason: reason.to_string(),
    }
}

/// Fsync `root` and every directory under it, so new entries are durable.
fn sync_tree(root: &Path, directories: &[PathBuf]) -> BackupResult<()> {
    directories
        .par_iter()
        .try_for_each(|dir| sync_dir(&root.join(dir)))?;
    sync_dir(root)
}

pub(super) fn report(dir: PathBuf, manifest: &Manifest, start: Instant) -> Report {
    Report {
        dir,
        directories: manifest.directories.len(),
        files: manifest.files.len(),
        bytes: manifest.total_bytes(),
        elapsed: start.elapsed(),
    }
}
