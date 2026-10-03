//! The backup manifest: the completeness marker and integrity record.
//!
//! `create` writes the manifest last, after every listed file is copied and
//! synced, so a directory without one is an interrupted backup. Each file
//! carries its size and SHA-256; restore and verify reject any difference.
//!
//! Paths are stored relative and `/`-separated. A tampered manifest must not
//! steer restore outside its target, so every path is checked to be a plain
//! relative path before it is joined to anything.

use super::{BackupError, BackupResult};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Component, Path, PathBuf};

/// File name of the manifest inside a backup directory.
pub const MANIFEST_FILE_NAME: &str = "inputlayer-backup.json";

/// Identifies the manifest kind, so an unrelated JSON file is not mistaken
/// for a backup.
const FORMAT: &str = "inputlayer-offline-backup";

/// Bumped on any incompatible manifest change.
const FORMAT_VERSION: u32 = 1;

/// One regular file in the backup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    /// Path relative to the data directory, `/`-separated.
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// Lowercase hex SHA-256 of the contents.
    pub sha256: String,
}

/// Complete description of an offline backup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    format: String,
    format_version: u32,
    /// Version of the tool that wrote the backup.
    pub engine_version: String,
    /// RFC 3339 creation time.
    pub created_at: String,
    /// Data directory the backup was taken from.
    pub source: PathBuf,
    /// Every directory, relative and `/`-separated, parents before children.
    /// Empty directories (e.g. a new KG's `relations/`) are part of the state.
    pub directories: Vec<String>,
    /// Every regular file except the source's `LOCK`.
    pub files: Vec<FileEntry>,
}

impl Manifest {
    /// A manifest for a backup of `source` taken now.
    pub fn new(source: PathBuf, directories: Vec<String>, files: Vec<FileEntry>) -> Self {
        Self {
            format: FORMAT.to_string(),
            format_version: FORMAT_VERSION,
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            source,
            directories,
            files,
        }
    }

    /// Total bytes of file content.
    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }

    /// Read and validate the manifest of the backup at `backup_dir`.
    ///
    /// A missing manifest means the backup never finished.
    pub fn load(backup_dir: &Path) -> BackupResult<Self> {
        let path = backup_dir.join(MANIFEST_FILE_NAME);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(BackupError::Incomplete(backup_dir.to_path_buf()));
            }
            Err(e) => return Err(BackupError::io(&path, e)),
        };
        let manifest: Self = serde_json::from_slice(&bytes)
            .map_err(|e| BackupError::InvalidManifest(format!("{}: {e}", path.display())))?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Write the manifest into `backup_dir` durably: temp file, fsync,
    /// rename. Its appearance is what marks the backup complete.
    pub fn store(&self, backup_dir: &Path) -> BackupResult<()> {
        let json = serde_json::to_vec_pretty(self)
            .map_err(|e| BackupError::InvalidManifest(e.to_string()))?;
        let tmp = backup_dir.join(format!("{MANIFEST_FILE_NAME}.tmp"));
        let mut file = File::create(&tmp).map_err(|e| BackupError::io(&tmp, e))?;
        file.write_all(&json)
            .and_then(|()| file.sync_all())
            .map_err(|e| BackupError::io(&tmp, e))?;
        let path = backup_dir.join(MANIFEST_FILE_NAME);
        fs::rename(&tmp, &path).map_err(|e| BackupError::io(&path, e))
    }

    fn validate(&self) -> BackupResult<()> {
        if self.format != FORMAT {
            return Err(BackupError::InvalidManifest(format!(
                "not an InputLayer backup manifest (format '{}')",
                self.format
            )));
        }
        if self.format_version != FORMAT_VERSION {
            return Err(BackupError::InvalidManifest(format!(
                "unsupported backup format version {} (this tool reads {FORMAT_VERSION})",
                self.format_version
            )));
        }
        let paths = self
            .directories
            .iter()
            .chain(self.files.iter().map(|f| &f.path));
        for path in paths {
            relative_path(path)?;
        }
        Ok(())
    }
}

/// Convert a manifest path to a safe relative [`PathBuf`].
///
/// Rejects empty, absolute and `..` paths, and the reserved names, so a
/// hand-edited manifest cannot write outside the restore target or smuggle
/// in a `LOCK` file.
pub fn relative_path(path: &str) -> BackupResult<PathBuf> {
    let unsafe_path = || BackupError::InvalidManifest(format!("unsafe path '{path}'"));
    let relative = PathBuf::from(path);
    let plain = !path.is_empty()
        && relative
            .components()
            .all(|c| matches!(c, Component::Normal(_)));
    if !plain {
        return Err(unsafe_path());
    }
    if path == MANIFEST_FILE_NAME || path == crate::storage::data_dir_lock::LOCK_FILE_NAME {
        return Err(unsafe_path());
    }
    Ok(relative)
}

/// The manifest spelling of a relative path: `/`-separated UTF-8.
pub fn manifest_path(relative: &Path) -> BackupResult<String> {
    let parts: Option<Vec<&str>> = relative
        .components()
        .map(|c| c.as_os_str().to_str())
        .collect();
    parts
        .map(|p| p.join("/"))
        .ok_or_else(|| BackupError::UnsupportedEntry(relative.to_path_buf(), "non-UTF-8 name"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn plain_relative_paths_are_accepted() {
        assert_eq!(
            relative_path("persist/wal/current.wal").unwrap(),
            PathBuf::from("persist/wal/current.wal")
        );
    }

    #[test]
    fn escaping_and_reserved_paths_are_rejected() {
        for bad in [
            "",
            "/etc/passwd",
            "../outside",
            "persist/../../outside",
            "./persist",
            "LOCK",
            MANIFEST_FILE_NAME,
        ] {
            let err = relative_path(bad).unwrap_err();
            assert!(
                matches!(err, BackupError::InvalidManifest(_)),
                "{bad:?} gave {err:?}"
            );
        }
    }

    #[test]
    fn store_then_load_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let manifest = Manifest::new(
            PathBuf::from("/data"),
            vec!["persist".into()],
            vec![FileEntry {
                path: "persist/x".into(),
                size: 3,
                sha256: "ab".into(),
            }],
        );
        manifest.store(tmp.path()).unwrap();
        assert_eq!(Manifest::load(tmp.path()).unwrap(), manifest);
        assert!(!tmp
            .path()
            .join(format!("{MANIFEST_FILE_NAME}.tmp"))
            .exists());
    }

    #[test]
    fn missing_manifest_means_incomplete() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(matches!(
            Manifest::load(tmp.path()).unwrap_err(),
            BackupError::Incomplete(_)
        ));
    }

    #[test]
    fn foreign_format_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let mut manifest = Manifest::new(PathBuf::from("/data"), vec![], vec![]);
        manifest.format = "something-else".into();
        manifest.store(tmp.path()).unwrap();
        assert!(matches!(
            Manifest::load(tmp.path()).unwrap_err(),
            BackupError::InvalidManifest(_)
        ));
    }
}
