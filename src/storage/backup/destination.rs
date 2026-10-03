//! The directory an operation writes into: claimed only when it holds no
//! data, and handed back as it was found when the operation fails.

use super::{BackupError, BackupResult};
use std::fs;
use std::path::{Path, PathBuf};

/// Created by `mkfs` at the root of a fresh ext4 volume (e.g. a new
/// Kubernetes PVC) and owned by root; never engine or backup state.
pub const LOST_AND_FOUND: &str = "lost+found";

/// A claimed destination directory.
#[derive(Debug)]
pub struct Destination {
    path: PathBuf,
    /// Whether this operation created the directory (and so may remove it).
    created: bool,
}

impl Destination {
    /// Claim `dest`, which must lie outside `protected`: either a new
    /// directory, or an existing empty one such as a mounted volume. An
    /// existing directory holding anything but `lost+found` is refused, so
    /// no data is ever overwritten.
    ///
    /// Missing parents are created only after the containment check, so a
    /// refused destination creates nothing. A new directory is made with an
    /// exclusive `create_dir`; one that appears concurrently is not adopted.
    pub fn claim(dest: &Path, protected: &Path) -> BackupResult<Self> {
        let existing = dest.symlink_metadata().is_ok();
        let path = if existing {
            canonical(dest)?
        } else {
            resolve_new_path(dest)?
        };
        if path.starts_with(protected) {
            return Err(BackupError::DestinationInsideSource {
                dest: path,
                inside: protected.into(),
            });
        }
        if existing {
            let is_dir = dest.symlink_metadata().is_ok_and(|m| m.is_dir());
            if !is_dir || !is_empty(&path)? {
                return Err(BackupError::DestinationExists(path));
            }
            return Ok(Self {
                path,
                created: false,
            });
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| BackupError::io(parent, e))?;
        }
        match fs::create_dir(&path) {
            Ok(()) => Ok(Self {
                path,
                created: true,
            }),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(BackupError::DestinationExists(path))
            }
            Err(e) => Err(BackupError::io(&path, e)),
        }
    }

    /// The claimed directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Run `work` on the destination and return its path. If `work` fails,
    /// a directory this claim created is removed and an existing one is
    /// emptied again, before the error is returned.
    pub fn fill<T>(
        self,
        work: impl FnOnce(&Path) -> BackupResult<T>,
    ) -> BackupResult<(PathBuf, T)> {
        match work(&self.path) {
            Ok(value) => Ok((self.path, value)),
            Err(cause) => match self.reset() {
                Ok(()) => Err(cause),
                Err(cleanup) => Err(BackupError::CleanupFailed {
                    cause: Box::new(cause),
                    dir: self.path,
                    cleanup,
                }),
            },
        }
    }

    /// Give the directory back unused, as [`Self::fill`] does on failure.
    pub fn release(self) -> std::io::Result<()> {
        self.reset()
    }

    fn reset(&self) -> std::io::Result<()> {
        if self.created {
            return fs::remove_dir_all(&self.path);
        }
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            if entry.file_name() == LOST_AND_FOUND {
                continue;
            }
            if entry.file_type()?.is_dir() {
                fs::remove_dir_all(entry.path())?;
            } else {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }
}

/// `dir` holds nothing but, possibly, `lost+found`.
fn is_empty(dir: &Path) -> BackupResult<bool> {
    let mut entries = fs::read_dir(dir).map_err(|e| BackupError::io(dir, e))?;
    entries.try_fold(true, |empty, entry| {
        let entry = entry.map_err(|e| BackupError::io(dir, e))?;
        Ok(empty && entry.file_name() == LOST_AND_FOUND)
    })
}

/// Absolute form of the not-yet-existing `path`: its nearest existing
/// ancestor, canonicalized, followed by the missing components, which must
/// be plain names.
fn resolve_new_path(path: &Path) -> BackupResult<PathBuf> {
    let invalid = || BackupError::InvalidDestination(path.into());
    let absolute = std::path::absolute(path).map_err(|e| BackupError::io(path, e))?;
    let mut missing = Vec::new();
    let mut existing = absolute.as_path();
    while existing.symlink_metadata().is_err() {
        missing.push(existing.file_name().ok_or_else(invalid)?);
        existing = existing.parent().ok_or_else(invalid)?;
    }
    let mut resolved = canonical(existing)?;
    resolved.extend(missing.iter().rev());
    Ok(resolved)
}

/// `path` with symlinks and `..` resolved; it must exist.
pub fn canonical(path: &Path) -> BackupResult<PathBuf> {
    path.canonicalize().map_err(|e| BackupError::io(path, e))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn failing(_: &Path) -> BackupResult<()> {
        Err(BackupError::InvalidManifest("boom".into()))
    }

    #[test]
    fn new_directory_is_removed_on_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("a/b");
        let claimed = Destination::claim(&dest, Path::new("/nonexistent")).unwrap();
        claimed
            .fill(|d| {
                fs::write(d.join("f"), b"x").unwrap();
                failing(d)
            })
            .unwrap_err();
        assert!(!dest.exists());
    }

    #[test]
    fn existing_empty_directory_is_emptied_not_removed_on_failure() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join(LOST_AND_FOUND)).unwrap();
        let claimed = Destination::claim(tmp.path(), Path::new("/nonexistent")).unwrap();
        claimed
            .fill(|d| {
                fs::create_dir(d.join("sub")).unwrap();
                fs::write(d.join("f"), b"x").unwrap();
                failing(d)
            })
            .unwrap_err();
        let left: Vec<_> = fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(left, [LOST_AND_FOUND]);
    }

    #[test]
    fn non_empty_directory_and_files_are_refused() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("f"), b"x").unwrap();
        for dest in [tmp.path().to_path_buf(), tmp.path().join("f")] {
            let err = Destination::claim(&dest, Path::new("/nonexistent")).unwrap_err();
            assert!(matches!(err, BackupError::DestinationExists(_)), "{err}");
        }
    }

    #[test]
    fn dot_dot_through_a_missing_directory_is_invalid() {
        let tmp = tempfile::tempdir().unwrap();
        let err = Destination::claim(&tmp.path().join("missing/.."), Path::new("/x")).unwrap_err();
        assert!(matches!(err, BackupError::InvalidDestination(_)), "{err}");
    }
}
