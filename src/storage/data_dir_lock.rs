//! Exclusive single-writer ownership of a data directory.
//!
//! Two engines writing one `data_dir` (for example a Kubernetes rolling
//! update on a `ReadWriteOnce` volume) interleave WAL appends and shard
//! rewrites and corrupt both. [`DataDirLock`] takes an OS advisory lock
//! (`flock` on Unix, `LockFileEx` on Windows) on `data_dir/LOCK` before
//! anything else touches the directory, and holds it for its own lifetime.
//!
//! The OS releases the lock when the owning file handle closes, which
//! includes the process exiting or crashing, so a leftover `LOCK` file never
//! blocks a restart. Drop also unlocks explicitly: a child process spawned
//! concurrently shares the open file until its `exec`, and closing our
//! handle alone would leave the lock held for that window. The pid written
//! into the file is diagnostic only; the OS lock is the sole authority.

use super::{StorageError, StorageResult};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;

/// Name of the lock file inside the data directory.
pub const LOCK_FILE_NAME: &str = "LOCK";

/// Held exclusive lock on a data directory, released on drop.
#[derive(Debug)]
pub struct DataDirLock {
    /// Open handle that owns the OS lock; dropping the struct releases it.
    file: File,
}

impl DataDirLock {
    /// Lock `data_dir`, creating it if missing.
    ///
    /// Fails with [`StorageError::DataDirLocked`] when another handle (in
    /// this or any other process) holds the lock. That path leaves the
    /// directory exactly as it found it.
    pub fn acquire(data_dir: &Path) -> StorageResult<Self> {
        fs::create_dir_all(data_dir)?;
        let path = data_dir.join(LOCK_FILE_NAME);
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        // Fully qualified: on toolchains >= 1.89 the inherent
        // `File::try_lock` would otherwise shadow the trait method.
        match fs4::FileExt::try_lock(&file) {
            Ok(()) => {}
            Err(fs4::TryLockError::WouldBlock) => {
                return Err(StorageError::DataDirLocked {
                    dir: data_dir.to_path_buf(),
                    owner: recorded_owner(&path),
                });
            }
            Err(fs4::TryLockError::Error(e)) => return Err(e.into()),
        }

        file.set_len(0)?;
        writeln!(file, "{}", std::process::id())?;
        Ok(Self { file })
    }
}

impl Drop for DataDirLock {
    fn drop(&mut self) {
        // Releases the lock on the shared open file, not just our handle.
        let _ = fs4::FileExt::unlock(&self.file);
    }
}

/// The pid the current owner wrote into the lock file, for the error message.
fn recorded_owner(path: &Path) -> String {
    fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn second_lock_on_same_dir_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let _held = DataDirLock::acquire(tmp.path()).unwrap();

        let err = DataDirLock::acquire(tmp.path()).unwrap_err();
        let StorageError::DataDirLocked { dir, owner } = &err else {
            panic!("expected DataDirLocked, got {err:?}");
        };
        assert_eq!(dir, tmp.path());
        assert_eq!(owner, &std::process::id().to_string());
        assert!(err.to_string().contains("storage.data_dir"));
    }

    #[test]
    fn refused_lock_leaves_lock_file_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let _held = DataDirLock::acquire(tmp.path()).unwrap();
        let before = fs::read(tmp.path().join(LOCK_FILE_NAME)).unwrap();

        DataDirLock::acquire(tmp.path()).unwrap_err();

        assert_eq!(fs::read(tmp.path().join(LOCK_FILE_NAME)).unwrap(), before);
    }

    #[test]
    fn drop_releases_lock() {
        let tmp = tempfile::tempdir().unwrap();
        drop(DataDirLock::acquire(tmp.path()).unwrap());
        DataDirLock::acquire(tmp.path()).unwrap();
    }

    /// Children spawned by other threads briefly share every open file.
    #[test]
    fn drop_releases_lock_while_processes_spawn() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let stop = Arc::new(AtomicBool::new(false));
        let spawner = std::thread::spawn({
            let stop = Arc::clone(&stop);
            move || {
                while !stop.load(Ordering::Relaxed) {
                    let _ = std::process::Command::new("true").status();
                }
            }
        });
        let tmp = tempfile::tempdir().unwrap();
        let refused = (0..2000)
            .filter(|_| DataDirLock::acquire(tmp.path()).is_err())
            .count();
        stop.store(true, Ordering::Relaxed);
        spawner.join().unwrap();
        assert_eq!(refused, 0);
    }

    #[test]
    fn stale_lock_file_does_not_block() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join(LOCK_FILE_NAME), "999999\n").unwrap();
        DataDirLock::acquire(tmp.path()).unwrap();
    }

    #[test]
    fn distinct_dirs_lock_independently() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let _a = DataDirLock::acquire(a.path()).unwrap();
        DataDirLock::acquire(b.path()).unwrap();
    }

    #[test]
    fn creates_missing_data_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("nested").join("data");
        DataDirLock::acquire(&dir).unwrap();
        assert!(dir.join(LOCK_FILE_NAME).is_file());
    }
}
