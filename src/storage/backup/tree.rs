//! Filesystem primitives for backup: walking a tree, hashed durable copies.

use super::manifest::manifest_path;
use super::{BackupError, BackupResult};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Copy buffer size; large enough that syscalls do not dominate.
const COPY_BUF_BYTES: usize = 1 << 20;

/// Every entry under a root, as relative paths.
#[derive(Debug, Default)]
pub struct Tree {
    /// Directories, parents before children.
    pub directories: Vec<PathBuf>,
    /// Regular files.
    pub files: Vec<PathBuf>,
}

/// Walk `root`, skipping the top-level names in `skip`.
///
/// Symlinks and special files are refused rather than followed: a data
/// directory never contains them, and following one could copy, or on
/// restore write, outside the tree.
pub fn walk(root: &Path, skip: &[&str]) -> BackupResult<Tree> {
    let mut tree = Tree::default();
    let mut pending = vec![PathBuf::new()];
    while let Some(dir) = pending.pop() {
        let abs = root.join(&dir);
        let mut entries = fs::read_dir(&abs)
            .and_then(|it| it.collect::<Result<Vec<_>, _>>())
            .map_err(|e| BackupError::io(&abs, e))?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let name = entry.file_name();
            if dir.as_os_str().is_empty() && skip.iter().any(|s| name == **s) {
                continue;
            }
            let relative = dir.join(&name);
            let kind = entry
                .file_type()
                .map_err(|e| BackupError::io(&entry.path(), e))?;
            if kind.is_dir() {
                tree.directories.push(relative.clone());
                pending.push(relative);
            } else if kind.is_file() {
                tree.files.push(relative);
            } else {
                let what = if kind.is_symlink() {
                    "symlink"
                } else {
                    "special file"
                };
                return Err(BackupError::UnsupportedEntry(entry.path(), what));
            }
        }
    }
    tree.directories.sort();
    tree.files.sort();
    Ok(tree)
}

/// Manifest spellings of `paths`.
pub fn manifest_paths(paths: &[PathBuf]) -> BackupResult<Vec<String>> {
    paths.iter().map(|p| manifest_path(p)).collect()
}

/// Copy `from` to the new file `to`, returning its size and SHA-256 hex.
///
/// `to` must not exist. The copy keeps the source's permissions and is
/// fsynced before returning.
pub fn copy_hashed(from: &Path, to: &Path) -> BackupResult<(u64, String)> {
    let mut src = File::open(from).map_err(|e| BackupError::io(from, e))?;
    let mut dst = File::options()
        .write(true)
        .create_new(true)
        .open(to)
        .map_err(|e| BackupError::io(to, e))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; COPY_BUF_BYTES];
    let mut size = 0u64;
    loop {
        let n = src.read(&mut buf).map_err(|e| BackupError::io(from, e))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        dst.write_all(&buf[..n])
            .map_err(|e| BackupError::io(to, e))?;
        size += n as u64;
    }
    let permissions = src
        .metadata()
        .map_err(|e| BackupError::io(from, e))?
        .permissions();
    dst.set_permissions(permissions)
        .and_then(|()| dst.sync_all())
        .map_err(|e| BackupError::io(to, e))?;
    Ok((size, format!("{:x}", hasher.finalize())))
}

/// Size and SHA-256 hex of `path`, read in place.
pub fn hash_file(path: &Path) -> BackupResult<(u64, String)> {
    let mut file = File::open(path).map_err(|e| BackupError::io(path, e))?;
    hash_open(&mut file, path)
}

/// Size and SHA-256 hex of `path`, after making its contents durable.
pub fn seal_file(path: &Path) -> BackupResult<(u64, String)> {
    let mut file = File::open(path).map_err(|e| BackupError::io(path, e))?;
    let digest = hash_open(&mut file, path)?;
    file.sync_all().map_err(|e| BackupError::io(path, e))?;
    Ok(digest)
}

fn hash_open(file: &mut File, path: &Path) -> BackupResult<(u64, String)> {
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; COPY_BUF_BYTES];
    let mut size = 0u64;
    loop {
        let n = file.read(&mut buf).map_err(|e| BackupError::io(path, e))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size += n as u64;
    }
    Ok((size, format!("{:x}", hasher.finalize())))
}

/// Fsync `dir` so the entries created in it survive a crash.
pub fn sync_dir(dir: &Path) -> BackupResult<()> {
    // Windows cannot open a directory as a file; NTFS metadata is journaled.
    if cfg!(windows) {
        return Ok(());
    }
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| BackupError::io(dir, e))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn walk_lists_dirs_and_files_and_skips_top_level_names() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::create_dir_all(root.join("a/empty")).unwrap();
        fs::write(root.join("a/f"), b"x").unwrap();
        fs::write(root.join("LOCK"), b"1").unwrap();
        fs::create_dir(root.join("b")).unwrap();
        fs::write(root.join("b/LOCK"), b"nested is kept").unwrap();

        let tree = walk(root, &["LOCK"]).unwrap();

        assert_eq!(
            tree.directories,
            [PathBuf::from("a"), "a/empty".into(), "b".into()]
        );
        assert_eq!(tree.files, [PathBuf::from("a/f"), "b/LOCK".into()]);
    }

    #[cfg(unix)]
    #[test]
    fn walk_refuses_symlinks() {
        let tmp = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink("/etc", tmp.path().join("escape")).unwrap();
        assert!(matches!(
            walk(tmp.path(), &[]).unwrap_err(),
            BackupError::UnsupportedEntry(_, "symlink")
        ));
    }

    #[test]
    fn copy_hashed_matches_hash_file_and_refuses_existing_target() {
        let tmp = tempfile::tempdir().unwrap();
        let from = tmp.path().join("from");
        let to = tmp.path().join("to");
        fs::write(&from, b"hello").unwrap();

        let copied = copy_hashed(&from, &to).unwrap();

        assert_eq!(copied, hash_file(&to).unwrap());
        assert_eq!(copied.0, 5);
        assert_eq!(
            copied.1,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert!(copy_hashed(&from, &to).is_err());
        assert_eq!(fs::read(&to).unwrap(), b"hello");
    }
}
