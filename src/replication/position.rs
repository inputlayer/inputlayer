//! A follower's durable stream position.
//!
//! Saved after each applied batch as `replication/follower.json` in the data
//! directory, replaced atomically. Applying an event is idempotent, so a
//! crash between applying events and saving the position only replays them.

use crate::storage::{StorageError, StorageResult};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Where a follower is in its primary's stream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Position {
    /// The primary incarnation the LSN belongs to; 0 for none.
    pub stream_id: u64,
    /// LSN of the last applied event.
    pub lsn: u64,
    /// The newest primary revision the follower's state includes.
    pub primary_revision: u64,
}

/// The position file inside data directory `data_dir`.
pub fn position_path(data_dir: &Path) -> PathBuf {
    data_dir.join("replication").join("follower.json")
}

impl Position {
    /// The saved position, or none (a fresh follower) when there is no file.
    ///
    /// # Errors
    /// The file exists but cannot be read or parsed.
    pub fn load(path: &Path) -> StorageResult<Self> {
        match fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                StorageError::Other(format!(
                    "replication position {} is unreadable ({e}); remove it to resync \
                     this follower from the primary",
                    path.display()
                ))
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Replace the file with this position: write a temporary file and
    /// rename it over the old one; with `durable`, fsync the file before and
    /// the directory after the rename.
    ///
    /// Without `durable` a crash may leave the old position (or, at worst,
    /// an unreadable file, which the follower answers with a resync). Both
    /// only make the follower apply events again, which is idempotent, as
    /// long as the changes up to this position are already durable.
    ///
    /// # Errors
    /// Any I/O failure; the old file is then unchanged.
    pub fn save(&self, path: &Path, durable: bool) -> StorageResult<()> {
        let dir = path
            .parent()
            .ok_or_else(|| StorageError::Other(format!("{} has no parent", path.display())))?;
        fs::create_dir_all(dir)?;
        let tmp = path.with_extension("json.tmp");
        let mut file = fs::File::create(&tmp)?;
        file.write_all(&serde_json::to_vec(self).map_err(|e| StorageError::Other(e.to_string()))?)?;
        if durable {
            file.sync_all()?;
        }
        fs::rename(&tmp, path)?;
        if durable {
            fs::File::open(dir)?.sync_all()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_saved_position_loads_back_and_a_missing_one_is_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = position_path(dir.path());
        assert_eq!(Position::load(&path).unwrap(), Position::default());
        let position = Position {
            stream_id: 9,
            lsn: 42,
            primary_revision: 17,
        };
        position.save(&path, true).unwrap();
        assert_eq!(Position::load(&path).unwrap(), position);
        let later = Position {
            lsn: 43,
            ..position
        };
        later.save(&path, false).unwrap();
        assert_eq!(Position::load(&path).unwrap(), later);
    }

    #[test]
    fn a_damaged_position_is_an_error_not_a_fresh_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = position_path(dir.path());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"{not json").unwrap();
        assert!(Position::load(&path).is_err());
    }
}
