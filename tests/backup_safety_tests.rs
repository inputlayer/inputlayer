//! Offline backup refusals: live sources, existing or nested destinations,
//! incomplete, corrupt and tampered backups. Every refusal leaves existing
//! data untouched and creates nothing.

// Test setup aborts on failure; `unwrap` is the intended behavior.
#![allow(clippy::unwrap_used)]

use inputlayer::storage::backup::{self, BackupError, Manifest, MANIFEST_FILE_NAME};
use inputlayer::value::Tuple;
use inputlayer::{Config, StorageEngine};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn engine(dir: &Path) -> StorageEngine {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    config.storage.performance.num_threads = 1;
    StorageEngine::new(config).unwrap()
}

/// A stopped data directory with a few facts, and its backup.
struct Fixture {
    temp: TempDir,
    data: PathBuf,
    backup: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let data = temp.path().join("data");
        let facts = (0..10).map(|i| Tuple::from_pair(i, i + 1)).collect();
        engine(&data)
            .insert_tuples_into("default", "edge", facts)
            .unwrap();
        let backup = temp.path().join("backup");
        backup::create(&data, &backup).unwrap();
        Self { temp, data, backup }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.temp.path().join(name)
    }

    /// A file in the backup that holds facts.
    fn data_file(&self) -> PathBuf {
        self.backup.join("persist/wal/current.wal")
    }
}

/// `restore` into a new path fails with `expected` and creates nothing.
fn assert_restore_refused(f: &Fixture, expected: fn(&BackupError) -> bool) {
    let target = f.path("restored");
    let err = backup::restore(&f.backup, &target).unwrap_err();
    assert!(expected(&err), "unexpected error: {err}");
    assert!(
        !target.exists(),
        "refused restore left {}",
        target.display()
    );
}

#[test]
fn live_source_is_refused_and_nothing_is_written() {
    let f = Fixture::new();
    let _running = engine(&f.data);
    let dest = f.path("second");

    let err = backup::create(&f.data, &dest).unwrap_err();

    assert!(matches!(err, BackupError::SourceInUse { .. }), "{err}");
    assert!(err.to_string().contains("stop the server"), "{err}");
    assert!(!dest.exists());
}

#[test]
fn existing_destination_is_never_overwritten() {
    let f = Fixture::new();
    let occupied = f.path("occupied");
    fs::create_dir(&occupied).unwrap();
    fs::write(occupied.join("keep"), b"precious").unwrap();

    let create = backup::create(&f.data, &occupied).unwrap_err();
    let restore = backup::restore(&f.backup, &occupied).unwrap_err();

    for err in [create, restore] {
        assert!(matches!(err, BackupError::DestinationExists(_)), "{err}");
    }
    assert_eq!(fs::read(occupied.join("keep")).unwrap(), b"precious");
    assert_eq!(fs::read_dir(&occupied).unwrap().count(), 1);
}

#[test]
fn destination_inside_the_source_is_refused_before_creating_anything() {
    let f = Fixture::new();
    let before: Vec<_> = fs::read_dir(&f.data)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();

    let err = backup::create(&f.data, &f.data.join("nested/backup")).unwrap_err();
    assert!(
        matches!(err, BackupError::DestinationInsideSource { .. }),
        "{err}"
    );
    let err = backup::restore(&f.backup, &f.backup.join("nested/restored")).unwrap_err();
    assert!(
        matches!(err, BackupError::DestinationInsideSource { .. }),
        "{err}"
    );

    let after: Vec<_> = fs::read_dir(&f.data)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    assert_eq!(before, after);
    assert!(!f.backup.join("nested").exists());
}

#[test]
fn non_data_directory_is_refused_and_not_created() {
    let f = Fixture::new();
    let missing = f.path("no-such-dir");
    let empty = f.path("empty");
    fs::create_dir(&empty).unwrap();

    for source in [&missing, &empty] {
        let err = backup::create(source, &f.path("out")).unwrap_err();
        assert!(matches!(err, BackupError::NotADataDir(_)), "{err}");
    }
    assert!(!missing.exists());
    assert!(!empty.join("LOCK").exists());
    assert!(!f.path("out").exists());
}

#[test]
fn backup_without_manifest_is_incomplete() {
    let f = Fixture::new();
    fs::remove_file(f.backup.join(MANIFEST_FILE_NAME)).unwrap();

    assert!(matches!(
        backup::verify(&f.backup).unwrap_err(),
        BackupError::Incomplete(_)
    ));
    assert_restore_refused(&f, |e| matches!(e, BackupError::Incomplete(_)));
}

#[test]
fn flipped_byte_is_detected_by_verify_and_restore() {
    let f = Fixture::new();
    let mut bytes = fs::read(f.data_file()).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xff;
    fs::write(f.data_file(), bytes).unwrap();

    let corrupt = |e: &BackupError| {
        matches!(e, BackupError::Corrupt { path, reason }
            if path == "persist/wal/current.wal" && reason.contains("SHA-256"))
    };
    assert!(corrupt(&backup::verify(&f.backup).unwrap_err()));
    assert_restore_refused(&f, corrupt);
}

#[test]
fn truncated_file_is_detected() {
    let f = Fixture::new();
    let len = fs::metadata(f.data_file()).unwrap().len();
    fs::File::options()
        .write(true)
        .open(f.data_file())
        .unwrap()
        .set_len(len - 1)
        .unwrap();

    assert_restore_refused(
        &f,
        |e| matches!(e, BackupError::Corrupt { reason, .. } if reason.contains("size")),
    );
}

#[test]
fn missing_and_extra_files_are_detected() {
    let f = Fixture::new();
    fs::write(f.backup.join("persist/stray"), b"x").unwrap();
    assert_restore_refused(
        &f,
        |e| matches!(e, BackupError::Corrupt { path, .. } if path == "persist/stray"),
    );

    fs::remove_file(f.backup.join("persist/stray")).unwrap();
    fs::remove_file(f.data_file()).unwrap();
    assert_restore_refused(
        &f,
        |e| matches!(e, BackupError::Corrupt { reason, .. } if reason.contains("missing")),
    );
}

#[test]
fn manifest_path_escaping_the_target_is_refused() {
    let f = Fixture::new();
    let manifest_path = f.backup.join(MANIFEST_FILE_NAME);
    let manifest = fs::read_to_string(&manifest_path).unwrap();
    let tampered = manifest.replace("\"persist/wal/current.wal\"", "\"../escaped\"");
    assert_ne!(manifest, tampered);
    fs::write(&manifest_path, tampered).unwrap();

    assert_restore_refused(&f, |e| matches!(e, BackupError::InvalidManifest(_)));
    assert!(!f.path("escaped").exists());
}

#[test]
fn backup_restores_into_a_directory_the_engine_opens() {
    let f = Fixture::new();
    let restored = f.path("restored");

    backup::restore(&f.backup, &restored).unwrap();

    let snapshot = engine(&restored).get_snapshot_for("default").unwrap();
    assert_eq!(snapshot.input_tuples.get("edge").map(|t| t.len()), Some(10));
}

#[test]
fn volume_mount_points_work_as_source_and_target() {
    // A fresh ext4 volume (e.g. a Kubernetes PVC) holds a root-owned
    // `lost+found`: it is neither backed up nor in the way of a restore.
    let f = Fixture::new();
    fs::create_dir(f.data.join("lost+found")).unwrap();
    let backup_dir = f.path("volume-backup");
    fs::create_dir_all(backup_dir.join("lost+found")).unwrap();
    let mount = f.path("volume");
    fs::create_dir_all(mount.join("lost+found")).unwrap();

    backup::create(&f.data, &backup_dir).unwrap();
    backup::verify(&backup_dir).unwrap();
    backup::restore(&backup_dir, &mount).unwrap();

    let manifest = Manifest::load(&backup_dir).unwrap();
    assert!(!manifest.directories.iter().any(|d| d == "lost+found"));
    assert!(mount.join("lost+found").is_dir());
    let snapshot = engine(&mount).get_snapshot_for("default").unwrap();
    assert_eq!(snapshot.input_tuples.get("edge").map(|t| t.len()), Some(10));
}

#[test]
fn failed_restore_into_an_empty_mount_point_leaves_it_empty() {
    let f = Fixture::new();
    fs::write(f.backup.join("persist/stray"), b"x").unwrap();
    let mount = f.path("volume");
    fs::create_dir(&mount).unwrap();

    backup::restore(&f.backup, &mount).unwrap_err();
    fs::remove_file(f.backup.join("persist/stray")).unwrap();
    // Corrupt the last file copied, so the failure comes after the copy started.
    let last = Manifest::load(&f.backup)
        .unwrap()
        .files
        .last()
        .unwrap()
        .path
        .clone();
    fs::write(f.backup.join(&last), b"tampered").unwrap();
    let err = backup::restore(&f.backup, &mount).unwrap_err();

    assert!(matches!(err, BackupError::Corrupt { .. }), "{err}");
    assert!(mount.is_dir());
    assert_eq!(fs::read_dir(&mount).unwrap().count(), 0);
}
