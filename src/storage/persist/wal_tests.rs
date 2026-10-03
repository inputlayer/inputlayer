#![allow(clippy::unwrap_used)]

use super::*;
use crate::value::Tuple;
use tempfile::TempDir;

fn open(dir: &Path) -> PersistWal {
    PersistWal::open(dir.to_path_buf()).unwrap().0
}

fn recovered(dir: &Path) -> Vec<Transaction> {
    PersistWal::open(dir.to_path_buf()).unwrap().1
}

fn txn(rev: u64, shards: &[&str]) -> Transaction {
    let mut txn = Transaction::new(rev);
    for (i, shard) in shards.iter().enumerate() {
        txn.insert(*shard, [Tuple::from_pair(rev as i32, i as i32)]);
    }
    txn
}

fn corrupt_files(dir: &Path) -> Vec<PathBuf> {
    fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "corrupt"))
        .collect()
}

#[test]
fn append_then_read_in_commit_order() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    let txns = [txn(1, &["db:a", "db:b"]), txn(2, &["db:a"])];
    for t in &txns {
        wal.append(t, true).unwrap();
    }
    assert_eq!(wal.read_all().unwrap(), txns);
}

#[test]
fn durable_append_survives_without_drop() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a", "db:b"]), true).unwrap();
    std::mem::forget(wal);
    assert_eq!(recovered(temp.path()), [txn(1, &["db:a", "db:b"])]);
}

#[test]
fn buffered_append_is_durable_after_sync() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a"]), false).unwrap();
    wal.sync().unwrap();
    std::mem::forget(wal);
    assert_eq!(recovered(temp.path()), [txn(1, &["db:a"])]);
}

#[test]
fn sync_with_no_writer_is_a_no_op() {
    let temp = TempDir::new().unwrap();
    open(temp.path()).sync().unwrap();
}

#[test]
fn clear_then_append() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a"]), true).unwrap();
    wal.clear().unwrap();
    assert_eq!(wal.file_size(), 0);
    wal.append(&txn(2, &["db:a"]), true).unwrap();
    assert_eq!(wal.read_all().unwrap(), [txn(2, &["db:a"])]);
}

#[test]
fn remove_shard_keeps_other_shards_of_the_same_transaction() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a", "db:b"]), true).unwrap();
    wal.append(&txn(2, &["db:a"]), true).unwrap();
    wal.append(&txn(3, &["db:b"]), true).unwrap();

    wal.remove_shard_entries("db:a").unwrap();

    let mut first = txn(1, &["db:a", "db:b"]);
    first.remove_shard("db:a");
    assert_eq!(wal.read_all().unwrap(), [first, txn(3, &["db:b"])]);
    assert!(!temp.path().join("current.wal.new").exists());

    wal.append(&txn(4, &["db:c"]), true).unwrap();
    assert_eq!(wal.read_all().unwrap().len(), 3);
}

#[test]
fn remove_last_shard_removes_the_file() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a"]), true).unwrap();
    wal.remove_shard_entries("db:a").unwrap();
    assert!(wal.read_all().unwrap().is_empty());
    assert!(!temp.path().join("current.wal").exists());
}

#[test]
fn remove_unknown_shard_leaves_the_file_alone() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a"]), true).unwrap();
    let before = fs::read(temp.path().join("current.wal")).unwrap();
    wal.remove_shard_entries("db:zzz").unwrap();
    assert_eq!(fs::read(temp.path().join("current.wal")).unwrap(), before);
}

#[test]
fn cleanup_removes_stale_files_but_keeps_quarantined_bytes() {
    let temp = TempDir::new().unwrap();
    let wal = open(temp.path());
    fs::write(temp.path().join("wal_1.archived"), "stale").unwrap();
    fs::write(temp.path().join("current.wal.new"), "incomplete").unwrap();
    fs::write(temp.path().join("current.wal.1.corrupt"), "evidence").unwrap();

    wal.cleanup_archives().unwrap();

    assert!(!temp.path().join("wal_1.archived").exists());
    assert!(!temp.path().join("current.wal.new").exists());
    assert!(temp.path().join("current.wal.1.corrupt").exists());
}

#[test]
fn open_cuts_a_torn_tail_and_appends_after_it() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a"]), true).unwrap();
    let valid_len = wal.file_size();
    drop(wal);
    let path = temp.path().join("current.wal");
    let mut f = OpenOptions::new().append(true).open(&path).unwrap();
    f.write_all(b"00000000:{\"rev\":2,\"op\xE2\x82").unwrap();
    drop(f);

    let mut wal = open(temp.path());
    assert_eq!(wal.file_size(), valid_len);
    assert!(corrupt_files(temp.path()).is_empty());
    wal.append(&txn(3, &["db:a"]), true).unwrap();
    assert_eq!(
        wal.read_all().unwrap(),
        [txn(1, &["db:a"]), txn(3, &["db:a"])]
    );
}

#[test]
fn record_without_its_newline_is_not_committed() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a"]), true).unwrap();
    wal.append(&txn(2, &["db:a"]), true).unwrap();
    drop(wal);
    let path = temp.path().join("current.wal");
    let mut bytes = fs::read(&path).unwrap();
    assert_eq!(bytes.pop(), Some(b'\n'));
    fs::write(&path, &bytes).unwrap();

    assert_eq!(recovered(temp.path()), [txn(1, &["db:a"])]);
}

#[test]
fn damage_before_the_end_recovers_the_prefix_and_quarantines_the_rest() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    for rev in 1..=3 {
        wal.append(&txn(rev, &["db:a", "db:b"]), true).unwrap();
    }
    drop(wal);
    let path = temp.path().join("current.wal");
    let bytes = fs::read(&path).unwrap();
    let first_end = bytes.iter().position(|&b| b == b'\n').unwrap() + 1;
    let mut damaged = bytes.clone();
    damaged[first_end + 20] ^= 0x01;
    fs::write(&path, &damaged).unwrap();

    assert_eq!(recovered(temp.path()), [txn(1, &["db:a", "db:b"])]);
    assert_eq!(fs::read(&path).unwrap(), &bytes[..first_end]);
    let saved = corrupt_files(temp.path());
    assert_eq!(saved.len(), 1);
    assert_eq!(fs::read(&saved[0]).unwrap(), &damaged[first_end..]);
}

#[test]
fn failed_write_is_cut_back_and_never_recovered() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a"]), true).unwrap();
    let valid_len = wal.file_size();

    wal.inject_fault(WalFault::Write);
    assert!(wal.append(&txn(2, &["db:a", "db:b"]), true).is_err());
    assert_eq!(wal.file_size(), valid_len);

    wal.append(&txn(3, &["db:b"]), true).unwrap();
    let expected = [txn(1, &["db:a"]), txn(3, &["db:b"])];
    assert_eq!(wal.read_all().unwrap(), expected);
    std::mem::forget(wal);
    assert_eq!(recovered(temp.path()), expected);
}

#[test]
fn failed_sync_is_cut_back_and_never_recovered() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a"]), true).unwrap();

    wal.inject_fault(WalFault::Sync);
    assert!(wal.append(&txn(2, &["db:a"]), true).is_err());
    std::mem::forget(wal);
    assert_eq!(recovered(temp.path()), [txn(1, &["db:a"])]);
}

#[test]
fn failed_cut_back_is_repaired_to_the_exact_length_before_the_next_write() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a"]), true).unwrap();

    // The whole record reaches the file and cannot be cut off: an intact record
    // whose commit failed. The repair must not keep it just because it is intact.
    wal.inject_fault(WalFault::Sync);
    wal.inject_fault(WalFault::Restore);
    assert!(wal.append(&txn(2, &["db:a"]), true).is_err());
    assert_eq!(wal.repair, Some(Repair::ToLength(wal.len)));

    wal.append(&txn(3, &["db:a"]), true).unwrap();
    let expected = [txn(1, &["db:a"]), txn(3, &["db:a"])];
    assert_eq!(wal.read_all().unwrap(), expected);
    std::mem::forget(wal);
    assert_eq!(recovered(temp.path()), expected);
}

/// Known limit of the rollback: the pending repair lives in memory only. A crash
/// after a failed commit whose cut-back also failed, before the next write or a
/// clean shutdown repairs the file, recovers that failed transaction.
#[test]
fn crash_before_a_failed_cut_back_is_repaired_recovers_the_failed_record() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a"]), true).unwrap();
    wal.inject_fault(WalFault::Sync);
    wal.inject_fault(WalFault::Restore);
    assert!(wal.append(&txn(2, &["db:a"]), true).is_err());

    // Crash: no further write and no drop, so the repair never runs.
    std::mem::forget(wal);
    assert_eq!(
        recovered(temp.path()),
        [txn(1, &["db:a"]), txn(2, &["db:a"])]
    );
}

#[test]
fn failed_cut_back_is_repaired_before_a_rewrite_reads_the_file() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a", "db:b"]), true).unwrap();
    wal.inject_fault(WalFault::Sync);
    wal.inject_fault(WalFault::Restore);
    assert!(wal.append(&txn(2, &["db:b"]), true).is_err());

    wal.remove_shard_entries("db:a").unwrap();

    let mut expected = txn(1, &["db:a", "db:b"]);
    expected.remove_shard("db:a");
    assert_eq!(wal.read_all().unwrap(), [expected]);
}

#[test]
fn failed_write_keeps_earlier_buffered_records() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a"]), false).unwrap();

    wal.inject_fault(WalFault::Write);
    assert!(wal.append(&txn(2, &["db:a"]), false).is_err());
    assert!(wal.repair.is_none());
    assert_eq!(wal.read_all().unwrap(), [txn(1, &["db:a"])]);
}

#[test]
fn failed_flush_repairs_to_the_intact_prefix_before_the_next_write() {
    let temp = TempDir::new().unwrap();
    let mut wal = open(temp.path());
    wal.append(&txn(1, &["db:a"]), true).unwrap();
    let valid_len = wal.file_size();

    let writer = wal.writer.as_mut().unwrap();
    writer.write_all(b"deadbeef:{\"rev").unwrap();
    writer.flush().unwrap();
    wal.discard_writer(Repair::ToIntactPrefix);
    assert!(wal.file_size() > valid_len);

    wal.append(&txn(2, &["db:a"]), true).unwrap();
    let expected = [txn(1, &["db:a"]), txn(2, &["db:a"])];
    assert_eq!(wal.read_all().unwrap(), expected);
    drop(wal);
    assert_eq!(recovered(temp.path()), expected);
}

#[test]
fn intact_record_of_unknown_format_fails_open() {
    let temp = TempDir::new().unwrap();
    let json = br#"{"rev":1,"ops":[{"rule":{"name":"r"}}]}"#;
    let mut bytes = format!("{:08x}:", crc32fast::hash(json)).into_bytes();
    bytes.extend_from_slice(json);
    bytes.push(b'\n');
    fs::write(temp.path().join("current.wal"), &bytes).unwrap();

    let err = PersistWal::open(temp.path().to_path_buf()).err().unwrap();
    assert!(matches!(err, StorageError::WalUnreadable { .. }), "{err}");
    assert_eq!(fs::read(temp.path().join("current.wal")).unwrap(), bytes);
}
