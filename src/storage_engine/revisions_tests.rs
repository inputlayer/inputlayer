use super::*;
use tempfile::TempDir;

fn recorded(dir: &TempDir) -> u64 {
    let bytes = std::fs::read(dir.path().join(RESERVATION_FILE)).unwrap();
    serde_json::from_slice::<Record>(&bytes).unwrap().reserved
}

#[test]
fn a_new_directory_counts_from_one_within_a_recorded_bound() {
    let counter = Counter::new();
    let dir = TempDir::new().unwrap();
    let _reservation = counter.open(dir.path(), false).unwrap();
    assert_eq!(counter.next(), 1);
    assert_eq!(recorded(&dir), BLOCK);
}

#[test]
fn a_reopened_directory_continues_above_its_recorded_bound() {
    let dir = TempDir::new().unwrap();
    let earlier = {
        let counter = Counter::new();
        let _reservation = counter.open(dir.path(), false).unwrap();
        counter.next();
        recorded(&dir)
    };

    // Another process: its counter starts over.
    let counter = Counter::new();
    let _reservation = counter.open(dir.path(), true).unwrap();
    let revision = counter.next();
    assert!(revision > earlier, "{revision} reissues up to {earlier}");
    assert!(revision <= recorded(&dir));
}

#[test]
fn state_without_a_recorded_bound_continues_far_above_its_runs() {
    let counter = Counter::new();
    let dir = TempDir::new().unwrap();
    let _reservation = counter.open(dir.path(), true).unwrap();
    assert!(counter.next() > UNRECORDED_FLOOR);
}

#[test]
fn the_bound_is_raised_before_a_revision_passes_it() {
    let counter = Counter::new();
    let dir = TempDir::new().unwrap();
    let _reservation = counter.open(dir.path(), false).unwrap();
    for _ in 0..BLOCK {
        counter.next();
    }
    assert_eq!(recorded(&dir), BLOCK);
    let revision = counter.next();
    assert!(revision <= recorded(&dir), "{revision} is not reserved");
}

#[test]
fn every_open_engine_keeps_its_bound_above_the_counter() {
    let counter = Counter::new();
    let (first, second) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let _first = counter.open(first.path(), false).unwrap();
    // The second continues far above the first's bound.
    let _second = counter.open(second.path(), true).unwrap();
    let revision = counter.next();
    assert!(revision <= recorded(&first), "{revision} is not reserved");
    assert!(revision <= recorded(&second), "{revision} is not reserved");
}

#[test]
fn an_unreadable_bound_fails_the_open() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("metadata")).unwrap();
    std::fs::write(dir.path().join(RESERVATION_FILE), "not json").unwrap();
    assert!(Counter::new().open(dir.path(), true).is_err());
}
