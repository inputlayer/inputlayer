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

#[test]
fn a_bound_that_cannot_be_raised_refuses_writes_and_is_never_passed() {
    let counter = Counter::new();
    let dir = TempDir::new().unwrap();
    let _reservation = counter.open(dir.path(), false).unwrap();
    let bound = recorded(&dir);

    // `metadata` is a file: the bound cannot be written.
    let metadata = dir.path().join("metadata");
    std::fs::remove_dir_all(&metadata).unwrap();
    std::fs::write(&metadata, "").unwrap();

    for _ in 0..bound - HEADROOM {
        counter.next();
    }
    assert!(counter.reserve_ahead().is_ok());
    counter.next();
    assert!(counter.reserve_ahead().is_err());
    for _ in 1..HEADROOM {
        let revision = counter.next();
        assert!(revision <= bound, "{revision} is above the bound on disk");
    }
    assert!(counter.reserve_ahead().is_err());
    let past = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| counter.next()));
    assert!(past.is_err(), "issued {past:?} above the bound on disk");

    std::fs::remove_file(&metadata).unwrap();
    counter.reserve_ahead().unwrap();
    let revision = counter.next();
    assert!(revision > bound);
    assert!(revision <= recorded(&dir), "{revision} is not reserved");
}

#[test]
fn a_closed_engine_is_dropped_without_writing_its_bound() {
    let counter = Counter::new();
    let (closed, open) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let reservation = counter.open(closed.path(), false).unwrap();
    let _open = counter.open(open.path(), false).unwrap();
    let removed = closed.path().to_path_buf();
    drop(reservation);
    drop(closed);

    counter.last.fetch_add(BLOCK, Ordering::SeqCst);
    counter.reserve_ahead().unwrap();
    assert!(
        !removed.exists(),
        "a closed engine's directory was recreated"
    );
    let revision = counter.next();
    assert!(revision <= recorded(&open), "{revision} is not reserved");
}

#[test]
fn an_engine_closing_while_bounds_are_raised_fails_no_write() {
    for _ in 0..50 {
        let counter = Counter::new();
        let (closing, open) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let reservation = counter.open(closing.path(), false).unwrap();
        let _open = counter.open(open.path(), false).unwrap();
        counter.last.fetch_add(BLOCK, Ordering::SeqCst);

        let raised = std::sync::atomic::AtomicBool::new(false);
        let result = std::thread::scope(|scope| {
            scope.spawn(|| {
                // Until the raise holds this engine's reservation.
                while Arc::strong_count(&reservation) == 1 && !raised.load(Ordering::SeqCst) {
                    std::hint::spin_loop();
                }
                drop(reservation);
                drop(closing);
            });
            let result = counter.reserve_ahead();
            raised.store(true, Ordering::SeqCst);
            result
        });
        result.unwrap();
        let revision = counter.next();
        assert!(revision <= recorded(&open), "{revision} is not reserved");
    }
}

#[test]
fn a_closed_engine_directory_is_not_written_by_a_raise_in_progress() {
    for _ in 0..50 {
        let counter = Counter::new();
        let (closing, open) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let reservation = counter.open(closing.path(), false).unwrap();
        let _open = counter.open(open.path(), false).unwrap();
        counter.last.fetch_add(BLOCK, Ordering::SeqCst);

        let raised = std::sync::atomic::AtomicBool::new(false);
        let at_close = std::thread::scope(|scope| {
            let closed = scope.spawn(|| {
                // Until the raise holds this engine's reservation.
                while Arc::strong_count(&reservation) == 1 && !raised.load(Ordering::SeqCst) {
                    std::hint::spin_loop();
                }
                counter.close(&reservation);
                recorded(&closing)
            });
            counter.reserve_ahead().unwrap();
            raised.store(true, Ordering::SeqCst);
            closed.join().unwrap()
        });
        assert_eq!(recorded(&closing), at_close, "written after its close");
    }
}
