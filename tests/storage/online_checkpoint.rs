//! Online checkpoint export: a running engine backs itself up at one
//! committed revision while writes continue, and the export restores to
//! exactly that revision. Interrupted, cancelled, corrupt and incomplete
//! exports never pass for a backup.

// Test setup aborts on failure; `unwrap` is the intended behavior.
#![allow(clippy::unwrap_used)]

use inputlayer::config::DurabilityMode;
use inputlayer::storage::backup::{self, BackupError, Export, Manifest, MANIFEST_FILE_NAME};
use inputlayer::value::{Tuple, Value};
use inputlayer::{Config, StorageEngine};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;
use tempfile::TempDir;

/// Data in `<root>/data`, exports in `<root>/backups`. A small flush
/// threshold leaves some facts in batches and some only in the WAL.
fn config(root: &Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = root.join("data");
    config.storage.backup_dir = Some(root.join("backups"));
    crate::harness::pool();
    config.storage.performance.num_threads = 2;
    config.storage.persist.durability_mode = DurabilityMode::Batched;
    config.storage.persist.buffer_size = 16;
    config
}

fn open(data_dir: &Path) -> StorageEngine {
    let mut config = config(data_dir.parent().unwrap());
    config.storage.data_dir = data_dir.to_path_buf();
    StorageEngine::new(config).unwrap()
}

fn ints(values: impl IntoIterator<Item = i64>) -> Vec<Tuple> {
    values
        .into_iter()
        .map(|v| Tuple::new(vec![Value::Int64(v)]))
        .collect()
}

/// The single-column integers in `kg`'s `relation`.
fn values(engine: &StorageEngine, kg: &str, relation: &str) -> BTreeSet<i64> {
    engine
        .execute_query_tuples_on(kg, &format!("seen(X) <- {relation}(X)"))
        .unwrap()
        .iter()
        .map(|t| match t.get(0) {
            Some(Value::Int64(v)) => *v,
            other => panic!("unexpected value {other:?}"),
        })
        .collect()
}

/// An engine holding `relations` relations of `per_relation` facts in KG `bulk`.
fn engine_with_bulk(root: &Path, relations: usize, per_relation: i64) -> StorageEngine {
    let engine = StorageEngine::new(config(root)).unwrap();
    engine.create_knowledge_graph("bulk").unwrap();
    for r in 0..relations {
        engine
            .insert_tuples_into("bulk", &format!("r{r}"), ints(0..per_relation))
            .unwrap();
    }
    engine
}

#[test]
fn export_during_writes_restores_one_consistent_revision() {
    let temp = TempDir::new().unwrap();
    let engine = Arc::new(StorageEngine::new(config(temp.path())).unwrap());
    engine.create_knowledge_graph("a").unwrap();
    engine.create_knowledge_graph("b").unwrap();

    // Each step commits tick(i) to `a`, then tock(i) to `b`: in any
    // consistent cut, tock(i) implies tick(i). Every commit takes the next
    // revision, so the cut at revision R holds exactly R - base of them.
    let base = engine.capture_checkpoint().unwrap().revision;
    let stop = Arc::new(AtomicBool::new(false));
    let steps = Arc::new(AtomicU64::new(0));
    let writer = {
        let (engine, stop, steps) = (Arc::clone(&engine), Arc::clone(&stop), Arc::clone(&steps));
        std::thread::spawn(move || {
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                engine.insert_tuples_into("a", "tick", ints([i])).unwrap();
                engine.insert_tuples_into("b", "tock", ints([i])).unwrap();
                i += 1;
                steps.store(i as u64, Ordering::Relaxed);
            }
        })
    };
    while steps.load(Ordering::Relaxed) < 50 {
        std::thread::yield_now();
    }

    let dest = temp.path().join("backups/during-writes");
    let export = engine.start_checkpoint_export(&dest).unwrap();
    let revision = export.revision;
    let exported = export.wait().unwrap();
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    assert_eq!(Manifest::load(&dest).unwrap().revision, Some(revision));
    backup::verify(&dest).unwrap();
    let restored = temp.path().join("restored");
    backup::restore(&dest, &restored).unwrap();
    let restored = open(&restored);

    let ticks = values(&restored, "a", "tick");
    let tocks = values(&restored, "b", "tock");
    let k = ticks.len() as i64;
    let m = tocks.len() as i64;
    assert_eq!(ticks, (0..k).collect(), "ticks are not a prefix");
    assert_eq!(tocks, (0..m).collect(), "tocks are not a prefix");
    assert!(
        m <= k && k <= m + 1,
        "not one revision: {k} ticks but {m} tocks"
    );
    assert_eq!(
        (k + m) as u64,
        revision - base,
        "the export does not hold exactly the commits up to revision {revision}"
    );
    assert!(k >= 50, "export missed commits made before it: {k}");
    assert_eq!(exported.files, Manifest::load(&dest).unwrap().files.len());

    // The restored engine continues after the exported revision.
    restored.insert_tuples_into("a", "tick", ints([k])).unwrap();
    assert!(restored.capture_checkpoint().unwrap().revision > revision);
}

#[test]
fn checkpoint_revision_names_the_newest_commit() {
    let temp = TempDir::new().unwrap();
    let engine = StorageEngine::new(config(temp.path())).unwrap();
    engine.create_knowledge_graph("a").unwrap();
    engine.insert_tuples_into("a", "x", ints([1])).unwrap();

    let first = engine.capture_checkpoint().unwrap().revision;
    assert_eq!(
        engine.capture_checkpoint().unwrap().revision,
        first,
        "no commit, same revision"
    );
    engine.insert_tuples_into("a", "x", ints([2])).unwrap();
    assert_eq!(engine.capture_checkpoint().unwrap().revision, first + 1);
    // An insert that changes nothing commits nothing.
    engine.insert_tuples_into("a", "x", ints([2])).unwrap();
    assert_eq!(engine.capture_checkpoint().unwrap().revision, first + 1);
}

#[test]
fn cancelled_export_removes_what_it_wrote() {
    let temp = TempDir::new().unwrap();
    let engine = engine_with_bulk(temp.path(), 4, 100);
    let checkpoint = engine.capture_checkpoint().unwrap();
    let dest = temp.path().join("backups/cancelled");

    let export = Export::claim(&dest, &temp.path().join("data")).unwrap();
    assert!(dest.is_dir(), "claimed before capture");
    let cancelled = export.write(&checkpoint, &AtomicBool::new(true));

    assert!(
        matches!(cancelled, Err(BackupError::Cancelled)),
        "{cancelled:?}"
    );
    assert!(!dest.exists());
}

#[test]
fn engine_shutdown_mid_export_leaves_a_complete_export_or_none() {
    let temp = TempDir::new().unwrap();
    let engine = engine_with_bulk(temp.path(), 40, 2_000);
    let dest = temp.path().join("backups/shutdown");

    let export = engine.start_checkpoint_export(&dest).unwrap();
    drop(engine);
    // Dropping the engine cancels the export and waits for it, so nothing
    // half-written is ever left behind.
    match export.wait() {
        Ok(_) => {
            backup::verify(&dest).unwrap();
        }
        Err(BackupError::Cancelled) => assert!(!dest.exists()),
        Err(other) => panic!("unexpected export error: {other}"),
    }
    // The data directory lock was released with the engine.
    drop(open(&temp.path().join("data")));
}

#[test]
fn incomplete_and_corrupt_exports_are_refused() {
    let temp = TempDir::new().unwrap();
    let engine = engine_with_bulk(temp.path(), 3, 50);
    let export = |name: &str| {
        let dest = temp.path().join("backups").join(name);
        engine
            .start_checkpoint_export(&dest)
            .unwrap()
            .wait()
            .unwrap();
        dest
    };

    // An export killed before its manifest was written.
    let incomplete = export("incomplete");
    fs::remove_file(incomplete.join(MANIFEST_FILE_NAME)).unwrap();
    assert!(matches!(
        backup::verify(&incomplete),
        Err(BackupError::Incomplete(_))
    ));
    let target = temp.path().join("restore-incomplete");
    assert!(matches!(
        backup::restore(&incomplete, &target),
        Err(BackupError::Incomplete(_))
    ));
    assert!(!target.exists());

    // One flipped byte in a batch file.
    let corrupt = export("corrupt");
    let batch = first_batch(&corrupt);
    let mut bytes = fs::read(&batch).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    fs::write(&batch, bytes).unwrap();
    assert!(matches!(
        backup::verify(&corrupt),
        Err(BackupError::Corrupt { .. })
    ));
    let target = temp.path().join("restore-corrupt");
    assert!(matches!(
        backup::restore(&corrupt, &target),
        Err(BackupError::Corrupt { .. })
    ));
    assert!(!target.exists());
}

fn first_batch(export: &Path) -> PathBuf {
    fs::read_dir(export.join("persist/batches"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "parquet"))
        .unwrap()
}

#[test]
fn exports_only_go_to_plain_names_outside_the_data_dir() {
    let temp = TempDir::new().unwrap();
    let mut inside = config(temp.path());
    inside.storage.backup_dir = Some(temp.path().join("data/backups"));
    let engine = StorageEngine::new(inside).unwrap();

    let dest = engine.checkpoint_destination(Some("nightly")).unwrap();
    assert!(matches!(
        engine.start_checkpoint_export(&dest),
        Err(BackupError::DestinationInsideSource { .. })
    ));
    assert!(
        !temp.path().join("data/backups").exists(),
        "a refusal creates nothing"
    );
    for bad in ["../escape", "a/b", ".hidden", ""] {
        assert!(
            matches!(
                engine.checkpoint_destination(Some(bad)),
                Err(BackupError::InvalidName(_))
            ),
            "{bad:?}"
        );
    }
    drop(engine);

    let mut unset = config(temp.path());
    unset.storage.backup_dir = None;
    let engine = StorageEngine::new(unset).unwrap();
    assert!(matches!(
        engine.checkpoint_destination(None),
        Err(BackupError::NoBackupDir)
    ));
    let occupied = temp.path().join("occupied");
    fs::create_dir_all(&occupied).unwrap();
    fs::write(occupied.join("keep"), b"x").unwrap();
    assert!(matches!(
        engine.start_checkpoint_export(&occupied),
        Err(BackupError::DestinationExists(_))
    ));
    assert_eq!(fs::read(occupied.join("keep")).unwrap(), b"x");
}

/// Captures racing KG creation, drops and writes must never deadlock.
#[test]
fn capture_stays_live_under_kg_churn_and_writes() {
    let temp = TempDir::new().unwrap();
    let engine = Arc::new(StorageEngine::new(config(temp.path())).unwrap());
    engine.create_knowledge_graph("hot").unwrap();
    let stop = Arc::new(AtomicBool::new(false));

    let churn = {
        let (engine, stop) = (Arc::clone(&engine), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                let kg = format!("churn{}", i % 4);
                if engine.create_knowledge_graph(&kg).is_ok() {
                    engine.insert_tuples_into(&kg, "x", ints([i])).unwrap();
                    engine.drop_knowledge_graph(&kg).unwrap();
                }
                i += 1;
            }
        })
    };
    let writer = {
        let (engine, stop) = (Arc::clone(&engine), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                engine.insert_tuples_into("hot", "x", ints([i])).unwrap();
                i += 1;
            }
        })
    };

    let (done_tx, done) = mpsc::channel();
    let capturer = {
        let engine = Arc::clone(&engine);
        std::thread::spawn(move || {
            let mut last = 0;
            for _ in 0..300 {
                let checkpoint = engine.capture_checkpoint().unwrap();
                assert!(checkpoint.revision >= last, "revision went backwards");
                last = checkpoint.revision;
            }
            done_tx.send(()).unwrap();
        })
    };
    let finished = done.recv_timeout(Duration::from_secs(120));
    stop.store(true, Ordering::Relaxed);
    assert!(finished.is_ok(), "checkpoint capture deadlocked");
    for thread in [capturer, churn, writer] {
        thread.join().unwrap();
    }
}

fn percentile(samples: &mut [Duration], p: f64) -> Duration {
    samples.sort();
    samples[((samples.len() - 1) as f64 * p) as usize]
}

/// Measurement, not a gate: capture cost, and commit and query latency on a
/// hot KG with and without exports running back to back. Skipped unless
/// `INPUTLAYER_MEASURE` is set; run with
/// `INPUTLAYER_MEASURE=1 cargo test --release --test storage online_checkpoint::measure -- --nocapture`.
#[test]
fn measure_capture_cost_and_hot_path_latency_during_exports() {
    if std::env::var_os("INPUTLAYER_MEASURE").is_none() {
        eprintln!("skipping measurement: set INPUTLAYER_MEASURE=1 to run");
        return;
    }
    let temp = TempDir::new().unwrap();
    let mut config = config(temp.path());
    config.storage.persist.buffer_size = 10_000;
    let engine = Arc::new(StorageEngine::new(config).unwrap());
    for kg in 0..50 {
        let kg = format!("kg{kg}");
        engine.create_knowledge_graph(&kg).unwrap();
        for r in 0..20 {
            engine
                .insert_tuples_into(&kg, &format!("r{r}"), ints(0..500))
                .unwrap();
        }
    }
    engine.create_knowledge_graph("hot").unwrap();
    engine
        .insert_tuples_into("hot", "seed", ints(0..1000))
        .unwrap();

    let mut captures: Vec<Duration> = (0..50)
        .map(|_| engine.capture_checkpoint().unwrap().capture_time)
        .collect();
    println!(
        "capture (51 KGs, 1001 relations, 501k facts): p50 {:?} max {:?}",
        percentile(&mut captures, 0.5),
        percentile(&mut captures, 1.0)
    );

    let measure = |label: &str, exporting: bool| {
        let stop = Arc::new(AtomicBool::new(false));
        let exporter = exporting.then(|| {
            let (engine, stop) = (Arc::clone(&engine), Arc::clone(&stop));
            let root = temp.path().join(format!("backups/{label}"));
            std::thread::spawn(move || {
                let mut runs = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let dest = root.join(runs.len().to_string());
                    let export = engine.start_checkpoint_export(&dest).unwrap();
                    let capture = export.capture_time;
                    let report = export.wait().unwrap();
                    fs::remove_dir_all(&dest).unwrap();
                    runs.push((capture, report.elapsed));
                }
                runs
            })
        });
        let reader = {
            let (engine, stop) = (Arc::clone(&engine), Arc::clone(&stop));
            std::thread::spawn(move || {
                let mut samples = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let start = std::time::Instant::now();
                    engine
                        .execute_query_tuples_on("hot", "q(X) <- seed(X), X < 10")
                        .unwrap();
                    samples.push(start.elapsed());
                }
                samples
            })
        };
        let mut next = 10_000_000 * i64::from(exporting);
        let mut commits = Vec::new();
        let until = std::time::Instant::now() + Duration::from_secs(4);
        while std::time::Instant::now() < until {
            let start = std::time::Instant::now();
            engine.insert_tuples_into("hot", "w", ints([next])).unwrap();
            commits.push(start.elapsed());
            next += 1;
        }
        stop.store(true, Ordering::Relaxed);
        let mut queries = reader.join().unwrap();
        let runs = exporter.map_or_else(Vec::new, |e| e.join().unwrap());
        let mut captures: Vec<Duration> = runs.iter().map(|r| r.0).collect();
        let mut writes: Vec<Duration> = runs.iter().map(|r| r.1).collect();
        if !runs.is_empty() {
            println!(
                "{label}: {} exports, capture p50 {:?} max {:?}, write p50 {:?}",
                runs.len(),
                percentile(&mut captures, 0.5),
                percentile(&mut captures, 1.0),
                percentile(&mut writes, 0.5)
            );
        }
        println!(
            "{label}: {} commits p50 {:?} p99 {:?} max {:?} | {} queries p50 {:?} p99 {:?} max {:?}",
            commits.len(),
            percentile(&mut commits, 0.5),
            percentile(&mut commits, 0.99),
            percentile(&mut commits, 1.0),
            queries.len(),
            percentile(&mut queries, 0.5),
            percentile(&mut queries, 0.99),
            percentile(&mut queries, 1.0),
        );
    };
    measure("idle", false);
    measure("exporting", true);
}
