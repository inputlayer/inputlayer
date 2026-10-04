//! A staged program's fact changes commit as one transaction: one WAL record
//! at one revision and one snapshot publish, or (on any failure) nothing at
//! all, now and after a restart. Rule and schema changes in the same program:
//! see `catalog_commit_tests`.

#![allow(clippy::unwrap_used)]

use super::*;
use crate::config::Config;
use crate::execution::{Halt, RequestControl, Stop};
use crate::schema::{ColumnSchema, SchemaType};
use crate::storage::persist::wal::WalFault;
use crate::value::Value;
use std::sync::atomic::AtomicBool;
use tempfile::TempDir;

const KG: &str = "default";

fn open(temp: &TempDir) -> StorageEngine {
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.num_threads = 2;
    config.storage.persist.durability_mode = crate::config::DurabilityMode::Immediate;
    StorageEngine::new(config).unwrap()
}

fn rows(storage: &StorageEngine, relation: &str) -> Vec<Tuple> {
    let snapshot = storage.get_snapshot_for(KG).unwrap();
    let mut rows: Vec<Tuple> = snapshot
        .input_tuples
        .get(relation)
        .map(|ts| ts.iter().cloned().collect())
        .unwrap_or_default();
    rows.sort();
    rows
}

/// WAL records written so far (one line per committed transaction).
fn wal_records(temp: &TempDir) -> Vec<String> {
    fs::read_to_string(temp.path().join("persist/wal/current.wal"))
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

fn t(a: i32) -> Tuple {
    Tuple::from_pair(a, a)
}

fn insert(relation: &str, tuples: Vec<Tuple>) -> FactChange {
    FactChange::Insert {
        relation: relation.to_string(),
        tuples,
    }
}

fn delete(relation: &str, tuples: Vec<Tuple>) -> FactChange {
    FactChange::Delete {
        relation: relation.to_string(),
        tuples,
    }
}

fn program(statements: Vec<Vec<FactChange>>) -> WriteProgram {
    let mut program = WriteProgram::new();
    for (index, changes) in statements.into_iter().enumerate() {
        program.push(index, StagedChanges::Facts(changes));
    }
    program
}

fn counts(commit: &ProgramCommit) -> Vec<(usize, usize)> {
    commit
        .statements
        .iter()
        .map(|outcome| match outcome.effect {
            StatementEffect::Facts(count) => (count.inserted, count.deleted),
            StatementEffect::Catalog(_) => panic!("not a fact statement"),
        })
        .collect()
}

#[test]
fn program_commits_one_record_across_relations() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage.insert_tuples_into(KG, "r", vec![t(1)]).unwrap();
    let before = wal_records(&temp).len();
    let revision = storage.get_snapshot_for(KG).unwrap().revision;

    let commit = storage
        .commit_program(
            KG,
            program(vec![
                vec![delete("r", vec![t(1)])],
                vec![insert("r", vec![t(2), t(3)])],
                vec![insert("s", vec![t(9)])],
            ]),
            None,
        )
        .unwrap();

    assert_eq!(counts(&commit), [(0, 1), (2, 0), (1, 0)]);
    assert_eq!(
        commit.relations,
        [
            RelationChange {
                relation: "r".into(),
                inserted: 2,
                deleted: 1
            },
            RelationChange {
                relation: "s".into(),
                inserted: 1,
                deleted: 0
            },
        ]
    );
    let records = wal_records(&temp);
    assert_eq!(records.len(), before + 1, "one WAL record per program");
    let record = records.last().unwrap();
    assert!(record.contains("default:r") && record.contains("default:s"));
    assert_ne!(storage.get_snapshot_for(KG).unwrap().revision, revision);
    assert_eq!(rows(&storage, "r"), [t(2), t(3)]);
    assert_eq!(rows(&storage, "s"), [t(9)]);

    drop(storage);
    let storage = open(&temp);
    assert_eq!(rows(&storage, "r"), [t(2), t(3)]);
    assert_eq!(rows(&storage, "s"), [t(9)]);
}

#[test]
fn late_arity_error_rejects_the_whole_program() {
    let temp = TempDir::new().unwrap();
    {
        let storage = open(&temp);
        storage.insert_tuples_into(KG, "r", vec![t(1)]).unwrap();
        let before = wal_records(&temp);
        let revision = storage.get_snapshot_for(KG).unwrap().revision;

        let err = storage
            .commit_program(
                KG,
                program(vec![
                    vec![delete("r", vec![t(1)])],
                    vec![insert("s", vec![t(5)])],
                    vec![insert("r", vec![Tuple::from_pair(2, 2), t(3)])],
                    vec![insert("r", vec![Tuple::new(vec![Value::Int64(1)])])],
                ]),
                None,
            )
            .unwrap_err();
        let CommitError::Rejected { statement, error } = err else {
            panic!("expected a rejection, got {err:?}");
        };
        assert_eq!(statement, 3);
        assert_eq!(
            error.to_string(),
            "Arity mismatch for relation 'r': existing arity is 2, but trying to insert tuples with arity 1"
        );
        assert_eq!(wal_records(&temp), before);
        assert_eq!(storage.get_snapshot_for(KG).unwrap().revision, revision);
        assert_eq!(rows(&storage, "r"), [t(1)]);
        assert!(rows(&storage, "s").is_empty());
    }
    let storage = open(&temp);
    assert_eq!(rows(&storage, "r"), [t(1)]);
    assert!(rows(&storage, "s").is_empty());
}

#[test]
fn arity_of_a_new_relation_is_set_by_its_first_staged_insert() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    let err = storage
        .commit_program(
            KG,
            program(vec![
                vec![insert("fresh", vec![t(1)])],
                vec![insert("fresh", vec![Tuple::new(vec![Value::Int64(1)])])],
            ]),
            None,
        )
        .unwrap_err();
    assert!(matches!(err, CommitError::Rejected { statement: 1, .. }));
    assert!(rows(&storage, "fresh").is_empty());
}

#[test]
fn delete_with_invalid_replacement_keeps_the_old_fact() {
    let temp = TempDir::new().unwrap();
    {
        let storage = open(&temp);
        let schema = RelationSchema::new("person")
            .with_column(ColumnSchema::new("id", SchemaType::Int))
            .with_column(ColumnSchema::new("age", SchemaType::Int));
        storage.register_or_update_schema_in(KG, schema).unwrap();
        storage
            .insert_tuples_into(KG, "person", vec![Tuple::from_pair(1, 30)])
            .unwrap();

        let replacement = Tuple::new(vec![Value::Int32(1), Value::string("thirty")]);
        let err = storage
            .commit_program(
                KG,
                program(vec![
                    vec![delete("person", vec![Tuple::from_pair(1, 30)])],
                    vec![insert("person", vec![replacement])],
                ]),
                None,
            )
            .unwrap_err();
        let CommitError::Rejected { statement, error } = err else {
            panic!("expected a rejection, got {err:?}");
        };
        assert_eq!(statement, 1);
        assert!(matches!(error, StorageError::WriteRejected(_)), "{error:?}");
        assert!(error
            .to_string()
            .starts_with("Insert rejected for 'person': "));
        assert_eq!(rows(&storage, "person"), [Tuple::from_pair(1, 30)]);
    }
    assert_eq!(rows(&open(&temp), "person"), [Tuple::from_pair(1, 30)]);
}

#[test]
fn duplicates_and_changes_that_cancel_out_write_nothing() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage.insert_tuples_into(KG, "r", vec![t(1)]).unwrap();
    let before = wal_records(&temp);
    let revision = storage.get_snapshot_for(KG).unwrap().revision;

    let commit = storage
        .commit_program(
            KG,
            program(vec![
                vec![insert("r", vec![t(1)])],
                vec![insert("r", vec![t(2), t(2)])],
                vec![delete("r", vec![t(2), t(7)])],
                vec![delete("r", vec![t(1)])],
                vec![insert("r", vec![t(1)])],
            ]),
            None,
        )
        .unwrap();

    // Counts are per statement, relative to the state the statement saw.
    assert_eq!(counts(&commit), [(0, 0), (1, 0), (0, 1), (0, 1), (1, 0)]);
    assert!(commit.relations.is_empty());
    assert_eq!(wal_records(&temp), before);
    assert_eq!(storage.get_snapshot_for(KG).unwrap().revision, revision);
    assert_eq!(rows(&storage, "r"), [t(1)]);
}

/// A program that deletes `r(1)` after reading `r` (as a conditional delete
/// over `r` would), staged on `snapshot`.
fn reads_r(snapshot: &Arc<KnowledgeGraphSnapshot>) -> WriteProgram {
    let mut staged = program(vec![vec![delete("r", vec![t(1)])]]);
    staged.read(snapshot, &snapshot.rules, "q(X, Y) <- r(X, Y)");
    staged
}

#[test]
fn stale_read_is_refused_and_a_fresh_one_commits() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage
        .insert_tuples_into(KG, "r", vec![t(1), t(2)])
        .unwrap();

    let stale = reads_r(&storage.get_snapshot_for(KG).unwrap());
    // Another writer changes `r` after the program read it.
    storage.delete_tuples_from(KG, "r", vec![t(2)]).unwrap();
    let before = wal_records(&temp);
    assert!(matches!(
        storage.commit_program(KG, stale, None),
        Err(CommitError::Stale)
    ));
    assert_eq!(wal_records(&temp), before);
    assert_eq!(rows(&storage, "r"), [t(1)]);

    let fresh = reads_r(&storage.get_snapshot_for(KG).unwrap());
    let commit = storage.commit_program(KG, fresh, None).unwrap();
    assert_eq!(counts(&commit), [(0, 1)]);
    assert!(rows(&storage, "r").is_empty());
}

#[test]
fn writes_to_unread_relations_do_not_conflict() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage.insert_tuples_into(KG, "r", vec![t(1)]).unwrap();
    let staged = reads_r(&storage.get_snapshot_for(KG).unwrap());
    storage.insert_tuples_into(KG, "other", vec![t(5)]).unwrap();
    let commit = storage.commit_program(KG, staged, None).unwrap();
    assert_eq!(counts(&commit), [(0, 1)]);
}

#[test]
fn rule_change_makes_a_read_stale() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage.insert_tuples_into(KG, "r", vec![t(1)]).unwrap();
    let staged = reads_r(&storage.get_snapshot_for(KG).unwrap());
    let rule = crate::statement::parse_rule_definition("+v(X) <- r(X, Y)").unwrap();
    storage.register_rule_in(KG, &rule).unwrap();
    assert!(matches!(
        storage.commit_program(KG, staged, None),
        Err(CommitError::Stale)
    ));
}

#[test]
fn blind_programs_never_go_stale() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    let staged = program(vec![vec![insert("r", vec![t(1)])]]);
    storage.insert_tuples_into(KG, "r", vec![t(1)]).unwrap();
    // Decided under the lock: the tuple is already there.
    let commit = storage.commit_program(KG, staged, None).unwrap();
    assert_eq!(counts(&commit), [(0, 0)]);
}

#[test]
fn cancelled_program_writes_nothing() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    let before = wal_records(&temp);
    let control = RequestControl::new(None);
    control.cancel();
    assert!(matches!(
        storage.commit_program(
            KG,
            program(vec![vec![insert("r", vec![t(1)])]]),
            Some(&control)
        ),
        Err(CommitError::Cancelled(Stop::Cancelled))
    ));
    assert_eq!(wal_records(&temp), before);
    assert!(rows(&storage, "r").is_empty());
}

#[test]
fn passed_deadline_writes_nothing() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    let before = wal_records(&temp);
    let control = RequestControl::new(Some(std::time::Instant::now()));
    assert!(matches!(
        storage.commit_program(
            KG,
            program(vec![vec![insert("r", vec![t(1)])]]),
            Some(&control)
        ),
        Err(CommitError::Cancelled(Stop::Deadline))
    ));
    assert_eq!(wal_records(&temp), before);
    assert!(rows(&storage, "r").is_empty());
}

#[test]
fn a_stop_after_the_commit_began_is_too_late() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    let control = RequestControl::new(None);
    storage
        .commit_program(
            KG,
            program(vec![vec![insert("r", vec![t(1)])]]),
            Some(&control),
        )
        .unwrap();
    assert!(control.is_committing());
    assert_eq!(control.cancel(), Halt::TooLate);
    assert_eq!(rows(&storage, "r"), vec![t(1)]);
}

#[test]
fn persistence_failure_rejects_the_whole_program_now_and_after_restart() {
    for fault in [WalFault::Write, WalFault::Sync] {
        let temp = TempDir::new().unwrap();
        {
            let storage = open(&temp);
            storage.insert_tuples_into(KG, "r", vec![t(1)]).unwrap();
            storage.persist.inject_wal_fault(fault);
            let err = storage
                .commit_program(
                    KG,
                    program(vec![
                        vec![delete("r", vec![t(1)])],
                        vec![insert("r", vec![t(2)]), insert("s", vec![t(3)])],
                    ]),
                    None,
                )
                .unwrap_err();
            assert!(matches!(err, CommitError::Failed(_)), "{fault:?}: {err:?}");
            assert_eq!(rows(&storage, "r"), [t(1)], "{fault:?}");
            assert!(rows(&storage, "s").is_empty(), "{fault:?}");
        }
        let storage = open(&temp);
        assert_eq!(rows(&storage, "r"), [t(1)], "{fault:?}");
        assert!(rows(&storage, "s").is_empty(), "{fault:?}");
    }
}

#[test]
fn readers_never_observe_part_of_a_program() {
    let temp = TempDir::new().unwrap();
    let storage = Arc::new(open(&temp));
    storage.insert_tuples_into(KG, "slot", vec![t(0)]).unwrap();
    let done = Arc::new(AtomicBool::new(false));

    // Each program replaces the slot's only tuple; a reader must always see
    // exactly one.
    let reader = {
        let storage = Arc::clone(&storage);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let mut reads = 0;
            while !done.load(Ordering::Relaxed) {
                let snapshot = storage.get_snapshot_for(KG).unwrap();
                let len = snapshot.input_tuples.get("slot").map_or(0, Relation::len);
                assert_eq!(len, 1, "observed a partial program after {reads} reads");
                reads += 1;
            }
        })
    };
    for i in 1..=200 {
        storage
            .commit_program(
                KG,
                program(vec![
                    vec![delete("slot", vec![t(i - 1)])],
                    vec![insert("slot", vec![t(i)])],
                ]),
                None,
            )
            .unwrap();
    }
    done.store(true, Ordering::Relaxed);
    reader.join().unwrap();
    assert_eq!(rows(&storage, "slot"), [t(200)]);
}

#[test]
fn durability_unknown_commit_blocks_every_later_commit_until_restart() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage.insert_tuples_into(KG, "r", vec![t(1)]).unwrap();
    for fault in [WalFault::Sync, WalFault::Restore, WalFault::SaveCut] {
        storage.persist.inject_wal_fault(fault);
    }
    let result = storage.commit_program(KG, program(vec![vec![insert("r", vec![t(2)])]]), None);
    assert!(matches!(result, Err(CommitError::OutcomeUnknown(_))));
    assert_eq!(rows(&storage, "r"), [t(1)]);
    for pending in [
        WriteProgram::new(),
        program(vec![vec![insert("r", vec![t(3)])]]),
    ] {
        assert!(matches!(
            storage.commit_program(KG, pending, None),
            Err(CommitError::StoreReadOnly)
        ));
    }
    assert!(matches!(
        storage.insert_tuples_into(KG, "r", vec![t(3)]),
        Err(StorageError::StoreReadOnly)
    ));
    assert!(matches!(
        storage.drop_relation_in(KG, "r"),
        Err(StorageError::StoreReadOnly)
    ));
    std::mem::forget(Arc::clone(&storage.persist));
    drop(storage);
    let recovered = open(&temp);
    assert_eq!(rows(&recovered, "r"), [t(1), t(2)]);
    recovered.insert_tuples_into(KG, "r", vec![t(3)]).unwrap();
}

#[test]
fn durability_deleted_relation_keeps_tombstone_after_unlink_sync_failure() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage.insert_tuples_into(KG, "r", vec![t(1)]).unwrap();
    storage.persist.flush("default:r").unwrap();
    let shards = temp.path().join("persist/shards");
    let metas: Vec<_> = fs::read_dir(&shards)
        .unwrap()
        .map(|e| {
            let path = e.unwrap().path();
            let bytes = fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect();
    crate::storage::persist::inject_sync_fault(shards);
    storage.drop_relation_in(KG, "r").unwrap();
    assert!(storage.persist.shard_info("default:r").is_err());
    assert!(storage
        .tombstones
        .lock()
        .relations
        .contains(&RelationTombstone::new(KG, "r")));
    std::mem::forget(Arc::clone(&storage.persist));
    drop(storage);
    for (path, bytes) in metas {
        fs::write(path, bytes).unwrap();
    }
    let recovered = open(&temp);
    assert!(rows(&recovered, "r").is_empty());
    assert!(!recovered
        .list_relations_in(KG)
        .unwrap()
        .contains(&"r".to_string()));
}

#[test]
fn durability_wal_retirement_failure_keeps_deletion_committed() {
    for keep_other in [false, true] {
        for recovery in 0..3 {
            let temp = TempDir::new().unwrap();
            let storage = open(&temp);
            storage.insert_tuples_into(KG, "r", vec![t(1)]).unwrap();
            if keep_other {
                storage.insert_tuples_into(KG, "other", vec![t(2)]).unwrap();
            }
            storage.persist.inject_wal_fault(WalFault::Rewrite);
            assert!(storage.drop_relation_in(KG, "r").is_err());
            assert_eq!(rows(&storage, "r"), [t(1)]);
            assert!(!storage
                .tombstones
                .lock()
                .relations
                .contains(&RelationTombstone::new(KG, "r")));
            let wal_dir = temp.path().join("persist/wal");
            let wal_path = wal_dir.join("current.wal");
            let before = fs::read(&wal_path).unwrap();
            crate::storage::persist::inject_sync_fault(wal_dir.clone());
            storage.drop_relation_in(KG, "r").unwrap();
            assert!(storage.persist.shard_info("default:r").is_ok());
            let tombstone = RelationTombstone::new(KG, "r");
            assert!(storage.tombstones.lock().relations.contains(&tombstone));
            assert!(rows(&storage, "r").is_empty());
            if recovery == 2 {
                crate::storage::persist::inject_sync_fault(wal_dir);
                assert!(matches!(
                    storage.insert_tuples_into(KG, "r", vec![t(3)]),
                    Err(StorageError::WalDurabilityPending(_))
                ));
                assert!(storage.tombstones.lock().relations.contains(&tombstone));
                storage.insert_tuples_into(KG, "r", vec![t(3)]).unwrap();
                assert!(!storage.tombstones.lock().relations.contains(&tombstone));
            }
            std::mem::forget(Arc::clone(&storage.persist));
            drop(storage);
            if recovery == 1 {
                fs::write(&wal_path, before).unwrap();
            }
            let recovered = open(&temp);
            assert_eq!(
                rows(&recovered, "r"),
                if recovery == 2 { vec![t(3)] } else { vec![] }
            );
            assert_eq!(
                rows(&recovered, "other"),
                if keep_other { vec![t(2)] } else { vec![] }
            );
        }
    }
}

fn open_with_budget(temp: &TempDir, budget: u64) -> StorageEngine {
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.max_graph_memory_bytes = budget;
    StorageEngine::new(config).unwrap()
}

/// Bytes of `n` tuples like `t(_)` against the budget.
fn budget_for(n: usize) -> u64 {
    (n * relation_store::stored_bytes(&t(0))) as u64
}

#[test]
fn a_write_past_the_graph_budget_is_refused_whole() {
    let temp = TempDir::new().unwrap();
    let storage = open_with_budget(&temp, budget_for(3));
    storage
        .commit_program(KG, program(vec![vec![insert("r", vec![t(1), t(2)])]]), None)
        .unwrap();
    let before = wal_records(&temp).len();

    // Over by one tuple: refused, blamed on the last inserting statement,
    // and nothing of the program is written or published.
    let err = storage
        .commit_program(
            KG,
            program(vec![
                vec![insert("s", vec![t(9)])],
                vec![insert("r", vec![t(3)])],
                vec![delete("r", vec![t(7)])],
            ]),
            None,
        )
        .unwrap_err();
    let CommitError::Rejected { statement, error } = err else {
        panic!("expected a rejection, got {err:?}");
    };
    assert_eq!(statement, 1);
    assert!(
        matches!(error, StorageError::MemoryBudgetExceeded { ref kg, .. } if kg == KG),
        "{error}"
    );
    assert_eq!(wal_records(&temp).len(), before);
    assert_eq!(rows(&storage, "r"), [t(1), t(2)]);
    assert!(rows(&storage, "s").is_empty());

    // A program whose net change fits passes: it frees what it adds.
    storage
        .commit_program(
            KG,
            program(vec![
                vec![delete("r", vec![t(1)])],
                vec![insert("r", vec![t(3), t(4)])],
            ]),
            None,
        )
        .unwrap();
    assert_eq!(rows(&storage, "r"), [t(2), t(3), t(4)]);
}

#[test]
fn a_graph_over_budget_still_loads_and_takes_deletes() {
    let temp = TempDir::new().unwrap();
    {
        let storage = open(&temp);
        storage
            .insert_tuples_into(KG, "r", (1..=5).map(t).collect())
            .unwrap();
    }
    let storage = open_with_budget(&temp, budget_for(2));
    assert_eq!(rows(&storage, "r").len(), 5, "recovery is never refused");

    let err = storage.insert_tuples_into(KG, "r", vec![t(6)]).unwrap_err();
    assert!(
        matches!(err, StorageError::MemoryBudgetExceeded { .. }),
        "{err}"
    );
    // Re-inserting what is stored grows nothing, and deleting always passes.
    storage.insert_tuples_into(KG, "r", vec![t(1)]).unwrap();
    storage.delete_tuples_from(KG, "r", vec![t(1)]).unwrap();
    assert_eq!(rows(&storage, "r").len(), 4);
}
