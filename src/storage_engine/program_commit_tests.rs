//! A staged program's fact changes commit as one transaction: one WAL record
//! at one revision and one snapshot publish, or (on any failure) nothing at
//! all, now and after a restart. Rule and schema changes in the same program:
//! see `catalog_commit_tests`.

#![allow(clippy::unwrap_used)]

use super::*;
use crate::config::Config;
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
    let cancel = AtomicBool::new(true);
    assert!(matches!(
        storage.commit_program(
            KG,
            program(vec![vec![insert("r", vec![t(1)])]]),
            Some(&cancel)
        ),
        Err(CommitError::Cancelled)
    ));
    assert_eq!(wal_records(&temp), before);
    assert!(rows(&storage, "r").is_empty());
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
