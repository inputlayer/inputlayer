//! `expect_revision` on commits: a program commits only while the state in
//! its precondition's scope is as it was at the expected revision, decided
//! under the KG's write lock, so of concurrent writers expecting the same
//! revision of the same scope exactly one commits.

#![allow(clippy::unwrap_used)]

use super::*;
use crate::config::Config;
use crate::execution::RequestControl;
use crate::schema::{ColumnSchema, RelationSchema, SchemaType};
use crate::storage_engine::{CommitError, FactChange, StagedChanges, StorageEngine, WriteProgram};
use crate::value::Tuple;
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

const KG: &str = "default";

fn open(temp: &TempDir) -> StorageEngine {
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.num_threads = 2;
    config.storage.persist.durability_mode = crate::config::DurabilityMode::Immediate;
    StorageEngine::new(config).unwrap()
}

fn t(a: i32) -> Tuple {
    Tuple::from_pair(a, a)
}

fn revision(storage: &StorageEngine) -> u64 {
    storage.get_snapshot_for(KG).unwrap().revision
}

fn expect(revision: u64, relations: Option<&[&str]>) -> Arc<RequestControl> {
    let relations = relations.map(|names| names.iter().map(ToString::to_string).collect());
    RequestControl::expecting(
        None,
        Some(Precondition {
            revision,
            relations,
        }),
    )
}

/// Insert `t(value)` into `relation` as a one-statement program under `control`.
fn commit(
    storage: &StorageEngine,
    relation: &str,
    value: i32,
    control: &RequestControl,
) -> Result<(), PreconditionError> {
    let program = WriteProgram::single(StagedChanges::Facts(vec![FactChange::Insert {
        relation: relation.to_string(),
        tuples: vec![t(value)],
    }]));
    match storage.commit_program(KG, program, Some(control)) {
        Ok(_) => Ok(()),
        Err(CommitError::Precondition(error)) => Err(error),
        Err(other) => panic!("unexpected commit error: {other:?}"),
    }
}

fn contains(storage: &StorageEngine, relation: &str, value: i32) -> bool {
    storage
        .get_snapshot_for(KG)
        .unwrap()
        .input_tuples
        .get(relation)
        .is_some_and(|tuples| tuples.contains(&t(value)))
}

#[test]
fn a_change_in_scope_after_the_revision_refuses_and_writes_nothing() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage.insert_tuples_into(KG, "eta", vec![t(1)]).unwrap();
    let seen = revision(&storage);
    storage.insert_tuples_into(KG, "eta", vec![t(2)]).unwrap();
    let changed_at = revision(&storage);

    let refused = commit(&storage, "claim", 1, &expect(seen, Some(&["eta"])));
    assert_eq!(
        refused,
        Err(PreconditionError::RelationChanged {
            revision: seen,
            relation: "eta".to_string(),
            at: changed_at,
        })
    );
    assert!(!contains(&storage, "claim", 1));
    assert_eq!(revision(&storage), changed_at, "nothing was published");

    // At the revision that made the change, the scope is unchanged.
    commit(&storage, "claim", 1, &expect(changed_at, Some(&["eta"]))).unwrap();
    assert!(contains(&storage, "claim", 1));
}

#[test]
fn changes_outside_the_scope_do_not_refuse() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage.insert_tuples_into(KG, "eta", vec![t(1)]).unwrap();
    let seen = revision(&storage);
    storage.insert_tuples_into(KG, "other", vec![t(9)]).unwrap();
    storage.delete_tuples_from(KG, "other", vec![t(9)]).unwrap();
    // The program's own write target is outside the scope too.
    commit(&storage, "eta_claim", 1, &expect(seen, Some(&["eta"]))).unwrap();
    // ...but the whole knowledge graph has changed since.
    assert!(matches!(
        commit(&storage, "eta_claim", 2, &expect(seen, None)),
        Err(PreconditionError::RelationChanged { .. })
    ));
}

#[test]
fn a_derived_scope_covers_the_relations_it_is_derived_from() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    let rule = crate::statement::parse_rule_definition("late(X) <- eta(X, Y), Y > 1").unwrap();
    storage.register_rule_in(KG, &rule).unwrap();
    storage.insert_tuples_into(KG, "eta", vec![t(1)]).unwrap();
    let seen = revision(&storage);

    storage
        .insert_tuples_into(KG, "unrelated", vec![t(1)])
        .unwrap();
    commit(&storage, "claim", 1, &expect(seen, Some(&["late"]))).unwrap();

    // `eta(0, 0)` does not change `late`'s rows, but `eta` is in scope.
    storage.insert_tuples_into(KG, "eta", vec![t(0)]).unwrap();
    assert!(matches!(
        commit(&storage, "claim", 2, &expect(seen, Some(&["late"]))),
        Err(PreconditionError::RelationChanged { relation, .. }) if relation == "eta"
    ));
}

#[test]
fn a_rule_change_refuses_every_scope() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage.insert_tuples_into(KG, "eta", vec![t(1)]).unwrap();
    let seen = revision(&storage);
    let rule = crate::statement::parse_rule_definition("v(X) <- other(X, Y)").unwrap();
    storage.register_rule_in(KG, &rule).unwrap();
    let at = revision(&storage);
    assert_eq!(
        commit(&storage, "claim", 1, &expect(seen, Some(&["eta"]))),
        Err(PreconditionError::RulesChanged { revision: seen, at })
    );
}

#[test]
fn dropping_a_relation_in_scope_is_a_change() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage.insert_tuples_into(KG, "eta", vec![t(1)]).unwrap();
    let seen = revision(&storage);
    storage.drop_relation_in(KG, "eta").unwrap();
    assert!(matches!(
        commit(&storage, "claim", 1, &expect(seen, Some(&["eta"]))),
        Err(PreconditionError::RelationChanged { relation, .. }) if relation == "eta"
    ));
}

#[test]
fn a_program_that_changes_nothing_is_still_refused() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage.insert_tuples_into(KG, "eta", vec![t(1)]).unwrap();
    let seen = revision(&storage);
    storage.insert_tuples_into(KG, "eta", vec![t(2)]).unwrap();
    // Re-inserting a present fact is a no-op, but the check comes first.
    assert!(commit(&storage, "eta", 1, &expect(seen, Some(&["eta"]))).is_err());
}

#[test]
fn misspelled_scope_relations_are_rejected_and_declared_ones_accepted() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage.insert_tuples_into(KG, "eta", vec![t(1)]).unwrap();
    let seen = revision(&storage);
    assert_eq!(
        commit(&storage, "claim", 1, &expect(seen, Some(&["eta", "etaa"]))),
        Err(PreconditionError::UnknownRelation("etaa".to_string()))
    );
    let schema = RelationSchema::new("declared")
        .with_column(ColumnSchema::new("a", SchemaType::Int))
        .with_column(ColumnSchema::new("b", SchemaType::Int));
    storage.register_schema_in(KG, schema).unwrap();
    let seen = revision(&storage);
    commit(&storage, "claim", 1, &expect(seen, Some(&["declared"]))).unwrap();
}

#[test]
fn revisions_not_issued_or_from_before_the_graph_existed_are_refused() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage.insert_tuples_into(KG, "eta", vec![t(1)]).unwrap();
    let future = super::super::snapshot::last_revision() + 1_000_000;
    assert!(matches!(
        commit(&storage, "claim", 1, &expect(future, None)),
        Err(PreconditionError::Unissued { revision, .. }) if revision == future
    ));

    // The graph is dropped and created again: what it held at `seen` is
    // gone, and its new state starts after `seen`.
    storage.create_knowledge_graph("other").unwrap();
    let other_revision = storage.get_snapshot_for("other").unwrap().revision;
    storage.drop_knowledge_graph("other").unwrap();
    storage.create_knowledge_graph("other").unwrap();
    let control = expect(other_revision, None);
    let program = WriteProgram::single(StagedChanges::Facts(vec![FactChange::Insert {
        relation: "claim".to_string(),
        tuples: vec![t(1)],
    }]));
    assert!(matches!(
        storage.commit_program("other", program, Some(&control)),
        Err(CommitError::Precondition(
            PreconditionError::Predates { .. }
        ))
    ));
}

#[test]
fn a_restarted_engine_refuses_revisions_of_the_previous_run() {
    let temp = TempDir::new().unwrap();
    let seen = {
        let storage = open(&temp);
        storage.insert_tuples_into(KG, "eta", vec![t(1)]).unwrap();
        revision(&storage)
    };
    let storage = open(&temp);
    assert!(contains(&storage, "eta", 1));
    assert!(matches!(
        commit(&storage, "claim", 1, &expect(seen, Some(&["eta"]))),
        Err(PreconditionError::Predates { .. })
    ));
    commit(
        &storage,
        "claim",
        1,
        &expect(revision(&storage), Some(&["eta"])),
    )
    .unwrap();
}

#[test]
fn of_concurrent_writers_expecting_one_revision_of_one_scope_exactly_one_commits() {
    const WRITERS: usize = 8;
    let temp = TempDir::new().unwrap();
    let storage = Arc::new(open(&temp));
    storage.insert_tuples_into(KG, "slot", vec![t(0)]).unwrap();

    for round in 0..20 {
        let seen = revision(&storage);
        let start = Arc::new(Barrier::new(WRITERS));
        let writers: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let storage = Arc::clone(&storage);
                let start = Arc::clone(&start);
                let value = (round * WRITERS + writer + 1) as i32;
                std::thread::spawn(move || {
                    start.wait();
                    let control = expect(seen, Some(&["slot"]));
                    commit(&storage, "slot", value, &control).map(|()| value)
                })
            })
            .collect();
        let outcomes: Vec<_> = writers.into_iter().map(|w| w.join().unwrap()).collect();
        let winners: Vec<i32> = outcomes.iter().filter_map(|o| o.clone().ok()).collect();
        assert_eq!(winners.len(), 1, "round {round}: {outcomes:?}");
        for outcome in &outcomes {
            if let Err(error) = outcome {
                assert!(
                    matches!(error, PreconditionError::RelationChanged { relation, .. } if relation == "slot"),
                    "{error:?}"
                );
            }
        }
        assert!(contains(&storage, "slot", winners[0]));
    }
}

#[test]
fn writers_outside_the_scope_never_refuse_a_scoped_commit() {
    const WRITERS: usize = 4;
    let temp = TempDir::new().unwrap();
    let storage = Arc::new(open(&temp));
    storage
        .insert_tuples_into(KG, "window", vec![t(0)])
        .unwrap();
    let seen = revision(&storage);
    let noise: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let storage = Arc::clone(&storage);
            std::thread::spawn(move || {
                for i in 0..50 {
                    let value = (writer * 1000 + i) as i32;
                    storage
                        .insert_tuples_into(KG, &format!("noise{writer}"), vec![t(value)])
                        .unwrap();
                }
            })
        })
        .collect();
    for value in 1..=50 {
        commit(&storage, "claim", value, &expect(seen, Some(&["window"]))).unwrap();
    }
    for writer in noise {
        writer.join().unwrap();
    }
}

#[test]
fn the_change_log_stamps_only_what_a_publish_changed() {
    let temp = TempDir::new().unwrap();
    let storage = open(&temp);
    storage.insert_tuples_into(KG, "a", vec![t(1)]).unwrap();
    let a_at = revision(&storage);
    storage.insert_tuples_into(KG, "b", vec![t(1)]).unwrap();
    let b_at = revision(&storage);
    storage.delete_tuples_from(KG, "b", vec![t(1)]).unwrap();
    let b_cleared = revision(&storage);

    let snapshot = storage.get_snapshot_for(KG).unwrap();
    let changes = snapshot.changes();
    assert!(changes.since() < a_at);
    assert_eq!(changes.relation_changed_at("a"), Some(a_at));
    assert!(b_at < b_cleared);
    assert_eq!(changes.relation_changed_at("b"), Some(b_cleared));
    assert_eq!(changes.relation_changed_at("never"), None);
}
