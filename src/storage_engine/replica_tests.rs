//! A follower that applies a primary's replication stream, or reconciles to
//! its checkpoint, holds the primary's state: facts, rules, schemas, graphs
//! and vector indexes, now and after a restart, however often an event is
//! replayed. A follower refuses client writes.

#![allow(clippy::unwrap_used)]

use super::*;
use crate::config::{Config, ReplicationRole};
use crate::replication::event::{decode_line, split_lines};
use crate::replication::resync::checkpoint_lines;
use crate::replication::{Event, Line, Read, ResyncMark};
use crate::schema::{ColumnSchema, SchemaType};
use crate::statement::IndexCreateOptions;
use crate::value::Value;
use std::collections::BTreeMap;
use tempfile::TempDir;

const KG: &str = "default";

fn open(temp: &TempDir, role: ReplicationRole) -> StorageEngine {
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.num_threads = 2;
    config.storage.persist.durability_mode = crate::config::DurabilityMode::Immediate;
    config.replication.role = role;
    StorageEngine::new(config).unwrap()
}

fn pair(a: i64, b: i64) -> Tuple {
    Tuple::new(vec![Value::Int64(a), Value::Int64(b)])
}

fn rule(text: &str) -> CatalogChange {
    CatalogChange::RegisterRule(crate::statement::parse_rule_definition(text).unwrap())
}

/// Apply the primary's events after `cursor` to `follower`; returns the new cursor.
fn pump(primary: &StorageEngine, follower: &StorageEngine, cursor: u64) -> u64 {
    let log = primary.replication_log().unwrap();
    match log.read_after(cursor, usize::MAX) {
        Read::Events { first, lines } => {
            for line in &lines {
                let event = match decode_line(line).unwrap() {
                    Line::Commit(txn) => Event::Commit(txn),
                    Line::Engine(event) => Event::Engine(event),
                    Line::Resync(mark) => panic!("resync mark {mark:?} in the log"),
                };
                follower.apply_replicated(event).unwrap();
            }
            first + lines.len() as u64 - 1
        }
        Read::UpToDate => cursor,
        Read::Unavailable => panic!("events after {cursor} are gone"),
    }
}

/// Bring `follower` to a checkpoint of `primary`, as the follower task does;
/// returns the LSN the checkpoint holds the events up to.
fn resync(primary: &StorageEngine, follower: &StorageEngine) -> u64 {
    let (checkpoint, head) = primary.capture_checkpoint_at_lsn().unwrap();
    let mut lines = Vec::new();
    checkpoint_lines::<StorageError>(&checkpoint, head, 3, |line| {
        lines.push(line);
        Ok(())
    })
    .unwrap();
    let mut graphs = Vec::new();
    let mut current: Option<(String, GraphState)> = None;
    for line in &lines {
        for line in split_lines(line) {
            match decode_line(line).unwrap() {
                Line::Resync(ResyncMark::Begin { graphs: names, .. }) => graphs = names,
                Line::Resync(ResyncMark::Graph { name, indexes }) => {
                    if let Some((name, state)) = current.take() {
                        follower.reconcile_graph(&name, state).unwrap();
                    }
                    let state = GraphState {
                        indexes,
                        ..GraphState::default()
                    };
                    current = Some((name, state));
                }
                Line::Commit(txn) => {
                    let (name, state) = current.as_mut().unwrap();
                    state.absorb(name, txn).unwrap();
                }
                Line::Resync(ResyncMark::End) => {
                    if let Some((name, state)) = current.take() {
                        follower.reconcile_graph(&name, state).unwrap();
                    }
                    follower.retain_replica_graphs(&graphs).unwrap();
                }
                Line::Engine(event) => panic!("engine event {event:?} in a checkpoint"),
            }
        }
    }
    head
}

/// Everything replication must carry, per graph, in comparable form.
#[derive(Debug, PartialEq)]
struct GraphView {
    facts: BTreeMap<String, Vec<Tuple>>,
    rules: BTreeMap<String, String>,
    schemas: BTreeMap<String, String>,
    indexes: Vec<String>,
}

fn state(engine: &StorageEngine) -> BTreeMap<String, GraphView> {
    let checkpoint = engine.capture_checkpoint().unwrap();
    checkpoint
        .knowledge_graphs
        .into_iter()
        .map(|kg| {
            let facts = kg
                .relations
                .iter()
                .map(|(name, tuples)| {
                    let mut tuples = tuples.to_vec();
                    tuples.sort();
                    (name.clone(), tuples)
                })
                .collect();
            let rules = kg
                .rules
                .list()
                .into_iter()
                .map(|name| {
                    let definition = serde_json::to_string(kg.rules.get(&name).unwrap()).unwrap();
                    (name, definition)
                })
                .collect();
            let schemas = kg
                .schemas
                .persistent_schemas()
                .map(|s| (s.name.clone(), serde_json::to_string(s).unwrap()))
                .collect();
            let mut indexes: Vec<String> = kg
                .indexes
                .iter()
                .map(|i| serde_json::to_string(i).unwrap())
                .collect();
            indexes.sort();
            (
                kg.name,
                GraphView {
                    facts,
                    rules,
                    schemas,
                    indexes,
                },
            )
        })
        .collect()
}

fn vector_schema() -> RelationSchema {
    RelationSchema::new("doc")
        .with_column(ColumnSchema::new("id", SchemaType::Int))
        .with_column(ColumnSchema::new(
            "emb",
            SchemaType::Vector { dim: Some(2) },
        ))
}

fn doc(id: i64, x: f32, y: f32) -> Tuple {
    Tuple::new(vec![Value::Int64(id), Value::vector(vec![x, y])])
}

fn index_options() -> IndexCreateOptions {
    IndexCreateOptions {
        name: "doc_emb".into(),
        relation: "doc".into(),
        column: "emb".into(),
        index_type: "hnsw".into(),
        metric: Some("euclidean".into()),
        m: None,
        ef_construction: None,
        ef_search: None,
    }
}

/// Every kind of replicated change, on the primary.
fn write_everything(primary: &StorageEngine) {
    primary
        .insert_tuples_into(KG, "edge", vec![pair(1, 2), pair(2, 3), pair(3, 4)])
        .unwrap();
    primary
        .delete_tuples_from(KG, "edge", vec![pair(3, 4)])
        .unwrap();
    primary
        .commit_catalog(KG, rule("reach(X, Y) <- edge(X, Y)"))
        .unwrap();
    primary
        .commit_catalog(KG, rule("reach(X, Z) <- reach(X, Y), edge(Y, Z)"))
        .unwrap();
    primary.register_schema_in(KG, vector_schema()).unwrap();
    primary
        .insert_tuples_into(KG, "doc", vec![doc(1, 0.0, 1.0), doc(2, 1.0, 0.0)])
        .unwrap();
    primary.create_index_in(KG, &index_options()).unwrap();
    primary.create_knowledge_graph("other").unwrap();
    primary
        .insert_tuples_into("other", "scratch", vec![pair(9, 9)])
        .unwrap();
    primary
        .insert_tuples_into("other", "kept", vec![pair(7, 7)])
        .unwrap();
    primary.drop_relation_in("other", "scratch").unwrap();
    primary.create_knowledge_graph("doomed").unwrap();
    primary
        .insert_tuples_into("doomed", "x", vec![pair(1, 1)])
        .unwrap();
    primary.drop_knowledge_graph("doomed").unwrap();
}

#[test]
fn a_follower_applying_the_stream_holds_the_primary_state() {
    let (p, f) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let primary = open(&p, ReplicationRole::Primary);
    let follower = open(&f, ReplicationRole::Follower);
    write_everything(&primary);
    pump(&primary, &follower, 0);
    assert_eq!(state(&follower), state(&primary));
    assert_eq!(
        follower
            .execute_query_with_rules_tuples_on(KG, "q(Y) <- reach(1, Y)")
            .unwrap()
            .len(),
        2,
        "replicated rules evaluate on the follower"
    );

    // Index drops and later changes keep following.
    primary.drop_index_in(KG, "doc_emb").unwrap();
    primary.drop_rule_in(KG, "reach").unwrap();
    primary
        .insert_tuples_into(KG, "edge", vec![pair(5, 6)])
        .unwrap();
    pump(&primary, &follower, 0);
    assert_eq!(state(&follower), state(&primary));
}

#[test]
fn replaying_events_already_applied_changes_nothing() {
    let (p, f) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let primary = open(&p, ReplicationRole::Primary);
    let follower = open(&f, ReplicationRole::Follower);
    write_everything(&primary);
    let head = pump(&primary, &follower, 0);
    let applied = state(&follower);
    // A crash before the position was saved replays from an older point.
    for from in [0, head / 2, head - 1] {
        pump(&primary, &follower, from);
        assert_eq!(state(&follower), applied, "replayed from {from}");
    }
}

#[test]
fn a_follower_reconciles_any_prior_state_to_a_checkpoint() {
    let (p, f) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    {
        // State the primary never had: other facts, a rule, a schema, a graph.
        let stale = open(&f, ReplicationRole::Standalone);
        stale
            .insert_tuples_into(KG, "edge", vec![pair(1, 2), pair(8, 8)])
            .unwrap();
        stale
            .insert_tuples_into(KG, "junk", vec![pair(0, 0)])
            .unwrap();
        stale
            .commit_catalog(KG, rule("stale(X) <- junk(X, X)"))
            .unwrap();
        stale
            .register_schema_in(
                KG,
                RelationSchema::new("typed").with_column(ColumnSchema::new("x", SchemaType::Int)),
            )
            .unwrap();
        stale.create_knowledge_graph("orphan").unwrap();
        stale
            .insert_tuples_into("orphan", "x", vec![pair(1, 1)])
            .unwrap();
    }
    let primary = open(&p, ReplicationRole::Primary);
    write_everything(&primary);
    let follower = open(&f, ReplicationRole::Follower);
    let head = resync(&primary, &follower);
    assert_eq!(state(&follower), state(&primary));

    // The stream after the checkpoint applies on top of it.
    primary
        .insert_tuples_into(KG, "edge", vec![pair(4, 5)])
        .unwrap();
    primary.drop_relation_in("other", "kept").unwrap();
    pump(&primary, &follower, head);
    assert_eq!(state(&follower), state(&primary));
}

#[test]
fn applied_state_survives_a_follower_restart() {
    let (p, f) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let primary = open(&p, ReplicationRole::Primary);
    write_everything(&primary);
    {
        let follower = open(&f, ReplicationRole::Follower);
        pump(&primary, &follower, 0);
    }
    assert_eq!(state(&open(&f, ReplicationRole::Follower)), state(&primary));
}

#[test]
fn a_follower_refuses_every_client_write() {
    let f = TempDir::new().unwrap();
    let follower = open(&f, ReplicationRole::Follower);
    let refused = |result: StorageResult<()>| {
        assert!(
            matches!(result, Err(StorageError::ReadOnlyReplica)),
            "{result:?}"
        );
    };
    refused(
        follower
            .insert_tuples_into(KG, "edge", vec![pair(1, 2)])
            .map(drop),
    );
    refused(
        follower
            .commit_catalog(KG, rule("r(X) <- edge(X, X)"))
            .map(drop),
    );
    refused(follower.register_schema_in(KG, vector_schema()));
    refused(follower.create_knowledge_graph("new"));
    refused(follower.drop_knowledge_graph(crate::auth::INTERNAL_KG));
    refused(follower.clear_relations_by_prefix_in(KG, "e").map(drop));
    refused(follower.create_index_in(KG, &index_options()).map(drop));
    // The replication applier still writes.
    let mut txn = Transaction::new(5);
    txn.insert(format!("{KG}:edge"), vec![pair(1, 2)]);
    follower.apply_replicated(Event::Commit(txn)).unwrap();
    assert_eq!(
        follower
            .execute_query_with_rules_tuples_on(KG, "q(X, Y) <- edge(X, Y)")
            .unwrap(),
        vec![pair(1, 2)]
    );
}

#[test]
fn a_primary_logs_nothing_for_writes_that_fail_or_change_nothing() {
    let p = TempDir::new().unwrap();
    let primary = open(&p, ReplicationRole::Primary);
    let log = Arc::clone(primary.replication_log().unwrap());
    let head = log.head();
    primary
        .insert_tuples_into(KG, "edge", vec![pair(1, 2)])
        .unwrap();
    assert_eq!(log.head(), head + 1);
    // A duplicate insert has an empty delta: no WAL record, no event.
    primary
        .insert_tuples_into(KG, "edge", vec![pair(1, 2)])
        .unwrap();
    assert_eq!(log.head(), head + 1);
    primary
        .persist
        .inject_wal_fault(crate::storage::persist::wal::WalFault::Write);
    assert!(primary
        .insert_tuples_into(KG, "edge", vec![pair(2, 3)])
        .is_err());
    assert_eq!(log.head(), head + 1, "a failed WAL write ships nothing");
}

#[test]
fn a_standalone_engine_keeps_no_log() {
    let s = TempDir::new().unwrap();
    let engine = open(&s, ReplicationRole::Standalone);
    assert!(engine.replication_log().is_none());
    assert!(!engine.is_replica());
}
