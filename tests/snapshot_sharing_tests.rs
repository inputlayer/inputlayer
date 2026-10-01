//! Snapshot isolation and insert dedup with structurally shared relations.

#![allow(clippy::unwrap_used)]

use inputlayer::protocol::Handler;
use inputlayer::{Config, Tuple, Value};
use std::collections::BTreeSet;
use tokio::runtime::Runtime;

const KG: &str = "sharing";

fn make_handler() -> (Handler, tempfile::TempDir, Runtime) {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    config.storage.performance.query_timeout_ms = 0;
    let handler = Handler::from_config(config).unwrap();
    handler.get_storage().create_knowledge_graph(KG).unwrap();
    (handler, tmp, Runtime::new().unwrap())
}

fn run(handler: &Handler, rt: &Runtime, program: &str) {
    rt.block_on(handler.execute_program(None, Some(KG.to_string()), program.to_string(), None))
        .unwrap();
}

fn pair(a: i64, b: i64) -> Tuple {
    Tuple::new(vec![Value::Int64(a), Value::Int64(b)])
}

fn seconds(tuples: &[Tuple]) -> BTreeSet<i64> {
    tuples
        .iter()
        .map(|t| match t.values()[0] {
            Value::Int64(v) => v,
            ref other => panic!("unexpected {other:?}"),
        })
        .collect()
}

/// A query evaluated on an old snapshot sees exactly the data and rules of
/// that snapshot, whatever the writer does afterwards.
#[test]
fn test_old_snapshot_unaffected_by_later_writes() {
    let (handler, _tmp, rt) = make_handler();
    // Enough edges to span several shared chunks.
    let edges: Vec<String> = (0..3000).map(|i| format!("({i}, {})", i + 1)).collect();
    run(&handler, &rt, &format!("+edge[{}]", edges.join(", ")));
    run(&handler, &rt, "+two_hop(X, Z) <- edge(X, Y), edge(Y, Z)");

    let old = handler.get_storage().get_snapshot_for(KG).unwrap();
    let query = "q(Z) <- two_hop(1, Z)";
    assert_eq!(
        seconds(&old.execute_with_rules_tuples(query).unwrap()),
        BTreeSet::from([3])
    );

    // Append, retract, add a rule and a relation after the snapshot was taken.
    run(&handler, &rt, "+edge[(1, 500000), (500000, 500001)]");
    run(&handler, &rt, "-edge(2, 3)");
    run(&handler, &rt, "+two_hop(X, Z) <- edge(X, Z)");
    run(&handler, &rt, "+other[(1, 1)]");

    assert_eq!(old.tuple_count(), 3000);
    assert!(!old.input_tuples.contains_key("other"));
    assert!(old.input_tuples["edge"].contains(&pair(2, 3)));
    assert_eq!(
        seconds(&old.execute_with_rules_tuples(query).unwrap()),
        BTreeSet::from([3])
    );

    let new = handler.get_storage().get_snapshot_for(KG).unwrap();
    assert_eq!(
        seconds(&new.execute_with_rules_tuples(query).unwrap()),
        BTreeSet::from([2, 500000, 500001])
    );
}

/// Session facts layer onto a request-local copy; the shared snapshot and
/// later queries never see them.
#[test]
fn test_session_facts_do_not_leak_into_snapshot() {
    let (handler, _tmp, rt) = make_handler();
    run(&handler, &rt, "+edge[(1, 2)]");
    let snap = handler.get_storage().get_snapshot_for(KG).unwrap();
    let with_session = snap
        .execute_with_session_facts("q(Y) <- edge(1, Y)", vec![("edge".to_string(), pair(1, 9))])
        .unwrap();
    assert_eq!(seconds(&with_session), BTreeSet::from([2, 9]));
    assert_eq!(snap.input_tuples["edge"].len(), 1);
    assert_eq!(
        seconds(&snap.execute_tuples("q(Y) <- edge(1, Y)").unwrap()),
        BTreeSet::from([2])
    );
}

/// Inserts keep set semantics against existing data and within a batch,
/// including after deletes and at sizes spanning many chunks.
#[test]
fn test_insert_dedup_set_semantics() {
    let (handler, _tmp, _rt) = make_handler();
    let storage = handler.get_storage();
    let batch: Vec<Tuple> = (0..5000).map(|i| pair(i, i)).collect();

    assert_eq!(
        storage.insert_tuples_into(KG, "r", batch.clone()).unwrap(),
        (5000, 0)
    );
    assert_eq!(
        storage.insert_tuples_into(KG, "r", batch).unwrap(),
        (0, 5000)
    );
    assert_eq!(
        storage
            .insert_tuples_into(KG, "r", vec![pair(-1, 0), pair(-1, 0), pair(7, 7)])
            .unwrap(),
        (1, 2)
    );
    assert_eq!(
        storage
            .delete_tuples_from(KG, "r", vec![pair(7, 7), pair(7, 7)])
            .unwrap(),
        1
    );
    assert_eq!(
        storage
            .insert_tuples_into(KG, "r", vec![pair(7, 7), pair(8, 8)])
            .unwrap(),
        (1, 1)
    );
    let snap = storage.get_snapshot_for(KG).unwrap();
    assert_eq!(snap.input_tuples["r"].len(), 5001);
}
