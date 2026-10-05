//! `.index create` and `.index rebuild` build without the knowledge graph's
//! lock: the writes made during a build reach the installed index, and a
//! build that was stopped or superseded installs nothing.

#![allow(clippy::unwrap_used)]

use super::*;
use crate::config::Config;
use crate::schema::{ColumnSchema, RelationSchema};
use crate::value::Value;
use std::time::Instant;
use tempfile::TempDir;

const KG: &str = "default";
const INDEX: &str = "doc_emb";

/// An engine whose `doc(id, emb)` holds `rows` vectors `(i, 1)` with id `i`.
fn engine(temp: &TempDir, rows: i64) -> StorageEngine {
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.num_threads = 2;
    let storage = StorageEngine::new(config).unwrap();
    storage
        .register_schema_in(
            KG,
            RelationSchema::new("doc")
                .with_column(ColumnSchema::new("id", SchemaType::Int))
                .with_column(ColumnSchema::new(
                    "emb",
                    SchemaType::Vector { dim: Some(2) },
                )),
        )
        .unwrap();
    let docs = (1..=rows).map(|i| doc(i, i as f32, 1.0)).collect();
    storage.insert_tuples_into(KG, "doc", docs).unwrap();
    storage
}

fn doc(id: i64, x: f32, y: f32) -> Tuple {
    Tuple::new(vec![Value::Int64(id), Value::vector(vec![x, y])])
}

fn options() -> IndexCreateOptions {
    IndexCreateOptions {
        name: INDEX.into(),
        relation: "doc".into(),
        column: "emb".into(),
        index_type: "hnsw".into(),
        metric: Some("euclidean".into()),
        m: None,
        ef_construction: None,
        ef_search: None,
    }
}

/// The ids the index holds nearest `query`, nearest first.
fn nearest(storage: &StorageEngine, query: [f32; 2], k: usize) -> Vec<Value> {
    storage
        .with_kg_read(KG, |db| {
            let managed = db.indexes.get(INDEX).ok_or("no index")?;
            managed.view().search(&query, k, None)
        })
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

fn index_names(storage: &StorageEngine) -> Vec<String> {
    storage
        .index_stats_in(KG, None)
        .unwrap()
        .into_iter()
        .map(|s| s.name)
        .collect()
}

/// A new row, a removed row and a moved row: id 41 at (100, 100), no id 1,
/// id 2 at (-50, -50).
fn write_meanwhile(storage: &StorageEngine) {
    storage
        .insert_tuples_into(KG, "doc", vec![doc(41, 100.0, 100.0)])
        .unwrap();
    storage
        .delete_tuples_from(KG, "doc", vec![doc(1, 1.0, 1.0), doc(2, 2.0, 1.0)])
        .unwrap();
    storage
        .insert_tuples_into(KG, "doc", vec![doc(2, -50.0, -50.0)])
        .unwrap();
}

fn assert_holds_the_writes_made_meanwhile(storage: &StorageEngine) {
    assert_eq!(nearest(storage, [100.0, 100.0], 1), vec![Value::Int64(41)]);
    assert_eq!(nearest(storage, [-50.0, -50.0], 1), vec![Value::Int64(2)]);
    let all = nearest(storage, [1.0, 1.0], 100);
    assert_eq!(all.len(), 40, "{all:?}");
    assert!(!all.contains(&Value::Int64(1)), "{all:?}");
}

#[test]
fn writes_made_during_a_create_reach_the_installed_index() {
    let temp = TempDir::new().unwrap();
    let storage = engine(&temp, 40);
    let opts = options();
    let (def, rows) = storage.with_kg_read(KG, |db| db.plan_index(&opts)).unwrap();
    let built = build_for_request(def, rows, None).unwrap();

    write_meanwhile(&storage);
    let stats = storage
        .with_kg_mut(KG, |db| db.install_created_index(&opts, built, None))
        .unwrap();

    assert_eq!(stats.tuple_count, 40);
    assert_holds_the_writes_made_meanwhile(&storage);
    // A later write reaches it the usual way.
    storage
        .insert_tuples_into(KG, "doc", vec![doc(42, -9.0, 9.0)])
        .unwrap();
    assert_eq!(nearest(&storage, [-9.0, 9.0], 1), vec![Value::Int64(42)]);
}

#[test]
fn writes_made_during_a_rebuild_reach_the_rebuilt_index() {
    let temp = TempDir::new().unwrap();
    let storage = engine(&temp, 50);
    storage.create_index_in(KG, &options(), None).unwrap();
    let doomed: Vec<Tuple> = (41..=50).map(|i| doc(i, i as f32, 1.0)).collect();
    storage.delete_tuples_from(KG, "doc", doomed).unwrap();
    let before = storage.index_stats_in(KG, Some(INDEX)).unwrap();
    assert_eq!(before[0].tombstone_count, 10);

    let (def, rows) = storage
        .with_kg_read(KG, |db| db.plan_rebuild(INDEX))
        .unwrap();
    let built = build_for_request(def, rows, None).unwrap();
    write_meanwhile(&storage);
    let stats = storage
        .with_kg_mut(KG, |db| db.install_rebuilt_index(built, None))
        .unwrap();

    // Tombstones: the removed row and the moved row's old vector.
    assert_eq!((stats.tuple_count, stats.tombstone_count), (40, 2));
    assert_holds_the_writes_made_meanwhile(&storage);
}

#[test]
fn a_build_stopped_by_its_request_creates_nothing() {
    let temp = TempDir::new().unwrap();
    let storage = engine(&temp, 40);
    let cancelled = RequestControl::new(None);
    cancelled.cancel();
    let expired = RequestControl::new(Some(Instant::now()));
    for (control, stop) in [(cancelled, Stop::Cancelled), (expired, Stop::Deadline)] {
        let err = storage
            .create_index_in(KG, &options(), Some(&*control))
            .unwrap_err();
        assert_eq!(err.to_string(), stop.message());
        assert_eq!(index_names(&storage), Vec::<String>::new());
    }
    storage.create_index_in(KG, &options(), None).unwrap();
    assert_eq!(index_names(&storage), vec![INDEX.to_string()]);
}

#[test]
fn a_rebuild_stopped_by_its_request_keeps_the_old_index() {
    let temp = TempDir::new().unwrap();
    let storage = engine(&temp, 40);
    storage.create_index_in(KG, &options(), None).unwrap();
    storage
        .delete_tuples_from(KG, "doc", vec![doc(1, 1.0, 1.0)])
        .unwrap();
    let control = RequestControl::new(None);
    control.cancel();

    let err = storage
        .rebuild_index_in(KG, INDEX, Some(&*control))
        .unwrap_err();

    assert_eq!(err.to_string(), Stop::Cancelled.message());
    let stats = storage.index_stats_in(KG, Some(INDEX)).unwrap();
    assert_eq!((stats[0].tuple_count, stats[0].tombstone_count), (39, 1));
}

#[test]
fn a_request_stopped_after_its_build_installs_nothing() {
    let temp = TempDir::new().unwrap();
    let storage = engine(&temp, 40);
    let opts = options();
    let control = RequestControl::new(None);
    let (def, rows) = storage.with_kg_read(KG, |db| db.plan_index(&opts)).unwrap();
    let built = build_for_request(def, rows, Some(&*control)).unwrap();

    control.cancel();
    let err = storage
        .with_kg_mut(KG, |db| {
            db.install_created_index(&opts, built, Some(&*control))
        })
        .unwrap_err();

    assert_eq!(err.to_string(), Stop::Cancelled.message());
    assert_eq!(index_names(&storage), Vec::<String>::new());
}

#[test]
fn a_build_superseded_meanwhile_installs_nothing() {
    let temp = TempDir::new().unwrap();
    let storage = engine(&temp, 40);
    let opts = options();

    // Another `.index create` of the same name finished first.
    let (def, rows) = storage.with_kg_read(KG, |db| db.plan_index(&opts)).unwrap();
    let built = build_for_request(def, rows, None).unwrap();
    storage.create_index_in(KG, &opts, None).unwrap();
    let err = storage
        .with_kg_mut(KG, |db| db.install_created_index(&opts, built, None))
        .unwrap_err();
    assert!(err.to_string().contains("already exists"), "{err}");

    // The index was dropped and created again with other parameters while
    // it was rebuilt.
    let (def, rows) = storage
        .with_kg_read(KG, |db| db.plan_rebuild(INDEX))
        .unwrap();
    let built = build_for_request(def, rows, None).unwrap();
    storage.drop_index_in(KG, INDEX).unwrap();
    let redefined = IndexCreateOptions {
        m: Some(8),
        ..options()
    };
    storage.create_index_in(KG, &redefined, None).unwrap();
    let err = storage
        .with_kg_mut(KG, |db| db.install_rebuilt_index(built, None))
        .unwrap_err();
    assert!(err.to_string().contains("redefined"), "{err}");
    let in_place = storage
        .with_kg_read(KG, |db| {
            Ok(db.indexes.get(INDEX).unwrap().definition.clone())
        })
        .unwrap();
    assert_eq!(in_place.index_type.hnsw_config().m, 8);
}

#[test]
fn a_row_written_during_a_build_that_the_index_cannot_hold_fails_the_create() {
    let temp = TempDir::new().unwrap();
    let storage = engine(&temp, 40);
    let opts = IndexCreateOptions {
        metric: Some("cosine".into()),
        ..options()
    };
    let (def, rows) = storage.with_kg_read(KG, |db| db.plan_index(&opts)).unwrap();
    let built = build_for_request(def, rows, None).unwrap();

    // A zero vector has no cosine direction. It is not checked against the
    // index, which is not installed yet.
    storage
        .insert_tuples_into(KG, "doc", vec![doc(99, 0.0, 0.0)])
        .unwrap();
    let err = storage
        .with_kg_mut(KG, |db| db.install_created_index(&opts, built, None))
        .unwrap_err();

    assert!(err.to_string().contains("row id=99"), "{err}");
    assert_eq!(index_names(&storage), Vec::<String>::new());
}
