//! Persist round-trips: every Value survives flush and restart, shard files never collide,
//! and v1 data dirs migrate without loss.

use inputlayer::config::DurabilityMode;
use inputlayer::storage::persist::batch::Update;
use inputlayer::storage::persist::{
    consolidate_to_current, to_tuples, FilePersist, PersistBackend, PersistConfig,
};
use inputlayer::value::{Tuple, Value};
use inputlayer::{Config, StorageEngine};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn engine(dir: &Path) -> StorageEngine {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    config.storage.performance.num_threads = 2;
    StorageEngine::new(config).expect("create storage engine")
}

fn persist(path: PathBuf) -> FilePersist {
    FilePersist::new(PersistConfig {
        path,
        buffer_size: 1000,
        durability_mode: DurabilityMode::Immediate,
        ..Default::default()
    })
    .expect("create persist")
}

fn query(storage: &StorageEngine, kg: &str, program: &str) -> BTreeSet<Tuple> {
    storage
        .execute_query_tuples_on(kg, program)
        .expect("query")
        .into_iter()
        .collect()
}

fn current(p: &FilePersist, shard: &str) -> BTreeSet<Tuple> {
    let mut updates = p.read(shard, 0).expect("read shard");
    consolidate_to_current(&mut updates);
    to_tuples(&updates).into_iter().collect()
}

fn t(values: Vec<Value>) -> Tuple {
    Tuple::new(values)
}

fn every_variant() -> Vec<Value> {
    vec![
        Value::Int32(-3),
        Value::Int64(1 << 40),
        Value::Float64(1.5),
        Value::Float64(f64::NEG_INFINITY),
        Value::string("Sam"),
        Value::string(""),
        Value::Bool(true),
        Value::Null,
        Value::vector(vec![1.0, 2.0]),
        Value::vector(vec![1.0, 2.0, 3.0]),
        Value::vector(vec![]),
        Value::vector_int8(vec![-128, 0, 127]),
        Value::vector_int8(vec![5]),
        Value::Timestamp(1_700_000_000_000),
    ]
}

#[test]
fn mixed_type_attribute_column_survives_restart() {
    let temp = TempDir::new().unwrap();
    let expected: BTreeSet<Tuple> = [
        t(vec![Value::string("age"), Value::Int64(30)]),
        t(vec![Value::string("name"), Value::string("Sam")]),
        t(vec![Value::string("h"), Value::Float64(1.5)]),
    ]
    .into_iter()
    .collect();
    {
        let storage = engine(temp.path());
        storage.create_knowledge_graph("kg").unwrap();
        storage
            .insert_tuples_into("kg", "attr", expected.iter().cloned().collect())
            .unwrap();
        storage.save_all().unwrap();
    }
    let storage = engine(temp.path());
    assert_eq!(
        query(&storage, "kg", "result(K, V) <- attr(K, V)"),
        expected
    );
}

#[test]
fn every_value_variant_in_one_column_survives_restart() {
    let temp = TempDir::new().unwrap();
    let expected: BTreeSet<Tuple> = every_variant()
        .into_iter()
        .enumerate()
        .map(|(i, v)| t(vec![Value::Int32(i as i32), v]))
        .collect();
    {
        let storage = engine(temp.path());
        storage.create_knowledge_graph("kg").unwrap();
        storage
            .insert_tuples_into("kg", "mixed", expected.iter().cloned().collect())
            .unwrap();
        storage.save_all().unwrap();
    }
    let storage = engine(temp.path());
    assert_eq!(
        query(&storage, "kg", "result(I, V) <- mixed(I, V)"),
        expected
    );
}

#[test]
fn every_value_variant_survives_flush_compaction_and_restart() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();
    // Null first: the first row must not decide the column type.
    let mut values = every_variant();
    values.rotate_left(7);
    let updates: Vec<Update> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Update::insert(t(vec![v.clone(), Value::Int32(i as i32)]), i as u64))
        .collect();
    let expected: BTreeSet<Tuple> = updates.iter().map(|u| u.data.clone()).collect();
    assert_eq!(values[0], Value::Null);
    {
        let p = persist(path.clone());
        p.ensure_shard("kg:mixed").unwrap();
        p.append("kg:mixed", &updates).unwrap();
        p.flush("kg:mixed").unwrap();
        assert_eq!(current(&p, "kg:mixed"), expected);
        p.compact("kg:mixed", 0).unwrap();
        assert_eq!(current(&p, "kg:mixed"), expected);
    }
    let p = persist(path);
    assert_eq!(current(&p, "kg:mixed"), expected);
}

#[test]
fn vectors_of_different_dims_survive_restart() {
    let temp = TempDir::new().unwrap();
    let expected: BTreeSet<Tuple> = [
        t(vec![Value::Int32(1), Value::vector(vec![1.0, 2.0])]),
        t(vec![Value::Int32(2), Value::vector(vec![1.0, 2.0, 3.0])]),
    ]
    .into_iter()
    .collect();
    {
        let storage = engine(temp.path());
        storage.create_knowledge_graph("kg").unwrap();
        storage
            .insert_tuples_into("kg", "emb", expected.iter().cloned().collect())
            .unwrap();
        storage.save_all().unwrap();
    }
    let storage = engine(temp.path());
    assert_eq!(query(&storage, "kg", "result(I, V) <- emb(I, V)"), expected);
}

#[test]
fn shards_with_colliding_sanitized_names_survive_restart() {
    let temp = TempDir::new().unwrap();
    {
        let storage = engine(temp.path());
        storage.create_knowledge_graph("a").unwrap();
        storage.create_knowledge_graph("a_b").unwrap();
        storage.insert_into("a", "b_c", vec![(1, 1)]).unwrap();
        storage.insert_into("a_b", "c", vec![(2, 2)]).unwrap();
        storage.save_all().unwrap();
    }
    let storage = engine(temp.path());
    assert_eq!(
        storage
            .execute_query_on("a", "result(X, Y) <- b_c(X, Y)")
            .unwrap(),
        vec![(1, 1)]
    );
    assert_eq!(
        storage
            .execute_query_on("a_b", "result(X, Y) <- c(X, Y)")
            .unwrap(),
        vec![(2, 2)]
    );
}

#[test]
fn shards_differing_only_in_case_get_distinct_files() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();
    {
        let p = persist(path.clone());
        for (shard, n) in [("kg:Edge", 1), ("kg:edge", 2)] {
            p.ensure_shard(shard).unwrap();
            p.append(shard, &[Update::insert(Tuple::from_pair(n, n), 1)])
                .unwrap();
            p.flush(shard).unwrap();
        }
    }
    let p = persist(path);
    assert_eq!(
        current(&p, "kg:Edge"),
        [Tuple::from_pair(1, 1)].into_iter().collect()
    );
    assert_eq!(
        current(&p, "kg:edge"),
        [Tuple::from_pair(2, 2)].into_iter().collect()
    );
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("copy fixture");
    for entry in std::fs::read_dir(src).expect("copy fixture") {
        let entry = entry.expect("copy fixture");
        let target = dst.join(entry.file_name());
        if entry.file_type().expect("copy fixture").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).expect("copy fixture");
        }
    }
}

fn v1_fixture() -> TempDir {
    let temp = TempDir::new().expect("copy fixture");
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/persist_v1");
    copy_dir(&src, temp.path());
    temp
}

/// The fixture as v1 stored it; v1 wrote timestamps as plain Int64.
fn assert_v1_fixture_contents(p: &FilePersist) {
    assert_eq!(
        current(p, "fx:edge"),
        [Tuple::from_pair(2, 3), Tuple::from_pair(3, 4)]
            .into_iter()
            .collect()
    );
    assert_eq!(
        current(p, "fx:person"),
        [
            t(vec![
                Value::string("sam"),
                Value::Int64(1 << 40),
                Value::Float64(1.5),
                Value::Bool(true),
                Value::Int64(1_700_000_000_000),
            ]),
            t(vec![
                Value::string("ana"),
                Value::Int64(-7),
                Value::Float64(-0.25),
                Value::Bool(false),
                Value::Int64(0),
            ]),
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(
        current(p, "fx:emb"),
        [
            t(vec![
                Value::Int32(1),
                Value::vector(vec![0.5, 1.0, -2.0]),
                Value::vector_int8(vec![1, -2]),
            ]),
            t(vec![
                Value::Int32(2),
                Value::vector(vec![3.0, 4.0, 5.0]),
                Value::vector_int8(vec![127, -128]),
            ]),
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(
        current(p, "fx:pending"),
        [t(vec![Value::string("wal"), Value::Int32(9)])]
            .into_iter()
            .collect()
    );
}

#[test]
fn v1_data_dir_migrates_without_loss() {
    let temp = v1_fixture();
    let path = temp.path().to_path_buf();
    {
        let p = persist(path.clone());
        assert_v1_fixture_contents(&p);
        assert_eq!(p.shard_info("fx:edge").unwrap().batch_count, 2);
    }
    let shards: Vec<String> = std::fs::read_dir(path.join("shards"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        shards.iter().all(|s| !s.starts_with("fx_")),
        "v1 meta files remain: {shards:?}"
    );
    for s in &shards {
        let meta = std::fs::read_to_string(path.join("shards").join(s)).unwrap();
        assert!(meta.contains("\"version\": 2"), "{s} not migrated: {meta}");
    }

    let p = persist(path);
    assert_v1_fixture_contents(&p);
}

#[test]
fn v1_orphan_batches_are_quarantined_not_deleted() {
    let temp = v1_fixture();
    let path = temp.path().to_path_buf();
    std::fs::copy(
        path.join("batches/1.parquet"),
        path.join("batches/99.parquet"),
    )
    .unwrap();
    let p = persist(path.clone());
    assert_v1_fixture_contents(&p);
    assert!(!path.join("batches/99.parquet").exists());
    assert!(path.join("batches/quarantine/99.parquet").exists());
}

#[test]
fn duplicate_shard_meta_blocks_orphan_cleanup() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();
    {
        let p = persist(path.clone());
        p.ensure_shard("kg:edge").unwrap();
        p.append("kg:edge", &[Update::insert(Tuple::from_pair(1, 2), 1)])
            .unwrap();
        p.flush("kg:edge").unwrap();
    }
    let shards = path.join("shards");
    let meta = std::fs::read_dir(&shards)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "json"))
        .unwrap();
    std::fs::copy(&meta, shards.join("copy.json")).unwrap();
    std::fs::write(path.join("batches/77.parquet"), b"orphan").unwrap();

    assert!(FilePersist::new(PersistConfig {
        path: path.clone(),
        ..Default::default()
    })
    .is_err());
    assert!(path.join("batches/77.parquet").exists());
}

#[test]
fn v1_orphans_are_quarantined_before_v1_metas_are_removed() {
    let temp = v1_fixture();
    let path = temp.path().to_path_buf();
    let person_batch = path.join("batches/3.parquet");
    let original = std::fs::read(&person_batch).unwrap();
    std::fs::copy(
        path.join("batches/1.parquet"),
        path.join("batches/99.parquet"),
    )
    .unwrap();
    std::fs::write(&person_batch, b"corrupt").unwrap();

    assert!(FilePersist::new(PersistConfig {
        path: path.clone(),
        ..Default::default()
    })
    .is_err());
    assert!(path.join("shards/fx_person.json").exists());
    assert!(path.join("batches/quarantine/99.parquet").exists());

    std::fs::write(&person_batch, original).unwrap();
    let p = persist(path.clone());
    assert_v1_fixture_contents(&p);
    assert!(path.join("batches/quarantine/99.parquet").exists());
}

fn write_v1_meta(path: &Path, file: &str, name: &str, batch: u32) {
    let meta = serde_json::json!({
        "version": 1,
        "name": name,
        "batches": [{
            "id": batch.to_string(),
            "path": format!("/v1-fixture/batches/{batch}.parquet"),
            "lower": 0,
            "upper": 10,
            "len": 2
        }],
        "since": 0,
        "upper": 10,
        "total_updates": 2
    });
    std::fs::write(path.join("shards").join(file), meta.to_string()).expect("write v1 meta");
}

#[test]
fn v1_meta_on_another_shards_v2_path_migrates_both() {
    let fixture = v1_fixture();
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();
    std::fs::create_dir_all(path.join("shards")).unwrap();
    std::fs::create_dir_all(path.join("batches")).unwrap();
    for b in ["1", "2"] {
        std::fs::copy(
            fixture.path().join(format!("batches/{b}.parquet")),
            path.join(format!("batches/{b}.parquet")),
        )
        .unwrap();
    }
    // v2 path of `a:b_c` is `a%3Ab_c.json`, the v1 path of `a%3Ab:c`.
    write_v1_meta(&path, "a_b_c.json", "a:b_c", 1);
    write_v1_meta(&path, "a%3Ab_c.json", "a%3Ab:c", 2);

    let expected = |batch: &str| {
        let solo = TempDir::new().unwrap();
        std::fs::create_dir_all(solo.path().join("shards")).unwrap();
        std::fs::create_dir_all(solo.path().join("batches")).unwrap();
        std::fs::copy(
            fixture.path().join(format!("batches/{batch}.parquet")),
            solo.path().join(format!("batches/{batch}.parquet")),
        )
        .unwrap();
        write_v1_meta(solo.path(), "s.json", "s:s", batch.parse().unwrap());
        current(&persist(solo.path().to_path_buf()), "s:s")
    };

    for _ in 0..2 {
        let p = persist(path.clone());
        assert_eq!(current(&p, "a:b_c"), expected("1"));
        assert_eq!(current(&p, "a%3Ab:c"), expected("2"));
        assert!(!current(&p, "a:b_c").is_empty());
        assert!(!current(&p, "a%3Ab:c").is_empty());
    }
    assert!(!path.join("batches/quarantine").exists());
}
