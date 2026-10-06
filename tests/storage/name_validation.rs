//! KG and relation name grammar, and shard ownership for legacy names.

// Test setup aborts on failure; `unwrap` is the intended behavior.
#![allow(clippy::unwrap_used)]

use inputlayer::protocol::{ErrorCode, Handler};
use inputlayer::storage::persist::{FilePersist, PersistBackend, PersistConfig, Transaction};
use inputlayer::storage::{
    DropTombstones, KnowledgeGraphInfo, KnowledgeGraphsMetadata, RelationTombstone, StorageError,
};
use inputlayer::{Config, DurabilityMode, RelationSchema, StorageEngine, Tuple, Value};
use std::path::Path;
use tempfile::TempDir;

fn config(dir: &Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    config.storage.performance.num_threads = 2;
    config
}

fn tuple(v: i64) -> Tuple {
    Tuple::new(vec![Value::Int64(v)])
}

fn is_invalid_name<T: std::fmt::Debug>(r: Result<T, StorageError>) -> bool {
    matches!(r, Err(StorageError::InvalidName(_)))
}

/// Write shards and KG metadata the way a pre-grammar build could have.
fn seed_legacy(dir: &Path, kgs: Option<&[&str]>, shards: &[(&str, &[i64])]) {
    let persist = FilePersist::new(PersistConfig {
        path: dir.join("persist"),
        durability_mode: DurabilityMode::Immediate,
        ..Default::default()
    })
    .unwrap();
    for (i, (shard, rows)) in shards.iter().enumerate() {
        let mut txn = Transaction::new(i as u64 + 1);
        txn.insert(*shard, rows.iter().map(|v| tuple(*v)));
        persist.commit(txn).unwrap();
        persist.flush(shard).unwrap();
    }
    if let Some(kgs) = kgs {
        let metadata = KnowledgeGraphsMetadata {
            version: "1.0".to_string(),
            knowledge_graphs: kgs
                .iter()
                .map(|name| KnowledgeGraphInfo {
                    name: (*name).to_string(),
                    created_at: String::new(),
                    last_accessed: String::new(),
                    relations_count: 0,
                    total_tuples: 0,
                })
                .collect(),
        };
        std::fs::create_dir_all(dir.join("metadata")).unwrap();
        metadata
            .save(&dir.join("metadata/knowledge_graphs.json"))
            .unwrap();
    }
}

fn relations(storage: &StorageEngine, kg: &str) -> Vec<(String, usize)> {
    let mut rels: Vec<(String, usize)> = storage
        .list_relations_with_typed_metadata_in(kg)
        .unwrap()
        .into_iter()
        .map(|(name, _, count)| (name, count))
        .collect();
    rels.sort();
    rels
}

#[test]
fn create_rejects_non_canonical_kg_names() {
    let temp = TempDir::new().unwrap();
    let storage = StorageEngine::new(config(temp.path())).unwrap();
    for name in ["user:42", "User", "_hidden", "a-b", "a/b", "..", ""] {
        assert!(
            is_invalid_name(storage.create_knowledge_graph(name)),
            "{name:?}"
        );
    }
    storage.create_knowledge_graph("user_42").unwrap();
}

#[test]
fn writes_reject_non_canonical_relation_names() {
    let temp = TempDir::new().unwrap();
    let storage = StorageEngine::new(config(temp.path())).unwrap();
    for rel in ["__dunder", "Foo", "a:b", "a-b"] {
        assert!(
            is_invalid_name(storage.insert_tuples_into("default", rel, vec![tuple(1)])),
            "insert {rel}"
        );
        assert!(
            is_invalid_name(storage.delete_tuples_from("default", rel, vec![tuple(1)])),
            "delete {rel}"
        );
        assert!(
            is_invalid_name(storage.register_schema_in("default", RelationSchema::new(rel))),
            "schema {rel}"
        );
        assert!(
            is_invalid_name(
                storage.register_or_update_schema_in("default", RelationSchema::new(rel))
            ),
            "upsert schema {rel}"
        );
        assert!(
            is_invalid_name(
                storage.register_or_update_session_schema_in("default", RelationSchema::new(rel))
            ),
            "session schema {rel}"
        );
    }
    assert!(storage.list_relations_in("default").unwrap().is_empty());
}

#[tokio::test]
async fn iql_rejects_invalid_names() {
    let temp = TempDir::new().unwrap();
    let handler = Handler::new(StorageEngine::new(config(temp.path())).unwrap());

    let out = handler
        .query_program(None, ".kg create user:42".to_string())
        .await;
    let text = format!("{out:?}");
    assert!(
        text.contains("must start with") || text.contains("may only contain"),
        "{text}"
    );

    for stmt in ["+__dunder[(\"a\",)]", "+Foo(1)"] {
        let result = handler.query_program(None, stmt.to_string()).await.unwrap();
        assert_eq!(result.errors.len(), 1, "{stmt}: {result:?}");
        assert_eq!(result.errors[0].code, ErrorCode::Validation, "{stmt}");
        assert!(
            result.errors[0]
                .message
                .contains("must start with a lowercase letter"),
            "{stmt}: {result:?}"
        );
    }
    let storage = handler.get_storage();
    assert!(!storage
        .list_knowledge_graphs()
        .contains(&"user:42".to_string()));
    assert!(storage.list_relations_in("default").unwrap().is_empty());
}

#[test]
fn legacy_colon_kg_creates_no_phantom_kg() {
    let temp = TempDir::new().unwrap();
    seed_legacy(
        temp.path(),
        Some(&["default", "user:42"]),
        &[("user:42:fact", &[1, 2])],
    );
    let storage = StorageEngine::new(config(temp.path())).unwrap();
    assert_eq!(storage.list_knowledge_graphs(), ["default", "user:42"]);
    assert_eq!(relations(&storage, "user:42"), [("fact".to_string(), 2)]);
}

#[test]
fn dropping_prefix_kg_keeps_legacy_colon_kg() {
    let temp = TempDir::new().unwrap();
    seed_legacy(
        temp.path(),
        Some(&["default", "user", "user:42"]),
        &[("user:fact", &[1]), ("user:42:fact", &[1, 2, 3])],
    );
    {
        let storage = StorageEngine::new(config(temp.path())).unwrap();
        assert_eq!(
            storage.list_knowledge_graphs(),
            ["default", "user", "user:42"]
        );
        assert_eq!(relations(&storage, "user"), [("fact".to_string(), 1)]);
        storage.drop_knowledge_graph("user").unwrap();
        assert_eq!(relations(&storage, "user:42"), [("fact".to_string(), 3)]);
    }
    let storage = StorageEngine::new(config(temp.path())).unwrap();
    assert_eq!(storage.list_knowledge_graphs(), ["default", "user:42"]);
    assert_eq!(relations(&storage, "user:42"), [("fact".to_string(), 3)]);
}

#[test]
fn legacy_invalid_names_stay_droppable() {
    let temp = TempDir::new().unwrap();
    seed_legacy(
        temp.path(),
        Some(&["default", "user:42", "ok"]),
        &[
            ("user:42:fact", &[1]),
            ("ok:__dunder", &[1]),
            ("ok:edge", &[1]),
        ],
    );
    {
        let mut storage = StorageEngine::new(config(temp.path())).unwrap();
        assert!(is_invalid_name(storage.insert_tuples_into(
            "user:42",
            "fact",
            vec![tuple(9)]
        )));
        storage.drop_knowledge_graph("user:42").unwrap();
        storage.use_knowledge_graph("ok").unwrap();
        storage.drop_relation_in("ok", "__dunder").unwrap();
    }
    let storage = StorageEngine::new(config(temp.path())).unwrap();
    assert_eq!(storage.list_knowledge_graphs(), ["default", "ok"]);
    assert_eq!(relations(&storage, "ok"), [("edge".to_string(), 1)]);
}

#[test]
fn shards_without_metadata_still_load() {
    let temp = TempDir::new().unwrap();
    seed_legacy(temp.path(), None, &[("kg1:edge", &[1, 2])]);
    let storage = StorageEngine::new(config(temp.path())).unwrap();
    assert_eq!(storage.list_knowledge_graphs(), ["default", "kg1"]);
    assert_eq!(relations(&storage, "kg1"), [("edge".to_string(), 2)]);
}

#[test]
fn legacy_colon_kg_without_metadata_creates_no_phantom_kg() {
    let temp = TempDir::new().unwrap();
    seed_legacy(temp.path(), None, &[("user:42:fact", &[1, 2])]);
    let storage = StorageEngine::new(config(temp.path())).unwrap();
    assert_eq!(storage.list_knowledge_graphs(), ["default", "user:42"]);
    assert_eq!(relations(&storage, "user:42"), [("fact".to_string(), 2)]);
}

#[test]
fn stale_metadata_keeps_legacy_colon_kg_separate() {
    let temp = TempDir::new().unwrap();
    seed_legacy(
        temp.path(),
        Some(&["default", "user"]),
        &[("user:fact", &[1]), ("user:42:fact", &[1, 2, 3])],
    );
    {
        let storage = StorageEngine::new(config(temp.path())).unwrap();
        assert_eq!(
            storage.list_knowledge_graphs(),
            ["default", "user", "user:42"]
        );
        assert_eq!(relations(&storage, "user"), [("fact".to_string(), 1)]);
        storage.drop_knowledge_graph("user").unwrap();
    }
    let storage = StorageEngine::new(config(temp.path())).unwrap();
    assert_eq!(storage.list_knowledge_graphs(), ["default", "user:42"]);
    assert_eq!(relations(&storage, "user:42"), [("fact".to_string(), 3)]);
}

/// Every loaded KG with its relations and tuple counts.
fn ownership(storage: &StorageEngine) -> Vec<(String, Vec<(String, usize)>)> {
    storage
        .list_knowledge_graphs()
        .into_iter()
        .map(|kg| {
            let rels = relations(storage, &kg);
            (kg, rels)
        })
        .collect()
}

#[test]
fn mixed_legacy_and_canonical_names_load_exact_ownership() {
    let temp = TempDir::new().unwrap();
    seed_legacy(
        temp.path(),
        Some(&["default", "user", "user:42", "user:42:x", "team", "team_a"]),
        &[
            ("user:fact", &[1]),
            ("user:42:fact", &[1, 2]),
            ("user:42:gone", &[1]),
            ("user:42:x:fact", &[1, 2, 3]),
            ("team:edge", &[1]),
            ("team_a:edge", &[1, 2]),
            ("team:a:edge", &[1, 2, 3, 4]),
            ("orphan:edge", &[5]),
        ],
    );
    let mut tombstones = DropTombstones::default();
    tombstones.knowledge_graphs.insert("user:42:x".to_string());
    tombstones
        .relations
        .insert(RelationTombstone::new("user:42", "gone"));
    tombstones
        .save(&temp.path().join("metadata/dropping.json"))
        .unwrap();

    let rel = |name: &str, count| (name.to_string(), count);
    let expected = vec![
        ("default".to_string(), vec![]),
        ("orphan".to_string(), vec![rel("edge", 1)]),
        ("team".to_string(), vec![rel("edge", 1)]),
        ("team:a".to_string(), vec![rel("edge", 4)]),
        ("team_a".to_string(), vec![rel("edge", 2)]),
        ("user".to_string(), vec![rel("fact", 1)]),
        ("user:42".to_string(), vec![rel("fact", 2)]),
    ];
    let first = ownership(&StorageEngine::new(config(temp.path())).unwrap());
    assert_eq!(first, expected);
    // The finished drops are gone from disk, so a restart loads the same.
    let restarted = ownership(&StorageEngine::new(config(temp.path())).unwrap());
    assert_eq!(restarted, expected);
    assert_eq!(
        DropTombstones::load(&temp.path().join("metadata/dropping.json")).unwrap(),
        DropTombstones::default()
    );
}

#[test]
fn create_rolls_back_when_metadata_save_fails() {
    let temp = TempDir::new().unwrap();
    let storage = StorageEngine::new(config(temp.path())).unwrap();
    let metadata = temp.path().join("metadata");
    if metadata.exists() {
        std::fs::remove_dir_all(&metadata).unwrap();
    }
    std::fs::write(&metadata, b"").unwrap();
    assert!(storage.create_knowledge_graph("orphan").is_err());
    assert!(!storage
        .list_knowledge_graphs()
        .contains(&"orphan".to_string()));
}
