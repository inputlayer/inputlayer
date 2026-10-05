//! Residency: a restarted engine registers knowledge graphs without loading
//! them, loads each on first use exactly once, and unloads idle ones only when
//! nothing uses them, invisibly. Crash and restart cycles lose no commit.
#![allow(clippy::unwrap_used)]

use super::*;
use crate::config::{Config, DurabilityMode};
use crate::schema::{ColumnSchema, RelationSchema, SchemaType};
use crate::value::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Barrier;
use tempfile::TempDir;

fn config(dir: &Path, max_loaded: usize) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    config.storage.performance.num_threads = 2;
    config.storage.persist.durability_mode = DurabilityMode::Immediate;
    config.storage.max_loaded_knowledge_graphs = max_loaded;
    config
}

fn open(dir: &Path) -> StorageEngine {
    StorageEngine::new(config(dir, 0)).unwrap()
}

fn open_limited(dir: &Path, max_loaded: usize) -> StorageEngine {
    StorageEngine::new(config(dir, max_loaded)).unwrap()
}

/// Stop without any shutdown work: no flush, no metadata save.
fn crash(storage: StorageEngine) {
    std::mem::forget(Arc::clone(&storage.persist));
    drop(storage);
}

fn int(v: i64) -> Tuple {
    Tuple::new(vec![Value::Int64(v)])
}

fn insert(storage: &StorageEngine, kg: &str, relation: &str, values: &[i64]) {
    storage
        .insert_tuples_into(kg, relation, values.iter().map(|&v| int(v)).collect())
        .unwrap();
}

fn rows(storage: &StorageEngine, kg: &str, relation: &str) -> BTreeSet<Tuple> {
    storage
        .get_snapshot_for(kg)
        .unwrap()
        .input_tuples
        .get(relation)
        .map(|tuples| tuples.iter().cloned().collect())
        .unwrap_or_default()
}

fn loaded(storage: &StorageEngine, kg: &str) -> bool {
    storage.is_knowledge_graph_loaded(kg).unwrap()
}

/// Two KGs with facts, a rule and a schema, written by an engine that then
/// crashed.
fn seed(dir: &Path) {
    let storage = open(dir);
    for kg in ["a", "b"] {
        storage.create_knowledge_graph(kg).unwrap();
        insert(&storage, kg, "e", &[1, 2, 3]);
    }
    let rule = crate::statement::parse_rule_definition("big(X) <- e(X), X > 1").unwrap();
    storage.register_rule_in("a", &rule).unwrap();
    let schema = RelationSchema::new("typed").with_column(ColumnSchema::new("x", SchemaType::Int));
    storage.register_schema_in("b", schema).unwrap();
    crash(storage);
}

#[test]
fn restart_registers_knowledge_graphs_without_loading_them() {
    let temp = TempDir::new().unwrap();
    seed(temp.path());

    let storage = open(temp.path());
    assert_eq!(storage.loaded_knowledge_graph_count(), 0);
    assert_eq!(storage.list_knowledge_graphs(), ["a", "b", "default"]);
    let summaries: BTreeMap<String, KgSummary> =
        storage.knowledge_graph_summaries().into_iter().collect();
    assert_eq!(summaries["a"].rules, 1, "rule count read without loading");
    assert!(summaries.values().all(|summary| !summary.loaded));
    assert_eq!(
        storage.loaded_knowledge_graph_count(),
        0,
        "stats load nothing"
    );

    let derived = storage
        .execute_query_with_rules_tuples_on("a", "q(X) <- big(X)")
        .unwrap();
    assert_eq!(derived.len(), 2);
    assert!(loaded(&storage, "a"));
    assert!(!loaded(&storage, "b"));
    assert_eq!(storage.loaded_knowledge_graph_count(), 1);
    assert!(storage.has_schema_in("b", "typed").unwrap());
    assert_eq!(rows(&storage, "b", "e"), [int(1), int(2), int(3)].into());
    assert_eq!(storage.loaded_knowledge_graph_count(), 2);

    let summaries: BTreeMap<String, KgSummary> =
        storage.knowledge_graph_summaries().into_iter().collect();
    assert_eq!(
        summaries["a"],
        KgSummary {
            relations: 1,
            tuples: 3,
            rules: 1,
            loaded: true
        }
    );
}

#[test]
fn concurrent_first_use_loads_once() {
    let temp = TempDir::new().unwrap();
    seed(temp.path());
    let storage = open(temp.path());
    let barrier = Barrier::new(8);
    let handles: Vec<_> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    storage.kg_handle("a").unwrap()
                })
            })
            .collect();
        workers.into_iter().map(|w| w.join().unwrap()).collect()
    });
    assert!(handles.iter().all(|h| Arc::ptr_eq(h, &handles[0])));
    assert_eq!(storage.loaded_knowledge_graph_count(), 1);
}

#[test]
fn revisions_continue_above_catalog_files_after_a_crash() {
    let temp = TempDir::new().unwrap();
    let saved = {
        let storage = open(temp.path());
        storage.create_knowledge_graph("a").unwrap();
        // A catalog-only commit: its revision is in the catalog files only,
        // since the WAL drops catalog changes once the files are saved.
        let rule = crate::statement::parse_rule_definition("p(X) <- e(X)").unwrap();
        storage.register_rule_in("a", &rule).unwrap();
        let saved = storage
            .kg_handle("a")
            .unwrap()
            .read()
            .rule_catalog
            .revision();
        assert!(saved > 0);
        crash(storage);
        saved
    };
    let storage = open(temp.path());
    assert!(!loaded(&storage, "a"));
    assert!(storage.logical_time.load(Ordering::SeqCst) > saved);
    let rule = crate::statement::parse_rule_definition("p(X) <- f(X)").unwrap();
    storage.register_rule_in("a", &rule).unwrap();
    assert!(
        storage
            .kg_handle("a")
            .unwrap()
            .read()
            .rule_catalog
            .revision()
            > saved
    );
    crash(storage);
    let storage = open(temp.path());
    assert_eq!(storage.rule_count_in("a", "p").unwrap(), Some(2));
}

#[test]
fn an_unloaded_knowledge_graph_reloads_unchanged_under_its_revision() {
    let temp = TempDir::new().unwrap();
    seed(temp.path());
    let storage = open_limited(temp.path(), 1);

    let first = storage.get_snapshot_for("a").unwrap();
    let revision = first.revision;
    let changes = first.changes().clone();
    let tuples = rows(&storage, "a", "e");
    drop(first);
    storage.get_snapshot_for("b").unwrap();
    assert!(!loaded(&storage, "a"), "loading b unloads a");
    assert!(loaded(&storage, "b"));
    assert_eq!(storage.loaded_knowledge_graph_count(), 1);
    let summaries: BTreeMap<String, KgSummary> =
        storage.knowledge_graph_summaries().into_iter().collect();
    assert_eq!(summaries["a"].tuples, 3, "an unloaded KG keeps its counts");
    assert!(!summaries["a"].loaded);

    let again = storage.get_snapshot_for("a").unwrap();
    assert_eq!(again.revision, revision, "same state, same revision");
    assert_eq!(
        *again.changes(),
        changes,
        "an expect_revision precondition checks as before the unload"
    );
    assert_eq!(rows(&storage, "a", "e"), tuples);
    drop(again);

    // Writes after a reload land and survive further unloads and a restart.
    insert(&storage, "a", "e", &[4]);
    storage.get_snapshot_for("b").unwrap();
    assert!(!loaded(&storage, "a"));
    let after = storage.get_snapshot_for("a").unwrap();
    assert!(after.revision > revision);
    assert!(rows(&storage, "a", "e").contains(&int(4)));
    drop(after);
    assert_eq!(storage.knowledge_graph_residency_counts(), (5, 4));
    crash(storage);
    let storage = open(temp.path());
    assert_eq!(
        rows(&storage, "a", "e"),
        [1, 2, 3, 4].map(int).into_iter().collect()
    );
}

#[test]
fn a_knowledge_graph_in_use_stays_loaded() {
    let temp = TempDir::new().unwrap();
    seed(temp.path());
    let storage = open(temp.path());
    let idle = Duration::ZERO;

    // A held snapshot: a query or evaluation still reads it.
    let snapshot = storage.get_snapshot_for("a").unwrap();
    assert_eq!(storage.unload_idle_knowledge_graphs(idle), 0);
    assert!(loaded(&storage, "a"));
    drop(snapshot);

    // A staged write: its commit checks its reads against the snapshot.
    let staged = storage.staging_base("a").unwrap();
    assert_eq!(storage.unload_idle_knowledge_graphs(idle), 0);
    drop(staged);

    // A held handle: a request is running.
    let handle = storage.kg_handle("a").unwrap();
    assert_eq!(storage.unload_idle_knowledge_graphs(idle), 0);
    drop(handle);

    // A pin: a subscription stands on it.
    let pin = storage.pin_knowledge_graph("a").unwrap();
    assert_eq!(storage.unload_idle_knowledge_graphs(idle), 0);
    drop(pin);

    assert_eq!(storage.unload_idle_knowledge_graphs(idle), 1);
    assert!(!loaded(&storage, "a"));
    assert_eq!(storage.loaded_knowledge_graph_count(), 0);
}

#[test]
fn a_knowledge_graph_with_state_only_in_memory_stays_loaded() {
    let temp = TempDir::new().unwrap();
    seed(temp.path());
    let storage = open(temp.path());
    let session =
        RelationSchema::new("scratch").with_column(ColumnSchema::new("x", SchemaType::Int));
    storage
        .register_or_update_session_schema_in("a", session)
        .unwrap();
    storage
        .with_kg_mut("b", |kg| kg.enable_incremental().map_err(|e| e.to_string()))
        .unwrap();
    assert_eq!(storage.unload_idle_knowledge_graphs(Duration::ZERO), 0);
    assert!(loaded(&storage, "a") && loaded(&storage, "b"));
}

#[test]
fn idle_unloading_spares_recent_and_internal_knowledge_graphs() {
    let temp = TempDir::new().unwrap();
    seed(temp.path());
    let storage = open(temp.path());
    storage
        .create_knowledge_graph(crate::auth::INTERNAL_KG)
        .unwrap();
    storage.get_snapshot_for("a").unwrap();
    assert_eq!(
        storage.unload_idle_knowledge_graphs(Duration::from_secs(3600)),
        0
    );
    assert_eq!(storage.unload_idle_knowledge_graphs(Duration::ZERO), 1);
    assert!(!loaded(&storage, "a"));
    assert!(loaded(&storage, crate::auth::INTERNAL_KG));
}

/// A writer that got the handle before an unload and the write lock after
/// retries on the reloaded knowledge graph: its commit is never applied to the
/// unloaded copy only.
#[test]
fn a_writer_holding_an_unloaded_handle_commits_to_the_reloaded_graph() {
    let temp = TempDir::new().unwrap();
    seed(temp.path());
    let storage = open(temp.path());
    let stale = storage.kg_handle("a").unwrap();
    {
        // What `try_unload` does, with the writer's handle still out.
        let slot = storage.slot("a").unwrap();
        let mut state = slot.state.lock();
        let mut db = stale.write();
        let (dormant, listing) = db.to_dormant();
        db.retired = Some(Retired::Unloaded);
        drop(db);
        *slot.listing.lock() = listing;
        slot.graph.store(None);
        state.dormant = Some(dormant);
        storage.loaded.fetch_sub(1, Ordering::AcqRel);
    }
    insert(&storage, "a", "e", &[9]);
    assert!(!stale
        .read()
        .store
        .get("e")
        .unwrap()
        .iter()
        .any(|t| *t == int(9)));
    let live = storage.kg_handle("a").unwrap();
    assert!(!Arc::ptr_eq(&stale, &live));
    assert!(rows(&storage, "a", "e").contains(&int(9)));
}

#[test]
fn dropping_a_dormant_knowledge_graph_removes_its_data() {
    let temp = TempDir::new().unwrap();
    seed(temp.path());
    let storage = open(temp.path());
    storage.drop_knowledge_graph("b").unwrap();
    assert!(storage.get_snapshot_for("b").is_err());
    storage.create_knowledge_graph("b").unwrap();
    assert!(rows(&storage, "b", "e").is_empty());
    crash(storage);
    let storage = open(temp.path());
    assert!(rows(&storage, "b", "e").is_empty());
    assert!(!storage.has_schema_in("b", "typed").unwrap());
    assert_eq!(rows(&storage, "a", "e").len(), 3);
}

#[test]
fn a_checkpoint_reads_dormant_knowledge_graphs_without_loading_them() {
    let temp = TempDir::new().unwrap();
    seed(temp.path());
    let storage = open(temp.path());
    insert(&storage, "a", "e", &[4]);
    storage.get_snapshot_for("b").unwrap();
    storage.unload_idle_knowledge_graphs(Duration::ZERO);
    insert(&storage, "a", "e", &[5]);
    assert!(loaded(&storage, "a") && !loaded(&storage, "b"));

    let checkpoint = storage.capture_checkpoint().unwrap();
    let kgs: BTreeMap<&str, usize> = checkpoint
        .knowledge_graphs
        .iter()
        .map(|kg| {
            let facts = kg.relations.iter().map(|(_, tuples)| tuples.len()).sum();
            (kg.name.as_str(), facts)
        })
        .collect();
    assert_eq!(kgs, BTreeMap::from([("a", 5), ("b", 3), ("default", 0)]));
    assert!(!loaded(&storage, "b"), "the capture does not keep b loaded");
}

#[test]
fn an_unreadable_knowledge_graph_fails_alone() {
    let temp = TempDir::new().unwrap();
    seed(temp.path());
    std::fs::write(temp.path().join("a/rules/catalog.json"), b"{ not json").unwrap();
    let storage = open(temp.path());
    for _ in 0..2 {
        let error = storage.get_snapshot_for("a").unwrap_err().to_string();
        assert!(error.contains("view catalog"), "{error}");
    }
    assert!(!loaded(&storage, "a"));
    assert_eq!(rows(&storage, "b", "e").len(), 3);
}

/// The persist layer holds a fact twice; loading clamps it to once with a
/// committed correction, so a later delete removes it for good.
#[test]
fn loading_clamps_drifted_multiplicities_durably() {
    let temp = TempDir::new().unwrap();
    {
        let storage = open(temp.path());
        storage.create_knowledge_graph("a").unwrap();
        insert(&storage, "a", "e", &[1]);
        let mut txn = Transaction::new(storage.logical_time.fetch_add(1, Ordering::SeqCst));
        txn.insert("a:e", [int(1)]);
        storage.persist.commit(txn).unwrap();
        crash(storage);
    }
    let storage = open(temp.path());
    assert_eq!(rows(&storage, "a", "e"), [int(1)].into());
    storage.delete_tuples_from("a", "e", vec![int(1)]).unwrap();
    crash(storage);
    let storage = open(temp.path());
    assert!(rows(&storage, "a", "e").is_empty());
}

/// Random writes over several knowledge graphs, crashing or stopping after
/// each round, sometimes with a one-KG residency limit so graphs unload and
/// reload between writes: every committed fact survives, nothing else does.
#[test]
fn crash_restart_cycles_keep_exactly_the_committed_facts() {
    let temp = TempDir::new().unwrap();
    let kgs = ["k0", "k1", "k2", "k3"];
    let mut model: BTreeMap<(String, String), BTreeSet<Tuple>> = BTreeMap::new();
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = move |bound: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % bound
    };
    for round in 0..12 {
        let max_loaded = usize::from(round % 3 == 2);
        let storage = open_limited(temp.path(), max_loaded);
        if round > 0 {
            assert_eq!(storage.loaded_knowledge_graph_count(), 0, "round {round}");
        }
        for ((kg, relation), expected) in &model {
            assert_eq!(
                &rows(&storage, kg, relation),
                expected,
                "round {round} {kg}:{relation}"
            );
        }
        if round == 0 {
            for kg in kgs {
                storage.create_knowledge_graph(kg).unwrap();
            }
        }
        for _ in 0..30 {
            let kg = kgs[next(4) as usize];
            let relation = format!("r{}", next(3));
            let values: Vec<i64> = (0..=next(4)).map(|_| next(20) as i64).collect();
            let entry = model.entry((kg.to_string(), relation.clone())).or_default();
            let tuples: Vec<Tuple> = values.iter().map(|&v| int(v)).collect();
            if next(3) == 0 {
                storage
                    .delete_tuples_from(kg, &relation, tuples.clone())
                    .unwrap();
                for tuple in &tuples {
                    entry.remove(tuple);
                }
            } else {
                storage
                    .insert_tuples_into(kg, &relation, tuples.clone())
                    .unwrap();
                entry.extend(tuples);
            }
        }
        if round % 2 == 0 {
            crash(storage);
        } else {
            storage.save_all().unwrap();
        }
    }
}
