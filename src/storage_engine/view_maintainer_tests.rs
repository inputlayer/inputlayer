//! The view maintainer on the commit path (`engine.views = "maintained"`):
//! its base arrangements equal the relation store at every revision, a
//! failed or slow maintainer never holds up or fails a commit, and it starts
//! and stops with its knowledge graph.
#![allow(clippy::unwrap_used)]

use super::*;
use crate::config::{Config, ViewsMode};
use crate::view_maintainer::BaseRows;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeSet;
use std::time::Duration;
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(60);

fn config(dir: &Path, views: ViewsMode, max_loaded: usize) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    config.storage.performance.num_threads = 2;
    config.storage.max_loaded_knowledge_graphs = max_loaded;
    config.engine.views = views;
    config
}

fn open(dir: &Path) -> StorageEngine {
    StorageEngine::new(config(dir, ViewsMode::Maintained, 0)).unwrap()
}

fn pair(a: i32, b: i32) -> Tuple {
    Tuple::from_pair(a, b)
}

/// The arrangement's rows, which must each be held exactly once.
fn set(rows: BaseRows) -> BTreeSet<Tuple> {
    let count = rows.rows.len();
    let set: BTreeSet<Tuple> = rows
        .rows
        .into_iter()
        .map(|(tuple, multiplicity)| {
            assert_eq!(multiplicity, 1, "{tuple:?} is held {multiplicity} times");
            tuple
        })
        .collect();
    assert_eq!(set.len(), count, "a tuple is listed twice");
    set
}

/// The oracle: at the published revision, both arrangements of every one of
/// `relations` equal the relation store. Returns that revision.
fn assert_views_equal_store(storage: &StorageEngine, kg: &str, relations: &[&str]) -> u64 {
    let graph = storage.kg_handle(kg).unwrap();
    let db = graph.read();
    let revision = db.snapshot.load().revision;
    let views = db.views.as_ref().expect("the KG runs a maintainer");
    assert!(
        views.wait_for(revision, WAIT),
        "frontier reaches {revision}"
    );
    assert_eq!(views.frontier(), revision);
    for relation in relations {
        let stored: BTreeSet<Tuple> = db
            .store
            .get(relation)
            .map(|tuples| tuples.iter().cloned().collect())
            .unwrap_or_default();
        let by_tuple = views.scan_tuples(relation).unwrap();
        assert_eq!(by_tuple.revision, revision);
        assert_eq!(set(by_tuple), stored, "{kg}.{relation} by tuple");
        let by_key = views.scan_keys(relation, None).unwrap();
        assert_eq!(by_key.revision, revision);
        assert_eq!(set(by_key), stored, "{kg}.{relation} by key");
    }
    revision
}

fn stats(storage: &StorageEngine, kg: &str) -> ViewStats {
    storage
        .view_stats()
        .into_iter()
        .find_map(|(name, stats)| (name == kg).then_some(stats))
        .unwrap_or_else(|| panic!("{kg} runs no maintainer"))
}

fn maintained(storage: &StorageEngine) -> Vec<String> {
    storage
        .view_stats()
        .into_iter()
        .map(|(name, _)| name)
        .collect()
}

/// Random histories of inserts (with duplicates of stored tuples and within
/// the batch), deletes (of stored and absent tuples), clears, relation drops
/// and rule changes that leave the facts alone: after every commit, the
/// arrangements equal the store at that commit's revision.
#[test]
fn base_arrangements_equal_the_store_at_every_revision() {
    const RELATIONS: [&str; 4] = ["edge", "node", "tmp_a", "tmp_b"];
    for seed in 0..4 {
        let temp = TempDir::new().unwrap();
        let storage = open(temp.path());
        storage.create_knowledge_graph("kg").unwrap();
        let mut rng = StdRng::seed_from_u64(seed);
        let mut last = assert_views_equal_store(&storage, "kg", &RELATIONS);
        let mut rules = 0;
        for step in 0..150 {
            let relation = RELATIONS[rng.gen_range(0..RELATIONS.len())];
            let mut batch: Vec<Tuple> = (0..rng.gen_range(1..10))
                .map(|_| pair(rng.gen_range(0..5), rng.gen_range(0..5)))
                .collect();
            match rng.gen_range(0..20) {
                0..=9 => {
                    // Some tuples twice in one batch.
                    let again: Vec<Tuple> = batch.iter().take(2).cloned().collect();
                    batch.extend(again);
                    storage.insert_tuples_into("kg", relation, batch).unwrap();
                }
                10..=15 => {
                    storage.delete_tuples_from("kg", relation, batch).unwrap();
                }
                16 => {
                    storage.clear_relations_by_prefix_in("kg", "tmp_").unwrap();
                }
                17 => {
                    // Fails when the relation does not exist: nothing changes.
                    let _ = storage.drop_relation_in("kg", relation);
                }
                _ => {
                    // A commit that publishes a revision without fact changes.
                    rules += 1;
                    let rule = crate::statement::parse_rule_definition(&format!(
                        "seen{rules}(X) <- source{rules}(X, Y)"
                    ))
                    .unwrap();
                    storage.register_rule_in("kg", &rule).unwrap();
                }
            }
            let revision = assert_views_equal_store(&storage, "kg", &RELATIONS);
            assert!(revision >= last, "seed {seed} step {step}");
            last = revision;
        }
        assert_eq!(stats(&storage, "kg").unavailable, None);
        assert_eq!(stats(&storage, "kg").pending_commits, 0);
    }
}

/// One commit that changes several relations reaches the arrangements as one
/// revision.
#[test]
fn a_multi_relation_program_is_one_revision() {
    let temp = TempDir::new().unwrap();
    let storage = open(temp.path());
    storage.create_knowledge_graph("kg").unwrap();
    storage
        .insert_tuples_into("kg", "a", vec![pair(1, 1), pair(2, 2)])
        .unwrap();
    let before = assert_views_equal_store(&storage, "kg", &["a", "b"]);

    let mut program = WriteProgram::new();
    program.push(
        0,
        StagedChanges::Facts(vec![
            FactChange::Insert {
                relation: "b".to_string(),
                tuples: vec![pair(3, 3)],
            },
            FactChange::Delete {
                relation: "a".to_string(),
                tuples: vec![pair(1, 1)],
            },
        ]),
    );
    let commit = storage.commit_program("kg", program, None).unwrap();
    let after = assert_views_equal_store(&storage, "kg", &["a", "b"]);
    assert_eq!(after, commit.revision);
    assert!(after > before);
}

/// A maintainer that panics is contained: writes to its KG still commit and
/// are read, the KG reports its views unavailable, and other KGs' maintainers
/// keep up. Reloading the KG starts a fresh maintainer from the store.
#[test]
fn a_maintainer_panic_is_contained() {
    let temp = TempDir::new().unwrap();
    {
        let storage = open(temp.path());
        for kg in ["broken", "healthy"] {
            storage.create_knowledge_graph(kg).unwrap();
            storage
                .insert_tuples_into(kg, "edge", vec![pair(1, 2)])
                .unwrap();
            assert_views_equal_store(&storage, kg, &["edge"]);
        }
        let stopped_at = stats(&storage, "broken").frontier;

        storage
            .kg_handle("broken")
            .unwrap()
            .read()
            .views
            .as_ref()
            .unwrap()
            .inject_panic();
        // Released by the failure, long before the timeout.
        assert!(!storage.wait_for_views("broken", u64::MAX, WAIT));

        // Writes still commit, and reads see them.
        for i in 0..50 {
            let (inserted, _) = storage
                .insert_tuples_into("broken", "edge", vec![pair(10 + i, 0)])
                .unwrap();
            assert_eq!(inserted, 1);
        }
        assert_eq!(
            storage
                .delete_tuples_from("broken", "edge", vec![pair(1, 2)])
                .unwrap(),
            1
        );
        let snapshot = storage.get_snapshot_for("broken").unwrap();
        assert_eq!(snapshot.input_tuples["edge"].len(), 50);
        let derived = storage
            .execute_query_on("broken", "out(X, Y) <- edge(X, Y)")
            .expect("the recompute path serves reads");
        assert_eq!(derived.len(), 50);

        // The KG is flagged, and its frontier stays where it stopped.
        let graph = storage.kg_handle("broken").unwrap();
        let reason = graph.read().views_unavailable().expect("flagged");
        assert!(reason.contains("panicked"), "{reason}");
        let broken = stats(&storage, "broken");
        assert_eq!(broken.unavailable, Some(reason));
        assert_eq!(broken.frontier, stopped_at);
        assert_eq!(broken.pending_commits, 0);
        assert!(!storage.wait_for_views("broken", stopped_at + 1, WAIT));
        drop(graph);

        // The other KG is unaffected.
        assert_eq!(
            storage
                .kg_handle("healthy")
                .unwrap()
                .read()
                .views_unavailable(),
            None
        );
        for i in 0..20 {
            storage
                .insert_tuples_into("healthy", "edge", vec![pair(100 + i, 0)])
                .unwrap();
            assert_views_equal_store(&storage, "healthy", &["edge"]);
        }
        assert_eq!(stats(&storage, "healthy").unavailable, None);
    }

    // A reload rebuilds the arrangements from the store, which lost nothing.
    let storage = open(temp.path());
    assert_views_equal_store(&storage, "broken", &["edge"]);
    assert_eq!(stats(&storage, "broken").unavailable, None);
    assert_eq!(
        storage.get_snapshot_for("broken").unwrap().input_tuples["edge"].len(),
        50
    );
}

/// Commits do not wait for the maintainer. With its worker held, more
/// commits than the old 1024-command queue could take all return; the
/// backlog shows as frontier lag and clears once the worker runs.
#[test]
fn a_stalled_maintainer_does_not_block_commits() {
    let temp = TempDir::new().unwrap();
    let mut config = config(temp.path(), ViewsMode::Maintained, 0);
    config.storage.persist.durability_mode = crate::config::DurabilityMode::Batched;
    let storage = StorageEngine::new(config).unwrap();
    storage.create_knowledge_graph("kg").unwrap();
    let held_at = assert_views_equal_store(&storage, "kg", &["edge"]);
    let release = storage
        .kg_handle("kg")
        .unwrap()
        .read()
        .views
        .as_ref()
        .unwrap()
        .stall();

    let commits = 1_500;
    for i in 0..commits {
        storage
            .insert_tuples_into("kg", "edge", vec![pair(i, i)])
            .unwrap();
    }
    std::thread::sleep(Duration::from_millis(20));
    let lagging = stats(&storage, "kg");
    assert_eq!(lagging.frontier, held_at, "the worker is held");
    assert_eq!(lagging.pending_commits, commits as usize);
    assert!(lagging.frontier_lag >= Duration::from_millis(20));
    // Every commit is published and read while the maintainer lags.
    let snapshot = storage.get_snapshot_for("kg").unwrap();
    assert_eq!(snapshot.input_tuples["edge"].len(), commits as usize);
    assert!(!storage.wait_for_views("kg", snapshot.revision, Duration::from_millis(10)));

    drop(release);
    assert_views_equal_store(&storage, "kg", &["edge"]);
    let current = stats(&storage, "kg");
    assert_eq!(current.pending_commits, 0);
    assert_eq!(current.frontier_lag, Duration::ZERO);
}

/// The default: no maintainer, whatever happens to the knowledge graphs.
#[test]
fn recompute_mode_runs_no_maintainer() {
    let temp = TempDir::new().unwrap();
    assert_eq!(Config::default().engine.views, ViewsMode::Recompute);
    let storage = StorageEngine::new(config(temp.path(), ViewsMode::Recompute, 0)).unwrap();
    storage.create_knowledge_graph("kg").unwrap();
    storage
        .insert_tuples_into("kg", "edge", vec![pair(1, 2)])
        .unwrap();
    assert!(maintained(&storage).is_empty());
    let graph = storage.kg_handle("kg").unwrap();
    assert!(graph.read().views.is_none());
    assert_eq!(graph.read().view_stats(), None);
    assert_eq!(graph.read().views_unavailable(), None);
    let revision = graph.read().snapshot.load().revision;
    assert!(!storage.wait_for_views("kg", revision, Duration::from_millis(10)));
}

/// A maintainer starts with its knowledge graph and stops when it is dropped.
#[test]
fn a_maintainer_starts_at_create_and_stops_at_drop() {
    let temp = TempDir::new().unwrap();
    let storage = open(temp.path());
    // The engine's own KGs run one too.
    let before = maintained(&storage);
    assert!(!before.contains(&"kg".to_string()));

    let created = storage.create_knowledge_graph_at("kg").unwrap();
    assert!(maintained(&storage).contains(&"kg".to_string()));
    assert!(storage.wait_for_views("kg", created, WAIT));
    assert_eq!(assert_views_equal_store(&storage, "kg", &["edge"]), created);
    storage
        .insert_tuples_into("kg", "edge", vec![pair(1, 2)])
        .unwrap();
    assert_views_equal_store(&storage, "kg", &["edge"]);

    let waiter = storage
        .kg_handle("kg")
        .unwrap()
        .read()
        .views
        .as_ref()
        .unwrap()
        .waiter();
    storage.drop_knowledge_graph("kg").unwrap();
    assert_eq!(maintained(&storage), before);
    // Released by the stop, long before the timeout.
    assert!(!waiter.wait_for(u64::MAX, WAIT), "the maintainer stopped");
    assert!(!storage.wait_for_views("kg", created, Duration::from_millis(10)));

    // The name is free again, with a maintainer of its own.
    storage.create_knowledge_graph("kg").unwrap();
    assert_views_equal_store(&storage, "kg", &["edge"]);
    assert_eq!(stats(&storage, "kg").trace_rows, 0);
}

/// Unloading a knowledge graph stops its maintainer; loading it again starts
/// one from the store at the revision the graph was unloaded with.
#[test]
fn a_maintainer_stops_at_unload_and_starts_at_load() {
    let temp = TempDir::new().unwrap();
    let storage = StorageEngine::new(config(temp.path(), ViewsMode::Maintained, 1)).unwrap();
    storage.create_knowledge_graph("a").unwrap();
    storage
        .insert_tuples_into("a", "edge", vec![pair(1, 2), pair(3, 4)])
        .unwrap();
    let unloaded_at = assert_views_equal_store(&storage, "a", &["edge"]);
    let waiter = storage
        .kg_handle("a")
        .unwrap()
        .read()
        .views
        .as_ref()
        .unwrap()
        .waiter();

    // Over the limit of one loaded KG: `a` is unloaded.
    storage.create_knowledge_graph("b").unwrap();
    storage
        .insert_tuples_into("b", "edge", vec![pair(5, 6)])
        .unwrap();
    let deadline = Instant::now() + WAIT;
    while storage.is_knowledge_graph_loaded("a").unwrap() {
        assert!(Instant::now() < deadline, "a is unloaded");
        storage.insert_tuples_into("b", "edge", vec![]).unwrap();
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!maintained(&storage).contains(&"a".to_string()));
    assert!(!waiter.wait_for(u64::MAX, WAIT), "the maintainer stopped");
    assert!(!storage.wait_for_views("a", unloaded_at, Duration::from_millis(10)));

    // First use loads it: a fresh maintainer holds the same state.
    assert_eq!(
        assert_views_equal_store(&storage, "a", &["edge"]),
        unloaded_at
    );
    assert_eq!(stats(&storage, "a").trace_rows, 4);
    storage
        .delete_tuples_from("a", "edge", vec![pair(1, 2)])
        .unwrap();
    assert!(assert_views_equal_store(&storage, "a", &["edge"]) > unloaded_at);
}

/// After a restart a knowledge graph is dormant and runs no maintainer until
/// its first use, which loads its facts into one.
#[test]
fn a_maintainer_starts_when_a_restarted_engine_loads_the_graph() {
    let temp = TempDir::new().unwrap();
    {
        let storage = open(temp.path());
        storage.create_knowledge_graph("kg").unwrap();
        storage
            .insert_tuples_into("kg", "edge", vec![pair(1, 2), pair(3, 4), pair(5, 6)])
            .unwrap();
        storage
            .delete_tuples_from("kg", "edge", vec![pair(3, 4)])
            .unwrap();
        storage
            .insert_tuples_into("kg", "node", vec![pair(7, 7)])
            .unwrap();
    }
    let storage = open(temp.path());
    assert!(!storage.is_knowledge_graph_loaded("kg").unwrap());
    assert!(!maintained(&storage).contains(&"kg".to_string()));

    assert_views_equal_store(&storage, "kg", &["edge", "node"]);
    // Two tuples of `edge` and one of `node`, in two arrangements each.
    assert_eq!(stats(&storage, "kg").trace_rows, 6);
    storage
        .insert_tuples_into("kg", "edge", vec![pair(9, 9)])
        .unwrap();
    assert_views_equal_store(&storage, "kg", &["edge", "node"]);
}
