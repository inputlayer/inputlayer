//! The maintainer alone: its arrangements against a model of the base
//! relations, its frontier and backlog, and what a failed or stopped worker
//! leaves behind. `storage_engine::view_maintainer_tests` covers it wired to
//! the commit path.
#![allow(clippy::unwrap_used)]

use super::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};

const WAIT: Duration = Duration::from_secs(60);

fn pair(a: i64, b: i64) -> Tuple {
    Tuple::new(vec![Value::Int64(a), Value::Int64(b)])
}

fn delta(relation: &str, added: Vec<Tuple>, removed: Vec<Tuple>) -> BaseChange {
    BaseChange {
        deltas: vec![BaseDelta {
            relation: relation.to_string(),
            added,
            removed,
        }],
        dropped: Vec::new(),
    }
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

/// Both arrangements of `relation` hold exactly `expected` at `revision`.
fn assert_holds(views: &ViewMaintainer, relation: &str, revision: u64, expected: &BTreeSet<Tuple>) {
    assert!(
        views.wait_for(revision, WAIT),
        "frontier reaches {revision}"
    );
    let by_tuple = views.scan_tuples(relation).unwrap();
    assert_eq!(by_tuple.revision, revision);
    assert_eq!(&set(by_tuple), expected, "by tuple, revision {revision}");
    let by_key = views.scan_keys(relation, None).unwrap();
    assert_eq!(by_key.revision, revision);
    assert_eq!(&set(by_key), expected, "by key, revision {revision}");

    let mut keyed: BTreeMap<Tuple, BTreeSet<Tuple>> = BTreeMap::new();
    for tuple in expected {
        keyed
            .entry(Tuple::new(vec![key_of(tuple)]))
            .or_default()
            .insert(tuple.clone());
    }
    for (key, tuples) in &keyed {
        let found = views.scan_keys(relation, key.get(0).cloned()).unwrap();
        assert_eq!(&set(found), tuples, "key {key:?}, revision {revision}");
    }
}

#[test]
fn loads_the_relations_it_starts_with() {
    let mut relations = RelationMap::new();
    relations.insert(
        "edge".to_string(),
        vec![pair(1, 2), pair(1, 3), pair(2, 3)].into(),
    );
    relations.insert("empty".to_string(), Vec::new().into());
    let views = ViewMaintainer::start("kg", 7, relations);

    let expected: BTreeSet<Tuple> = [pair(1, 2), pair(1, 3), pair(2, 3)].into();
    assert_holds(&views, "edge", 7, &expected);
    assert_holds(&views, "empty", 7, &BTreeSet::new());
    assert_holds(&views, "never_written", 7, &BTreeSet::new());
    assert_eq!(views.frontier(), 7);
}

#[test]
fn a_key_lookup_returns_only_that_keys_tuples() {
    let views = ViewMaintainer::start("kg", 1, RelationMap::new());
    views.feed(
        2,
        delta("edge", vec![pair(1, 2), pair(1, 3), pair(2, 3)], vec![]),
    );
    assert!(views.wait_for(2, WAIT));
    let under_one = set(views.scan_keys("edge", Some(Value::Int64(1))).unwrap());
    assert_eq!(under_one, [pair(1, 2), pair(1, 3)].into());
    let under_nine = views.scan_keys("edge", Some(Value::Int64(9))).unwrap();
    assert!(under_nine.rows.is_empty());
}

/// A tuple without columns is arranged under the `Null` key.
#[test]
fn a_tuple_without_columns_is_keyed_by_null() {
    let views = ViewMaintainer::start("kg", 1, RelationMap::new());
    views.feed(2, delta("flag", vec![Tuple::empty()], vec![]));
    assert_holds(&views, "flag", 2, &[Tuple::empty()].into());
    let under_null = set(views.scan_keys("flag", Some(Value::Null)).unwrap());
    assert_eq!(under_null, [Tuple::empty()].into());
}

/// Random histories of inserts, deletes and re-inserts over several
/// relations, with revisions that skip (other knowledge graphs take
/// revisions too) and snapshots that change nothing: at every revision both
/// arrangements equal the model.
#[test]
fn arrangements_equal_the_model_at_every_revision() {
    for seed in 0..6 {
        let mut rng = StdRng::seed_from_u64(seed);
        let views = ViewMaintainer::start("kg", 1, RelationMap::new());
        let relations = ["a", "b", "c"];
        let mut model: BTreeMap<&str, BTreeSet<Tuple>> = BTreeMap::new();
        let mut revision = 1;
        for _ in 0..120 {
            revision += rng.gen_range(1..4);
            let relation = relations[rng.gen_range(0..relations.len())];
            let held = model.entry(relation).or_default();
            let change = match rng.gen_range(0..10) {
                // A snapshot that changes no base relation.
                0 => BaseChange::default(),
                1 => {
                    held.clear();
                    BaseChange {
                        deltas: Vec::new(),
                        dropped: vec![relation.to_string()],
                    }
                }
                _ => {
                    // The commit path feeds net changes: tuples that were
                    // absent and are added, tuples that were present and go.
                    let mut added = BTreeSet::new();
                    let mut removed = BTreeSet::new();
                    for _ in 0..rng.gen_range(1..12) {
                        let tuple = pair(rng.gen_range(0..6), rng.gen_range(0..6));
                        if rng.gen_bool(0.6) {
                            if !held.contains(&tuple) {
                                added.insert(tuple);
                            }
                        } else if held.contains(&tuple) && !added.contains(&tuple) {
                            removed.insert(tuple);
                        }
                    }
                    for tuple in &removed {
                        held.remove(tuple);
                    }
                    held.extend(added.iter().cloned());
                    delta(
                        relation,
                        added.into_iter().collect(),
                        removed.into_iter().collect(),
                    )
                }
            };
            views.feed(revision, change);
            for relation in relations {
                let expected = model.get(relation).cloned().unwrap_or_default();
                assert_holds(&views, relation, revision, &expected);
            }
        }
        assert_eq!(views.unavailable(), None);
    }
}

/// A relation dropped and written again starts empty.
#[test]
fn a_dropped_relation_is_rebuilt_empty() {
    let views = ViewMaintainer::start("kg", 1, RelationMap::new());
    views.feed(2, delta("edge", vec![pair(1, 2), pair(3, 4)], vec![]));
    views.feed(
        3,
        BaseChange {
            deltas: Vec::new(),
            dropped: vec!["edge".to_string()],
        },
    );
    assert_holds(&views, "edge", 3, &BTreeSet::new());
    assert_eq!(views.stats().trace_rows, 0);
    views.feed(4, delta("edge", vec![pair(5, 6)], vec![]));
    assert_holds(&views, "edge", 4, &[pair(5, 6)].into());
}

/// The old worker's queue held 1024 commands and blocked the writer beyond
/// them. Feeding never waits: with the worker held, thousands of commits
/// queue at once and show as a lagging frontier, which recovers in one round.
#[test]
fn feeding_never_waits_for_the_worker() {
    let views = ViewMaintainer::start("kg", 1, RelationMap::new());
    assert!(views.wait_for(1, WAIT));
    let release = views.stall();

    let commits = 5_000;
    let started = Instant::now();
    for i in 0..commits {
        views.feed(2 + i, delta("edge", vec![pair(i as i64, 0)], vec![]));
    }
    let fed_in = started.elapsed();
    assert!(fed_in < Duration::from_secs(10), "feeding took {fed_in:?}");

    std::thread::sleep(Duration::from_millis(20));
    let lagging = views.stats();
    assert_eq!(lagging.frontier, 1, "the worker is held");
    assert_eq!(lagging.pending_commits, commits as usize);
    assert!(lagging.frontier_lag >= Duration::from_millis(20));
    assert!(!views.wait_for(2, Duration::from_millis(10)));

    drop(release);
    let last = 1 + commits;
    assert!(views.wait_for(last, WAIT));
    let current = views.stats();
    assert_eq!(current.frontier, last);
    assert_eq!(current.pending_commits, 0);
    assert_eq!(current.frontier_lag, Duration::ZERO);
    let expected: BTreeSet<Tuple> = (0..commits as i64).map(|i| pair(i, 0)).collect();
    assert_holds(&views, "edge", last, &expected);
}

#[test]
fn stats_meter_the_traces() {
    let views = ViewMaintainer::start("kg", 1, RelationMap::new());
    assert!(views.wait_for(1, WAIT));
    assert_eq!(views.stats().trace_rows, 0);
    assert_eq!(views.stats().trace_bytes, 0);

    let tuples: Vec<Tuple> = (0..100).map(|i| pair(i, i)).collect();
    views.feed(2, delta("edge", tuples.clone(), vec![]));
    assert!(views.wait_for(2, WAIT));
    let full = views.stats();
    // Each tuple once in each of the two arrangements.
    assert_eq!(full.trace_rows, 200);
    let data: usize = tuples.iter().map(Tuple::estimated_bytes).sum();
    assert!(full.trace_bytes >= 2 * data as u64, "{full:?}");

    // A wide tuple costs what it holds.
    let wide = Tuple::new(vec![
        Value::Int64(1),
        Value::String("x".repeat(4096).into()),
    ]);
    views.feed(3, delta("wide", vec![wide], vec![]));
    assert!(views.wait_for(3, WAIT));
    let with_wide = views.stats();
    assert_eq!(with_wide.trace_rows, 202);
    assert!(with_wide.trace_bytes >= full.trace_bytes + 2 * 4096);
}

/// Retractions cancel in the traces as their batches merge: memory follows
/// the live relation, not the history.
#[test]
fn traces_follow_the_live_relation_not_the_history() {
    let views = ViewMaintainer::start("kg", 1, RelationMap::new());
    let mut revision = 1;
    for round in 0..200 {
        let tuples: Vec<Tuple> = (0..50).map(|i| pair(round, i)).collect();
        revision += 1;
        views.feed(revision, delta("churn", tuples.clone(), vec![]));
        revision += 1;
        views.feed(revision, delta("churn", vec![], tuples));
        assert!(views.wait_for(revision, WAIT));
    }
    assert_holds(&views, "churn", revision, &BTreeSet::new());
    // 40,000 updates went into the two traces.
    let deadline = Instant::now() + WAIT;
    while views.stats().trace_rows > 4_000 {
        assert!(Instant::now() < deadline, "{:?}", views.stats());
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A panic in the worker stays in the worker: the maintainer reports why it
/// stopped, feeding still returns, and waiters are released.
#[test]
fn a_worker_panic_is_contained() {
    let views = ViewMaintainer::start("kg", 1, RelationMap::new());
    views.feed(2, delta("edge", vec![pair(1, 2)], vec![]));
    assert!(views.wait_for(2, WAIT));
    assert_eq!(views.unavailable(), None);

    views.inject_panic();
    // Released by the failure, long before the timeout.
    assert!(!views.wait_for(u64::MAX, WAIT));
    let reason = views.unavailable().expect("the maintainer is unavailable");
    assert!(
        reason.contains("injected view maintainer panic"),
        "{reason}"
    );

    for revision in 3..2_000 {
        views.feed(
            revision,
            delta("edge", vec![pair(revision as i64, 0)], vec![]),
        );
    }
    let stats = views.stats();
    assert_eq!(stats.frontier, 2, "the frontier stays where it stopped");
    assert_eq!(stats.pending_commits, 0, "nothing queues for a dead worker");
    assert_eq!(stats.frontier_lag, Duration::ZERO);
    assert_eq!(stats.unavailable, Some(reason));
    assert_eq!(views.scan_tuples("edge"), None);
    assert_eq!(views.scan_keys("edge", None), None);
}

/// Dropping the maintainer stops its thread, also with a backlog queued.
#[test]
fn dropping_the_maintainer_stops_its_worker() {
    let views = ViewMaintainer::start("kg", 1, RelationMap::new());
    assert!(views.wait_for(1, WAIT));
    let shared = Arc::downgrade(&views.shared);
    assert_eq!(shared.strong_count(), 2, "the handle and the worker");

    let release = views.stall();
    for revision in 2..3_000 {
        views.feed(
            revision,
            delta("edge", vec![pair(revision as i64, 0)], vec![]),
        );
    }
    let waiter = views.waiter();
    drop(release);
    drop(views);
    // The drop joined the worker: only the waiter holds the state now.
    assert_eq!(shared.strong_count(), 1);
    assert!(!waiter.wait_for(u64::MAX, WAIT), "a stopped maintainer");
}

#[test]
fn many_maintainers_run_side_by_side() {
    let all: Vec<ViewMaintainer> = (0..16)
        .map(|i| ViewMaintainer::start(&format!("kg{i}"), 1, RelationMap::new()))
        .collect();
    for (i, views) in all.iter().enumerate() {
        views.feed(2, delta("edge", vec![pair(i as i64, 0)], vec![]));
    }
    for (i, views) in all.iter().enumerate() {
        assert_holds(views, "edge", 2, &[pair(i as i64, 0)].into());
    }
}
