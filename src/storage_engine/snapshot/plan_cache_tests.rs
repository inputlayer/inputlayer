//! Compiled plans shared across snapshots of the same rules.

use super::*;
use crate::value::Value;

fn edges(pairs: &[(i64, i64)]) -> HashMap<String, Vec<Tuple>> {
    let tuples = pairs
        .iter()
        .map(|&(a, b)| Tuple::new(vec![Value::Int64(a), Value::Int64(b)]))
        .collect();
    HashMap::from([("edge".to_string(), tuples)])
}

fn rules(text: &str) -> Vec<Rule> {
    crate::parser::parse_program(text).unwrap().rules
}

fn sorted(mut tuples: Vec<Tuple>) -> Vec<Tuple> {
    tuples.sort();
    tuples
}

fn snapshot_after(
    data: &[(i64, i64)],
    rule_text: &str,
    previous: Option<&KnowledgeGraphSnapshot>,
) -> KnowledgeGraphSnapshot {
    KnowledgeGraphSnapshot::with_rules_after(
        edges(data),
        rules(rule_text),
        1,
        HashSet::new(),
        previous,
    )
}

const TWO_HOP: &str = "two_hop(X, Z) <- edge(X, Y), edge(Y, Z)";
const QUERY: &str = "__query__(Z) <- two_hop(1, Z)";

/// The cached path's result, checked against a fresh compilation.
fn cached(snapshot: &KnowledgeGraphSnapshot, program: &str) -> (Vec<Tuple>, TimingBreakdown) {
    let (tuples, timing, _) = snapshot
        .execute_with_rules_tuples_cached(program, TimingMode::Summary)
        .unwrap();
    let fresh = snapshot.execute_with_rules_tuples(program).unwrap();
    assert_eq!(
        sorted(tuples.clone()),
        sorted(fresh),
        "cached plan disagrees"
    );
    (sorted(tuples), timing.unwrap())
}

#[test]
fn a_plan_is_compiled_once_and_reused() {
    let snapshot = snapshot_after(&[(1, 2), (2, 3), (2, 4)], TWO_HOP, None);
    let (first, _) = cached(&snapshot, QUERY);
    assert_eq!(snapshot.persistent_rules().cached_plans(), 1);

    let (again, timing) = cached(&snapshot, QUERY);
    assert_eq!(again, first);
    assert_eq!(snapshot.persistent_rules().cached_plans(), 1);
    let compile = timing.parse_us + timing.sip_us + timing.ir_build_us + timing.optimize_us;
    assert_eq!(compile, 0, "a hit compiles nothing: {timing:?}");
}

#[test]
fn an_execution_tells_whether_its_plan_came_from_the_cache() {
    let first = snapshot_after(&[(1, 2), (2, 3)], TWO_HOP, None);
    let plan_cached = |snapshot: &KnowledgeGraphSnapshot| {
        let (_, timing, plan_cached) = snapshot
            .execute_with_rules_tuples_cached(QUERY, TimingMode::Off)
            .unwrap();
        assert!(timing.is_none());
        plan_cached
    };
    assert!(!plan_cached(&first), "the first execution compiles");
    assert!(plan_cached(&first));
    let next = snapshot_after(&[(1, 2), (2, 5)], TWO_HOP, Some(&first));
    assert!(plan_cached(&next));
    let changed = snapshot_after(&[(1, 2)], "two_hop(X, Z) <- edge(X, Z)", Some(&next));
    assert!(!plan_cached(&changed), "a rule change compiles again");
}

#[test]
fn snapshots_of_the_same_rules_share_plans_and_read_their_own_data() {
    let first = snapshot_after(&[(1, 2), (2, 3)], TWO_HOP, None);
    let (rows, _) = cached(&first, QUERY);
    assert_eq!(rows, vec![Tuple::new(vec![Value::Int64(3)])]);

    let next = snapshot_after(&[(1, 2), (2, 3), (2, 5)], TWO_HOP, Some(&first));
    assert!(Arc::ptr_eq(
        first.persistent_rules(),
        next.persistent_rules()
    ));
    let (rows, timing) = cached(&next, QUERY);
    assert_eq!(
        timing.optimize_us, 0,
        "served from the first snapshot's plan"
    );
    let expected: Vec<Tuple> = [3, 5]
        .iter()
        .map(|&z| Tuple::new(vec![Value::Int64(z)]))
        .collect();
    assert_eq!(rows, expected);
}

#[test]
fn a_rule_change_starts_without_plans() {
    let first = snapshot_after(&[(1, 2), (2, 3)], TWO_HOP, None);
    cached(&first, QUERY);
    let changed = snapshot_after(
        &[(1, 2), (2, 3)],
        "two_hop(X, Z) <- edge(X, Z)",
        Some(&first),
    );
    assert!(!Arc::ptr_eq(
        first.persistent_rules(),
        changed.persistent_rules()
    ));
    assert_eq!(changed.persistent_rules().cached_plans(), 0);
    let (rows, _) = cached(&changed, QUERY);
    assert_eq!(rows, vec![Tuple::new(vec![Value::Int64(2)])]);
}

#[test]
fn magic_sets_seeds_are_restored_for_every_execution() {
    let reach = "reach(X, Y) <- edge(X, Y)\nreach(X, Z) <- reach(X, Y), edge(Y, Z)";
    let query = "__query__(Y) <- reach(1, Y)";
    let first = snapshot_after(&[(1, 2), (2, 3), (7, 8)], reach, None);
    let (rows, _) = cached(&first, query);
    assert_eq!(rows.len(), 2);
    let next = snapshot_after(&[(1, 2), (2, 3), (3, 4), (7, 8)], reach, Some(&first));
    let (rows, timing) = cached(&next, query);
    assert_eq!(timing.magic_sets_us + timing.optimize_us, 0);
    assert_eq!(rows.len(), 3);
}

#[test]
fn a_plan_for_other_optimizer_passes_is_not_reused() {
    let first = snapshot_after(&[(1, 2), (2, 3)], TWO_HOP, None);
    cached(&first, QUERY);
    let mut next = snapshot_after(&[(1, 2), (2, 3)], TWO_HOP, Some(&first));
    next.optimization.enable_constant_specialization = false;
    let (rows, _) = cached(&next, QUERY);
    assert_eq!(rows, vec![Tuple::new(vec![Value::Int64(3)])]);
    let (_, timing) = cached(&next, QUERY);
    assert_eq!(timing.optimize_us, 0, "recompiled once for the new passes");
}

#[test]
fn the_cache_is_bounded() {
    let snapshot = snapshot_after(&[(1, 2)], TWO_HOP, None);
    for k in 0..=MAX_CACHED_PLANS {
        snapshot
            .execute_with_rules_tuples_cached(
                &format!("__query__(Z) <- two_hop({k}, Z)"),
                TimingMode::Off,
            )
            .unwrap();
    }
    assert_eq!(snapshot.persistent_rules().cached_plans(), MAX_CACHED_PLANS);
}

#[test]
fn a_failing_program_is_not_cached() {
    let snapshot = snapshot_after(&[(1, 2)], TWO_HOP, None);
    let unsafe_rule = "__query__(Z) <- edge(1, Y)";
    assert!(snapshot
        .execute_with_rules_tuples_cached(unsafe_rule, TimingMode::Off)
        .is_err());
    assert_eq!(snapshot.persistent_rules().cached_plans(), 0);
}
