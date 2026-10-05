//! Lifting queries to shapes, and partitioning a lifted result by binding.

use serde_json::json;

use super::*;

fn lifted(query: &str) -> Option<Lifted> {
    let goal = parse_query(&query[1..]).unwrap();
    lift(query, &goal)
}

fn shape_of(query: &str) -> (String, Binding) {
    let lifted = lifted(query).unwrap_or_else(|| panic!("{query} did not lift"));
    (lifted.shape.query, lifted.binding)
}

fn s(value: &str) -> ParamValue {
    ParamValue::Str(value.to_string())
}

#[test]
fn queries_differing_only_in_constants_share_a_shape() {
    let one = r#"?speech("s-1", G, T, Subj, V, K, P), speech_owner("s-1", "sp-1", 1)"#;
    let two = r#"?speech("s-2", G, T, Subj, V, K, P), speech_owner("s-2", "sp-2", 4)"#;
    let (shape, binding) = shape_of(one);
    assert_eq!(
        shape,
        "?speech(_L0, G, T, Subj, V, K, P), speech_owner(_L0, _L1, _L2)"
    );
    assert_eq!(binding, [s("s-1"), s("sp-1"), ParamValue::Int(1)]);
    let (other, binding) = shape_of(two);
    assert_eq!(other, shape);
    assert_eq!(binding, [s("s-2"), s("sp-2"), ParamValue::Int(4)]);
}

#[test]
fn equal_strings_share_a_parameter_and_repeated_integers_stay() {
    let (shape, binding) = shape_of(r#"?a("x", X), b("x", 7, Y), c(7, "y")"#);
    assert_eq!(shape, "?a(_L0, X), b(_L0, 7, Y), c(7, _L1)");
    assert_eq!(binding, [s("x"), s("y")]);
    // A different equality pattern is a different shape.
    let (other, _) = shape_of(r#"?a("x", X), b("z", 7, Y), c(7, "y")"#);
    assert_ne!(other, shape);
}

#[test]
fn negations_comparisons_and_other_constants_are_part_of_the_shape() {
    let (shape, binding) = shape_of(r#"?a("x", X, 2.5, true), !b(X, "y"), X > 3, X != "z""#);
    assert_eq!(
        shape,
        r#"?a(_L0, X, 2.5, true), !b(X, "y"), X > 3, X != "z""#
    );
    assert_eq!(binding, [s("x")]);
}

#[test]
fn queries_without_liftable_constants_or_in_other_forms_are_not_lifted() {
    for query in [
        "?a(X, Y)",
        "?a(1, X), b(1, Y)",
        r#"?a("x", Y:desc)"#,
        r#"?a("x", _L0)"#,
        r#"?a("x", Y), b(Y, _L3)"#,
        r#"?a("x", Y), limit(3)"#,
        "?a(X, 2.5)",
    ] {
        assert!(lifted(query).is_none(), "{query} lifted");
    }
}

#[test]
fn projection_drops_parameters_first_seen_in_the_body() {
    let lifted = lifted(r#"?a("x", X), b(X, "y", Z), c(Z, W)"#).unwrap();
    let shape = &lifted.shape;
    assert_eq!(shape.query, "?a(_L0, X), b(X, _L1, Z), c(Z, W)");
    // Lifted columns: _L0, X, _L1, Z, W. The view's: "x", X, Z, W.
    assert_eq!(shape.key_columns, [0, 2]);
    assert_eq!(shape.projection, [0, 1, 3, 4]);
}

#[test]
fn integer_parameters_match_rows_as_the_engine_matches_the_constant() {
    let int = |v| row_value(ParamKind::Int, &v);
    assert_eq!(int(json!(7)), Ok(Some(ParamValue::Int(7))));
    assert_eq!(int(json!(-7)), Ok(Some(ParamValue::Int(-7))));
    assert_eq!(int(json!(7.0)), Ok(Some(ParamValue::Int(7))));
    assert_eq!(int(json!(7.000_000_000_01)), Ok(Some(ParamValue::Int(7))));
    assert_eq!(int(json!(7.5)), Ok(None));
    assert_eq!(int(json!("7")), Ok(None));
    assert_eq!(int(json!(true)), Ok(None));
    assert_eq!(int(json!(null)), Ok(None));
    assert_eq!(int(json!(1e17)), Err(()), "several integers match it");

    let string = |v| row_value(ParamKind::Str, &v);
    assert_eq!(string(json!("7")), Ok(Some(s("7"))));
    assert_eq!(string(json!(7)), Ok(None));
}

#[test]
fn a_lifted_result_partitions_into_each_bindings_projected_rows() {
    let shape = lifted(r#"?a("x", X), b(X, "y", 1)"#).unwrap().shape;
    // Lifted columns: _L0, X, _L1, _L2; a view's: "x", X.
    let rows = vec![
        vec![json!("x"), json!(1), json!("y"), json!(1)],
        vec![json!("x"), json!(2), json!("y"), json!(1.0)],
        vec![json!("x"), json!(3), json!("y"), json!(2)],
        vec![json!("w"), json!(4), json!("y"), json!(1)],
        vec![json!("x"), json!(5), json!("y"), json!(1.5)],
    ];
    let parts = Partitions::build(&shape, rows).unwrap();
    let part = |binding: Binding| {
        parts
            .get(&binding)
            .map(|part| (part.result.sorted_rows(), part.rows))
            .unwrap_or_default()
    };
    assert_eq!(
        part(vec![s("x"), s("y"), ParamValue::Int(1)]),
        (
            vec![vec![json!("x"), json!(1)], vec![json!("x"), json!(2)]],
            2
        )
    );
    assert_eq!(
        part(vec![s("x"), s("y"), ParamValue::Int(2)]),
        (vec![vec![json!("x"), json!(3)]], 1)
    );
    assert_eq!(
        part(vec![s("w"), s("y"), ParamValue::Int(1)]),
        (vec![vec![json!("w"), json!(4)]], 1)
    );
    assert_eq!(parts.len(), 3, "the 1.5 row matches no integer");
}

#[test]
fn an_ambiguous_row_fails_the_partition() {
    let shape = lifted(r#"?a("x", 3, X)"#).unwrap().shape;
    let rows = vec![vec![json!("x"), json!(1e17), json!(1)]];
    assert!(Partitions::build(&shape, rows).is_err());
}

#[test]
fn a_round_shares_while_no_slower_than_the_views_own_evaluations_in_parallel() {
    // 8 bindings on 4 permits: two waves of own evaluations, 200us.
    assert!(keeps_sharing(150, 100, 8, 4), "faster");
    assert!(keeps_sharing(200, 100, 8, 4), "as fast");
    assert!(!keeps_sharing(201, 100, 8, 4), "slower");
    assert!(!keeps_sharing(1_000, 100, 8, 4));
    // Many more bindings than permits.
    assert!(keeps_sharing(5_000, 100, 200, 4));
    assert!(!keeps_sharing(5_001, 100, 200, 4));
}

#[test]
fn costs_are_smoothed_so_one_outlier_does_not_decide() {
    let average = AtomicU64::new(0);
    assert_eq!(smooth(&average, Duration::from_micros(800)), 800, "first");
    assert_eq!(smooth(&average, Duration::from_micros(1_600)), 900);
    assert_eq!(smooth(&average, Duration::ZERO), 787, "at least 1us");
    assert_eq!(average.load(Ordering::Relaxed), 787);
}

#[test]
fn probes_wait_a_number_of_commits_that_doubles_with_each_failed_probe() {
    // Every commit evaluates every view: the wait scales with the bindings.
    assert_eq!(probe_after(200, 1), PROBE_AFTER * 200);
    assert_eq!(probe_after(1, 1), PROBE_AFTER);
    assert_eq!(probe_after(0, 1), PROBE_AFTER);
    assert_eq!(probe_after(10, 0), PROBE_AFTER * 10);
    assert_eq!(probe_after(10, 2), PROBE_AFTER * 20);
    assert_eq!(probe_after(10, 3), PROBE_AFTER * 40);
    let longest = (PROBE_AFTER * 10) << MAX_PROBE_BACKOFF;
    assert_eq!(probe_after(10, MAX_PROBE_BACKOFF + 1), longest);
    assert_eq!(probe_after(10, 1_000), longest);
    assert_eq!(probe_after(u64::MAX, 3), u64::MAX);
}

mod rounds {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::TempDir;

    use super::super::{Families, Family, MemberQuery, RoundState};
    use super::lifted;
    use crate::protocol::subscription::{
        ReevaluatingQuery, Refresh, StandingQuery, SubscriptionMetrics,
    };
    use crate::protocol::Handler;
    use crate::Config;

    const KG: &str = "rounds";

    /// The fewest compute permits with which families probe.
    fn handler() -> (Arc<Handler>, TempDir) {
        handler_with(super::super::MIN_PERMITS_FOR_PROBES)
    }

    fn handler_with(permits: usize) -> (Arc<Handler>, TempDir) {
        handler_capped(
            permits,
            Config::default().storage.performance.max_result_rows,
        )
    }

    /// A handler of `permits` whose results hold at most `max_result_rows`.
    fn handler_capped(permits: usize, max_result_rows: usize) -> (Arc<Handler>, TempDir) {
        let tmp = TempDir::new().unwrap();
        let mut config = Config::default();
        config.storage.data_dir = tmp.path().join("data");
        config.storage.performance.max_result_rows = max_result_rows;
        let handler = Arc::new(
            Handler::from_config(config)
                .unwrap()
                .with_compute_permits(permits),
        );
        handler.get_storage().create_knowledge_graph(KG).unwrap();
        (handler, tmp)
    }

    async fn write(handler: &Handler, program: &str) {
        handler
            .execute_program(None, Some(KG.to_string()), program.to_string(), None)
            .await
            .unwrap();
    }

    fn member(
        families: &Families,
        handler: &Arc<Handler>,
        metrics: &Arc<SubscriptionMetrics>,
        query: &str,
    ) -> MemberQuery {
        let own = ReevaluatingQuery::new(Arc::clone(handler), KG, query).unwrap();
        families.member(own, lifted(query).unwrap(), metrics)
    }

    /// Views of `queries` that never refresh: bindings that, with the
    /// views a test refreshes, outnumber [`handler`]'s compute permits.
    fn idle(
        families: &Families,
        handler: &Arc<Handler>,
        metrics: &Arc<SubscriptionMetrics>,
        queries: &[&str],
    ) -> Vec<MemberQuery> {
        queries
            .iter()
            .map(|query| member(families, handler, metrics, query))
            .collect()
    }

    fn inserted(refresh: &Refresh) -> Vec<serde_json::Value> {
        refresh.queries[0]
            .inserted
            .iter()
            .map(|row| json!(row))
            .collect()
    }

    /// Wait for `family`'s probe, if any, to be judged.
    async fn probed(family: &Family) {
        while family.probing.load(Ordering::Relaxed) {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn views_share_a_round_that_lets_its_snapshot_go() {
        let (handler, _tmp) = handler();
        write(&handler, "+item[(\"s1\", 1), (\"s2\", 2)]").await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let mut one = member(&families, &handler, &metrics, r#"?item("s1", X)"#);
        let mut two = member(&families, &handler, &metrics, r#"?item("s2", X)"#);
        let _idle = idle(
            &families,
            &handler,
            &metrics,
            &[r#"?item("s3", X)"#, r#"?item("s4", X)"#],
        );
        assert!(!one.family.shares(), "no own cost to compare a round with");
        // First refreshes evaluate each view's own query, compiling its plan:
        // not a cost to compare a round with.
        assert_eq!(inserted(&one.refresh().await.unwrap()), [json!(["s1", 1])]);
        assert_eq!(inserted(&two.refresh().await.unwrap()), [json!(["s2", 2])]);
        assert_eq!(one.family.own_cost_us.load(Ordering::Relaxed), 0);
        assert!(!one.family.probing.load(Ordering::Relaxed));
        assert!(!one.family.shares());
        // Their next evaluations reuse the plans: their cost starts a probe.
        // Own evaluations slower than any round here: the guard keeps sharing.
        write(&handler, "+item[(\"s1\", 2), (\"s2\", 3)]").await;
        assert_eq!(inserted(&one.refresh().await.unwrap()), [json!(["s1", 2])]);
        let own = one.family.own_cost_us.load(Ordering::Relaxed);
        assert!(own > 0);
        one.family
            .own_cost_us
            .store(own.max(1_000_000_000), Ordering::Relaxed);
        assert_eq!(inserted(&two.refresh().await.unwrap()), [json!(["s2", 3])]);
        probed(&one.family).await;
        assert!(one.family.shares(), "the probe kept sharing");
        let before = metrics.shared_evaluations();

        write(&handler, "+item[(\"s1\", 3), (\"s2\", 4)]").await;
        let first = one.refresh().await.unwrap();
        let second = two.refresh().await.unwrap();
        assert_eq!(inserted(&first), [json!(["s1", 3])]);
        assert_eq!(inserted(&second), [json!(["s2", 4])]);
        assert_eq!(first.revision, second.revision);
        assert_eq!(
            metrics.shared_evaluations(),
            before + 1,
            "one round for both views"
        );
        let round = one.family.latest.load_full().unwrap();
        assert!(
            matches!(*round.state.lock(), RoundState::Started { revision } if revision == first.revision),
            "the round evaluated one snapshot and holds none"
        );

        // A view leaving the family leaves no more bindings than permits.
        drop(two);
        write(&handler, "+item(\"s1\", 5)").await;
        assert_eq!(inserted(&one.refresh().await.unwrap()), [json!(["s1", 5])]);
        assert_eq!(metrics.shared_evaluations(), before + 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_view_sharing_at_the_probes_revision_reads_its_round() {
        let (handler, _tmp) = handler();
        write(&handler, "+item[(\"s1\", 1), (\"s2\", 2)]").await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let mut one = member(&families, &handler, &metrics, r#"?item("s1", X)"#);
        let mut two = member(&families, &handler, &metrics, r#"?item("s2", X)"#);
        let _idle = idle(
            &families,
            &handler,
            &metrics,
            &[r#"?item("s3", X)"#, r#"?item("s4", X)"#],
        );
        one.refresh().await.unwrap();
        two.refresh().await.unwrap();

        // Hold the probe until own evaluations are slower than any round.
        let family = Arc::clone(&one.family);
        let gate = family.probe_gate.write().await;
        write(&handler, "+item[(\"s1\", 2), (\"s2\", 3)]").await;
        assert_eq!(inserted(&one.refresh().await.unwrap()), [json!(["s1", 2])]);
        assert!(family.probing.load(Ordering::Relaxed), "probe started");
        family.own_cost_us.store(1_000_000_000, Ordering::Relaxed);
        drop(gate);
        probed(&family).await;
        assert!(family.shares(), "the probe kept sharing");
        assert_eq!(metrics.shared_evaluations(), 1);

        // The view not yet refreshed at the probe's revision reads its round.
        assert_eq!(inserted(&two.refresh().await.unwrap()), [json!(["s2", 3])]);
        assert_eq!(
            metrics.shared_evaluations(),
            1,
            "one evaluation for the revision"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shapes_of_different_parameter_kinds_are_different_families() {
        let (handler, _tmp) = handler();
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let one = member(&families, &handler, &metrics, r#"?item("s1", X)"#);
        let int = member(&families, &handler, &metrics, "?item(1, X)");
        let two = member(&families, &handler, &metrics, r#"?item("s2", X)"#);
        assert!(!Arc::ptr_eq(&one.family, &int.family));
        assert!(Arc::ptr_eq(&one.family, &two.family));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_slow_round_stops_sharing_even_when_it_compiled_its_plan() {
        let (handler, _tmp) = handler();
        write(&handler, "+item[(\"s1\", 1), (\"s2\", 2)]").await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let mut one = member(&families, &handler, &metrics, r#"?item("s1", X)"#);
        let mut two = member(&families, &handler, &metrics, r#"?item("s2", X)"#);
        let _idle = idle(
            &families,
            &handler,
            &metrics,
            &[r#"?item("s3", X)"#, r#"?item("s4", X)"#],
        );
        one.refresh().await.unwrap();
        two.refresh().await.unwrap();
        // Own evaluations far faster than any round.
        one.family.own_cost_us.store(1, Ordering::Relaxed);
        one.family.sharing.store(true, Ordering::Relaxed);
        assert!(one.family.shares());

        write(&handler, "+item(\"s1\", 3)").await;
        assert_eq!(inserted(&one.refresh().await.unwrap()), [json!(["s1", 3])]);
        assert_eq!(
            metrics.shared_evaluations(),
            1,
            "the round compiled its plan"
        );
        assert!(!one.family.shares(), "and was still judged too slow");
        assert!(
            one.family.latest.load().is_none(),
            "the round judged too slow is let go"
        );
    }

    /// A chain of `n` edges, reachable as `reach`: a bound reach is short
    /// near its end, the unbound closure quadratic.
    async fn chain(handler: &Handler, n: usize) {
        let edges: Vec<String> = (0..n)
            .map(|i| format!("(\"n{i}\", \"n{}\")", i + 1))
            .collect();
        write(
            handler,
            &format!(
                "+edge[{}]\n\
                 +reach(X, Y) <- edge(X, Y)\n\
                 +reach(X, Z) <- reach(X, Y), edge(Y, Z)",
                edges.join(", ")
            ),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_probe_too_slow_to_share_never_makes_a_view_wait() {
        let (handler, _tmp) = handler();
        write(&handler, "+item[(\"s1\", 1), (\"s2\", 2)]").await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let mut one = member(&families, &handler, &metrics, r#"?item("s1", X)"#);
        let mut two = member(&families, &handler, &metrics, r#"?item("s2", X)"#);
        let _idle = idle(
            &families,
            &handler,
            &metrics,
            &[r#"?item("s3", X)"#, r#"?item("s4", X)"#],
        );
        one.refresh().await.unwrap();
        two.refresh().await.unwrap();
        assert_eq!(metrics.shared_evaluations(), 0);

        // Hold the probe until the views have refreshed.
        let family = Arc::clone(&one.family);
        let gate = family.probe_gate.write().await;
        // A plan-cached own evaluation makes a probe due.
        write(&handler, "+item(\"x\", 1)").await;
        one.refresh().await.unwrap();
        assert!(family.probing.load(Ordering::Relaxed), "probe started");
        // While it is held, views evaluate their own queries without waiting.
        write(&handler, "+item(\"s2\", 3)").await;
        let refresh = two.refresh().await.unwrap();
        assert_eq!(inserted(&refresh), [json!(["s2", 3])]);
        assert!(!family.shares());
        assert_eq!(metrics.shared_evaluations(), 0, "the probe is held");
        // Own evaluations far faster than any round, set after the last own
        // evaluation so that its measured cost does not move the average.
        family.own_cost_us.store(1, Ordering::Relaxed);
        drop(gate);

        probed(&family).await;
        assert_eq!(metrics.shared_evaluations(), 1, "only the probe's round");
        assert!(!family.shares(), "the probe was judged too slow");
        assert_eq!(family.stops.load(Ordering::Relaxed), 1);
        assert!(
            family.latest.load().is_none(),
            "the round judged too slow is let go"
        );
        write(&handler, "+item(\"s1\", 4)").await;
        assert_eq!(inserted(&one.refresh().await.unwrap()), [json!(["s1", 4])]);
        assert!(!family.probing.load(Ordering::Relaxed), "backing off");
        assert_eq!(metrics.shared_evaluations(), 1);
    }

    /// Views of `?reach("nK", Y)` for `bindings` nodes near the end of a
    /// 300-edge chain on a handler of `permits`, refreshed over a few
    /// commits: none evaluates the lifted, unbound closure, whose 45k rows
    /// the row cap would refuse anyway.
    async fn recursion_bound_family_never_evaluates_its_lifted_query(
        permits: usize,
        bindings: usize,
    ) {
        let (handler, _tmp) = handler_capped(permits, 1_000);
        chain(&handler, 300).await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let mut views: Vec<_> = (299 - bindings..299)
            .map(|i| {
                member(
                    &families,
                    &handler,
                    &metrics,
                    &format!("?reach(\"n{i}\", Y)"),
                )
            })
            .collect();
        for view in &mut views {
            view.refresh().await.unwrap();
        }
        let family = Arc::clone(&views[0].family);
        for round in 0..3 {
            write(&handler, &format!("+edge(\"n300\", \"t{round}\")")).await;
            for view in &mut views {
                assert_eq!(inserted(&view.refresh().await.unwrap()).len(), 1);
                // Own evaluations slower than any round here.
                family.own_cost_us.store(1_000_000_000, Ordering::Relaxed);
                assert!(!family.probing.load(Ordering::Relaxed), "no probe");
            }
        }
        // Not even when told to share.
        family.sharing.store(true, Ordering::Relaxed);
        write(&handler, "+edge(\"n300\", \"u\")").await;
        for view in &mut views {
            assert_eq!(inserted(&view.refresh().await.unwrap()).len(), 1);
        }
        assert!(!family.shares());
        assert_eq!(metrics.shared_evaluations(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_recursion_bound_family_never_evaluates_its_lifted_query() {
        recursion_bound_family_never_evaluates_its_lifted_query(8, 9).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn without_permits_to_probe_a_recursion_bound_family_never_shares() {
        recursion_bound_family_never_evaluates_its_lifted_query(2, 4).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_round_from_before_a_rule_change_does_not_share_again() {
        let (handler, _tmp) = handler();
        write(&handler, "+item[(\"s1\", 1), (\"s2\", 2)]").await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let mut views: Vec<_> = (1..=4)
            .map(|i| {
                member(
                    &families,
                    &handler,
                    &metrics,
                    &format!("?item(\"s{i}\", X)"),
                )
            })
            .collect();
        let family = Arc::clone(&views[0].family);
        for view in &mut views {
            view.refresh().await.unwrap();
        }
        write(&handler, "+item(\"s1\", 3)").await;
        family.own_cost_us.store(1_000_000_000, Ordering::Relaxed);
        views[0].refresh().await.unwrap();
        probed(&family).await;
        assert!(family.shares());

        // A round starts on the rules before the change, then is held.
        let gate = family.round_gate.write().await;
        write(&handler, "+item(\"s2\", 4)").await;
        let old_rules = revision(&handler);
        let held = refreshing(views.remove(1));
        latest_round(
            &family,
            |state| matches!(state, RoundState::Started { revision } if revision == old_rules),
        )
        .await;

        write(&handler, "+tagged(S) <- item(S, 1)").await;
        views[0].refresh().await.unwrap();
        assert!(!family.shares());
        // A cost measured under the new rules, which the held round's old
        // rules must not be judged against.
        family.own_cost_us.store(1_000_000_000, Ordering::Relaxed);
        drop(gate);
        let (_, refresh) = held.await.unwrap();
        assert_eq!(inserted(&refresh), [json!(["s2", 4])]);
        assert!(!family.shares(), "the round's rules are no longer judged");
        assert_eq!(family.stops.load(Ordering::Relaxed), 0, "not a failure");
        assert_eq!(family.shared_cost_us.load(Ordering::Relaxed), 0);
        let now = views[0].own.current_snapshot().unwrap();
        let judged = family.judged.load_full().unwrap();
        assert!(Arc::ptr_eq(&judged.rules, now.persistent_rules()));
        assert_eq!(family.own_cost_us.load(Ordering::Relaxed), 1_000_000_000);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_own_cost_from_before_a_rule_change_is_dropped() {
        let (handler, _tmp) = handler();
        write(&handler, "+item[(\"s1\", 1), (\"s2\", 2)]").await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let mut views: Vec<_> = (1..=4)
            .map(|i| {
                member(
                    &families,
                    &handler,
                    &metrics,
                    &format!("?item(\"s{i}\", X)"),
                )
            })
            .collect();
        let family = Arc::clone(&views[0].family);
        for view in &mut views {
            view.refresh().await.unwrap();
        }
        let before = views[0].own.current_snapshot().unwrap();

        write(&handler, "+tagged(S) <- item(S, 1)").await;
        views[0].refresh().await.unwrap();
        assert_eq!(family.own_cost_us.load(Ordering::Relaxed), 0);
        // An own evaluation of the old rules that finishes after the change.
        let probe = family.record_own(Some(std::time::Duration::from_millis(5)), &before);
        assert!(!probe);
        assert_eq!(family.own_cost_us.load(Ordering::Relaxed), 0);
        assert_eq!(family.own_since_stop.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_rule_change_starts_the_costs_over_and_judges_the_shape_again() {
        let (handler, _tmp) = handler();
        write(
            &handler,
            "+edge[(\"n1\", \"n2\"), (\"n2\", \"n3\")]\n+reach(X, Y) <- edge(X, Y)",
        )
        .await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let mut views: Vec<_> = (1..=4)
            .map(|i| {
                member(
                    &families,
                    &handler,
                    &metrics,
                    &format!("?reach(\"n{i}\", Y)"),
                )
            })
            .collect();
        let family = Arc::clone(&views[0].family);
        for view in &mut views {
            view.refresh().await.unwrap();
        }
        write(&handler, "+edge(\"n4\", \"n5\")").await;
        family.own_cost_us.store(1_000_000_000, Ordering::Relaxed);
        views[0].refresh().await.unwrap();
        probed(&family).await;
        assert!(family.shares(), "not recursive: the probe kept sharing");
        let shared = metrics.shared_evaluations();

        // The rule change makes `reach` recursive.
        write(&handler, "+reach(X, Z) <- reach(X, Y), edge(Y, Z)").await;
        views[0].refresh().await.unwrap();
        assert!(!family.shares(), "costs of the old rules no longer hold");
        assert_eq!(family.own_cost_us.load(Ordering::Relaxed), 0);
        assert_eq!(family.stops.load(Ordering::Relaxed), 0, "not a failure");
        for view in &mut views[1..] {
            view.refresh().await.unwrap();
        }
        for round in 0..2 {
            write(&handler, &format!("+edge(\"n5\", \"t{round}\")")).await;
            for view in &mut views {
                view.refresh().await.unwrap();
                family.own_cost_us.store(1_000_000_000, Ordering::Relaxed);
                assert!(!family.probing.load(Ordering::Relaxed), "no probe");
            }
        }
        assert!(!family.shares());
        assert_eq!(metrics.shared_evaluations(), shared);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn without_permits_to_probe_a_family_shares_by_its_shape() {
        let (handler, _tmp) = handler_with(2);
        write(&handler, "+item[(\"s1\", 1), (\"s2\", 2)]").await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let mut one = member(&families, &handler, &metrics, r#"?item("s1", X)"#);
        let mut two = member(&families, &handler, &metrics, r#"?item("s2", X)"#);
        let _idle = idle(&families, &handler, &metrics, &[r#"?item("s3", X)"#]);
        one.refresh().await.unwrap();
        two.refresh().await.unwrap();

        // The first plan-cached own evaluation decides, without a probe.
        write(&handler, "+item[(\"s1\", 3), (\"s2\", 4)]").await;
        assert_eq!(inserted(&one.refresh().await.unwrap()), [json!(["s1", 3])]);
        assert!(one.family.shares());
        assert!(!one.family.probing.load(Ordering::Relaxed));
        assert_eq!(metrics.shared_evaluations(), 0);
        // Own evaluations slower than any round here: the guard keeps sharing.
        one.family
            .own_cost_us
            .store(1_000_000_000, Ordering::Relaxed);
        assert_eq!(inserted(&two.refresh().await.unwrap()), [json!(["s2", 4])]);
        assert_eq!(metrics.shared_evaluations(), 1);
        assert!(one.family.shares());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_probe_runs_while_queries_hold_every_compute_permit() {
        let (handler, _tmp) = handler();
        write(&handler, "+item[(\"s1\", 1), (\"s2\", 2)]").await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let mut one = member(&families, &handler, &metrics, r#"?item("s1", X)"#);
        let mut two = member(&families, &handler, &metrics, r#"?item("s2", X)"#);
        let _idle = idle(
            &families,
            &handler,
            &metrics,
            &[r#"?item("s3", X)"#, r#"?item("s4", X)"#],
        );
        one.refresh().await.unwrap();
        two.refresh().await.unwrap();
        one.family
            .own_cost_us
            .store(1_000_000_000, Ordering::Relaxed);

        let held = handler.hold_compute_permits();
        Family::probe(&one.family, two.own.current_snapshot().unwrap());
        probed(&one.family).await;
        assert_eq!(metrics.shared_evaluations(), 1);
        assert!(one.family.shares(), "the probe passed the guard");
        drop(held);

        write(&handler, "+item[(\"s1\", 3), (\"s2\", 4)]").await;
        assert_eq!(inserted(&one.refresh().await.unwrap()), [json!(["s1", 3])]);
        assert_eq!(inserted(&two.refresh().await.unwrap()), [json!(["s2", 4])]);
        assert_eq!(metrics.shared_evaluations(), 2, "views read one round");
    }

    /// The newest revision of the test knowledge graph.
    fn revision(handler: &Handler) -> u64 {
        handler.get_storage().get_snapshot_for(KG).unwrap().revision
    }

    /// Wait until `family`'s latest round is in a state `ready` accepts.
    async fn latest_round(family: &Family, ready: impl Fn(RoundState) -> bool) {
        loop {
            let state = family.latest.load_full().map(|round| *round.state.lock());
            if state.is_some_and(&ready) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    /// Views of `?item("sK", X)` for `active`, refreshed once, plus idle
    /// views so that the family outnumbers [`handler`]'s compute permits,
    /// sharing.
    async fn sharing_views(
        families: &Families,
        handler: &Arc<Handler>,
        metrics: &Arc<SubscriptionMetrics>,
        active: &[&str],
    ) -> (Vec<MemberQuery>, Vec<MemberQuery>) {
        let mut views: Vec<MemberQuery> = active
            .iter()
            .map(|s| member(families, handler, metrics, &format!("?item(\"{s}\", X)")))
            .collect();
        let idle = idle(
            families,
            handler,
            metrics,
            &[
                r#"?item("i1", X)"#,
                r#"?item("i2", X)"#,
                r#"?item("i3", X)"#,
            ],
        );
        for view in &mut views {
            view.refresh().await.unwrap();
        }
        let family = &views[0].family;
        family.sharing.store(true, Ordering::Relaxed);
        assert!(family.shares());
        (views, idle)
    }

    /// Refresh `view` on its own task.
    fn refreshing(mut view: MemberQuery) -> tokio::task::JoinHandle<(MemberQuery, Refresh)> {
        tokio::spawn(async move {
            let refresh = view.refresh().await.unwrap();
            (view, refresh)
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn views_that_ask_before_a_round_starts_share_it_at_the_newest_revision() {
        let (handler, _tmp) = handler();
        write(&handler, "+item[(\"s1\", 1), (\"s2\", 2)]").await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let (mut views, _idle) = sharing_views(&families, &handler, &metrics, &["s1", "s2"]).await;
        let family = Arc::clone(&views[0].family);
        let before = metrics.shared_evaluations();

        // Another round evaluating holds the family's turn: the next waits.
        let turn = family.evaluating.lock().await;
        write(&handler, "+item(\"s1\", 3)").await;
        let first = refreshing(views.remove(0));
        latest_round(&family, |state| matches!(state, RoundState::Pending { .. })).await;
        // A commit after the round was created: a view holding it joins the
        // same round, which will start no older than its snapshot.
        write(&handler, "+item(\"s2\", 4)").await;
        let newest = revision(&handler);
        let second = refreshing(views.remove(0));
        latest_round(
            &family,
            |state| matches!(state, RoundState::Pending { needs } if needs == newest),
        )
        .await;
        drop(turn);

        let (_, first) = first.await.unwrap();
        let (_, second) = second.await.unwrap();
        assert_eq!(inserted(&first), [json!(["s1", 3])]);
        assert_eq!(inserted(&second), [json!(["s2", 4])]);
        assert_eq!(first.revision, newest);
        assert_eq!(second.revision, newest);
        assert_eq!(
            metrics.shared_evaluations(),
            before + 1,
            "one round for both revisions"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_view_needing_a_newer_revision_than_the_round_evaluating_waits_for_the_next() {
        let (handler, _tmp) = handler();
        write(&handler, "+item[(\"s1\", 1), (\"s2\", 2), (\"s3\", 3)]").await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let (mut views, _idle) =
            sharing_views(&families, &handler, &metrics, &["s1", "s2", "s3"]).await;
        let family = Arc::clone(&views[0].family);
        let before = metrics.shared_evaluations();

        // The first round starts, then is held before it evaluates.
        let gate = family.round_gate.write().await;
        write(&handler, "+item(\"s1\", 10)").await;
        let started = revision(&handler);
        let first = refreshing(views.remove(0));
        latest_round(
            &family,
            |state| matches!(state, RoundState::Started { revision } if revision == started),
        )
        .await;

        // Commits after it started: the views that saw them neither read it
        // nor start a round beside it; both join one pending round.
        write(&handler, "+item(\"s2\", 20)").await;
        let second = refreshing(views.remove(0));
        latest_round(&family, |state| matches!(state, RoundState::Pending { .. })).await;
        write(&handler, "+item(\"s3\", 30)").await;
        let newest = revision(&handler);
        let third = refreshing(views.remove(0));
        latest_round(
            &family,
            |state| matches!(state, RoundState::Pending { needs } if needs == newest),
        )
        .await;
        assert_eq!(
            metrics.shared_evaluations(),
            before,
            "the first round is held"
        );
        drop(gate);

        let (_, first) = first.await.unwrap();
        let (_, second) = second.await.unwrap();
        let (_, third) = third.await.unwrap();
        assert_eq!(first.revision, started);
        assert_eq!(inserted(&first), [json!(["s1", 10])]);
        assert_eq!(second.revision, newest);
        assert_eq!(inserted(&second), [json!(["s2", 20])]);
        assert_eq!(third.revision, newest);
        assert_eq!(inserted(&third), [json!(["s3", 30])]);
        assert_eq!(metrics.shared_evaluations(), before + 2, "two rounds");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn views_refresh_while_requests_hold_every_compute_permit() {
        let (handler, _tmp) = handler();
        write(&handler, "+item[(\"s1\", 1), (\"s2\", 2)]").await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let (mut views, _idle) = sharing_views(&families, &handler, &metrics, &["s1", "s2"]).await;
        write(&handler, "+item[(\"s1\", 3), (\"s2\", 4)]").await;

        // Requests queued for compute permits, such as a burst of writes
        // waiting for their graph's commit, hold up no standing query.
        let held = handler.hold_compute_permits();
        let shared = views[0].refresh().await.unwrap();
        assert_eq!(inserted(&shared), [json!(["s1", 3])]);
        views[1].family.sharing.store(false, Ordering::Relaxed);
        let own = views[1].refresh().await.unwrap();
        assert_eq!(inserted(&own), [json!(["s2", 4])]);
        drop(held);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_family_whose_views_fit_on_the_compute_permits_never_shares() {
        // 3 sessions on 4 compute permits: their own evaluations run at once.
        let (handler, _tmp) = handler_with(4);
        write(&handler, "+item[(\"s1\", 1), (\"s2\", 2), (\"s3\", 3)]").await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let mut views: Vec<_> = (1..=3)
            .map(|i| {
                member(
                    &families,
                    &handler,
                    &metrics,
                    &format!("?item(\"s{i}\", X)"),
                )
            })
            .collect();
        let family = Arc::clone(&views[0].family);
        for view in &mut views {
            view.refresh().await.unwrap();
        }
        for round in 10..13 {
            write(
                &handler,
                &format!("+item[(\"s1\", {round}), (\"s2\", {round}), (\"s3\", {round})]"),
            )
            .await;
            for view in &mut views {
                assert_eq!(inserted(&view.refresh().await.unwrap()).len(), 1);
                // Own evaluations slower than any round here.
                family.own_cost_us.store(1_000_000_000, Ordering::Relaxed);
                assert!(!family.probing.load(Ordering::Relaxed), "no probe");
            }
        }
        // Not even when sharing was judged to pay with more views.
        family.sharing.store(true, Ordering::Relaxed);
        assert!(!family.shares());
        write(&handler, "+item(\"s1\", 99)").await;
        assert_eq!(
            inserted(&views[0].refresh().await.unwrap()),
            [json!(["s1", 99])]
        );
        assert_eq!(metrics.shared_evaluations(), 0);
    }
}
