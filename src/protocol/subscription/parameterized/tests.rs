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

mod rounds {
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::TempDir;

    use super::super::{Families, MemberQuery};
    use super::lifted;
    use crate::protocol::subscription::{
        ReevaluatingQuery, Refresh, StandingQuery, SubscriptionMetrics,
    };
    use crate::protocol::Handler;
    use crate::Config;

    const KG: &str = "rounds";

    fn handler() -> (Arc<Handler>, TempDir) {
        let tmp = TempDir::new().unwrap();
        let mut config = Config::default();
        config.storage.data_dir = tmp.path().join("data");
        let handler = Arc::new(Handler::from_config(config).unwrap());
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

    fn inserted(refresh: &Refresh) -> Vec<serde_json::Value> {
        refresh.inserted.iter().map(|row| json!(row)).collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn views_share_a_round_that_lets_its_snapshot_go() {
        let (handler, _tmp) = handler();
        write(&handler, "+item[(\"s1\", 1), (\"s2\", 2)]").await;
        let families = Families::default();
        let metrics = Arc::new(SubscriptionMetrics::default());
        let mut one = member(&families, &handler, &metrics, r#"?item("s1", X)"#);
        let mut two = member(&families, &handler, &metrics, r#"?item("s2", X)"#);
        // First refreshes evaluate each view's own query.
        assert_eq!(inserted(&one.refresh().await.unwrap()), [json!(["s1", 1])]);
        assert_eq!(inserted(&two.refresh().await.unwrap()), [json!(["s2", 2])]);
        assert_eq!(metrics.shared_evaluations(), 0);

        write(&handler, "+item[(\"s1\", 3), (\"s2\", 4)]").await;
        let first = one.refresh().await.unwrap();
        let second = two.refresh().await.unwrap();
        assert_eq!(inserted(&first), [json!(["s1", 3])]);
        assert_eq!(inserted(&second), [json!(["s2", 4])]);
        assert_eq!(first.revision, second.revision);
        assert_eq!(metrics.shared_evaluations(), 1, "one round for both views");
        let round = one.family.latest.load_full().unwrap();
        assert!(
            round.snapshot.load().is_none(),
            "the evaluated round dropped its snapshot"
        );

        // A view leaving the family leaves one binding: nothing to share.
        drop(two);
        write(&handler, "+item(\"s1\", 5)").await;
        assert_eq!(inserted(&one.refresh().await.unwrap()), [json!(["s1", 5])]);
        assert_eq!(metrics.shared_evaluations(), 1);
    }
}
