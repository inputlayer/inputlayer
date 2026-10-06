//! Multi-clause rules whose clauses share a derived relation (issue #91).

use crate::harness::handler;
use inputlayer::protocol::Handler;

async fn exec(handler: &Handler, program: &str) -> inputlayer::protocol::wire::QueryResult {
    handler
        .query_program(None, program.to_string())
        .await
        .unwrap_or_else(|e| panic!("Failed to execute '{program}': {e}"))
}

async fn run_all(handler: &Handler, program: &str) -> inputlayer::protocol::wire::QueryResult {
    let mut last = None;
    for line in program.lines().map(str::trim).filter(|l| !l.is_empty()) {
        last = Some(exec(handler, line).await);
    }
    last.expect("program has no statements")
}

fn sorted_rows(result: &inputlayer::protocol::wire::QueryResult) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = result
        .rows
        .iter()
        .map(|row| row.values.iter().map(|v| format!("{v}")).collect())
        .collect();
    rows.sort();
    rows
}

fn strs(rows: &[&[&str]]) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = rows
        .iter()
        .map(|r| r.iter().map(|s| format!("\"{s}\"")).collect())
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn test_multi_clause_persistent_union_with_constant_returns_both_clauses() {
    let (handler, _t) = handler();
    let result = run_all(
        &handler,
        r#"
        +claim(id: string, entity: string, attr: string, val: string)
        +claim_modality(id: string, modality: string)
        +link(x: string, y: string)
        +claim[("c1", "e1", "attr", "v1"), ("c2", "e2", "attr", "v2"), ("c5", "anna_1", "distinct_from", "anna_2")]
        +claim_modality[("c1", "asserted"), ("c2", "asserted"), ("c5", "asserted")]
        +link[("e1", "e2")]
        +active(C, E, A, V) <- claim(C, E, A, V), claim_modality(C, "asserted")
        +r("bridge", C1, C2) <- active(C1, E1, A1, V1), active(C2, E2, A2, V2), link(E1, E2)
        +r("solo", C, C) <- active(C, E1, "distinct_from", E2)
        ?r(K, C1, C2)
        "#,
    )
    .await;
    assert_eq!(
        sorted_rows(&result),
        strs(&[&["bridge", "c1", "c2"], &["solo", "c5", "c5"]])
    );
}

#[tokio::test]
async fn test_multi_clause_persistent_self_join_union_keeps_head_arity() {
    let (handler, _t) = handler();
    let result = run_all(
        &handler,
        r#"
        +ca(c: string, e: string)
        +ca[("c1", "e1"), ("c2", "e1")]
        +a(C, E) <- ca(C, E)
        +r("joined", C1, C2) <- a(C1, E), a(C2, E), C1 != C2
        +r("solo", C, C) <- a(C, E)
        ?r(K, C1, C2)
        "#,
    )
    .await;
    assert_eq!(result.schema.len(), 3);
    assert_eq!(
        sorted_rows(&result),
        strs(&[
            &["joined", "c1", "c2"],
            &["joined", "c2", "c1"],
            &["solo", "c1", "c1"],
            &["solo", "c2", "c2"],
        ])
    );
}

#[tokio::test]
async fn test_multi_clause_union_poisoning_is_not_retroactive() {
    let (handler, _t) = handler();
    run_all(
        &handler,
        r#"
        +ca(c: string, e: string)
        +ca[("c1", "e1"), ("c2", "e1")]
        +a(C, E) <- ca(C, E)
        +r("joined", C1, C2) <- a(C1, E), a(C2, E), C1 != C2
        "#,
    )
    .await;
    let before = exec(&handler, "?r(K, C1, C2)").await;
    assert_eq!(before.rows.len(), 2);
    exec(&handler, r#"+r("solo", C, C) <- a(C, E)"#).await;
    let after = exec(&handler, "?r(K, C1, C2)").await;
    assert_eq!(after.rows.len(), 4);
}

#[tokio::test]
async fn test_multi_clause_session_rules_sharing_derived_relation() {
    let (handler, _t) = handler();
    run_all(
        &handler,
        r#"
        +ca(c: string, e: string)
        +ca[("c1", "e1"), ("c2", "e1")]
        "#,
    )
    .await;
    let result = exec(
        &handler,
        "a(C, E) <- ca(C, E)\n\
         r(\"joined\", C1, C2) <- a(C1, E), a(C2, E), C1 != C2\n\
         r(\"solo\", C, C) <- a(C, E)\n\
         ?r(K, C1, C2)",
    )
    .await;
    assert_eq!(result.schema.len(), 3);
    assert_eq!(
        sorted_rows(&result),
        strs(&[
            &["joined", "c1", "c2"],
            &["joined", "c2", "c1"],
            &["solo", "c1", "c1"],
            &["solo", "c2", "c2"],
        ])
    );
}

/// The consistency-core `conflict` relation as one multi-clause rule over
/// shared `active` / `coentity` relations.
#[tokio::test]
async fn test_multi_clause_consistency_conflict_collapsed_into_one_relation() {
    let (handler, _t) = handler();
    let result = run_all(
        &handler,
        r#"
        +claim(id: string, entity: string, attr: string, val: string)
        +claim_modality(id: string, modality: string)
        +same_as(e1: string, e2: string)
        +functional(attr: string)
        +asymmetric(attr: string)
        +claim[("c1", "bob", "age_band", "young"), ("c2", "robert", "age_band", "old"), ("c3", "anna_1", "distinct_from", "anna_2"), ("c4", "ann", "parent_of", "joe"), ("c5", "joe", "parent_of", "ann"), ("c6", "bob", "city", "paris")]
        +claim_modality[("c1", "asserted"), ("c2", "asserted"), ("c3", "asserted"), ("c4", "asserted"), ("c5", "asserted"), ("c6", "negated")]
        +same_as[("bob", "robert"), ("anna_1", "anna_2")]
        +functional[("age_band",)]
        +asymmetric[("parent_of",)]
        +active(C, E, A, V) <- claim(C, E, A, V), claim_modality(C, "asserted")
        +mentioned(E) <- claim(C, E, A, V)
        +coentity(E, E) <- mentioned(E)
        +coentity(X, Y) <- same_as(X, Y)
        +coentity(X, Y) <- same_as(Y, X)
        +conflict("identity", C, C) <- active(C, E1, "distinct_from", E2), coentity(E1, E2)
        +conflict("functional", C1, C2) <- active(C1, E1, A, V1), active(C2, E2, A, V2), coentity(E1, E2), functional(A), V1 != V2
        +conflict("asymmetry", C1, C2) <- active(C1, X, A, Y), active(C2, Y, A, X), asymmetric(A), X != Y
        +conflict("polarity", C1, C2) <- active(C1, E1, A, V), claim(C2, E2, A, V), claim_modality(C2, "negated"), coentity(E1, E2)
        ?conflict(K, C1, C2)
        "#,
    )
    .await;
    assert_eq!(
        sorted_rows(&result),
        strs(&[
            &["asymmetry", "c4", "c5"],
            &["asymmetry", "c5", "c4"],
            &["functional", "c1", "c2"],
            &["functional", "c2", "c1"],
            &["identity", "c3", "c3"],
        ])
    );
}

#[tokio::test]
async fn test_multi_clause_with_negation_branch_sharing_variables() {
    let (handler, _t) = handler();
    let result = run_all(
        &handler,
        r#"
        +ca(c: string, e: string)
        +blocked(c: string)
        +ca[("c1", "e1"), ("c2", "e1"), ("c3", "e2")]
        +blocked[("c2",)]
        +a(C, E) <- ca(C, E)
        +r("pair", C1, C2) <- a(C1, E), a(C2, E), C1 != C2
        +r("open", C1, C1) <- a(C1, E), !blocked(C1)
        ?r(K, X, Y)
        "#,
    )
    .await;
    assert_eq!(
        sorted_rows(&result),
        strs(&[
            &["open", "c1", "c1"],
            &["open", "c3", "c3"],
            &["pair", "c1", "c2"],
            &["pair", "c2", "c1"],
        ])
    );
}

#[tokio::test]
async fn test_multi_clause_recursion_over_derived_relation_with_bound_query() {
    let (handler, _t) = handler();
    let result = run_all(
        &handler,
        r#"
        +raw(x: string, y: string, kind: string)
        +raw[("a", "b", "road"), ("b", "c", "road"), ("c", "d", "rail"), ("x", "y", "road")]
        +e(X, Y) <- raw(X, Y, "road")
        +reach(X, Y) <- e(X, Y)
        +reach(X, Z) <- reach(X, Y), e(Y, Z)
        +reach(X, Y) <- raw(X, Y, "rail")
        ?reach("a", Y)
        "#,
    )
    .await;
    assert_eq!(sorted_rows(&result), strs(&[&["a", "b"], &["a", "c"]]));
}

#[tokio::test]
async fn test_multi_clause_aggregate_over_union_sharing_derived_relation() {
    let (handler, _t) = handler();
    let result = run_all(
        &handler,
        r#"
        +ca(c: string, e: string)
        +ca[("c1", "e1"), ("c2", "e1"), ("c3", "e2")]
        +a(C, E) <- ca(C, E)
        +r(C1, C2) <- a(C1, E), a(C2, E), C1 != C2
        +r(C, C) <- a(C, E)
        +tally(C, count<D>) <- r(C, D)
        ?tally(C, N)
        "#,
    )
    .await;
    let rows: Vec<Vec<String>> = sorted_rows(&result);
    assert_eq!(
        rows,
        vec![
            vec!["\"c1\"".to_string(), "2".to_string()],
            vec!["\"c2\"".to_string(), "2".to_string()],
            vec!["\"c3\"".to_string(), "1".to_string()],
        ]
    );
}
