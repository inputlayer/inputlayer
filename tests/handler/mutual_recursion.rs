//! Mutually recursive rules evaluate to a joint fixpoint over their SCC.

use crate::harness::handler;
use inputlayer::protocol::Handler;
use inputlayer::provenance::proof_tree::NodeKind;

async fn exec(handler: &Handler, program: &str) -> inputlayer::protocol::wire::QueryResult {
    handler
        .query_program(None, program.to_string())
        .await
        .unwrap_or_else(|e| panic!("Failed to execute '{program}': {e}"))
}

async fn run_all(handler: &Handler, program: &str) {
    for line in program.lines().map(str::trim).filter(|l| !l.is_empty()) {
        exec(handler, line).await;
    }
}

/// Rows as sorted strings.
async fn rows(handler: &Handler, query: &str) -> Vec<String> {
    let result = exec(handler, query).await;
    let mut rows: Vec<String> = result
        .rows
        .iter()
        .map(|row| {
            row.values
                .iter()
                .map(|v| format!("{v}"))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect();
    rows.sort();
    rows
}

fn expect(values: &[&str]) -> Vec<String> {
    let mut v: Vec<String> = values.iter().map(|s| (*s).to_string()).collect();
    v.sort();
    v
}

const EVEN_ODD: &str = r"
    +succ[(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8)]
    +zero[(0)]
    +is_even(N) <- zero(N)
    +is_even(N) <- succ(M, N), is_odd(M)
    +is_odd(N) <- succ(M, N), is_even(M)
";

#[tokio::test]
async fn test_even_odd_over_chain_reaches_fixpoint() {
    let (handler, _t) = handler();
    run_all(&handler, EVEN_ODD).await;
    assert_eq!(
        rows(&handler, "?is_even(X)").await,
        expect(&["0", "2", "4", "6", "8"])
    );
    assert_eq!(
        rows(&handler, "?is_odd(X)").await,
        expect(&["1", "3", "5", "7"])
    );
}

#[tokio::test]
async fn test_three_way_cycle_reaches_fixpoint() {
    let (handler, _t) = handler();
    run_all(
        &handler,
        r"
        +succ[(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7)]
        +zero[(0)]
        +m0(N) <- zero(N)
        +m0(N) <- succ(M, N), m2(M)
        +m1(N) <- succ(M, N), m0(M)
        +m2(N) <- succ(M, N), m1(M)
        ",
    )
    .await;
    assert_eq!(rows(&handler, "?m0(X)").await, expect(&["0", "3", "6"]));
    assert_eq!(rows(&handler, "?m1(X)").await, expect(&["1", "4", "7"]));
    assert_eq!(rows(&handler, "?m2(X)").await, expect(&["2", "5"]));
}

#[tokio::test]
async fn test_bound_query_over_mutual_recursion() {
    let (handler, _t) = handler();
    run_all(&handler, EVEN_ODD).await;
    assert_eq!(rows(&handler, "?is_even(6)").await, expect(&["6"]));
    assert!(rows(&handler, "?is_even(5)").await.is_empty());

    // Binary relations with the bound column invariant across the cycle
    run_all(
        &handler,
        r"
        +edge[(1, 2), (2, 3), (3, 4), (4, 5), (10, 11), (11, 12)]
        +odd_path(X, Y) <- edge(X, Y)
        +odd_path(X, Z) <- even_path(X, Y), edge(Y, Z)
        +even_path(X, Z) <- odd_path(X, Y), edge(Y, Z)
        ",
    )
    .await;
    assert_eq!(
        rows(&handler, "?odd_path(1, Y)").await,
        expect(&["1,2", "1,4"])
    );
    assert_eq!(
        rows(&handler, "?even_path(1, Y)").await,
        expect(&["1,3", "1,5"])
    );
    assert_eq!(
        rows(&handler, "?even_path(10, Y)").await,
        expect(&["10,12"])
    );
}

#[tokio::test]
async fn test_base_retraction_shrinks_mutual_fixpoint() {
    let (handler, _t) = handler();
    run_all(&handler, EVEN_ODD).await;
    exec(&handler, "-succ(3, 4)").await;
    assert_eq!(rows(&handler, "?is_even(X)").await, expect(&["0", "2"]));
    assert_eq!(rows(&handler, "?is_odd(X)").await, expect(&["1", "3"]));
}

#[tokio::test]
async fn test_stratified_negation_above_mutual_recursion() {
    let (handler, _t) = handler();
    run_all(&handler, EVEN_ODD).await;
    run_all(
        &handler,
        r"
        +number[(0), (1), (2), (3), (4), (5), (6), (7), (8), (9)]
        +not_even(N) <- number(N), !is_even(N)
        ",
    )
    .await;
    assert_eq!(
        rows(&handler, "?not_even(X)").await,
        expect(&["1", "3", "5", "7", "9"])
    );
}

#[tokio::test]
async fn test_negation_inside_cycle_is_rejected() {
    let (handler, _t) = handler();
    run_all(&handler, "+base[(1), (2)]").await;
    let result = exec(
        &handler,
        "a(X) <- base(X), !b(X)\nb(X) <- base(X), !a(X)\n?a(X)",
    )
    .await;
    assert_eq!(result.errors.len(), 1, "{result:?}");
    assert_eq!(result.errors[0].index, 1);
    assert!(
        result.errors[0].message.contains("Unstratified negation"),
        "{:?}",
        result.errors
    );
}

#[tokio::test]
async fn test_session_rules_mutual_recursion() {
    let (handler, _t) = handler();
    run_all(
        &handler,
        "+succ[(0, 1), (1, 2), (2, 3), (3, 4)]\n+zero[(0)]",
    )
    .await;
    let result = exec(
        &handler,
        "ev(N) <- zero(N)\nev(N) <- succ(M, N), od(M)\nod(N) <- succ(M, N), ev(M)\n?od(X)",
    )
    .await;
    let mut got: Vec<String> = result
        .rows
        .iter()
        .map(|r| format!("{}", r.values[0]))
        .collect();
    got.sort();
    assert_eq!(got, expect(&["1", "3"]));
}

#[tokio::test]
async fn test_session_rule_over_persistent_mutual_recursion() {
    let (handler, _t) = handler();
    run_all(&handler, EVEN_ODD).await;
    let result = exec(&handler, "big_even(N) <- is_even(N), N > 4\n?big_even(X)").await;
    let mut got: Vec<String> = result
        .rows
        .iter()
        .map(|r| format!("{}", r.values[0]))
        .collect();
    got.sort();
    assert_eq!(got, expect(&["6", "8"]));
}

#[tokio::test]
async fn test_why_explains_mutually_recursive_fact() {
    let (handler, _t) = handler();
    run_all(&handler, EVEN_ODD).await;
    let result = exec(&handler, ".why ?is_even(4)").await;
    let graphs = result.proof_trees.expect("proof trees");
    assert!(!graphs.is_empty(), "is_even(4) should have a proof");

    let graph = &graphs[0];
    let root = &graph.nodes[&graph.roots[0]];
    assert_eq!(root.conclusion.pred, "is_even");
    assert_eq!(root.kind, NodeKind::Rule);
    let preds: Vec<&str> = graph
        .nodes
        .values()
        .map(|n| n.conclusion.pred.as_str())
        .collect();
    assert!(
        preds.contains(&"is_odd"),
        "proof should pass through is_odd"
    );
    assert!(
        preds.contains(&"zero"),
        "proof should bottom out at zero(0)"
    );
    assert!(graph.nodes.values().all(|n| n.kind != NodeKind::Truncated));
}

#[tokio::test]
async fn test_self_recursion_unaffected() {
    let (handler, _t) = handler();
    run_all(
        &handler,
        r"
        +edge[(1, 2), (2, 3), (3, 1)]
        +reach(X, Y) <- edge(X, Y)
        +reach(X, Z) <- reach(X, Y), edge(Y, Z)
        ",
    )
    .await;
    assert_eq!(
        rows(&handler, "?reach(1, Y)").await,
        expect(&["1,1", "1,2", "1,3"])
    );
}
