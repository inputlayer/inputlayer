//! Variable-vs-variable comparisons across value types, end to end through `Handler`.

use crate::harness::handler;
use inputlayer::protocol::Handler;

async fn exec(handler: &Handler, program: &str) -> inputlayer::protocol::wire::QueryResult {
    handler
        .query_program(None, program.to_string())
        .await
        .unwrap_or_else(|e| panic!("Failed to execute '{program}': {e}"))
}

async fn rows(handler: &Handler, query: &str) -> Vec<Vec<String>> {
    let result = exec(handler, query).await;
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

async fn setup_pairs(handler: &Handler) {
    exec(handler, "+pair(a: string, b: string)").await;
    exec(
        handler,
        r#"+pair[("apple", "banana"), ("kiwi", "kiwi"), ("pear", "fig")]"#,
    )
    .await;
}

#[tokio::test]
async fn test_string_vars_all_operators_order_lexicographically() {
    let (handler, _t) = handler();
    setup_pairs(&handler).await;

    let lt = strs(&[&["apple", "banana"]]);
    let eq = strs(&[&["kiwi", "kiwi"]]);
    let gt = strs(&[&["pear", "fig"]]);
    let union = |a: &[Vec<String>], b: &[Vec<String>]| {
        let mut v = [a, b].concat();
        v.sort();
        v
    };

    assert_eq!(rows(&handler, "?pair(A, B), A < B").await, lt);
    assert_eq!(rows(&handler, "?pair(A, B), A <= B").await, union(&lt, &eq));
    assert_eq!(rows(&handler, "?pair(A, B), A > B").await, gt);
    assert_eq!(rows(&handler, "?pair(A, B), A >= B").await, union(&gt, &eq));
    assert_eq!(rows(&handler, "?pair(A, B), A = B").await, eq);
    assert_eq!(rows(&handler, "?pair(A, B), A != B").await, union(&lt, &gt));
}

#[tokio::test]
async fn test_string_vars_compare_in_persistent_rule() {
    let (handler, _t) = handler();
    setup_pairs(&handler).await;
    exec(&handler, "+ordered(A, B) <- pair(A, B), A < B").await;

    assert_eq!(
        rows(&handler, "?ordered(A, B)").await,
        strs(&[&["apple", "banana"]])
    );
}

#[tokio::test]
async fn test_mixed_type_vars_are_incomparable() {
    let (handler, _t) = handler();
    exec(&handler, r#"+mixed[(1, "a"), (2, 3.5)]"#).await;

    // int vs string: no ordering holds, only `!=`.
    assert_eq!(
        rows(&handler, "?mixed(X, Y), X < Y").await,
        vec![vec!["2".to_string(), "3.5".to_string()]]
    );
    assert!(rows(&handler, "?mixed(X, Y), X >= Y").await.is_empty());
    assert_eq!(rows(&handler, "?mixed(X, Y), X != Y").await.len(), 2);
}
