//! Scalar aggregate typing and aggregate head column order.

use inputlayer::protocol::{Handler, WireValue};
use inputlayer::Config;
use tempfile::TempDir;

fn handler() -> (Handler, TempDir) {
    let temp = TempDir::new().expect("temp dir");
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    let storage = inputlayer::StorageEngine::new(config).expect("storage engine");
    (Handler::new(storage), temp)
}

async fn run(program: &[&str], query: &str) -> Vec<Vec<WireValue>> {
    let (h, _t) = handler();
    for stmt in program {
        h.query_program(None, (*stmt).to_string())
            .await
            .unwrap_or_else(|e| panic!("'{stmt}': {e}"));
    }
    let result = h
        .query_program(None, query.to_string())
        .await
        .unwrap_or_else(|e| panic!("'{query}': {e}"));
    let mut rows: Vec<Vec<WireValue>> = result.rows.into_iter().map(|r| r.values).collect();
    rows.sort_by_key(|r| format!("{r:?}"));
    rows
}

fn s(v: &str) -> WireValue {
    WireValue::String(v.to_string())
}

#[tokio::test]
async fn sum_of_floats_is_not_truncated() {
    let rows = run(
        &["+p[(\"a\", 1.5), (\"a\", 2.5)]", "+t(G, sum<P>) <- p(G, P)"],
        "?t(G, S)",
    )
    .await;
    assert_eq!(rows, vec![vec![s("a"), WireValue::Float64(4.0)]]);
}

#[tokio::test]
async fn sum_of_ints_stays_int() {
    let rows = run(
        &["+p[(\"a\", 1), (\"a\", 2)]", "+t(G, sum<P>) <- p(G, P)"],
        "?t(G, S)",
    )
    .await;
    assert_eq!(rows, vec![vec![s("a"), WireValue::Int64(3)]]);
}

#[tokio::test]
async fn min_max_compare_numerically_across_types() {
    let program = [
        "+p[(\"a\", 5), (\"a\", 2.5)]",
        "+lo(G, min<P>) <- p(G, P)",
        "+hi(G, max<P>) <- p(G, P)",
    ];
    assert_eq!(
        run(&program, "?lo(G, M)").await,
        vec![vec![s("a"), WireValue::Float64(2.5)]]
    );
    assert_eq!(
        run(&program, "?hi(G, M)").await,
        vec![vec![s("a"), WireValue::Int64(5)]]
    );
}

#[tokio::test]
async fn avg_skips_non_numeric() {
    let rows = run(
        &[
            "+p[(\"a\", 2), (\"a\", 4), (\"a\", \"x\")]",
            "+t(G, avg<P>) <- p(G, P)",
        ],
        "?t(G, A)",
    )
    .await;
    assert_eq!(rows, vec![vec![s("a"), WireValue::Float64(3.0)]]);
}
