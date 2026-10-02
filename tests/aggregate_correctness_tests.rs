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

#[tokio::test]
async fn aggregate_before_group_key_keeps_head_order() {
    let rows = run(
        &["+p[(\"a\", 1), (\"a\", 2)]", "+t(count<P>, G) <- p(G, P)"],
        "?t(N, G)",
    )
    .await;
    assert_eq!(rows, vec![vec![WireValue::Int64(2), s("a")]]);
}

#[tokio::test]
async fn transient_aggregate_query_keeps_head_order() {
    let rows = run(
        &["+p[(\"a\", 1), (\"a\", 2)]"],
        "t(count<P>, G) <- p(G, P)\n?t(N, G)",
    )
    .await;
    assert_eq!(rows, vec![vec![WireValue::Int64(2), s("a")]]);
}

#[tokio::test]
async fn join_on_reordered_aggregate_binds_correct_columns() {
    let rows = run(
        &[
            "+p[(\"a\", 1), (\"a\", 2), (\"b\", 3)]",
            "+label[(\"a\", \"alpha\"), (\"b\", \"beta\")]",
            "+t(count<P>, G) <- p(G, P)",
            "+named(L, N) <- t(N, G), label(G, L)",
        ],
        "?named(L, N)",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![s("alpha"), WireValue::Int64(2)],
            vec![s("beta"), WireValue::Int64(1)],
        ]
    );
}

#[tokio::test]
async fn interleaved_group_keys_and_aggregates_keep_head_order() {
    let rows = run(
        &[
            "+p[(\"a\", 1, 10), (\"a\", 1, 20), (\"b\", 2, 5)]",
            "+t(sum<V>, G, count<V>, H) <- p(G, H, V)",
        ],
        "?t(S, G, N, H)",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![
                WireValue::Int64(30),
                s("a"),
                WireValue::Int64(2),
                WireValue::Int64(1)
            ],
            vec![
                WireValue::Int64(5),
                s("b"),
                WireValue::Int64(1),
                WireValue::Int64(2)
            ],
        ]
    );
}
