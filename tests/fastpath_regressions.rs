//! Transitive-closure fast paths must match the general evaluator exactly.

use inputlayer::protocol::Handler;
use inputlayer::{Config, OptimizationConfig};
use tempfile::TempDir;

fn handler(magic: bool, workers: usize) -> (Handler, TempDir) {
    let temp = TempDir::new().expect("temp dir");
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.num_threads = workers;
    config.optimization = OptimizationConfig {
        enable_magic_sets: magic,
        ..OptimizationConfig::default()
    };
    let storage = inputlayer::StorageEngine::new(config).expect("storage engine");
    (Handler::new(storage), temp)
}

async fn run(handler: &Handler, program: &[&str], query: &str) -> Vec<String> {
    for stmt in program {
        handler
            .query_program(None, (*stmt).to_string())
            .await
            .unwrap_or_else(|e| panic!("'{stmt}': {e}"));
    }
    let result = handler
        .query_program(None, query.to_string())
        .await
        .unwrap_or_else(|e| panic!("'{query}': {e}"));
    let mut rows: Vec<String> = result
        .rows
        .iter()
        .map(|r| format!("{:?}", r.values))
        .collect();
    rows.sort();
    rows
}

/// Runs `query` with magic sets on and off; asserts both equal `expected`.
async fn assert_rows(program: &[&str], query: &str, expected: &[&str]) {
    let mut expected: Vec<String> = expected.iter().map(|s| (*s).to_string()).collect();
    expected.sort();
    for magic in [true, false] {
        let (h, _t) = handler(magic, 1);
        let rows = run(&h, program, query).await;
        assert_eq!(rows, expected, "magic_sets={magic}, query {query}");
    }
}

fn row(values: &[i64]) -> String {
    let values: Vec<inputlayer::protocol::WireValue> = values
        .iter()
        .map(|v| inputlayer::protocol::WireValue::Int64(*v))
        .collect();
    format!("{values:?}")
}

const EDGES: &str = "+e[(1, 2), (2, 3), (3, 4), (1, 5)]";

#[tokio::test]
async fn bound_tc_keeps_recursive_filter() {
    let program = [
        EDGES,
        "+r(X, Y) <- e(X, Y)",
        "+r(X, Z) <- r(X, Y), e(Y, Z), Z != 3",
    ];
    let expected = [row(&[1, 2]), row(&[1, 5])];
    let expected: Vec<&str> = expected.iter().map(String::as_str).collect();
    assert_rows(&program, "?r(1, Y)", &expected).await;
}

#[tokio::test]
async fn bound_tc_keeps_base_filter() {
    let program = [
        EDGES,
        "+r(X, Y) <- e(X, Y), Y != 2",
        "+r(X, Z) <- r(X, Y), e(Y, Z)",
    ];
    let expected = [row(&[1, 5])];
    let expected: Vec<&str> = expected.iter().map(String::as_str).collect();
    assert_rows(&program, "?r(1, Y)", &expected).await;
}

#[tokio::test]
async fn bound_tc_plain_matches() {
    let program = [EDGES, "+r(X, Y) <- e(X, Y)", "+r(X, Z) <- r(X, Y), e(Y, Z)"];
    let expected = [row(&[1, 2]), row(&[1, 3]), row(&[1, 4]), row(&[1, 5])];
    let expected: Vec<&str> = expected.iter().map(String::as_str).collect();
    assert_rows(&program, "?r(1, Y)", &expected).await;
}

#[tokio::test]
async fn tc_with_reversed_base() {
    let program = [
        "+e[(1, 2), (2, 3)]",
        "+r(Y, X) <- e(X, Y)",
        "+r(X, Z) <- e(X, Y), r(Y, Z)",
    ];
    let expected = [
        row(&[1, 1]),
        row(&[1, 2]),
        row(&[2, 1]),
        row(&[2, 2]),
        row(&[3, 2]),
    ];
    let expected: Vec<&str> = expected.iter().map(String::as_str).collect();
    assert_rows(&program, "?r(X, Y)", &expected).await;
}

#[tokio::test]
async fn tc_with_swapped_recursive_head() {
    let program = [
        "+e[(1, 2), (2, 3)]",
        "+r(X, Y) <- e(X, Y)",
        "+r(Z, X) <- e(X, Y), r(Y, Z)",
    ];
    let expected = [row(&[1, 2]), row(&[2, 3]), row(&[3, 1])];
    let expected: Vec<&str> = expected.iter().map(String::as_str).collect();
    assert_rows(&program, "?r(X, Y)", &expected).await;
}

#[tokio::test]
async fn tc_plain_matches() {
    let program = [
        "+e[(1, 2), (2, 3)]",
        "+r(X, Y) <- e(X, Y)",
        "+r(X, Z) <- e(X, Y), r(Y, Z)",
    ];
    let expected = [row(&[1, 2]), row(&[1, 3]), row(&[2, 3])];
    let expected: Vec<&str> = expected.iter().map(String::as_str).collect();
    assert_rows(&program, "?r(X, Y)", &expected).await;
}
