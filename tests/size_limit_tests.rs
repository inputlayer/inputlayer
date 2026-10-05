//! Request-supplied sizes on the server path.
//!
//! A count, limit or capacity in a program (`lsh_probes`'s `num_probes`,
//! `hnsw_nearest`'s `k` and `ef_search`, `top_k`'s K, an index's
//! `ef_construction`) once sized an allocation directly: a value such as
//! 3e17 made the allocator fail and the process abort, before any memory
//! limit could act. A literal over its bound is now refused with
//! `validation`, a size computed at run time only gets what the input can
//! produce, and the handler keeps serving either way.

// Test setup aborts on failure; `unwrap` is the intended behavior.
#![allow(clippy::unwrap_used)]

use inputlayer::protocol::wire::{ErrorCode, QueryResult, WireValue};
use inputlayer::protocol::Handler;
use inputlayer::size_limits::{
    MAX_EF_CONSTRUCTION, MAX_EF_SEARCH, MAX_HNSW_K, MAX_LSH_PROBES, MAX_TOP_K,
};
use inputlayer::{Config, StorageEngine};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config as ProptestConfig, RngAlgorithm, TestRng, TestRunner};
use tempfile::TempDir;

/// The size from the audit's reproduction.
const HUGE: i64 = 300_000_000_000_000_000;

fn handler() -> (Handler, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    (Handler::new(StorageEngine::new(config).unwrap()), temp)
}

async fn run(handler: &Handler, program: &str) -> Result<QueryResult, (ErrorCode, String)> {
    match handler.query_program(None, program.to_string()).await {
        Err(e) => Err((ErrorCode::Validation, e)),
        Ok(result) => match result.errors.first() {
            Some(e) => Err((e.code, e.message.clone())),
            None => Ok(result),
        },
    }
}

async fn exec(handler: &Handler, program: &str) -> QueryResult {
    run(handler, program)
        .await
        .unwrap_or_else(|e| panic!("'{program}' failed: {e:?}"))
}

/// The message of a program refused with `validation`.
async fn refused(handler: &Handler, program: &str) -> String {
    match run(handler, program).await {
        Err((ErrorCode::Validation, message)) => message,
        other => panic!("'{program}': expected a validation error, got {other:?}"),
    }
}

/// The handler still answers a trivial query.
async fn assert_serving(handler: &Handler) {
    let result = exec(handler, "?a(X)").await;
    assert_eq!(result.rows.len(), 1);
}

/// Lengths of the vector cells in the first column holding vectors.
fn vector_lengths(result: &QueryResult) -> Vec<usize> {
    result
        .rows
        .iter()
        .filter_map(|r| {
            r.values.iter().find_map(|v| match v {
                WireValue::Vector(v) => Some(v.len()),
                _ => None,
            })
        })
        .collect()
}

async fn setup(handler: &Handler) {
    exec(handler, "+a[(1)]").await;
}

/// Vectors of `n` rows, so the index walks its graph (over 1,024 entries).
async fn setup_index(handler: &Handler, n: usize) {
    exec(handler, "+docs(id: int, emb: vector)").await;
    let facts: Vec<String> = (0..n)
        .map(|i| {
            let x = i as f32;
            format!("({i}, [{:?}, {:?}, 1.0])", x.sin(), x.cos())
        })
        .collect();
    exec(handler, &format!("+docs[{}]", facts.join(", "))).await;
    exec(
        handler,
        ".index create doc_idx on docs(emb) metric euclidean",
    )
    .await;
}

#[tokio::test]
async fn lsh_probes_literal_over_bound_is_refused() {
    let (handler, _tmp) = handler();
    setup(&handler).await;
    let ok = exec(&handler, "?a(X), P = lsh_probes(0, 4, 4)").await;
    assert_eq!(vector_lengths(&ok), vec![4]);

    let message = refused(&handler, &format!("?a(X), P = lsh_probes(0, 4, {HUGE})")).await;
    assert!(
        message.contains(&format!(
            "lsh_probes: num_probes must be at most {MAX_LSH_PROBES}"
        )),
        "{message}"
    );
    let over = MAX_LSH_PROBES + 1;
    refused(&handler, &format!("?a(X), P = lsh_probes(0, 62, {over})")).await;
    let at = exec(
        &handler,
        &format!("?a(X), P = lsh_probes(0, 62, {MAX_LSH_PROBES})"),
    )
    .await;
    assert_eq!(vector_lengths(&at), vec![MAX_LSH_PROBES]);
    assert_serving(&handler).await;
}

#[tokio::test]
async fn lsh_multi_probe_literal_over_bound_is_refused() {
    let (handler, _tmp) = handler();
    setup(&handler).await;
    let message = refused(
        &handler,
        &format!("?a(X), P = lsh_multi_probe([1.0, 0.5], 0, 8, {HUGE})"),
    )
    .await;
    assert!(message.contains("lsh_multi_probe: num_probes"), "{message}");
    assert_serving(&handler).await;
}

/// A size bound from data is not a literal the parser sees: the function
/// returns every distinct probe instead of allocating for the request.
#[tokio::test]
async fn lsh_probes_runtime_size_gets_only_distinct_probes() {
    let (handler, _tmp) = handler();
    setup(&handler).await;
    exec(&handler, &format!("+np[({HUGE})]")).await;
    let result = exec(&handler, "?np(N), P = lsh_probes(0, 4, N)").await;
    // 4 bits: the bucket, 4 one-bit, 6 two-bit and 4 three-bit flips.
    assert_eq!(vector_lengths(&result), vec![15]);

    let result = exec(
        &handler,
        "?np(N), P = lsh_multi_probe([1.0, -0.5], 0, 3, N)",
    )
    .await;
    assert_eq!(vector_lengths(&result), vec![8]);
    assert_serving(&handler).await;
}

#[tokio::test]
async fn hnsw_nearest_k_and_ef_search_over_bound_are_refused() {
    let (handler, _tmp) = handler();
    setup(&handler).await;
    setup_index(&handler, 1100).await;
    let q = r#"hnsw_nearest("doc_idx", [0.0, 1.0, 1.0]"#;

    let message = refused(&handler, &format!("?{q}, {HUGE}, Id, D)")).await;
    assert!(
        message.contains(&format!("hnsw_nearest: k must be at most {MAX_HNSW_K}")),
        "{message}"
    );
    let message = refused(&handler, &format!("?{q}, 5, Id, D, {HUGE})")).await;
    assert!(
        message.contains(&format!(
            "hnsw_nearest: ef_search must be at most {MAX_EF_SEARCH}"
        )),
        "{message}"
    );
    refused(&handler, &format!("?{q}, 5, Id, D, 0)")).await;

    // At the bounds the graph search runs, beam and fetch clamped to the
    // index size.
    let result = exec(&handler, &format!("?{q}, 5, Id, D, {MAX_EF_SEARCH})")).await;
    assert_eq!(result.rows.len(), 5);
    let all = exec(&handler, &format!("?{q}, 1100, Id, D)")).await;
    let result = exec(&handler, &format!("?{q}, {MAX_HNSW_K}, Id, D)")).await;
    assert_eq!(result.rows, all.rows);
    assert_serving(&handler).await;
}

#[tokio::test]
async fn index_ef_parameters_over_bound_are_refused() {
    let (handler, _tmp) = handler();
    setup(&handler).await;
    exec(&handler, "+docs(id: int, emb: vector)").await;
    exec(&handler, "+docs[(1, [1.0, 0.0])]").await;

    let over = MAX_EF_CONSTRUCTION + 1;
    let message = refused(
        &handler,
        &format!(".index create i on docs(emb) ef_construction {over}"),
    )
    .await;
    assert!(
        message.contains(&format!(
            "ef_construction must be between 1 and {MAX_EF_CONSTRUCTION}"
        )),
        "{message}"
    );
    let over = MAX_EF_SEARCH + 1;
    let message = refused(
        &handler,
        &format!(".index create i on docs(emb) ef_search {over}"),
    )
    .await;
    assert!(
        message.contains(&format!("ef_search must be between 1 and {MAX_EF_SEARCH}")),
        "{message}"
    );
    exec(
        &handler,
        &format!(
            ".index create i on docs(emb) ef_construction {MAX_EF_CONSTRUCTION} ef_search {MAX_EF_SEARCH}"
        ),
    )
    .await;
    assert_serving(&handler).await;
}

#[tokio::test]
async fn top_k_over_bound_is_refused() {
    let (handler, _tmp) = handler();
    setup(&handler).await;
    exec(&handler, "+scores[(\"a\", 1), (\"b\", 2), (\"c\", 3)]").await;

    let over = MAX_TOP_K + 1;
    let message = refused(
        &handler,
        &format!("best(top_k<{over}, N, S:desc>) <- scores(N, S)\n?best(N, S)"),
    )
    .await;
    assert!(
        message.contains(&format!("top_k: k must be at most {MAX_TOP_K}")),
        "{message}"
    );
    let message = refused(
        &handler,
        &format!("best(top_k_threshold<{HUGE}, 0.5, N, S:desc>) <- scores(N, S)\n?best(N, S)"),
    )
    .await;
    assert!(
        message.contains("top_k_threshold: k must be at most"),
        "{message}"
    );

    // At the bound, the heap holds only what the group has.
    let result = exec(
        &handler,
        &format!("best(top_k<{MAX_TOP_K}, N, S:desc>) <- scores(N, S)\n?best(N, S)"),
    )
    .await;
    assert_eq!(result.rows.len(), 3);
    assert_serving(&handler).await;
}

/// `replace` and `concat` whose result would pass the computed-string bound
/// return null instead of allocating it.
#[tokio::test]
async fn string_builders_over_bound_return_null() {
    let (handler, _tmp) = handler();
    setup(&handler).await;
    let chunk = "x".repeat(60_000);
    exec(&handler, &format!("+s[(\"{chunk}\")]")).await;

    // 60,001 matches of the empty pattern, each replaced by 60,000 bytes.
    let result = exec(&handler, "?s(S), R = replace(S, \"\", S)").await;
    assert_eq!(result.rows.len(), 1);
    assert!(
        result.rows[0].values.contains(&WireValue::Null),
        "{:?}",
        result.rows[0]
            .values
            .iter()
            .map(std::mem::discriminant)
            .collect::<Vec<_>>()
    );
    // Nested six deep, concat would build 3^6 copies (about 44 MB).
    let nested = (0..6).fold("S".to_string(), |inner, _| {
        format!("concat({inner}, {inner}, {inner})")
    });
    let result = exec(&handler, &format!("?s(S), R = {nested}")).await;
    assert!(result.rows[0].values.contains(&WireValue::Null));

    let result = exec(&handler, "?s(S), R = replace(S, \"x\", \"yy\")").await;
    assert!(!result.rows[0].values.contains(&WireValue::Null));
    assert_serving(&handler).await;
}

/// The largest client timeout sets a deadline (or none, where `Instant`
/// cannot hold it) without panicking.
#[tokio::test]
async fn huge_client_timeout_does_not_panic() {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.query_timeout_ms = 0;
    let handler = Handler::new(StorageEngine::new(config).unwrap());
    let control = handler.request_control(Some(u64::MAX));
    assert!(!control.is_stopped());
}

/// Fuzz seed: sizes from the boundaries and from a fixed-seed generator, in
/// every size position, through parse and evaluation. Each program is either
/// refused with `validation` or answered; none may take the handler down.
#[tokio::test]
async fn fuzz_size_parameters_never_take_the_handler_down() {
    let (handler, _tmp) = handler();
    setup(&handler).await;
    setup_index(&handler, 1100).await;
    exec(&handler, "+scores[(\"a\", 1), (\"b\", 2), (\"c\", 3)]").await;

    let templates: &[&str] = &[
        "?a(X), P = lsh_probes(0, 62, {n})",
        "?a(X), P = lsh_probes(0, {n}, {n})",
        "?a(X), P = lsh_multi_probe([1.0, 0.5], {n}, {n}, {n})",
        "?a(X), P = lsh_bucket([1.0, 0.5], 0, {n})",
        "?a(X), P = substr(\"abcdef\", {n}, {n})",
        r#"?hnsw_nearest("doc_idx", [0.0, 1.0, 1.0], {n}, Id, D)"#,
        r#"?hnsw_nearest("doc_idx", [0.0, 1.0, 1.0], 3, Id, D, {n})"#,
        "best(top_k<{n}, N, S:desc>) <- scores(N, S)\n?best(N, S)",
        "best(top_k_threshold<{n}, 1.5, N, S:asc>) <- scores(N, S)\n?best(N, S)",
        "?scores(N, S), limit({n})",
        "?scores(N, S), limit({n}, {n})",
    ];

    let mut sizes: Vec<u64> = vec![
        0,
        1,
        2,
        61,
        62,
        63,
        1023,
        1024,
        1025,
        MAX_HNSW_K as u64,
        MAX_HNSW_K as u64 + 1,
        MAX_LSH_PROBES as u64,
        MAX_LSH_PROBES as u64 + 1,
        MAX_TOP_K as u64,
        MAX_TOP_K as u64 + 1,
        u64::from(u32::MAX),
        HUGE as u64,
        i64::MAX as u64,
        i64::MAX as u64 + 1,
        u64::MAX,
    ];
    let mut runner = TestRunner::new_with_rng(
        ProptestConfig::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &[7; 32]),
    );
    let strategy = prop_oneof![0u64..100_000, any::<u64>()];
    for _ in 0..40 {
        sizes.push(strategy.new_tree(&mut runner).unwrap().current());
    }

    for template in templates {
        for &n in &sizes {
            let program = template.replace("{n}", &n.to_string());
            match run(&handler, &program).await {
                Ok(_) | Err((ErrorCode::Validation, _)) => {}
                Err(other) => panic!("'{program}': unexpected {other:?}"),
            }
        }
    }
    assert_serving(&handler).await;
}
