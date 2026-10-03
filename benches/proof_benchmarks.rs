//! `.why` proof construction benchmarks: a tiny bound proof over a large
//! knowledge graph, many findings proven in one call, and recursive proofs.

// Benchmark setup aborts on failure; `unwrap` is the intended behavior.
#![allow(clippy::unwrap_used)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use inputlayer::{protocol::handler::Handler, Config};
use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Facts per insert statement while loading a fixture.
const INSERT_BATCH: u32 = 10_000;

fn make_bench_handler() -> (Handler, TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    // Disable query timeout so benchmarks run freely
    config.storage.performance.query_timeout_ms = 0;
    let handler = Handler::from_config(config).expect("handler");
    (handler, tmp)
}

/// Insert `rows` into `relation` in batches.
fn insert(rt: &Runtime, handler: &Handler, relation: &str, rows: Vec<String>) {
    for batch in rows.chunks(INSERT_BATCH as usize) {
        let program = format!("+{relation}[{}]", batch.join(", "));
        rt.block_on(handler.query_program(None, program)).unwrap();
    }
}

fn run(rt: &Runtime, handler: &Handler, program: &str) {
    rt.block_on(handler.query_program(None, program.to_string()))
        .unwrap();
}

/// One finding whose proof touches a handful of tuples of a large relation.
fn bench_tiny_proof_large_kg(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("why_tiny_proof_large_kg");
    for size in [10_000u32, 100_000] {
        let (handler, _tmp) = make_bench_handler();
        insert(
            &rt,
            &handler,
            "edge",
            (0..size).map(|i| format!("({i}, {})", i + 1)).collect(),
        );
        insert(
            &rt,
            &handler,
            "risk",
            (0..size).step_by(100).map(|i| format!("({i},)")).collect(),
        );
        run(&rt, &handler, "+flagged(X, Y) <- edge(X, Y), risk(Y)");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| run(&rt, &handler, ".why ?flagged(99, Y)"));
        });
    }
    group.finish();
}

/// Every finding of a query proven in one call, as the gateway does per turn.
fn bench_many_findings(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("why_many_findings");
    // Transactions per fixture; 1.9% of them are findings.
    for size in [5_000u32, 20_000] {
        let (handler, _tmp) = make_bench_handler();
        insert(
            &rt,
            &handler,
            "txn",
            (0..size)
                .map(|i| format!("({i}, {}, {})", i % 200, i % 1000))
                .collect(),
        );
        insert(
            &rt,
            &handler,
            "watched",
            (0..200).map(|a| format!("({a},)")).collect(),
        );
        run(
            &rt,
            &handler,
            "+finding(T, A) <- txn(T, A, Amt), watched(A), Amt > 980",
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| run(&rt, &handler, ".why ?finding(T, A)"));
        });
    }
    group.finish();
}

/// Recursive proofs over a chain: deep, shared sub-proofs. The rule recurses
/// on the right; proof search on a left-recursive closure is exponential in
/// path length and would measure that, not lookups.
fn bench_recursive(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("why_recursive");
    for size in [100u32, 400] {
        let (handler, _tmp) = make_bench_handler();
        insert(
            &rt,
            &handler,
            "link",
            (1..size).map(|i| format!("({i}, {})", i + 1)).collect(),
        );
        run(&rt, &handler, "+reach(X, Y) <- link(X, Y)");
        run(&rt, &handler, "+reach(X, Z) <- link(X, Y), reach(Y, Z)");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| run(&rt, &handler, ".why ?reach(1, Y)"));
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(2));
    targets = bench_tiny_proof_large_kg, bench_many_findings, bench_recursive
}
criterion_main!(benches);
