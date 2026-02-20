//! Query performance benchmarks: scan, join, and recursive closure.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use inputlayer::{protocol::handler::Handler, Config};
use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn make_bench_handler() -> (Handler, TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    // Disable query timeout so benchmarks run freely
    config.storage.performance.query_timeout_ms = 0;
    let handler = Handler::from_config(config).expect("handler");
    (handler, tmp)
}

fn bench_simple_scan(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("simple_scan");
    for size in [100u32, 1_000, 10_000] {
        let (handler, _tmp) = make_bench_handler();

        // Pre-populate
        rt.block_on(async {
            let tuples: Vec<String> = (1..=size).map(|i| format!("({i},)")).collect();
            let program = format!("+node[{}]", tuples.join(", "));
            handler.query_program(None, program).await.unwrap();
        });

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| rt.block_on(handler.query_program(None, "?node(X)".to_string())));
        });
    }
    group.finish();
}

fn bench_two_way_join(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("two_way_join");
    for size in [100u32, 1_000] {
        let (handler, _tmp) = make_bench_handler();

        rt.block_on(async {
            let edges: Vec<String> = (1..size).map(|i| format!("({i}, {})", i + 1)).collect();
            handler
                .query_program(None, format!("+edge[{}]", edges.join(", ")))
                .await
                .unwrap();
            let nodes: Vec<String> = (1..=size).map(|i| format!("({i},)")).collect();
            handler
                .query_program(None, format!("+active[{}]", nodes.join(", ")))
                .await
                .unwrap();
        });

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                rt.block_on(handler.query_program(None, "?edge(X, Y), active(X)".to_string()))
            });
        });
    }
    group.finish();
}

fn bench_recursive_closure(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("recursive_closure");
    // Keep smaller to avoid long runtimes with recursive fixpoint
    for size in [50u32, 200] {
        let (handler, _tmp) = make_bench_handler();

        rt.block_on(async {
            // Linear chain: 1->2->3->...->size
            let edges: Vec<String> = (1..size).map(|i| format!("({i}, {})", i + 1)).collect();
            handler
                .query_program(None, format!("+edge[{}]", edges.join(", ")))
                .await
                .unwrap();
            // Add persistent transitive closure rules
            handler
                .query_program(None, "+reach(X, Y) <- edge(X, Y)".to_string())
                .await
                .unwrap();
            handler
                .query_program(None, "+reach(X, Z) <- reach(X, Y), edge(Y, Z)".to_string())
                .await
                .unwrap();
        });

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| rt.block_on(handler.query_program(None, "?reach(X, Y)".to_string())));
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(3));
    targets = bench_simple_scan, bench_two_way_join, bench_recursive_closure
}
criterion_main!(benches);
