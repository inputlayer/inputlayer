//! Aggregation performance benchmarks: COUNT, SUM, MIN, MAX over varying dataset sizes.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use inputlayer::{protocol::handler::Handler, Config};
use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn make_bench_handler() -> (Handler, TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    config.storage.performance.query_timeout_ms = 0;
    let handler = Handler::from_config(config).expect("handler");
    (handler, tmp)
}

fn bench_count_aggregation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("count_agg");
    for size in [1_000u32, 10_000] {
        let (handler, _tmp) = make_bench_handler();

        rt.block_on(async {
            // 10 groups, each with size/10 rows
            let tuples: Vec<String> = (1..=size).map(|i| format!("({}, {})", i % 10, i)).collect();
            handler
                .query_program(None, format!("+data[{}]", tuples.join(", ")))
                .await
                .unwrap();
            handler
                .query_program(
                    None,
                    "+data_count(Group, count<Val>) <- data(Group, Val)".to_string(),
                )
                .await
                .unwrap();
        });

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| rt.block_on(handler.query_program(None, "?data_count(G, C)".to_string())));
        });
    }
    group.finish();
}

fn bench_sum_aggregation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("sum_agg");
    for size in [1_000u32, 10_000] {
        let (handler, _tmp) = make_bench_handler();

        rt.block_on(async {
            let tuples: Vec<String> = (1..=size).map(|i| format!("({}, {})", i % 10, i)).collect();
            handler
                .query_program(None, format!("+sales[{}]", tuples.join(", ")))
                .await
                .unwrap();
            handler
                .query_program(
                    None,
                    "+sales_sum(Group, sum<Val>) <- sales(Group, Val)".to_string(),
                )
                .await
                .unwrap();
        });

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| rt.block_on(handler.query_program(None, "?sales_sum(G, S)".to_string())));
        });
    }
    group.finish();
}

fn bench_min_max_aggregation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("min_max_agg");
    for size in [1_000u32, 10_000] {
        let (handler, _tmp) = make_bench_handler();

        rt.block_on(async {
            let tuples: Vec<String> = (1..=size).map(|i| format!("({}, {})", i % 10, i)).collect();
            handler
                .query_program(None, format!("+scores[{}]", tuples.join(", ")))
                .await
                .unwrap();
            handler
                .query_program(
                    None,
                    "+scores_min(Group, min<Val>) <- scores(Group, Val)".to_string(),
                )
                .await
                .unwrap();
            handler
                .query_program(
                    None,
                    "+scores_max(Group, max<Val>) <- scores(Group, Val)".to_string(),
                )
                .await
                .unwrap();
        });

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    handler
                        .query_program(None, "?scores_min(G, V)".to_string())
                        .await
                        .unwrap();
                    handler
                        .query_program(None, "?scores_max(G, V)".to_string())
                        .await
                        .unwrap();
                })
            });
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(3));
    targets = bench_count_aggregation, bench_sum_aggregation, bench_min_max_aggregation
}
criterion_main!(benches);
