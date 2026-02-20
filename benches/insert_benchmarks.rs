//! Insert performance benchmarks: single, small batch, and large batch.

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

fn bench_single_insert(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (handler, _tmp) = make_bench_handler();

    let mut counter = 0u64;
    c.bench_function("insert_single", |b| {
        b.iter(|| {
            counter += 1;
            let program = format!("+point[({counter},)]");
            rt.block_on(handler.query_program(None, program))
        });
    });
}

fn bench_batch_insert(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("batch_insert");
    let base_offset = 1_000_000u64; // avoid key collisions across sizes
    for (idx, &size) in [100u64, 1_000, 10_000].iter().enumerate() {
        let (handler, _tmp) = make_bench_handler();
        let offset = base_offset * (idx as u64 + 1);
        let mut call_count = 0u64;

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &sz| {
            b.iter(|| {
                call_count += 1;
                let start = offset + call_count * sz;
                let tuples: Vec<String> = (start..start + sz).map(|i| format!("({i},)")).collect();
                let program = format!("+batch[{}]", tuples.join(", "));
                rt.block_on(handler.query_program(None, program))
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
    targets = bench_single_insert, bench_batch_insert
}
criterion_main!(benches);
