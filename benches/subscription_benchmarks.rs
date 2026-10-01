//! Standing-query re-evaluation latency.
//!
//! A bound subscription (`?two_hop(1, Z)`, Magic Sets) on a KG of 1K-10K
//! facts; each iteration commits one insert that changes the result, then
//! times the refresh that produces the delta.

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use inputlayer::protocol::subscription::{ReevaluatingQuery, StandingQuery};
use inputlayer::protocol::Handler;
use inputlayer::Config;
use rand::prelude::*;
use tokio::runtime::Runtime;

const KG: &str = "bench";

fn make_handler(edges: u32) -> (Arc<Handler>, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    config.storage.performance.query_timeout_ms = 0;
    config.storage.performance.max_insert_tuples = 0;
    config.storage.performance.max_result_rows = 0;
    config.storage.performance.max_query_size_bytes = 0;
    config.storage.persist.enabled = false;
    let handler = Arc::new(Handler::from_config(config).expect("handler"));
    handler
        .get_storage()
        .create_knowledge_graph(KG)
        .expect("create kg");

    let nodes = edges / 4;
    let mut rng = StdRng::seed_from_u64(7);
    let tuples: Vec<String> = (0..edges)
        .map(|_| {
            let (src, dst) = (rng.gen_range(1..=nodes), rng.gen_range(1..=nodes));
            format!("({src}, {dst})")
        })
        .collect();
    let rt = Runtime::new().expect("runtime");
    for program in [
        format!("+edge[{}]", tuples.join(", ")),
        "+two_hop(X, Z) <- edge(X, Y), edge(Y, Z)".to_string(),
    ] {
        rt.block_on(handler.execute_program(None, Some(KG.to_string()), program, None))
            .expect("setup");
    }
    (handler, tmp)
}

fn bench_refresh_after_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription_refresh_after_insert");
    let rt = Runtime::new().expect("runtime");
    for edges in [1_000u32, 10_000] {
        let (handler, _tmp) = make_handler(edges);
        let mut view =
            ReevaluatingQuery::new(Arc::clone(&handler), KG, "?two_hop(1, Z)", None).expect("view");
        rt.block_on(view.refresh()).expect("initial refresh");
        let mut next_node = 1_000_000u64;
        group.bench_with_input(BenchmarkId::from_parameter(edges), &edges, |b, _| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    // edge(1, n) + edge(n, n+1) adds two_hop(1, n+1).
                    let insert =
                        format!("+edge[(1, {next_node}), ({next_node}, {})]", next_node + 1);
                    next_node += 2;
                    rt.block_on(handler.execute_program(None, Some(KG.to_string()), insert, None))
                        .expect("insert");
                    let start = Instant::now();
                    let refresh = rt.block_on(view.refresh()).expect("refresh");
                    total += start.elapsed();
                    assert!(!refresh.inserted.is_empty());
                }
                total
            });
        });
    }
    group.finish();
}

criterion_group! {
    name = subscriptions;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(2))
        .sample_size(30);
    targets = bench_refresh_after_insert
}
criterion_main!(subscriptions);
