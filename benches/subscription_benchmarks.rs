//! Standing-query re-evaluation latency and fan-out.
//!
//! A bound subscription (`?two_hop(1, Z)`, Magic Sets) on a KG of 1K-10K
//! facts; each iteration commits one insert that changes the result, then
//! times the refresh that produces the delta.
//!
//! Fan-out: 1, 64 or 100 subscribers on one KG of 10K facts, all on the same
//! query (one shared view) or each on its own; each iteration commits one
//! insert and times from its acknowledgement until every subscriber has its
//! delta. Evaluations per commit and the process RSS are printed per case.
//!
//! Groups: the refresh of a subscription group of 5 queries on the 10K-fact
//! KG (four bound `two_hop` queries and `?marker(X)`), after a commit that
//! changes only `marker` (`one_changed`: the four `two_hop` queries are not
//! re-run) or every member (`all_changed`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use inputlayer::protocol::subscription::{
    ConnectionSubscriptions, GroupQuery, ReevaluatingQuery, StandingQuery,
};
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
            ReevaluatingQuery::new(Arc::clone(&handler), KG, "?two_hop(1, Z)").expect("view");
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
                    assert!(!refresh.queries[0].inserted.is_empty());
                }
                total
            });
        });
    }
    group.finish();
}

/// Resident set size of this process, from `/proc/self/status`.
fn rss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

/// `subscribers` subscriptions spread over connections of at most 64 each.
async fn subscribe_all(
    handler: &Arc<Handler>,
    subscribers: usize,
    distinct: bool,
) -> Vec<(ConnectionSubscriptions, usize)> {
    let mut connections = Vec::new();
    for chunk_start in (0..subscribers).step_by(64) {
        let count = (subscribers - chunk_start).min(64);
        let mut subscriptions = ConnectionSubscriptions::new(Arc::clone(handler), None);
        for i in chunk_start..chunk_start + count {
            let query = if distinct {
                format!("?two_hop(1, Z), Z > -{i}")
            } else {
                "?two_hop(1, Z)".to_string()
            };
            let opening = subscriptions
                .begin_subscribe(KG, &format!("s{i}"), &query)
                .expect("subscribe");
            subscriptions
                .finish_subscribe(opening.run().await, |_| true)
                .expect("subscribe");
        }
        connections.push((subscriptions, count));
    }
    connections
}

fn bench_fanout_after_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription_fanout_after_insert");
    let rt = Runtime::new().expect("runtime");
    for distinct in [false, true] {
        for subscribers in [1usize, 64, 100] {
            let (handler, _tmp) = make_handler(10_000);
            let mut connections = rt.block_on(subscribe_all(&handler, subscribers, distinct));
            let rss_after_subscribe = rss_kb();
            let evaluations_before = handler.subscription_metrics().evaluations();
            let mut commits = 0u64;
            let mut next_node = 1_000_000u64;
            let name = if distinct { "distinct" } else { "identical" };
            group.bench_with_input(BenchmarkId::new(name, subscribers), &subscribers, |b, _| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let insert =
                            format!("+edge[(1, {next_node}), ({next_node}, {})]", next_node + 1);
                        next_node += 2;
                        commits += 1;
                        total += rt.block_on(async {
                            handler
                                .execute_program(None, Some(KG.to_string()), insert, None)
                                .await
                                .expect("insert");
                            let start = Instant::now();
                            for (subscriptions, count) in &mut connections {
                                let mut delivered = 0;
                                while delivered < *count {
                                    let subscriber = subscriptions.next_delivery().await;
                                    if subscriptions.deliver(subscriber, |_| true).is_some() {
                                        delivered += 1;
                                    }
                                }
                            }
                            start.elapsed()
                        });
                    }
                    total
                });
            });
            let evaluations = handler.subscription_metrics().evaluations() - evaluations_before;
            eprintln!(
                "fanout {name}/{subscribers}: {:.2} evaluations per commit, {} views, \
                 RSS after subscribe {} kB, now {} kB",
                evaluations as f64 / commits.max(1) as f64,
                handler.subscription_metrics().views(),
                rss_after_subscribe.unwrap_or(0),
                rss_kb().unwrap_or(0),
            );
        }
    }
    group.finish();
}

fn bench_group_refresh_after_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription_group_refresh_after_insert");
    let rt = Runtime::new().expect("runtime");
    let (handler, _tmp) = make_handler(10_000);
    let members = [
        "?two_hop(1, Z)",
        "?two_hop(2, Z)",
        "?two_hop(3, Z)",
        "?two_hop(4, Z)",
        "?marker(X)",
    ];
    for all in [false, true] {
        let mut view = GroupQuery::new(Arc::clone(&handler), KG, &members).expect("view");
        rt.block_on(view.refresh()).expect("initial refresh");
        let mut next_node = if all { 3_000_000u64 } else { 2_000_000u64 };
        let name = if all { "all_changed" } else { "one_changed" };
        group.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    // Each `two_hop(k, Z)` gains `m` through `n`.
                    let insert = if all {
                        format!(
                            "+edge[(1, {n}), (2, {n}), (3, {n}), (4, {n}), ({n}, {m})]\n\
                             +marker({n})",
                            n = next_node,
                            m = next_node + 1
                        )
                    } else {
                        format!("+marker({next_node})")
                    };
                    next_node += 2;
                    rt.block_on(handler.execute_program(None, Some(KG.to_string()), insert, None))
                        .expect("insert");
                    let start = Instant::now();
                    let refresh = rt.block_on(view.refresh()).expect("refresh");
                    total += start.elapsed();
                    let changed = refresh.queries.iter().filter(|q| !q.is_unchanged());
                    assert_eq!(changed.count(), if all { 5 } else { 1 });
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
    targets = bench_refresh_after_insert, bench_fanout_after_insert, bench_group_refresh_after_insert
}
criterion_main!(subscriptions);
