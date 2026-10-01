//! HNSW index benchmarks: 10K x 384-dim vectors, cosine.
//!
//! - `hnsw_index/*`: the index alone (build, k=10 search, single insert)
//! - `hnsw_server/*`: the same through `Handler` (`hnsw_nearest` query,
//!   single-row insert into an indexed relation)

// Benchmark setup aborts on failure; `unwrap` is the intended behavior.
#![allow(clippy::unwrap_used)]

use criterion::{criterion_group, criterion_main, Criterion};
use inputlayer::{protocol::handler::Handler, Config, HnswConfig, HnswIndex};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::Duration;
use tokio::runtime::Runtime;

const COUNT: usize = 10_000;
const DIM: usize = 384;
/// Rows per insert program; keeps each program under the 1 MiB query limit.
const BATCH: usize = 200;

fn random_vectors(n: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|_| (0..DIM).map(|_| rng.gen_range(-1.0f32..1.0)).collect())
        .collect()
}

fn literal(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| format!("{x:.5}")).collect();
    format!("[{}]", parts.join(", "))
}

fn bench_index(c: &mut Criterion) {
    let vectors = random_vectors(COUNT, 42);
    let rows: Vec<(i64, Vec<f32>)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (i as i64, v.clone()))
        .collect();
    let mut group = c.benchmark_group("hnsw_index");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    group.bench_function("build_10000x384", |b| {
        b.iter(|| HnswIndex::build(HnswConfig::default(), rows.clone()).unwrap());
    });

    let index = HnswIndex::build(HnswConfig::default(), rows).unwrap();
    let queries = random_vectors(100, 7);
    let mut qi = 0;
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("search_k10_10000x384", |b| {
        b.iter(|| {
            qi = (qi + 1) % queries.len();
            index.search(&queries[qi], 10, None, index.epoch()).unwrap()
        });
    });

    let inserts = random_vectors(2_000, 9);
    let mut next = COUNT as i64;
    group.bench_function("insert_one_10000x384", |b| {
        b.iter(|| {
            let v = inserts[next as usize % inserts.len()].clone();
            index.apply(&[(next, v)], &[]).unwrap();
            next += 1;
        });
    });
    group.finish();
}

fn bench_server(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    config.storage.performance.query_timeout_ms = 0;
    let handler = Handler::from_config(config).unwrap();
    let vectors = random_vectors(COUNT, 42);
    rt.block_on(async {
        handler
            .query_program(None, "+vecs(id: int, emb: vector)".to_string())
            .await
            .unwrap();
        for (chunk_idx, chunk) in vectors.chunks(BATCH).enumerate() {
            let facts: Vec<String> = chunk
                .iter()
                .enumerate()
                .map(|(i, v)| format!("({}, {})", chunk_idx * BATCH + i, literal(v)))
                .collect();
            handler
                .query_program(None, format!("+vecs[{}]", facts.join(", ")))
                .await
                .unwrap();
        }
        handler
            .query_program(None, ".index create v_idx on vecs(emb)".to_string())
            .await
            .unwrap();
    });

    let mut group = c.benchmark_group("hnsw_server");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(10));
    let query = format!(
        r#"?hnsw_nearest("v_idx", {}, 10, Id, Dist)"#,
        literal(&random_vectors(1, 7)[0])
    );
    group.bench_function("query_k10_10000x384", |b| {
        b.iter(|| {
            rt.block_on(handler.query_program(None, query.clone()))
                .unwrap()
        });
    });

    let inserts = random_vectors(500, 9);
    let mut next = COUNT;
    group.bench_function("insert_one_10000x384", |b| {
        b.iter(|| {
            let program = format!(
                "+vecs[({next}, {})]",
                literal(&inserts[next % inserts.len()])
            );
            next += 1;
            rt.block_on(handler.query_program(None, program)).unwrap()
        });
    });
    group.finish();
}

criterion_group!(vector_index, bench_index, bench_server);
criterion_main!(vector_index);
