//! Production benchmark suite for InputLayer.
//!
//! Tests at realistic scales (1K–100K+ tuples) across 6 groups:
//! 1. Graph Reachability — Transitive Closure (compare: Soufflé, Neo4j)
//! 2. Incremental Update Propagation (compare: Materialize)
//! 3. Multi-hop Deduction — README Example (unique to InputLayer)
//! 4. Analytical 3-Way Join (compare: DuckDB)
//! 5. HNSW Vector Search (compare: Qdrant, Pinecone)
//! 6. Persistence Round-Trip (WAL + Recovery)

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use inputlayer::{protocol::handler::Handler, Config, DurabilityMode};
use rand::prelude::*;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Handler with all safety limits disabled for benchmarking (no persistence).
fn make_bench_handler() -> (Handler, TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    config.storage.performance.query_timeout_ms = 0;
    config.storage.performance.max_insert_tuples = 0;
    config.storage.performance.max_result_rows = 0;
    config.storage.performance.max_query_size_bytes = 0;
    config.storage.persist.enabled = false;
    let handler = Handler::from_config(config).expect("handler");
    (handler, tmp)
}

/// Handler with persistence enabled (Immediate durability).
fn make_persist_handler() -> (Handler, TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    config.storage.performance.query_timeout_ms = 0;
    config.storage.performance.max_insert_tuples = 0;
    config.storage.performance.max_result_rows = 0;
    config.storage.performance.max_query_size_bytes = 0;
    config.storage.persist.enabled = true;
    config.storage.persist.durability_mode = DurabilityMode::Immediate;
    let handler = Handler::from_config(config).expect("handler");
    (handler, tmp)
}

// ---------------------------------------------------------------------------
// Data generators
// ---------------------------------------------------------------------------

/// Generate random Erdős–Rényi graph edges as a batch insert string.
fn generate_random_graph(nodes: u32, edges: u32, seed: u64) -> String {
    let mut rng = StdRng::seed_from_u64(seed);
    let tuples: Vec<String> = (0..edges)
        .map(|_| {
            let src = rng.gen_range(1..=nodes);
            let dst = rng.gen_range(1..=nodes);
            format!("({src}, {dst})")
        })
        .collect();
    format!("+edge[{}]", tuples.join(", "))
}

/// Generate additional random edges (for incremental benchmarks).
fn generate_incremental_edges(nodes: u32, count: u32, seed: u64) -> String {
    let mut rng = StdRng::seed_from_u64(seed);
    let tuples: Vec<String> = (0..count)
        .map(|_| {
            let src = rng.gen_range(1..=nodes);
            let dst = rng.gen_range(1..=nodes);
            format!("({src}, {dst})")
        })
        .collect();
    format!("+edge[{}]", tuples.join(", "))
}

/// Generate knowledge graph data for multi-hop deduction benchmark.
/// Returns (part_of inserts, located_in inserts, memory+about inserts).
fn generate_knowledge_graph(
    memories: usize,
    links: usize,
    cities: usize,
) -> (String, String, String) {
    let mut rng = StdRng::seed_from_u64(42);
    let num_categories = links / 4; // ~4 part_of links per category on average
    let num_regions = cities / 5; // ~5 cities per region

    // part_of: concept -> category hierarchy
    let part_of_tuples: Vec<String> = (0..links)
        .map(|i| {
            let concept = format!("concept_{i}");
            let cat = rng.gen_range(0..num_categories.max(1));
            format!("(\"{concept}\", \"category_{cat}\")")
        })
        .collect();

    // located_in: city -> region
    let located_in_tuples: Vec<String> = (0..cities)
        .map(|i| {
            let region = i % num_regions.max(1);
            format!("(\"city_{i}\", \"region_{region}\")")
        })
        .collect();

    // memories + about links
    let mut memory_stmts = Vec::new();
    let memory_tuples: Vec<String> = (0..memories)
        .map(|i| format!("(\"m{i}\", \"text_{i}\")"))
        .collect();
    memory_stmts.push(format!("+memory[{}]", memory_tuples.join(", ")));

    let about_tuples: Vec<String> = (0..memories)
        .map(|i| {
            let concept = rng.gen_range(0..links.max(1));
            format!("(\"m{i}\", \"concept_{concept}\")")
        })
        .collect();
    memory_stmts.push(format!("+about[{}]", about_tuples.join(", ")));

    (
        format!("+part_of[{}]", part_of_tuples.join(", ")),
        format!("+located_in[{}]", located_in_tuples.join(", ")),
        memory_stmts.join("\n"),
    )
}

/// Generate synthetic orders/products/customers data.
/// Returns (customer inserts, product inserts, order inserts).
fn generate_orders_data(customers: u32, products: u32, orders: u32) -> (String, String, String) {
    let mut rng = StdRng::seed_from_u64(99);
    let num_regions = 10u32;

    let cust_tuples: Vec<String> = (1..=customers)
        .map(|i| {
            let region = (i % num_regions) + 1;
            format!("({i}, \"region_{region}\")")
        })
        .collect();

    let prod_tuples: Vec<String> = (1..=products)
        .map(|i| {
            let cat = (i % 5) + 1;
            let price = rng.gen_range(1.0..100.0_f64);
            format!("({i}, \"category_{cat}\", {price:.2})")
        })
        .collect();

    let order_tuples: Vec<String> = (1..=orders)
        .map(|i| {
            let cust = rng.gen_range(1..=customers);
            let prod = rng.gen_range(1..=products);
            let qty = rng.gen_range(1..=20_u32);
            format!("({i}, {cust}, {prod}, {qty})")
        })
        .collect();

    (
        format!("+customer[{}]", cust_tuples.join(", ")),
        format!("+product[{}]", prod_tuples.join(", ")),
        format!("+orders[{}]", order_tuples.join(", ")),
    )
}

/// Generate random normalized 128-dim float vectors.
fn generate_random_vectors(count: usize, dims: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .map(|_| {
            let raw: Vec<f32> = (0..dims).map(|_| rng.gen_range(-1.0..1.0_f32)).collect();
            let norm: f32 = raw.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                raw.iter().map(|x| x / norm).collect()
            } else {
                // Fallback: unit vector along first dim
                let mut v = vec![0.0; dims];
                v[0] = 1.0;
                v
            }
        })
        .collect()
}

/// Format a vector as an InputLayer literal: [0.1, 0.2, ...]
fn format_vector(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| format!("{x:.6}")).collect();
    format!("[{}]", parts.join(", "))
}

// ---------------------------------------------------------------------------
// 1. Graph Reachability — Transitive Closure
// ---------------------------------------------------------------------------

/// Benchmark graph sizes. Set `BENCH_MAX_NODES=1000` to cap the largest graph
/// (useful in CI where memory is limited). Default: all sizes up to 2000 nodes.
fn bench_graph_sizes() -> Vec<(u32, u32)> {
    let max_nodes: u32 = std::env::var("BENCH_MAX_NODES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000);
    vec![(500, 1_000), (1_000, 2_000), (2_000, 4_000)]
        .into_iter()
        .filter(|(n, _)| *n <= max_nodes)
        .collect()
}

fn bench_transitive_closure(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("transitive_closure");
    group.sample_size(10);

    let sizes = bench_graph_sizes();

    for (nodes, edges) in sizes {
        let (handler, _tmp) = make_bench_handler();
        let label = format!("{nodes}n_{edges}e");

        // Setup: insert edges + define transitive closure rules
        rt.block_on(async {
            let insert = generate_random_graph(nodes, edges, 42);
            handler.query_program(None, insert).await.unwrap();
            handler
                .query_program(None, "+reach(X, Y) <- edge(X, Y)".to_string())
                .await
                .unwrap();
            handler
                .query_program(None, "+reach(X, Z) <- reach(X, Y), edge(Y, Z)".to_string())
                .await
                .unwrap();
        });

        group.bench_with_input(BenchmarkId::new("materialize", &label), &(), |b, _| {
            b.iter(|| rt.block_on(handler.query_program(None, "?reach(X, Y)".to_string())));
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 1b. Magic Sets — Bound Recursive Query vs Full TC
// ---------------------------------------------------------------------------

fn bench_magic_sets(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("magic_sets");
    group.sample_size(10);

    let sizes = bench_graph_sizes();

    for (nodes, edges) in sizes {
        let label = format!("{nodes}n_{edges}e");

        // Setup: fresh handler with edges + TC rules
        let (handler, _tmp) = make_bench_handler();
        rt.block_on(async {
            let insert = generate_random_graph(nodes, edges, 42);
            handler.query_program(None, insert).await.unwrap();
            handler
                .query_program(None, "+reach(X, Y) <- edge(X, Y)".to_string())
                .await
                .unwrap();
            handler
                .query_program(None, "+reach(X, Z) <- reach(X, Y), edge(Y, Z)".to_string())
                .await
                .unwrap();
        });

        // Benchmark: Full TC (unbound) — baseline
        group.bench_with_input(BenchmarkId::new("full_tc", &label), &(), |b, _| {
            b.iter(|| rt.block_on(handler.query_program(None, "?reach(X, Y)".to_string())));
        });

        // Benchmark: Bound query ?reach(1, Y) — should be much faster with magic sets
        group.bench_with_input(BenchmarkId::new("bound_from_1", &label), &(), |b, _| {
            b.iter(|| rt.block_on(handler.query_program(None, "?reach(1, Y)".to_string())));
        });

        // Benchmark: Point query ?reach(1, 42)
        group.bench_with_input(BenchmarkId::new("point_1_42", &label), &(), |b, _| {
            b.iter(|| rt.block_on(handler.query_program(None, "?reach(1, 42)".to_string())));
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Incremental Update Propagation
// ---------------------------------------------------------------------------

fn bench_incremental_updates(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("incremental_updates");
    group.sample_size(10);

    // (base_edges, nodes, incremental_count) — sparse graphs for tractable TC
    let configs: Vec<(u32, u32, u32)> =
        vec![(1_000, 500, 10), (2_000, 1_000, 10), (2_000, 1_000, 100)];

    for (base_edges, nodes, inc_count) in configs {
        let label = format!("{base_edges}base_{inc_count}inc");

        // We need a fresh handler per iteration for the incremental insert,
        // so we use iter_custom to control timing precisely.
        group.bench_with_input(BenchmarkId::new("requery", &label), &(), |b, _| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for i in 0..iters {
                    let (handler, _tmp) = make_bench_handler();

                    // Setup: insert base graph + rules + force initial materialization
                    rt.block_on(async {
                        let insert = generate_random_graph(nodes, base_edges, 42);
                        handler.query_program(None, insert).await.unwrap();
                        handler
                            .query_program(None, "+reach(X, Y) <- edge(X, Y)".to_string())
                            .await
                            .unwrap();
                        handler
                            .query_program(
                                None,
                                "+reach(X, Z) <- reach(X, Y), edge(Y, Z)".to_string(),
                            )
                            .await
                            .unwrap();
                        // Force initial materialization
                        handler
                            .query_program(None, "?reach(X, Y)".to_string())
                            .await
                            .unwrap();
                    });

                    // Incremental: insert new edges, then re-query — measure only the re-query
                    rt.block_on(async {
                        let new_edges = generate_incremental_edges(nodes, inc_count, 1000 + i);
                        handler.query_program(None, new_edges).await.unwrap();

                        let start = Instant::now();
                        handler
                            .query_program(None, "?reach(X, Y)".to_string())
                            .await
                            .unwrap();
                        total += start.elapsed();
                    });
                }
                total
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Multi-hop Deduction — The README Example
// ---------------------------------------------------------------------------

fn bench_multi_hop_deduction(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("multi_hop_deduction");
    group.sample_size(10);

    // (memories, part_of links, cities)
    let sizes: Vec<(usize, usize, usize)> =
        vec![(100, 200, 50), (1_000, 2_000, 200), (10_000, 20_000, 500)];

    for (memories, links, cities) in sizes {
        let (handler, _tmp) = make_bench_handler();
        let label = format!("{memories}mem_{links}links_{cities}cities");

        // Setup: insert knowledge graph + define rules
        rt.block_on(async {
            let (part_of, located_in, mem_about) =
                generate_knowledge_graph(memories, links, cities);
            handler.query_program(None, part_of).await.unwrap();
            handler.query_program(None, located_in).await.unwrap();
            handler.query_program(None, mem_about).await.unwrap();

            // Define deductive rules
            handler
                .query_program(None, "+connected(A, B) <- part_of(A, B)".to_string())
                .await
                .unwrap();
            handler
                .query_program(
                    None,
                    "+connected(A, B) <- part_of(A, Mid), connected(Mid, B)".to_string(),
                )
                .await
                .unwrap();
            handler
                .query_program(
                    None,
                    "+relevant(Id, Text, Place) <- memory(Id, Text), about(Id, Topic), connected(Topic, Region), located_in(Place, Region)".to_string(),
                )
                .await
                .unwrap();
        });

        // Query: what's relevant for a specific city?
        group.bench_with_input(BenchmarkId::new("city_lookup", &label), &(), |b, _| {
            b.iter(|| {
                rt.block_on(
                    handler.query_program(None, "?relevant(Id, Text, \"city_42\")".to_string()),
                )
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Analytical 3-Way Join
// ---------------------------------------------------------------------------

fn bench_three_way_join(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("three_way_join");
    group.sample_size(10);

    // (customers, products, orders)
    let sizes: Vec<(u32, u32, u32)> = vec![(1_000, 100, 10_000), (10_000, 1_000, 100_000)];

    for (customers, products, orders) in sizes {
        let (handler, _tmp) = make_bench_handler();
        let label = format!("{customers}c_{products}p_{orders}o");

        // Setup: insert data
        rt.block_on(async {
            let (cust, prod, ord) = generate_orders_data(customers, products, orders);
            handler.query_program(None, cust).await.unwrap();
            handler.query_program(None, prod).await.unwrap();
            handler.query_program(None, ord).await.unwrap();
        });

        // 3-way join + filter
        group.bench_with_input(BenchmarkId::new("join_filter", &label), &(), |b, _| {
            b.iter(|| {
                rt.block_on(handler.query_program(
                    None,
                    "?orders(_, CustId, ProdId, Qty), customer(CustId, \"region_1\"), product(ProdId, Cat, Price), Total = Qty * Price".to_string(),
                ))
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 5. HNSW Vector Search
// ---------------------------------------------------------------------------

fn bench_vector_search(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("vector_search");
    group.sample_size(10);

    let dims = 128;
    let batch_size = 1_000;

    for count in [1_000usize, 10_000] {
        let (handler, _tmp) = make_bench_handler();
        let label = format!("{count}v_{dims}d");

        // Setup: insert vectors in batches of 1K
        rt.block_on(async {
            let vectors = generate_random_vectors(count, dims, 42);
            for chunk_start in (0..count).step_by(batch_size) {
                let chunk_end = (chunk_start + batch_size).min(count);
                let tuples: Vec<String> = (chunk_start..chunk_end)
                    .map(|i| format!("({}, {})", i + 1, format_vector(&vectors[i])))
                    .collect();
                let insert = format!("+vectors[{}]", tuples.join(", "));
                handler.query_program(None, insert).await.unwrap();
            }
        });

        // Search: cosine similarity with a random query vector
        let query_vec = generate_random_vectors(1, dims, 999);
        let query_lit = format_vector(&query_vec[0]);

        group.bench_with_input(BenchmarkId::new("cosine_search", &label), &(), |b, _| {
            b.iter(|| {
                rt.block_on(handler.query_program(
                    None,
                    format!("?vectors(Id, V), Dist = cosine(V, {query_lit}), Dist < 0.5"),
                ))
            });
        });
    }

    // Insertion throughput benchmark
    {
        let label = "insert_1k_128d";
        let vectors = generate_random_vectors(1_000, dims, 77);
        let tuples: Vec<String> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| format!("({}, {})", i + 1, format_vector(v)))
            .collect();
        let insert_stmt = format!("+vecs[{}]", tuples.join(", "));

        group.bench_function(BenchmarkId::new("insert_batch", label), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (handler, _tmp) = make_bench_handler();
                    let start = Instant::now();
                    rt.block_on(handler.query_program(None, insert_stmt.clone()))
                        .unwrap();
                    total += start.elapsed();
                }
                total
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 6. Persistence Round-Trip
// ---------------------------------------------------------------------------

fn bench_persistence(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("persistence");
    group.sample_size(10);

    let size = 10_000u32;

    // Pre-generate the insert statement (shared across iterations)
    let tuples: Vec<String> = (1..=size)
        .map(|i| format!("({i}, \"value_{i}\", {i})"))
        .collect();
    let insert_stmt = format!("+persist_data[{}]", tuples.join(", "));

    // A: Insert with persistence enabled vs disabled
    group.bench_function(BenchmarkId::new("insert_10k", "persist_on"), |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (handler, _tmp) = make_persist_handler();
                let start = Instant::now();
                rt.block_on(handler.query_program(None, insert_stmt.clone()))
                    .unwrap();
                total += start.elapsed();
            }
            total
        });
    });

    group.bench_function(BenchmarkId::new("insert_10k", "persist_off"), |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (handler, _tmp) = make_bench_handler();
                let start = Instant::now();
                rt.block_on(handler.query_program(None, insert_stmt.clone()))
                    .unwrap();
                total += start.elapsed();
            }
            total
        });
    });

    // B: Save + Load round-trip
    group.bench_function("save_load_10k", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let tmp = tempfile::tempdir().expect("tempdir");
                let data_dir = tmp.path().to_path_buf();

                // Insert data with persistence
                {
                    let (handler, _tmp2) = {
                        let mut config = Config::default();
                        config.storage.data_dir = data_dir.clone();
                        config.storage.performance.query_timeout_ms = 0;
                        config.storage.performance.max_insert_tuples = 0;
                        config.storage.performance.max_result_rows = 0;
                        config.storage.performance.max_query_size_bytes = 0;
                        config.storage.persist.enabled = true;
                        config.storage.persist.durability_mode = DurabilityMode::Immediate;
                        let handler = Handler::from_config(config).expect("handler");
                        (handler, tmp.path().to_path_buf())
                    };
                    rt.block_on(handler.query_program(None, insert_stmt.clone()))
                        .unwrap();
                    // Handler drops here, flushing persist
                }

                // Measure: create new handler from same data_dir (recovery)
                let start = Instant::now();
                let mut config = Config::default();
                config.storage.data_dir = data_dir;
                config.storage.performance.query_timeout_ms = 0;
                config.storage.performance.max_insert_tuples = 0;
                config.storage.performance.max_result_rows = 0;
                config.storage.performance.max_query_size_bytes = 0;
                config.storage.persist.enabled = true;
                let _handler2 = Handler::from_config(config).expect("handler recovery");
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion config & main
// ---------------------------------------------------------------------------

criterion_group! {
    name = production;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(15))
        .warm_up_time(Duration::from_secs(5))
        .sample_size(20);
    targets = bench_transitive_closure, bench_magic_sets, bench_incremental_updates,
              bench_multi_hop_deduction, bench_three_way_join,
              bench_vector_search, bench_persistence
}
criterion_main!(production);
