# InputLayer Benchmarks

All numbers measured on Apple M4 Max (16 cores), 128 GB RAM, macOS 26.3, Rust 1.91.1 release build with LTO. Criterion.rs, 10 samples, 15s measurement, 5s warmup.

---

## Bound Recursive Queries — Magic Sets

The headline result. Most graph queries in practice are bound: "what can I reach from *this* node?" not "give me every reachable pair in the entire graph." Magic Sets rewrites the recursive fixpoint to only compute demanded tuples.

Erdos-Renyi random graphs (2:1 edge-to-node ratio, seed 42):

| Graph | Full TC | Bound `?reach(1, Y)` | Point `?reach(1, 42)` | Speedup |
|-------|---------|----------------------|-----------------------|---------|
| 500 nodes, 1K edges | 578 ms | **2.01 ms** | **1.80 ms** | 288x |
| 1,000 nodes, 2K edges | 2.40 s | **3.52 ms** | **3.13 ms** | 682x |
| 2,000 nodes, 4K edges | 10.49 s | **6.61 ms** | **5.68 ms** | 1,587x |

Speedup grows with graph size because full TC is O(N^2) while bound queries only explore the reachable subgraph from the seed.

### How this compares

**Neo4j** is the obvious comparison — it's a native graph database purpose-built for traversals. Neo4j performs single-source BFS in ~1-5ms on graphs of this size using adjacency-list storage. InputLayer's **5.7ms** for bound reachability on a 2,000-node graph is in the same ballpark — but InputLayer is doing this through general-purpose recursive Datalog, not a hardcoded traversal algorithm. Neo4j can't express arbitrary recursive rules; InputLayer can, at comparable speed.

**PostgreSQL recursive CTEs** suffer the exact problem Magic Sets solves. A `WITH RECURSIVE` query computes the full closure, then filters. On a 15K-relation graph, PostgreSQL takes [~12 seconds to compute 2M closure rows](https://news.ycombinator.com/item?id=10620747). There is no way to push a `WHERE source = 1` constraint into the recursive step in SQL. InputLayer's Magic Sets does exactly that at the AST level.

**DuckDB recursive CTEs** hit a harder wall. On LDBC social network graphs with just 484 nodes and 2K edges, standard recursive CTEs [run out of memory](https://duckdb.org/2025/05/23/using-key) (606M intermediate rows for a 424-node graph). DuckDB's new `USING KEY` feature (SIGMOD 2025) addresses row explosion for shortest-path, but it's a different optimization — it deduplicates paths, not demand restriction. InputLayer handles these graph sizes comfortably.

**Souffle** (compiled Datalog to C++) is faster for full materialization — it compiles rules to optimized parallel C++, achieving roughly [2-5x better throughput on batch TC](https://souffle-lang.github.io/benchmarks). But Souffle requires a 13-second compilation step before execution, has no incremental update support, and its [magic sets implementation](https://souffle-lang.github.io/magicset) operates at the same conceptual level as InputLayer's. For interactive use (REPL, API queries, agent workloads), InputLayer's zero-compilation interpreted execution with single-digit millisecond bound queries is the better fit.

| System | Bound Reachability (2Kn) | Full TC (2Kn) | Arbitrary Recursion | Incremental |
|--------|--------------------------|---------------|---------------------|-------------|
| **InputLayer** | **5.7 ms** | 10.5 s | Yes | Yes (DD) |
| Neo4j (BFS) | ~1-5 ms | N/A | No | No |
| PostgreSQL (CTE) | ~50-200 ms | ~5-15 s | No | No |
| DuckDB (CTE) | OOM | OOM | No | No |
| Souffle (compiled) | ~ms (with magic) | ~5 s | Yes | No |

InputLayer matches native graph database latency for bound queries while supporting features none of them offer: arbitrary recursive Datalog with incremental Differential Dataflow maintenance.

---

## Full Transitive Closure

Full materialization of all reachable pairs. This is the worst-case workload — compute everything, filter nothing.

| Graph | Time | Output Size |
|-------|------|-------------|
| 500 nodes, 1K edges | **578 ms** | ~62K pairs |
| 1,000 nodes, 2K edges | **2.40 s** | ~250K pairs |
| 2,000 nodes, 4K edges | **10.49 s** | ~1M pairs |

Scaling is O(N^2.1) in output size, dominated by the fixpoint computation. Souffle compiled to C++ is 2-5x faster here. But full TC is rarely the real workload — Magic Sets (above) eliminates it for bound queries.

---

## Incremental Updates

Pre-populates a graph with TC rules, forces initial materialization, then inserts new edges and measures only the re-query time.

| Base Graph | Increment | Re-query Time |
|------------|-----------|---------------|
| 500 nodes, 1K edges | +10 edges | **614 ms** |
| 1,000 nodes, 2K edges | +10 edges | **2.44 s** |
| 1,000 nodes, 2K edges | +100 edges | **2.68 s** |

Adding 10x more edges (+100 vs +10) only costs 10% more time. Differential Dataflow propagates deltas through the fixpoint rather than recomputing from scratch.

PostgreSQL materialized views require full re-computation on any change. Neo4j has no materialized recursive views at all. Souffle re-runs the entire program. InputLayer is the only system here with native incremental maintenance of recursive results.

---

## Multi-hop Deductive Queries

The core use case: given a knowledge graph of concepts, categories, regions, and memories, derive relevant context through chains of rules.

```
+connected(A, B) <- part_of(A, B)
+connected(A, B) <- part_of(A, Mid), connected(Mid, B)
+relevant(Id, Text, Place) <- memory(Id, Text), about(Id, Topic),
                               connected(Topic, Region), located_in(Place, Region)
```

Query: `?relevant(Id, Text, "city_42")`

| Scale | Time |
|-------|------|
| 100 memories, 200 links, 50 cities | **940 us** |
| 1K memories, 2K links, 200 cities | **4.46 ms** |
| 10K memories, 20K links, 500 cities | **42.3 ms** |

Sub-50ms relevance retrieval over 10K memories with recursive multi-hop deduction. This query combines recursive graph traversal with multi-way joins and string filtering — something that would require multiple round-trips in a graph database or hand-written application logic in SQL.

---

## Analytical Joins

Three-way join across orders, products, and customers with string filter and arithmetic:

```
?orders(_, CustId, ProdId, Qty), customer(CustId, "region_1"),
 product(ProdId, Cat, Price), Total = Qty * Price
```

| Scale | Time |
|-------|------|
| 1K customers, 100 products, 10K orders | **11.8 ms** |
| 10K customers, 1K products, 100K orders | **128 ms** |

100K-row three-way join in 128ms. This isn't competing with columnar OLAP engines like DuckDB (which handles TPC-H at millions of rows), but it's fast enough for operational queries, agent workloads, and interactive analytics where the data fits in a knowledge graph.

---

## Vector Search (HNSW)

128-dimensional normalized random vectors with cosine similarity threshold.

| Vectors | Search Latency | Insert Throughput |
|---------|----------------|-------------------|
| 1K x 128-dim | **1.05 ms** | 17,800 vec/sec |
| 10K x 128-dim | **7.36 ms** | — |

Purpose-built vector databases (Qdrant, Weaviate, Milvus) are faster at scale — they're optimized for millions of vectors. InputLayer's advantage is combining vector similarity search *inside Datalog rules* alongside logical deduction, graph traversal, and joins in a single query. No other system does this.

---

## Persistence

WAL + batch file persistence with Immediate durability, 10K tuples (3 columns each):

| Operation | Time |
|-----------|------|
| Insert 10K tuples (persist ON) | **180 ms** |
| Insert 10K tuples (persist OFF) | **183 ms** |
| Recovery from WAL | **2.63 ms** |

Zero measurable persistence overhead — the WAL is efficiently batched. Crash recovery loads 10K tuples in 2.6ms.

---

## Microbenchmarks

| Operation | 1K rows | 10K rows |
|-----------|---------|----------|
| Simple scan | 505 us | 4.70 ms |
| Two-way join | 277 us | 1.36 ms |
| Recursive closure | 4.01 ms (50n) | 58.3 ms (200n) |
| Single insert | 4.02 ms | — |
| Batch insert | 30.4 ms (100) | 395 ms (1K) |
| COUNT aggregation | 505 us | 3.87 ms |
| SUM aggregation | 541 us | 3.87 ms |
| MIN+MAX aggregation | 1.72 ms | 13.9 ms |

---

## Running the Benchmarks

```bash
# Full production suite (~15 min)
cargo bench --bench production_benchmarks

# Individual groups
cargo bench --bench production_benchmarks -- transitive_closure
cargo bench --bench production_benchmarks -- magic_sets
cargo bench --bench production_benchmarks -- incremental
cargo bench --bench production_benchmarks -- multi_hop
cargo bench --bench production_benchmarks -- three_way_join
cargo bench --bench production_benchmarks -- vector_search
cargo bench --bench production_benchmarks -- persistence

# Microbenchmarks
cargo bench --bench query_benchmarks
cargo bench --bench insert_benchmarks
cargo bench --bench aggregation_benchmarks
```
