# InputLayer

[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**A reasoning engine for AI agents.**

Your agent retrieves context by searching for things that *look like* the question. That fails when the answer is connected through a chain of facts — not surface similarity. A shellfish allergy doesn't look like a restaurant query. A drug interaction doesn't look like a prescription request. A sanctions-listed subsidiary doesn't look like a wire transfer.

InputLayer gives your agent a reasoning layer. You store facts, define rules, and the engine derives everything that logically follows — including things you never explicitly stored. When data changes, derived knowledge updates instantly. No batch jobs, no stale caches.

---

## What InputLayer is

InputLayer is a **modern database for AI agents** built on three key concepts:

- **Knowledge graph**: data is stored as facts and relationships, not flat documents
- **Deductive**: you define rules, and the system automatically derives everything that logically follows
- **Streaming**: when facts change, all derived conclusions update instantly — no batch jobs, no stale caches

The query language is declarative — you state *what* you want, not *how* to compute it. Rules compose naturally and support full recursion.

### A concrete example

```datalog
// Facts: who manages whom
+manages("alice", "bob")
+manages("bob", "charlie")
+manages("bob", "diana")

// Rule: transitive authority (recursive)
+authority(X, Y) <- manages(X, Y)
+authority(X, Z) <- manages(X, Y), authority(Y, Z)

// Query: who does Alice have authority over?
?authority("alice", Person)
```

Result: `bob`, `charlie`, `diana` — computed through recursive rule evaluation, not keyword search.

Now add vector search in the same query:

```datalog
// Find documents that Alice can access AND that are relevant to her question
?authority("alice", Author),
 document(DocId, Author, Embedding),
 Similarity = cosine(Embedding, [0.9, 0.1, ...]),
 Similarity > 0.7
```

This is **policy-filtered semantic search** — logical access control and vector similarity in a single query. No other system does this natively.

---

## Why this matters

AI agents retrieve context with vector search. Vector search finds things that *look like* the answer. Reasoning finds things that *are* the answer.

- You ask for **dinner recommendations**. The agent finds sushi preferences. It misses your **shellfish allergy** — because medical info doesn't look like a restaurant query.

- Your doctor prescribes **new medication**. The health assistant checks for side effects. It misses a **drug interaction** — because "current medications" doesn't look like "is this drug safe?"

- A compliance officer asks about a **wire transfer**. The system finds similar transactions. It misses that the recipient is a **subsidiary of a sanctioned entity** — because that requires graph traversal, not pattern matching.

The critical context is always connected through a **chain of facts**. Finding it requires reasoning.

InputLayer sits between raw memory and the LLM. Instead of hoping the right context lands in top-K results, you define *what counts as relevant* with rules — and get back exactly that.

---

## What it does

**Structured reasoning.** Follow chains of relationships to surface context that search alone misses. Recursive rules derive transitive closure, reachability, authority chains — any logical relationship.

**Hybrid retrieval.** Vector similarity, graph traversal, and logical rules in a single query. Policy-filtered semantic search. Access control and relevance in one pass.

**Incremental maintenance.** When facts change, only affected derivations recompute. Insert one edge into a 2,000-node graph, re-query transitive closure: 6.83ms vs 11.3s full recompute. That's [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow) under the hood.

**Correct retraction.** Delete a fact and every conclusion derived through it disappears automatically — even through recursive rule chains. No stale permissions, no phantom data.

**Explainable results.** Every derived fact traces back to the rules and base facts that produced it. "The system derived X because rule A applied to facts B and C" — not "the vector was close."

---

## How it compares

| Capability | Vector DBs | Graph DBs | SQL | **InputLayer** |
|---|---|---|---|---|
| Vector similarity | native | plugin | - | **native** |
| Graph traversal | - | native | CTEs | **native** |
| Rule-based inference | - | - | - | **native** |
| Recursive reasoning | - | Cypher paths | recursive CTEs | **natural recursion** |
| Incremental updates | - | - | some | **native** |
| Correct retraction | - | - | - | **native** |
| Explainable retrieval | - | paths | - | **rule traces** |

---

## What's in the box

- 55 built-in functions (vector ops, temporal, math, string, LSH)
- HNSW vector indexes (cosine, euclidean, dot product, manhattan)
- Recursive queries with Magic Sets optimization
- Incremental computation via Differential Dataflow
- Persistent storage (Parquet + write-ahead log)
- Multi-tenancy (isolated knowledge graphs)
- WebSocket API with streaming transport
- Python SDK with object-logic mapper
- Single binary. No cluster, no JVM, no dependencies.

---

## Getting started

```bash
# Build from source
git clone https://github.com/inputlayer/inputlayer.git
cd inputlayer
cargo build --release

# Interactive REPL
./target/release/inputlayer

# WebSocket server
./target/release/inputlayer-server --port 8080
```

The query language is intuitive — if you've used SQL, the basics take about 10 minutes.

---

## Documentation

- [Quick Start Guide](docs/guides/quickstart.md)
- [Core Concepts](docs/guides/core-concepts.md)
- [Syntax Reference](docs/reference/syntax-cheatsheet.md)
- [Commands Reference](docs/reference/commands.md)
- [Built-in Functions](docs/reference/functions.md)
- [Architecture](docs/internals/architecture.md)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache 2.0 - see [LICENSE](LICENSE).
