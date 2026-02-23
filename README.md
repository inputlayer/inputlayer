# InputLayer

[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**A incremental deductive knowledge graph for AI agents.**

You give it facts. You give it rules. It figures out everything that logically follows - including things you never explicitly stored.

---

## How it works

You tell InputLayer that Alice is Bob's parent and Bob is Charlie's parent:

```datalog
+parent("Alice", "Bob")
+parent("Bob", "Charlie")
+parent("Bob", "Diana")
```

You write a rule - if X is a parent of Y, and Y is a parent of Z, then X is a grandparent of Z:

```datalog
+grandparent(X, Z) <- parent(X, Y), parent(Y, Z)
+sibling(X, Y) <- parent(P, X), parent(P, Y), X != Y
```

Now ask:

```datalog
?grandparent(X, Y)
```

| X | Y |
|---|---|
| Alice | Charlie |
| Alice | Diana |

```datalog
?sibling(X, Y)
```

| X | Y |
|---|---|
| Charlie | Diana |
| Diana | Charlie |

You inserted 3 facts. Nobody inserted grandparents. Nobody inserted siblings. InputLayer **derived** them from the rules. That's deduction - the database produces facts that were never put in.

Add a new fact - `+parent("Charlie", "Eve")` - and InputLayer instantly derives that Alice is Eve's great-grandparent through the existing rules. No new code. No re-processing everything. Only the affected results update - that's [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow) under the hood.

---

## Why this matters for AI agents

AI agents today retrieve context by searching for memories that **look like** the current question. That works when the answer resembles the question. It fails - silently - when it doesn't.

- You ask for **dinner recommendations in Tokyo**. The agent finds your sushi preferences and past trips to Japan. It misses your **shellfish allergy** - because medical info doesn't look like a restaurant query. It books a crab place.

- Your doctor prescribes a **new medication**. Your health assistant checks for side effects. It misses that it **interacts with a drug you already take** - because "current medications" doesn't look like "is this drug safe?"

The critical context is always connected to the question through a **chain of facts**, not surface resemblance. Finding it requires reasoning, not search.

InputLayer gives your agent a reasoning layer between raw memory and the LLM. Instead of hoping the right context lands in the top-K search results, the agent defines *what counts as relevant* with rules - and gets back exactly that.

- **Structured memory.** Follow chains of relationships to surface context that search alone misses.
- **Built-in policies.** Access control, confidentiality, data filtering - part of the query, not an afterthought.
- **Multi-hop expansion.** Found 10 relevant documents? Find everything they cite. Two lines.
- **Hybrid reasoning.** Vector similarity, graph traversal, and logical rules in a single query.

---

## How it compares

| Capability | Vector DBs | Graph DBs | SQL | **InputLayer** |
|---|---|---|---|---|
| Vector similarity | native | plugin | - | **native** |
| Graph traversal | - | native | CTEs | **native** |
| Rule-based inference | - | - | - | **native** |
| Recursive reasoning | - | Cypher paths | recursive CTEs | **natural recursion** |
| Incremental updates | - | - | some | **native** |
| Explainable retrieval | - | paths | - | **rule traces** |

---

## What's in the box

- 55 built-in functions (vector ops, temporal, math, string, LSH)
- HNSW vector index (cosine, euclidean, dot product, manhattan)
- Recursive queries (transitive closure, graph reachability, fixpoint)
- Incremental computation via Differential Dataflow
- Persistent storage (Parquet + write-ahead log)
- Multi-tenancy (isolated knowledge graphs per tenant)
- WebSocket API with AsyncAPI docs
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

The query language is Datalog. If you've used SQL, the basics take about 10 minutes. If you've used Prolog, you already know it.

---

## Roadmap

- [ ] Python SDK
- [ ] Docker image
- [ ] LangChain / LlamaIndex integration
- [ ] Provenance API - trace any result back through the rules that derived it
- [ ] Confidence propagation through reasoning chains

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
