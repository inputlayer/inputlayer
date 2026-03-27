# InputLayer

[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0%20%2B%20Commons%20Clause-blue.svg)](./LICENSE)

**The reasoning data layer for enterprise AI systems.**

InputLayer sits between your data and your AI. Give it facts and rules - it derives
everything that follows, keeps those derivations current as facts change, and traces
every result back to the logic that produced it.

Built on [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow).
When a fact changes, only the affected derivations recompute. Not the whole graph.

---

## The Problem

Enterprise AI systems fail in a specific, consistent way: they retrieve context
by similarity, not by consequence.

A supply chain agent asks whether an order can ship by Friday. The relevant facts -
current inventory, supplier lead times, which carriers are suspended, which
suppliers are under sanctions review - are not semantically similar to "can this
order ship by Friday." They are connected through chains of operational
relationships. Similarity search does not follow chains. It finds what sounds like
the answer.

The same pattern appears in every domain:

**Manufacturing:** An agent asks whether a production line can run tonight.
The answer depends on maintenance schedules, parts availability, and which
equipment has an active quality hold. None of those facts are semantically
close to the question.

**Financial risk:** Compliance asks whether a transaction is suspicious.
The answer requires traversing entity ownership chains - Entity A paid Entity B,
B is a subsidiary of C, C is on a sanctions list. Pattern matching finds similar
transactions. It does not follow the ownership graph.

**Healthcare:** A patient asks what they can eat. The answer requires following:
prescribed medication -> drug interactions -> ingredient contraindications -> food.
Medical information lives in a different embedding space from dietary questions.

These are not retrieval failures. They are reasoning failures. The context
exists. The system cannot reach it.

---

## What InputLayer Does

InputLayer is a deductive context graph. You define facts and rules. The engine
derives everything that logically follows and keeps those derivations current as
your data changes.

```
facts + rules -> derived facts, updated incrementally
```

Three properties matter for production use:

**Incremental maintenance.** When a fact changes, only the affected derivations
recompute. Insert one new edge into a 2,000-node graph and re-query transitive
closure: 6.83ms. Full recompute: 11.3 seconds. The engine handles the delta -
you do not.

**Correct retraction.** Delete a fact and every conclusion derived through it
disappears automatically, including through chains of recursive rules. No phantom
permissions. No stale cache. No manual invalidation.

**Explainable derivation.** Every result traces back to the base facts and rules
that produced it. Not "the vector was close" - a full derivation chain you can
audit, log, and show to a regulator.

---

## How It Works

Here is the supply chain example. The agent needs to know whether an order can
ship by Friday.

Facts about the current operational state:

```datalog
+supplier[
    ("sup_01", "status", "active"),
    ("sup_02", "status", "suspended"),
    ("sup_03", "status", "active")
]

+required_supplier[("order_2847", "sup_01"), ("order_2847", "sup_02")]
+lead_time[("sup_01", 3), ("sup_03", 2)]
```

Rules that derive fulfillment status from those facts:

```datalog
+order_blocked(OrderId, SupplierId, "supplier_suspended") <-
    required_supplier(OrderId, SupplierId),
    supplier(SupplierId, "status", "suspended")

+can_ship_by_friday(OrderId) <-
    required_supplier(OrderId, SupplierId),
    supplier(SupplierId, "status", "active"),
    lead_time(SupplierId, Days),
    Days =< 4,
    !order_blocked(OrderId, _, _)
```

Query:

```datalog
?order_blocked("order_2847", Supplier, Reason)
```

Result: `order_2847` is blocked because `sup_02` is suspended. When `sup_02`
is reinstated, the derivation updates automatically. No pipeline re-run.

---

## The Shellfish Problem

This is the clearest illustration of why retrieval alone fails.

A travel agent has hundreds of memories about a user. The user says:
"Recommend me some restaurants in Tokyo."

The agent retrieves the top 10 memories by vector similarity. It gets memories
about past trips to Japan, restaurant preferences, favorite cuisines.

The user's shellfish allergy is nowhere in the top 10. It is health information.
Medical conditions do not live near restaurant queries in embedding space.

The agent recommends a crab kaiseki restaurant. The user ends up in hospital.

The allergy was the most important context. The relevance comes through a chain:

```
Tokyo restaurants -> Japanese cuisine -> shellfish is a staple -> user has shellfish allergy
```

Here is how InputLayer handles this:

```datalog
+user_memory[
    ("m1", "loves_sushi", "User loves sushi", [0.91, 0.12, 0.03]),
    ("m2", "shellfish_allergy", "Severe shellfish allergy", [0.22, 0.05, 0.88]),
    ("m3", "visited_paris", "Visited Paris last year", [0.45, 0.67, 0.11])
]

+related_to[
    ("sushi", "japanese_cuisine"),
    ("shellfish", "japanese_cuisine"),
    ("japanese_cuisine", "tokyo")
]

+memory_topic[("m1", "sushi"), ("m2", "shellfish"), ("m3", "paris")]

+trip_relevant(MemId, Text, "direct") <-
    user_memory(MemId, _, Text, _),
    memory_topic(MemId, Topic),
    related_to(Topic, "tokyo")

+trip_relevant(MemId, Text, "inferred") <-
    user_memory(MemId, _, Text, _),
    memory_topic(MemId, Topic),
    related_to(Topic, Bridge),
    related_to(Bridge, "tokyo")
```

Query: `?trip_relevant(Id, Text, How)`

| Id | Text | How |
|---|---|---|
| m1 | User loves sushi | direct |
| m2 | Severe shellfish allergy | inferred |

The allergy was never tagged as trip-relevant. The engine followed
`shellfish -> japanese_cuisine -> tokyo` and derived it from the rules.

---

## Where InputLayer Fits

InputLayer is not a replacement for your existing stack. It is the reasoning
layer that sits between your data and your AI system.

```
[Your data sources] -> [InputLayer: facts + rules + derived context] -> [Your AI]
```

- For similarity search: use Pinecone, pgvector, or Weaviate
- For transactions and relational data: use Postgres
- For stream ingestion: use Kafka, Flink, or Materialize
- For orchestration: use your existing AI platform

InputLayer handles the reasoning question: given the current state of the world,
what context is logically relevant to this decision?

---

## Capability Comparison

| Capability | Vector DBs | Graph DBs | SQL | InputLayer |
|---|---|---|---|---|
| Vector similarity | native | plugin | - | native |
| Graph traversal | - | native | CTEs | native |
| Rule-based inference | - | - | - | native |
| Recursive reasoning | - | Cypher paths | recursive CTEs | natural |
| Incremental updates | - | - | some | native |
| Correct retraction | - | - | - | native |
| Explainable derivation | - | paths | - | rule traces |

---

## Getting Started

```bash
docker run -p 8080:8080 ghcr.io/inputlayer/inputlayer

# Or build from source
git clone https://github.com/inputlayer/inputlayer.git
cd inputlayer
cargo build --release
./target/release/inputlayer                         # interactive REPL
./target/release/inputlayer-server --port 8080      # WebSocket server
```

See the [Quick Start Guide](./docs/guides/quickstart.md). If you know SQL, the
query language takes about 10 minutes to learn.

---

## What's Included

- 55 built-in functions (vector ops, temporal, math, string, LSH bucketing)
- HNSW vector index (cosine, euclidean, dot product, manhattan)
- Recursive queries (transitive closure, graph reachability, fixpoint)
- Incremental computation via Differential Dataflow
- Persistent storage (Parquet + write-ahead log)
- Multi-tenancy - isolated knowledge graphs per tenant
- WebSocket API with AsyncAPI docs
- Python SDK
- Provenance API - every result ships with a complete derivation proof
- Single binary. No cluster, no JVM, no external dependencies.

---

## Roadmap

- [ ] LangChain integration
- [ ] Hybrid search (BM25 + vector)
- [ ] Confidence propagation through reasoning chains

---

## Documentation

- [Quick Start Guide](./docs/guides/quickstart.md)
- [Core Concepts](./docs/guides/core-concepts.md)
- [Syntax Reference](./docs/reference/syntax-cheatsheet.md)
- [Commands Reference](./docs/reference/commands.md)
- [Built-in Functions](./docs/reference/functions.md)
- [Architecture](./docs/internals/architecture.md)

## Contributing

See [CONTRIBUTING](CONTRIBUTING).

## License

Apache 2.0 + Commons Clause for open source and non-commercial use.
Commercial use requires a separate license - see [COMMERCIAL_LICENSE.md](./COMMERCIAL_LICENSE.md).
