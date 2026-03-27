# InputLayer

[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0%20%2B%20Commons%20Clause-blue.svg)](./LICENSE)

**Streaming reasoning layer for AI. Incremental rules engine with vector search, graph traversal, and explainable derivation traces.**

InputLayer sits between your data and your AI. Give it facts and rules - it derives
everything that follows, keeps those derivations current as facts change, and traces
every result back to the logic that produced it.

Built on [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow).
When a fact changes, only the affected derivations recompute. Not the whole graph.

---

## The Problem

A shopper types: *"I need ink for my printer."*

They do not say which printer. Eight months ago they bought a Canon PIXMA MG3620.
The correct cartridges are the Canon PG-245 and CL-246. There are 847 ink
cartridges in the catalogue.

A vector search for "printer ink" returns cartridges ranked by semantic similarity.
The problem: every ink cartridge is semantically similar to "printer ink." In
embedding space, a Canon PG-245, an Epson 202, and a Brother LC3013 are
essentially identical. Similarity search has no mechanism to distinguish them.

The correct answer requires closing a chain across three completely separate fact
domains:

```
shopper owns Canon PIXMA MG3620       (purchase history)
  -> MG3620 uses PG-245 and CL-246    (compatibility matrix)
  -> PG-245 in stock, 2-pack, $34.99  (live inventory)
  -> recommend this specific SKU
```

None of those connections are semantic. Compatibility is a structured logical
relationship - a printer takes specific cartridge models, period. There is no
text similarity between "Canon PIXMA MG3620" and "PG-245." The connection exists
only as a fact in a compatibility table.

The shopper does not know their cartridge model. That is why they asked the agent
instead of searching directly. The agent has everything it needs to answer - the
purchase history, the compatibility data, the inventory - in three separate systems
that nothing connects at query time.

**The standard workaround is glue code:** query purchase history, extract the
printer model, hit a compatibility API, cross-reference with inventory, filter
results, re-rank. Four round trips across separate systems. Brittle to model name
variations. No live inventory awareness. Nothing recomputes when stock changes.
And if the shopper asks a follow-up question, you run all of it again.

InputLayer closes the chain in one query, keeps it current as inventory changes,
and produces a derivation proof for every result.

---

## How It Works

InputLayer is a streaming reasoning layer. You define facts and rules. The engine
derives everything that logically follows, keeps those derivations current as data
changes incrementally, and traces every result back to the rules that produced it.

```
facts + rules -> derived facts, updated incrementally
```

Five properties matter for production use:

**Recursive rule evaluation.** Rules can reference themselves. Define product
compatibility hierarchies, category trees, or transitive access policies - the
engine follows the chain to its conclusion without you specifying the depth.

**Vector search and graph in one query.** Run a vector similarity search and a
rule-based graph traversal in the same query. No glue code, no separate round
trips, no two systems that may disagree.

**Incremental maintenance.** When a fact changes, only the affected derivations
recompute. Insert one new edge into a 2,000-node graph: 6.83ms to re-derive
transitive closure. Full recompute: 11.3 seconds. The engine handles the delta.

**Correct retraction.** Delete a fact and every conclusion derived through it
disappears automatically - but only if no other derivation path supports it. No
stale recommendations. No phantom in-stock items. No manual cache invalidation.

**Derivation proofs.** Every result ships with a complete, machine-readable proof -
the full chain of rules and base facts that produced it. Exposed via the
Provenance API, per result.

---

## The Demo

The printer ink scenario, in code.

### The data

```datalog
// Purchase history - what the shopper owns
+owns("shopper_42", "canon-pixma-mg3620", "2025-07-14")
+owns("shopper_42", "hp-envy-6055e",      "2024-02-03")

// Compatibility matrix - which cartridges work with which printers
// This is structured product data, not text. There is no semantic path
// from a printer model name to a cartridge model name.
+compatible("canon-pixma-mg3620", "canon-pg-245")
+compatible("canon-pixma-mg3620", "canon-cl-246")
+compatible("canon-pixma-mg3620", "canon-pg-245xl")
+compatible("canon-pixma-mg3620", "canon-cl-246xl")
+compatible("hp-envy-6055e",      "hp-67-black")
+compatible("hp-envy-6055e",      "hp-67-tricolor")
+compatible("hp-envy-6055e",      "hp-67xl-black")
// ... thousands of printer-cartridge relationships

// Live inventory with embeddings for the product descriptions
+product("canon-pg-245",    "Canon PG-245 Black Ink Cartridge",       14.99, [0.81, 0.44, ...])
+product("canon-cl-246",    "Canon CL-246 Color Ink Cartridge",       16.99, [0.79, 0.41, ...])
+product("canon-pg-245xl",  "Canon PG-245XL High Yield Black",        22.99, [0.83, 0.46, ...])
+product("epson-202-black", "Epson 202 Black Ink Cartridge",          13.99, [0.82, 0.43, ...])
+product("hp-67-black",     "HP 67 Black Original Ink Cartridge",     16.99, [0.80, 0.42, ...])
// ... 847 total cartridges

+in_stock("canon-pg-245",   42)
+in_stock("canon-cl-246",   18)
+in_stock("canon-pg-245xl",  3)
+in_stock("epson-202-black", 67)
+in_stock("hp-67-black",    29)
```

### The rules

```datalog
// A cartridge is relevant to a shopper if they own a compatible printer
+compatible_with_shopper(Shopper, CartridgeId) <-
    owns(Shopper, PrinterId, _),
    compatible(PrinterId, CartridgeId)

// A cartridge is recommendable if it is compatible and in stock
+recommendable(Shopper, CartridgeId) <-
    compatible_with_shopper(Shopper, CartridgeId),
    in_stock(CartridgeId, Qty), Qty > 0
```

### The query - compatibility rules and semantic search in one pass

The shopper typed "ink for my printer." No model name. No cartridge number.
This single query closes the chain from owned device to correct in-stock SKU,
ranked by cosine distance to the query embedding (lower = more relevant).

```datalog
?recommendable("shopper_42", CartridgeId),
 product(CartridgeId, Description, Price, Embedding),
 query_vec(Q),
 Distance = cosine(Embedding, Q),
 Distance < 0.05
```

**Actual engine output:**

```
┌──────────────┬──────────────────┬──────────────────────────────────────────┬───────┬──────────┐
│ shopper_42   │ CartridgeId      │ Description                              │ Price │ Distance │
├──────────────┼──────────────────┼──────────────────────────────────────────┼───────┼──────────┤
│ "shopper_42" │ "canon-pg-245"   │ "Canon PG-245 Black Ink Cartridge"       │ 14.99 │   0.0000 │
│ "shopper_42" │ "hp-67-black"    │ "HP 67 Black Original Ink Cartridge"     │ 16.99 │   0.0002 │
│ "shopper_42" │ "hp-67xl-black"  │ "HP 67XL Black High Yield Ink Cartridge" │ 25.99 │   0.0007 │
│ "shopper_42" │ "canon-pg-245xl" │ "Canon PG-245XL High Yield Black"        │ 22.99 │   0.0010 │
│ "shopper_42" │ "canon-cl-246"   │ "Canon CL-246 Color Ink Cartridge"       │ 16.99 │   0.0091 │
│ "shopper_42" │ "hp-67-tricolor" │ "HP 67 Tri-color Original Ink Cartridge" │ 17.99 │   0.0189 │
└──────────────┴──────────────────┴──────────────────────────────────────────┴───────┴──────────┘
6 rows
```

`epson-202-black` and `brother-lc3013` - semantically identical to the Canon and
HP cartridges in embedding space - do not appear. They are compatible with other
printers, not the ones this shopper owns. The rules excluded them.

`canon-cl-246xl` is compatible but has 0 stock, so the `Qty > 0` condition in
the `recommendable` rule excluded it.

**Derivation proof for `canon-pg-245` (via proof query):**

```
┌──────────────┬──────────────────────┬──────────────┬─────┐
│ shopper_42   │ PrinterId            │ _            │ Qty │
├──────────────┼──────────────────────┼──────────────┼─────┤
│ "shopper_42" │ "canon-pixma-mg3620" │ "2025-07-14" │  42 │
└──────────────┴──────────────────────┴──────────────┴─────┘
```

The chain: shopper owns `canon-pixma-mg3620` (purchased 2025-07-14), which is
compatible with `canon-pg-245`, which has 42 units in stock. Any of these facts
can be audited, corrected, or updated independently.

---

## When Facts Change

### Stock runs out mid-session

The three remaining PG-245XL units sell while the shopper is browsing.

```datalog
-in_stock("canon-pg-245xl", 3)
+in_stock("canon-pg-245xl", 0)
```

Re-running the same query now returns 5 rows instead of 6 - `canon-pg-245xl`
is gone:

```
┌──────────────┬──────────────────┬──────────────────────────────────────────┬───────┬──────────┐
│ shopper_42   │ CartridgeId      │ Description                              │ Price │ Distance │
├──────────────┼──────────────────┼──────────────────────────────────────────┼───────┼──────────┤
│ "shopper_42" │ "canon-pg-245"   │ "Canon PG-245 Black Ink Cartridge"       │ 14.99 │   0.0000 │
│ "shopper_42" │ "hp-67-black"    │ "HP 67 Black Original Ink Cartridge"     │ 16.99 │   0.0002 │
│ "shopper_42" │ "hp-67xl-black"  │ "HP 67XL Black High Yield Ink Cartridge" │ 25.99 │   0.0007 │
│ "shopper_42" │ "canon-cl-246"   │ "Canon CL-246 Color Ink Cartridge"       │ 16.99 │   0.0091 │
│ "shopper_42" │ "hp-67-tricolor" │ "HP 67 Tri-color Original Ink Cartridge" │ 17.99 │   0.0189 │
└──────────────┴──────────────────┴──────────────────────────────────────────┴───────┴──────────┘
5 rows
```

No cache flush. No re-run. The retraction propagated through the rule chain automatically.

### Shopper buys a new printer

```datalog
+owns("shopper_42", "epson-ecotank-et-2800", "2026-03-27")
+compatible[("epson-ecotank-et-2800", "epson-522-black"), ("epson-ecotank-et-2800", "epson-522-cyan")]
```

The same query now returns 7 rows - Epson 522 products appear automatically:

```
┌──────────────┬───────────────────┬──────────────────────────────────────────┬───────┬──────────┐
│ shopper_42   │ CartridgeId       │ Description                              │ Price │ Distance │
├──────────────┼───────────────────┼──────────────────────────────────────────┼───────┼──────────┤
│ "shopper_42" │ "canon-pg-245"    │ "Canon PG-245 Black Ink Cartridge"       │ 14.99 │   0.0000 │
│ "shopper_42" │ "hp-67-black"     │ "HP 67 Black Original Ink Cartridge"     │ 16.99 │   0.0002 │
│ "shopper_42" │ "epson-522-black" │ "Epson 522 Black Ink Bottle"             │  7.99 │   0.0006 │
│ "shopper_42" │ "hp-67xl-black"   │ "HP 67XL Black High Yield Ink Cartridge" │ 25.99 │   0.0007 │
│ "shopper_42" │ "canon-cl-246"    │ "Canon CL-246 Color Ink Cartridge"       │ 16.99 │   0.0091 │
│ "shopper_42" │ "hp-67-tricolor"  │ "HP 67 Tri-color Original Ink Cartridge" │ 17.99 │   0.0189 │
│ "shopper_42" │ "epson-522-cyan"  │ "Epson 522 Cyan Ink Bottle"              │  7.99 │   0.0224 │
└──────────────┴───────────────────┴──────────────────────────────────────────┴───────┴──────────┘
7 rows
```

No reindex. No batch job. The new printer's compatible cartridges appeared
because the rules derived them from the new facts.

### The standard alternative - and why it breaks

Without InputLayer, the typical implementation is:

```python
# Step 1: query purchase history for printer models
printers = db.query("SELECT product_id FROM orders WHERE user=? AND category='printer'")

# Step 2: for each printer, hit compatibility API
cartridges = []
for printer in printers:
    compatible = compatibility_api.get(printer.model_name)  # brittle to name variations
    cartridges.extend(compatible)

# Step 3: filter by inventory
in_stock = inventory_api.filter(cartridges)

# Step 4: re-rank by relevance
results = vector_db.search("ink for my printer", filter={"id": {"$in": in_stock}})
```

Four systems. Four round trips. The inventory check is a snapshot - if something
sells out between step 3 and the shopper clicking buy, they see a false result.
Model name variations between the purchase history and the compatibility database
cause silent misses. And none of this recomputes when anything changes - it runs
from scratch on every query.

InputLayer replaces all four steps with two rules and one query. The derivation
is live, correct, and auditable.

| What changes | Glue code approach | InputLayer |
|---|---|---|
| Item sells out | Stale until next inventory poll | Retracts in milliseconds |
| New printer purchased | Excluded until next reindex | Compatible cartridges available immediately |
| Compatibility data updated | Requires reindex + cache clear | Propagates through affected derivations only |

---

## Where InputLayer Fits

InputLayer is not a replacement for your existing stack. It is the streaming
reasoning layer that sits between your data and your AI system.

```
[Your data sources] -> [InputLayer: facts + rules + derived context] -> [Your AI]
```

- For similarity search: use Pinecone, pgvector, or Weaviate
- For transactions and relational data: use Postgres
- For stream ingestion: use Kafka, Flink, or Materialize
- For orchestration: use your existing AI platform

InputLayer handles the reasoning question: given the current state of the world,
what context is logically relevant to this decision - and why?

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

See the [Quick Start Guide](./docs/guides/quickstart.md). If you know SQL, the query
language takes about 10 minutes to learn. If you know Prolog, you already know it.

To run the printer ink demo from this README:

```bash
# Start the server
./target/release/inputlayer-server --port 8080

# Run the demo (in another terminal)
./target/release/inputlayer-client --script examples/retail/printer-ink.idl
```

See [examples/retail/](./examples/retail/) for the full runnable script.

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
- Provenance API - every result ships with a complete derivation proof, tracing
  the full chain of rules and base facts that produced it
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
