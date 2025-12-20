# InputLayer.AI

[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![Core License](https://img.shields.io/badge/Core%20License-Apache%202.0-blue.svg)](LICENSE)
[![Enterprise](https://img.shields.io/badge/Enterprise-Commercial-lightgrey.svg)](#enterprise--commercial)

**Policy-first, incremental and explainable retrieval engine for AI systems.**

InputLayer is a high-performance **incremental Datalog engine** built on [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow), designed to be the **retrieval + policy layer** for RAG and agentic applications.

It includes:
- **Rules & recursion** (Datalog) for *governance, entitlements, and business logic*
- **Native vector similarity** for *semantic retrieval*
- **Incremental computation** for *real-time updates at low latency*
- **Persistent storage** for *durability and reproducibility*
- **Data connectors** for easy integration into existing stacks.

---

## Why InputLayer (for AI/RAG)

Most RAG stacks glue together: vector DB + filters + ACL checks + “business logic” scattered across services.

InputLayer collapses the stack bloat and lets you express retrieval as **one auditable program**:

- **Policy-first retrieval:** encode “who can see what” and “what should be returned” as rules
- **Explainable by construction:** retrieval logic is explicit, reviewable, testable, and versionable
- **Real-time:** updates flow through incrementally—no full reindex/recompute cycles
- **Hybrid-ready:** combine structured facts, relationships, and vector similarity in the same query plan

Typical uses:
- **Entitlements-aware RAG** (“retrieval firewall” before the LLM sees anything)
- **Knowledge graph + vector retrieval**
- **Context assembly for agents** (tools, memory, constraints, recency, and access rules)
- **Personalization & recommendations** with deterministic constraints
- **Data ingestions and transform pipelines** with validation and incremental join support

---

## Features

- **Incremental Computation** — Differential Dataflow for efficient incremental updates
- **Recursive Queries** — full support for recursive Datalog with automatic fixpoint iteration
- **Stratified Negation** — negation with automatic dependency analysis
- **Aggregations** — count, sum, min, max, avg with grouping support
- **Vector Operations** — native euclidean, cosine, dot product, and manhattan distance
- **Persistent Storage** — Parquet-based storage with WAL for durability
- **Type System** — optional typed relations with schema validation
- **Client–Server** — QUIC-based RPC for distributed deployment

---

## Quick Start

### Installation

```bash
cargo install inputlayer
````

Or build from source:

```bash
git clone https://github.com/InputLayer/inputlayer.git
cd inputlayer
cargo build --release
```

### Run the REPL

```bash
inputlayer
```

---

## A “policy-first RAG” example

This example shows the core idea: **retrieve candidates semantically, then enforce policy via rules**.

```datalog
// Create and use a database
.db create ragdb
.db use ragdb

// --- Facts: users, groups, documents, ACLs ---
+member[("alice", "engineering"), ("bob", "sales")].
+doc[(101, "Design Doc"), (102, "Sales Pitch"), (103, "Runbook")].
+acl[("engineering", 101), ("engineering", 103), ("sales", 102)].

// --- Facts: embeddings (toy vectors) ---
+emb[(101, [1.0, 0.0, 0.0]),
     (102, [0.0, 1.0, 0.0]),
     (103, [1.0, 1.0, 0.0])].

// Query embedding
+q[([0.9, 0.1, 0.0])].

// --- Policy: user can access doc if user is in a group allowed by ACL ---
view can_access(User: string, DocId: int) :-
  member(User, Group),
  acl(Group, DocId).

// --- Retrieval: semantic candidates + distance score ---
view candidate(DocId: int, Dist: float) :-
  emb(DocId, V),
  q(Q),
  Dist = cosine(V, Q).

// --- Final retrieval: enforce policy + rank ---
view retrieve(User: string, DocId: int, Dist: float) :-
  can_access(User, DocId),
  candidate(DocId, Dist).

// Ask: what can alice retrieve?
?- retrieve("alice", DocId, Dist).
```

**What you get:** one place to define *both* “what’s relevant” and “what’s allowed”.

---

## Vector Similarity Search (standalone)

```datalog
.db create vectors_db
.db use vectors_db

// Insert vectors
+vectors[(1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0]), (3, [1.0, 1.0, 0.0])].
+query[([0.0, 0.0, 0.0])].

// Compute distances
view nearest(Id: int, Dist: float) :-
  vectors(Id, V),
  query(Q),
  Dist = euclidean(V, Q).

?- nearest(X, Y).
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        InputLayer                        │
├─────────────────────────────────────────────────────────┤
│   Parser → IR Builder → Optimizer → Code Generator       │
├─────────────────────────────────────────────────────────┤
│              Differential Dataflow Runtime               │
├─────────────────────────────────────────────────────────┤
│                Storage (Parquet + WAL)                   │
└─────────────────────────────────────────────────────────┘

```

---

## Syntax Reference

### Database Commands

```datalog
.db create <name>     // Create database
.db use <name>        // Switch to database
.db drop <name>       // Drop database
.db list              // List databases
```

### Facts

```datalog
+relation[(1, 2), (3, 4)].     // Insert
-relation[(1, 2)].             // Delete
```

### Views

```datalog
// Basic view
view connected(X: int, Y: int) :- edge(X, Y).

// Recursive view
view reach(X: int, Y: int) :- edge(X, Y).
view reach(X: int, Y: int) :- edge(X, Z), reach(Z, Y).

// With negation
view not_connected(X: int, Y: int) :- node(X), node(Y), !edge(X, Y).

// With aggregation
view dept_count(D: int, count<E>: int) :- employee(E, D).
```

### Queries

```datalog
?- relation(X, Y).            // Query all
?- relation(1, X).            // With constant
?- view(X, Y), X > 10.        // With filter
```

### Types

* `int` — 64-bit signed integer
* `string` — UTF-8 string
* `float` — 64-bit floating point

### Built-in Functions

| Function            | Description             |
| ------------------- | ----------------------- |
| `euclidean(v1, v2)` | Euclidean (L2) distance |
| `cosine(v1, v2)`    | Cosine distance         |
| `dot(v1, v2)`       | Dot product             |
| `manhattan(v1, v2)` | Manhattan (L1) distance |
| `normalize(v)`      | Unit vector             |

### Aggregations

| Function   | Description  |
| ---------- | ------------ |
| `count<X>` | Count values |
| `sum<X>`   | Sum values   |
| `min<X>`   | Minimum      |
| `max<X>`   | Maximum      |
| `avg<X>`   | Average      |

---

## Client–Server Mode

Start the server:

```bash
inputlayer-server --host 0.0.0.0 --port 9090
```

Connect with client:

```bash
inputlayer-client --host 127.0.0.1 --port 9090
```

## Configuration

Create `inputlayer.toml`:

```toml
[storage]
data_dir = "./data"
wal_enabled = true

[execution]
workers = 4
```

---

## Performance Notes

* **Incremental:** only recomputes affected results on updates
* **Parallel:** multi-threaded via Timely Dataflow
* **Efficient Storage:** columnar Parquet with compression
* **Optimized Joins:** join spanning tree planning

---

## Enterprise & Commercial

InputLayer is built to be **open-source first**, with optional paid offerings for production environments.

### Core (Open Source)

* Licensed under **Apache 2.0** (see [LICENSE](LICENSE))
* Ideal for embedding in prototypes, internal tools, and custom RAG stacks

### Enterprise (Commercial)

For organizations that need production-grade operations and governance, we offer an **Enterprise Edition** and **support contracts** (commercial license).

Typical Enterprise needs:

* SSO/SAML/OIDC and SCIM
* Audit logs, governance workflows, policy management UX
* HA clustering, backups/restore, upgrade tooling
* Observability, SLOs, query replay, cost attribution
* Long-term support, security reviews, and SLA-backed support

> If you need a commercial license or support, open a GitHub Discussion (or contact us via the channel listed in this repo).

---

## Roadmap (near-term)

* “Explain” output for retrieval (rule traces / provenance)
* Connector ecosystem (docs/warehouses/streams) as plugins
* Stronger operational tooling for Kubernetes and multi-node deployments
* Reference architectures for RAG + agents (with eval harness)

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

We aim for:

* Small, reviewable PRs
* Strong test coverage (unit + snapshot tests)
* Clear examples and docs for common RAG/policy patterns

---

## Running Tests

```bash
# Unit tests
cargo test

# Snapshot tests (192 test cases)
./scripts/run_snapshot_tests.sh

# Specific category
./scripts/run_snapshot_tests.sh -f recursion

# Verbose output
./scripts/run_snapshot_tests.sh -v
```

---

## Benchmarks

```bash
cargo bench
```
