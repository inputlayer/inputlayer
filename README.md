# InputLayer

[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Elastic%202.0-blue.svg)](./LICENSE)

**Streaming reasoning layer for AI systems.**

Store facts. Define rules. InputLayer derives the conclusions, keeps them current as data changes, and explains every result with a proof tree. Combine recursive reasoning with vector search in a single query. Source-available and free to use.

---

## Quick Example

Connecting flights - define direct routes as facts, let InputLayer derive all reachable destinations:

```iql
// Facts: direct flight routes
+direct_flight[("New York", "London"), ("London", "Paris"), ("Paris", "Tokyo"), ("Tokyo", "Sydney")]

// Rules: you can reach a destination directly, or through connections
+can_reach(A, B) <- direct_flight(A, B)
+can_reach(A, C) <- direct_flight(A, B), can_reach(B, C)

// Query: where can you fly from New York?
?can_reach("New York", Dest)
```

```
┌────────────┬──────────┐
│ New York   │ Dest     │
├────────────┼──────────┤
│ "New York" │ "London" │
│ "New York" │ "Paris"  │
│ "New York" │ "Tokyo"  │
│ "New York" │ "Sydney" │
└────────────┴──────────┘
4 rows
```

Four facts, two rules, and the engine derived every reachable destination - including connections through intermediate cities.

---

## What Makes It Different

### Rules + vector search in one query

A shopper asks for printer ink. In embedding space, every ink cartridge looks the same. But only specific models fit their printer - that's a structured fact, not a similarity score. InputLayer evaluates compatibility rules and ranks by cosine distance in a single query.

### Correct conclusion retraction

An entity is cleared from a sanctions list. Every flag derived through it retracts - but only if no second ownership path still supports it. InputLayer tracks every derivation path independently and only retracts when all paths are gone.

### Incremental updates

One fact changes in a 2,000-node graph with 400,000 derived relationships. InputLayer updates only the affected derivations in **6.83ms**. Full recompute: 11.3 seconds. **1,652x faster.**

### Provenance

Run `.why` on any result and get a structured proof tree showing which facts and which rules produced it. Run `.why_not` to see exactly which condition blocked a derivation.

```iql
.why ?can_reach("New York", "Sydney")
// [rule] can_reach (clause 1): can_reach(A, C) <- direct_flight(A, B), can_reach(B, C)
//   [base] direct_flight("New York", "London")
//   [rule] can_reach (clause 1): ...
//     [base] direct_flight("London", "Paris")
//     [rule] can_reach (clause 1): ...
//       [base] direct_flight("Paris", "Tokyo")
//       [rule] can_reach (clause 0): can_reach(A, B) <- direct_flight(A, B)
//         [base] direct_flight("Tokyo", "Sydney")
```

---

## Get Started

```bash
# Docker
docker run -p 8080:8080 ghcr.io/inputlayer/inputlayer

# Or build from source
git clone https://github.com/inputlayer/inputlayer.git
cd inputlayer
cargo build --release
./target/release/inputlayer-server --port 8080
```

Open [http://localhost:8080](http://localhost:8080) for the interactive GUI, or connect via WebSocket at `ws://localhost:8080/ws`.

If you know SQL, the query language takes about 10 minutes to learn. See the [Quick Start Guide](https://inputlayer.ai/docs/guides/quickstart/).

---

## Ontologies, Ready to Go

InputLayer ships ready-made ontologies for common use cases in the [ontology registry](https://github.com/inputlayer/ontology-registry) — rule packs you install into a running engine with one command, Helm-style. The first is **`consistency-core` (Verified Completions)**: logical-consistency verification for AI conversations — contradictions, timeline cycles, identity mix-ups, and policy violations, every finding backed by verbatim quoted spans and a proof tree, validated against a 1,628-scenario adversarial corpus.

```bash
il search                                      # browse the registry
il install consistency-core --kg mychat --create   # sha256-verified, one atomic deploy
il list --kg mychat                            # what's installed, pinned by version+digest
```

The `il` CLI builds with the engine (`cargo build --bin il`) and talks to the server over the same WebSocket API as every other client. The design keeps one hard rule: the LLM only ever writes *data* — the rules are human-written, reviewed in the registry, and frozen at load. See `docs/internals/verified-completions/` for the rule pack's design, benchmark corpus, and extraction contract.

---

## SDKs

**Python:**
```bash
pip install inputlayer
```

```python
from inputlayer import InputLayer

async with InputLayer() as il:
    kg = il.knowledge_graph("default")
    result = await kg.query(CanReach)
```

**TypeScript:**
```bash
npm install inputlayer-js
```

See [Python SDK docs](https://inputlayer.ai/docs/guides/python-sdk/) and [TypeScript SDK docs](https://inputlayer.ai/docs/guides/js-sdk/).

---

## Use Cases

- **[Financial Risk](https://inputlayer.ai/use-cases/financial-risk/)** - Trace ownership chains to any depth for sanctions screening. Correct retraction handles the diamond problem.
- **[Conversational Commerce](https://inputlayer.ai/use-cases/commerce/)** - Compatibility rules + vector similarity in one query. The wrong cartridge never gets recommended.
- **[Manufacturing](https://inputlayer.ai/use-cases/manufacturing/)** - Multi-hop dependency chains from training records to production line availability, updated in milliseconds.
- **[Supply Chain](https://inputlayer.ai/use-cases/supply-chain/)** - A port closes and every affected supplier, order, and SLA penalty is identified across the graph.
- **[Agentic AI](https://inputlayer.ai/use-cases/agentic-ai/)** - Agent memory as a knowledge graph with `.why` proof trees for every conclusion.

---

## Built On

[Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow) by Frank McSherry. Incremental computation engine written in Rust. Single binary, no external dependencies.

## Documentation

- [Quick Start](https://inputlayer.ai/docs/guides/quickstart/)
- [Core Concepts](https://inputlayer.ai/docs/guides/core-concepts/)
- [Explainability (.why / .why_not)](https://inputlayer.ai/docs/guides/explainability/)
- [Vector Search](https://inputlayer.ai/docs/guides/vectors/)
- [Recursion](https://inputlayer.ai/docs/guides/recursion/)
- [Python SDK](https://inputlayer.ai/docs/guides/python-sdk/)
- [TypeScript SDK](https://inputlayer.ai/docs/guides/js-sdk/)
- [WebSocket API Docs](https://inputlayer.ai/docs/guides/configuration/)

## Contributing

See [CONTRIBUTING](CONTRIBUTING).

## License

InputLayer uses a split licensing model: the core is protected, the clients are permissive.

| Component | Path | License |
|-----------|------|---------|
| **Core** (server, engine, everything not listed below) | repository root | [Elastic License 2.0](./LICENSE) |
| Python SDK | `packages/inputlayer-py` | Apache 2.0 |
| TypeScript SDK | `packages/inputlayer-js` | Apache 2.0 |
| API client | `packages/api-client` | Apache 2.0 |
| VS Code extension | `packages/inputlayer-vscode` | MIT |

**Core (Elastic License 2.0):** free to use, copy, modify, and run - including commercially and in production. You may not provide InputLayer to third parties as a hosted or managed service, and you may not circumvent license-key functionality or remove licensing notices. For rights beyond that, see [COMMERCIAL_LICENSE.md](./COMMERCIAL_LICENSE.md).

These terms apply to all versions of InputLayer, including every pre-1.0 development version preceding the official 1.0 release.

**Clients (Apache 2.0 / MIT):** embed them in any application without restriction.

"InputLayer" is a trademark of InputLayer - see [NOTICE](./NOTICE).
