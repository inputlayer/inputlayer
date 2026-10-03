# InputLayer

[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Elastic%202.0-blue.svg)](./LICENSE)

**The streaming engine for AI agents.**

### Agents that react in milliseconds, not on the next run.

Today's agents are batch jobs. They wake up, re-read everything, reason, act and go back to sleep: stale between runs, slow to react, and paying to recompute what didn't change. InputLayer makes your agents streaming. Every change is reasoned over the moment it lands, only what changed is recomputed, and a conclusion that stops being true is withdrawn right away.

Self-hosted, source-available under the Elastic License 2.0. Rust engine with Python and JS SDKs.

> Your data stack went streaming years ago. Your agents are the last batch jobs left.

---

## Why Streaming Agents

- **React when it happens, not when the cron fires.** A batch agent learns about a change on its next run, seconds to hours later. A streaming agent knows within milliseconds and can act while it still matters.
- **Never act on something that stopped being true.** Batch agents see what's there and miss what disappeared: the cancelled order, the revoked approval, the delay that cleared. InputLayer withdraws a conclusion the moment its last reason goes away.
- **Pay for change, not for re-reading the world.** Batch cost grows with data volume times how often you run. Streaming cost grows with what actually changed.

## How It Works

1. **Stream facts in** from the systems you already run: orders, carriers, permissions, inventory, written as they change from CDC, webhooks or your app.
2. **Declare the reasoning** as rules, not glue code. Rules chain, recurse and combine with vector similarity.
3. **React to changes.** When a fact changes, InputLayer updates just the affected conclusions and tells the agent what was added and what was withdrawn, with the facts and rules behind each one.

```iql
// rules, written once
+late(O) <- shipment(O,S), eta(S,T), promised(O,P), T > P
+can_offer(O,C) <- late(O), customer(O,C), eligible(C, "expedite")
```

```python
# agent: react to what changed
for change in kg.watch("?can_offer(O, C)"):
    for row in change.added:   offer(row)
    for row in change.removed: withdraw(row)
```

The SDK form shown is the upcoming release; standing queries run over the [WebSocket API](https://inputlayer.ai/docs/guides/websocket-api/) today.

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

## Under the Hood

### Rules + vector search in one query

A shopper asks for printer ink. In embedding space, every ink cartridge looks the same. But only specific models fit their printer - that's a structured fact, not a similarity score. InputLayer evaluates compatibility rules and ranks by cosine distance in a single query.

### Conclusions withdrawn when they stop holding

An entity is cleared from a sanctions list. Every flag derived through it is withdrawn - but only if no second ownership path still supports it. InputLayer tracks every derivation path independently and only retracts when all paths are gone.

### Incremental updates

When a fact changes, InputLayer updates only the affected derivations instead of recomputing everything. After inserting 100 edges into a 2,000-node graph with recursive rules, a bound reachability query answers in **6.83 ms**, versus 11.3 seconds to recompute the full transitive closure (single machine; see [BENCHMARKS.md](./BENCHMARKS.md)).

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

See the [Quick Start Guide](https://inputlayer.ai/docs/guides/quickstart/) to load a sample and watch conclusions change as facts do.

---

## Ontologies, Ready to Go

InputLayer ships ready-made ontologies for common use cases in the [ontology registry](https://github.com/inputlayer/ontology-registry) — rule packs you install into a running engine with one command, Helm-style. The first is **`consistency-core` (Verified Completions)**: logical-consistency verification for AI conversations — contradictions, timeline cycles, identity mix-ups, and policy violations, every finding backed by verbatim quoted spans and a proof tree, validated against a 1,628-scenario adversarial corpus.

```bash
il search                                      # browse the registry
il install consistency-core --kg mychat --create   # sha256-verified, one atomic deploy
il list --kg mychat                            # what's installed, pinned by version+digest
```

The `il` CLI builds with the engine (`cargo build --bin il`) and talks to the server over the same WebSocket API as every other client. It also carries schema migrations - `il migration generate / apply / revert / status` - with language-neutral JSON migration files (see the [migrations guide](https://inputlayer.ai/docs/guides/migrations/)). The design keeps one hard rule: the LLM only ever writes *data* — the rules are human-written, reviewed in the registry, and frozen at load. See `docs/internals/verified-completions/` for the rule pack's design, benchmark corpus, and extraction contract.

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

Built for agents that act on a world that keeps changing. Keep your LLM, your vector store for documents and your systems of record; InputLayer is the streaming layer between your data and your agent's decisions.

- **[Financial Risk](https://inputlayer.ai/use-cases/financial-risk/)** - Sanctions and ownership flags that update the moment an ownership link changes. A flag is withdrawn only when every path supporting it is gone.
- **[Conversational Commerce](https://inputlayer.ai/use-cases/commerce/)** - Compatibility rules + vector similarity in one query, with recommendations withdrawn the moment stock runs out.
- **[Manufacturing](https://inputlayer.ai/use-cases/manufacturing/)** - Production line availability recomputed per event, not per sweep, from training records to equipment status.
- **[Supply Chain](https://inputlayer.ai/use-cases/supply-chain/)** - A port closes and every affected supplier, order, and SLA penalty is identified across the graph as it happens.
- **[Agentic AI](https://inputlayer.ai/use-cases/agentic-ai/)** - Agent conclusions that stay current as observations change, with `.why` proof trees for every conclusion.

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
