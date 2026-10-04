# InputLayer

[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Elastic%202.0-blue.svg)](./LICENSE)

**The live knowledge graph for AI agents.**

### Models think. InputLayer knows.

A fact changes. InputLayer derives what it means for your agent, without another prompt.

InputLayer applies your rules as facts change, updating what your agent should say or do, even while other work continues. Keep your models and framework; connect them to current results and evidence.

<sub>"Knows" means accepted facts plus rule-derived conclusions; source freshness and delivery still apply.</sub>

Self-hosted, source-available under the Elastic License 2.0. Rust engine with Python and JS SDKs.

> Decision models judge. Language models think. InputLayer knows.

---

## Separate Knowing from Thinking

Every turn, agents ask the model things the system already knows: is this order late, is this customer eligible, what else is affected. With InputLayer, facts and rules live outside the prompt, so the agent is not limited by the context window. Facts stream in, your rules derive the answers, and only the answers enter the prompt.

- **Current, exact answers.** A fact changes and the affected conclusions update, including the ones that stop being true, with the facts and rules behind each.
- **Not limited by the context window.** Facts and rules live outside the prompt; only the derived answers go in.
- **A deterministic fast path.** A small intent model picks which known question was asked; the engine answers it exactly from live facts, with no generative model on that path. Open questions still go to the LLM.
- **Fits the stack you have.** [LangGraph](https://inputlayer.ai/docs/guides/langgraph/) memory, state and checkpointer; [LangChain](https://inputlayer.ai/docs/guides/langchain/) tool and retriever; an OpenAI-compatible [fact-checking gateway](https://inputlayer.ai/docs/guides/verified-completions/); and change triggers your agent can wake on.

```iql
// rules, written once
+late(O) <- shipment(O,S), eta(S,T), promised(O,P), T > P
+can_offer(O,C) <- late(O), customer(O,C), eligible(C, "expedite")
```

```python
# agent: told what changed, no re-reading
for change in kg.watch("?can_offer(O, C)"):
    for row in change.added:   offer(row)
    for row in change.removed: withdraw(row)
```

The SDK form shown is the upcoming release; standing queries run over the [WebSocket API](https://inputlayer.ai/docs/guides/websocket-api/) today.

**Example.** "Where's order 4821, can it still make Friday?" A small intent model maps it to `ask_status(4821)`; the engine answers "due Thursday" from live facts and a template speaks it. The carrier update lands mid-sentence: the old answer is withdrawn and the agent says "Correction: Friday". Only open questions go to the LLM.

**Why now.** Your data stack went live years ago: nightly ETL became change data capture, cron jobs became event-driven services, full refreshes became incremental views. Your agents are the last batch jobs left.

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

The SDKs are not on PyPI or npm yet; install them from a source checkout of this repository.

**Python** (3.10+):
```bash
git clone https://github.com/inputlayer/inputlayer.git
pip install ./inputlayer/packages/inputlayer-py
# extras: pip install "./inputlayer/packages/inputlayer-py[pandas,langchain,langgraph]"
```

```python
from inputlayer import InputLayer

async with InputLayer() as il:
    kg = il.knowledge_graph("default")
    result = await kg.query(CanReach)
```

**TypeScript** (Node.js 18+):
```bash
# build and pack the SDK from the checkout
(cd inputlayer/packages/inputlayer-js && npm ci && npm run build && npm pack)
# then, in your project, install the tarball under the name `inputlayer`
npm install inputlayer@file:/path/to/inputlayer/packages/inputlayer-js/inputlayer-js-dev-0.1.1.tgz
```

See [Python SDK docs](https://inputlayer.ai/docs/guides/python-sdk/) and [TypeScript SDK docs](https://inputlayer.ai/docs/guides/js-sdk/).

---

## Flagship Guide

**[How to build a voice agent that knows](https://inputlayer.ai/blog/building-a-voice-agent-that-knows/)** - a voice pipeline with InputLayer at its heart: facts and rules in a live knowledge graph, known questions answered without a model, and an agent that corrects itself mid-sentence when the world changes.

Keep your LLM, your vector store for documents and your systems of record; InputLayer is the live knowledge graph between your data and your agent's decisions.

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
