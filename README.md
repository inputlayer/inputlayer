# InputLayer

[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0%20%2B%20Commons%20Clause-blue.svg)](./LICENSE)

**Streaming reasoning layer for AI systems.**

Store facts. Define rules. InputLayer derives the conclusions, keeps them current as data changes, and explains every result with a proof tree. Combine recursive reasoning with vector search in a single query. Open source.

---

## Quick Example

Connecting flights - define direct routes as facts, let InputLayer derive all reachable destinations:

```datalog
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

```datalog
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

Apache 2.0 + Commons Clause. Open source for non-commercial use. Commercial use requires a separate license - see [COMMERCIAL_LICENSE.md](./COMMERCIAL_LICENSE.md).
