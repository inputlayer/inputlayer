# InputLayer

[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**Reasoning context graph for AI agents.**

Vector search finds what's similar. InputLayer finds what follows. InputLayer is the only platform where skills, memory, and planning all live in the same reasoning engine. Every decision is explainable via rule traces. Experience compounds via pheromone trails and value functions that update incrementally. And the developer API stays clean — 4 lines to first value, ~20 lines for a full planning loop.

```python
from inputlayer import InputLayer
from inputlayer.ontologies import ACO, AgentMemory
from inputlayer.skill_packs import web_navigation, e_commerce

il = InputLayer()
agent = il.agent("shopper", ontology=[ACO, AgentMemory])
agent.install_skills(web_navigation)
agent.install_skills(e_commerce)

agent.start_episode(goal="Buy a nickel-finish nightstand under $140")
for step in range(30):
    snapshot = agent.observe(url=page.url, dom_text=page.accessibility.snapshot())
    plan = agent.plan(goal="Buy a nickel-finish nightstand under $140", observation=snapshot)
    execute(plan)  # your browser automation
    reward, done = evaluate(page)
    agent.step(plan=plan, result={"url": page.url}, reward=reward, done=done)
    if done: break
agent.end_episode()
# Pheromone trails now guide future episodes toward successful paths
```

---

## The Shellfish Problem

Imagine you have an AI travel agent. Over past conversations, it's built up hundreds of memories about the user  - travel preferences, food likes, work history, health information, hobbies, family details.

Now the user says: **"Recommend me some restaurants in Tokyo."**

Before answering, the agent needs to pull relevant context from its memory. It embeds the user's message and does a vector search over all stored memories, ranked by similarity. The top 10 go into the LLM prompt.

The results make intuitive sense: memories about past trips to Japan, restaurant preferences, and food opinions rank near the top  - they're in the same semantic neighborhood as "restaurants in Tokyo." Travel and dining content sounds like travel and dining queries.

But the user's **shellfish allergy** is nowhere near the top 10. It's a piece of health information. In embedding space, medical conditions live in a completely different neighborhood from restaurant recommendations. The allergy might as well be about blood pressure medication as far as the embeddings are concerned  - it just doesn't sound like a restaurant query.

So the agent never sees it. It recommends a crab kaiseki place. The user ends up in a hospital.

**The allergy is the most important context here.** Japanese cuisine  - especially in Tokyo  - heavily features shellfish: crab, shrimp, lobster, oysters. But that connection goes through world knowledge, not through vector similarity:

```
Tokyo restaurants → Japanese cuisine → shellfish is a staple → user has shellfish allergy
```

No embedding model will make a medical condition rank highly against a restaurant query. They're in different semantic domains. The relevance comes from a **chain of relationships**  - and that's a reasoning problem, not a search problem.

Here's how InputLayer handles it. First, the data:

```datalog
+user_memory[
    ("m1", "loves_sushi", "User loves sushi", [0.91, 0.12, 0.03]),
    ("m2", "shellfish_allergy", "Severe shellfish allergy", [0.22, 0.05, 0.88]),
    ("m3", "visited_paris", "Visited Paris last year", [0.45, 0.67, 0.11])
]

+related_to[("sushi", "japanese_cuisine"), ("shellfish", "japanese_cuisine"),
            ("japanese_cuisine", "tokyo")]

+memory_topic[("m1", "sushi"), ("m2", "shellfish"), ("m3", "paris")]
```

Then rules that follow those relationships:

```datalog
// Direct: memories about topics related to the destination
+trip_relevant(MemId, Text, "direct") <-
    user_memory(MemId, _, Text, _),
    memory_topic(MemId, Topic),
    related_to(Topic, "tokyo")

// Transitive: memories connected THROUGH an intermediate topic
+trip_relevant(MemId, Text, "inferred") <-
    user_memory(MemId, _, Text, _),
    memory_topic(MemId, Topic),
    related_to(Topic, Bridge),
    related_to(Bridge, "tokyo")
```

```datalog
?trip_relevant(Id, Text, How)
```

| Id | Text | How |
|----|------|-----|
| m1 | User loves sushi | direct |
| m2 | Severe shellfish allergy | inferred |

The allergy was never tagged as trip-relevant. Nobody inserted it as a result. The engine followed shellfish → japanese\_cuisine → tokyo and derived it from the rules.

---

## What is this thing

InputLayer is a deductive context graph. You give it facts and rules, and it figures out everything that logically follows. It's built on [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow), so when your data changes, only the affected results recompute  - not the whole thing.

It also has a built-in HNSW vector index, so you can do similarity search and logical reasoning in the same query. You don't need to glue Pinecone and Neo4j together and hope they agree.

The query language is Datalog. If you've used SQL, the basics take about 10 minutes to pick up. If you've used Prolog, you already know it.

```
facts + rules → derived facts (updated incrementally)
```

---

## How It Compares

| Capability | Vector DBs | Graph DBs | SQL | **InputLayer** |
|---|---|---|---|---|
| Vector Similarity | native | plugin | -- | **native** |
| Graph Traversal | -- | native | CTEs | **native** |
| Rule-Based Inference | -- | -- | -- | **native** |
| Incremental Updates | -- | -- | some | **native** |
| Recursive Reasoning | -- | Cypher paths | recursive CTEs | **natural recursion** |
| Explainable Retrieval | -- | paths | -- | **rule traces** |

---

## Use Cases

**Agent memory.** The shellfish problem above. Your agent has context scattered across hundreds of memories and the relevant pieces aren't semantically close to the query. Rules let you follow chains of relationships to surface what matters.

**Access control baked into the query.** Instead of filtering results after retrieval, you write the policy as a rule. Who can see what is part of the computation, not a middleware layer that might disagree.

```datalog
+accessible_doc(User, Doc, Score) <-
    document(Doc, Embedding),
    !confidential(Doc),
    has_permission(User, Doc),
    query_embedding(User, QEmb),
    Score = cosine(Embedding, QEmb),
    Score > 0.5
```

**Multi-hop expansion.** You found 10 relevant documents. Now find documents cited by those documents. That's two lines of Datalog, not a pipeline of API calls.

**Temporal weighting.** Combine semantic similarity with time decay in the same rule. Recent context matters more  - express that as logic, not post-processing.

---

## When to Use Something Else

If all you need is similarity search, use Pinecone or pgvector. If you need transactions, use Postgres. If you need stream processing, use Materialize or Flink.

InputLayer sits between your data and your LLM. It's the reasoning layer  - it figures out what to retrieve. It complements your existing stack, doesn't replace it.

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

## Getting Started

```bash
# Build from source
git clone https://github.com/inputlayer/inputlayer.git
cd inputlayer
cargo build --release

# Interactive REPL
./target/release/inputlayer

# WebSocket server
./target/release/inputlayer-server --port 8080
# WebSocket endpoint: ws://localhost:8080/ws
# AsyncAPI docs: http://localhost:8080/api/ws-docs
# AsyncAPI YAML: http://localhost:8080/api/asyncapi.yaml
```

See the [documentation](docs/) for guides, syntax reference, and the full function library.

---

## Roadmap

- [ ] Python SDK
- [ ] Docker image
- [ ] LangChain integration
- [ ] Hybrid search (BM25 + vector)
- [ ] Provenance API -- trace any result back through the rules that derived it
- [ ] Confidence propagation through reasoning chains

---

## Documentation

- [Quick Start Guide](docs/guides/quickstart.md)
- [Core Concepts](docs/guides/core-concepts.md)
- [Syntax Reference](docs/reference/syntax-cheatsheet.md)
- [Commands Reference](docs/reference/commands.md)
- [Built-in Functions](docs/reference/functions.md)
- [Architecture](docs/internals/architecture.md)

## License

Apache 2.0 -- see [LICENSE](LICENSE).
