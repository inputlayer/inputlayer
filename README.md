# InputLayer

[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**The knowledge and reasoning layer for AI systems.**

InputLayer is an incremental engine purpose-built for AI applications — combining vector similarity, graph traversal, and rule-based reasoning in one fast, lightweight service. It's the layer between your data and your AI where retrieval logic, policies, and context assembly live.

## AI Retrieval is a Reasoning Problem

Vector search finds similar content. But similarity isn't relevance.

Real retrieval requires reasoning:
- **"What can this user access?"** → traverse group memberships, check policies
- **"What's relevant to this conversation?"** → combine semantic similarity with recency, preferences, context
- **"Who are the experts on this topic?"** → follow authorship, co-authorship, citation graphs
- **"What tools can this agent use?"** → evaluate capabilities, permissions, current state

This isn't filtering. It's inference.

Take "who are the experts on topic X?" There's no experts table to query. Instead: Alice authored a paper, that paper's embedding is similar to topic X, so Alice is an expert. Bob co-authored with Alice, so Bob inherits partial expertise. These facts—`expert(Alice, X, 0.9)` and `expert(Bob, X, 0.6)`—were never stored. They were derived by combining authorship, embeddings, and co-author relationships. And when Alice publishes a new paper or Bob joins a new collaboration, the derived expertise scores update automatically.

```datalog
+expert(Person, Topic, Score) :-
    authored(Person, Paper),
    similar(Paper, Topic, Score),
    Score > 0.8.

+expert(Person, Topic, Score) :-
    co_author(Person, DirectExpert),
    expert(DirectExpert, Topic, DirectScore),
    Score = DirectScore * 0.7.
```

That's inference: new facts derived from rules over existing facts, kept current as the underlying data changes.

**Vector databases** give you similarity. **SQL databases** give you joins and filters. **InputLayer** gives you reasoning: vectors + graphs + rules, evaluated incrementally as your data changes.

---

## What Makes InputLayer AI-Native

### Vector Operations as Primitives

Similarity search isn't an afterthought - it's built into the core:

```datalog
+relevant(DocId, Score) :-
    embeddings(DocId, Vec),
    query_embedding(Q),
    Score = cosine(Vec, Q),
    Score > 0.7.
```

Combine similarity with any other logic—filtering, ranking, access control—in the same query.

### Natural Graph Recursion

AI systems need to traverse relationships: org charts, knowledge graphs, dependency chains, reasoning paths. Recursion is natural, not awkward CTEs:

```datalog
// Transitive closure - who can Alice reach through her network?
+reachable(A, B) :- knows(A, B).
+reachable(A, C) :- reachable(A, B), knows(B, C).

// Combine with similarity
+relevant_in_network(User, Doc, Score) :-
    reachable(User, Author),
    authored(Author, Doc),
    similar(Doc, Score).
```

### Rule-Based Reasoning

Express policies and constraints as logical rules. The system infers what follows:

```datalog
// Policy: users can access docs if they're in an allowed group
+can_access(User, Doc) :-
    user_group(User, Group),
    doc_group(Doc, Group).

// Policy: premium users also get early access content
+can_access(User, Doc) :-
    premium_user(User),
    early_access(Doc).

// The engine figures out what each user can access
```

This isn't SQL transforms—it's logic programming with incremental maintenance.

### Lightweight and Fast

Single binary, starts in seconds, no cluster required. Deploy as a sidecar next to your inference service or as a shared service. Get started locally, scale when you need to.

---

## Use Cases

### RAG with Complex Retrieval Logic

When retrieval is more than "find similar vectors":

```datalog
// Combine: similarity + access control + recency + user preferences
+retrieve(User, Doc, FinalScore) :-
    similar_to_query(Doc, SimScore),
    can_access(User, Doc),
    recency_boost(Doc, RecencyScore),
    user_preference_boost(User, Doc, PrefScore),
    FinalScore = SimScore * 0.5 + RecencyScore * 0.3 + PrefScore * 0.2.
```

All incrementally maintained. When permissions change, when preferences update, when new documents arrive—results stay fresh.

### Agent Context Assembly

Agents need the right context: available tools, relevant memory, applicable constraints. InputLayer assembles this in real-time:

```datalog
// What tools can this agent use right now?
+available_tool(Agent, Tool) :-
    agent_capability(Agent, Cap),
    tool_requires(Tool, Cap),
    tool_enabled(Tool).

// What's relevant from memory?
+relevant_memory(Agent, Memory, Score) :-
    agent_memory(Agent, Memory, Embedding),
    current_context(CtxEmbedding),
    Score = cosine(Embedding, CtxEmbedding),
    Score > 0.6.

// What constraints apply?
+active_constraint(Agent, Constraint) :-
    agent_role(Agent, Role),
    role_constraint(Role, Constraint).
```

### Knowledge Graph + Vector Hybrid

Traverse structured relationships AND rank by semantic similarity:

```datalog
// Find experts: people connected to the topic through papers they authored
+expert(Person, Topic, Score) :-
    authored(Person, Paper),
    paper_embedding(Paper, Vec),
    topic_embedding(Topic, TopicVec),
    Score = cosine(Vec, TopicVec),
    Score > 0.8.

// Expand through co-authorship network
+extended_expert(Person, Topic, Score) :-
    co_author(Person, Expert),
    expert(Expert, Topic, ExpertScore),
    Score = ExpertScore * 0.7.
```

### Explainable Retrieval

Every derived fact can be traced back through the rules that produced it. When you need to answer "why did the AI see this document?", the rules ARE the explanation.

---

## How It Works

### 1. Ingest Your Data

```datalog
// Facts from your systems
+users[("alice", "engineering"), ("bob", "sales")].
+documents[(101, "Design Doc", "2024-01-15")].
+embeddings[(101, [0.9, 0.1, 0.0, ...])].
+access_rules[("engineering", 101)].
```

### 2. Define Your Logic

```datalog
// Derived views - incrementally maintained
+can_access(User, DocId) :-
    users(User, Group),
    access_rules(Group, DocId).

+search_results(User, DocId, Score) :-
    can_access(User, DocId),
    query_similarity(DocId, Score),
    Score > 0.7.
```

### 3. Query in Real-Time

```datalog
?- search_results("alice", DocId, Score).
```

When source data changes, derived views update in milliseconds.

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                   InputLayer                     │
├─────────────────────────────────────────────────┤
│  Facts     │   Rules     │  Vectors  │  Queries │
│  (data)    │   (logic)   │  (embed)  │  (API)   │
├─────────────────────────────────────────────────┤
│       Incremental Computation Engine             │
│          (Differential Dataflow)                 │
├─────────────────────────────────────────────────┤
│       Persistent Storage (Parquet + WAL)         │
└─────────────────────────────────────────────────┘
```

**Deployment options:**
- Sidecar alongside your inference service
- Shared service for multiple applications
- Local development with the same binary

---

## Key Capabilities

| Capability | Description |
|------------|-------------|
| **Vector Similarity** | Native cosine, euclidean, dot product, manhattan |
| **Recursive Queries** | Natural graph traversal with automatic fixpoint |
| **Incremental Updates** | Changes propagate in milliseconds |
| **Rule-Based Logic** | Policies and constraints as declarative rules |
| **Persistent Storage** | Durable state with write-ahead logging |
| **Lightweight** | Single binary, no cluster required |

---

## Roadmap

Building toward a complete AI knowledge layer:

- [ ] **Provenance API** - Trace any result back through the rules that derived it
- [ ] **Temporal operators** - Recency decay, time windows, versioned facts
- [ ] **Confidence propagation** - Uncertainty through rule chains
- [ ] **Context budgets** - Token-aware context assembly
- [ ] **Embedding pipeline integration** - Automatic embedding generation
- [ ] **Multi-modal support** - Text, image, audio embeddings together

---

## When to Use InputLayer

**Good fit:**
- RAG with complex retrieval logic (not just similarity search)
- Agent context assembly (tools, memory, constraints)
- Knowledge graphs with vector similarity
- Policy-filtered data access
- Any AI system where retrieval logic is getting complicated

**Use something else for:**
- Primary transactional database → Postgres, MySQL
- Simple vector search → Pinecone, pgvector
- Stream processing / analytics → Materialize, Flink
- Authentication → Auth0, Okta

InputLayer complements your existing stack—it's the reasoning layer, not the storage layer.

---

## Getting Started

### Install

```bash
cargo install inputlayer
```

### Run the REPL

```bash
inputlayer-client
```

### As a Server

```bash
inputlayer-server --host 0.0.0.0 --port 9090
```

---

## Documentation

- [Getting Started](docs/getting-started/)
- [Syntax Reference](docs/reference/syntax.md)
- [Commands Reference](docs/reference/commands.md)
- [Data Modeling Guide](docs/guide/data-modeling.md)

---

## License

- **Core**: Apache 2.0 ([LICENSE](LICENSE))
- **Enterprise**: Commercial license for SSO, HA, audit logging, SLA support

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

---

**Questions?** [GitHub Discussions](https://github.com/anthropics/inputlayer/discussions) · [Documentation](docs/)
