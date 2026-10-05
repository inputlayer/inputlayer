# inputlayer-client-dev

Python SDK for [InputLayer](https://github.com/inputlayer/inputlayer), the live rules engine for AI agents.

Take the rules out of your prompts: declare facts and rules as typed Python classes, and InputLayer derives every conclusion from the current facts whenever it is read. Write Python, no query syntax required: the SDK compiles your classes into IQL, InputLayer's rule language, and sends it over WebSocket. A knowledge graph (`il.knowledge_graph("support")`) is the facts and rule-derived conclusions of one domain.

**Today and next.** This package declares relations and rules (`Relation`, `Derived`, `From`), writes and deletes facts, and queries the derived views. It also subscribes to them: `kg.subscribe()` (the engine pushes each change to the agent), `kg.watch()` and `kg.on()`, described in the [Python SDK guide](../../docs/content/docs/guides/python-sdk.mdx#subscriptions), and reads or subscribes to several results at one revision: `kg.read()` and `kg.subscribe_group()` ([Subscriptions](#subscriptions)). And it has `kg.claim()` (an agent's action is recorded only while the rules allow it), built on guarded programs (`kg.program().when()`, committed whole or not at all) and `R.any()` / `~R.any()` existence checks, described in [Guarded writes and claims](../../docs/content/docs/guides/python-sdk.mdx#guarded-writes-and-claims); these are not in a release yet. The [main README](../../README.md) shows both forms side by side.

## Installation

```bash
# from the root of an InputLayer source checkout
pip install ./packages/inputlayer-py

# With extras
pip install "./packages/inputlayer-py[pandas]"      # DataFrame support
pip install "./packages/inputlayer-py[langchain]"   # LangChain integration
pip install "./packages/inputlayer-py[all]"         # everything
```

The package is not published to PyPI yet, so install it from a source checkout.

Requirements: Python 3.10+ and a running InputLayer server.

This also installs the `inputlayer-migrate` tool for schema migrations; with the InputLayer product CLI installed, use it as `il migration <verb>`.

## Quick Start

Start a server and export its admin API key as `INPUTLAYER_API_KEY` ([Get Started](../../README.md#get-started) shows both in three commands), then run:

<!-- quickstart:python (run by .github/workflows/quickstart.yml) -->
```python
import asyncio
import os

from inputlayer import InputLayer, Relation

class Employee(Relation):
    id: int
    name: str
    department: str
    salary: float
    active: bool

async def main():
    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        api_key=os.environ["INPUTLAYER_API_KEY"],
    ) as il:
        kg = il.knowledge_graph("demo")

        # Define schema (idempotent)
        await kg.define(Employee)

        # Insert data
        await kg.insert([
            Employee(id=1, name="Alice", department="eng", salary=120000.0, active=True),
            Employee(id=2, name="Bob", department="hr", salary=90000.0, active=True),
            Employee(id=3, name="Charlie", department="eng", salary=110000.0, active=False),
        ])

        # Query with filter
        engineers = await kg.query(
            Employee,
            where=lambda e: (e.department == "eng") & (e.active == True),
        )
        for emp in engineers:
            print(f"{emp.name}: ${emp.salary}")

asyncio.run(main())
```

Prints:

<!-- quickstart:python-output -->
```
Alice: $120000.0
```

## Core Concepts

### Relations

Define typed schemas as Python classes. Each `Relation` subclass maps to an InputLayer relation.

```python
from inputlayer import Relation, Vector, Timestamp

class Document(Relation):
    id: int
    title: str
    embedding: Vector[384]
    created_at: Timestamp
```

Supported types: `int`, `float`, `str`, `bool`, `Vector[N]`, `VectorInt8[N]`, `Timestamp`, `datetime` (both stored as Unix milliseconds)

### Derived Relations (Rules)

Define computed views using `Derived` and the `From(...).where(...).select(...)` builder. Derived data is computed from the current facts on every query, so a read always reflects the latest writes.

```python
from typing import ClassVar
from inputlayer import Derived, From, Relation

class Edge(Relation):
    src: int
    dst: int

class Reachable(Derived):
    src: int
    dst: int
    rules: ClassVar[list] = []

Reachable.rules = [
    # Base case: direct edges
    From(Edge).select(src=Edge.src, dst=Edge.dst),
    # Recursive: transitive closure
    From(Reachable, Edge)
        .where(lambda r, e: r.dst == e.src)
        .select(src=Reachable.src, dst=Edge.dst),
]
```

### Queries

Filter, join, aggregate, and sort - all with Python expressions:

```python
from inputlayer import count, sum_, avg

# Filter
result = await kg.query(Employee, where=lambda e: e.salary > 100000)

# Aggregation by group
result = await kg.query(
    Employee.department,
    count(Employee.id),
    avg(Employee.salary),
    join=[Employee],
)

# Order + limit
result = await kg.query(
    Employee,
    order_by=Employee.salary.desc(),
    limit=10,
)
```

### Vector Search

```python
from inputlayer import HnswIndex

await kg.create_index(HnswIndex(
    name="doc_emb_idx",
    relation=Document,
    column="embedding",
    metric="cosine",
))

result = await kg.vector_search(
    Document,
    query_vec=[0.1, 0.2, ...],
    k=10,
    metric="cosine",
)
```

### Session Rules

Ephemeral views that exist only for the current connection:

```python
await kg.session.define_rules(ActiveEngineer)
result = await kg.query(ActiveEngineer, join=[ActiveEngineer])
await kg.session.clear()
```

### DataFrames

Load from and export to pandas:

```python
import pandas as pd

df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "score": [95.0, 87.0]})
await kg.insert(Student, data=df)

result = await kg.query(Student)
export_df = result.to_df()
```

### Notifications

Subscribe to live data change events:

```python
@il.on("persistent_update", relation="sensor_reading")
def on_update(event):
    print(f"{event.count} new readings")
```

### Subscriptions

`kg.subscribe()` keeps one query's result exact on the client: a `snapshot`, then a `delta` per change, with `unverified` and `resync` around anything that broke the stream (a lost connection, a gap, a slow consumer). The [Python SDK guide](../../docs/content/docs/guides/python-sdk.mdx#subscriptions) covers it in full.

```python
async for change in kg.subscribe(Late):
    for row in change.retracted:
        cancel(row)
    for row in change.inserted:
        start(row)
```

When an agent decides on several results together, read or subscribe to them together. `kg.read()` runs several queries on one snapshot; `kg.subscribe_group()` keeps several queries current as one subscription. Both take a mapping from a name to a target: a relation class, a column, a tuple of `query()` arguments, a dict of them (`{"select": [Order.id], "where": ...}`), or raw IQL (`"?eta(O, T)"`).

```python
snap = await kg.read({"orders": Order, "eta": Eta})
snap.revision, snap.results["orders"], snap.results["eta"]

async for change in kg.subscribe_group({"orders": Order, "eta": Eta}):
    orders, eta = change.members["orders"], change.members["eta"]
    if eta.unchanged:
        ...  # only orders moved
```

What they promise:

- **One revision per answer.** Every result of a read is its query's exact answer at `snap.revision`, whatever commits meanwhile. After every verified group event (`snapshot`, `delta`, `resync`), every member, with all earlier events applied, is its query's exact answer at `change.revision`. A program that changes two members' results shows up in both in the same event, never in one first.
- **Every member in every event.** A `delta` lists all members, each `unchanged` when its rows did not move (`inserted` and `retracted` empty). A refresh that changes no member's rows (only what a member's projection leaves out, say) delivers nothing. `unverified` and `resync` cover the whole group: `resync` gives each member the exact difference between the rows held and the fresh snapshot.
- **Persistent data only.** Like a subscription, a read and a group see persistent facts and rules, not the session's: a target that reads a session rule raises `SubscriptionRejected` (`session_view`), and so does a target that is not one `?` query (an OR condition, an aggregate, a negated constant), naming the query; define a persistent rule and read or subscribe to that. A read keeps `order_by`, `limit` and `offset` (`offset` with `limit`, and not on a page of a projection) and lists the results a limit or the engine's result cap cut in `snap.truncated`; a group, like a subscription, tracks whole results.

What they do not promise: commits coalesce. A group delta reflects the latest revision when the engine refreshes, so a row that appears and disappears between two refreshes is never seen, and two consecutive events can be several revisions apart. What must not be missed belongs in facts. Revisions order states of one knowledge graph within one engine run; two groups or two subscriptions do not share events.

## LangChain Integration

```bash
pip install "./packages/inputlayer-py[langchain]"
```

The full integration guide lives at [docs/guides/langchain](../../docs/content/docs/guides/langchain.mdx). Highlights:

### Vector store

Drop-in `langchain_core.vectorstores.VectorStore` backed by an InputLayer `Relation`. Embeds documents through any LangChain `Embeddings` instance, supports metadata filters, deletion, and `as_retriever()`:

```python
from inputlayer import Relation, Vector
from inputlayer.integrations.langchain import InputLayerVectorStore
from langchain_openai import OpenAIEmbeddings

class Chunk(Relation):
    id: str
    content: str
    source: str
    embedding: Vector

await kg.define(Chunk)
vs = InputLayerVectorStore(kg=kg, relation=Chunk, embeddings=OpenAIEmbeddings())
await vs.aadd_texts(["..."], metadatas=[{"source": "wiki"}], ids=["doc1"])
docs = await vs.asimilarity_search("query", k=5, filter={"source": "wiki"})
```

### Retriever

`InputLayerRetriever` runs in vector mode (with an `Embeddings` instance) or in InputLayer Query Language mode with safe `:input` parameter binding:

```python
from inputlayer.integrations.langchain import InputLayerRetriever

retriever = InputLayerRetriever(
    kg=kg,
    query="?article(I, T, C, Cat, E), user_interest(:input, Cat)",
    page_content_columns=["content"],
    metadata_columns=["title", "category"],
)
result = await retriever.ainvoke("alice")
```

### Structured agent tools

`tools_from_relations` generates one `StructuredTool` per `Relation` with typed equality, range, and IN-list filters. The LLM never has to write IQL:

```python
from inputlayer.integrations.langchain import tools_from_relations
tools = tools_from_relations(kg, [Employee, Article])
agent = create_tool_calling_agent(llm, tools, prompt)
```

For agents that genuinely need raw IQL access, `InputLayerIQLTool` is the escape hatch; it is read-only by default (queries, `.why` and `.why_not` only) and runs writes only with `read_only=False`. All components support both sync (`invoke`) and async (`ainvoke`) and are safe to use inside Jupyter, FastAPI, and LangGraph.

### Examples

See [`examples/langchain/`](examples/langchain/) — 17 examples covering AI/LLM integration patterns:

```bash
# List all examples
uv run python -m examples.langchain.runner --list

# Run specific examples
uv run python -m examples.langchain.runner 1 3 9

# Run a range
uv run python -m examples.langchain.runner 1-5

# Run all
uv run python -m examples.langchain.runner --all
```

| # | Example | Description |
|---|---------|-------------|
| 1 | Retriever + IQL | Join queries with `{input}` placeholder |
| 2 | Vector search | Cosine similarity with distance filter |
| 3 | Tool for agents | Raw IQL + template mode |
| 4 | LCEL chain | Full retriever \| prompt \| llm \| parser pipeline |
| 5 | KG building | Extract facts from documents with LLM |
| 6 | Explainable RAG | `.why()` proof trees + `.why_not()` explanations |
| 7 | Multi-hop reasoning | Transitive closure over org graph |
| 8 | Conversational memory | Chat turns as facts, rules derive context |
| 9 | Access-controlled RAG | Clearance-based document filtering via rules |
| 10 | Multi-agent | Researcher + fact-checker with shared KG |
| 11 | Anomaly detection | Salary band rules flag violations |
| 12 | Hallucination detection | Ground LLM claims against KG facts |
| 13 | Guardrails | Policy rules block unsafe content |
| 14 | GraphRAG | Entity extraction + community detection |
| 15 | Semantic caching | Cache LLM responses, topic-based matching |
| 16 | Recommendation engine | Collaborative filtering via IQL rules |
| 17 | Data lineage | Source attribution + conflict detection |

Requires a running InputLayer server and optionally LM Studio (or any OpenAI-compatible server) for LLM examples.

## LangGraph Integration

Install the langgraph extra:

```bash
pip install "./packages/inputlayer-py[langgraph]"
```

The LangGraph integration provides:

- **`InputLayerCheckpointer`**: Persist graph state in an InputLayer KG. Supports `prune_thread()` / `adelete_thread()` for storage management and full async/sync parity.
- **`InputLayerMemory`**: Semantic long-term memory. Stores conversation turns as facts, derives active topics and relevant context via rules. Supports `adelete_thread()` for thread cleanup.
- **`kg_node`**: Factory for query/insert/delete graph nodes.
- **`kg_router`**: Conditional edge routing driven by IQL queries.
- **`InputLayerState`**: TypedDict base class with the required `kg` field for graph state.
- **`escape_iql`**: String escaping for safe IQL interpolation in parameterized queries.

```python
from inputlayer import InputLayer
from inputlayer.integrations.langgraph import (
    InputLayerCheckpointer,
    InputLayerMemory,
    InputLayerState,
    escape_iql,
    kg_node,
    kg_router,
)
from langgraph.graph import END, StateGraph

class MyState(InputLayerState):
    question: str
    answer: str

async with InputLayer("ws://localhost:8080/ws", username="admin", password="...") as il:
    kg = il.knowledge_graph("my_agent")

    # Checkpointer: persist graph state across process restarts
    checkpointer = InputLayerCheckpointer(kg=kg)
    await checkpointer.setup()

    # Memory: semantic recall with rule-derived context
    memory = InputLayerMemory(kg=kg)
    await memory.setup()

    # Build a graph with KG-driven nodes and routing
    graph = StateGraph(MyState)
    graph.add_node("search", kg_node(query="?relevant(X, Y)", state_key="results"))
    graph.add_node("recall", memory.recall_node(state_key="context"))
    graph.add_node("store", memory.store_node(state_key="new_message"))
    graph.set_entry_point("recall")
    graph.add_edge("recall", "search")
    graph.add_edge("search", "store")
    graph.add_edge("store", END)

    app = graph.compile(checkpointer=checkpointer)
```

### LangGraph Examples

See [`examples/langgraph/`](examples/langgraph/) - 12 examples covering agent patterns:

```bash
# List all examples
uv run python -m examples.langgraph.runner --list

# Run specific examples
uv run python -m examples.langgraph.runner 1 10 11

# Run a range
uv run python -m examples.langgraph.runner 1-5

# Run all
uv run python -m examples.langgraph.runner --all
```

| # | Example | Description |
|---|---------|-------------|
| 1 | Reasoning loop | Accumulate facts, rules decide when to stop |
| 2 | Investigation | Multi-step evidence gathering |
| 3 | Human-in-the-loop | Policy rules gate actions for approval |
| 4 | Branching pipeline | Route documents through parallel analysis |
| 5 | Self-correcting agent | Validation rules catch and fix errors |
| 6 | Collaborative planning | Multi-agent task decomposition |
| 7 | Event correlation | Pattern detection across event streams |
| 8 | Tool selection | Rules pick the right tool per context |
| 9 | Streaming aggregation | Threshold-based alerts from streaming data |
| 10 | Resumable graph | Checkpoint, crash, resume from persisted state |
| 11 | Semantic memory | Store turns as facts, recall derived context |
| 12 | Resumable chat | Checkpointer and memory together on one KG |

Requires a running InputLayer server. Examples marked [LLM] need LM Studio (or any OpenAI-compatible server) at `localhost:1234`. Set `INPUTLAYER_URL`, `INPUTLAYER_USER`, `INPUTLAYER_PASSWORD` to override server defaults.

## Sync Client

For scripts, notebooks, and non-async contexts:

```python
import os
from inputlayer import InputLayerSync

with InputLayerSync("ws://localhost:8080/ws", api_key=os.environ["INPUTLAYER_API_KEY"]) as il:
    kg = il.knowledge_graph("demo")
    kg.define(Employee)
    kg.insert(Employee(id=1, name="Alice", department="eng", salary=120000.0, active=True))
    result = kg.query(Employee)
```

## Migrations

The SDK includes a Django-style migration system for production schema management. The `il` CLI is installed with the package.

```bash
# Generate a migration from your models
il migration generate --models myapp.models

# Apply pending migrations
il migration apply --url ws://localhost:8080/ws --kg production

# Check status
il migration status --url ws://localhost:8080/ws --kg production

# Rollback
il migration revert --url ws://localhost:8080/ws --kg production 0001_initial
```

The autodetector diffs your current Python models against the last migration's state and generates the minimal set of operations (create/drop relations, create/drop/replace rules, create/drop indexes). Each migration file is self-contained with a full state snapshot.

## API Reference

### `InputLayer` / `InputLayerSync`

| Method | Description |
|--------|-------------|
| `knowledge_graph(name)` | Get or create a knowledge graph handle |
| `list_knowledge_graphs()` | List all knowledge graphs |
| `drop_knowledge_graph(name)` | Drop a knowledge graph |
| `create_user(username, password, role)` | Create a user |
| `drop_user(username)` | Drop a user |
| `set_role(username, role)` | Change a user's role |
| `set_password(username, password)` | Change a user's password |
| `list_users()` | List all users |
| `create_api_key(label, ttl=None)` | Create an API key, optionally expiring after `ttl` (e.g. `"90d"`) |
| `list_api_keys()` | List API keys with owner, created/expires/last-used times and status |
| `expire_api_key(label, ttl)` | Bring a key's expiry forward (rotation grace period) |
| `revoke_api_key(label)` | Revoke an API key |
| `on(event_type, ...)` | Register notification callback |
| `notifications()` | Async iterator over events |

### `KnowledgeGraph` / `KnowledgeGraphSync`

| Method | Description |
|--------|-------------|
| `define(*relations)` | Deploy schema definitions (idempotent) |
| `relations()` | List all relations |
| `describe(relation)` | Describe a relation's schema |
| `drop_relation(relation)` | Drop a relation |
| `insert(facts, data=None)` | Insert facts (objects, dicts, or DataFrame) |
| `delete(facts, where=None)` | Delete facts |
| `query(*select, join=, where=, order_by=, limit=, offset=)` | Query the knowledge graph |
| `vector_search(relation, query_vec, k=, radius=, metric=, where=)` | Vector similarity search |
| `define_rules(*targets)` | Deploy persistent rules |
| `list_rules()` | List all rules |
| `rule_definition(name)` | Get compiled rule clauses |
| `drop_rule(name)` | Drop a rule |
| `clear_rule(name)` | Remove every clause of a rule |
| `create_index(HnswIndex(...))` | Create HNSW index |
| `list_indexes()` | List indexes |
| `index_stats(name)` | Get index statistics |
| `drop_index(name)` | Drop an index |
| `rebuild_index(name)` | Rebuild an index |
| `grant_access(username, role)` | Grant per-KG access |
| `revoke_access(username)` | Revoke per-KG access |
| `list_acl()` | List access control entries |
| `debug(*select, ...)` | Show query plan without executing (same arguments as `query`) |
| `execute(iql)` | Execute raw IQL |
| `subscribe(*select, ..., queue=1024)` | Async iterator of `Change` events (snapshot, then deltas) |
| `subscribe_group({name: target}, queue=1024)` | Async iterator of `GroupChange` events: several queries kept exact at one revision per event |
| `read({name: target}, timeout=None)` | Several queries answered on one snapshot: `ReadResult(revision, results, truncated)` (`truncated`: names of cut results) |
| `watch(*select, ...)` | Async iterator of the whole current result (`Live`) |
| `on(*select, callback)` | Call `callback` with every `Change`; returns a handle with `close()` |
| `status()` | Get server status |
| `compact()` | Trigger storage compaction |

### `Session`

| Method | Description |
|--------|-------------|
| `insert(facts)` | Insert session-scoped facts |
| `define_rules(*targets)` | Define session-scoped rules |
| `list_rules()` | List session rules |
| `drop_rule(name)` | Drop a session rule |
| `clear()` | Clear all session state |

### `ResultSet`

| Method/Property | Description |
|--------|-------------|
| `__iter__` | Iterate as typed objects |
| `__len__` | Row count |
| `first()` | First row or `None` |
| `scalar()` | Single value from 1x1 result |
| `to_dicts()` | List of dicts |
| `to_tuples()` | List of tuples |
| `to_df()` | pandas DataFrame |
| `row_count` | Number of rows returned |
| `total_count` | Total count (with limit/offset) |
| `execution_time_ms` | Query execution time |
| `truncated` | Whether results were truncated |

### `il` CLI

| Command | Description |
|---------|-------------|
| `il migration generate --models <module>` | Generate migration from model diff |
| `il migration apply --url <ws> --kg <name>` | Apply pending migrations |
| `il migration revert --url <ws> --kg <name> <target>` | Revert to a target migration |
| `il migration status --url <ws> --kg <name>` | Show applied/pending status |

### Aggregation Functions

`count`, `count_distinct`, `sum_`, `min_`, `max_`, `avg`, `top_k`, `top_k_threshold`, `within_radius`

### Built-in Functions

Access via `from inputlayer import functions as fn`:

- **Distance**: `fn.cosine`, `fn.euclidean`, `fn.dot`, `fn.manhattan`
- **Vector ops**: `fn.normalize`, `fn.vec_dim`, `fn.vec_add`, `fn.vec_scale`
- **Int8 distance**: `fn.cosine_int8`, `fn.euclidean_int8`, `fn.dot_int8`, `fn.manhattan_int8`
- **Quantization**: `fn.quantize_linear`, `fn.quantize_symmetric`, `fn.dequantize`, `fn.dequantize_scaled`
- **LSH**: `fn.lsh_bucket`, `fn.lsh_probes`, `fn.lsh_multi_probe`
- **Temporal**: `fn.time_now`, `fn.time_diff`, `fn.time_add`, `fn.time_sub`, `fn.time_decay`, `fn.time_decay_linear`, `fn.time_before`, `fn.time_after`, `fn.time_between`, `fn.within_last`, `fn.intervals_overlap`, `fn.interval_contains`, `fn.interval_duration`, `fn.point_in_interval`
- **Math**: `fn.abs_`, `fn.sqrt`, `fn.pow_`, `fn.log`, `fn.exp`, `fn.sin`, `fn.cos`, `fn.tan`, `fn.floor`, `fn.ceil`, `fn.sign`, `fn.min_val`, `fn.max_val`
- **String**: `fn.len_`, `fn.upper`, `fn.lower`, `fn.trim`, `fn.substr`, `fn.replace`, `fn.concat`
- **Type conversion**: `fn.to_int`, `fn.to_float`
- **HNSW**: `fn.hnsw_nearest`

## Examples

See the [`examples/`](examples/) directory:

| Example | Description |
|---------|-------------|
| `01_quickstart.py` | Basic connect, define, insert, query |
| `02_social_network.py` | Graph traversal, transitive closure, mutual follows |
| `03_rag_pipeline.py` | Vector + structured hybrid search |
| `04_ecommerce.py` | Collaborative filtering, revenue aggregation |
| `05_rbac.py` | Transitive role inheritance |
| `06_realtime_dashboard.py` | Notifications + aggregation |
| `07_dataframe_etl.py` | Pandas DataFrame load/export |
| `08_session_rules.py` | Ad-hoc ephemeral views |
| `09_access_control.py` | User/ACL management |
| `10_migrations.py` | Django-style schema versioning |
| `langchain/` | 17 LangChain integration examples (see above) |

## Development

```bash
cd packages/inputlayer-py
uv sync --extra dev
uv run pytest tests/ -v
uv run ruff check src/ tests/
uv run mypy src/inputlayer/
```

## License

Apache 2.0. See [LICENSE](./LICENSE). (The InputLayer core server is separately licensed under the Elastic License 2.0.)
