# inputlayer-client-dev

Python Object-Logic Mapper (OLM) for [InputLayer](https://github.com/inputlayer/inputlayer) - the streaming reasoning layer for AI systems.

Write Python. No query syntax required. The OLM compiles typed Python classes into InputLayer queries over WebSocket.

## Installation

```bash
pip install inputlayer-client-dev

# With extras
pip install inputlayer-client-dev[pandas]      # DataFrame support
pip install inputlayer-client-dev[langchain]   # LangChain integration
pip install inputlayer-client-dev[all]         # everything
```

Requirements: Python 3.10+ and a running InputLayer server.

This also installs the `il` CLI for managing schema migrations.

## Quick Start

```python
import asyncio
from inputlayer import InputLayer, Relation

class Employee(Relation):
    id: int
    name: str
    department: str
    salary: float
    active: bool

async def main():
    async with InputLayer("ws://localhost:8080/ws", username="admin", password="admin") as il:
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

Supported types: `int`, `float`, `str`, `bool`, `Vector[N]`, `VectorInt8[N]`, `Timestamp`

### Derived Relations (Rules)

Define computed views using `Derived` and the `From(...).where(...).select(...)` builder. InputLayer keeps derived data up to date automatically when the underlying facts change.

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

Subscribe to real-time data change events:

```python
@il.on("persistent_update", relation="sensor_reading")
def on_update(event):
    print(f"{event.count} new readings")
```

## LangChain Integration

```bash
pip install inputlayer-client-dev[langchain]
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

For agents that genuinely need raw IQL access, `InputLayerIQLTool` is the escape hatch. All components support both sync (`invoke`) and async (`ainvoke`) and are safe to use inside Jupyter, FastAPI, and LangGraph.

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

## Sync Client

For scripts, notebooks, and non-async contexts:

```python
from inputlayer import InputLayerSync

with InputLayerSync("ws://localhost:8080/ws", username="admin", password="admin") as il:
    kg = il.knowledge_graph("demo")
    kg.define(Employee)
    kg.insert(Employee(id=1, name="Alice", department="eng", salary=120000.0, active=True))
    result = kg.query(Employee)
```

## Migrations

The SDK includes a Django-style migration system for production schema management. The `il` CLI is installed with the package.

```bash
# Generate a migration from your models
il makemigrations --models myapp.models

# Apply pending migrations
il migrate --url ws://localhost:8080/ws --kg production

# Check status
il showmigrations --url ws://localhost:8080/ws --kg production

# Rollback
il revert --url ws://localhost:8080/ws --kg production 0001_initial
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
| `create_api_key(label)` | Create an API key |
| `list_api_keys()` | List active API keys |
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
| `clear_rule(name)` | Clear materialized rule data |
| `create_index(HnswIndex(...))` | Create HNSW index |
| `list_indexes()` | List indexes |
| `index_stats(name)` | Get index statistics |
| `drop_index(name)` | Drop an index |
| `rebuild_index(name)` | Rebuild an index |
| `grant_access(username, role)` | Grant per-KG access |
| `revoke_access(username)` | Revoke per-KG access |
| `list_acl()` | List access control entries |
| `explain(*select, ...)` | Show query plan without executing |
| `execute(iql)` | Execute raw IQL |
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
| `il makemigrations --models <module>` | Generate migration from model diff |
| `il migrate --url <ws> --kg <name>` | Apply pending migrations |
| `il revert --url <ws> --kg <name> <target>` | Revert to a target migration |
| `il showmigrations --url <ws> --kg <name>` | Show applied/pending status |

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

Apache 2.0 + Commons Clause. See [LICENSE](../../LICENSE).
