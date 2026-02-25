# inputlayer

Python Object-Logic Mapper (OLM) for [InputLayer](https://github.com/inputlayer/inputlayer) - the reasoning engine for AI agents.

Write Python. No query syntax required. The OLM compiles typed Python classes into InputLayer queries over WebSocket.

## Installation

```bash
pip install inputlayer
```

With pandas support:

```bash
pip install inputlayer[pandas]
```

**Requirements**: Python 3.10+, a running InputLayer server.

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

        # Define schema
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
            print(f"{emp.Name}: ${emp.Salary}")

        await il.drop_knowledge_graph("demo")

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

**Supported types**: `int`, `float`, `str`, `bool`, `Vector[N]`, `VectorInt8[N]`, `Timestamp`

### Derived Relations (Rules)

Define computed views using `Derived` and `From(...).where(...).select(...)`:

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

Filter, join, aggregate, sort - all with Python expressions:

```python
from inputlayer import count, sum_, avg

# Filter
result = await kg.query(Employee, where=lambda e: e.salary > 100000)

# Aggregation
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

## Migrations

Django-style schema versioning for production deployments. Generate numbered migration files, apply to servers, revert when needed.

```bash
# Generate migration from models
inputlayer-migrate makemigrations --models myapp.models

# Apply pending migrations
inputlayer-migrate migrate --url ws://prod:8080/ws --kg production

# Show status
inputlayer-migrate showmigrations --url ws://prod:8080/ws --kg production

# Rollback
inputlayer-migrate revert --url ws://prod:8080/ws --kg production 0001_initial
```

The autodetector diffs your current Python models against the last migration's state and generates the minimal set of operations (create/drop relations, create/drop/replace rules, create/drop indexes). Each migration file is self-contained with a full state snapshot - no need to replay history.

See the [Migrations guide](../../docs/guides/python-sdk.md#migrations) for full details and the `examples/10_migrations.py` example.

## Sync Client

For non-async code:

```python
from inputlayer import InputLayerSync

with InputLayerSync("ws://localhost:8080/ws", username="admin", password="admin") as il:
    kg = il.knowledge_graph("demo")
    await kg.define(Employee)
```

## API Reference

### `InputLayer` (async client)

| Method | Description |
|--------|-------------|
| `knowledge_graph(name)` | Get a KG handle |
| `list_knowledge_graphs()` | List all KGs |
| `drop_knowledge_graph(name)` | Drop a KG |
| `create_user(username, password, role)` | Create a user |
| `drop_user(username)` | Drop a user |
| `list_users()` | List all users |
| `create_api_key(label)` | Create an API key |
| `revoke_api_key(label)` | Revoke an API key |
| `on(event_type, ...)` | Register notification callback |

### `KnowledgeGraph`

| Method | Description |
|--------|-------------|
| `define(*relations)` | Deploy schema definitions |
| `insert(facts)` | Insert facts |
| `delete(facts, where=...)` | Delete facts |
| `query(*select, join=, where=, order_by=, limit=)` | Query the KG |
| `vector_search(relation, query_vec, k=, metric=)` | Vector similarity search |
| `define_rules(*targets)` | Deploy persistent rules |
| `list_rules()` | List all rules |
| `drop_rule(name)` | Drop a rule |
| `create_index(HnswIndex(...))` | Create HNSW index |
| `list_indexes()` | List indexes |
| `drop_index(name)` | Drop an index |
| `grant_access(username, role)` | Grant per-KG access |
| `revoke_access(username)` | Revoke per-KG access |
| `explain(*select, ...)` | Show query plan |
| `execute(datalog)` | Execute raw query |

### `Session`

| Method | Description |
|--------|-------------|
| `insert(facts)` | Insert session-scoped facts |
| `define_rules(*targets)` | Define session-scoped rules |
| `clear()` | Clear all session state |

### `ResultSet`

| Method | Description |
|--------|-------------|
| `__iter__` | Iterate typed rows |
| `__len__` | Row count |
| `first()` | First row or `None` |
| `scalar()` | Single value from first row |
| `to_dicts()` | List of dicts |
| `to_tuples()` | List of tuples |
| `to_df()` | pandas DataFrame |

### `inputlayer-migrate` CLI

| Command | Description |
|---------|-------------|
| `makemigrations --models <module>` | Generate migration from model diff |
| `migrate --url <ws> --kg <name>` | Apply pending migrations |
| `revert --url <ws> --kg <name> <target>` | Revert to a target migration |
| `showmigrations --url <ws> --kg <name>` | Show applied/pending status |

### Migration Operations

| Class | Description |
|-------|-------------|
| `CreateRelation(name, columns)` | Create a relation schema |
| `DropRelation(name, columns)` | Drop a relation (reversible) |
| `CreateRule(name, clauses)` | Create rule clauses |
| `DropRule(name, clauses)` | Drop a rule (reversible) |
| `ReplaceRule(name, old, new)` | Replace rule clauses |
| `CreateIndex(name, relation, column, ...)` | Create HNSW index |
| `DropIndex(name, relation, column, ...)` | Drop HNSW index (reversible) |
| `RunDatalog(forward, backward)` | Custom query commands |

### Aggregation Functions

`count`, `count_distinct`, `sum_`, `min_`, `max_`, `avg`, `top_k`, `top_k_threshold`, `within_radius`

### Built-in Functions

Access via `from inputlayer import functions`:

- **Distance**: `euclidean`, `cosine`, `dot`, `manhattan`
- **Vector ops**: `normalize`, `vector_add`, `vector_sub`, `vector_scale`
- **LSH**: `lsh_hash`, `lsh_hamming`, `lsh_bucket`
- **Quantization**: `quantize_int8`, `dequantize_int8`, `quantize_binary`, `hamming_binary`
- **Int8 distance**: `euclidean_int8`, `cosine_int8`, `dot_int8`, `manhattan_int8`
- **Temporal**: `now`, `timestamp_add`, `timestamp_sub`, `timestamp_diff`, `year`, `month`, `day`, `hour`, `minute`, `second`, `day_of_week`, `format_timestamp`, `parse_timestamp`, `timestamp_trunc`
- **Math**: `abs_`, `ceil`, `floor`, `round_`, `sqrt`, `pow_`, `log`, `log2`, `log10`, `exp`, `sin`, `cos`, `tan`, `min_val`, `max_val`
- **String**: `length`, `upper`, `lower`, `concat`, `substring`, `trim`, `contains`
- **Type conversion**: `to_int`, `to_float`
- **HNSW**: `hnsw_nearest`

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

## Development

```bash
cd packages/inputlayer-py
pip install -e ".[dev]"
python -m pytest tests/ -v
```

## License

Apache-2.0
