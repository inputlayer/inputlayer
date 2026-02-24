# Python SDK

InputLayer ships a Python Object-Logic Mapper (OLM) that lets you define schemas, insert data, run queries, and manage rules using pure Python - no Datalog syntax required. The OLM compiles Python expressions into Datalog over WebSocket.

## Installation

```bash
pip install inputlayer

# With pandas support
pip install inputlayer[pandas]
```

**Requirements**: Python 3.10+, a running InputLayer server.

## Connecting

```python
import asyncio
from inputlayer import InputLayer

async def main():
    async with InputLayer("ws://localhost:8080/ws", username="admin", password="admin") as il:
        print(f"Connected, server version: {il.server_version}")

asyncio.run(main())
```

Authentication supports username/password or API keys:

```python
# API key auth
async with InputLayer("ws://localhost:8080/ws", api_key="your-key-here") as il:
    ...
```

## Defining Schemas

Define typed relations as Python classes:

```python
from inputlayer import Relation, Vector, Timestamp

class Employee(Relation):
    id: int
    name: str
    department: str
    salary: float
    active: bool

class Document(Relation):
    id: int
    title: str
    embedding: Vector[384]
    created_at: Timestamp
```

Deploy to the server:

```python
kg = il.knowledge_graph("myapp")
await kg.define(Employee, Document)
```

`define()` is idempotent - calling it again on an existing relation is safe.

### Supported Types

| Python Type | Datalog Type | Description |
|-------------|-------------|-------------|
| `int` | `int` | 64-bit integer |
| `float` | `float` | 64-bit floating point |
| `str` | `string` | UTF-8 string |
| `bool` | `bool` | Boolean |
| `Vector[N]` | `vector(N)` | N-dimensional float vector |
| `VectorInt8[N]` | `vector_int8(N)` | N-dimensional int8 vector |
| `Timestamp` | `timestamp` | Unix epoch microseconds |

## Inserting Data

```python
# Single fact
await kg.insert(Employee(id=1, name="Alice", department="eng", salary=120000.0, active=True))

# Batch insert
await kg.insert([
    Employee(id=2, name="Bob", department="hr", salary=90000.0, active=True),
    Employee(id=3, name="Charlie", department="eng", salary=110000.0, active=False),
])

# From a pandas DataFrame
import pandas as pd
df = pd.DataFrame({"id": [4, 5], "name": ["Dave", "Eve"], ...})
await kg.insert(Employee, data=df)
```

## Querying

### Basic Queries

```python
# All employees
result = await kg.query(Employee)
for emp in result:
    print(f"{emp.Name}: ${emp.Salary}")

# With filter
engineers = await kg.query(
    Employee,
    where=lambda e: (e.department == "eng") & (e.active == True),
)
```

### Column Selection

```python
result = await kg.query(
    Employee.name, Employee.salary,
    join=[Employee],
    where=lambda e: e.department == "eng",
)
```

### Joins

```python
class Department(Relation):
    name: str
    budget: float

result = await kg.query(
    Employee.name, Department.budget,
    join=[Employee, Department],
    on=lambda e, d: e.department == d.name,
)
```

### Aggregations

```python
from inputlayer import count, sum_, avg, min_, max_

result = await kg.query(
    Employee.department,
    count(Employee.id),
    avg(Employee.salary),
    join=[Employee],
)
```

### Ordering and Pagination

```python
result = await kg.query(
    Employee,
    order_by=Employee.salary.desc(),
    limit=10,
    offset=20,
)
```

## Derived Relations (Rules)

Define computed views with recursive logic:

```python
from typing import ClassVar
from inputlayer import Derived, From

class Edge(Relation):
    src: int
    dst: int

class Reachable(Derived):
    src: int
    dst: int
    rules: ClassVar[list] = []

# Base case + recursive case
Reachable.rules = [
    From(Edge).select(src=Edge.src, dst=Edge.dst),
    From(Reachable, Edge)
        .where(lambda r, e: r.dst == e.src)
        .select(src=Reachable.src, dst=Edge.dst),
]

# Deploy and query
await kg.define_rules(Reachable)
result = await kg.query(Reachable, where=lambda r: r.src == 1)
```

## Vector Search

```python
from inputlayer import HnswIndex

# Create index
await kg.create_index(HnswIndex(
    relation="document",
    column="embedding",
    metric="cosine",
    ef_construction=200,
    max_neighbors=32,
))

# Search
result = await kg.vector_search(
    Document,
    query_vec=[0.1, 0.2, ...],
    k=10,
    metric="cosine",
)
```

## Session Rules

Ephemeral rules scoped to the current WebSocket connection:

```python
await kg.session.define_rules(MyTempView)
result = await kg.query(MyTempView, join=[MyTempView])
await kg.session.clear()  # or just disconnect
```

## Notifications

Subscribe to real-time data change events:

```python
@il.on("persistent_update", relation="sensor_reading")
def on_update(event):
    print(f"[{event.relation}] {event.count} rows changed")
```

## Migrations

The migration system provides Django-style schema versioning: generate numbered migration files from model diffs, apply them to a server, revert if needed, and track what's deployed.

### Why Migrations?

Without migrations, `define()` and `define_rules()` fire Datalog commands blindly:
- No idempotency - calling `define_rules()` twice duplicates clauses
- No rollback - can't revert a bad deploy
- No state tracking - no record of what's deployed

Migrations solve all of these.

### Workflow

**1. Define models** in your project (relations, derived, indexes as shown above).

**2. Generate a migration** from the current model state:

```bash
inputlayer-migrate makemigrations --models myapp.models
# → creates migrations/0001_initial.py
```

**3. Apply** on deploy:

```bash
inputlayer-migrate migrate --url ws://prod:8080/ws --kg production
```

**4. Iterate** - change models, generate a new migration that diffs from the previous state:

```bash
inputlayer-migrate makemigrations --models myapp.models
# → creates migrations/0002_auto.py
inputlayer-migrate migrate --url ws://prod:8080/ws --kg production
```

**5. Rollback** if broken:

```bash
inputlayer-migrate revert --url ws://prod:8080/ws --kg production 0001_initial
```

**6. Check status**:

```bash
inputlayer-migrate showmigrations --url ws://prod:8080/ws --kg production
# [X] 0001_initial
# [ ] 0002_auto
```

### Migration Files

Each migration is a self-contained Python file with operations and a full state snapshot:

```python
# migrations/0001_initial.py
from inputlayer.migrations import Migration
from inputlayer.migrations import operations as ops

class M(Migration):
    dependencies = []

    operations = [
        ops.CreateRelation(
            name="employee",
            columns=[("id", "int"), ("name", "string"), ("salary", "float")],
        ),
        ops.CreateRule(
            name="senior",
            clauses=["+senior(Name) <- employee(_, Name, Salary), Salary > 100000"],
        ),
    ]

    state = {
        "relations": {
            "employee": [("id", "int"), ("name", "string"), ("salary", "float")],
        },
        "rules": {
            "senior": ["+senior(Name) <- employee(_, Name, Salary), Salary > 100000"],
        },
        "indexes": {},
    }
```

### Operations

| Operation | Forward | Backward |
|-----------|---------|----------|
| `CreateRelation(name, columns)` | Schema definition | `.rel drop` |
| `DropRelation(name, columns)` | `.rel drop` | Schema definition |
| `CreateRule(name, clauses)` | Rule clauses | `.rule drop` |
| `DropRule(name, clauses)` | `.rule drop` | Rule clauses |
| `ReplaceRule(name, old, new)` | Drop + new clauses | Drop + old clauses |
| `CreateIndex(name, ...)` | `.index create` | `.index drop` |
| `DropIndex(name, ...)` | `.index drop` | `.index create` |
| `RunDatalog(fwd, bwd)` | Custom commands | Custom commands |

### Programmatic Usage

You can also use the migration system from Python code:

```python
from inputlayer.migrations.autodetector import detect_changes
from inputlayer.migrations.state import ModelState
from inputlayer.migrations.writer import generate_migration

# Build state from current models
state = ModelState.from_models(
    relations=[Employee, Department],
    derived=[Reachable],
    indexes=[doc_idx],
)

# Diff against empty → initial migration
ops = detect_changes(ModelState(), state)
filename, content = generate_migration(1, ops, state.to_dict(), [])
```

## Error Handling

```python
from inputlayer import (
    InputLayerError,
    AuthenticationError,
    KnowledgeGraphNotFoundError,
    SchemaConflictError,
)

try:
    await kg.define(Employee)
except AuthenticationError:
    print("Bad credentials")
except SchemaConflictError as e:
    print(f"Schema mismatch: {e.conflicts}")
except InputLayerError as e:
    print(f"Server error: {e}")
```

## Sync Client

For scripts and non-async contexts:

```python
from inputlayer import InputLayerSync

with InputLayerSync("ws://localhost:8080/ws", username="admin", password="admin") as il:
    kg = il.knowledge_graph("demo")
    kg.define(Employee)
    kg.insert(Employee(id=1, name="Alice", department="eng", salary=120000.0, active=True))
    result = kg.query(Employee)
```

## Examples

See [`packages/inputlayer-py/examples/`](../../packages/inputlayer-py/examples/) for complete runnable examples covering social networks, RAG pipelines, e-commerce, RBAC, real-time dashboards, DataFrame ETL, session rules, and access control.

## API Reference

Full API reference: [`packages/inputlayer-py/README.md`](../../packages/inputlayer-py/README.md)
