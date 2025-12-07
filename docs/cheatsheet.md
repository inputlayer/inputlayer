# InputLayer Datalog Cheatsheet

A unified Datalog-native syntax for InputLayer, designed for intuitive data manipulation and querying.

## Quick Reference

| Operator | Meaning | Persisted | DD Semantics |
|----------|---------|-----------|--------------|
| `+` | Insert | Yes | diff = +1 |
| `-` | Delete | Yes | diff = -1 |
| `:=` | Define view | Yes | Incremental maintenance |
| `:-` | Transient rule | No | Ad-hoc computation |
| `?-` | Query | No | Ad-hoc query |

## Meta Commands

Meta commands start with `.` and control the system:

```
.db                  Show current database
.db list             List all databases
.db create <name>    Create database
.db use <name>       Switch to database
.db drop <name>      Drop database (cannot drop current)

.rel                 List relations (base facts)
.rel <name>          Describe relation schema

.view                List persistent views
.view <name>         Query view (show computed data from DD)
.view def <name>     Show view definition (rules)
.view drop <name>    Drop view

.compact             Compact WAL and consolidate batch files
.status              Show system status
.help                Show this help
.quit                Exit client
```

## Data Manipulation

### Insert Facts (`+`)

Single fact:
```datalog
+edge(1, 2).
```

Bulk insert:
```datalog
+edge[(1, 2), (2, 3), (3, 4)].
```

### Delete Facts (`-`)

Single fact:
```datalog
-edge(1, 2).
```

Conditional delete (query-based):
```datalog
-edge(X, Y) :- X > 5.
```

### Atomic Updates

Delete and insert in one atomic operation:
```datalog
-person(X, OldAge), +person(X, NewAge) :- person(X, OldAge), NewAge = OldAge + 1.
```

## Persistent Views (`:=`)

Views are persistent rules that are incrementally maintained by Differential Dataflow.

Simple view:
```datalog
path(X, Y) := edge(X, Y).
```

Recursive view (transitive closure):
```datalog
path(X, Y) := edge(X, Y).
path(X, Z) := path(X, Y), edge(Y, Z).
```

View with filter:
```datalog
adult(Name, Age) := person(Name, Age), Age >= 18.
```

Views are saved to `{db_dir}/views/catalog.json` and automatically loaded on database startup.

## Transient Rules (`:-`)

Transient rules are executed immediately but not persisted:

```datalog
result(X, Y) :- edge(X, Y), X < Y.
```

Transient rules can use views:
```datalog
reachable(X) :- path(1, X).
```

## Queries (`?-`)

Query a relation or view:

Simple query:
```datalog
?- edge(1, X).
```

Query with constraints:
```datalog
?- person(Name, Age), Age > 30.
```

Query a view:
```datalog
?- path(1, X).
```

## Examples

### Graph Database

```datalog
-- Create database
.db create social

-- Add edges
+follows[(1, 2), (2, 3), (3, 4), (1, 4)].

-- Define reachability view
reach(X, Y) := follows(X, Y).
reach(X, Z) := reach(X, Y), follows(Y, Z).

-- Query who user 1 can reach
?- reach(1, X).

-- Save to disk
.save
```

### Person Database

```datalog
-- Create database
.db create hr

-- Add people
+person[(1, 25), (2, 30), (3, 45), (4, 22)].

-- Define view for adults
adult(Id, Age) := person(Id, Age), Age >= 21.

-- Query adults
?- adult(Id, Age).

-- Age everyone by 1 year (atomic update)
-person(X, OldAge), +person(X, NewAge) :- person(X, OldAge), NewAge = OldAge + 1.
```

## Differential Dataflow Semantics

InputLayer is built on Differential Dataflow (DD), which uses a diff-based model:

- `+fact.` sends `(fact, time, +1)` to DD
- `-fact.` sends `(fact, time, -1)` to DD
- Views are incrementally maintained using DD's `iterative()` scopes
- Queries are executed using DD's dataflow operators

The persistence layer stores `(data, time, diff)` triples, enabling:
- Time-travel queries (historical state)
- Efficient incremental updates
- Crash recovery with WAL (Write-Ahead Log)

## Architecture

```
Statement Parser     →  Statement enum
       ↓
Storage Engine       →  Multi-database management
       ↓
View Catalog         →  Persistent view definitions (JSON)
       ↓
Datalog Engine       →  DD-based execution
       ↓
Persist Layer        →  WAL + batched storage
```

## File Locations

- Config: `~/.inputlayer/config.toml` or `./inputlayer.toml`
- Data: `{data_dir}/{database}/`
- Views: `{data_dir}/{database}/views/catalog.json`
- Persist: `{data_dir}/persist/{database}:{relation}/`
