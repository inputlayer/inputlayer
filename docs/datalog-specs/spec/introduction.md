# Introduction

## What is Datalog?

Datalog is a query language that looks like logic programming. If you've used SQL, Datalog will feel familiar but simpler. Instead of writing:

```sql
SELECT p.name FROM person p
JOIN friend f ON p.id = f.person_id
WHERE f.friend_id = 1;
```

You write:

```datalog
?- person(Id, Name), friend(Id, 1).
```

The key difference: Datalog handles recursion naturally. Finding all people reachable through a chain of friendships is one simple rule, not a complex CTE.

## How InputLayer Uses Datalog

InputLayer is a streaming deductive knowledge base that uses Datalog as its query language. It adds practical features:

**Data manipulation with prefix operators:**
```datalog
+edge(1, 2).     % Insert a fact
-edge(1, 2).     % Delete a fact
?- edge(X, Y).   % Query facts
```

**Rules that define derived data:**
```datalog
+path(X, Y) :- edge(X, Y).                    % Base case
+path(X, Z) :- path(X, Y), edge(Y, Z).        % Recursive case
```

**Incremental computation:** When you add or remove facts, InputLayer only recomputes what changed. This makes updates fast even on large datasets.

## Key Terms

| Term | What it means |
|------|---------------|
| **Knowledge Graph** | A named knowledge base. One InputLayer instance can have many knowledge graphs. |
| **Relation** | A table. Facts are stored in relations. |
| **Fact** | A row in a relation. Written as `+name(value1, value2).` |
| **Rule** | A definition that derives new facts from existing ones. |
| **View** | A relation defined by rules (not stored facts). |
| **Query** | A one-time question. Written as `?- pattern.` |
| **Session Rule** | A temporary rule that disappears when you disconnect. |
| **Persistent Rule** | A rule saved to disk that survives restarts. |

## Syntax Overview

InputLayer uses prefix characters to indicate the operation:

| Prefix | Operation | Persisted? |
|--------|-----------|------------|
| `+` | Insert fact or register persistent rule | Yes |
| `-` | Delete fact or drop rule | Yes |
| `:-` | Define session rule | No |
| `?-` | Run query | No |

## Language Features

InputLayer supports these Datalog features:

| Feature | Example |
|---------|---------|
| **Negation** | `!friend(X, Y)` - true when no matching fact exists |
| **Comparisons** | `X > 10`, `Name != "admin"` |
| **Arithmetic** | `Total = Price * Qty` |
| **Aggregations** | `count<X>`, `sum<Amount>`, `min<Age>` |
| **Vectors** | `[1.0, 2.0, 3.0]` with distance functions |
| **Schemas** | `+user(id: int @key, name: string).` |

## Architecture

```
Input (text)
    ↓
Statement Parser    Parses Datalog syntax
    ↓
Storage Engine      Manages knowledge graphs
    ↓
Rule Catalog        Stores persistent rules
    ↓
Datalog Engine      Runs queries with Differential Dataflow
    ↓
Persist Layer       Saves data with WAL and Parquet files
```

## Next Steps

- **[Core Concepts](basic_concepts.md)** - Learn about relations, rules, and queries
- **[Grammar Reference](grammar.md)** - Detailed syntax for all constructs
- **[Extensions](extensions.md)** - Aggregations, vectors, and meta commands
