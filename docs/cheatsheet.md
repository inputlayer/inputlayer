# InputLayer Datalog Cheatsheet

A unified Datalog-native syntax for InputLayer, designed for intuitive data manipulation and querying.

## Quick Reference

| Operator | Meaning | Persisted | DD Semantics |
|----------|---------|-----------|--------------|
| `+` | Insert fact or persistent rule | Yes | diff = +1 |
| `-` | Delete fact or drop rule | Yes | diff = -1 |
| `:-` | Session rule (transient) | No | Ad-hoc computation |
| `?-` | Query | No | Ad-hoc query |

## Key Terminology

| Term | Description |
|------|-------------|
| **Fact** | Base data stored in a relation (e.g., `+edge(1, 2).`) |
| **Rule** | Derived relation defined by a Datalog rule (persistent) |
| **Session Rule** | Transient rule that exists only for current session |
| **Schema** | Type definition for a relation's columns |
| **Query** | One-shot question against facts and rules |

## Meta Commands

Meta commands start with `.` and control the system:

```
.kg                  Show current knowledge graph
.kg list             List all knowledge graphs
.kg create <name>    Create knowledge graph
.kg use <name>       Switch to knowledge graph
.kg drop <name>      Drop knowledge graph (cannot drop current)

.rel                 List relations (base facts)
.rel <name>          Describe relation schema

.rule                List persistent rules
.rule <name>         Query rule (show computed data)
.rule def <name>     Show rule definition
.rule drop <name>    Drop rule
.rule clear <name>   Clear all clauses for re-registration

.session             List session rules
.session clear       Clear all session rules
.session drop <n>    Remove session rule #n

.load <file>         Load and execute a .dl file
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
-edge(X, Y) :- edge(X, Y), X > 5.
```

### Updates (Delete then Insert)

To update data, delete the old value then insert the new:
```datalog
% Delete old value
-counter(1, 0).
% Insert new value
+counter(1, 5).
```

## Persistent Rules (`+head :- body`)

Persistent rules are saved to disk and incrementally maintained by Differential Dataflow.

Simple rule:
```datalog
+path(X, Y) :- edge(X, Y).
```

Recursive rule (transitive closure):
```datalog
+path(X, Y) :- edge(X, Y).
+path(X, Z) :- path(X, Y), edge(Y, Z).
```

Rule with filter:
```datalog
+adult(Name, Age) :- person(Name, Age), Age >= 18.
```

Rules are saved to `{kg_dir}/rules/catalog.json` and automatically loaded on knowledge graph startup.

## Session Rules (`:-`)

Session rules are executed immediately but not persisted. They're useful for ad-hoc analysis:

```datalog
result(X, Y) :- edge(X, Y), X < Y.
```

Session rules can reference persistent rules:
```datalog
reachable_from_one(X) :- path(1, X).
```

Multiple session rules accumulate and evaluate together:
```datalog
foo(X, Y) :- bar(X, Y).
foo(X, Z) :- foo(X, Y), foo(Y, Z).  % Adds to previous rule
```

## Queries (`?-`)

Query a relation or rule:

Simple query:
```datalog
?- edge(1, X).
```

Query with constraints:
```datalog
?- person(Name, Age), Age > 30.
```

Query a derived relation:
```datalog
?- path(1, X).
```

## Schema Declarations

Define typed relations:

```datalog
+employee(id: int, name: string, dept_id: int).
+user(id: int, email: string, name: string).
```

## Aggregations

```datalog
+total_sales(Dept, sum<Amount>) :- sales(Dept, _, Amount).
+employee_count(Dept, count<Id>) :- employee(Id, _, Dept).
+max_salary(Dept, max<Salary>) :- employee(_, Salary, Dept).
+min_age(min<Age>) :- person(_, Age).
+avg_score(avg<Score>) :- test_results(_, Score).
```

## Vector Operations

```datalog
% Insert vectors
+vectors[(1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0])].

% Query with distance computation
?- vectors(Id1, V1), vectors(Id2, V2), Id1 < Id2,
   Dist = euclidean(V1, V2), Dist < 1.0.

% Query with similarity computation
?- vectors(Id1, V1), vectors(Id2, V2), Id1 < Id2,
   Sim = cosine(V1, V2), Sim > 0.9.
```

Available distance functions: `euclidean`, `cosine`, `dot`, `manhattan`

**Note**: Vector functions are used in query bodies, not in rule heads.

## Examples

### Social Graph

```datalog
% Create knowledge graph
.kg create social
.kg use social

% Add edges
+follows[(1, 2), (2, 3), (3, 4), (1, 4)].

% Define reachability rule (persistent)
+reach(X, Y) :- follows(X, Y).
+reach(X, Z) :- reach(X, Y), follows(Y, Z).

% Query who user 1 can reach
?- reach(1, X).
```

### Access Control (RBAC)

```datalog
.kg create acl
.kg use acl

% Facts: users, roles, permissions
+user_role[("alice", "admin"), ("bob", "viewer")].
+role_permission[("admin", "read"), ("admin", "write"), ("viewer", "read")].

% Rule: user has permission if they have a role with that permission
+has_permission(User, Perm) :-
  user_role(User, Role),
  role_permission(Role, Perm).

% Query: what can alice do?
?- has_permission("alice", Perm).
```

### Policy-First RAG

```datalog
.kg create rag
.kg use rag

% Facts
+member[("alice", "engineering"), ("bob", "sales")].
+doc[(101, "Design Doc"), (102, "Sales Pitch")].
+acl[("engineering", 101), ("sales", 102)].
+emb[(101, [1.0, 0.0]), (102, [0.0, 1.0])].

% Rule: user can access docs via group membership
+can_access(User, DocId) :- member(User, Group), acl(Group, DocId).

% Query: what can alice retrieve, with similarity score?
?- can_access("alice", DocId), emb(DocId, V),
   Sim = cosine(V, [0.9, 0.1]), Sim > 0.5.
```

## Differential Dataflow Semantics

InputLayer is built on Differential Dataflow (DD), which uses a diff-based model:

- `+fact.` sends `(fact, time, +1)` to DD
- `-fact.` sends `(fact, time, -1)` to DD
- Rules are incrementally maintained using DD's `iterate()` operator
- Queries are executed using DD's dataflow operators

The persistence layer stores `(data, time, diff)` triples, enabling:
- Efficient incremental updates
- Crash recovery with WAL (Write-Ahead Log)

## Architecture

```
Statement Parser     →  Statement enum (facts, rules, queries)
       ↓
Storage Engine       →  Multi-knowledge-graph management
       ↓
Rule Catalog         →  Persistent rule definitions (JSON)
       ↓
Datalog Engine       →  DD-based execution
       ↓
Persist Layer        →  WAL + batched Parquet storage
```

## File Locations

- Config: `~/.inputlayer/config.toml` or `./inputlayer.toml`
- Data: `{data_dir}/{knowledge_graph}/`
- Rules: `{data_dir}/{knowledge_graph}/rules/catalog.json`
- Persist: `{data_dir}/persist/{knowledge_graph}:{relation}/`
