# Extensions

This section documents InputLayer-specific features beyond standard Datalog.

## Aggregations

InputLayer supports aggregation functions using angle-bracket syntax:

```datalog
+dept_count(Dept, count<Id>) :- employee(Id, _, Dept).
```

### Aggregation Functions

| Function | Description | Example |
|----------|-------------|---------|
| `count<X>` | Count occurrences | `count<Id>` |
| `sum<X>` | Sum of values | `sum<Amount>` |
| `min<X>` | Minimum value | `min<Price>` |
| `max<X>` | Maximum value | `max<Score>` |
| `avg<X>` | Average value | `avg<Age>` |

### Grouped Aggregations

Non-aggregated columns become grouping keys:

```datalog
% Count employees per department
+dept_count(Dept, count<Id>) :- employee(Id, _, Dept).
```

Result groups by `Dept` and counts `Id` values in each group.

### Global Aggregations

Aggregate over all data (no grouping):

```datalog
% Count all employees
+total_count(count<Id>) :- employee(Id, _, _).

% Sum all salaries
+total_salary(sum<Sal>) :- employee(_, Sal, _).
```

### Multiple Aggregations

Multiple aggregations over the same relation:

```datalog
+salary_stats(Dept, count<Id>, sum<Sal>, avg<Sal>) :-
    employee(Id, Sal, Dept).
```

### Examples

```datalog
% Setup data
+sales[("north", 100), ("north", 200), ("south", 150), ("south", 50)].

% Total by region
+region_total(Region, sum<Amount>) :- sales(Region, Amount).
?- region_total(Region, Total).
% ("north", 300)
% ("south", 200)

% Global total
+total(sum<Amount>) :- sales(_, Amount).
?- total(T).
% (500)

% Count by region
+region_count(Region, count<Amount>) :- sales(Region, Amount).
?- region_count(R, C).
% ("north", 2)
% ("south", 2)
```

## Vector Operations

InputLayer supports vectors for similarity search and embeddings.

### Vector Literals

Vectors are comma-separated floats in square brackets:

```datalog
+embedding(1, [0.1, 0.2, 0.3]).
+embedding(2, [0.4, 0.5, 0.6]).
+embedding(3, [0.1, 0.2, 0.35]).
```

### Distance Functions

| Function | Description |
|----------|-------------|
| `euclidean(v1, v2)` | L2 (Euclidean) distance |
| `cosine(v1, v2)` | Cosine distance (1 - similarity) |
| `dot(v1, v2)` | Dot product |
| `manhattan(v1, v2)` | L1 (Manhattan) distance |

### Examples

```datalog
% Find similar embeddings
?- embedding(A, V1), embedding(B, V2),
   A < B,
   Dist = euclidean(V1, V2),
   Dist < 0.5.

% Compute cosine similarity
?- embedding(1, V1), embedding(2, V2),
   Sim = cosine(V1, V2).

% RAG: filter by permission, rank by similarity
?- can_access("alice", DocId),
   doc_embedding(DocId, V),
   QueryVec = [0.1, 0.2, 0.3],
   Sim = cosine(V, QueryVec).
```

## Meta Commands

System commands start with `.` and don't end with `.`:

### Knowledge Graph Commands

```
.kg                     Show current knowledge graph
.kg list                List all knowledge graphs
.kg create <name>       Create new knowledge graph
.kg use <name>          Switch to knowledge graph
.kg drop <name>         Delete knowledge graph
```

### Relation Commands

```
.rel                    List all relations
.rel <name>             Show relation schema and stats
```

### Rule Commands

```
.rule                   List all persistent rules
.rule <name>            Query rule (show computed data)
.rule def <name>        Show rule definition
.rule drop <name>       Drop persistent rule
.rule clear <name>      Clear all clauses for re-registration
```

### Session Commands

```
.session                List session rules
.session clear          Clear all session rules
.session drop <n>       Drop session rule #n
```

### Other Commands

```
.load <file>            Load and execute a .dl file
.compact                Compact WAL and consolidate batches
.status                 Show system status
.help                   Show help
.quit                   Exit client
```

### Examples

```
> .kg create myproject
Knowledge graph 'myproject' created.
Switched to knowledge graph: myproject

> .kg list
2 knowledge graphs:
  default
* myproject

> .rel
2 relations:
  edge (3 facts)
  person (5 facts)

> .rule
1 persistent rule:
  path

> .rule def path
+path(X, Y) :- edge(X, Y).
+path(X, Z) :- path(X, Y), edge(Y, Z).
```

## Schema Declarations

Declare relation schemas with types:

```datalog
+employee(id: int, name: string, dept: string).
```

### Type Keywords

| Keyword | Type |
|---------|------|
| `int` | 64-bit signed integer |
| `float` | 64-bit floating point |
| `string` | UTF-8 text |
| `bool` | Boolean |
| `vector[N]` | N-dimensional vector |

### Session vs Persistent Schemas

```datalog
% Persistent schema - saved with knowledge graph
+user(id: int, name: string).

% Session schema - only for current connection
user(id: int, name: string).
```

### Examples

```datalog
% Simple typed schema
+person(name: string, age: int).

% With vectors
+embedding(id: int, vec: vector[128]).

% Multiple columns
+order_item(order_id: int, item_id: int, quantity: int).
```

## Bulk Operations

Insert multiple facts efficiently:

```datalog
+edge[(1, 2), (2, 3), (3, 4), (4, 5)].
+person[("alice", 30), ("bob", 25), ("charlie", 35)].
```

This is more efficient than individual inserts.

## Conditional Delete

Delete facts matching a pattern:

```datalog
% Delete edges where source > 5
-edge(X, Y) :- edge(X, Y), X > 5.

% Delete all facts from a relation
-edge(X, Y) :- edge(X, Y).
```

## File Loading

Load and execute Datalog files:

```
.load examples/graph.dl
```

The file is executed statement by statement.
