# Core Concepts

This section explains the fundamental building blocks of InputLayer Datalog.

## Relations (Tables)

A relation is a table that stores facts. Each relation has:
- A **name** (lowercase, like `edge` or `person`)
- A **schema** (the columns and their types)
- **Facts** (the rows)

```datalog
% Create a relation by inserting facts
+person("alice", 30).
+person("bob", 25).
```

This creates a `person` relation with 2 columns (name and age) and 2 rows.

## Facts (Rows)

A fact is a single row in a relation. Facts contain only values (no variables).

```datalog
% Insert facts
+edge(1, 2).           % edge has two integer columns
+user("alice", true).  % user has string and boolean columns
```

Facts are stored persistently. When you restart InputLayer, your facts are still there.

## Rules (Derived Tables)

Rules define new relations based on existing data. A rule has:
- A **head** (the relation being defined)
- A **body** (the conditions for deriving facts)

```datalog
% "path exists from X to Y if edge exists from X to Y"
+path(X, Y) :- edge(X, Y).

% "path exists from X to Z if path exists from X to Y and edge from Y to Z"
+path(X, Z) :- path(X, Y), edge(Y, Z).
```

The head is `path(X, Y)`. The body is everything after `:-`.

Rules don't store data directly - they compute results from stored facts. When facts change, rule results update automatically.

## Views

A view is a relation defined by rules. In the example above, `path` is a view.

Views are virtual tables:
- They don't store data directly
- Results are computed when queried
- Updates are incremental (only changed parts recompute)

## Queries

A query asks a question about your data. It returns matching facts.

```datalog
% Find all edges starting from node 1
?- edge(1, X).

% Find all people older than 25
?- person(Name, Age), Age > 25.
```

Queries return results but don't modify any data.

## Variables

Variables start with an uppercase letter. They match any value.

| Pattern | Matches |
|---------|---------|
| `edge(X, Y)` | All edges (both columns bound to variables) |
| `edge(1, Y)` | Edges starting from node 1 |
| `edge(X, X)` | Self-loops (same value in both columns) |
| `edge(_, Y)` | All edges (ignore first column) |

The underscore `_` is a special variable that means "I don't care about this value."

## Constants

Constants are literal values:

| Type | Examples |
|------|----------|
| **Integer** | `1`, `-42`, `1000000` |
| **Float** | `3.14`, `-0.5`, `1e10` |
| **String** | `"hello"`, `"alice"`, `"with spaces"` |
| **Boolean** | `true`, `false` |
| **Vector** | `[1.0, 2.0, 3.0]` |
| **Atom** | `alice`, `bob` (unquoted lowercase = string) |

## Atoms (Predicate Calls)

An atom is a relation name with arguments:

```datalog
edge(1, 2)              % Atom with two constants
person(Name, Age)       % Atom with two variables
friend(X, "alice")      % Atom with variable and constant
```

In a rule body, atoms specify patterns to match. In a rule head, atoms specify what to produce.

## Schema (Column Types)

You can declare a relation's schema before inserting data:

```datalog
% Declare schema with types
+employee(id: int, name: string, salary: int).

% Now insert data
+employee(1, "alice", 50000).
+employee(2, "bob", 60000).
```

Schema declarations are optional. If you don't declare a schema, InputLayer infers types from the first fact.

### Session vs Persistent Schemas

Like rules, schemas can be session or persistent:

```datalog
% Persistent schema - saved with knowledge graph
+user(id: int, email: string, name: string).

% Session schema - only for current connection
user(id: int, email: string, name: string).
```

## Program Structure

An InputLayer session consists of:

1. **Stored facts** - data in relations
2. **Persistent rules** - saved rule definitions
3. **Session rules** - temporary rules for this session
4. **Queries** - questions you ask

```datalog
% 1. Insert facts (stored)
+edge[(1, 2), (2, 3), (3, 4)].

% 2. Define persistent rule (saved)
+path(X, Y) :- edge(X, Y).
+path(X, Z) :- path(X, Y), edge(Y, Z).

% 3. Define session rule (temporary)
long_path(X, Y) :- path(X, Y), X < Y.

% 4. Query
?- path(1, X).
```

## Knowledge Graphs

A knowledge graph is a named container for relations and rules. It's like a database in SQL systems.

```
.kg create social        % Create a knowledge graph
.kg use social           % Switch to it
+follows(1, 2).          % Data goes in 'social'
.kg create work          % Create another
.kg use work             % Switch to it
+reports_to(1, 2).       % Data goes in 'work'
```

Each knowledge graph is isolated. Relations in `social` are separate from relations in `work`.
