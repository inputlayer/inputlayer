# Core Concepts

This guide explains the fundamental concepts you need to understand InputLayer.

## The Three Building Blocks

InputLayer programs consist of three types of statements:

```
┌─────────────────────────────────────────────────────────┐
│                     InputLayer                          │
├─────────────────────────────────────────────────────────┤
│  FACTS          RULES              QUERIES              │
│  +edge(1,2).    +path :- edge.     ?- path(1,X).       │
│                                                         │
│  Base data      Derived data       Questions            │
│  (stored)       (computed)         (answered)           │
└─────────────────────────────────────────────────────────┘
```

## 1. Facts (Base Data)

Facts are the raw data you store. They're like rows in a database table.

### Syntax

```datalog
+relation(value1, value2, ...).
```

### Examples

```datalog
% A single fact: edge from node 1 to node 2
+edge(1, 2).

% Multiple facts at once (bulk insert)
+edge[(1, 2), (2, 3), (3, 4)].

% Different data types
+person("alice", 30).           % String and integer
+location("NYC", 40.7, -74.0).  % Floats
+embedding(1, [0.1, 0.2, 0.3]). % Vector
```

### Key Points

- Facts are **persistent** - saved to disk
- Facts are **immutable** - you add or remove, not update
- The relation name (`edge`, `person`) is like a table name
- Values can be: integers, floats, strings, or vectors

### Deleting Facts

```datalog
% Delete a specific fact
-edge(1, 2).

% Conditional delete (delete all edges from node 1)
-edge(1, Y) :- edge(1, Y).
```

## 2. Rules (Derived Data)

Rules compute new data from existing facts and other rules. They're the heart of Datalog.

### Syntax

```datalog
+head(X, Y, ...) :- body1, body2, ..., constraint.
```

- **Head**: The derived relation being defined
- **Body**: Conditions that must be true (facts or other rules)
- **Variables**: Uppercase letters (X, Y, Z) - matched across the rule

### Simple Rule Example

```datalog
% "adult" contains people who are 18 or older
+adult(Name, Age) :- person(Name, Age), Age >= 18.
```

Reading this: "Name and Age are in `adult` IF they're in `person` AND Age >= 18."

### Join Example

Variables that appear in multiple body atoms create joins:

```datalog
% Facts
+employee("alice", "engineering").
+department("engineering", "Building A").

% Rule: where does each employee work?
+works_in(Name, Building) :-
  employee(Name, Dept),
  department(Dept, Building).
```

The shared variable `Dept` joins `employee` and `department`.

### Recursive Rules

Rules can reference themselves for powerful recursive computations:

```datalog
% Base case: direct edges are paths
+path(X, Y) :- edge(X, Y).

% Recursive case: extend paths through edges
+path(X, Z) :- path(X, Y), edge(Y, Z).
```

This computes transitive closure - all pairs that are connected by any path.

### Key Points

- Rules are **persistent** - saved with the database
- Rules are **incrementally maintained** - when facts change, derived data updates automatically
- Multiple rules with the same head **union** their results

## 3. Queries (Questions)

Queries ask questions about your data (facts and derived rules).

### Syntax

```datalog
?- pattern, pattern, ..., constraint.
```

### Examples

```datalog
% All edges from node 1
?- edge(1, X).

% All paths with length info
?- path(X, Y).

% Filtered query
?- person(Name, Age), Age > 25.

% Constant matching
?- employee("alice", Dept).
```

### Key Points

- Queries are **not stored** - one-time questions
- Results are returned immediately
- Variables in the query become columns in the result

## Session Rules (Transient)

For ad-hoc analysis, use session rules (no `+` prefix):

```datalog
% Session rule - not persisted
temp(X, Y) :- edge(X, Y), X < Y.
```

Session rules:
- Exist only for the current session
- Are cleared when you switch databases or exit
- Useful for exploratory analysis

View session rules with `.session`, clear with `.session clear`.

## Variables and Patterns

### Variable Conventions

- **Uppercase** = Variable: `X`, `Name`, `Age`
- **Lowercase** = Constant or relation name: `edge`, `"alice"`, `42`
- **Underscore** = Placeholder (ignore): `_`

### Pattern Matching

```datalog
% Match any edge
?- edge(X, Y).

% Match edges from node 1
?- edge(1, X).

% Match self-loops
?- edge(X, X).

% Ignore second column
?- person(Name, _).
```

## Negation

Use `!` or `not` to express "does not exist":

```datalog
% People without a manager
+unmanaged(E) :- employee(E), !manages(_, E).

% Nodes with no outgoing edges (sinks)
+sink(X) :- node(X), !edge(X, _).
```

**Important**: Negation is *stratified* - you can't have circular dependencies through negation.

## Aggregations

Compute aggregates over groups:

```datalog
% Count employees per department
+dept_size(Dept, count<Emp>) :- employee(Emp, Dept).

% Total salary per department
+dept_salary(Dept, sum<Salary>) :- employee(_, Dept, Salary).

% Other aggregates: min, max, avg
+oldest(max<Age>) :- person(_, Age).
```

## Schemas (Optional Typing)

Declare types for relations:

```datalog
% Schema declaration
+employee(id: int, name: string, dept: string).

% Now insertions are type-checked
+employee(1, "alice", "engineering").  % OK
+employee("x", "bob", "sales").        % Error: id should be int
```

## How It All Fits Together

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   FACTS     │────▶│   RULES     │────▶│  RESULTS    │
│  (stored)   │     │  (derived)  │     │  (queried)  │
└─────────────┘     └─────────────┘     └─────────────┘
      │                   │                   ▲
      │                   │                   │
      ▼                   ▼                   │
┌─────────────────────────────────────────────┘
│         Incremental Updates via
│         Differential Dataflow
└─────────────────────────────────────────────┘
```

When you:
1. **Add a fact** → Rules automatically recompute affected derived data
2. **Delete a fact** → Derived data that depended on it is removed
3. **Query** → Current state of facts + derived data is returned

This is all **incremental** - only the changes are processed, not the entire dataset.

## Summary Table

| Concept | Syntax | Persisted | Purpose |
|---------|--------|-----------|---------|
| Fact | `+rel(a, b).` | Yes | Store base data |
| Delete | `-rel(a, b).` | Yes | Remove base data |
| Rule | `+head :- body.` | Yes | Derive new data |
| Session Rule | `head :- body.` | No | Ad-hoc derivation |
| Query | `?- pattern.` | No | Ask questions |
| Schema | `+rel(a: type).` | Yes | Type constraints |

## Next Steps

- **[REPL Guide](04-repl-guide.md)** - All available commands
- **[Basic Queries Tutorial](../tutorials/01-basic-queries.md)** - More query patterns
- **[Recursion Tutorial](../tutorials/02-recursion.md)** - Deep dive into recursive rules
