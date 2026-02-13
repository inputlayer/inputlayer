# Data Modeling Guide

Identity semantics, schema options, and update patterns.

## Identity Model

InputLayer uses **pure multiset semantics** by default, where the **entire tuple is the identity**. This is the native model for Differential Dataflow (DD).

### Default Behavior (No Schema)

Without an explicit schema, tuples are identified by all their values:

```datalog
+person("alice", 30)     // Insert tuple ("alice", 30)
+person("alice", 31)     // Insert different tuple ("alice", 31)
```

Both tuples coexist because they are different values. There is no concept of "alice" as an entity with a mutable "age" attribute.

### Implications

| Aspect | Behavior |
|--------|----------|
| Tuple identity | ALL columns (entire tuple) |
| Duplicate handling | Multiset - same tuple can exist multiple times |
| Updates | Must know all column values to delete |

## Schema Declarations

Schemas define the structure and constraints for relations.

### Basic Schema

Declare a schema using typed arguments:

```datalog
+person(id: int, name: string, age: int)
```

## Update Patterns

### Pattern 1: Exact Delete (Know All Values)

When you know the exact tuple to delete:

```datalog
-person("alice", 30)
+person("alice", 31)
```

### Pattern 2: Conditional Delete (Unknown Values)

When you don't know all column values, use a conditional delete:

```datalog
// Delete alice regardless of age
-person("alice", Age) <- person("alice", Age)
+person("alice", 31)
```

### Pattern 3: Atomic Update

Combine delete and insert in one atomic operation:

```datalog
-person(Name, OldAge), +person(Name, NewAge) <-
  person(Name, OldAge),
  Name = "alice",
  NewAge = OldAge + 1
```

This executes at the same logical timestamp, ensuring atomicity.

## Deletion Patterns

### Delete Specific Tuple

```datalog
-edge(1, 2)
```

### Delete All Matching Tuples

```datalog
// Delete all edges from node 5
-edge(5, Y) <- edge(5, Y)

// Delete all high earners
-employee(Name, Dept, Salary) <-
  employee(Name, Dept, Salary),
  Salary > 100000
```

### Delete Entire Relation

To delete a relation (schema + all data):

```datalog
-person
```

**Note**: This only works for relations without data. To delete all data first:

```datalog
-person(X, Y, Z) <- person(X, Y, Z)  // Delete all tuples
-person                               // Delete relation
```

## Schema Inference

When no schema is declared, it's inferred from the first insert:

```datalog
+person("alice", 30)          // Inferred: person(string, int)
+person("bob", 25)            // OK: matches inferred schema
+person("charlie", "young")   // ERROR: type mismatch (string vs int)
```

## Transient vs Persistent

### Persistent Schema (`+` prefix)

Stored in the database catalog:

```datalog
+person(id: int, name: string, age: int)
```

### Transient Schema (no prefix)

Session-only, cleared on database switch:

```datalog
temp(x: int, y: int)
temp(1, 2)
temp(3, 4)
// Cleared when switching databases
```

Use transient schemas for:
- REPL exploration with type safety
- Temporary working data
- Testing schema designs before persisting

## Rules (Views)

### Rule Identity

A **view** (derived relation) is identified by its **head predicate name**. A view contains one or more rules:

```datalog
+reachable(X, Y) <- edge(X, Y)                   // Creates view, adds rule 1
+reachable(X, Y) <- reachable(X, Z), edge(Z, Y)  // Adds rule 2 to same view
```

### Deleting Views

Delete an entire view with:

```datalog
-reachable
```

Individual rule clauses can be removed using `.rule remove`:

```datalog
.rule remove reachable 1   // Remove first clause of 'reachable' rule
.rule drop reachable       // Remove entire 'reachable' rule (all clauses)
```

To completely delete a view:

```datalog
-reachable
```

Or use file-based workflow:

```datalog
.load views/reachable.idl --replace
```

### Session Rules

Rules without `+` are transient:

```datalog
temp(X, Y) <- edge(X, Y), X < Y
```

Session rules:
- Are not persisted
- Are cleared on database switch
- Support recursion (full fixed-point iteration)

## File-Based Workflow

For complex views with many rules, use `.idl` script files:

```datalog
// views/reachable.idl
+reachable(X, Y) <- edge(X, Y)
+reachable(X, Y) <- reachable(X, Z), edge(Z, Y)
```

### Load Modes

| Mode | Syntax | Behavior |
|------|--------|----------|
| **Default** | `.load file.idl` | Error if any name already exists |
| **Replace** | `.load file.idl --replace` | Delete existing, then load |
| **Merge** | `.load file.idl --merge` | Add rules to existing views |

### Example Workflow

```datalog
// Initial load
.load views/access_control.idl

// After modifying the file, reload with replace
.load views/access_control.idl --replace
```

## Best Practices

### 1. Use Explicit Schemas

Explicit schemas catch type errors early:

```datalog
+employee(id: int, name: string, salary: float)
```

### 2. Use Conditional Deletes for Unknown Values

```datalog
// Update all employees in a department
-employee(Id, OldDept, Name), +employee(Id, "Engineering", Name) <-
  employee(Id, OldDept, Name),
  OldDept = "Legacy"
```

### 3. Use File-Based Workflow for Complex Rules

Keep rule definitions in version-controlled files:

```
views/
  access_control.idl
  graph_analysis.idl
  reporting.idl
```

### 4. Use Persistent Rules for Automatic Materialization

Persistent rules are automatically materialized and updated when base data changes:

```datalog
// Session rules compute fresh each query:
reachable(X, Y) <- edge(X, Y)
reachable(X, Y) <- reachable(X, Z), edge(Z, Y)

// Persistent rules materialize and cache results:
+reachable(X, Y) <- edge(X, Y)
+reachable(X, Y) <- reachable(X, Z), edge(Z, Y)
```

Both session and persistent rules support full recursion with fixed-point iteration.
