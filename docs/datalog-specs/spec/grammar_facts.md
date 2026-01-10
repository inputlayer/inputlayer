# Facts and Data

Facts are the base data in InputLayer. They're stored in relations (tables) and persist across restarts.

## Insert Facts (`+`)

Add a single fact with the `+` prefix:

```datalog
+edge(1, 2).
+person("alice", 30).
+message("hello", true).
```

### Bulk Insert

Insert multiple facts at once using brackets:

```datalog
+edge[(1, 2), (2, 3), (3, 4)].
+person[("alice", 30), ("bob", 25), ("charlie", 35)].
```

This is more efficient than inserting facts one by one.

## Delete Facts (`-`)

Remove a fact with the `-` prefix:

```datalog
-edge(1, 2).
-person("alice", 30).
```

### Conditional Delete

Delete facts matching a pattern:

```datalog
% Delete all edges where source > 5
-edge(X, Y) :- edge(X, Y), X > 5.

% Delete all people under 18
-person(Name, Age) :- person(Name, Age), Age < 18.
```

### Delete All Facts

Delete all facts from a relation:

```datalog
-edge(X, Y) :- edge(X, Y).
```

## Grammar

```ebnf
insert      ::= "+" predicate "(" terms ")" "." ;
bulk_insert ::= "+" predicate "[" tuple ("," tuple)* "]" "." ;
delete      ::= "-" predicate "(" terms ")" "." ;
cond_delete ::= "-" head ":-" body "." ;
tuple       ::= "(" terms ")" ;
terms       ::= term ("," term)* ;
predicate   ::= [a-z][a-zA-Z0-9_]* ;
```

## Schema Declarations

Declare a relation's structure before inserting data:

```datalog
+person(id: int, name: string, age: int).
```

### Supported Types

| Type | Description | Examples |
|------|-------------|----------|
| `int` | 64-bit signed integer | `1`, `-42`, `1000000` |
| `float` | 64-bit floating point | `3.14`, `-0.5` |
| `string` | UTF-8 text | `"hello"`, `"alice"` |
| `bool` | Boolean | `true`, `false` |

### Constraints

Add constraints after the type:

```datalog
+user(
    id: int @key,
    email: string @unique,
    name: string @not_empty,
    age: int @range(0, 150)
).
```

| Constraint | Effect |
|------------|--------|
| `@key` | Primary key. Enables upsert (insert or update). |
| `@unique` | Values must be unique across all facts. |
| `@not_empty` | String cannot be empty. |
| `@range(min, max)` | Numeric value must be within range. |

### Composite Keys

Mark multiple columns as `@key` for a composite key:

```datalog
+enrollment(student_id: int @key, course_id: int @key, grade: float).
```

## Type Inference

If you don't declare a schema, InputLayer infers types from the first fact:

```datalog
+person("alice", 30).     % Inferred: person(string, int)
+person("bob", 25).       % OK
+person(123, "charlie").  % ERROR: wrong types
```

## Updates

InputLayer doesn't have an UPDATE command. Instead:

### Manual Update (No Key)

Delete the old fact, insert the new one:

```datalog
-employee(1, "Alice", 75000).
+employee(1, "Alice", 80000).
```

### Automatic Update (With Key)

With `@key` constraint, inserting an existing key updates the row:

```datalog
+employee(id: int @key, name: string, salary: int).
+employee(1, "Alice", 75000).
+employee(1, "Alice", 80000).  % Updates Alice's salary
```

## Common Errors

### Arity Mismatch

Wrong number of values:

```datalog
+edge(1, 2).
+edge(1, 2, 3).  % ERROR: edge has 2 columns, got 3 values
```

### Type Mismatch

Wrong value types:

```datalog
+person("alice", 30).
+person(123, "bob").  % ERROR: expected (string, int), got (int, string)
```

### Insert into View

Can't insert facts into a derived relation:

```datalog
+path(X, Y) :- edge(X, Y).
+path(1, 2).  % ERROR: path is a view, not a base relation
```

## Examples

### Simple Data

```datalog
+edge(1, 2).
+edge(2, 3).
?- edge(X, Y).
```

### Typed Relation

```datalog
+employee(id: int @key, name: string, salary: float).
+employee[(1, "Alice", 75000.0), (2, "Bob", 65000.0)].
?- employee(Id, Name, Salary), Salary > 70000.
```

### Conditional Operations

```datalog
% Insert initial data
+score[("alice", 85), ("bob", 70), ("charlie", 90)].

% Delete low scores
-score(Name, S) :- score(Name, S), S < 75.

% Check remaining
?- score(Name, S).
```
