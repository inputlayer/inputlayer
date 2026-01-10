# Error Reference

This section documents common errors you may encounter when using InputLayer.

## Syntax Errors

### Invalid Statement

```
Error: Invalid statement: unexpected token
```

Check that your statement ends with `.` and follows correct syntax.

### Parse Error

```
Error: Parse error at line N
```

Common causes:
- Missing period at end of statement
- Unbalanced parentheses
- Invalid characters

## Relation Errors

### Undefined Relation

```
Error: Undefined relation 'foo'
```

The relation doesn't exist. Insert facts or check spelling:

```datalog
+foo(1, 2).        % Creates relation
?- foo(X, Y).      % Now works
```

### Arity Mismatch

```
Error: Arity mismatch for 'edge': expected 2, got 3
```

You provided the wrong number of columns:

```datalog
+edge(1, 2).       % edge has 2 columns
+edge(1, 2, 3).    % ERROR: 3 values for 2-column relation
```

### Type Mismatch

```
Error: Type mismatch: expected int, got string
```

Value types don't match the schema:

```datalog
+person(name: string, age: int).
+person(30, "alice").   % ERROR: reversed types
```

### Insert into View

```
Error: Cannot insert into view 'path'
```

You can't insert facts into a derived relation (view):

```datalog
+path(X, Y) :- edge(X, Y).   % path is a view
+path(1, 2).                  % ERROR: can't insert into view
```

## Rule Errors

### Unsafe Variable

```
Error: Unsafe variable 'X' in rule head
```

All head variables must appear in a positive body literal:

```datalog
% BAD: X not bound in body
+bad(X) :- edge(A, B).

% GOOD: X bound by edge
+good(X) :- edge(X, _).
```

### Unsafe Negation Variable

```
Error: Variable 'X' in negation not bound by positive literal
```

Variables in negations must also appear in positive literals:

```datalog
% BAD: X only in negation
+bad(X) :- !excluded(X).

% GOOD: X bound first
+good(X) :- items(X), !excluded(X).
```

### Unstratifiable Negation

```
Error: Circular negation detected
```

A relation can't negatively depend on itself:

```datalog
% BAD: a depends on !a
+a(X) :- b(X), !a(X).

% GOOD: negate a different relation
+a(X) :- b(X), !c(X).
```

## Arithmetic Errors

### Division by Zero

```
Error: Division by zero
```

Check your data for zero divisors:

```datalog
% May error if Y = 0
+ratio(X, R) :- data(X, Y), R = X / Y.

% Safe: filter out zeros
+ratio(X, R) :- data(X, Y), Y != 0, R = X / Y.
```

### Overflow

```
Error: Integer overflow
```

Result exceeds 64-bit integer range.

## Knowledge Graph Errors

### Knowledge Graph Not Found

```
Error: Knowledge graph 'foo' not found
```

The knowledge graph doesn't exist:

```
.kg create foo    % Create it first
.kg use foo       % Then use it
```

### Cannot Drop Current

```
Error: Cannot drop current knowledge graph
```

Switch to a different knowledge graph first:

```
.kg use other     % Switch away
.kg drop target   % Now drop works
```

## Constraint Errors

### Key Violation

```
Error: Duplicate key for 'user'
```

With `@key` constraint, each key must be unique:

```datalog
+user(id: int @key, name: string).
+user(1, "alice").
+user(1, "bob").    % ERROR: key 1 already exists
```

Note: With `@key`, duplicate key insertions update the existing row instead of error.

### Unique Violation

```
Error: Unique constraint violation for column 'email'
```

With `@unique` constraint, values must be unique:

```datalog
+user(id: int, email: string @unique).
+user(1, "alice@example.com").
+user(2, "alice@example.com").  % ERROR: email not unique
```

### Not Empty Violation

```
Error: Value cannot be empty for column 'name'
```

With `@not_empty` constraint, strings can't be empty:

```datalog
+person(name: string @not_empty).
+person("").   % ERROR: empty string
```

### Range Violation

```
Error: Value out of range for column 'age'
```

With `@range` constraint, values must be in range:

```datalog
+person(age: int @range(0, 150)).
+person(-1).   % ERROR: below minimum
+person(200).  % ERROR: above maximum
```

## Aggregation Errors

### Invalid Aggregation Variable

```
Error: Aggregation variable 'X' not found in body
```

The aggregated variable must appear in the rule body:

```datalog
% BAD: Z not in body
+bad(sum<Z>) :- data(X, Y).

% GOOD: X appears in body
+good(sum<X>) :- data(X, _).
```

## Connection Errors

### Server Unavailable

```
Error: Could not connect to server at http://127.0.0.1:8080
```

The InputLayer server isn't running. Start it with:

```bash
inputlayer-server
```

### Request Timeout

```
Error: Request timed out
```

The query took too long. Try:
- Adding filters to reduce data
- Breaking into smaller queries
- Increasing timeout in config
