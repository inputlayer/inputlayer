# Queries

Queries retrieve data from InputLayer. They return matching facts without modifying any data.

## Basic Syntax

Queries start with `?-` and end with `.`:

```datalog
?- edge(X, Y).
```

## Grammar

```ebnf
query ::= "?-" body "." ;
body  ::= literal ("," literal)* ;
```

## Query Patterns

### Get All Data

Query all facts in a relation:

```datalog
?- edge(X, Y).
```

Result:
```
3 rows:
  (1, 2)
  (2, 3)
  (3, 4)
```

### Filter by Value

Fix one column to filter results:

```datalog
% Edges starting from node 1
?- edge(1, Y).
```

Result:
```
1 row:
  (2)
```

### Check Existence

Query with all constants to check if a fact exists:

```datalog
?- edge(1, 2).
```

Result:
```
1 row:
  ()
```

An empty tuple `()` means "yes, it exists". No results means "no".

### Ignore Columns

Use `_` (wildcard) to ignore columns you don't need:

```datalog
% Get all source nodes (ignore destination)
?- edge(X, _).
```

Result:
```
3 rows:
  (1)
  (2)
  (3)
```

## Queries with Conditions

### Comparisons

Filter results with comparison operators:

```datalog
?- person(Name, Age), Age > 25.
```

### Multiple Conditions

Combine conditions with commas (AND):

```datalog
?- person(Name, Age), Age >= 18, Age <= 65.
```

### Negation

Find facts that don't match a pattern:

```datalog
% People who are not managers
?- employee(Id, Name), !manager(Id).
```

## Joins

### Two-Way Join

Join two relations:

```datalog
?- employee(Id, Name), works_in(Id, DeptId), department(DeptId, DeptName).
```

This finds employees with their department names.

### Self-Join

Join a relation with itself:

```datalog
% Find pairs where A follows B and B follows A (mutual follows)
?- follows(A, B), follows(B, A), A < B.
```

The `A < B` avoids duplicate pairs.

## Querying Views

Query derived relations (views) the same way as base relations:

```datalog
% Define a rule
+path(X, Y) :- edge(X, Y).
+path(X, Z) :- path(X, Y), edge(Y, Z).

% Query the derived data
?- path(1, X).
```

## Computed Values

### Arithmetic

Compute values in queries:

```datalog
?- product(Name, Price, Qty), Total = Price * Qty.
```

### Vector Functions

Compute distances or similarities:

```datalog
?- vectors(Id1, V1), vectors(Id2, V2),
   Id1 < Id2,
   Dist = euclidean(V1, V2).
```

## Aggregations in Queries

Use aggregation functions:

```datalog
% Count employees per department
?- works_in(_, Dept), count<_>.

% This is equivalent to defining a rule and querying it
```

For complex aggregations, define a rule first:

```datalog
+dept_count(Dept, count<Id>) :- works_in(Id, Dept).
?- dept_count(Dept, Count).
```

## Result Format

Query results are returned as tuples:

```
N rows:
  (value1, value2, ...)
  (value1, value2, ...)
```

If no matches are found:

```
No results.
```

## Examples

### Simple Lookup

```datalog
+person[("alice", 30), ("bob", 25), ("charlie", 35)].

?- person("alice", Age).
```

Result:
```
1 row:
  (30)
```

### Filtered Search

```datalog
+employee[(1, "alice", 75000), (2, "bob", 65000), (3, "charlie", 80000)].

?- employee(Id, Name, Salary), Salary > 70000.
```

Result:
```
2 rows:
  (1, "alice", 75000)
  (3, "charlie", 80000)
```

### Complex Query

```datalog
% Data
+follows[(1, 2), (2, 3), (3, 1), (2, 1)].

% Find mutual follows
?- follows(A, B), follows(B, A), A < B.
```

Result:
```
2 rows:
  (1, 2)
  (1, 3)
```

### Query with Aggregation

```datalog
+sales[("north", 100), ("north", 200), ("south", 150)].

% Define aggregation rule
+total_by_region(Region, sum<Amount>) :- sales(Region, Amount).

% Query it
?- total_by_region(Region, Total).
```

Result:
```
2 rows:
  ("north", 300)
  ("south", 150)
```
