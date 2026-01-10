# Result Formats

InputLayer displays query results in a simple tuple format.

## Default Format

Query results are displayed as tuples:

```
N rows:
  (value1, value2, ...)
  (value1, value2, ...)
```

### Example

```datalog
?- edge(X, Y).
```

```
3 rows:
  (1, 2)
  (2, 3)
  (3, 4)
```

## Single Column Results

```datalog
?- edge(1, Y).
```

```
1 row:
  (2)
```

## Empty Results

```datalog
?- edge(100, X).
```

```
No results.
```

## Existence Check

When all columns are constants:

```datalog
?- edge(1, 2).
```

```
1 row:
  ()
```

An empty tuple means "yes, it exists".
