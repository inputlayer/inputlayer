# InputLayer Datalog Language Specification

**Version 1.0**

## Overview

InputLayer is a Datalog database built on Differential Dataflow. It provides a simple, declarative language for storing facts, defining rules, and querying data. This document describes the InputLayer Datalog dialect.

## What is Datalog?

Datalog is a declarative query language based on logic programming. If you know SQL, think of Datalog as SQL with:
- **Simpler syntax** - no verbose keywords like SELECT, FROM, WHERE
- **Built-in recursion** - compute transitive closures without CTEs
- **Automatic optimization** - the engine decides how to execute queries

## What InputLayer Adds

InputLayer extends Datalog with features for real-world applications:

| Feature | Description |
|---------|-------------|
| **Prefix operators** | `+` inserts, `-` deletes, `?-` queries |
| **Persistent rules** | Rules saved to disk and incrementally maintained |
| **Session rules** | Temporary rules for ad-hoc analysis |
| **Aggregations** | `count<X>`, `sum<X>`, `min<X>`, `max<X>`, `avg<X>` |
| **Vector operations** | Distance and similarity functions for embeddings |
| **Typed schemas** | `+user(id: int, name: string).` |
| **Knowledge graphs** | Multiple isolated databases in one instance |

## Quick Example

```datalog
% Insert facts
+edge[(1, 2), (2, 3), (3, 4)].

% Define a persistent rule (saved to disk)
+path(X, Y) :- edge(X, Y).
+path(X, Z) :- path(X, Y), edge(Y, Z).

% Query the derived data
?- path(1, X).
```

Result:
```
3 rows:
  (2)
  (3)
  (4)
```

## How It Works

InputLayer is built on Differential Dataflow, which means:
- **Incremental updates** - when you add or remove facts, only affected results are recomputed
- **Persistent storage** - facts and rules survive restarts
- **Automatic indexing** - the engine builds indexes as needed

## Document Structure

This specification covers:
1. **Core Concepts** - facts, rules, queries, variables
2. **Grammar Reference** - detailed syntax for all language constructs
3. **Extensions** - aggregations, vectors, meta commands
4. **Error Reference** - common error messages and their causes
