# InputLayer Datalog Specification

This section contains the authoritative specification for InputLayer's Datalog dialect. All claims in this documentation must match the actual implementation.

## Contents

### Core Language

| Document | Description |
|----------|-------------|
| [Syntax](syntax.md) | Complete grammar with EBNF notation |
| [Types](types.md) | Value types: Int64, Float64, String, Bool, Null, Vector, VectorInt8, Timestamp, Interval |
| [Basic Concepts](basic-concepts.md) | Facts, relations, and atoms |

### Rules and Queries

| Document | Description |
|----------|-------------|
| [Rules](rules.md) | Persistent (`+`) and session rules |
| [Queries](queries.md) | Query syntax and execution |
| [Grammar](grammar.md) | Full grammar overview |

### Error Handling

| Document | Description |
|----------|-------------|
| [Errors](errors.md) | Error codes and messages |

---

## Type System Summary

InputLayer supports 9 value types:

| Type | Syntax | Example |
|------|--------|---------|
| Int64 | Integer literals | `42`, `-7` |
| Float64 | Decimal literals | `3.14`, `-0.5` |
| String | Quoted strings | `"hello"`, `'world'` |
| Bool | Boolean literals | `true`, `false` |
| Null | Null value | `null` |
| Vector | f32 array | `[1.0, 2.0, 3.0]` |
| VectorInt8 | i8 array | (via quantization) |
| Timestamp | Int64 milliseconds | `time_now()` |
| Interval | Timestamp pair | `(start, end)` |

---

## Quick Reference

### Fact Syntax
```datalog
+relation(value1, value2).     % Persistent fact
relation(value1, value2).      % Session fact
-relation(value1, value2).     % Delete fact
```

### Rule Syntax
```datalog
+derived(X, Y) :- base(X, Z), other(Z, Y).     % Persistent rule
derived(X, Y) :- base(X, Z), other(Z, Y).      % Session rule
```

### Query Syntax
```datalog
?- relation(X, Y), X > 10.
```

### Aggregation Syntax
```datalog
?- count<X> :- relation(X, _).
?- sum<Value> :- data(_, Value).
?- top_k<5, Score, desc> :- scores(Item, Score).
```

---

## Specification Principles

1. **Authoritative**: Every syntax element matches parser behavior exactly
2. **Complete**: All features documented (no undocumented features)
3. **Tested**: Examples are runnable and verified
4. **Accurate**: Claims match implementation (session rules DO support recursion)
