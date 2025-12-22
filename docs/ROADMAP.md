# InputLayer Roadmap

This document tracks planned features and improvements for InputLayer.

## Planned Features

### String Functions

**Priority**: High
**Status**: Not implemented

String manipulation functions for use in rules and queries:

| Function | Description | Example |
|----------|-------------|---------|
| `upper(s)` | Convert to uppercase | `upper("hello")` → `"HELLO"` |
| `lower(s)` | Convert to lowercase | `lower("HELLO")` → `"hello"` |
| `strlen(s)` | String length | `strlen("hello")` → `5` |
| `concat(a, b)` | Concatenate strings | `concat("a", "b")` → `"ab"` |
| `starts_with(s, prefix)` | Check prefix | Boolean result |
| `ends_with(s, suffix)` | Check suffix | Boolean result |
| `contains(s, substr)` | Check substring | Boolean result |
| `substr(s, start, len)` | Extract substring | Portion of string |

**Use cases**:
- Text normalization in ETL pipelines
- Pattern matching and filtering
- Data cleaning and transformation

---

### Math Functions

**Priority**: Medium
**Status**: Not implemented

Mathematical functions for numeric operations:

| Function | Description | Example |
|----------|-------------|---------|
| `abs(n)` | Absolute value | `abs(-5)` → `5` |
| `floor(n)` | Floor value | `floor(3.7)` → `3` |
| `ceil(n)` | Ceiling value | `ceil(3.2)` → `4` |
| `round(n)` | Round to nearest | `round(3.5)` → `4` |
| `sqrt(n)` | Square root | `sqrt(16)` → `4.0` |
| `pow(base, exp)` | Exponentiation | `pow(2, 3)` → `8` |

**Use cases**:
- Numeric data transformations
- Distance and similarity calculations
- Statistical computations

---

### Vector Functions in Rule Heads

**Priority**: High
**Status**: Partially implemented (queries only)

Currently, vector functions (`euclidean`, `cosine`, `dot`, `manhattan`) only work in query bodies. They should also work in persistent rule heads:

**Current limitation**:
```datalog
// Works - query body
?- embedding(Id1, V1), embedding(Id2, V2), Dist = euclidean(V1, V2).

// Does NOT work - rule head
+similarity(Id1, Id2, cosine(V1, V2)) :-
    embedding(Id1, V1), embedding(Id2, V2).
```

**Target behavior**:
```datalog
// Should work - compute and store similarities
+similarity(Id1, Id2, Score) :-
    embedding(Id1, V1), embedding(Id2, V2),
    Id1 < Id2,
    Score = cosine(V1, V2).
```

**Use cases**:
- Pre-computing similarity matrices
- Building recommendation indexes
- Caching expensive vector computations

---

### External Data Loading

**Priority**: Medium
**Status**: Not implemented

Load data from external file formats:

```datalog
.load users.json as user
.load sales.parquet as sale
.load events.csv as event
```

**Supported formats**:
| Format | Description |
|--------|-------------|
| `.json` | JSON arrays or objects |
| `.parquet` | Apache Parquet columnar format |
| `.csv` | Comma-separated values |

**Features**:
- Schema inference from file structure
- Type coercion for compatible types
- Streaming load for large files

**Use cases**:
- ETL from data lakes
- Importing analytics data
- Integration with external systems

---

## Implementation Notes

### Adding New Built-in Functions

To add a new built-in function:

1. Add variant to `BuiltinFunc` enum in `src/ast/mod.rs`
2. Implement parsing in `BuiltinFunc::parse()`
3. Add evaluation logic in `src/code_generator/mod.rs`
4. Add tests in `examples/datalog/` with snapshot
5. Update documentation

### Enabling Functions in Rule Heads

Currently, functions in rule heads produce "Unsafe rule" errors because the IR treats them as unbound. To fix:

1. Modify `src/ir/safety.rs` to recognize function calls
2. Update `src/code_generator/mod.rs` to evaluate functions during materialization
3. Ensure incremental maintenance works with computed values

---

## Version History

### v0.1.0 (Current)
- Basic Datalog operations
- Persistent and session rules
- Aggregations (count, sum, min, max, avg)
- Vector distance functions (query-only)
- Type declarations and schemas

### v0.2.0 (Planned)
- String functions
- Math functions
- Vector functions in rule heads
- External data loading
