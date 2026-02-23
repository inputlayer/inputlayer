# InputLayer Roadmap

This document tracks planned features and improvements for InputLayer.

## Recently Completed

### String Functions (v0.1.0)

**Status**: Implemented

7 string functions available: `len`, `upper`, `lower`, `trim`, `substr`, `replace`, `concat`.

See `docs/reference/functions.md` Section 8 for full reference.

---

### Math Functions (v0.1.0)

**Status**: Implemented

13 math functions available: `abs`, `abs_int64`, `abs_float64`, `sqrt`, `pow`, `log`, `exp`, `sin`, `cos`, `tan`, `floor`, `ceil`, `sign`.

See `docs/reference/functions.md` Section 7 for full reference.

---

### TopK Aggregate (v0.1.0)

**Status**: Implemented

Top-K selection with ordering support:

```datalog
+top_scores(top_k<3, Name, Score:desc>) <- scores(Name, Score)
```

Variants: `top_k`, `top_k_threshold`, `within_radius`.

---

### Functions in Rule Heads (v0.1.0)

**Status**: Implemented

All builtin functions (vector, math, string, temporal) now work in rule heads via computed head variables:

```datalog
// Compute and store similarities
+similarity(Id1, Id2, Score) <-
    embedding(Id1, V1), embedding(Id2, V2),
    Id1 < Id2,
    Score = cosine(V1, V2)

// Arithmetic in rule heads
+doubled(X, Y) <- nums(X), Y = X * 2
```

---

## Planned Features

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

---

### Distributed Execution

**Priority**: Low
**Status**: Not implemented

Multi-node execution for horizontal scaling. Currently single-node only.

---

### User-Defined Aggregates

**Priority**: Low
**Status**: Not implemented

Allow users to define custom aggregation functions beyond the built-in set.

---

### Worst-Case Optimal Joins

**Priority**: Low
**Status**: Not implemented

WCOJ for cyclic queries where traditional binary join plans are suboptimal.

---

## Implementation Notes

### Adding New Built-in Functions

To add a new built-in function:

1. Add variant to `BuiltinFunc` enum in `src/ast/mod.rs`
2. Implement parsing in `BuiltinFunc::parse()`
3. Add IR variant to `BuiltinFunction` in `src/ir/mod.rs`
4. Add conversion in `src/ir_builder/mod.rs`
5. Add evaluation logic in `src/code_generator/mod.rs`
6. Add tests in `examples/datalog/` with snapshot
7. Update `docs/reference/functions.md`

---

## Version History

### v0.1.0 (Current)
- 55 builtin functions (vector, temporal, math, string, LSH, quantization)
- Functions work in rule heads via computed head variables
- HNSW vector indexes with full Datalog query integration (`hnsw_nearest` builtin)
- Count distinct aggregate (`count_distinct`)
- TopK, TopKThreshold, WithinRadius aggregates
- 3,085 unit tests + 1,119 snapshot tests = 4,204 total (as of 2026-02-24)
- WebSocket API with auth, streaming results, and notification replay (AsyncAPI-documented)
- Production hardening complete (55/55 issues):
  - Multi-user authentication and ACL system
  - Rate limiting (per-connection WS, per-IP HTTP)
  - Auto-compaction for DD-native persist layer
  - Lock-free DD queries via storage snapshots
  - Streaming result transport for large results (>1 MB)
  - Relation drop, rule drop by prefix, clear by prefix
  - TLS deployment support
- WAL-based persistence with configurable durability
- Basic Datalog operations
- Persistent and session rules
- Aggregations (count, sum, min, max, avg)
- Vector distance functions
- Type declarations and schemas
- Differential Dataflow backend
