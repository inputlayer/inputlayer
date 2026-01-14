# Test Implementation Plan

A strategic plan to implement 649 missing tests across 70 categories, prioritized by risk, effort, and value.

**Current State**: 508 implemented / 1157 total (44% coverage)
**Target State**: 1157 implemented / 1157 total (100% coverage)
**Gap**: 649 tests

---

## Executive Summary

### Implementation Strategy

The tests are organized into **4 phases** based on:
1. **Production Risk** - Will missing test cause crashes/data loss in production?
2. **Implementation Effort** - How complex is the test to write?
3. **Value Delivered** - How much confidence does this test provide?
4. **Dependencies** - What infrastructure is needed first?

### Test Type Distribution

| Type | Count | Framework | Notes |
|------|-------|-----------|-------|
| **Snapshot Tests** | ~350 | `.dl` + `.dl.out` files | Datalog syntax tests |
| **Unit Tests** | ~200 | `#[test]` in Rust | Module-level tests |
| **Integration Tests** | ~60 | Rust + server | API/client tests |
| **Stress Tests** | ~40 | Custom harness | Performance/scale tests |

---

## Phase 1: P0 Critical - Production Stability (Est: 80 tests)

**Goal**: Prevent crashes, data loss, and security issues in production.
**Priority**: IMMEDIATE - Do before any release.

### 1.1 Panic Path Coverage (25 tests)

**Why Critical**: 34 statement downcast panics + 146 lock unwraps can crash the server on malformed input.

**Implementation Approach**: Unit tests with crafted malicious inputs.

```rust
// Example: Test invalid statement downcast
#[test]
fn test_invalid_statement_type_does_not_panic() {
    let statement = Statement::Query(...); // Create query
    let result = handle_as_fact(statement); // Try to handle as fact
    assert!(result.is_err()); // Should error, not panic
}
```

**Tests to implement**:
| Test | Location | Effort |
|------|----------|--------|
| Invalid statement downcast (10 variants) | `src/execution/` | Medium |
| IR builder with invalid AST (5 cases) | `src/ir/` | Medium |
| Optimizer with invalid IR (4 cases) | `src/optimizer/` | Medium |
| Lock poisoning recovery (6 cases) | `src/storage/` | High |

### 1.2 Concurrent Access Testing (12 tests)

**Why Critical**: 146 lock unwraps with ZERO concurrency tests. Any deadlock or lock poisoning crashes production.

**Implementation Approach**: Multi-threaded integration tests.

```rust
#[test]
fn test_concurrent_writes_no_deadlock() {
    let storage = create_test_storage();
    let handles: Vec<_> = (0..10).map(|i| {
        let storage = storage.clone();
        thread::spawn(move || {
            for j in 0..100 {
                storage.append(format!("tuple_{i}_{j}"));
            }
        })
    }).collect();
    for h in handles { h.join().unwrap(); }
    // Verify all data present
}
```

**Tests to implement**:
| Test | Effort | Notes |
|------|--------|-------|
| Concurrent append same shard | High | Thread safety |
| Concurrent append different shards | Medium | Isolation |
| Append while flush | High | Race condition |
| Append while compaction | High | Race condition |
| Read while write | Medium | Consistency |
| Lock poisoning recovery | High | Fault tolerance |

### 1.3 Crash Recovery Testing (10 tests)

**Why Critical**: No tests verify data survives crashes. Data loss risk.

**Implementation Approach**: Kill process during operations, verify recovery.

```rust
#[test]
fn test_crash_during_wal_append_recovers() {
    // Start write operation
    // Kill process (simulate crash)
    // Restart
    // Verify WAL replays correctly
}
```

**Tests to implement**:
| Test | Effort | Notes |
|------|--------|-------|
| Crash during WAL append | High | Requires process control |
| Crash during flush | High | Partial Parquet files |
| Crash during compaction | High | Data loss risk |
| Crash during metadata write | High | Consistency |
| Recovery from partial WAL | Medium | Edge case |
| Double WAL replay (idempotency) | Medium | Important guarantee |

### 1.4 Critical Error Variants (10 tests)

**Why Critical**: Some error paths lead to panics instead of graceful errors.

**Implementation Approach**: Snapshot tests triggering each error.

**Tests to implement**:
| Test | Type | Effort |
|------|------|--------|
| AVG of empty group (div by zero) | Snapshot | Low |
| SUM overflow saturation | Snapshot | Low |
| Query timeout handling | Integration | Medium |
| Memory limit handling | Integration | Medium |
| max_result_size enforcement | Integration | Medium |
| Mutual negation cycle error | Snapshot | Low |
| Insert into view error | Snapshot | Low |
| Unbound head variable error | Snapshot | Low |

### 1.5 Boundary Value Testing - Critical Subset (10 tests)

**Why Critical**: Edge cases at limits often cause crashes.

**Tests to implement**:
| Test | Type | Effort |
|------|------|--------|
| INT64_MIN arithmetic | Snapshot | Low |
| INT64_MAX arithmetic | Snapshot | Low |
| 0-dimension vector | Snapshot | Low |
| Empty body rule rejection | Snapshot | Low |
| Recursion at 1001 depth (over limit) | Snapshot | Low |
| 10001 row result (over limit) | Integration | Medium |
| 0ms query timeout | Integration | Medium |

---

## Phase 2: P1 High - Core Functionality (Est: 150 tests)

**Goal**: Ensure all documented features work correctly.
**Priority**: HIGH - Complete for beta release.

### 2.1 REST API Endpoint Coverage (27 tests)

**Why Important**: Primary programmatic interface. Currently 0% tested.

**Implementation Approach**: Integration tests with `reqwest` client.

```rust
#[tokio::test]
async fn test_post_query_execute() {
    let client = setup_test_server().await;
    let resp = client.post("/api/v1/query/execute")
        .json(&QueryRequest { query: "?- edge(X, Y)." })
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let result: QueryResponse = resp.json().await.unwrap();
    assert!(result.rows.len() > 0);
}
```

**Tests to implement** (grouped by endpoint):
- Knowledge Graph: GET/POST/DELETE (8 tests)
- Query Execute: POST + error cases (6 tests)
- Relations: GET/POST/DELETE data (7 tests)
- Error responses: 400/404/500 (6 tests)

### 2.2 Configuration Impact Testing (31 tests)

**Why Important**: 34/36 config options untested. Users can't tune production.

**Implementation Approach**: Unit tests with config variations.

```rust
#[test]
fn test_max_result_size_enforced() {
    let config = Config { query: QueryConfig { max_result_size: 100, .. } };
    let engine = DatalogEngine::with_config(config);
    // Insert 1000 rows
    let result = engine.query("?- big_table(X).");
    assert!(result.is_err() || result.unwrap().len() <= 100);
}
```

**Tests to implement** (grouped by section):
- Server config: host, port, timeouts (6 tests)
- Storage config: path, WAL, compaction (6 tests)
- Query config: limits, cache, optimizer (6 tests)
- Logging config: level, format, rotation (5 tests)
- Vector config: dimensions, quantization (4 tests)
- Client config: timeouts, retries (6 tests)

### 2.3 Error Variant Coverage (32 tests)

**Why Important**: 22 InputLayerError + 14 StorageError variants. Many untested.

**Implementation Approach**: Unit tests triggering each error variant.

**Tests to implement**:
- InputLayerError: 18 untested variants
- StorageError: 14 untested variants

### 2.4 Public Method Coverage (30 tests)

**Why Important**: Key modules at 0% coverage.

**Implementation Approach**: Unit tests for each public method.

**Priority modules**:
| Module | Methods | Coverage | Effort |
|--------|---------|----------|--------|
| PersistentStore | 8 | 0% | High |
| WAL | 7 | 0% | Medium |
| QueryCache | 6 | 0% | Medium |
| Optimizer | 5 | 0% | Medium |
| Client | 4 | 20% | Medium |

### 2.5 Serialization Round-trip (22 tests)

**Why Important**: Data integrity depends on correct serialization.

**Implementation Approach**: Property-based tests with `proptest`.

```rust
proptest! {
    #[test]
    fn test_value_json_roundtrip(v: Value) {
        let json = serde_json::to_string(&v).unwrap();
        let parsed: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v, parsed);
    }
}
```

**Tests to implement**:
- Value types (9 variants): JSON roundtrip
- Abomonation (3 tests): Binary roundtrip for DD
- REST DTOs (6 tests): API contract
- Wire protocol (4 tests): Client-server

### 2.6 Numeric Edge Cases (22 tests)

**Why Important**: Arithmetic bugs cause silent data corruption.

**Tests to implement**:
- Division edge cases: AVG empty, infinity, NaN (4 tests)
- Integer overflow: SUM, arithmetic, multiplication (5 tests)
- Type cast safety: i64→i32, f64→f32 (4 tests)
- Float special values: NaN, infinity, epsilon (5 tests)
- Quantization edge cases: overflow, scale=0 (4 tests)

---

## Phase 3: P2 Medium - Feature Completeness (Est: 200 tests)

**Goal**: Test all language features and edge cases.
**Priority**: MEDIUM - Complete for v1.0 release.

### 3.1 Snapshot Tests for Missing Language Features (~100 tests)

**Implementation Approach**: Add `.dl` + `.dl.out` files.

**Grouped by category**:

#### 3.1.1 Negation (4 tests)
- Mutual negation cycle detection error
- Three-way negation cycle error
- (Others already well tested at 94%)

#### 3.1.2 Aggregations (8 tests)
- Unknown aggregate function error
- Invalid aggregation variable
- Aggregation on non-numeric (SUM)
- AVG producing non-integer
- SUM overflow
- COUNT with NULL
- TOP_K with ties
- AVG of single value

#### 3.1.3 Arithmetic (8 tests)
- Chained operations (A+B+C+D)
- Float + Integer mixing
- Unary minus (-X)
- Double negative (X - (-Y))
- Operator associativity
- Deeply nested parentheses (10+)
- (Division by zero already fixed)

#### 3.1.4 Types (10 tests)
- i64 max/min boundary
- Very small/large floats
- Float precision limits
- NaN handling
- Infinity handling
- String with quotes
- Multi-line strings
- Boolean in comparisons
- String comparison (lexicographic)
- Compare with NULL/missing

#### 3.1.5 Joins (2 tests)
- Join on multiple columns
- Join with type coercion

#### 3.1.6 Filters (2 tests)
- Variable op Expression
- Constant op Constant (compile-time eval)

#### 3.1.7 Recursion (3 tests)
- Four-way mutual recursion
- Recursion depth limit test
- Right-linear vs left-linear

#### 3.1.8 Meta Commands (6 tests)
- .rule drop non-existent
- .session drop invalid index
- .load syntax error in file
- .load circular dependency
- .load empty file
- .load mode verification

#### 3.1.9 Session Management (3 tests)
- Session shadows persistent
- Persistent referencing session
- KG data isolation

#### 3.1.10 Schema (3 tests)
- Schema type mismatch
- Schema on insert
- Schema persistence

#### 3.1.11 Error Handling (6 tests)
- Missing period
- Unbalanced parentheses
- Invalid identifier
- Unbound head variable
- Unbound comparison variable
- Function call in rule head

#### 3.1.12 Edge Cases (4 tests)
- Single row result
- Very large result set
- Very wide tuples (20+ columns)
- Relation with 1 column

### 3.2 Comments & Syntax (10 tests)
- Nested block comments
- Comment at end of line
- Comment between statements
- Comment inside rule
- Empty comment
- Whitespace handling (5 tests)

### 3.3 Record Types & Field Access (10 tests)
**Note**: These appear in AST but parser doesn't support them!
- Simple field access (U.id)
- Chained field access
- Field access in head/body/query
- Record pattern destructuring
- Partial/nested record patterns

### 3.4 Advanced Type System (10 tests)
- List type (list[T])
- Type alias chain
- Recursive type definition
- Range refinement
- Pattern refinement
- Multiple refinements

### 3.5 Delete Operations Extended (5 tests)
- Delete entire relation (-name.)
- Delete rule
- Delete with dependencies

### 3.6 Function Calls Advanced (7 tests)
- Nested function calls (2-3 levels)
- Unknown function error
- Wrong argument count/type

### 3.7 Parsing Edge Cases (12 tests)
- Scientific notation negative exp
- Leading zeros
- Escape sequences (\n, \t, \\, \")
- Very long identifiers
- Unicode identifiers
- Tab whitespace

### 3.8 Feature Interaction Matrix (22 tests)
- Negation + Recursion + Aggregation combos (5 tests)
- Arithmetic + Aggregation + Joins combos (4 tests)
- Vectors + Joins + Filters combos (4 tests)
- Schema + Negation + Delete combos (3 tests)
- Session + Persistent + Views combos (4 tests)
- Recursion + Arithmetic + Aggregation combos (2 tests)

---

## Phase 4: P3 Low - Polish & Scale (Est: 220 tests)

**Goal**: Stress testing, edge cases, infrastructure tests.
**Priority**: LOW - Nice to have for v1.0.

### 4.1 Large Scale & Stress Tests (10 tests)
- 10K, 100K, 1M row datasets
- Wide tuples (20, 50 columns)
- 1MB strings
- 10K dimension vectors
- 100+ relations/rules

### 4.2 Transaction Semantics (5 tests)
- Atomic insert (all or nothing)
- Rollback on error
- Partial batch failure
- State after failures

### 4.3 Concurrency & Parallelism (6 tests)
- Parallel query execution
- Concurrent inserts
- Session/KG isolation under concurrency

### 4.4 Storage & WAL (15 tests)
- WAL append/read/replay
- WAL compaction
- Persistence layer tests
- Consolidation tests

### 4.5 Query Cache (8 tests)
- Cache hit/miss
- TTL expiration
- LRU eviction
- Cache invalidation

### 4.6 Optimizer Passes (9 tests)
- Each optimization pass in isolation
- Optimization idempotency

### 4.7 Join Planning (7 tests)
- Star/chain query patterns
- MST construction
- Cost calculation

### 4.8 Client/Server Protocol (8 tests)
- Connection handling
- Heartbeat
- Reconnection

### 4.9 CLI Argument Testing (16 tests)
- Server CLI flags
- Client CLI flags
- Environment variables

### 4.10 IR-Level Functions (8 tests)
- Internal functions not exposed via syntax

### 4.11 String Functions (10 tests) - Roadmap
- upper/lower/strlen/concat etc.
- **Note**: NOT IMPLEMENTED yet - skip until implemented

### 4.12 Additional Math Functions (8 tests) - Roadmap
- floor/ceil/round/sqrt/pow etc.
- **Note**: NOT IMPLEMENTED yet - skip until implemented

### 4.13 External Data Loading (11 tests)
- JSON/Parquet/CSV loading
- Schema inference
- **Note**: May NOT be IMPLEMENTED - verify first

---

## Implementation Order & Dependencies

```
Phase 1 (P0 Critical) - No dependencies, start immediately
├── 1.1 Panic Path Coverage
├── 1.2 Concurrent Access Testing
├── 1.3 Crash Recovery Testing
├── 1.4 Critical Error Variants
└── 1.5 Boundary Value Testing

Phase 2 (P1 High) - Can parallelize
├── 2.1 REST API Coverage (needs running server)
├── 2.2 Configuration Testing (needs config infrastructure)
├── 2.3 Error Variant Coverage (no deps)
├── 2.4 Public Method Coverage (no deps)
├── 2.5 Serialization Round-trip (no deps)
└── 2.6 Numeric Edge Cases (no deps)

Phase 3 (P2 Medium) - Snapshot tests, can parallelize
├── 3.1 Language Feature Snapshots (no deps)
├── 3.2-3.7 Syntax Tests (no deps)
└── 3.8 Feature Interaction Matrix (depends on features working)

Phase 4 (P3 Low) - Infrastructure heavy
├── 4.1-4.3 Stress Tests (needs test harness)
├── 4.4-4.8 Infrastructure Tests (module-specific)
└── 4.9-4.13 CLI/Roadmap (some not implemented)
```

---

## Resource Estimates

### By Test Type

| Type | Count | Avg Time | Total Estimate |
|------|-------|----------|----------------|
| Snapshot tests (.dl) | ~350 | 5 min | ~30 hours |
| Unit tests (Rust) | ~200 | 15 min | ~50 hours |
| Integration tests | ~60 | 30 min | ~30 hours |
| Stress tests | ~40 | 60 min | ~40 hours |
| **Total** | **649** | - | **~150 hours** |

### By Phase

| Phase | Tests | Estimate | Cumulative Coverage |
|-------|-------|----------|---------------------|
| Phase 1 (P0) | 80 | 30 hours | 51% |
| Phase 2 (P1) | 150 | 45 hours | 64% |
| Phase 3 (P2) | 200 | 45 hours | 81% |
| Phase 4 (P3) | 220 | 30 hours | 100% |

---

## Quick Wins - Low Effort, High Value

These 50 tests can be implemented quickly with high impact:

### Snapshot Tests (5 min each)
1. `12_errors/21_mutual_negation_cycle.dl` - Stratification error
2. `12_errors/22_insert_into_view.dl` - Semantic error
3. `12_errors/23_unbound_head_var.dl` - Safety check
4. `12_errors/24_missing_period.dl` - Parse error
5. `12_errors/25_unbalanced_parens.dl` - Parse error
6. `14_aggregations/17_avg_empty_group.dl` - Critical edge case
7. `14_aggregations/18_sum_overflow.dl` - Overflow handling
8. `14_aggregations/19_unknown_aggregate.dl` - Error message
9. `11_types/15_int64_min.dl` - Boundary test
10. `11_types/16_int64_max.dl` - Boundary test
11. `09_recursion/17_four_way_mutual.dl` - Complex recursion
12. `15_arithmetic/18_chained_ops.dl` - A+B+C+D
13. `15_arithmetic/19_unary_minus.dl` - Negative prefix
14. `08_negation/29_three_way_cycle.dl` - Cycle detection
15. `17_rule_commands/09_drop_nonexistent.dl` - Error case

### Unit Tests (15 min each)
16. `Value::Int64` JSON roundtrip
17. `Value::String` JSON roundtrip
18. `Value::Vector` JSON roundtrip
19. `InputLayerError::Timeout` trigger
20. `InputLayerError::ResourceLimitExceeded` trigger
21. `StorageError::Io` trigger
22. `QueryCache::insert()` basic
23. `QueryCache::get()` hit/miss
24. `Optimizer::optimize()` no-op
25. `PersistentStore::new()` basic

---

## Verification Checklist

After implementing all tests, verify:

- [ ] All 1157 tests pass: `cargo test`
- [ ] All 318 snapshot tests pass: `./scripts/run_snapshot_tests.sh`
- [ ] No panics on malformed input: Fuzz testing
- [ ] No data loss on crash: Crash testing
- [ ] Concurrency safe: Multi-threaded stress test
- [ ] Configuration works: Each option tested
- [ ] REST API works: All endpoints tested
- [ ] Error messages clear: Each error variant tested

---

## Maintenance

This plan should be updated when:
- Tests are implemented (update counts)
- New features are added (add test requirements)
- Bugs are found (add regression tests)
- Priorities change (reorder phases)

**Owner**: Engineering Team
**Review**: After each phase completion
