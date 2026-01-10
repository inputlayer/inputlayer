# InputLayer Production Readiness Tasks

Based on the comprehensive review findings, here are the prioritized tasks broken down into actionable items.

---

## P0 - Critical (Block Deployment)

### TASK-001: Fix Path Traversal Vulnerability in `.load` Command
**Priority**: P0 | **Effort**: Medium | **Risk**: Security Critical

**Files to modify**:
- `src/bin/client.rs:842-844` (handle_load function)
- `src/statement/meta.rs:196-212` (parse_load_command)

**Requirements**:
1. Create `validate_path()` function that:
   - Canonicalizes the path using `std::path::Path::canonicalize()`
   - Validates path is within allowed directory (data_dir or cwd)
   - Rejects paths containing `..` components
   - Rejects absolute paths unless explicitly allowed
2. Apply validation in `handle_load()` before `fs::read_to_string()`
3. Apply validation in script execution path (`execute_script()` at line 308)
4. Add tests for path traversal attempts

**Acceptance Criteria**:
- [ ] `.load /etc/passwd` returns "Access denied" error
- [ ] `.load ../../../etc/shadow` returns "Access denied" error
- [ ] `.load valid_file.dl` in current directory works
- [ ] Tests cover path traversal scenarios

---

### TASK-002: Fix Path Traversal in Script Execution
**Priority**: P0 | **Effort**: Small | **Risk**: Security Critical

**Files to modify**:
- `src/bin/client.rs:308-310` (execute_script function)

**Requirements**:
1. Reuse `validate_path()` from TASK-001
2. Apply to `--script` argument and positional script paths

**Acceptance Criteria**:
- [ ] Script paths are validated before execution
- [ ] Path traversal blocked for scripts

---

### TASK-003: Replace Critical panic! Calls with Result Returns
**Priority**: P0 | **Effort**: Large | **Risk**: Stability Critical

**Files to modify**:
- `src/sip_rewriting/mod.rs:605,637` (2 panics)
- `src/join_planning/mod.rs:950` (1 panic)
- `src/rule_catalog.rs:732` (1 panic)
- `src/recursion.rs:380,944` (2 panics)

**Requirements**:
1. Define error types for each module (or use existing)
2. Replace `panic!()` with `Err()` returns
3. Propagate errors up the call stack
4. Ensure callers handle the new Result types

**Acceptance Criteria**:
- [ ] No panic! in sip_rewriting production paths
- [ ] No panic! in join_planning production paths
- [ ] No panic! in rule_catalog production paths
- [ ] No panic! in recursion production paths
- [ ] All tests still pass

---

### TASK-004: Add Temporal Operations Test Suite
**Priority**: P0 | **Effort**: Large | **Risk**: Quality Critical

**Files to create**:
- `examples/datalog/29_temporal_ops/01_time_functions.dl`
- `examples/datalog/29_temporal_ops/02_time_decay.dl`
- `examples/datalog/29_temporal_ops/03_intervals.dl`

**Functions to test** (13 total):
- `time_now`, `time_diff`, `time_add`, `time_sub`
- `time_decay`, `time_decay_linear`
- `time_before`, `time_after`, `time_between`
- `within_last`, `intervals_overlap`, `interval_contains`
- `interval_duration`, `point_in_interval`

**Requirements**:
1. Create test file for basic time functions
2. Create test file for decay functions
3. Create test file for interval operations
4. Include edge cases (zero duration, negative, boundaries)
5. Generate snapshots

**Acceptance Criteria**:
- [ ] All 13 temporal functions have at least 1 test
- [ ] Edge cases covered (empty, zero, negative)
- [ ] Snapshots generated and passing

---

## P1 - High (Fix Before Beta)

### TASK-005: Fix Silent Error Ignores in Data Operations
**Priority**: P1 | **Effort**: Medium | **Risk**: Data Integrity

**Files to modify**:
- `src/bin/client.rs:992,1040,1234` (delete operations)
- `src/bin/client.rs:1243` (insert operation)

**Requirements**:
1. Replace `let _ = state.storage.delete(...)` with proper error handling
2. Replace `let _ = state.storage.insert(...)` with proper error handling
3. Report errors to user with clear messages
4. Consider whether to continue or abort on error

**Acceptance Criteria**:
- [ ] Failed deletes report error to user
- [ ] Failed inserts report error to user
- [ ] No silent `let _ =` on data operations

---

### TASK-006: Add Line/Column Context to Parse Errors
**Priority**: P1 | **Effort**: Large | **Risk**: UX Critical

**Files to modify**:
- `src/parser/mod.rs` (main parser)
- `src/statement/parser.rs` (statement parser)

**Requirements**:
1. Track line/column during parsing
2. Include position in error messages
3. Format: `"Parse error at line 5, column 12: Invalid atom"`
4. Apply to all `Err(format!(...))` calls in parser

**Acceptance Criteria**:
- [ ] Parse errors include line number
- [ ] Parse errors include column number
- [ ] Error messages are actionable

---

### TASK-007: Fix Unsafe Default Fallbacks in Join Planning
**Priority**: P1 | **Effort**: Small | **Risk**: Correctness Critical

**Files to modify**:
- `src/join_planning/mod.rs:821-822`

**Current code**:
```rust
left_schema.iter().position(...).unwrap_or(0)  // WRONG!
right_schema.iter().position(...).unwrap_or(0) // WRONG!
```

**Requirements**:
1. Replace `unwrap_or(0)` with proper error handling
2. Return error if key not found in schema
3. Add test case for missing key scenario

**Acceptance Criteria**:
- [ ] Missing key returns error, not wrong column
- [ ] Test verifies error on missing key

---

### TASK-008: Add File Size Limits for Loading
**Priority**: P1 | **Effort**: Small | **Risk**: DoS Prevention

**Files to modify**:
- `src/bin/client.rs:842` (handle_load)
- `src/storage/csv.rs:68` (CSV reader)

**Requirements**:
1. Add configurable max file size (default: 100MB)
2. Check file size before reading
3. Return clear error if exceeded
4. Apply to `.load` command and CSV import

**Acceptance Criteria**:
- [ ] Files > max size rejected with clear error
- [ ] Max size is configurable
- [ ] Applied to all file loading paths

---

### TASK-009: Add Vector Operations Test Coverage
**Priority**: P1 | **Effort**: Medium | **Risk**: Quality

**Files to create**:
- `examples/datalog/30_vector_advanced/01_lsh_bucket.dl`
- `examples/datalog/30_vector_advanced/02_vector_ops.dl`

**Functions to test**:
- `lsh_bucket` - Locality-sensitive hashing
- `normalize` - Vector normalization
- `vec_add` - Vector addition
- `vec_scale` - Vector scaling
- `vec_dim` - Vector dimension

**Acceptance Criteria**:
- [ ] All 5 vector functions tested
- [ ] Edge cases (empty vector, single element) covered

---

## P2 - Medium (Fix Before Production)

### TASK-010: Standardize Error Message Formats
**Priority**: P2 | **Effort**: Medium | **Risk**: UX

**Files to modify**:
- `src/bin/client.rs` (multiple locations)
- `src/parser/mod.rs`

**Requirements**:
1. Define error message conventions:
   - Prefix: `"Error: "` for generic, `"Parse error: "` for parsing
   - Include actionable guidance where possible
   - End with period
2. Apply consistently across codebase

**Acceptance Criteria**:
- [ ] All errors follow consistent format
- [ ] Actionable errors include guidance

---

### TASK-011: Fix Documentation Output Format Mismatches
**Priority**: P2 | **Effort**: Small | **Risk**: Documentation

**Files to modify**:
- `docs/reference/commands.md`
- OR `src/bin/client.rs` (implementation)

**Mismatches to fix**:
| Feature | Doc says | Code does | Fix |
|---------|----------|-----------|-----|
| `.kg list` marker | `(current)` | `*` | Update docs or code |
| `.rel` output | Shows counts | No counts | Add counts to code |
| Query results | `Results: N rows` | `[N] rows:` | Standardize |

**Requirements**:
1. Decide on canonical format for each
2. Update either docs or implementation
3. Update snapshot tests if implementation changes

**Acceptance Criteria**:
- [ ] Documentation matches implementation
- [ ] Snapshots updated if needed

---

### TASK-012: Make Hardcoded Values Configurable
**Priority**: P2 | **Effort**: Medium | **Risk**: Operations

**Files to modify**:
- `src/bin/server.rs:40-42` (server defaults)
- `src/protocol/mod.rs:98-104` (protocol constants)
- `src/config.rs` (add new config options)

**Values to make configurable**:
- Server address (currently `127.0.0.1:5433`)
- Certificate paths
- Query timeout (currently `30_000ms`)
- Max message size (currently `16MB`)
- Optimizer max iterations (currently `10`)

**Acceptance Criteria**:
- [ ] Values configurable via config file
- [ ] Environment variable overrides work
- [ ] Sensible defaults preserved

---

### TASK-013: Simplify Storage API Surface
**Priority**: P2 | **Effort**: Large | **Risk**: API Breaking

**Files to modify**:
- `src/storage_engine/mod.rs`

**Current state**: 6 query methods, 4 insert methods

**Requirements**:
1. Create builder pattern or options struct
2. Reduce to 2 query methods: `execute_query(opts)`, `execute_query_tuples(opts)`
3. Reduce to 2 insert methods: `insert(opts)`, `insert_tuples(opts)`
4. Deprecate old methods with clear migration path

**Acceptance Criteria**:
- [ ] API surface reduced by 50%+
- [ ] Old methods deprecated (not removed)
- [ ] Migration guide documented

---

### TASK-014: Rename `clear_rule` to `reset_rule`
**Priority**: P2 | **Effort**: Small | **Risk**: API Breaking

**Files to modify**:
- `src/storage_engine/mod.rs`
- `src/statement/meta.rs`
- `docs/reference/commands.md`

**Requirements**:
1. Rename `clear_rule` → `reset_rule` in storage engine
2. Rename `.rule clear` → `.rule reset` in meta commands
3. Update documentation
4. Add deprecation alias for old name

**Acceptance Criteria**:
- [ ] New name in use
- [ ] Old name produces deprecation warning
- [ ] Docs updated

---

### TASK-015: Remove panic! from Parser Test Helpers
**Priority**: P2 | **Effort**: Medium | **Risk**: Code Quality

**Files to modify**:
- `src/statement/mod.rs` (39 panics in test helpers)

**Requirements**:
1. Move test helper functions to `#[cfg(test)]` module
2. Or convert to Result-returning functions
3. Ensure they're not callable from production code

**Acceptance Criteria**:
- [ ] Test helpers not in production binary
- [ ] Or return Result instead of panic

---

## P3 - Low (Technical Debt)

### TASK-016: Fix Clippy Warnings
**Priority**: P3 | **Effort**: Small | **Risk**: Code Quality

**Files to modify**:
- `src/parser/mod.rs:344,351,358` (redundant else)
- `src/ir_builder/mod.rs:784` (needless continue)
- `src/sip_rewriting/mod.rs:52` (unused import)
- `src/code_generator/mod.rs:91,103` (unused imports)

**Acceptance Criteria**:
- [ ] `cargo clippy` produces no warnings

---

### TASK-017: Remove Dead Code Annotations
**Priority**: P3 | **Effort**: Small | **Risk**: Code Quality

**Files to modify**:
- `src/statement/schema.rs:75,377,418`

**Requirements**:
1. Either use the dead code
2. Or remove it entirely

**Acceptance Criteria**:
- [ ] No `#[allow(dead_code)]` without justification

---

### TASK-018: Standardize Output Message Punctuation
**Priority**: P3 | **Effort**: Small | **Risk**: UX Polish

**Files to modify**:
- `src/bin/client.rs` (various output messages)

**Requirements**:
1. All success messages end with period
2. Consistent use of quotes around names
3. Consistent singular/plural handling

**Acceptance Criteria**:
- [ ] All messages follow style guide

---

### TASK-019: Update Stale Code Comments
**Priority**: P3 | **Effort**: Small | **Risk**: Documentation

**Files to modify**:
- `src/bin/client.rs:34` (remove `.save` reference)

**Requirements**:
1. Remove references to unimplemented features
2. Or implement the features

**Acceptance Criteria**:
- [ ] All code comments accurate

---

### TASK-020: Add Rate Limiting to REPL
**Priority**: P3 | **Effort**: Medium | **Risk**: DoS Prevention

**Files to modify**:
- `src/bin/client.rs` (main REPL loop)

**Requirements**:
1. Add configurable rate limit (commands per second)
2. Add query complexity estimation
3. Reject or queue expensive queries

**Acceptance Criteria**:
- [ ] Rate limiting configurable
- [ ] Rapid command spam handled gracefully

---

## Summary

| Priority | Tasks | Total Effort |
|----------|-------|--------------|
| P0 | 4 | ~3-4 days |
| P1 | 5 | ~4-5 days |
| P2 | 6 | ~5-6 days |
| P3 | 5 | ~2-3 days |
| **Total** | **20** | **~14-18 days** |

---

## Execution Order Recommendation

1. **Week 1**: P0 tasks (security + critical stability)
   - TASK-001, TASK-002 (security)
   - TASK-003 (panic removal)
   - TASK-004 (temporal tests)

2. **Week 2**: P1 tasks (high-impact quality)
   - TASK-005, TASK-006, TASK-007
   - TASK-008, TASK-009

3. **Week 3**: P2 tasks (production polish)
   - TASK-010 through TASK-015

4. **Ongoing**: P3 tasks (technical debt)
   - TASK-016 through TASK-020
