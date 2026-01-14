# Snapshot Test Coverage Gap Analysis & Implementation Plan

**Created**: 2024-01-12
**Last Updated**: 2024-01-12
**Status**: In Progress

---

## Executive Summary

| Metric | Current Value |
|--------|---------------|
| Total Features in Codebase | **140+** |
| Total Snapshot Tests | **261** |
| Features with ZERO test coverage | **~25** |
| Tests showing "No results" (potential issues) | **42** |
| Tests capturing errors (false positives) | **36** |
| Estimated True Coverage | **~65-70%** |

---

## Completed Work

### Phase 1: Fix top_k Aggregation ✅
- [x] Fixed `split_by_comma()` to track angle brackets
- [x] Fixed `parse_aggregate()` to handle ranking aggregates
- [x] Updated `07_top_k.dl.out` snapshot
- [x] Tests pass: 261 snapshot tests

### Phase 2: Temporal Operation Tests ✅
- [x] Created `29_temporal/` directory with 11 tests
- [x] Tests created but show "No results" (functions not working through Datalog interface)
- [x] Need to debug temporal function evaluation

### Phase 3: Vector Operation Tests ✅
- [x] Added `07_normalize.dl`, `08_vec_add.dl`, `09_vec_scale.dl`, `10_lsh_bucket.dl`
- [x] Total: 10 vector tests

### Phase 4: Edge Case Tests ✅
- [x] Added `14_wildcard_patterns.dl`, `15_relation_operations.dl`
- [x] Total: 15 edge case tests

### Phase 5: Meta Command Tests ✅
- [x] Added `03_rel_commands.dl`, `04_session_commands.dl`
- [x] Tests passing

---

## 🔴 CRITICAL: Remaining Untested Features

### P0.1: Debug Temporal Function Evaluation
**Status**: NOT STARTED
**Priority**: CRITICAL

All 11 temporal tests show "No results". The functions exist but don't work through Datalog:

```
29_temporal/01_time_now.dl        → "No results"
29_temporal/02_time_diff.dl       → "No results"
... (all 11 tests)
```

**Root Cause Investigation Needed**:
- Temporal functions work in Rust unit tests
- `as_timestamp()` accepts both Timestamp and Int64 values
- Issue likely in how builtin functions are evaluated in rules

**Action Items**:
- [ ] Debug why vector functions work but temporal functions don't
- [ ] Check `src/code_generator/mod.rs` builtin function evaluation
- [ ] Fix the evaluation path
- [ ] Verify tests produce actual results

---

### P0.2: Int8 Quantization Tests
**Status**: NOT STARTED
**Priority**: CRITICAL

4 production functions with ZERO coverage (75% memory savings feature):

| Function | Purpose | Test File |
|----------|---------|-----------|
| `quantize_linear(v)` | Linear scaling to int8 | `30_quantization/01_quantize_linear.dl` |
| `quantize_symmetric(v)` | Symmetric scaling | `30_quantization/02_quantize_symmetric.dl` |
| `dequantize(v)` | Int8 back to f32 | `30_quantization/03_dequantize.dl` |
| `dequantize_scaled(v, scale)` | Scaled dequantization | `30_quantization/04_dequantize_scaled.dl` |

**Action Items**:
- [ ] Create `examples/datalog/30_quantization/` directory
- [ ] Create test files for each function
- [ ] Run `make test-all`

---

### P0.3: Int8 Distance Function Tests
**Status**: NOT STARTED
**Priority**: CRITICAL

4 native int8 distance functions with ZERO coverage:

| Function | Purpose | Test File |
|----------|---------|-----------|
| `euclidean_int8(v1, v2)` | L2 distance | `30_quantization/05_euclidean_int8.dl` |
| `cosine_int8(v1, v2)` | Cosine similarity | `30_quantization/06_cosine_int8.dl` |
| `dot_product_int8(v1, v2)` | Dot product | `30_quantization/07_dot_product_int8.dl` |
| `manhattan_int8(v1, v2)` | L1 distance | `30_quantization/08_manhattan_int8.dl` |

---

## 🟠 HIGH: Feature Completeness

### P1.1: Multi-Probe LSH Tests
**Status**: NOT STARTED

5 advanced LSH functions with ZERO coverage:

| Function | Purpose | Test File |
|----------|---------|-----------|
| `lsh_probes(bucket, num)` | Probe sequence | `16_vectors/11_lsh_probes.dl` |
| `lsh_bucket_with_distances(...)` | Bucket + boundaries | `16_vectors/12_lsh_bucket_distances.dl` |
| `lsh_probes_ranked(...)` | Smart ordering | `16_vectors/13_lsh_probes_ranked.dl` |
| `lsh_multi_probe(...)` | Combined op | `16_vectors/14_lsh_multi_probe.dl` |
| `lsh_multi_probe_int8(...)` | Int8 version | `30_quantization/09_lsh_multi_probe_int8.dl` |

---

### P1.2: Math Utility Tests
**Status**: NOT STARTED

| Function | Purpose | Test File |
|----------|---------|-----------|
| `abs_int64(x)` | Absolute value (int) | `15_arithmetic/18_abs_int.dl` |
| `abs_float64(x)` | Absolute value (float) | `15_arithmetic/19_abs_float.dl` |

---

### P1.3: Missing Meta Command Tests
**Status**: NOT STARTED

| Command | Purpose | Test File |
|---------|---------|-----------|
| `.session drop <n>` | Drop specific rule | `01_knowledge_graph/05_session_drop.dl` |
| `.load --replace` | Atomic replace | `25_unified_prefix/06_load_replace.dl` |
| `.load --merge` | Merge mode | `25_unified_prefix/07_load_merge.dl` |
| `.compact` | Storage compaction | `01_knowledge_graph/06_compact.dl` |

---

### P1.4: Missing Temporal Function
**Status**: NOT STARTED

| Function | Purpose | Test File |
|----------|---------|-----------|
| `point_in_interval(ts, start, end)` | Point check | `29_temporal/12_point_in_interval.dl` |

---

## 🟡 MEDIUM: Edge Cases

### P2.1: Vector Edge Cases

| Edge Case | Test File |
|-----------|-----------|
| Empty vectors `[]` | `16_vectors/15_empty_vector.dl` |
| Single-element vectors | `16_vectors/16_single_element.dl` |
| High dimensional (1000+) | `16_vectors/17_high_dim.dl` |
| Zero vectors | `16_vectors/18_zero_vector.dl` |
| NaN/Infinity handling | `16_vectors/19_special_values.dl` |

---

### P2.2: Aggregation Edge Cases

| Edge Case | Test File |
|-----------|-----------|
| AVG of single value | `14_aggregations/17_avg_single.dl` |
| SUM overflow | `14_aggregations/18_sum_overflow.dl` |
| COUNT with NULL | `14_aggregations/19_count_null.dl` |
| MIN/MAX with ties | `14_aggregations/20_minmax_ties.dl` |

---

### P2.3: Join Edge Cases

| Edge Case | Test File |
|-----------|-----------|
| 4+ way joins | `06_joins/06_four_way_join.dl` |
| Self-join with aggregation | `06_joins/07_self_join_agg.dl` |

---

## 🔵 LOW: Polish

### P3.1: Audit False Positive Tests
- [ ] Review all 42 "No results" tests
- [ ] Determine if legitimate or broken
- [ ] Fix or document each

### P3.2: Performance Tests
- [ ] Large dataset (10K+ rows)
- [ ] Deep recursion limits
- [ ] Complex join performance

---

## Implementation Checklist

### Current Sprint: P0 Critical Fixes

- [ ] **P0.1**: Debug temporal function evaluation
  - [ ] Investigate code_generator builtin handling
  - [ ] Compare with working vector functions
  - [ ] Fix the evaluation path
  - [ ] Verify 11 temporal tests produce results
  - [ ] Run `make test-all`

- [ ] **P0.2**: Int8 Quantization Tests
  - [ ] Create `30_quantization/` directory
  - [ ] Create 4 quantization test files
  - [ ] Run `make test-all`

- [ ] **P0.3**: Int8 Distance Tests
  - [ ] Create 4 int8 distance test files
  - [ ] Run `make test-all`

### Next Sprint: P1 Feature Completeness

- [ ] **P1.1**: Multi-probe LSH tests (5 files)
- [ ] **P1.2**: Math utility tests (2 files)
- [ ] **P1.3**: Meta command tests (4 files)
- [ ] **P1.4**: point_in_interval test (1 file)

### Future: P2 Edge Cases

- [ ] **P2.1**: Vector edge cases (5 files)
- [ ] **P2.2**: Aggregation edge cases (4 files)
- [ ] **P2.3**: Join edge cases (2 files)

---

## Test Verification Rule

**CRITICAL**: After EVERY change, run:
```bash
make test-all
```

Current baseline:
- Unit tests: 1018 passed
- Snapshot tests: 261 passed

**NEVER proceed to the next item until ALL tests pass.**

---

## Estimated Remaining Work

| Priority | Category | New Tests |
|----------|----------|-----------|
| P0 | Critical fixes | ~12 files |
| P1 | Feature completeness | ~12 files |
| P2 | Edge cases | ~11 files |
| P3 | Polish | Audit existing |
| **Total** | | **~35 new files** |

Plus debugging temporal functions (P0.1) which may require code fixes.
