# Test Execution Methodology

A rigorous process for implementing 649 tests without breaking anything.

---

## Core Principles

### 1. Never Break Green
- Test suite must pass before AND after each change
- If tests fail, fix immediately before continuing
- No "temporary" failures allowed

### 2. Small Batches
- Maximum 5 tests per batch
- Full verification after each batch
- Git commit at each checkpoint

### 3. Validate What You Test
- Each test must be manually verified to test what it claims
- Run test in isolation to confirm behavior
- Document expected vs actual behavior
- If you find a bug, fix it immediately and verify that everything works

### 4. Defense in Depth
- Multiple verification layers
- Automated + manual checks
- Cross-reference with codebase behavior

---

## Execution Workflow

```
┌─────────────────────────────────────────────────────────────┐
│                    FOR EACH TEST BATCH                       │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  1. BASELINE CHECK                                           │
│     └── make test-all (must pass)                           │
│     └── git status (clean working tree)                     │
│                                                              │
│  2. IMPLEMENT TESTS (max 5)                                  │
│     └── Write test file                                      │
│     └── Generate expected output (for snapshots)            │
│     └── Run individual test                                  │
│     └── Verify test behavior manually                        │
│                                                              │
│  3. VERIFICATION GATE                                        │
│     └── make test-all (must still pass)                     │
│     └── Run new tests in isolation                          │
│     └── Cross-check: does test actually test the claim?     │
│                                                              │
│  4. CHECKPOINT                                               │
│     └── git add -A                                          │
│     └── git commit -m "test: [category] add X tests"        │
│     └── Update TEST_COVERAGE_GAPS.md                        │
│                                                              │
│  5. VALIDATION REVIEW                                        │
│     └── Re-read test: is logic correct?                     │
│     └── Edge case check: did we miss anything?              │
│     └── False positive check: would test pass if broken?    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## Detailed Process

### Step 1: Baseline Check

Before ANY work, verify current state:

```bash
# Must be clean
git status

# Must pass - this is our baseline
make test-all

# Record baseline metrics
cargo test 2>&1 | grep -E "^test result" > /tmp/baseline.txt
./scripts/run_snapshot_tests.sh 2>&1 | tail -5 >> /tmp/baseline.txt

# Commit baseline if needed
git stash  # if uncommitted changes
```

**STOP if baseline fails.** Fix existing issues first.

### Step 2: Implement Tests (Batch of 5)

#### For Snapshot Tests (.dl files):

```bash
# 1. Create test file
cat > examples/datalog/12_errors/21_mutual_negation_cycle.dl << 'EOF'
% Test: Mutual negation cycle detection
% Expected: Error about circular negation dependency

.kg create test_mutual_neg
.kg use test_mutual_neg

% Define mutually negating rules - should error
+a(X) :- b(X), !c(X).
+c(X) :- d(X), !a(X).

+b[(1), (2), (3)].
+d[(1), (2), (3)].

% This query should fail due to stratification
?- a(X).

.kg use default
.kg drop test_mutual_neg
EOF

# 2. Generate expected output by running it
cargo run --bin inputlayer-client --release --quiet -- --script \
  < examples/datalog/12_errors/21_mutual_negation_cycle.dl \
  > examples/datalog/12_errors/21_mutual_negation_cycle.dl.out 2>&1

# 3. CRITICAL: Review the output - is it what we expect?
cat examples/datalog/12_errors/21_mutual_negation_cycle.dl.out

# 4. If output shows ERROR as expected, good. If not, investigate.
```

#### For Unit Tests (Rust):

```rust
// 1. Add test to appropriate module
#[cfg(test)]
mod tests {
    #[test]
    fn test_value_int64_json_roundtrip() {
        let original = Value::Int64(i64::MAX);
        let json = serde_json::to_string(&original).unwrap();
        let parsed: Value = serde_json::from_str(&json).unwrap();

        // CRITICAL: Assert specific behavior, not just "no panic"
        assert_eq!(original, parsed, "INT64_MAX should roundtrip exactly");
    }
}

// 2. Run in isolation first
// cargo test test_value_int64_json_roundtrip -- --nocapture

// 3. Verify output shows expected behavior
```

### Step 3: Verification Gate

**This is the critical step that prevents regressions.**

```bash
# Run full test suite
make test-all

# Compare with baseline - should have MORE passing tests, not fewer
cargo test 2>&1 | grep -E "^test result" > /tmp/current.txt
diff /tmp/baseline.txt /tmp/current.txt

# Run new tests in isolation to verify they actually test something
cargo test test_value_int64_json_roundtrip -- --nocapture

# For snapshot tests, run individually
./scripts/run_snapshot_tests.sh examples/datalog/12_errors/21_mutual_negation_cycle.dl
```

**Verification Checklist:**
- [ ] `make test-all` passes
- [ ] New tests pass
- [ ] No existing tests broke
- [ ] Test output matches expected behavior
- [ ] Test would FAIL if the feature was broken

### Step 4: Checkpoint

```bash
# Stage all changes
git add -A

# Commit with descriptive message
git commit -m "test(errors): add mutual negation cycle detection test

- Add 12_errors/21_mutual_negation_cycle.dl
- Verifies stratification rejects circular negation
- Part of Phase 1 P0 Critical tests

Coverage: 509/1157 (44%)"

# Update tracking document
# Edit docs/TEST_COVERAGE_GAPS.md - mark test as ✅
```

### Step 5: Validation Review

**Self-review checklist for each test:**

```markdown
## Test Review: 21_mutual_negation_cycle.dl

### What does this test claim to verify?
Mutual negation cycles are detected and rejected.

### Does the test actually verify this?
YES - Creates a(X) :- !c(X) and c(X) :- !a(X), expects error.

### Would this test pass if the feature was broken?
NO - If stratification check was removed, query would run (and possibly infinite loop).

### Edge cases covered?
- [x] Basic mutual negation
- [ ] Three-way cycle (separate test)
- [ ] Cycle through views (separate test)

### Test quality score: 4/5
- Clear setup
- Clear expectation
- Isolated from other features
- Good error message check
- Could add more cycle variants
```

---

## Batch Organization

### Batch Size Rules

| Test Type | Max per Batch | Reason |
|-----------|---------------|--------|
| Snapshot tests | 5 | Quick to verify |
| Unit tests | 3 | Need more careful review |
| Integration tests | 2 | Complex setup/teardown |
| Stress tests | 1 | Long running, risky |

### Batch Grouping Strategy

Group related tests together for efficiency:

```
Batch 1: Error Messages (5 snapshot tests)
├── 12_errors/21_mutual_negation_cycle.dl
├── 12_errors/22_insert_into_view.dl
├── 12_errors/23_unbound_head_var.dl
├── 12_errors/24_missing_period.dl
└── 12_errors/25_unbalanced_parens.dl

Batch 2: Boundary Values (5 snapshot tests)
├── 11_types/15_int64_min.dl
├── 11_types/16_int64_max.dl
├── 11_types/17_float_infinity.dl
├── 11_types/18_float_nan.dl
└── 11_types/19_zero_vector.dl

Batch 3: Value Serialization (3 unit tests)
├── test_value_int64_json_roundtrip
├── test_value_string_json_roundtrip
└── test_value_vector_json_roundtrip
```

---

## Failure Recovery

### If `make test-all` Fails After Changes

```bash
# 1. Identify which tests failed
make test-all 2>&1 | grep -E "FAILED|error"

# 2. Check if it's a new test or existing test
git diff --name-only  # See what changed

# 3. If new test is wrong, fix it
# 4. If existing test broke, STOP and investigate

# 5. If stuck, rollback to last checkpoint
git checkout -- .  # Discard changes
# OR
git reset --hard HEAD~1  # Undo last commit
```

### If Test Passes But Behavior Is Wrong

```bash
# 1. Run test with verbose output
cargo test test_name -- --nocapture

# 2. Manually verify the behavior
cargo run --bin inputlayer-client --release

# 3. If test is wrong, fix the test
# 4. If code is wrong, file a bug (don't fix in test PR)
```

---

## Daily Workflow

```
Morning:
├── git pull (get any upstream changes)
├── make test-all (verify baseline)
└── Review today's batch plan

Work Session (repeat):
├── Implement batch of 5 tests
├── Run verification gate
├── Commit checkpoint
├── Update tracking docs
└── Take break (fresh eyes for next batch)

End of Day:
├── make test-all (final verification)
├── Push commits to branch
├── Update TEST_COVERAGE_GAPS.md totals
└── Plan tomorrow's batches
```

---

## Quality Gates

### Before Merging Any Test PR

1. **Automated Checks**
   - [ ] `make test-all` passes
   - [ ] `cargo clippy` has no new warnings
   - [ ] `cargo fmt --check` passes

2. **Manual Review**
   - [ ] Each test has clear purpose
   - [ ] Each test would fail if feature broke
   - [ ] No false positives (tests that always pass)
   - [ ] No flaky tests (random pass/fail)

3. **Documentation**
   - [ ] TEST_COVERAGE_GAPS.md updated
   - [ ] Commit messages descriptive
   - [ ] Complex tests have comments

### Weekly Validation

```bash
# Full regression check
make clean && make test-all

# Coverage report (if available)
cargo tarpaulin --out Html

# Snapshot test audit - verify .out files match behavior
./scripts/run_snapshot_tests.sh --regenerate
git diff examples/  # Should be empty if tests are correct
```

---

## Anti-Patterns to Avoid

### DON'T: Write Tests That Always Pass

```rust
// BAD - This test passes even if serialization is broken
#[test]
fn test_value_serialization() {
    let v = Value::Int64(42);
    let json = serde_json::to_string(&v);
    assert!(json.is_ok());  // Too weak!
}

// GOOD - This test verifies actual behavior
#[test]
fn test_value_int64_serialization() {
    let v = Value::Int64(42);
    let json = serde_json::to_string(&v).unwrap();
    assert_eq!(json, "42");  // Specific expected output

    let parsed: Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, v);  // Roundtrip verification
}
```

### DON'T: Ignore Test Failures

```bash
# BAD
make test-all || true  # Ignoring failures!

# GOOD
make test-all  # Fail the script if tests fail
```

### DON'T: Commit Without Running Tests

```bash
# BAD
git add -A && git commit -m "add tests"

# GOOD
make test-all && git add -A && git commit -m "test: add X tests"
```

### DON'T: Write Tests for Unimplemented Features

```rust
// BAD - Feature doesn't exist yet
#[test]
fn test_string_upper_function() {
    // upper() is in ROADMAP, not implemented
    assert_eq!(eval("upper('hello')"), "HELLO");
}

// GOOD - Test what exists
#[test]
fn test_abs_int64_function() {
    // abs_int64() is implemented
    assert_eq!(eval("abs_int64(-42)"), 42);
}
```

---

## Metrics & Tracking

### After Each Batch

Update `docs/TEST_COVERAGE_GAPS.md`:

```markdown
**Last Updated**: 2026-01-14
**Total Test Cases Tracked**: 1157
**Implemented**: 513 (44%)  ← Update this
**Missing**: 644 (56%)      ← Update this
```

### Weekly Summary

```markdown
## Week of 2026-01-14

### Progress
- Tests implemented: 25
- Tests remaining: 624
- Coverage: 46% → 48%

### Batches Completed
- Batch 1: Error Messages (5) ✅
- Batch 2: Boundary Values (5) ✅
- Batch 3: Value Serialization (3) ✅
- Batch 4: Aggregation Errors (5) ✅
- Batch 5: Panic Paths (5) ✅
- Batch 6: (in progress)

### Issues Found
- AVG of empty group returns NaN, not error (filed #123)
- .load --replace flag is ignored (known issue)

### Next Week Plan
- Phase 1 completion (55 tests remaining)
- Start Phase 2 REST API tests
```

---

## Emergency Procedures

### If Production Code Needs to Change

Sometimes tests reveal bugs. Handle carefully:

1. **Document the bug** - Create issue with test that exposes it
2. **Don't fix in test PR** - Keep test-only changes separate
3. **Mark test as expected failure** if needed temporarily
4. **Fix code in separate PR** with its own review

### If Test Suite Takes Too Long

```bash
# Run only new tests during development
cargo test test_value_ -- --nocapture

# Run full suite before commit
make test-all

# Consider parallelization
cargo test -- --test-threads=4
```

### If Snapshot Test Output Changes Unexpectedly

```bash
# 1. Check if code changed behavior
git log --oneline -10 src/

# 2. If behavior change is intentional, regenerate
cargo run --bin inputlayer-client --release --quiet -- --script \
  < examples/datalog/XX_category/YY_test.dl \
  > examples/datalog/XX_category/YY_test.dl.out 2>&1

# 3. If behavior change is a bug, file issue and keep old .out
```

---

## Success Criteria

### Phase Complete When:

1. All planned tests implemented
2. `make test-all` passes
3. All tests in TEST_COVERAGE_GAPS.md marked ✅
4. No known false positives
5. Coverage metrics updated
6. Commits clean and atomic

### Project Complete When:

1. 1157/1157 tests implemented (100%)
2. Full test suite runs in < 10 minutes
3. No flaky tests
4. Documentation complete
5. CI/CD integration verified
