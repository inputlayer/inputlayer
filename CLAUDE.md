# Claude Code Project Guidelines

## Mandatory Rules

### 1. Test Verification After Every Change

**CRITICAL**: After EVERY code or test change, you MUST run:

```bash
make test-all
```

This ensures:
- All unit tests pass (950+ tests)
- All snapshot tests pass (290 tests)
- No regressions are introduced

**NEVER skip this step.** If tests fail, fix them before proceeding to the next change.

**IMPORTANT**: Only accept a situation where ALL tests are green (0 failures). A task is NOT complete until `make test-all` shows:
- Unit Tests: ALL passed
- Snapshot Tests: 290 passed, 0 failed
- make check: PASS

**Workflow**:
1. Make a code change
2. Run `make test-all`
3. If ANY tests fail → fix immediately
4. Only proceed to next change after ALL tests pass (0 failures)
5. Repeat for every single change
6. A task is ONLY complete when all tests are green

### 2. Snapshot Test Protocol

When adding or modifying snapshot tests:
1. Create the `.dl` test file
2. Run `./scripts/run_snapshot_tests.sh` to generate/update `.out` files
3. Verify the output is correct (not an error message being captured as "expected")
4. Run `make test-all` to confirm everything passes

### 3. Verify Feature Works Before Adding Test

Before writing a snapshot test for a feature:
1. Manually test the feature works in the REPL
2. Only then create the snapshot test
3. This prevents "false positive" tests that capture errors as expected output

## Project Structure

- `src/` - Rust source code
- `examples/datalog/` - Snapshot tests organized by category
- `tests/` - Integration tests
- `scripts/run_snapshot_tests.sh` - Snapshot test runner
- `docs/TEST_COVERAGE_GAPS.md` - **Exhaustive test coverage analysis** (434 test cases, tracks what exists vs missing)

## Test Commands

```bash
# Run all tests
make test-all

# Run only unit tests
cargo test

# Run only snapshot tests
./scripts/run_snapshot_tests.sh

# Update snapshot outputs
./scripts/run_snapshot_tests.sh --update
```
