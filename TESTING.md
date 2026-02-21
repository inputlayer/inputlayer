# Testing

InputLayer has three test tiers: unit tests, integration tests, and end-to-end snapshot tests.

## Quick Reference

```bash
make test-all       # Full verification: build + unit + snapshot (~70s, all CPUs)
make test-fast      # Unit tests only (~30s)
make test           # Unit + snapshot tests
make e2e-test       # Snapshot tests only (parallel)
make test-affected  # Run only snapshots affected by uncommitted changes
```

## Test Tiers

### Tier 1: Unit Tests

Standard Rust `#[test]` functions (~1560 tests). Cover the parser, IR builder, code generator, join planner, optimizer passes, and all built-in functions.

```bash
make unit-test          # cargo test --all-features
make test-release       # Same, in release mode
cargo test --all-features -- test_name  # Run a specific test
```

### Tier 2: Integration Tests

Rust integration tests in `tests/`. Exercise the engine end-to-end within a single process.

```bash
make integration-test   # cargo test --all-features --test '*'
```

### Tier 3: Snapshot Tests (E2E)

~995 Datalog scripts in `examples/datalog/` organized across 33 categories. Each `.idl` file has a corresponding `.idl.out` file with expected output. The test runner starts a server, executes each script via the client binary, and compares actual output against the snapshot.

```bash
make e2e-test                              # Run all (parallel, 4 jobs)
make e2e-update                            # Regenerate all .dl.out files
./scripts/run_snapshot_tests.sh -f joins   # Run only tests matching "joins"
./scripts/run_snapshot_tests.sh -j 1       # Sequential mode
./scripts/run_snapshot_tests.sh -v         # Verbose (shows diffs, forces sequential)
./scripts/run_snapshot_tests.sh -j 8       # 8 parallel jobs
```

Options:
| Flag | Description |
|------|-------------|
| `-f PATTERN` | Filter tests by grep pattern (e.g., `recursion`, `06_joins\|08_negation`) |
| `-j N` | Parallel jobs (default: 4, use 1 for sequential) |
| `-v` | Verbose mode with full diffs (forces sequential) |
| `-u` | Update mode  - regenerate `.idl.out` files |

Environment variables:
| Variable | Default | Description |
|----------|---------|-------------|
| `INPUTLAYER_TEST_PARALLEL` | 4 | Default parallel job count |
| `INPUTLAYER_TEST_PORT` | 8080 | Server port for tests |
| `INPUTLAYER_RESTART_INTERVAL` | 500 | Restart server every N tests (sequential mode) |

## Server Tracing (Debug Logs to File)

Enable structured server tracing logs (useful for diagnosing hangs/timeouts):

```bash
IL_TRACE=1 IL_TRACE_FILE=/tmp/inputlayer-trace.log ./scripts/run_snapshot_tests.sh
```

Optional:
| Variable | Default | Description |
|----------|---------|-------------|
| `IL_TRACE_JSON` | 0 | Set to `1` for JSON logs |
| `IL_TRACE_LEVEL` | `trace` | Log level (e.g., `info`, `debug`, `trace`) |

### Affected-Only Tests

`make test-affected` maps changed source files to relevant test categories and runs only those. Useful for fast feedback during development.

```bash
./scripts/test-affected.sh          # Changes since HEAD (uncommitted)
./scripts/test-affected.sh HEAD~3   # Changes in last 3 commits
./scripts/test-affected.sh main     # Changes since main branch
```

Source-to-category mapping:
- `src/join_planning/`, `src/sip_rewriting/` → `06_joins`, `80_sip`
- `src/ir_builder/` → `06_joins`, `07_filters`, `08_negation`, `14_aggregations`, `10_edge_cases`
- `src/code_generator/` → `09_recursion`, `18_advanced_patterns`
- `src/parser/`, `src/statement/` → `12_errors`, `17_rule_commands`, `28_docs_coverage`, `33_meta`, `39_meta_complete`
- `src/value/`, `src/ir/`, `src/lib.rs`, `src/config.rs` → runs all tests

## Makefile Targets

### Development Workflow

| Target | Description | When to Use |
|--------|-------------|-------------|
| `make test-fast` | Unit tests only | Quick feedback during coding |
| `make test` | Unit + snapshot | Pre-commit check |
| `make test-all` | Build + unit + snapshot + check | Full verification before merge |
| `make test-affected` | Snapshot tests for changed files only | Fast E2E feedback |

### Code Quality

| Target | Description |
|--------|-------------|
| `make check` | Formatting + clippy + doc-check + cargo check |
| `make fmt` | Auto-format code |
| `make lint` | Run clippy lints |
| `make fix` | Auto-fix formatting and lint issues |

### Build

| Target | Description |
|--------|-------------|
| `make build` | Debug build |
| `make build-release` | Release build |
| `make clean` | Remove build artifacts |

### Maintenance

| Target | Description |
|--------|-------------|
| `make e2e-update` | Regenerate all snapshot `.idl.out` files |
| `make flush-dev` | Delete `./data` folder to reset server state |
| `make release VERSION=x.x.x` | Create release branch, bump version, push |

## Writing Snapshot Tests

Each snapshot test is a `.idl` file in `examples/datalog/<category>/`:

```datalog
// Test: Descriptive Name
// Description: What this test verifies

.kg create test_unique_name_n<CAT>t<NUM>
.kg use test_unique_name_n<CAT>t<NUM>

+edge[(1,2), (2,3)]
+path(X, Y) <- edge(X, Y)

?path(X, Y)

// Cleanup
.kg use default
.kg drop test_unique_name_n<CAT>t<NUM>
```

Rules:
1. **Unique KG names**  - append `_n<category_number>t<file_number>` suffix (e.g., `_n08t01`) to prevent parallel test collisions.
2. **Always clean up**  - switch back to `default` and drop your KG at the end.
3. **File naming**  - `<number>_<description>.idl` (e.g., `01_simple_negation.idl`). Numbers must be unique within a category.
4. **Generate snapshots**  - run `./scripts/run_snapshot_tests.sh -u -f <category>` to create the `.idl.out` file, then verify the output is correct.

## Test Categories

| Category | Tests | Description |
|----------|-------|-------------|
| `01_knowledge_graph` | KG lifecycle | Create, use, list, drop knowledge graphs |
| `02_relations` | Fact CRUD | Insert, delete, query base relations |
| `04_session` | Sessions | Session-scoped rules and facts |
| `06_joins` | Joins | Two-way through five-way joins, self-joins, cross products |
| `07_filters` | Filters | Equality, comparison, range, string filters |
| `08_negation` | Negation | Antijoin, double negation, stratification |
| `09_recursion` | Recursion | Transitive closure, mutual recursion, bounded |
| `10_edge_cases` | Edge cases | Empty relations, wide tuples, boundary values |
| `11_types` | Types | Integers, floats, strings, booleans, nulls |
| `12_errors` | Errors | Syntax errors, arity mismatches, safety violations |
| `13_performance` | Performance | Wide joins, many joins, large result sets |
| `14_aggregations` | Aggregations | Count, sum, avg, min, max, top-k, grouping |
| `15_arithmetic` | Arithmetic | Add, subtract, multiply, divide, modulo, unary |
| `16_vectors` | Vectors | Vector operations, similarity, LSH |
| `17_rule_commands` | Rule management | Rule list, remove, drop |
| `18_advanced_patterns` | Advanced | Window functions, pivots, rankings, CTEs |
| `19_self_checking` | Self-checking | Tests that validate their own results |
| `20_applications` | Applications | Graph analysis, BOM, common ancestors |
| `21_query_features` | Query features | Projections, computed columns, wildcards |
| `22_set_operations` | Set operations | Union, intersection, difference |
| `24_rel_schemas` | Schemas | Explicit schema declarations |
| `25_unified_prefix` | Prefix syntax | Unified command prefix format |
| `27_atomic_ops` | Atomic operations | Conditional insert/delete |
| `28_docs_coverage` | Docs coverage | Tests covering documented syntax |
| `29_temporal` | Temporal | Time arithmetic, session duration |
| `30_quantization` | Quantization | Vector quantization |
| `31_lsh` | LSH | Locality-sensitive hashing |
| `32_math` | Math functions | abs, round, sign, power, trig |
| `33_meta` | Meta commands | Status, session, KG info |
| `35_strings` | String functions | Length, concat, trim, substring, contains |
| `39_meta_complete` | Meta complete | Comprehensive meta command coverage |
| `40_load_command` | Load command | Loading data from files |
| `80_sip` | SIP | Sideways information passing |
