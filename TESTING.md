# Testing

InputLayer has three test tiers: unit tests, integration tests, and end-to-end snapshot tests.

## Quick Reference

```bash
make test-all       # Full verification: build + unit + snapshot (~70s, all CPUs)
make test-fast      # Unit tests only (~30s)
make test           # Unit + snapshot tests
make e2e-test       # Snapshot tests only (parallel)
make e2e-reactive   # Reactive agent path against real engine processes
make test-affected  # Run only snapshots affected by uncommitted changes
make perf-gate      # Performance gate: this tree vs the approved baseline (same host)
make bench-genbi    # Reactive agent benchmark on genbi-trust (needs GENBI_TRUST_DIR)
make oracle-test    # Differential correctness oracle only (~15s)
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

### Differential Correctness Oracle

`tests/differential_oracle/` replays one history (statements, restarts and named checkpoints) through independent adapters and compares their results at every checkpoint:

| Adapter | What it is |
|---------|------------|
| `reference` | Naive finite evaluator in the test: stratified naive fixpoint over sets. Shares only the parser with the engine. |
| `recompute` | The engine's snapshot evaluator, queried afresh. |
| `subscription` | Standing queries assembled purely from pushed `inserted`/`retracted` deltas, through the real notification, dependency-filtering and coalescing path a subscribed agent uses. |
| `spec` | Results recorded in `.iql.out` transcripts (corpus cases only). |

Histories come from hand-written scenarios (duplicate supports, recursive edge removal, negation, aggregates, rule replacement, restart), seeded random generation, and the `.iql.out` corpus of the derived-result categories. Results are compared as Z-sets, so a row reported twice or retracted without being present is a divergence of its own. A divergence is minimized (delta debugging) to a short reproducing script.

Constructs the reference does not model (e.g. `avg`, `top_k`, arithmetic, floats, session state) are reported as explicit skips with a reason, never counted as agreement; the engine adapters are still compared with each other and the spec. A new evaluation strategy (such as persistent per-KG dataflows) joins by implementing the `Adapter` trait: `observe` takes the revision the result must reflect.

```bash
make oracle-test                                  # All oracle tests
INPUTLAYER_ORACLE_SEEDS=500 make oracle-test      # More random histories
INPUTLAYER_ORACLE_SEED=17 cargo test --all-features --test differential_oracle seeded  # One seed
```

### Tier 3: Snapshot Tests (E2E)

~995 IQL scripts in `examples/iql/` organized across 33 categories. Each `.iql` file has a corresponding `.iql.out` file with expected output. The test runner starts a server, executes each script via the client binary, and compares actual output against the snapshot.

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
| `-u` | Update mode  - regenerate `.iql.out` files |

Environment variables:
| Variable | Default | Description |
|----------|---------|-------------|
| `INPUTLAYER_TEST_PARALLEL` | 4 | Default parallel job count |
| `INPUTLAYER_TEST_PORT` | 8080 | Server port for tests |
| `INPUTLAYER_RESTART_INTERVAL` | 500 | Restart server every N tests (sequential mode) |

## Performance Gate

`make perf-gate` is the performance acceptance check for every implementation
PR. It builds the approved baseline commit and this tree's server, measures
both on this host in interleaved rounds, and checks query latency,
durable-write throughput and writer-to-subscribed-agent delta latency against
the budgets in `perf-gate/policy.toml`. Only a PASS is acceptable. Attach
`target/perf-gate/latest/report.md` to the PR. Method, fixtures and runner
requirements are in [`perf-gate/README.md`](perf-gate/README.md). The
Criterion benches in `benches/` are diagnostic only.

## Reactive Agent Path (E2E)

`make e2e-reactive` drives the supported agent path end to end: each test
starts a real `inputlayer-server` process with its own data directory, agents
subscribe to standing queries over `/ws`, and independent writer connections
insert and retract facts and change rules. Agents must receive the exact
added/retracted rows as `subscription_delta` pushes, with contiguous `seq`, and
end equal to a fresh full query on another connection, without re-querying.
Scenarios cover one subscriber, 64 subscribers, reconnect/resubscribe and
crash-restart, unrelated writes, and write bursts.

```bash
make e2e-reactive                                   # release build, writes latency samples
cargo test --test e2e_reactive                      # same scenarios, debug build
```

Results over `storage.performance.max_result_rows` are required to fail
closed: the subscription is refused, or a refresh pushes `subscription_error`
and the next delta is relative to the last complete result.

Defects tracked by the reactive plan run as **expected failures**
(`tests/e2e_reactive/known_defects.rs`): out-of-order and restart-cursor
notification delivery (W04), and an oversized delta that advances the
subscription without delivery (W05). Each
asserts the correct contract; its own violation passes as `XFAIL`, any other
violation fails, and a holding contract fails as `XPASS` so the marker is
removed and the scenario becomes required when the plan item lands.

Every writer->agent delivery is recorded as a raw sample (write sent, write
acknowledged, delta arrived) in `target/e2e-reactive/<scenario>.jsonl`, schema
`inputlayer.reactive.delta_latency.v1` (see `testkit/src/metrics.rs`). The
harness lives in the test-only `testkit` crate (engine process, `/ws` agent
client, fixtures, samples), shared with the benches.

## Server Tracing (Debug Logs to File)

Enable structured server tracing logs (useful for diagnosing hangs/timeouts):

```bash
INPUTLAYER_TRACE=1 INPUTLAYER_TRACE_FILE=/tmp/inputlayer-trace.log ./scripts/run_snapshot_tests.sh
```

Optional:
| Variable | Default | Description |
|----------|---------|-------------|
| `INPUTLAYER_TRACE_JSON` | 0 | Set to `1` for JSON logs |
| `INPUTLAYER_TRACE_LEVEL` | `trace` | Log level (e.g., `info`, `debug`, `trace`) |

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
| `make perf-gate` | Paired latency/throughput gate over `/ws` vs the approved baseline | Every implementation PR (see `perf-gate/README.md`) |
| `make perf-gate-check` | Clippy + unit tests of the gate tool | After changing `perf-gate/` |
| `make e2e-reactive` | Reactive agent path against real engines, latency samples | Subscription or wire changes |
| `make oracle-test` | Differential correctness oracle only | Changing evaluation, subscriptions or rule maintenance |

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
| `make e2e-update` | Regenerate all snapshot `.iql.out` files |
| `make flush-dev` | Delete `./data` folder to reset server state |
| `make release VERSION=x.x.x` | Create release branch, bump version, push |

## Writing Snapshot Tests

Each snapshot test is a `.iql` file in `examples/iql/<category>/`:

```iql
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
3. **File naming**  - `<number>_<description>.iql` (e.g., `01_simple_negation.iql`). Numbers must be unique within a category.
4. **Generate snapshots**  - run `./scripts/run_snapshot_tests.sh -u -f <category>` to create the `.iql.out` file, then verify the output is correct.

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
