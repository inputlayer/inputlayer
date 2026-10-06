# Testing

InputLayer's tests are organised in tiers. Each behaviour is tested once, at the layer that owns it, and every tier has a time budget (`tests/budget.toml`). This file says what belongs where, how to run each tier, how to add a test, what to do with a flake, and lists the end-to-end scenarios that gate the production-readiness and incremental-views work.

## Quick reference

```bash
make test-fast             # fmt, clippy, doc and build checks, then unit-test
make unit-test             # every workspace test in debug: unit, component, scenarios, oracle (the PR gate's cargo run)
make test                  # unit-test plus every snapshot spec
make test-all              # full verification: release build, tests, specs, scenarios in release, SDKs, coverage
make test-scenarios        # the scenario suite alone, in debug
make test-scenarios-modes  # the scenario suite once per views mode (S16)
make test-budget           # unit-test and every spec, then each tier's time against tests/budget.toml
make e2e-test              # every snapshot spec (parallel, release binaries)
make test-affected         # the snapshot specs your uncommitted changes affect
make oracle-test           # differential correctness oracle alone
make e2e-reactive          # scenario suite in release, writing writer->agent latency samples
make js-test               # JS SDK unit tests
make python-test           # Python SDK unit tests
make pre-pr                # before every push to a PR: the checks your changes need, then the perf gate
make perf-gate-remote      # performance gate for a commit on the benchmark host (shared lock)
```

See [CONTRIBUTING](CONTRIBUTING#pre-commit-checks) for `make pre-pr` routing, base selection and required pre-push checks.

## Tiers

| Tier | Owns | Where | Budget (PR gate, 4 vCPU) | PR gate | push to `main` | Elsewhere |
|---|---|---|---|---|---|---|
| T0 static | formatting, clippy on every target, rustdoc, `cargo deny` and `cargo audit`, secret scan, SDK type checks | `make check`, `make lint`, `make deny`, `make secret-check` | parallel jobs | yes | | |
| T1 unit | one module's logic in isolation: parser, IR, code generator operators, planner, values, config, auth primitives, WAL codec | `#[cfg(test)]` modules in `src/` of every workspace crate, doc tests | `unit = 60` s | yes | | |
| T2 component | one layer through its public API in one process: `StorageEngine` and persistence, `Handler`, the loopback `/ws` protocol, standing queries, replication | one binary per layer: `tests/storage/`, `tests/handler/`, `tests/ws/`, `tests/standing_query/`; other `tests/*.rs` | `component = 180` s | yes | | |
| T3 scenario | several agents on real `inputlayer-server` processes over the wire: subscribe, query, write with `expect_revision`, limits, restart, failover; the [catalogue](#scenario-catalogue) | `tests/scenarios/` on the `testkit` crate | `scenarios = 120` s | yes (debug) | release, with latency samples | |
| T4 spec snapshots | IQL syntax and query semantics of record, one `.iql` script and its `.iql.out` transcript per case | `examples/iql/<category>/`, `scripts/run_snapshot_tests.sh` | `specs = 60` s | affected categories | all | |
| T5 oracle and property | the same history through independent evaluators, compared at every revision; property tests | `tests/differential_oracle/`, `tests/property_arithmetic.rs` | `oracle_pr = 20` s | 12 seeds | 50 seeds | |
| T6 SDK contract | both SDKs against a mock server with the shared conformance fixtures; live suites against a server | `packages/inputlayer-js`, `packages/inputlayer-py`, `packages/conformance` | | unit, when SDK, protocol or spec paths change | unit and live | |
| T7 perf | the perf gate, the engine suite, the sessions and views benchmarks | `perf-gate/`, `scripts/perf-gate-remote.sh` | 12-50 min | no | | benchmark host |
| T8 soak | sustained concurrency against the oracle's reference, many and slow subscribers | `tests/differential_oracle/soak/`, `scripts/soak.sh` | smoke in T5; 30 min sustained | smoke | smoke | benchmark host |
| T9 release | quick starts in a fresh container, docker, coverage, MSRV, website, VC gate | `.github/workflows/full-suite.yml`, `quickstart.yml` | up to 60 min | path-filtered quick starts | | release checkpoints |

The PR gate (`.github/workflows/ci.yml`) is T0 in parallel jobs, then T1-T5 from one debug build (`make unit-test`, then `run_snapshot_tests.sh --debug --skip-build --affected`), plus the SDK unit suites when their inputs change. A push to `main` (`.github/workflows/main.yml`) runs every spec, the oracle at 50 seeds, the live SDK suites and the scenarios in release. [CONTRIBUTING](CONTRIBUTING#continuous-integration) has the triggers.

### Budgets

`tests/budget.toml` holds each tier's budget in seconds of test run time on the PR gate's 4-vCPU runner (compile time is not counted). `scripts/check-test-budget.py` sums each tier from a `cargo test` log, using the "finished in" time of every test binary mapped to its tier by source path (`src/` is unit, `tests/scenarios/` scenarios, `tests/differential_oracle/` oracle_pr, any other `tests/` component), and the spec runner's `Specs finished in` line. It fails when a tier runs more than 25 percent over its budget, names the slowest binaries of that tier, fails a binary no path maps to a tier, and writes every binary's and tier's time to `tests-timing.json`.

```bash
make test-budget                                                    # run the PR gate's tests here, then check
./scripts/check-test-budget.py --cargo-log cargo-test.log \
    --specs-log specs.log --json tests-timing.json                  # check logs you already have
./scripts/test-timings.sh cargo-test.log                            # per-binary table, slowest first
```

The budgets are calibrated on the CI runner: a busy development box runs the same tests two to three times slower, so a local `make test-budget` over budget is not by itself a regression. Raising a budget is a decision for the PR that needs it, with the reason in its description. The check accepts a GitHub Actions job log as downloaded (`gh api repos/inputlayer/inputlayer/actions/jobs/<id>/logs`), timestamps included.

## Where a new test belongs

| You changed | Write the test in | Not in |
|---|---|---|
| a parser rule, an operator, a planner pass, a value conversion | the module's `#[cfg(test)]` tests (T1) | `tests/` |
| what a statement does through `Handler` (errors, counts, atomicity, proofs, limits) | the handler's component test binary in `tests/` (T2) | a spec, unless the output format is the point |
| storage, WAL, restart, backup, replication between two processes | the storage or replication binary in `tests/` (T2) | |
| a wire frame, pipelining, cancellation, revocation, standing queries over `/ws` | `tests/ws/` or `tests/standing_query/` (T2) | a scenario, unless several connections or processes are the point |
| query semantics a user types (joins, negation, recursion, aggregates, functions) | a spec in `examples/iql/<category>/` (T4), plus an oracle scenario if deletes or revisions matter | a pipeline test in `tests/` |
| anything that spans subscribe, query, write and revision, several agents, limits while others continue, restart or failover | a scenario in `tests/scenarios/` (T3), naming the issue it gates | a new `tests/*.rs` binary |
| incremental maintenance correctness (deletes, recursion, negation, aggregates, rule replacement) | an oracle scenario in `tests/differential_oracle/scenarios.rs` (T5), and a corpus pack (`shop.rs` is one) for new constructs | |
| SDK behaviour | a fixture in `packages/conformance` when both SDKs must agree; the SDK's own unit test otherwise; a live test when the engine's acceptance of a form is the point | |
| a latency, throughput or setup-cost claim | a perf-gate fixture with a budget or ceiling in `perf-gate/policy.toml` (T7) | a timing assertion in any test |

Rules for every test:

- No verdict from wall-clock time outside the perf tier. A behaviour that depends on cost gets a seam behind the `test-support` feature (`Handler::with_sharing_regardless_of_cost()` is the pattern) or a counter.
- No `#[ignore]` to hide a failure. A test that cannot pass yet is a `KnownDefect` expected failure naming its issue (see [Expected failures](#expected-failures)), which fails loudly once the defect is fixed. The one exception is a scenario whose timing bounds need a release engine (`saturation`), ignored in debug builds and run by `make e2e-reactive`.
- No `sleep` to wait for a state; use the harness's `converge`, `expect_quiet`, `poll_delta`, barriers or notifies.
- Process-wide state (nesting limit, rayon pool, tracing subscriber) is either made per instance or tested in its own binary; a `tracing` hook uses a global subscriber armed per thread (the `tests/ws` harness is the pattern).
- A new test binary under `tests/` needs a reason in the PR; the default is a module in the layer's existing binary.
- Every `.iql` spec uses a unique knowledge graph name (`_n<category>t<file>`) and drops it at the end.

## Tier 1: unit

```bash
cargo test --lib --all-features                   # the engine crate
cargo test -p inputlayer-gateway --lib            # a workspace member
cargo test --all-features -- module::tests::name  # one test
make test-release                                 # the workspace in release
```

About 3,400 tests, most of them in the engine crate's library binary (about a minute on the CI runner).

## Tier 2: component

```bash
make integration-test                             # every tests/ binary
cargo test --all-features --test handler          # one layer
cargo test --all-features --test ws ws_cancel::   # one module of it
```

Component tests are one binary per layer, each a directory with one module per subject and a `harness` module for what the modules share. A new component test is a module of its layer's binary, not a new `tests/*.rs` file.

| Binary | Layer | Driven through |
|--------|-------|----------------|
| `tests/storage` | Storage engine and the persist layer: durability, recovery, restart equivalence, the data directory lock, backups, name validation | `StorageEngine` and `FilePersist` in process |
| `tests/handler` | Programs and their outcomes: statement status and counts, limits, authorization, provenance, indexes, evaluation regressions, server startup | `Handler` in process, no socket |
| `tests/ws` | The `/ws` protocol: sessions, pipelining, cancellation, delivery, `expect_revision`, notification cursors, credential revocation, login hardening | An in-process server on a loopback port and a raw `/ws` client |
| `tests/standing_query` | Standing queries, shared views and subscription groups | The same, with its own client |

Each binary starts its layer per test (temporary data directory, bootstrap admin, a loopback port for `/ws`). The modules of a binary share one process: a test that sets a process-wide value (an environment variable, the tracing subscriber, the thread pool size) takes it from the harness or holds the harness's lock for it. `nesting_depth_tests` stays its own binary because the nesting limit is process-wide. Deadlines and cancellation are tested adversarially over an in-process `/ws` connection by `tests/ws/ws_cancel.rs`: a queued request whose deadline passes or that is cancelled never runs later, a running query stops promptly, and a large write cancelled at increasing delays across its commit boundary always reports an outcome that matches the data, and a blind retry leaves exactly one copy. `tests/replication_tests.rs` runs a primary and a warm-standby follower as two processes: consistent prefixes when the primary dies, partitions, resyncs and synchronous shipping. Credential revocation is covered over a real `/ws` connection by `tests/ws/credential_revocation.rs`; handler unit tests in `src/protocol/handler/credential_mutation_tests.rs` cover failed password and role replacements, user drop and recreation across restart, grants and API keys refused for unknown users, and a bootstrap key that fails to store.

## Tier 3: scenarios

`tests/scenarios` is one test binary of scenarios against real `inputlayer-server` processes, built on the test-only `testkit` crate. The production-readiness and incremental-view changes are related, so scenarios exercise them together: rows, deltas, revisions, structured errors and the engine's work counters in one run, not each feature in isolation. New end-to-end coverage goes here as a module, not as a new `tests/*.rs` binary.

```bash
make test-scenarios                                    # debug; also part of unit-test (the PR gate)
cargo test --all-features --test scenarios -- limits   # one module
make e2e-reactive                                      # release, latency samples, ignored scenarios included
make test-scenarios-modes                              # once per views mode (S16)
INPUTLAYER_SCENARIO_VIEWS=maintained make test-scenarios   # refused until V2 (#309)
```

The suite runs in about 20 s in debug on 4 cores. `make test-all` and `make ci-test-all` run it once, in release through `make e2e-reactive`: their debug unit stage runs every other workspace test without the scenarios binary.

### Harness

- `EngineBuilder` starts an engine with a private data directory, generated config and free port. Settings a scenario may change: `views(Mode)` (`engine.views`; `Mode::from_env("INPUTLAYER_SCENARIO_VIEWS")` runs the suite once per mode, and `maintained` panics with "mode not available" until V2 defines the setting; `try_views` returns that as `Violation::Unavailable` for a scenario comparing the modes), `memory_limits(query_bytes, graph_bytes)`, `max_query_cost`, `nesting_limit`, `max_result_rows`, `ws_max_subscriptions`, `notification_buffer_size`, `ws_send_timeout_ms`, `max_connections`, `cpus` and `replication` (a primary or a follower of one). `Engine::stop`, `restart` and `crash_restart` drive the process.
- `Engine::metrics()` reads `/metrics/prometheus` into `Counters` (`queries`, `rule_evaluations`, `view_reads`, `subscription_evaluations`, `view_maintenance_us`); a counter the engine does not export is `None`, and `Counters::require` turns it into `Violation::NotMeasurable`, so an assertion on it fails (or is an expected failure), never a skip.
- `Engine::create_user`, `grant` and `create_api_key` (a key limited to a role on one knowledge graph and optionally to relations) with `WsClient::connect_with_key` give scenarios scoped agents besides the bootstrap admin key.
- `WsClient::execute_expecting(program, revision, relations)` sends `expect_revision`; `execute_at(program, revision)` sends `at` (V13 #316). `QueryResult::revision` is the reply's revision: set for writes, `None` for queries until V9 (#315); `QueryResult::statements` holds a write's per-statement counts. `try_execute` and `try_execute_expecting` (with an `Expect` that can also pin `expect_epoch`) return the engine's `Refusal` with its structured `code` and, for an unparsable program, its parse errors, instead of a violation. `try_execute_at` does the same for a read `at` a revision, and `QueryResult::views_at` is a write acknowledgement's `views_at` (V14 #320).
- `Fixture::shop_pack(Size)` installs one knowledge graph whose rules cover join, comparison, negation, recursion, negation over recursion and an aggregate (`Size::Vector` adds embeddings, the `emb_idx` HNSW index and the `near` rule; `Size::Lab` is about a million `link` edges for the benchmark host). Its anchors (order `o-42`, chains `i0`-`i4` and `i5`-`i9`) are documented on the function. Installing `Size::Small` takes under 200 ms in release: the perf gate's `shop_install` fixture holds it to that ceiling on the benchmark host ([Tier 7](#tier-7-performance)). No PR or coverage run asserts on wall-clock time.

Every scenario uses one assertion vocabulary (`tests/scenarios/support.rs`): `View::assert_matches` against a fresh query on another connection, `Delta::assert_rows`, `write_revision_matches_delta`, `refused(reply, code, message)`, `others_unaffected(agents)`, `no_rule_evaluations(counters)` and `revision_aligned(read, revision, ..)`; the testkit agent checks contiguous `seq` and increasing `revision` on every delta it applies. Each scenario's doc comment states its steps, its captain's table row and the milestone 9 issues it gates.

### Expected failures

Tracked defects and contracts milestone 9 has not delivered yet run as **expected failures** through `inputlayer_testkit::KnownDefect`, naming the issue that fixes them. Each asserts the correct contract: its own violation passes as `XFAIL`, any other violation fails, and a holding contract fails as `XPASS`, so the marker is removed and the part becomes required in the PR that lands the issue. A check that runs into two issues one after the other (a counter missing, then the work it shows) is judged with `KnownDefect::judge_first`, so each stage is an XFAIL of its own issue. A `Reproduction::Racy` defect passes with a `NOT REPRODUCED` note when the race does not show. `make unit-test`, `make test-scenarios`, `make ci-test-all` and `make e2e-reactive` print the open `XFAIL` list when the run ends (`make xfail-list` prints the last run's): `KnownDefect` appends each line to the file named by `INPUTLAYER_XFAIL_LOG`.

The counter checks and S4's `revision` are asserted for the target contract, which the default `recompute` mode never reaches; until V20 removes recompute, a `recompute` run of the mode matrix keeps those markers when the `maintained` run flips. Latency halves (S2's p50 at lab size, S17's 1,000-write shape) are the perf tier's B1/B2 and B15 on the benchmark host.

### Scenario catalogue

"Runs" = a required pass on today's engine. "XFAIL" = asserted now as an expected failure naming its issue; it flips to required when the issue lands. Rows are the captain's table: 1 deployed rule as live view; 2 one-row reads constant; 3 subscriptions read the changed rows between revisions; 4 many agents, one view, one pass, each gets its key; 5 subscribing to an arbitrary query refused; 6 ad-hoc queries join facts with views; 7 subscription and query consistent at a stated revision; 8 vector indexes on tables and views at one revision, similarity inside live rules; 9 documentation describes what runs.

| Id | Scenario | Test | Rows | Gates | Status |
|---|---|---|---|---|---|
| S1 | Agent lifecycle: subscribe to a deployed rule for its key, deltas, claim with `expect_revision`, the claim withdraws the offer | `lifecycle::s1_agent_lifecycle_on_a_deployed_rule` | 1, 3 | V10 #317, V11 #318, V14 #320 | runs |
| S2 | One-row view read is a lookup: rows of 50 `eligible` reads and the recursive `related` | `reads::s2_one_row_view_read_is_a_lookup` | 1, 2 | V1 #308, V9 #315 | rows run; no rule evaluated, every read from a view XFAIL |
| S3 | Ad-hoc query joins facts, a session rule and views | `reads::s3_ad_hoc_query_joins_facts_and_views` | 6 | V9 #315 | rows run; only the query's own work XFAIL |
| S4 | Subscription and query agree at a revision | `consistency::s4_subscription_and_query_agree_at_a_revision` | 3, 7 | V9 #315, V13 #316 | exact deltas at each write's revision run; query `revision`, `at: r` reads and `revision_compacted` XFAIL |
| S5 | Many agents, each hears its key: 200 keyed and 10 unkeyed agents | `fanout::s5_many_agents_each_hear_only_their_key` | 4 | V10 #317, V11 #318, V12 #319 | key isolation runs; one maintenance pass per commit XFAIL |
| S6 | Subscribe only to a deployed rule | `subscribe_rules::s6_subscribe_to_a_query_is_refused` | 5 | V11 #318 | the bodies answer as queries; refusing `.subscribe` of a join or filtered view XFAIL |
| S7 | Concurrent claim refused with the reason | `claims::s7_concurrent_claim_is_refused_with_the_reason` | 1 (`expect_revision`, decider keys) | V14 #320 | runs |
| S8 | Rule replaced while subscribed: one exact old-vs-new delta, one generation per read | `generations::s8_rule_replaced_while_subscribed` | 1 | V7 #314 | diff runs; refusing a drop of `link` under `related` XFAIL |
| S9 | Retraction through recursion and negation, and S9b with a second support | `retraction::s9_*`, `retraction::s9b_*` | 1, 3 | V4 #311, V5 #312; V0 #307 keeps it green | runs, live and through the oracle |
| S10 | An embedding change updates a live similarity rule: move, closer candidate, delete; equal to brute force; identical on a second run | `vectors::s10_embedding_change_updates_a_live_similarity_rule` | 8 | VV2 #324, VV5 #327, VV6 #328 | deltas run; no rule evaluated by a `near` read XFAIL (#327), a vector read `at: r` XFAIL (#315, then #328) |
| S11 | An index on a view equals the view through a base write, a rule replacement and a restart | `vectors::s11_index_on_a_view_follows_the_view` | 8 | VV1 #323, VV3 #325, VV6 #328 | the view's rows run; the index on the view XFAIL (#325: `.index create` on a rule is refused) |
| S12 | Restart mid-scenario preserves revisions | `restart::s12_restart_mid_scenario_preserves_revisions` | 3 | V18 #330 | runs |
| S13 | Failover mid-scenario: a synchronous follower holds every acknowledged change and refuses writes after the primary dies | `failover::s13_failover_mid_scenario_keeps_every_acknowledged_change` | 3 | V18 #330, EN-12 part 3 #285 | follower half runs; promotion and the claim on the new primary XFAIL (#285) |
| S14 | Limits refuse cleanly while 20 other agents keep receiving deltas: memory, nesting, `top_k` size, graph budget | `limits::s14_limits_refuse_cleanly_while_other_agents_continue` | readiness | V15 #321 | runs |
| S15 | Multi-tenant isolation and scoped keys | `tenancy::s15_tenants_and_scoped_keys_are_isolated` | 1 (per tenant) | V10 #317 | runs |
| S16 | Mode equivalence: identical rows, deltas and revisions in both views modes | `modes::s16_both_views_modes_give_identical_rows_deltas_and_revisions`, `make test-scenarios-modes` | all | V2 #309 through V20 #332 | recompute runs; the `maintained` comparison XFAIL (#309) |
| S17 | Write burst, read-your-writes: 4 writers x 100 writes, a rule rebuild | `burst::s17_write_burst_reads_its_writes` | 3 | V12 #319, V14 #320 | reads run; `views_at` on the rebuild's acknowledgement XFAIL |
| S18 | Docs describe what runs: the D1 claim list finds nothing outside an allow-list | not written yet (#338) | 9 | D1 #306, V20 #332 | not yet a test |

Notes on what the scenarios assert where the strategy's table and the documented contract differ: a restarted engine continues above every revision of its earlier runs and starts a new stream epoch (S12 asserts a new epoch, and a refused pre-crash `expect_revision` both pinned to its epoch and bare, #380); a `writer` key may subscribe on its own graph (S15 refuses its access to the other graph instead) and each permission refusal carries `access_denied`. S9 on the shop pack: the cut retracts (i1, i3), (i1, i4) and the offers behind them, and blocking and unblocking i4 are quiet because nothing reaches i4 from o-42 after the cut; S9b adds `link(i5, i4)` so the cut keeps (o-42, i4) and the negation flips it one row each way. S9's writes live in the oracle's shop corpus (`tests/differential_oracle/shop.rs`), so the live scenario, `oracle_check` and the oracle's own corpus run one history. S13's follower counts revisions of its own: a claim carrying the dead primary's revision is refused either `precondition_failed` (not issued here) or `store_read_only`. S14's refusals: a cross product over `max_query_memory_bytes` is `resource_exhausted`, a term nested past the parser's limit and a `top_k` over its cap are `validation`, and a write that would grow the graph past `max_graph_memory_bytes` is `resource_exhausted` with nothing applied. S16 runs S9b's live history once per mode; the whole-suite matrix is `make test-scenarios-modes`, whose `maintained` run may fail until V9 (#315) and is required after.

### The agent path

The `reactive` module drives the supported agent path end to end: agents subscribe to standing queries over `/ws`, and independent writer connections insert and retract facts and change rules. Agents must receive the exact added and retracted rows as `subscription_delta` pushes, with contiguous `seq` and increasing `revision`, and end equal to a fresh full query on another connection, without re-querying. Scenarios cover one subscriber, 64 subscribers, reconnect and resubscribe, crash-restart, unrelated writes, write bursts, deltas arriving while the agent's own long query runs on its connection, and an agent cancelling that query by id (`cancel`) and keeping its subscription.

- Every request the harness sends carries an `id`, and a reply that does not echo it fails the scenario (`Violation::Uncorrelated`); a `notice` is never taken for a reply. `wire` pipelines requests (malformed ones included) while the engine interleaves pushes, a streamed result and a `notifications_missed` notice, and requires every reply to correlate in order.
- `stream` requires the stream contract: notifications arrive in strictly increasing `seq` order under concurrent writers, a reconnect cursor from before an engine restart gets one `replay_gap` notice and nothing replayed, and commits racing a `.subscribe` all reach the agent. Results over `storage.performance.max_result_rows` fail closed: the subscription is refused, or a refresh pushes `subscription_error` and the next delta is relative to the last complete result.
- `delivery` requires payloads over one frame to arrive whole: a delta past the 16 MiB frame limit streams as one logical delta that the agent applies only at its end, and a large snapshot streams as a `.subscribe` reply naming its subscription. The testkit agent rejects a streamed delta whose chunks are missing, duplicated, out of order or short of its end frame's counts (`Violation::BrokenStream`).
- `saturation` requires correct deliveries under overload at the scale of #292: 960 sessions, each subscribed to its own bound standing query, while writers saturate the engine for 30 s. Each probe's delta reaches exactly its session, once, within a bound of the write's acknowledgement, and no session gets a stray delta or a `subscription_error`. Its timing bounds hold for a release engine, so it runs only in `make e2e-reactive` and is ignored in debug builds.
- `views` requires the view work counters on `/metrics/prometheus` to tell how reads of deployed rules were answered: today each read of a deployed rule, subscribing to one and each evaluation that refreshes it is exactly one rule evaluation, a read of base relations is none, and nothing is served from a view yet.

Every writer->agent delivery in a release run is recorded as a raw sample (write sent, write acknowledged, delta arrived) in `target/e2e-reactive/<scenario>.jsonl`, schema `inputlayer.reactive.delta_latency.v1` (see `testkit/src/metrics.rs`).

Not yet covered by scenarios (each is added when the work that enables it lands): public Python and JavaScript SDK agents (agents use the testkit's raw `/ws` client until the SDKs have a subscribe API; cross-SDK conformance comes with R3); gateway finding additions, resolutions and authoritative reset.

## Tier 4: snapshot specs

About 1,150 IQL scripts in `examples/iql/`, organised in categories. Each `.iql` file has a `.iql.out` transcript of the expected output. The runner starts a server, executes each script through the client binary and compares the output with the transcript.

```bash
make e2e-test                                     # every spec, parallel, release binaries
make e2e-update                                   # regenerate every .iql.out
./scripts/run_snapshot_tests.sh -f 08_negation    # one category
./scripts/run_snapshot_tests.sh -u -f 08_negation # regenerate after a deliberate change
./scripts/run_snapshot_tests.sh --debug --skip-build   # against the target/debug binaries cargo test built
make test-affected                                # categories your uncommitted changes affect
```

| Flag | Description |
|------|-------------|
| `-f PATTERN` | Filter specs by grep pattern (e.g., `recursion`, `06_joins\|08_negation`); a pattern matching no spec fails the run |
| `-j N` | Parallel jobs (default 4; 1 for sequential) |
| `-v` | Verbose mode with full diffs (forces sequential) |
| `-u` | Update mode: regenerate `.iql.out` files |
| `--skip-build` | Use the binaries already built instead of running `cargo build` |
| `--debug` | Build and run the `target/debug` binaries `cargo test` builds (default: release) |
| `--affected REF` | Run only the categories the changes since `REF` affect (see below) |

| Variable | Default | Description |
|----------|---------|-------------|
| `INPUTLAYER_TEST_PARALLEL` | 4 | Default parallel job count |
| `INPUTLAYER_TEST_PORT` | 8080 | Server port for specs (set a free one when other runs share the host) |
| `INPUTLAYER_RESTART_INTERVAL` | 500 | Restart the server every N specs (sequential mode) |

The run ends with `Specs finished in <seconds>s`, the specs tier of the [budgets](#budgets).

### Affected-only specs

`make test-affected` maps changed files to the spec categories they affect and runs only those (`run_snapshot_tests.sh --affected REF`; the PR gate uses the same map). Changes are the working tree against `REF`, so uncommitted edits count.

```bash
./scripts/test-affected.sh          # changes since HEAD (uncommitted)
./scripts/test-affected.sh HEAD~3   # changes in the last 3 commits
./scripts/test-affected.sh main     # changes since main
./scripts/run_snapshot_tests.sh --debug --affected main   # the same, on debug binaries
```

- a leaf module listed in `scripts/affected-map.toml` → the categories whose statements reach it (for example `src/provenance/` → the categories that run `.why` or `.why_not`)
- `examples/iql/<category>/...` → that category
- any other file under `src/`, `Cargo.toml`, `Cargo.lock`, `config.toml`, `ws-protocol/`, `ontology-client/`, `scripts/run_snapshot_tests.sh`, the map or its generator → every spec (a plan or parser change can reach any category; the whole corpus takes about 1.5 min on 4 cores against debug binaries)
- anything else (tests, docs, SDKs) → no specs

`scripts/affected-map.toml` is generated: `./scripts/gen-affected-map.py` matches each category's statement types in the text of its specs, the files they `.load` and their recorded transcripts, and lists the categories per leaf module from a fixed statement-type to module table. Regenerate it after adding or changing specs.

### Writing a spec

```iql
// Test: Descriptive Name
// Description: What this test verifies

.kg create test_unique_name_n08t01
.kg use test_unique_name_n08t01

+edge[(1,2), (2,3)]
+path(X, Y) <- edge(X, Y)
?path(X, Y)

.kg use default
.kg drop test_unique_name_n08t01
```

1. **Unique knowledge graph names**: append `_n<category_number>t<file_number>` (e.g., `_n08t01`) so parallel specs never collide.
2. **Always clean up**: switch back to `default` and drop your graph at the end.
3. **File naming**: `<number>_<description>.iql`; numbers are unique within a category.
4. **Generate the transcript** with `./scripts/run_snapshot_tests.sh -u -f <category>`, read the `.iql.out` and check it is right before committing it.

| Category | Specs | Covers |
|---|---|---|
| `01_knowledge_graph` | 8 | create, use, list, drop |
| `02_relations` | 12 | insert, delete, query base relations |
| `04_session` | 35 | session facts and rules, isolation |
| `06_joins` | 41 | two- to five-way joins, self joins, cross products |
| `07_filters` | 28 | equality, comparison, range, string filters |
| `08_negation` | 70 | antijoin, double negation, stratification |
| `09_recursion` | 60 | transitive closure, mutual recursion, bounded |
| `10_edge_cases` | 110 | empty relations, wide tuples, boundaries |
| `11_types` | 67 | integers, floats, strings, booleans, nulls |
| `12_errors` | 55 | syntax, arity, safety, cycles |
| `13_performance` | 12 | wide and many joins, large results |
| `14_aggregations` | 125 | count, sum, avg, min, max, top-k, grouping |
| `15_arithmetic` | 56 | operators, unary, modulo |
| `16_vectors`, `30_quantization`, `31_lsh` | 22 + 8 + 5 | vector functions, similarity, quantization, LSH |
| `17_rule_commands` | 29 | rule list, remove, drop |
| `18_advanced_patterns` | 106 | windows, pivots, rankings |
| `19_self_checking`, `20_applications` | 4 + 10 | self-validating and application examples |
| `21_query_features`, `22_set_operations` | 67 + 28 | projections, computed columns, wildcards; union, intersection, difference |
| `24_rel_schemas`, `25_unified_prefix`, `27_atomic_ops` | 6 + 8 + 22 | schemas, prefix syntax, conditional insert and delete |
| `28_docs_coverage` | 24 | documented syntax |
| `29_temporal`, `32_math`, `34_type_conversion`, `35_strings` | 19 + 19 + 6 + 19 | time, math, conversion and string functions |
| `33_meta` | 35 | status, session, KG, relation, rule, index and debug-plan commands, help |
| `36_explain_trace`, `41_timing_breakdown` | 16 + 2 | `.why`, `.why_not`, timing |
| `40_load_command` | 7 | `.load` |
| `80_sip` | 6 | sideways information passing |
| `90_product_fixtures` | 9 | product rule packs: consistency pack, prompt integrity, landing page examples |

## Tier 5: oracle and property

`tests/differential_oracle/` replays one history (statements, restarts and named checkpoints) through independent adapters and compares their results at every checkpoint:

| Adapter | What it is |
|---------|------------|
| `reference` | Naive finite evaluator in the test: stratified naive fixpoint over sets. Shares only the parser with the engine. |
| `recompute` | The engine's snapshot evaluator, queried afresh. |
| `subscription` | Standing queries assembled purely from pushed `inserted`/`retracted` deltas, through the real notification, dependency-filtering, coalescing and shared-view path a subscribed agent uses. Each query has two subscribers on one shared view, one taking every publication and one only the settled result; they must agree. |
| `subscription[group]` | Every query in one subscription group, each member assembled from pushed group deltas. |
| `spec` | Results recorded in `.iql.out` transcripts (corpus cases only). |
| `maintained` | `recompute` on an engine whose persistent rules are incrementally maintained views (`engine.views = "maintained"`), when `INPUTLAYER_ORACLE_VIEWS=maintained`. Until V2 (#309) adds the mode, every test fails saying it is not available, never as a pass. |

Histories come from hand-written scenarios (duplicate supports, recursive edge removal, negation, aggregates, rule replacement, restart), seeded random generation, the `.iql.out` corpus of the derived-result categories, and the shop pack corpus (`shop.rs`): the scenario suite's shop pack with writes through each of its constructs and a restart, and the histories of scenario S9, which the scenario suite also feeds to the oracle (`oracle_check`). Results are compared as Z-sets, so a row reported twice or retracted without being present is a divergence of its own. A divergence is minimized (delta debugging) to a short reproducing script.

Constructs the reference does not model (e.g. `avg`, `top_k`, arithmetic, floats, session state) are reported as explicit skips with a reason, never counted as agreement; the engine adapters are still compared with each other and the spec. A new evaluation strategy joins by implementing the `Adapter` trait: `observe` takes the revision the result must reflect.

```bash
make oracle-test                                  # every oracle test, 12 seeds
INPUTLAYER_ORACLE_SEEDS=500 make oracle-test      # more random histories
INPUTLAYER_ORACLE_SEED=17 cargo test --all-features --test differential_oracle seeded   # one seed
INPUTLAYER_ORACLE_VIEWS=maintained make oracle-test   # add the maintained adapter (V3-V5 acceptance: 0 divergences)
```

`tests/property_arithmetic.rs` holds the property tests of typed arithmetic.

### Fuzzing

`fuzz/` holds cargo-fuzz targets for the client input the server parses: one IQL statement, a whole `execute` program with its `params`, and a `/ws` client frame. They run the server's own parse, bind and classification code on the server's stack size, and check invariants beyond not crashing (a persistent rule reloads unchanged; a request run read-only holds only queries). Seeds come from the example programs and the deep-nesting and oversized-body cases of #295. Campaigns belong on the benchmark host; see [`fuzz/README.md`](fuzz/README.md).

```bash
make fuzz                                         # Every target, 10 minutes each
make fuzz FUZZ_SECS=3600 FUZZ_TARGETS=iql_program # One target, an hour
```

## Tier 6: SDK contract

```bash
make js-test            # JS SDK unit tests
make python-test        # Python SDK unit tests (uv)
make js-test-live       # the JS SDK guide's queries, integration, connection, guard and subscription tests against a server it starts
make python-sdk-live    # the Python equivalents
make python-test-live   # langchain integration against a server
```

Both unit suites replay `packages/conformance/connection/*.json` through a mock server, so the two connection cores are held to one wire behaviour. Add a fixture there whenever a frame sequence must be handled identically by both SDKs.

## Tier 7: performance

`make perf-gate` is the performance acceptance check for every implementation PR. It builds the approved baseline commit and this tree's server, measures both on this host in interleaved rounds, and checks query latency, durable-write throughput and writer-to-subscribed-agent delta latency against the budgets in `perf-gate/policy.toml`. Only a PASS is acceptable. Attach `target/perf-gate/latest/report.md` to the PR. Method, fixtures and runner requirements are in [`perf-gate/README.md`](perf-gate/README.md). Heavy perf runs go to the dedicated benchmark host, not to a shared development box: `make perf-gate-remote` runs the gate there for a commit under the host's shared lock, and `make pre-pr PRE_PR_PERF=perf-gate-remote` makes the pre-PR gate do so. `make bench-engine-remote`, `make bench-sessions-remote` and `make bench-views-remote` run the engine suite, the session-scale and the views benchmarks there. The Criterion benches in `benches/` are diagnostic only.

Besides relative budgets, the policy holds absolute ceilings: the gate's `shop_install` fixture installs the scenario suite's shop pack (`Size::Small`) into fresh knowledge graphs, and its p50 must stay under 200 ms whatever the baseline measured.

## Tier 8: soak

The oracle's adapters replay one history at a time, in process. `tests/differential_oracle/soak/` holds a real server under concurrent load to the same reference evaluator:

- **Writers** commit concurrently. Each owns a disjoint share of the base facts, so it knows exactly what each program must change and checks the effective counts the engine reports.
- **A rule churner** atomically replaces rule variants (recursion shape, negation, aggregates) while the writers run.
- **Consumers** maintain standing queries from pushed deltas: fast ones; slow ones reading behind a one-frame inbox and a 4 KB socket buffer; ones that stop reading for a while; subscription groups; short-lived connections attaching to shared views; and auditors that `read` every query at one revision.

Every write reply names the revision it committed at, and every snapshot, delta and read names the revision it is exact at. A verifier thread replays the commits in revision order through the reference and checks each observation at its revision. Each writer commits one program at a time, so every commit at or below the lowest of the writers' last acknowledged revisions is known; observations above it wait. When the writers stop, every consumer must settle on the reference's final state. Slow and stalled consumers may be disconnected only as the protocol documents (`slow_consumer`, or the send timeout), then reconnect. Fast consumers must never be disconnected. Server memory is sampled once a second.

The default is a smoke of a few seconds that runs with the oracle. `INPUTLAYER_SOAK_*` variables scale it (see `Config` in `soak/mod.rs`). `scripts/soak.sh` runs it in release and writes `result.json` and `summary.md` to `target/soak/latest`. The sustained profile (30 minutes, ~400 consumers) is for the benchmark host:

```bash
make soak SOAK_ARGS="--profile smoke"             # locally, with a report
make soak SOAK_ARGS="--secs 120 --set FAST=50"    # sustained profile, shorter and smaller
make soak-remote                                  # sustained soak on the benchmark host
```

## Flake policy

1. A test that fails without a code cause is a flake. Within one working day it is fixed or turned into a `KnownDefect` expected failure (`Reproduction::Racy` when it depends on a race) that names an issue; it is never `#[ignore]`d, never wrapped in a retry, and re-running the failed job is not a fix.
2. The known patterns and their fixes: a verdict from a wall-clock sample (use the `test-support` seam and assert on counters), process-wide state shared between tests in one binary (make it per instance, or give the test its own binary), a thread-local `tracing` subscriber in a multi-threaded binary (global subscriber, armed per thread), a lock released only by closing a descriptor a child process inherited (explicit unlock on drop), OS-seeded randomness in an index build (pin the seed or the level scale).
3. A perf-gate INCONCLUSIVE is not a pass; it is re-run on the benchmark host, never argued away on a busy box.

## Server tracing for a failing test

```bash
INPUTLAYER_TRACE=1 INPUTLAYER_TRACE_FILE=/tmp/inputlayer-trace.log ./scripts/run_snapshot_tests.sh -f 09_recursion
```

| Variable | Default | Description |
|----------|---------|-------------|
| `INPUTLAYER_TRACE_JSON` | 0 | Set to `1` for JSON logs |
| `INPUTLAYER_TRACE_LEVEL` | `trace` | Log level (e.g., `info`, `debug`, `trace`) |

Scenario engines write `server.log` into their temporary directory (`Engine::log_path`).

## Makefile targets

### Development workflow

| Target | Description | When to use |
|--------|-------------|-------------|
| `make test-fast` | Checks, then unit-test | Quick feedback during coding |
| `make test` | Checks, unit-test and every spec | Broad local check before a PR |
| `make test-all` | Checks and static analysis, release build, tests, specs, scenarios in release, SDK suites, coverage | Full verification before merge |
| `make unit-test` | Every workspace test in debug, then the open XFAIL list | What the PR gate's cargo run does |
| `make integration-test` | Every `tests/` binary | Component changes |
| `make test-scenarios` | The scenario suite in debug | Changes that span connections, revisions or processes |
| `make test-scenarios-modes` | The scenario suite once per views mode (S16) | Milestone 9 work |
| `make test-budget` | The PR gate's tests, then each tier against `tests/budget.toml` | A PR that adds tests or slows them |
| `make xfail-list` | The open expected failures of the last run | |
| `make test-affected` | Specs for changed files only | Fast spec feedback |
| `make pre-pr` | [Pre-PR pipeline](CONTRIBUTING#pre-commit-checks) | Before every push to a PR |
| `make perf-gate` | Paired latency/throughput gate over `/ws` vs the approved baseline | Every implementation PR (see `perf-gate/README.md`) |
| `make perf-gate-remote` | The same gate for a commit on the benchmark host | Instead of `make perf-gate` on a shared development box |
| `make bench-engine-remote` | Engine suite (rules, closure, deletes and updates, claims, `.why`, sessions, memory, recovery, WAL share) on the benchmark host | Release checkpoints and engine baselines |
| `make bench-views-remote` | Write and read cost against deployed rules by graph size, rule count and subscribers, with rule-evaluation counts, on the benchmark host | Changing evaluation, subscriptions or rule maintenance (the view work, #305) |
| `make perf-gate-check` | Clippy and unit tests of the gate tool | After changing `perf-gate/` |
| `make pre-pr-selftest` | Behavioural tests of `make pre-pr` routing (`scripts/test_pre_pr.py`) | After changing `Makefile` or `scripts/` |
| `make e2e-reactive` | Scenario suite in release against real engines, latency samples | Subscription or wire changes |
| `make oracle-test` | Differential correctness oracle only | Changing evaluation, subscriptions or rule catalog changes |
| `make fuzz` | cargo-fuzz campaign over the IQL parser and `/ws` frame targets (`fuzz/README.md`) | Changing the parser, parameter binding or frame decoding; long campaigns on the benchmark host |

### Code quality

| Target | Description |
|--------|-------------|
| `make check` | Formatting, clippy, doc-check and cargo check |
| `make fmt` | Auto-format code |
| `make lint` | Run clippy lints |
| `make deny` | Supply chain: cargo deny (licenses, sources, bans, advisories) and cargo audit, as in CI (`deny.toml`, `.cargo/audit.toml`) |
| `make secret-check` | gitleaks over every commit reachable from HEAD (`.gitleaks.toml`) |
| `make install-hooks` | Optional hooks: fmt and staged secret scan on commit, clippy on push |
| `make hooks-test` | Prove the hooks reject misformatted Rust and staged credentials |
| `make fix` | Auto-fix formatting and lint issues |

### Build and maintenance

| Target | Description |
|--------|-------------|
| `make build` | Debug build |
| `make build-release` | Release build |
| `make clean` | Remove build artifacts |
| `make e2e-update` | Regenerate all snapshot `.iql.out` files |
| `make flush-dev` | Delete the `./data` folder to reset server state |
| `make release VERSION=x.x.x` | Create release branch, bump version, push |
