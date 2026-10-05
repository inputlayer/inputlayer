# Testing

InputLayer has three test tiers: unit tests, integration tests, and end-to-end snapshot tests.

## Quick Reference

```bash
make test-all       # Full verification: build + unit + snapshot (~70s, all CPUs)
make test-fast      # Unit tests only (~30s)
make test           # Unit + snapshot tests
make e2e-test       # Snapshot tests only (parallel)
make e2e-reactive   # Scenario suite in release, writing delta-latency samples
make test-affected  # Run only snapshots affected by uncommitted changes
make pre-pr         # Before every push to a PR: formatting and affected component checks in parallel, then perf-gate
make perf-gate      # Performance gate: this tree vs the approved baseline (same host)
make perf-gate-remote     # The same gate for HEAD on the benchmark host (heavy runs go there)
make bench-engine-remote  # Engine suite on the benchmark host: absolute numbers, not judged
make bench-sessions-remote  # Session-scale benchmark on the benchmark host
make bench-views-remote     # Views benchmark (write and read cost vs graph, rules, subscribers) on the benchmark host
make bench-genbi    # Reactive agent benchmark on genbi-trust (needs GENBI_TRUST_DIR)
make oracle-test    # Differential correctness oracle only (~15s)
```

See [CONTRIBUTING](CONTRIBUTING#pre-commit-checks) for `make pre-pr` routing, base selection
and required pre-push checks.

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
| `subscription` | Standing queries assembled purely from pushed `inserted`/`retracted` deltas, through the real notification, dependency-filtering, coalescing and shared-view path a subscribed agent uses. Each query has two subscribers on one shared view, one taking every publication and one only the settled result; they must agree. |
| `spec` | Results recorded in `.iql.out` transcripts (corpus cases only). |

Histories come from hand-written scenarios (duplicate supports, recursive edge removal, negation, aggregates, rule replacement, restart), seeded random generation, and the `.iql.out` corpus of the derived-result categories, and scenario histories of the [scenario suite](#catalogue-scenarios) (`oracle_check`). Results are compared as Z-sets, so a row reported twice or retracted without being present is a divergence of its own. A divergence is minimized (delta debugging) to a short reproducing script.

Constructs the reference does not model (e.g. `avg`, `top_k`, arithmetic, floats, session state) are reported as explicit skips with a reason, never counted as agreement; the engine adapters are still compared with each other and the spec. A new evaluation strategy (such as persistent per-KG dataflows) joins by implementing the `Adapter` trait: `observe` takes the revision the result must reflect.

```bash
make oracle-test                                  # All oracle tests
INPUTLAYER_ORACLE_SEEDS=500 make oracle-test      # More random histories
INPUTLAYER_ORACLE_SEED=17 cargo test --all-features --test differential_oracle seeded  # One seed
```

#### Concurrency soak

The adapters above replay one history at a time, in process. `tests/differential_oracle/soak/` holds a real server under concurrent load to the same reference evaluator:

- **Writers** commit concurrently. Each owns a disjoint share of the base facts, so it knows exactly what each program must change and checks the effective counts the engine reports.
- **A rule churner** atomically replaces rule variants (recursion shape, negation, aggregates) while the writers run.
- **Consumers** maintain standing queries from pushed deltas: fast ones; slow ones reading behind a one-frame inbox and a 4 KB socket buffer; ones that stop reading for a while; subscription groups; short-lived connections attaching to shared views; and auditors that `read` every query at one revision.

Every write reply names the revision it committed at, and every snapshot, delta and read names the revision it is exact at. A verifier thread replays the commits in revision order through the reference and checks each observation at its revision. Each writer commits one program at a time, so every commit at or below the lowest of the writers' last acknowledged revisions is known; observations above it wait. When the writers stop, every consumer must settle on the reference's final state. Slow and stalled consumers may be disconnected only as the protocol documents (`slow_consumer`, or the send timeout), then reconnect. Fast consumers must never be disconnected. Server memory is sampled once a second.

The default is a smoke of a few seconds that runs with the oracle. `INPUTLAYER_SOAK_*` variables scale it (see `Config` in `soak/mod.rs`). `scripts/soak.sh` runs it in release and writes `result.json` and `summary.md` to `target/soak/latest`. The sustained profile (30 minutes, ~400 consumers) is for the benchmark host:

```bash
make soak SOAK_ARGS="--profile smoke"                      # Locally, with a report
make soak SOAK_ARGS="--secs 120 --set FAST=50"             # Sustained profile, shorter and smaller
make soak-remote                                           # Sustained soak on the benchmark host
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
| `-f PATTERN` | Filter tests by grep pattern (e.g., `recursion`, `06_joins\|08_negation`); a pattern matching no spec fails the run |
| `-j N` | Parallel jobs (default: 4, use 1 for sequential) |
| `-v` | Verbose mode with full diffs (forces sequential) |
| `-u` | Update mode  - regenerate `.iql.out` files |
| `--skip-build` | Use the binaries already built instead of running `cargo build` |
| `--debug` | Build and run the `target/debug` binaries `cargo test` builds (default: release) |
| `--affected REF` | Run only the categories the changes since `REF` affect (see [Affected-Only Tests](#affected-only-tests)) |

The PR gate runs `./scripts/run_snapshot_tests.sh --debug --skip-build --affected <base>` right after `make unit-test`, against the binaries that build left in `target/debug`; pushes to `main` run every spec the same way (`.github/workflows/main.yml`).

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
requirements are in [`perf-gate/README.md`](perf-gate/README.md). Heavy
perf runs go to the dedicated benchmark host, not to a shared development
box: `make perf-gate-remote` runs the gate there for a commit, and
`make pre-pr PRE_PR_PERF=perf-gate-remote` makes the pre-PR gate do so. The
Criterion benches in `benches/` are diagnostic only.

## Scenario Suite (E2E)

`tests/scenarios` is one test binary of scenarios against real
`inputlayer-server` processes, built on the test-only `testkit` crate. The
production-readiness and incremental-view changes are related, so scenarios
exercise them together: rows, deltas, revisions, structured errors and the
engine's work counters in one run, not each feature in isolation. New
end-to-end coverage goes here as a module, not as a new `tests/*.rs` binary.

```bash
cargo test --test scenarios                         # debug; also part of plain `cargo test` (the PR gate)
make e2e-reactive                                   # release build, writes latency samples
INPUTLAYER_SCENARIO_VIEWS=maintained cargo test --test scenarios  # refused until V2 (#309)
```

Modules: `reactive`, `stream`, `delivery`, `saturation` and `wire` (the agent
path below), `harness` (the testkit pieces scenarios build on, checked against
a real engine), the catalogue scenarios `lifecycle`, `claims`, `retraction`,
`restart` and `tenancy` (below), and the scenarios that hold the milestone 9
contract as expected failures, `reads`, `consistency`, `fanout`,
`subscribe_rules`, `generations` and `burst` (below). The suite runs in about
25 s in debug on 4 cores. `make test-all` and `make ci-test-all` run it once, in
release through `make e2e-reactive`: their debug unit stage runs every other
workspace test without the scenarios binary.

### Harness

- `EngineBuilder` starts an engine with a private data directory, generated
  config and free port. Settings a scenario may change: `views(Mode)`
  (`engine.views`; `Mode::from_env("INPUTLAYER_SCENARIO_VIEWS")` lets CI run
  the suite once per mode, and `maintained` panics with "mode not available"
  until V2 defines the setting), `memory_limits(query_bytes, graph_bytes)`,
  `max_query_cost`, `nesting_limit`, `max_result_rows`,
  `ws_max_subscriptions`, `notification_buffer_size` and `replication`.
- `Engine::metrics()` reads `/metrics/prometheus` into `Counters`
  (`queries`, `rule_evaluations`, `view_reads`, `subscription_evaluations`,
  `view_maintenance_us`); a counter the engine does not export is `None`, and
  `Counters::require` turns it into `Violation::NotMeasurable`, so an assertion
  on it is an expected failure until the counter lands (V1 #308), never a skip.
- `Engine::create_user`, `grant` and `create_api_key` (a key limited to a role
  on one knowledge graph and optionally to relations) with
  `WsClient::connect_with_key` give scenarios scoped agents besides the
  bootstrap admin key.
- `WsClient::execute_expecting(program, revision, relations)` sends
  `expect_revision`; `execute_at(program, revision)` sends `at` (V13 #316).
  `QueryResult::revision` is the reply's revision: set for writes, `None` for
  queries until V9 (#315); `QueryResult::statements` holds a write's
  per-statement counts. `try_execute` and `try_execute_expecting` (with an
  `Expect` that can also pin `expect_epoch`) return the engine's `Refusal`
  with its structured `code` instead of a violation, for scenarios that assert
  how a request is refused.
  `try_execute_at` does the same for a read `at` a revision, and
  `QueryResult::views_at` is a write acknowledgement's `views_at` (V14 #320).
- `Fixture::shop_pack(Size)` installs one knowledge graph whose rules cover
  join, comparison, negation, recursion, negation over recursion and an
  aggregate (`Size::Vector` adds embeddings, the `emb_idx` HNSW index and the
  `near` rule; `Size::Lab` is about a million `link` edges for the benchmark
  host). Its anchors (order `o-42`, chains `i0`-`i4` and `i5`-`i9`) are
  documented on the function. The `Size::Small` install time is measured and
  reported by the harness (`shop_pack_installs_and_derives_its_anchors` prints
  it; visible with `--nocapture`, as in `make e2e-reactive`). Nothing enforces
  the 200 ms budget yet: the perf-tier fixture tracked in #347 will, on the
  benchmark host. No PR or coverage run asserts on wall-clock time.

### Catalogue scenarios

The strategy's scenarios that hold on today's engine, each on the shop pack
with agents, writers and auditors on their own keys. Each asserts rows, deltas
and revisions, structured errors, and that other agents are unaffected, with
one vocabulary (`tests/scenarios/support.rs`): `View::assert_matches` against
a fresh query on another connection, `Delta::assert_rows`,
`write_revision_matches_delta`, `refused(reply, code, message)` and
`others_unaffected(agents)`; the testkit agent checks contiguous `seq` and
increasing `revision` on every delta it applies. Together they take a few
seconds of the suite.

| Scenario | Test | Captain's table row | Gates (milestone 9) |
|----------|------|---------------------|---------------------|
| S1 agent lifecycle on a deployed rule | `lifecycle::s1_agent_lifecycle_on_a_deployed_rule` | 1, 3 | V10 #317, V11 #318, V14 #320 |
| S7 concurrent claim refused with the reason | `claims::s7_concurrent_claim_is_refused_with_the_reason` | 1 (`expect_revision`, decider keys) | V14 #320 |
| S9 retraction through recursion and negation, and S9b with a second support | `retraction::s9_*`, `retraction::s9b_*` (live, and both through the oracle) | 1, 3 | V4 #311, V5 #312; V0 #307 keeps it green |
| S12 restart mid-scenario | `restart::s12_restart_mid_scenario_preserves_revisions` | 3 | V18 #330 |
| S15 multi-tenant isolation and scoped keys | `tenancy::s15_tenants_and_scoped_keys_are_isolated` | 1 (per tenant) | V10 #317 |

Each scenario's doc comment states its steps, its row and its gates. S9
follows the table on the shop pack: the cut retracts (i1, i3), (i1, i4) and
the offers behind them, and blocking and unblocking i4 are quiet because
nothing reaches i4 from o-42 after the cut; S9b adds `link(i5, i4)` so the
cut keeps (o-42, i4) and the negation flips it one row each way. Both
histories also run through the differential oracle (`oracle_check`, which
includes the oracle's adapters by path): the reference, recompute,
subscription and subscription-group adapters must agree at every checkpoint,
and the reference must model the whole history.

Where the strategy's table and the documented contract differ, a scenario
asserts the contract and says so in its doc comment: revisions restart with
the engine and are paired with the run's stream epoch (S12 asserts a new epoch,
a refused pre-crash `expect_revision` pinned to its epoch, and, as an expected
failure (#380), a refused bare pre-crash `expect_revision` once the new run
has issued that revision again; it also asserts the notifications after the
reconnect are exactly the new run's writes with contiguous `seq`), and a `writer` key may subscribe on
its own graph (S15 refuses its access to the other graph instead). A
permission refusal carries no structured `code` today; S15 asserts
`access_denied` as an expected failure (`KnownDefect`, #364), so it fails
loudly once the code is sent.

### Expected-failure scenarios

The scenarios whose contract is milestone 9's: each runs the part that holds
today as a required pass and asserts the rest as an expected failure
(`Reproduction::Deterministic`) naming the issue that makes it hold. When that
issue lands the XFAIL turns into an XPASS, which fails the run until the
marker is removed and the part becomes required.

| Scenario | Test | Required today | Expected failure (issue) |
|----------|------|----------------|--------------------------|
| S2 one-row view read is a lookup | `reads::s2_one_row_view_read_is_a_lookup` | rows of 50 `eligible` reads and the recursive `related` | no rule evaluated, every read served from a view (V1 #308 counters, then V9 #315) |
| S3 ad-hoc query joins facts and views | `reads::s3_ad_hoc_query_joins_facts_and_views` | rows equal the question asked of base relations; a session rule joins the `offer` view | only the query's own work (V1 #308, then V9 #315) |
| S4 subscription and query agree at a revision | `consistency::s4_subscription_and_query_agree_at_a_revision` | exact deltas at each write's revision | query replies carry `revision` (V9 #315); `at: r` reads answer at r, `at: s` equals the snapshot, a compacted revision is refused `revision_compacted` (V13 #316) |
| S5 many agents, each hears its key | `fanout::s5_many_agents_each_hear_only_their_key` | 200 keyed and 10 unkeyed agents: exactly the touched keys and the unkeyed agents hear a write, nobody hears a write that changes no row | one maintenance pass per commit, no rule evaluated by refreshes (V1 #308, then V10 #317) |
| S6 subscribe only to a deployed rule | `subscribe_rules::s6_subscribe_to_a_query_is_refused` | the same bodies answer as queries; a rule subscription still works | `.subscribe` of a join or a filtered view refused `invalid_request`, `deploy a rule first` (V11 #318) |
| S8 rule replaced while subscribed | `generations::s8_rule_replaced_while_subscribed` | one exact old-vs-new delta at the replacement's revision, concurrent reads see one generation, `.rule remove` is one exact delta | dropping `link` under `related` refused with `conflict` naming the dependents (V7 #314) |
| S17 write burst, read-your-writes | `burst::s17_write_burst_reads_its_writes` | 4 writers x 100 writes, each read back on two connections; the agent hears every row once, through a rule rebuild | the rebuild's acknowledgement carries `views_at` (V14 #320) |

A check that runs into two issues one after the other (the counters do not
exist before V1, and once they do they show the work V9 or V10 removes) is
judged with `KnownDefect::judge_first`, so each stage is an XFAIL of its own
issue. The counter checks and S4's `revision` are asserted for the target
contract, which the default `recompute` mode never reaches; until V20 removes
it, a `recompute` run of the matrix keeps those markers when the `maintained`
run flips. Latency halves (S2's p50 at lab size, S17's 1,000-write shape) are
the perf tier's B1/B2 and B15 on the benchmark host.

### Reactive agent path

The `reactive` module drives the supported agent path end to end: each test
starts a real `inputlayer-server` process with its own data directory, agents
subscribe to standing queries over `/ws`, and independent writer connections
insert and retract facts and change rules. Agents must receive the exact
added/retracted rows as `subscription_delta` pushes, with contiguous `seq` and
increasing `revision`, and end equal to a fresh full query on another
connection, without re-querying.
Scenarios cover one subscriber, 64 subscribers, reconnect/resubscribe and
crash-restart, unrelated writes, write bursts, deltas arriving while the
agent's own long query runs on its connection, and an agent cancelling that
query by id (`cancel`) and keeping its subscription.

Deadlines and cancellation are tested adversarially over an in-process `/ws`
connection by `tests/ws_cancel_tests.rs`: a queued request whose deadline
passes or that is cancelled never runs later, a running query stops promptly,
and a large write cancelled at increasing delays across its commit boundary
always reports an outcome that matches the data, and a blind retry leaves
exactly one copy.

Every request the harness sends carries an `id`, and a reply that does not
echo it fails the scenario (`Violation::Uncorrelated`); a `notice` is never
taken for a reply. `tests/scenarios/wire.rs` pipelines requests (malformed
ones included) while the engine interleaves pushes, a streamed result and a
`notifications_missed` notice, and requires every reply to correlate in order.

`tests/scenarios/stream.rs` requires the stream contract: notifications
arrive in strictly increasing `seq` order under concurrent writers, a reconnect
cursor from before an engine restart gets one `replay_gap` notice and nothing
replayed, and commits racing a `.subscribe` all reach the agent.

Results over `storage.performance.max_result_rows` are required to fail
closed: the subscription is refused, or a refresh pushes `subscription_error`
and the next delta is relative to the last complete result.

`tests/scenarios/delivery.rs` requires payloads over one frame to arrive
whole: a delta past the 16 MiB frame limit streams as one logical delta that
the agent applies only at its end and converges from, and a large snapshot
streams as a `.subscribe` reply naming its subscription. The testkit agent
rejects a streamed delta whose chunks are missing, duplicated, out of order or
short of its end frame's counts (`Violation::BrokenStream`).

`tests/scenarios/saturation.rs` requires correct deliveries under overload,
at the scale of issue #292: 960 sessions, each subscribed to its own bound
standing query, while writers saturate the engine for 30 s. Each probe's delta
reaches exactly its session, once, within a bound of the write's
acknowledgement, and no session gets a stray delta or a `subscription_error`
(its module doc states the full contract). Its timing bounds hold for a
release engine, so it runs only in `make e2e-reactive` and is ignored in
debug builds.

`tests/scenarios/views.rs` requires the view work counters on
`/metrics/prometheus` to tell how reads of deployed rules were answered:
today each read of a deployed rule, subscribing to one and each refresh is
exactly one rule evaluation, a read of base relations is none, and nothing is
served from a view yet.

Quarantined scenarios are ignored unconditionally, so the PR gate (plain
`cargo test`) stays deterministic, and run in the nightly tier. Until a
nightly workflow exists, `make e2e-reactive` stands in for it and passes
`--include-ignored`. Today: `harness::shop_pack_vector_serves_the_near_rule`,
quarantined for a rare engine hang in `.index create` (#377).

Tracked defects run as **expected failures** through
`inputlayer_testkit::KnownDefect`, naming the issue that fixes them (the
milestone 9 ones are listed under Expected-failure scenarios;
`tenancy::s15_tenants_and_scoped_keys_are_isolated`, #364;
`restart::s12_restart_mid_scenario_preserves_revisions`, #380). Each asserts the
correct contract; its own violation passes as `XFAIL`, any other violation
fails, and a holding contract fails as `XPASS` so the marker is removed and
the scenario becomes required when the issue lands. `make unit-test`,
`make ci-test-all` and `make e2e-reactive` print the open `XFAIL` list when
the run ends (`make xfail-list` prints the last run's): `KnownDefect` appends
each line to the file named by `INPUTLAYER_XFAIL_LOG`.

Not yet covered by this pipeline (each is added when the work that enables it
lands):

- Public Python and JavaScript SDK agents. Agents use the testkit's raw `/ws`
  client until the SDKs have a subscribe API; cross-SDK conformance comes with
  R3.
- Pending calls interleaved with pushes on one connection, and slow
  consumers. The perf gate's `interference` fixture measures
  slow-consumer latency, not correctness.
- Credential revocation. It is covered over a real `/ws` connection by
  `tests/credential_revocation_tests.rs`, not here. Handler unit tests in
  `src/protocol/handler/credential_mutation_tests.rs` also cover failed password
  and role replacements, user drop and recreation across restart, grants and
  API keys refused for unknown users, and a bootstrap key that fails to store.
- Gateway finding additions, resolutions and authoritative reset.
- Running the same histories against recompute and persistent-dataflow modes
  (R4). Until then, the differential oracle (`make oracle-test`) compares
  recompute and subscription maintenance against a naive reference.

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

`make test-affected` maps changed files to the spec categories they affect and runs only those (`run_snapshot_tests.sh --affected REF`; the PR gate uses the same map). Changes are the working tree against `REF`, so uncommitted edits count.

```bash
./scripts/test-affected.sh          # Changes since HEAD (uncommitted)
./scripts/test-affected.sh HEAD~3   # Changes in last 3 commits
./scripts/test-affected.sh main     # Changes since main branch
./scripts/run_snapshot_tests.sh --debug --affected main   # The same, on debug binaries
```

File-to-category mapping:
- `src/vector_ops.rs`, `src/hnsw_index.rs`, `src/hnsw_index_tests.rs` → `16_vectors`, `30_quantization`, `31_lsh`
- `src/temporal_ops.rs` → `29_temporal`
- `examples/iql/<category>/...` → that category
- any other file under `src/`, `Cargo.toml`, `Cargo.lock`, `config.toml`, `ws-protocol/`, `ontology-client/`, `scripts/run_snapshot_tests.sh` → every spec (a plan or parser change can reach any category; the whole corpus takes about 1.5 min on 4 cores against debug binaries)
- anything else (tests, docs, SDKs) → no specs

## Makefile Targets

### Development Workflow

| Target | Description | When to Use |
|--------|-------------|-------------|
| `make test-fast` | Unit tests only | Quick feedback during coding |
| `make test` | Unit + snapshot | Broad local check before a PR |
| `make test-all` | Build + unit + snapshot + check | Full verification before merge |
| `make test-affected` | Snapshot tests for changed files only | Fast E2E feedback |
| `make pre-pr` | [Pre-PR pipeline](CONTRIBUTING#pre-commit-checks) | Before every push to a PR |
| `make perf-gate` | Paired latency/throughput gate over `/ws` vs the approved baseline | Every implementation PR (see `perf-gate/README.md`) |
| `make perf-gate-remote` | The same gate for a commit on the benchmark host | Instead of `make perf-gate` on a shared development box |
| `make bench-engine-remote` | Engine suite (rules, closure, deletes and updates, claims, `.why`, sessions, memory, recovery, WAL share) on the benchmark host | Release checkpoints and engine baselines |
| `make bench-views-remote` | Write and read cost against deployed rules by graph size, rule count and subscribers, with rule-evaluation counts (`perf-gate views`) on the benchmark host | Changing evaluation, subscriptions or rule maintenance (the view work, #305) |
| `make perf-gate-check` | Clippy + unit tests of the gate tool | After changing `perf-gate/` |
| `make pre-pr-selftest` | Behavioural tests of `make pre-pr` routing (`scripts/test_pre_pr.py`) | After changing `Makefile` or `scripts/` |
| `make e2e-reactive` | Scenario suite in release against real engines, latency samples | Subscription or wire changes |
| `make oracle-test` | Differential correctness oracle only | Changing evaluation, subscriptions or rule catalog changes |

### Code Quality

| Target | Description |
|--------|-------------|
| `make check` | Formatting + clippy + doc-check + cargo check |
| `make fmt` | Auto-format code |
| `make lint` | Run clippy lints |
| `make deny` | Supply chain: cargo deny (licenses, sources, bans, advisories) + cargo audit, as in CI (`deny.toml`, `.cargo/audit.toml`) |
| `make secret-check` | gitleaks over every commit reachable from HEAD (`.gitleaks.toml`) |
| `make install-hooks` | Optional hooks: fmt + staged secret scan on commit, clippy on push |
| `make hooks-test` | Prove the hooks reject misformatted Rust and staged credentials |
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
