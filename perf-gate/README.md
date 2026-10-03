# Performance gate

`make perf-gate` measures this tree's `inputlayer-server` against the
**approved baseline** on the same host, over the real `/ws` protocol, and
exits zero only when no required metric is worse than its budget. It runs
before every PR as the last step of `make pre-pr`, after the fast checks;
attach the report to the PR record. Changes
that do not touch the runtime still record the run, as evidence that runtime
is unchanged. Being test-only or off the hot path is a statement in the
record, not an exemption.

```bash
make perf-gate                          # candidate = working tree, ~12 min on a quiet host
make perf-gate PERF_GATE_ARGS="--aa"    # baseline vs itself: host noise check
scripts/perf-gate.sh --help             # all options
```

The report lands in `target/perf-gate/latest/report.md`. Next to it are
`run.json`, which holds every raw sample, and `verdict.json`.

## How it measures

- **Black box, same host.** The gate starts real server processes, built the
  way the release image is built (`--release --all-features`), and drives
  them through `/ws` exactly as an agent or SDK does. The server needs no
  instrumentation and holds no timer locks. Every latency is taken in the
  client from send to receipt of the frame, read before the JSON is parsed.
- **Paired and interleaved.** The baseline commit is built from `git archive`
  and cached per commit and toolchain. Each round runs every fixture on both
  arms. Each (round, fixture, arm) gets a fresh server and a fresh data
  directory under `target/`, which is a real disk, so durable inserts pay
  their fsync. The arm order alternates by round and by fixture, so drift on
  the host affects both arms evenly.
- **Production configuration**, apart from the overrides listed in
  `src/server.rs` (`SERVER_OVERRIDES`), each with its reason in the run
  file: no per-connection message cap, no per-IP request cap, GUI off.
  Durability stays `immediate` and the result caps stay on.
- **Correctness first.** Every timed reply is checked against an answer the
  client computes independently from the fixed-seed dataset: row counts for
  queries and inserts, and the exact row each delta must carry. A wrong or
  missing result invalidates the run. A faster but wrong candidate cannot
  pass.
- **No shared per-request harness state.** Each client task records its own
  samples. Writer and agent timestamps are joined only after a run finishes.

## Fixtures

| Fixture | Workload (profile `standard`) | Series / rates |
|---|---|---|
| `cheap_query` | `?edge(1, Y)` on 2K nodes / 4K edges: 400 serial, then 8 clients x 150 | `latency_us`, `concurrent_latency_us`, `queries_per_sec` |
| `bound_query` | warm `?reach(1, Y)` (transitive closure, Magic Sets), same graph: 150 serial, 8 x 25 | same |
| `insert_single` | 200 durable `+event(i, i)` serial, then 4 writers x 50 | `ack_us`, `concurrent_ack_us`, `facts_per_sec`, `concurrent_facts_per_sec` |
| `insert_batch` | 20 durable batches of 1,000 facts | `ack_us`, `facts_per_sec` |
| `delta_single` | 1 agent subscribed to `?two_hop(1, Z)` on 2.5K nodes / 10K edges; an external writer, open loop, every 20 ms, 150 writes | `delta_us` (writer send to delta at agent), `last_agent_us`, `ack_us`, `subscribe_us` |
| `delta_fanout` | 64 agents on the same query, a write every 80 ms, 100 writes | same |
| `delta_first` | 40 fresh agents, one after another, on the `delta_single` graph: each subscribes and the writer inserts a probe at once, then another after the agent has been quiet for 100 ms | `first_delta_us` (first write after subscribing, send to delta), `warm_delta_us` (the later write), `subscribe_us` |
| `interference` | 1 probe agent while another connection loops a long join (`?two_hop(X, Z), edge(Z, X)`) and a slow consumer stops reading with large results pending; a write every 30 ms, 120 writes | `delta_us`, `ack_us`, `long_request_us` |

Each fixture also records `server_peak_rss_kb`.

In the delta fixtures, each write inserts `edge(1, P_k)`. That adds exactly
the row `two_hop(1, Q_k)`. The same write retracts the probe inserted 32
writes earlier, so the result size stays bounded and every retraction is
measured as well. The writer runs open loop on a fixed schedule, so a slow
server cannot hide latency by slowing the writer down (no coordinated
omission).

`delta_first` instead writes right after each subscription, the moment a
reactive agent's first change typically follows it. Its first delta must
cost what a warm one does: subscribing must leave no work, and no transport
stall, for the first write to pay. (A ~40 ms first delta is the signature
of Nagle's algorithm waiting on the agent's delayed ACK of the subscribe
reply.)

The `quick` profile is for developing the gate. It has too few samples for
p99 and therefore never passes.

## How it judges

The policy lives in `policy.toml`. The unit of replication is the round:
each round contributes one value per arm and metric, either a nearest-rank
percentile of that round's raw samples or that round's rate. Both arms of a
round run back to back, so each round yields one **paired cost ratio**,
candidate over baseline. Above 1.0 is worse, for latency and throughput
alike. Drift on the host between rounds cancels out of the ratio. The
estimate is the median of the per-round ratios. Its interval is the exact,
distribution-free 95% confidence interval of a median: binomial order
statistics, with no resampling. At least 6 rounds are needed for an interval
to exist at all. The default of 10 rounds tolerates one outlier round on each
side.

| Status | Meaning | Exit |
|---|---|---|
| PASS | The whole interval is within `1 + tolerance` | 0 |
| FAIL | The whole interval is above `1 + tolerance`: a supported regression | 1 |
| INCONCLUSIVE | The interval straddles the budget: too noisy to tell. Rerun on a quieter host or add rounds | 2 |
| INVALID | A required metric is missing, a run failed or returned a wrong result, or there are too few rounds or samples | 3 |

Only PASS is acceptable. Noisy or missing data cannot pass. The gate is
judged on the metrics listed in `required`. All other series and rates are
reported as diagnostics. A verdict can be recomputed from a kept `run.json`
under a new policy with `perf-gate compare <run.json>`.

The tolerances (p50 +5%, p99 +10%, throughput -5%) are the plan's proposed
ceilings and stay **provisional** until captain decision D1 approves the
budgets and the runner. Tightening a tolerance is always allowed. Loosening
one, or changing the required set, needs a stated reason and review.

## Baseline

`baselines/approved.toml` names the approved baseline commit. That commit's
calibration run (`--aa`) belongs next to it: the gzipped raw samples, plus a
report recording the host's run-to-run noise. Record it in a quiet window,
because an A/A run taken on a busy host is evidence only of the noise. Only move the
baseline in a reviewed change that states why, for example after an
accepted improvement.

## Runner requirements

Results are only as good as the host. Use a host with no other heavy work
running: the report records the load average at the start and end of a run,
and an A/A run (`--aa`) shows whether noise fits the budgets. If you can, pin
the servers with `--server-cpus` (`taskset -c`) and run the gate itself on
other CPUs. A shared CI runner is too noisy to give a verdict, so CI only
lints and tests the tool (`make perf-gate-check`).

## Relationship to the Criterion benches

The six Criterion harnesses under `benches/` are in-process diagnostic
microbenchmarks and stay as they are. They report means, not request p99.
Some of them disable persistence, or time a refresh without the insert that
triggered it. Use them to investigate a regression the gate finds, not as
the acceptance oracle.

## GenBI-trust reactive agent benchmark

`make bench-genbi` measures this tree's server as the substrate for reactive
AI agents on the organisation's
[genbi-trust](https://github.com/inputlayer/genbi-trust) suite: 96 business
scenarios in 24 categories, each with an IQL seed, business questions, ordered
mutations and evaluator-private expected checkpoints. It is a detailed
benchmark, not a baseline comparison; it does not replace `make perf-gate`.

```bash
GENBI_TRUST_DIR=/path/to/genbi-trust make bench-genbi
GENBI_TRUST_DIR=... make bench-genbi GENBI_ARGS="--cases priority --repeat 3"
GENBI_TRUST_DIR=... make bench-genbi GENBI_ARGS="--fault drop-retractions"  # must FAIL
scripts/bench-genbi.sh --help
```

The suite is read in place and never copied into this repository.
`target/genbi-bench/latest/` holds `result.json` (schema
`inputlayer-genbi-bench/v1`: the perf gate's environment fingerprint, raw
microsecond series, rates and gauges per scenario run) and `summary.md`.

Per scenario, on a fresh server with the gate's configuration:

1. **Cold load.** The seed's statements are sent one request each, with
   consecutive bulk inserts into one relation merged into one batch (the same
   facts). `.kg` commands and `?` queries in the seed are dropped. A
   statement that fails is a finding; the seeds are marked unverified
   upstream and are never edited.
2. **Questions.** Every SQL check in `queries.json` and `mutations.json`
   becomes one standing query: equality filters bind constants in one atom,
   selected columns are projected from the full engine rows. SQLite views map
   to seed relations through `src/genbi/binding.rs`. Checks that do not
   translate (aggregates, `EXISTS`, `IN`, relations the seed lacks) are
   reported as unsupported, never approximated.
3. **Subscribe.** The writer evaluates each question once (`initial_query_us`,
   cold), then each agent connection (`--agents`) subscribes to all of them
   (`subscribe_us`); the snapshot is the agent's answer for the `initial`
   checkpoint.
4. **Mutations**, in order, from the independent writer connection. Each SQL
   statement becomes IQL: `INSERT` an insert, `DELETE` a (conditional)
   delete, `UPDATE` a retract and re-insert of the matching facts, read
   untimed beforehand. The seeds keep the reporting clock and period in
   `clock`/`period` as well as `benchmark_context`; an `UPDATE` of the table
   rewrites them too. A mutation's statements go out as one program
   (`ack_us`), as an agent-facing writer would send them; they commit one by
   one, so agents may see intermediate deltas.
5. **Convergence.** Agents apply pushes until none arrives for `--quiet-ms`.
   The writer then re-queries every question (`requery_us`: the full
   re-evaluation a polling agent pays after the ack, and the truth the delta
   path must match), and agents keep applying pushes until their answers
   equal it or `--deadline-ms` passes. `delta_us` is writer send to the last
   delta frame, split into `delta_insert_us` / `delta_retract_us` by whether
   the answer lost rows; `ack_to_delta_us` is the same delta from the ack:
   propagation without the durable commit.
6. **Correctness.** At every checkpoint each agent's maintained answer is
   compared with the expected rows using `score_suite.py`'s rules
   (multiset, integers stay integers). A check fails with `delta divergence`
   when an agent's answer differs from the fresh evaluation, `subscription
   error` on a pushed error, and `result mismatch` when agent and re-query
   agree but differ from the expected rows (the seed's rules or data
   disagree with the reference model). After an unsupported or failed
   mutation, later checkpoints are `not run`. Checks after a change that
   removed rows are tallied separately as retraction correctness.

Memory is `/proc` RSS after load, after subscribing and at the end, plus
peak RSS. Throughput is `load_statements` (seed statements per second of
cold load) and `converged_mutations` (mutations per second of send-to-
converged time, one serial writer).

The summary lists the reactive-path categories first (multiple supports and
retraction, change impact, event replay, conversation revisions, change
cost, ingestion freshness, qualified workflows, then the reasoning
categories), then the rest, then every failing check with its reason and
missing/extra row counts, then all findings. Expected rows are never
printed or written; only counts are.

The command exits 0 when every agent converged on every answer and no
subscription errored; delta-path failures exit 1. `--strict` also fails on
result mismatches and unsupported checks. `--fault drop-retractions` (or
`drop-inserts`) breaks the agents' delta application on purpose: a run with
retractions must then fail, which proves the checks catch a broken delta
path.

## Extending

New fixtures go in `src/fixtures/`. Add a variant to `Fixture`, add its
parameters to `Profile`, and list its gated metrics in `policy.toml`. The
run-file schema (`src/schema.rs`, `inputlayer-perf-gate/v1`) is stable: a
change in meaning needs a new schema version, and the gate refuses to judge
run files with an unknown schema. SDK-level fixtures (Python/JS subscription
clients) belong here once the SDKs expose subscriptions.
