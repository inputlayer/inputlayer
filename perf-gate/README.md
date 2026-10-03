# Performance gate

`make perf-gate` measures this tree's `inputlayer-server` against the
**approved baseline** on the same host, over the real `/ws` protocol, and
exits zero only when no required metric is worse than its budget. Run it
for every implementation PR and attach the report to the PR record. Changes
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
| `interference` | 1 probe agent while another connection loops a long join (`?two_hop(X, Z), edge(Z, X)`) and a slow consumer stops reading with large results pending; a write every 30 ms, 120 writes | `delta_us`, `ack_us`, `long_request_us` |

Each fixture also records `server_peak_rss_kb`.

In the delta fixtures, each write inserts `edge(1, P_k)`. That adds exactly
the row `two_hop(1, Q_k)`. The same write retracts the probe inserted 32
writes earlier, so the result size stays bounded and every retraction is
measured as well. The writer runs open loop on a fixed schedule, so a slow
server cannot hide latency by slowing the writer down (no coordinated
omission).

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

`baselines/approved.toml` names the approved baseline commit. The
`baselines/` directory keeps that commit's calibration run (`--aa`): the raw
samples plus a report recording the host's run-to-run noise. Only move the
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

## Extending

New fixtures go in `src/fixtures/`. Add a variant to `Fixture`, add its
parameters to `Profile`, and list its gated metrics in `policy.toml`. The
run-file schema (`src/schema.rs`, `inputlayer-perf-gate/v1`) is stable: a
change in meaning needs a new schema version, and the gate refuses to judge
run files with an unknown schema. SDK-level fixtures (Python/JS subscription
clients) belong here once the SDKs expose subscriptions.
