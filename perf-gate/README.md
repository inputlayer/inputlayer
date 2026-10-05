# Performance gate

`make perf-gate` measures this tree's `inputlayer-server` against the
**approved baseline** on the same host, over the real `/ws` protocol, and
exits zero only when no required metric is worse than its budget. It runs
before every push to a PR as the last step of `make pre-pr`, after the fast checks;
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
`run.json`, which holds every raw sample, `verdict.json`, and `summary.md`
(the absolute numbers of each arm, from `perf-gate summary <run.json>`).

## The benchmark host

Heavy perf runs go to the dedicated benchmark host, `sam-dev-benchmarks`
(32 vCPU AMD EPYC-Genoa, 125 GB, nothing else running), not to a shared
development box. On the development box, run only the mandatory fast checks
and send the measurement there:

```bash
make perf-gate-remote                           # the gate for HEAD (commit first)
make perf-gate-remote REV=<commit> PERF_GATE_ARGS="--aa --rounds 20"
make pre-pr PRE_PR_PERF=perf-gate-remote        # pre-pr with the gate measured there
make bench-engine-remote REV=origin/main        # the engine suite (below), not judged
make bench-sessions-remote REV=origin/main      # the session-scale benchmark (below)
make bench-views-remote REV=origin/main         # the views benchmark (below)
```

`scripts/perf-gate-remote.sh` fetches the commit into the host's clone
(`~/bench/inputlayer`), or pushes it there when origin does not have it yet,
checks it out, builds release binaries there, runs `scripts/perf-gate.sh`
detached (a dropped connection does not stop it; `--attach <stamp>`
collects it), and copies `run.json`, `report.md`, `verdict.json`,
`summary.md`, the log and `bench.txt` back to
`target/perf-gate/remote/<stamp>/` (`target/perf-gate/remote/latest`). It
follows the host's rules (`~/README-bench.txt`): one benchmark at a time (a
lock, and a refusal while any `inputlayer-server` runs), `nproc`, the commit
and the command recorded in `bench.txt` with every result, and no server left
running afterwards. The lock is `~/perf-gate-remote/lock` on the host: any
other benchmark there should hold it too (`flock`), and `--wait-lock
<seconds>` queues a run behind one that does. The gate's clients run on CPUs 0-7 and the servers on
8-31, whole SMT core pairs each (`--gate-cpus`, `--server-cpus`).
`PERF_GATE_HOST` selects another host.

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
| `delta_single` | 1 agent subscribed to `?two_hop(1, Z)` on 2.5K nodes / 10K edges; an external writer, open loop, every 20 ms, 150 writes | `delta_us` (scheduled write to delta at agent), `last_agent_us`, `ack_us`, `subscribe_us` |
| `delta_fanout` | 64 agents on the same query, a write every 80 ms, 100 writes | same |
| `delta_first` | 40 fresh agents, one after another, on the `delta_single` graph: each subscribes and the writer inserts a probe at once, then another after the agent has been quiet for 100 ms | `first_delta_us` (first write after subscribing, send to delta), `warm_delta_us` (the later write), `subscribe_us` |
| `delta_keyed` | 300 agents, agent `k` subscribed to `?r1(H_k, Y)` of the deployed rule `r1(X, Y) <- edge(X, Y), label(X, "hot")` over 100K edges (four-node chains, every hundredth head `H` labelled hot); an external writer, open loop, every 20 ms, 120 writes, write `w` adding one row to agent `w mod 300` | `delta_us` (scheduled write to delta at its agent), `ack_us`, `subscribe_us`, `writes_per_server_cpu_sec` (writes per second of server CPU over the write phase) |
| `interference` | 1 probe agent while another connection loops a long join (`?two_hop(X, Z), edge(Z, X)`) and a slow consumer stops reading with large results pending; a write every 30 ms, 120 writes | `delta_us`, `ack_us`, `long_request_us` |

Each fixture also records `server_peak_rss_kb`.

In the delta fixtures, each write inserts `edge(1, P_k)`. That adds exactly
the row `two_hop(1, Q_k)`. The same write retracts the probe inserted 32
writes earlier, so the result size stays bounded and every retraction is
measured as well. Delta and interference writers send on a fixed schedule
independently of acknowledgement collection. Both acknowledgement and delta
latency start at the scheduled write time, including any delay from socket
backpressure or task scheduling (no coordinated omission). The run-file schema
remains `inputlayer-perf-gate/v1`.

`delta_first` instead writes right after each subscription, the moment a
reactive agent's first change typically follows it. Its first delta must
cost what a warm one does: subscribing must leave no work, and no transport
stall, for the first write to pay. (A ~40 ms first delta is the signature
of Nagle's algorithm waiting on the agent's delayed ACK of the subscribe
reply.)

`delta_keyed` is the keyed case of the views benchmark (#308): every view
reads `edge`, so every write refreshes all 300, and they differ only in
their constant. The engine evaluates such a family once per write and hands
each view its rows; evaluating each view's own query instead costs a write
many times the server CPU long before the deltas arrive late (issue #378:
66 times at 1M edges and 1,000 keys), which `writes_per_server_cpu_sec`
shows on an idle host too.

The `quick` profile is for developing the gate. It has too few samples for
p99 and therefore never passes.

## Engine suite (not gated)

The gate's fixtures cover protocol overhead, a bound recursive query,
durable inserts and write-to-delta latency. The engine suite measures what
they leave out, with the same harness, servers and correctness checks. Its
fixtures are not in the policy's `required` set: they are measured and
reported, not gated. `--fixtures engine` runs them, `--fixtures all` runs
both groups, and the default stays the gate's nine.

| Fixture | Workload (profile `standard`) | Series / rates / gauges |
|---|---|---|
| `rule_query` | warm `?two_hop(1, Z)` (non-recursive join rule) on 2.5K nodes / 10K edges: 300 serial, 8 clients x 100 | `latency_us`, `concurrent_latency_us`, `queries_per_sec` |
| `unbound_query` | `?reach(X, Y)`, the whole transitive closure of 200 nodes / 300 edges (about 12K rows): 40 serial, 4 x 10 | same |
| `writes` | 150 durable deletes `-event(i, i)`, 150 conditional deletes `-event(X, Y) <- event(X, Y), X = i`, 150 conditional updates `-event(K, V), +event(K, 0) <- event(K, V), K = i` | `delete_ack_us`, `conditional_delete_ack_us`, `update_ack_us` |
| `claims` | the guarded insert an SDK `claim()` sends (`-il_ghost(0), +claim(K, o) <- task(K), K = k, !claim(K, _)`): 150 wins on fresh keys, 150 losses on held keys, then 8 connections racing for each of 40 keys (exactly one winner each, checked) | `win_ack_us`, `lose_ack_us`, `race_ack_us` |
| `why` | `.why ?two_hop(1, Z)` and `.why ?reach(1, Y)` on 200 nodes / 300 edges, 20 each | `two_hop_us`, `reach_us` |
| `sessions` | 100 sessions in one graph (2.5K nodes / 10K edges), session `k` subscribed to `?two_hop(k, Z)`; an external writer, open loop, every 200 ms, 100 writes, each changing one session's answer | `delta_us` (write to the target session's delta), `ack_us`, `subscribe_us`, `subscriptions_per_sec`, `rss_per_session_kb` |
| `memory_facts` | a fresh server: 90K two-integer facts loaded, then read back | `rss_idle_kb`, `rss_bytes_per_fact` (after loading), `peak_bytes_per_fact` (high-water mark after reading them back) |
| `memory_graphs` | a fresh server: 8 graphs of 2.5K nodes / 10K edges with the two-hop rule, each queried once | `rss_idle_kb`, `rss_first_graph_kb`, `rss_per_graph_kb` (mean growth over the next 7) |
| `recovery` | 10K edges, the two-hop rule and 50K durable facts in 1K batches; then 3 times: SIGKILL, restart on the same data directory | `restart_ready_us` (kill to first accepted login), `first_query_us` (first `?two_hop(1, Z)` after it; every fact is checked) |
| `insert_async` | `insert_single` on a server with `storage.persist.durability_mode = async` (recorded in the run file): with `insert_single`, the synchronous WAL's share of a write | as `insert_single` |

`delta_single`, `delta_fanout` and `delta_first` already cover write-to-delta
latency with one and with 64 agents on one query; `sessions` adds many
sessions with different bound queries in one graph, at a gentle write rate.
The session-scale benchmark (below) loads them until they saturate. Every fixture records
`server_peak_rss_kb`.

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
other CPUs. Per-PR CI does not measure: a shared runner is too noisy to
give a verdict, so the PR gate only lints and tests the tool
(`make perf-gate-check`).

## At release checkpoints

At a release checkpoint, run the heavy measurements on the benchmark host:
the gate against the approved baseline (`make perf-gate-remote REV=<checkpoint>
PERF_GATE_ARGS="--rounds 20"`) and the engine suite (`make
bench-engine-remote REV=<checkpoint>`), and keep the benchmark host busy
with them between checkpoints rather than measuring on a development box.
The CI job below measures the same commit on a hosted runner.

### CI

The full suite (`.github/workflows/full-suite.yml`, job `Performance gate`)
runs the gate on every release checkpoint, on a dedicated 32-vCPU runner
with nothing else on it. Both arms run on that one runner, so its speed
cancels out of the paired ratios. `scripts/perf-gate-ci.sh` drives it:

- The baseline and the checkout are built with `--release --all-features`.
  Baseline servers are cached per commit and toolchain.
- The servers are pinned to CPUs 2 and up, leaving 0-1 to the gate's
  clients. The job sets the `performance` frequency governor and turns
  boost off where the VM exposes them; the report records the governor.
- It runs 20 rounds. If the verdict is INCONCLUSIVE, it reruns once with 30.
  Change the counts with `PERF_GATE_ROUNDS` and `PERF_GATE_RETRY_ROUNDS`.
- FAIL or INVALID fails the checkpoint. A retry that is still INCONCLUSIVE
  passes the job with a warning: runner noise alone does not block a
  release, but it is not a PASS, and the summary says so. Each attempt's
  report is in the job summary. The
  `perf-gate-runs` artifact holds every attempt's `run.json`, `report.md`
  and `verdict.json` for 30 days.

To measure a branch before its checkpoint, dispatch the workflow with only
the performance runs (the gate and the `Views benchmark` job):
`gh workflow run full-suite.yml --ref <branch> -f perf_gate_only=true`.
Add `-f perf_gate_aa=true` to measure the baseline against itself, which
checks the runner's noise and records a calibration run for the baseline.

The runner is not yet quiet enough for a PASS every time (#198). An A/A run
on it (2026-10-04, 20 then 30 rounds) was INCONCLUSIVE on 7 required
metrics, among them `insert_single.ack_us.p50`, `insert_batch.ack_us.p50`
and the `delta_single` delta percentiles.

## Relationship to the Criterion benches

The Criterion harnesses under `benches/` are in-process diagnostic
microbenchmarks and stay as they are. They report means, not request p99.
Some of them disable persistence, or time a refresh without the insert that
triggered it. Use them to investigate a regression the gate finds, not as
the acceptance oracle. `view_maintenance_benchmarks` is the headroom
reference of the views benchmark (below).

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
converged time, one serial writer). Convergence requires a successful, complete
re-query for every unretired subscription and a matching maintained answer without a
subscription error. Missing ground truth (including truncated re-queries) never
counts as convergence or contributes a convergence latency sample. A question
whose re-query fails is retired: its subscriptions are reported as `retired`
and no longer count toward convergence, so later mutations still converge on
the remaining answers; a mutation with every question retired measured nothing
and is not counted. A subscription error stops counting once a later
re-query of its question succeeds.

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

## Session-scale benchmark

`make bench-sessions` measures what standing queries cost as the sessions on
one knowledge graph grow. The workload is the voice-agent reference
architecture (`content/blog/building-a-voice-agent-that-knows.mdx`). It uses the
Appendix A rule pack verbatim (`src/sessions/pack.iql`) and repeats the
method of the architecture's hostile review. It is a scaling benchmark, not a
baseline comparison; it does not replace `make perf-gate`.

```bash
make bench-sessions                                          # 1,10,25,50,100,200 sessions
make bench-sessions SESSIONS_ARGS="--baseline-rev origin/main"   # before/after, same host
make bench-sessions SESSIONS_ARGS="--sessions 100 --max-delta-p99-ms 100"  # voice budget
scripts/bench-sessions.sh --help
```

Each size runs on a fresh server with the gate's configuration. The one extra
setting is no cap on unauthenticated connections per address (hundreds of
local sockets). Session connections authenticate with an API key, because
concurrent password logins from one address are throttled by design.

- **World.** Per session: an order, its shipment with an on-time ETA, an open
  session with a `delivery_status` goal, a speech owner and an executor lease.
- **Subscriptions.** Every session, on its own connection, subscribes to the
  article's speech query `?speech("s-i", ...), speech_owner("s-i", "sp-i", 1)`.
  One executor connection subscribes to `work` for all sessions. With
  `--mode shared-view`, one connection instead subscribes to the unbound
  speech query and routes rows by session in the client: the review's
  comparison point.
- **Probe.** An adapter connection replaces a random session's ETA (the
  article's section 4.1 revision) and times the commit reply and the arrival
  of that session's delta. The delta must carry the new ETA as a `state`
  row. Without load, 40 probes run 20 ms apart. Then a probe runs every
  150 ms for `--load-secs`, after a 2 s warm-up.
- **Load.** Every session commits a receipt (`playback`, `playback_time`)
  about once per second (`--rate`), with ±50% jitter, on its own connection.
  The receipts name a goal no claim reads. Each write is in every speech
  query's dependencies yet changes no result, which isolates the cost that
  sessions impose on each other.
- **Measured.** Write-to-delta and commit latency percentiles with and
  without load, receipt commit latency, achieved receipts per second, and
  server CPU (user plus system, from `/proc`) over the loaded window, plus
  peak RSS.

A probe waits 10 s for its delta. A delta that comes later still counts, as
**late**: it is reported with its latency (`late_delta_ms`) but kept out of
the latency summaries. Once the load stops, overdue probes get 60 s more.
A run fails when a probe's delta never arrives (**missing**), when any delta
arrives that no probe caused (**stray**), or on an error. A saturated server
can be late, never missing or stray. `--max-delta-p99-ms`
additionally fails the working tree's run at any size up to
`--budget-sessions` whose loaded write-to-delta p99 is over budget or that
has any late delta (a late delta is over any budget below the probe timeout).
`target/bench-sessions/latest/` holds `result.json` (schema
`inputlayer-perf-gate/sessions/v2`; v1 counted late deltas as missing and
then stray) and `summary.md`.

## Views benchmark

`make bench-views` measures what writes and reads cost against deployed
rules as the graph, the rule catalog and the subscribers grow (#308). It is
the baseline of the work that turns persistent rules into maintained views
(epic #305): the matrix of the incremental-views review (its report, Section
4) as a perf-gate scenario, `perf-gate views`. Latency is reported, not
judged. The acceptance bars of the view work are checked against these
numbers.

```bash
make bench-views                                        # 10K, 100K, 1M edges
make bench-views VIEWS_ARGS="--edges 10000,100000"      # smaller graphs
make bench-views VIEWS_ARGS="--baseline-rev origin/main"   # before/after, same host
make bench-views-remote REV=origin/main                 # on the benchmark host, under its lock
scripts/bench-views.sh --help
```

Each graph size runs on a fresh server with the gate's configuration plus
caps lifted for 1,000 subscriber connections from one address. Durability is
the default (`immediate`: one fsync per commit).

- **Graph.** Chains of four nodes (three edges each); every hundredth chain
  head is labelled hot.
- **Rules.** `r1(X,Y) <- edge(X,Y), label(X,"hot")` (non-recursive), `path`
  (the right-recursive transitive closure, two rows per edge) and
  `hot_reach(X,Y) <- label(X,"hot"), path(X,Y)` (a recursive dependency,
  three rows per hot head).
- **Idle reads.** A base point read `?edge(K,Y)`, `r1` unbound and bound,
  `path` bound, `hot_reach` unbound and bound, 10 times each (`--reads`):
  latency, rows and server CPU per read.
- **Write phases.** Each write inserts an edge from a hot head to a fresh
  node, which changes every view. The agent's ad-hoc bound read `?r1(K,Y)`
  follows on the writing connection. A phase times the write's reply (ack),
  the last delta it must cause (write->delta) and that read. It also records
  server CPU per write (the read included), frames pushed to subscribers per
  write and RSS. P0 has no subscribers; P1 one and P2 100 unbound subscribers
  of `r1`; P3 one unbound subscriber of `hot_reach`. P4 has keyed subscribers
  `?r1(K_i,Y)`, 1/10/100/1,000 (`--keyed`), and P5 keyed subscribers of the
  recursive view (`--recursive-keyed`, 1/100, or 1/10 from 1M edges). Each
  write changes one key, and only that key's subscriber waits for it. P6 is
  one unbound `r1` subscriber after 50 and then 200 unrelated rules join the
  catalog (`--filler-rules`), with the `r1` idle reads repeated. Phases run
  30 writes each (`--writes`); at 1M edges or more, the subscriber phases run
  20 (`--large-writes`).
- **View work.** Every read and phase records the engine's view counters
  from `/metrics/prometheus` per read or per write:
  `inputlayer_rule_evaluations_total` (reads answered by evaluating deployed
  rules), `inputlayer_view_reads_total` (reads served from a view) and
  `inputlayer_view_maintenance_us_total`. Today every read of a deployed rule
  is one rule evaluation. A standing query's refresh is one too, so P4 with
  10 keyed subscribers shows 11 per write. The columns are empty for a server
  that does not export the counters.

A delta that does not arrive within 20 s of its write is **late**. A late
delta, a failed subscription or a failed step fails the run (exit 1).
`target/bench-views/latest/` holds `result.json` (schema
`inputlayer-perf-gate/views/v1`) and `summary.md`, with tables in the review
report's shape. Full-suite runs include it as the `Views benchmark` job,
report-only, on the perf gate's 32-vCPU runner. Numbers comparable with the
review come from the benchmark host.

The headroom reference is `cargo bench --bench view_maintenance_benchmarks`.
It is the review's differential-dataflow micro-benchmark: the same graph and
rules kept as long-lived arrangements on one worker, with a single-edge
insert, a retraction and a non-hot insert each propagated through every view,
and a key lookup. It prints load time, RSS and rows per size
(`VIEW_BENCH_EDGES=10000,100000` picks sizes).

## Extending

New fixtures go in `src/fixtures/`. Add a variant to `Fixture`, add its
parameters to `Profile`, and list its gated metrics in `policy.toml`. The
run-file schema (`src/schema.rs`, `inputlayer-perf-gate/v1`) is stable: a
change in meaning needs a new schema version, and the gate refuses to judge
run files with an unknown schema. SDK-level fixtures (Python/JS subscription
clients) belong here once the SDKs expose subscriptions.
