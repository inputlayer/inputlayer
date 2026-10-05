# Engine baseline, 2026-10-04

What the symbolic engine alone measures on the dedicated benchmark host, at
main `25058f160c2360d7c126c1d530db235e165bf792`. Engine only: a release
`inputlayer-server` driven over its `/ws` protocol by the perf-gate harness,
with no model, gateway or decision pipeline in the path. These are absolute
numbers to compare later runs on the same host against. They are not a gate
verdict.

## Machine and method

| | |
|---|---|
| host | `sam-dev-benchmarks` (`computeinstance-e00nfsdgf9dzq987g3`), dedicated: one benchmark at a time, nothing else running |
| CPU | AMD EPYC-Genoa, `nproc` 32 (16 cores x 2 SMT), 125 GB RAM, kernel 6.11.0-1016-nvidia, ext4 data disk |
| pinning | harness clients on CPUs 0-7, server on CPUs 8-31 (whole core pairs) |
| server | `25058f16`, `cargo build --release --all-features`, rustc 1.99.0; production configuration except the overrides recorded in each run file (no per-connection or per-IP request caps, GUI off); durability `immediate` |
| harness | perf-gate at `fm/bench-machine` (gate fixtures identical to main's; engine suite added on that branch) |
| statistic | each number is the median over runs (one per round and arm; both arms are the same binary) of that run's nearest-rank p50 or p99, rate or gauge; the range is that statistic's spread over runs |

Every timed reply is checked against an answer computed independently from
the fixed-seed dataset. A run with a wrong or missing result counts as failed,
and none failed.

The numbers come from three runs, each started with `scripts/perf-gate-remote.sh`:

| run | UTC | harness commit | command on the host |
|---|---|---|---|
| gate fixtures, 30 rounds x 2 arms | 19:54-20:20 | `e301b1e8` | `scripts/perf-gate.sh --aa --baseline-rev 25058f160c2360d7c126c1d530db235e165bf792 --rounds 30 --gate-cpus 0-7 --server-cpus 8-31` |
| engine suite, 10 rounds x 2 arms | 19:30-19:54 | `af00a997` | `scripts/perf-gate.sh --aa --baseline-rev 25058f160c2360d7c126c1d530db235e165bf792 --fixtures engine --no-verdict --rounds 10 --gate-cpus 0-7 --server-cpus 8-31` |
| memory fixtures, 10 rounds x 2 arms | 21:50-21:51 (it waited for another lane's lease on the host lock) | `ca5a2842` | `scripts/perf-gate.sh --aa --baseline-rev 25058f160c2360d7c126c1d530db235e165bf792 --fixtures memory_facts,memory_graphs --no-verdict --rounds 10 --gate-cpus 0-7 --server-cpus 8-31` |

The raw samples are in `25058f16-aa-sam-dev-benchmarks.run.json.gz`,
`engine-baseline-2026-10-04-engine.run.json.gz` and
`engine-baseline-2026-10-04-memory.run.json.gz` next to this file.
`perf-gate summary <run.json>` reproduces every table below from them.
Fixture workloads are described in [`../README.md`](../README.md).

## Headline

| What | Workload | p50 | p99 |
|---|---|---|---|
| Query, base relation | `?edge(1, Y)`, 2K nodes / 4K edges, serial | 0.366 ms | 0.408 ms |
| Query, non-recursive rule | `?two_hop(1, Z)`, 2.5K nodes / 10K edges, serial | 2.002 ms | 2.199 ms |
| Query, bound recursive rule | `?reach(1, Y)` (Magic Sets), 2K nodes / 4K edges, serial | 8.361 ms | 8.639 ms |
| Query, unbound recursive rule | `?reach(X, Y)`, the whole closure (11,785 rows) of 200 nodes / 300 edges | 45.474 ms | 46.009 ms |
| Durable insert, one fact | `+event(i, i)`, serial, fsync per commit | 1.563 ms | 7.008 ms |
| Durable insert, 1,000 facts | one batch | 3.875 ms | 16.617 ms |
| Durable delete, one fact | `-event(i, i)` | 1.946 ms | 7.488 ms |
| Conditional update, one fact | `-event(K, V), +event(K, 0) <- event(K, V), K = i` | 2.107 ms | 8.194 ms |
| Guarded insert (`claim`), wins | fresh key | 1.889 ms | 7.154 ms |
| Guarded insert (`claim`), loses | key already held | 0.432 ms | 0.493 ms |
| Guarded insert, 8 connections racing for one key | exactly one winner per key, checked | 3.814 ms | 10.043 ms |
| Write to delta, 1 subscribed agent | external writer every 20 ms, `?two_hop(1, Z)` on 10K edges | 7.229 ms | 13.496 ms |
| Write to delta, 64 agents on one query | every agent's delta | 7.993 ms | 15.021 ms |
| Write to delta, 100 sessions with their own bound queries | one graph, session `k` on `?two_hop(k, Z)`; the target session's delta | 8.302 ms | 14.121 ms |
| First delta after subscribing | fresh agent, write at once | 4.188 ms | 10.524 ms |
| `.why` proof, non-recursive rule | `.why ?two_hop(1, Z)`, 200 nodes / 300 edges | 3.384 ms | 3.523 ms |
| `.why` proof, recursive rule | `.why ?reach(1, Y)` (4 rows), same graph | 1.67 s | 1.68 s |
| Crash recovery | SIGKILL, restart on the same data (10K edges, 50K facts) to first accepted login | 103.8 ms | 153.8 ms |
| First query after recovery | `?two_hop(1, Z)` | 3.913 ms | 4.147 ms |

| Throughput | per second |
|---|---|
| `?edge(1, Y)`, 8 clients | 13,244 queries |
| `?two_hop(1, Z)`, 8 clients | 3,860 queries |
| `?reach(1, Y)`, 8 clients | 899 queries |
| `?reach(X, Y)` (whole closure), 4 clients | 81 queries |
| durable single-fact inserts, 1 writer | 560 facts |
| durable single-fact inserts, 4 writers | 604 facts |
| durable 1,000-fact batches, 1 writer | 177,583 facts |
| single-fact inserts with `durability_mode = async`, 1 writer | 5,809 facts |
| subscriptions opened, one after another (100 sessions) | 56 |

| Memory (resident set) | |
|---|---|
| idle server | 36.4 MB |
| first graph (2.5K nodes / 10K edges, two-hop rule, queried) | 9.2 MB |
| each further graph of the same size | 2.1 MB |
| per base fact (90K two-integer facts), after loading | 81 bytes |
| per base fact, high-water mark after reading all 90K back | 402 bytes |
| per session subscribed to its own bound query (`sessions`) | 87 KB |

## Findings

- **A recursive `.why` costs 1.67 s here, 500 times the non-recursive one.**
  `.why ?reach(1, Y)` proves 4 rows on a 300-edge graph in 1.67 s
  (1.65-1.70 s in every run), against 3.4 ms for `.why ?two_hop(1, Z)` and
  8.4 ms for the bound recursive query itself. A local smoke test of an
  older build took 1.2-2.7 s even on a 4-edge graph. The cost does not
  track the answer's size.
- **The synchronous WAL is most of a single write.** One durable fact
  acknowledges in 1.563 ms p50; with `durability_mode = async` the same
  write takes 0.143 ms (5,809 against 560 facts/s serial). Batching
  amortizes it: 1,000 facts commit in 3.9 ms.
- **100 sessions in one graph cost the target session about 1 ms of delta
  latency** over a single agent (8.3 against 7.2 ms p50) at one write every
  200 ms, with about 87 KB of resident memory per session. Writes were
  spaced, so this does not show saturation. Main does not yet share
  evaluations between sessions whose queries differ only in constants;
  #254 adds that and its own sessions benchmark.
- **A losing claim is cheap, a winning one costs a durable write.** A
  guarded insert whose guard fails returns in 0.43 ms. One that wins pays
  the commit (1.9 ms). With 8 connections racing for each key, every key had
  exactly one winner and the racers saw 3.8 ms p50.

## Coverage

The cases asked for, and where each is measured:

| Case | Fixtures |
|---|---|
| query latency, non-recursive rules | `cheap_query` (base relation), `rule_query` (join rule) |
| query latency, recursive rules | `bound_query` (bound, Magic Sets), `unbound_query` (whole closure) |
| update latency | `insert_single`, `insert_batch`, `writes` (deletes, conditional deletes, conditional updates) |
| write to delta for subscriptions | `delta_single`, `delta_fanout`, `delta_first`, `interference` |
| many concurrent sessions and subscriptions | `delta_fanout` (64 agents, one query), `sessions` (100 sessions, their own bound queries) |
| throughput | the `*_per_sec` rates of the query and insert fixtures |
| guarded commits and claims | `claims` (wins, losses, 8 racers per key) |
| `.why` proof cost | `why` (non-recursive and recursive) |
| memory per graph and per fact | `memory_graphs`, `memory_facts`, and every fixture's peak RSS |
| restart and recovery time | `recovery` (SIGKILL, restart, first query) |
| WAL durability overhead | `insert_async` against `insert_single` |

Not covered yet: aggregates and negation-heavy rules end to end (the
Criterion benches in `benches/` cover aggregates in process), graphs larger
than 10K edges, sessions at saturation (`sessions` spaces writes 200 ms
apart, and #254 brings a sessions benchmark that pushes load), and a
graceful restart (only a crash is measured).

## Noise and the gate (A/A calibration)

The 30-round A/A run of the gate fixtures on this host
(`25058f16-aa-sam-dev-benchmarks.report.md`) is INCONCLUSIVE: 18 of the 22
required metrics PASS. Four do not: `insert_single.ack_us.p99` (interval
0.843..1.153 against a budget of 1.10), `insert_single.facts_per_sec`
(0.972..1.060 against 1.05), `insert_single.concurrent_facts_per_sec`
(0.975..1.072) and `delta_single.delta_us.p99` (0.912..1.110). Every
interval contains 1.0, so this is noise, not a bias between the arms, and
it is concentrated in fsync-bound single writes. A 20-round run at
`bc381b2e` an hour earlier (`bc381b2e-aa-sam-dev-benchmarks.report.md`)
had 8 metrics INCONCLUSIVE, the same four among them. The host is quieter
than the shared development box (where the approved baseline's own
calibration left `insert_single.ack_us.p50` and `insert_batch.ack_us.p50`
INCONCLUSIVE), but it does not yet give a PASS on every required metric.
So the approved baseline is not moved here.

## Full tables

### Gate fixtures (30 rounds x 2 arms)

| fixture | latency series | p50 | p99 | p50 range over runs | runs | samples |
|---|---|---|---|---|---|---|
| bound_query | concurrent_latency_us | 8.337 ms | 12.499 ms | 8.135 ms..8.671 ms | 60 | 12000 |
| bound_query | latency_us | 8.361 ms | 8.639 ms | 8.281 ms..8.907 ms | 60 | 9000 |
| cheap_query | concurrent_latency_us | 0.576 ms | 0.932 ms | 0.543 ms..0.612 ms | 60 | 72000 |
| cheap_query | latency_us | 0.366 ms | 0.408 ms | 0.345 ms..0.395 ms | 60 | 24000 |
| delta_fanout | ack_us | 5.662 ms | 10.674 ms | 5.055 ms..6.283 ms | 60 | 6000 |
| delta_fanout | delta_us | 7.993 ms | 15.021 ms | 7.504 ms..8.743 ms | 60 | 645120 |
| delta_fanout | last_agent_us | 8.135 ms | 15.213 ms | 7.606 ms..8.891 ms | 60 | 10080 |
| delta_fanout | subscribe_us | 1.316 ms | 3.798 ms | 1.249 ms..1.345 ms | 60 | 3840 |
| delta_first | first_delta_us | 4.188 ms | 10.524 ms | 3.843 ms..4.828 ms | 60 | 2400 |
| delta_first | subscribe_us | 3.678 ms | 4.122 ms | 3.438 ms..3.935 ms | 60 | 2400 |
| delta_first | warm_delta_us | 5.011 ms | 10.895 ms | 4.719 ms..5.590 ms | 60 | 2400 |
| delta_single | ack_us | 4.879 ms | 11.246 ms | 4.306 ms..5.669 ms | 60 | 9000 |
| delta_single | delta_us | 7.229 ms | 13.496 ms | 6.521 ms..8.082 ms | 60 | 16080 |
| delta_single | last_agent_us | 7.229 ms | 13.496 ms | 6.521 ms..8.082 ms | 60 | 16080 |
| delta_single | subscribe_us | 3.799 ms | 3.799 ms | 2.600 ms..4.353 ms | 60 | 60 |
| insert_batch | ack_us | 3.875 ms | 16.617 ms | 3.239 ms..4.362 ms | 60 | 1200 |
| insert_single | ack_us | 1.563 ms | 7.008 ms | 1.379 ms..1.781 ms | 60 | 12000 |
| insert_single | concurrent_ack_us | 6.066 ms | 14.566 ms | 5.430 ms..7.640 ms | 60 | 12000 |
| interference | ack_us | 5.096 ms | 11.046 ms | 4.528 ms..5.713 ms | 60 | 7200 |
| interference | delta_us | 7.912 ms | 13.505 ms | 7.347 ms..8.507 ms | 60 | 12480 |
| interference | long_request_us | 196.479 ms | 205.328 ms | 195.171 ms..197.829 ms | 60 | 1140 |

| fixture | rate | per second | range over runs | runs |
|---|---|---|---|---|
| bound_query | queries_per_sec | 899 | 784..931 | 60 |
| cheap_query | queries_per_sec | 13244 | 12349..14106 | 60 |
| insert_batch | facts_per_sec | 177583 | 139825..201205 | 60 |
| insert_single | concurrent_facts_per_sec | 604 | 482..703 | 60 |
| insert_single | facts_per_sec | 560 | 483..679 | 60 |

| fixture | gauge | value | range over runs | runs |
|---|---|---|---|---|
| bound_query | server_peak_rss_kb | 58678 | 56084..64520 | 60 |
| cheap_query | server_peak_rss_kb | 43334 | 42100..44212 | 60 |
| delta_fanout | server_peak_rss_kb | 51732 | 48976..53508 | 60 |
| delta_first | server_peak_rss_kb | 68674 | 48568..71296 | 60 |
| delta_single | server_peak_rss_kb | 49632 | 48592..53208 | 60 |
| insert_batch | server_peak_rss_kb | 41908 | 41256..42876 | 60 |
| insert_single | server_peak_rss_kb | 38780 | 38184..39488 | 60 |
| interference | server_peak_rss_kb | 243658 | 240892..262404 | 60 |

### Engine suite (10 rounds x 2 arms)

The engine run also had the first version of the memory fixture, which
measured facts and graphs on one server; its rows are left out (see
`memory_facts` and `memory_graphs` below).

| fixture | latency series | p50 | p99 | p50 range over runs | runs | samples |
|---|---|---|---|---|---|---|
| claims | lose_ack_us | 0.432 ms | 0.493 ms | 0.406 ms..0.441 ms | 20 | 3000 |
| claims | race_ack_us | 3.814 ms | 10.043 ms | 3.706 ms..4.081 ms | 20 | 6400 |
| claims | win_ack_us | 1.889 ms | 7.154 ms | 1.794 ms..1.999 ms | 20 | 3000 |
| insert_async | ack_us | 0.143 ms | 0.241 ms | 0.125 ms..0.152 ms | 20 | 4000 |
| insert_async | concurrent_ack_us | 0.235 ms | 0.657 ms | 0.203 ms..0.264 ms | 20 | 4000 |
| recovery | first_query_us | 3.913 ms | 4.147 ms | 2.705 ms..4.209 ms | 20 | 60 |
| recovery | restart_ready_us | 103.771 ms | 153.812 ms | 102.841 ms..105.657 ms | 20 | 60 |
| rule_query | concurrent_latency_us | 1.980 ms | 2.449 ms | 1.944 ms..2.036 ms | 20 | 16000 |
| rule_query | latency_us | 2.002 ms | 2.199 ms | 1.986 ms..2.065 ms | 20 | 6000 |
| sessions | ack_us | 4.502 ms | 10.233 ms | 4.078 ms..5.131 ms | 20 | 2000 |
| sessions | delta_us | 8.302 ms | 14.121 ms | 7.794 ms..8.931 ms | 20 | 2000 |
| sessions | subscribe_us | 3.414 ms | 3.895 ms | 3.317 ms..3.567 ms | 20 | 2000 |
| unbound_query | concurrent_latency_us | 45.855 ms | 53.011 ms | 45.214 ms..46.230 ms | 20 | 800 |
| unbound_query | latency_us | 45.474 ms | 46.009 ms | 45.229 ms..46.038 ms | 20 | 800 |
| why | reach_us | 1.67 s | 1.68 s | 1.65 s..1.70 s | 20 | 400 |
| why | two_hop_us | 3.384 ms | 3.523 ms | 3.305 ms..3.513 ms | 20 | 400 |
| writes | conditional_delete_ack_us | 4.466 ms | 10.981 ms | 4.361 ms..4.706 ms | 20 | 3000 |
| writes | delete_ack_us | 1.946 ms | 7.488 ms | 1.817 ms..2.118 ms | 20 | 3000 |
| writes | update_ack_us | 2.107 ms | 8.194 ms | 1.981 ms..2.416 ms | 20 | 3000 |

| fixture | rate | per second | range over runs | runs |
|---|---|---|---|---|
| insert_async | concurrent_facts_per_sec | 15103 | 14018..17992 | 20 |
| insert_async | facts_per_sec | 5809 | 5155..6647 | 20 |
| rule_query | queries_per_sec | 3860 | 3429..4008 | 20 |
| sessions | subscriptions_per_sec | 56 | 55..58 | 20 |
| unbound_query | queries_per_sec | 81 | 80..82 | 20 |

| fixture | gauge | value | range over runs | runs |
|---|---|---|---|---|
| claims | server_peak_rss_kb | 41314 | 40604..41864 | 20 |
| insert_async | server_peak_rss_kb | 38774 | 38432..39268 | 20 |
| recovery | server_peak_rss_kb | 53104 | 52364..54612 | 20 |
| rule_query | server_peak_rss_kb | 52950 | 52076..54404 | 20 |
| sessions | rss_per_session_kb | 87 | 74..92 | 20 |
| sessions | server_peak_rss_kb | 63332 | 61960..65136 | 20 |
| unbound_query | server_peak_rss_kb | 63058 | 62264..68976 | 20 |
| why | server_peak_rss_kb | 40160 | 39536..40572 | 20 |
| writes | server_peak_rss_kb | 40112 | 39748..40568 | 20 |

### Memory fixtures (10 rounds x 2 arms)

| fixture | gauge | value | range over runs | runs |
|---|---|---|---|---|
| memory_facts | peak_bytes_per_fact | 402 | 375..410 | 20 |
| memory_facts | rss_bytes_per_fact | 81 | 52..84 | 20 |
| memory_facts | rss_idle_kb | 36440 | 36016..36944 | 20 |
| memory_facts | server_peak_rss_kb | 72228 | 69296..72600 | 20 |
| memory_graphs | rss_first_graph_kb | 9218 | 6912..9728 | 20 |
| memory_graphs | rss_idle_kb | 36388 | 35768..36752 | 20 |
| memory_graphs | rss_per_graph_kb | 2121 | 2048..2450 | 20 |
| memory_graphs | server_peak_rss_kb | 60536 | 58040..61060 | 20 |
