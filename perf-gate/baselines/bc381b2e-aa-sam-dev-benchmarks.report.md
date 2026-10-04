<!--
A/A calibration of main bc381b2e on the benchmark host sam-dev-benchmarks,
2026-10-04 18:41-19:00 UTC, by scripts/perf-gate-remote.sh.
nproc: 32 (16 cores x 2 SMT). The "8 CPUs" below is the gate's own
affinity (--gate-cpus 0-7), recorded before the fingerprint counted the
host's CPUs; the host has 32.
gate tool commit: 53ec7b4d1b2fabc565a6713d45209982b650152a (fm/bench-machine;
its gate fixtures are main's)
command: scripts/perf-gate.sh --aa --baseline-rev bc381b2ef75d207c88e1c7080093cbed9ab4422c --rounds 20 --gate-cpus 0-7 --server-cpus 8-31
The start load average (4.00) is the release build that ran just before.
-->
## Performance gate: INCONCLUSIVE

Policy **provisional**: cost budget p50 +5%, p99 +10%, throughput -5%; 95% interval of the median paired per-round ratio. Budgets are the plan's proposed ceilings, pending captain decision D1; tightening is always allowed.

- baseline: `bc381b2ef75d207c88e1c7080093cbed9ab4422c` (binary sha256 2518a6bd9920)
- candidate: `bc381b2ef75d207c88e1c7080093cbed9ab4422c` (binary sha256 2518a6bd9920)
- profile `standard`, 20 rounds, fixtures cheap_query, bound_query, insert_single, insert_batch, delta_single, delta_fanout, delta_first, interference
- host `computeinstance-e00nfsdgf9dzq987g3`: AMD EPYC-Genoa Processor (8 CPUs, 125 GB), kernel 6.11.0-1016-nvidia, governor unknown, data on ext4 (/home/sam/bench/inputlayer/target/perf-gate/runs/20261004T184333Z/servers), server CPUs 8-31
- load average: start `4.00 2.67 1.30 1/445 28432`, end `0.80 0.83 0.92 1/441 39291`
- build: rustc 1.99.0 (b940084d7 2026-09-28); cargo build --release --all-features

### Required metrics

| metric | baseline | candidate | cost ratio | interval | budget | baseline spread | status |
|---|---|---|---|---|---|---|---|
| bound_query.concurrent_latency_us.p99 | 12.502 ms | 12.575 ms | 1.019 | 0.956..1.048 | 1.10 | 12.6% | PASS |
| bound_query.latency_us.p50 | 8.488 ms | 8.478 ms | 1.004 | 0.991..1.007 | 1.05 | 2.7% | PASS |
| bound_query.latency_us.p99 | 8.735 ms | 8.733 ms | 0.999 | 0.993..1.012 | 1.10 | 6.3% | PASS |
| bound_query.queries_per_sec | 893/s | 881/s | 1.014 | 0.978..1.043 | 1.05 | 11.7% | PASS |
| cheap_query.latency_us.p50 | 0.366 ms | 0.366 ms | 1.003 | 0.992..1.020 | 1.05 | 10.1% | PASS |
| cheap_query.latency_us.p99 | 0.406 ms | 0.411 ms | 1.014 | 0.995..1.025 | 1.10 | 10.1% | PASS |
| cheap_query.queries_per_sec | 13206/s | 13176/s | 1.001 | 0.978..1.023 | 1.05 | 15.0% | PASS |
| delta_fanout.delta_us.p50 | 7.939 ms | 7.689 ms | 0.986 | 0.928..1.025 | 1.05 | 15.9% | PASS |
| delta_fanout.delta_us.p99 | 14.688 ms | 15.736 ms | 0.986 | 0.912..1.136 | 1.10 | 426.9% | INCONCLUSIVE |
| delta_fanout.last_agent_us.p99 | 14.877 ms | 16.023 ms | 0.988 | 0.916..1.147 | 1.10 | 422.8% | INCONCLUSIVE |
| delta_first.first_delta_us.p50 | 4.086 ms | 4.111 ms | 1.011 | 0.975..1.024 | 1.05 | 8.6% | PASS |
| delta_first.warm_delta_us.p50 | 4.934 ms | 4.949 ms | 0.999 | 0.991..1.018 | 1.05 | 6.7% | PASS |
| delta_single.delta_us.p50 | 7.139 ms | 7.048 ms | 1.004 | 0.936..1.037 | 1.05 | 15.3% | PASS |
| delta_single.delta_us.p99 | 14.242 ms | 13.406 ms | 0.996 | 0.917..1.045 | 1.10 | 36.0% | PASS |
| insert_batch.ack_us.p50 | 3.985 ms | 3.955 ms | 1.019 | 0.943..1.074 | 1.05 | 33.0% | INCONCLUSIVE |
| insert_batch.facts_per_sec | 175341/s | 171069/s | 1.027 | 0.933..1.092 | 1.05 | 31.7% | INCONCLUSIVE |
| insert_single.ack_us.p50 | 1.563 ms | 1.621 ms | 1.006 | 0.984..1.035 | 1.05 | 23.7% | PASS |
| insert_single.ack_us.p99 | 6.388 ms | 6.439 ms | 0.941 | 0.813..1.276 | 1.10 | 117.0% | INCONCLUSIVE |
| insert_single.concurrent_facts_per_sec | 605/s | 599/s | 1.017 | 0.969..1.074 | 1.05 | 18.2% | INCONCLUSIVE |
| insert_single.facts_per_sec | 551/s | 551/s | 1.010 | 0.959..1.059 | 1.05 | 22.3% | INCONCLUSIVE |
| interference.delta_us.p50 | 8.162 ms | 8.190 ms | 0.999 | 0.986..1.035 | 1.05 | 13.1% | PASS |
| interference.delta_us.p99 | 13.960 ms | 14.271 ms | 0.983 | 0.949..1.109 | 1.10 | 38.7% | INCONCLUSIVE |

### Diagnostic metrics (not gated)

| metric | baseline | candidate | cost ratio | interval | budget | baseline spread | status |
|---|---|---|---|---|---|---|---|
| bound_query.concurrent_latency_us.p50 | 8.438 ms | 8.472 ms | 1.002 | 0.995..1.014 | 1.05 | 5.2% | PASS |
| cheap_query.concurrent_latency_us.p50 | 0.581 ms | 0.580 ms | 0.998 | 0.983..1.030 | 1.05 | 11.7% | PASS |
| cheap_query.concurrent_latency_us.p99 | 0.934 ms | 0.946 ms | 1.002 | 0.989..1.089 | 1.10 | 76.6% | PASS |
| delta_fanout.ack_us.p50 | 5.740 ms | 5.452 ms | 0.988 | 0.900..1.041 | 1.05 | 22.2% | PASS |
| delta_fanout.ack_us.p99 | 11.236 ms | 10.639 ms | 0.903 | 0.709..0.993 | 1.10 | 63.5% | PASS |
| delta_fanout.last_agent_us.p50 | 8.066 ms | 7.779 ms | 0.986 | 0.927..1.030 | 1.05 | 15.5% | PASS |
| delta_fanout.subscribe_us.p50 | 1.325 ms | 1.327 ms | 1.002 | 0.992..1.009 | 1.05 | 2.3% | PASS |
| delta_fanout.subscribe_us.p99 | - | - | - | - | 1.10 | - | INVALID: baseline round 1: 64 samples, need 100 |
| delta_first.first_delta_us.p99 | - | - | - | - | 1.10 | - | INVALID: baseline round 1: 40 samples, need 100 |
| delta_first.subscribe_us.p50 | 3.624 ms | 3.593 ms | 0.991 | 0.982..1.009 | 1.05 | 8.4% | PASS |
| delta_first.subscribe_us.p99 | - | - | - | - | 1.10 | - | INVALID: baseline round 1: 40 samples, need 100 |
| delta_first.warm_delta_us.p99 | - | - | - | - | 1.10 | - | INVALID: baseline round 1: 40 samples, need 100 |
| delta_single.ack_us.p50 | 4.973 ms | 4.909 ms | 1.019 | 0.914..1.060 | 1.05 | 21.5% | INCONCLUSIVE |
| delta_single.ack_us.p99 | 12.104 ms | 11.325 ms | 0.996 | 0.893..1.053 | 1.10 | 43.4% | PASS |
| delta_single.last_agent_us.p50 | 7.139 ms | 7.048 ms | 1.004 | 0.936..1.037 | 1.05 | 15.3% | PASS |
| delta_single.last_agent_us.p99 | 14.242 ms | 13.406 ms | 0.996 | 0.917..1.045 | 1.10 | 36.0% | PASS |
| delta_single.subscribe_us.p50 | - | - | - | - | 1.05 | - | INVALID: baseline round 1: 1 samples, need 20 |
| delta_single.subscribe_us.p99 | - | - | - | - | 1.10 | - | INVALID: baseline round 1: 1 samples, need 100 |
| insert_batch.ack_us.p99 | - | - | - | - | 1.10 | - | INVALID: baseline round 1: 20 samples, need 100 |
| insert_single.concurrent_ack_us.p50 | 6.069 ms | 6.135 ms | 1.030 | 0.984..1.063 | 1.05 | 14.1% | INCONCLUSIVE |
| insert_single.concurrent_ack_us.p99 | 14.608 ms | 14.758 ms | 1.001 | 0.880..1.092 | 1.10 | 75.4% | PASS |
| interference.ack_us.p50 | 5.310 ms | 5.359 ms | 0.998 | 0.965..1.060 | 1.05 | 21.2% | INCONCLUSIVE |
| interference.ack_us.p99 | 11.296 ms | 11.630 ms | 0.972 | 0.935..1.154 | 1.10 | 104.5% | INCONCLUSIVE |
| interference.long_request_us.p50 | - | - | - | - | 1.05 | - | INVALID: baseline round 1: 19 samples, need 20 |
| interference.long_request_us.p99 | - | - | - | - | 1.10 | - | INVALID: baseline round 1: 19 samples, need 100 |
