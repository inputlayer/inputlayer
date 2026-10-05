<!--
A/A calibration of main 25058f16 on the benchmark host sam-dev-benchmarks,
2026-10-04 19:54-20:20 UTC, by scripts/perf-gate-remote.sh (30 rounds, the
CI job's retry count). nproc: 32 (16 cores x 2 SMT); gate clients on CPUs
0-7, servers on 8-31.
gate tool commit: e301b1e8d8e7a0d87730093208582f488e085082 (fm/bench-machine;
its gate fixtures are main's)
command: scripts/perf-gate.sh --aa --baseline-rev 25058f160c2360d7c126c1d530db235e165bf792 --rounds 30 --gate-cpus 0-7 --server-cpus 8-31
Raw samples: 25058f16-aa-sam-dev-benchmarks.run.json.gz.
-->
## Performance gate: INCONCLUSIVE

Policy **provisional**: cost budget p50 +5%, p99 +10%, throughput -5%; 95% interval of the median paired per-round ratio. Budgets are the plan's proposed ceilings, pending captain decision D1; tightening is always allowed.

- baseline: `25058f160c2360d7c126c1d530db235e165bf792` (binary sha256 cd5103a6c32a)
- candidate: `25058f160c2360d7c126c1d530db235e165bf792` (binary sha256 cd5103a6c32a)
- profile `standard`, 30 rounds, fixtures cheap_query, bound_query, insert_single, insert_batch, delta_single, delta_fanout, delta_first, interference
- host `computeinstance-e00nfsdgf9dzq987g3`: AMD EPYC-Genoa Processor (32 CPUs, 125 GB), kernel 6.11.0-1016-nvidia, governor unknown, data on ext4 (/home/sam/bench/inputlayer/target/perf-gate/runs/20261004T195451Z/servers), server CPUs 8-31
- load average: start `0.47 0.85 0.94 1/444 67524`, end `0.53 0.73 0.79 1/468 90641`
- build: rustc 1.99.0 (b940084d7 2026-09-28); cargo build --release --all-features

### Required metrics

| metric | baseline | candidate | cost ratio | interval | budget | baseline spread | status |
|---|---|---|---|---|---|---|---|
| bound_query.concurrent_latency_us.p99 | 12.463 ms | 12.544 ms | 1.008 | 0.980..1.032 | 1.10 | 11.7% | PASS |
| bound_query.latency_us.p50 | 8.361 ms | 8.359 ms | 0.999 | 0.996..1.001 | 1.05 | 7.4% | PASS |
| bound_query.latency_us.p99 | 8.636 ms | 8.640 ms | 1.002 | 0.998..1.007 | 1.10 | 36.0% | PASS |
| bound_query.queries_per_sec | 899/s | 901/s | 1.001 | 0.983..1.023 | 1.05 | 15.0% | PASS |
| cheap_query.latency_us.p50 | 0.366 ms | 0.367 ms | 1.006 | 1.000..1.019 | 1.05 | 13.4% | PASS |
| cheap_query.latency_us.p99 | 0.407 ms | 0.410 ms | 1.009 | 0.998..1.017 | 1.10 | 32.6% | PASS |
| cheap_query.queries_per_sec | 13263/s | 13178/s | 1.009 | 0.975..1.030 | 1.05 | 9.3% | PASS |
| delta_fanout.delta_us.p50 | 8.070 ms | 7.919 ms | 1.003 | 0.967..1.037 | 1.05 | 15.0% | PASS |
| delta_fanout.delta_us.p99 | 15.760 ms | 14.871 ms | 0.972 | 0.893..1.087 | 1.10 | 63.3% | PASS |
| delta_fanout.last_agent_us.p99 | 15.934 ms | 15.006 ms | 0.971 | 0.893..1.081 | 1.10 | 62.6% | PASS |
| delta_first.first_delta_us.p50 | 4.188 ms | 4.322 ms | 1.001 | 0.978..1.024 | 1.05 | 19.2% | PASS |
| delta_first.warm_delta_us.p50 | 4.999 ms | 5.066 ms | 1.006 | 0.977..1.019 | 1.05 | 15.2% | PASS |
| delta_single.delta_us.p50 | 7.271 ms | 7.208 ms | 0.992 | 0.953..1.037 | 1.05 | 17.7% | PASS |
| delta_single.delta_us.p99 | 13.581 ms | 13.405 ms | 0.989 | 0.912..1.110 | 1.10 | 57.7% | INCONCLUSIVE |
| insert_batch.ack_us.p50 | 3.887 ms | 3.857 ms | 0.979 | 0.957..1.036 | 1.05 | 28.2% | PASS |
| insert_batch.facts_per_sec | 181209/s | 176591/s | 1.018 | 0.974..1.037 | 1.05 | 33.9% | PASS |
| insert_single.ack_us.p50 | 1.563 ms | 1.568 ms | 1.000 | 0.972..1.040 | 1.05 | 20.7% | PASS |
| insert_single.ack_us.p99 | 7.204 ms | 6.423 ms | 0.966 | 0.843..1.153 | 1.10 | 70.5% | INCONCLUSIVE |
| insert_single.concurrent_facts_per_sec | 612/s | 589/s | 1.038 | 0.975..1.072 | 1.05 | 25.9% | INCONCLUSIVE |
| insert_single.facts_per_sec | 561/s | 557/s | 1.015 | 0.972..1.060 | 1.05 | 23.7% | INCONCLUSIVE |
| interference.delta_us.p50 | 7.902 ms | 7.932 ms | 1.004 | 0.994..1.038 | 1.05 | 13.8% | PASS |
| interference.delta_us.p99 | 13.525 ms | 13.417 ms | 0.983 | 0.891..1.055 | 1.10 | 26.6% | PASS |

### Diagnostic metrics (not gated)

| metric | baseline | candidate | cost ratio | interval | budget | baseline spread | status |
|---|---|---|---|---|---|---|---|
| bound_query.concurrent_latency_us.p50 | 8.342 ms | 8.322 ms | 0.998 | 0.993..1.011 | 1.05 | 6.3% | PASS |
| cheap_query.concurrent_latency_us.p50 | 0.572 ms | 0.579 ms | 1.011 | 0.981..1.033 | 1.05 | 10.5% | PASS |
| cheap_query.concurrent_latency_us.p99 | 0.932 ms | 0.926 ms | 1.010 | 0.963..1.049 | 1.10 | 16.6% | PASS |
| delta_fanout.ack_us.p50 | 5.636 ms | 5.665 ms | 0.995 | 0.960..1.048 | 1.05 | 18.0% | PASS |
| delta_fanout.ack_us.p99 | 10.945 ms | 10.249 ms | 0.944 | 0.783..1.021 | 1.10 | 68.2% | PASS |
| delta_fanout.last_agent_us.p50 | 8.168 ms | 8.027 ms | 1.007 | 0.973..1.036 | 1.05 | 15.4% | PASS |
| delta_fanout.subscribe_us.p50 | 1.316 ms | 1.317 ms | 0.999 | 0.995..1.007 | 1.05 | 4.9% | PASS |
| delta_fanout.subscribe_us.p99 | - | - | - | - | 1.10 | - | INVALID: baseline round 1: 64 samples, need 100 |
| delta_first.first_delta_us.p99 | - | - | - | - | 1.10 | - | INVALID: baseline round 1: 40 samples, need 100 |
| delta_first.subscribe_us.p50 | 3.654 ms | 3.733 ms | 1.008 | 0.983..1.037 | 1.05 | 12.6% | PASS |
| delta_first.subscribe_us.p99 | - | - | - | - | 1.10 | - | INVALID: baseline round 1: 40 samples, need 100 |
| delta_first.warm_delta_us.p99 | - | - | - | - | 1.10 | - | INVALID: baseline round 1: 40 samples, need 100 |
| delta_single.ack_us.p50 | 4.936 ms | 4.855 ms | 0.982 | 0.961..1.028 | 1.05 | 27.6% | PASS |
| delta_single.ack_us.p99 | 11.485 ms | 11.123 ms | 0.979 | 0.889..1.097 | 1.10 | 68.2% | PASS |
| delta_single.last_agent_us.p50 | 7.271 ms | 7.208 ms | 0.992 | 0.953..1.037 | 1.05 | 17.7% | PASS |
| delta_single.last_agent_us.p99 | 13.581 ms | 13.405 ms | 0.989 | 0.912..1.110 | 1.10 | 57.7% | INCONCLUSIVE |
| delta_single.subscribe_us.p50 | - | - | - | - | 1.05 | - | INVALID: baseline round 1: 1 samples, need 20 |
| delta_single.subscribe_us.p99 | - | - | - | - | 1.10 | - | INVALID: baseline round 1: 1 samples, need 100 |
| insert_batch.ack_us.p99 | - | - | - | - | 1.10 | - | INVALID: baseline round 1: 20 samples, need 100 |
| insert_single.concurrent_ack_us.p50 | 6.025 ms | 6.196 ms | 1.031 | 0.988..1.078 | 1.05 | 27.5% | INCONCLUSIVE |
| insert_single.concurrent_ack_us.p99 | 14.252 ms | 14.989 ms | 1.042 | 1.008..1.089 | 1.10 | 65.1% | PASS |
| interference.ack_us.p50 | 5.088 ms | 5.133 ms | 1.017 | 0.991..1.058 | 1.05 | 21.9% | INCONCLUSIVE |
| interference.ack_us.p99 | 11.144 ms | 11.015 ms | 0.973 | 0.894..1.055 | 1.10 | 30.8% | PASS |
| interference.long_request_us.p50 | - | - | - | - | 1.05 | - | INVALID: baseline round 1: 19 samples, need 20 |
| interference.long_request_us.p99 | - | - | - | - | 1.10 | - | INVALID: baseline round 1: 19 samples, need 100 |
