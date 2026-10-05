# Session scale, 2026-10-05

How standing-query cost grows with the sessions on one knowledge graph, on the
dedicated benchmark host, at main `8b574ef2d6fd97c6a781a88fe315dfbb79403418`
(with #254's shared evaluations). The workload is the voice-agent reference
pack, measured by `make bench-sessions` (#254's harness; see
[`../README.md`](../README.md#session-scale-benchmark)). Each session commits
receipts at the given rate on its own connection, which every speech query
depends on but no result changes. A probe replaces a random session's ETA,
and the harness times that session's delta. Engine only: no model or
decision pipeline in the path.

Host: `sam-dev-benchmarks` (AMD EPYC-Genoa, `nproc` 32, 125 GB), server
pinned to CPUs 8-31. Every run was started with `scripts/perf-gate-remote.sh
--bench sessions` in the host's clone at the commit shown, with the harness's
fixed seeds: probe RNG `0x2545F4914F6CDD1D`, session `i` RNG
`0x9E3779B97F4A7C15 ^ (i * 0xBF58476D1CE4E5B9)`.

## 1 write per session per second

`scripts/bench-sessions.sh --sessions 100,250,500,1000 --rate 1 --server-cpus 8-31`
at `8b574ef2`, 2026-10-05 06:19-06:21 UTC. Load average 5.1 at the start:
another lane's builds were running, so read the tails with care.

| Sessions | Writes/s achieved | Server CPU (cores) | Peak RSS | Write to delta, no load, p50 / p99 | Write to delta under load, p50 / p99 | Missing / stray (v1) |
|---|---|---|---|---|---|---|
| 100 | 98.7 | 1.81 | 193 MB | 25.4 / 85.3 ms | 39.9 / 77.7 ms | 0 / 0 |
| 250 | 245.5 | 7.75 | 323 MB | 38.9 / 397.2 ms | 129.1 / 376.6 ms | 0 / 0 |
| 500 | 250.1 | 16.47 | 452 MB | 64.8 / 1,300 ms | 2,857 / 3,426 ms | 0 / 0 |
| 1,000 | 192.5 | 19.05 | 613 MB | 117.3 / 3,673 ms | no probe within 10 s | 2 / 1 |

The graph saturates between 250 and 500 sessions at about 250 writes/s.
Above that, achieved writes fall below the offered rate and every
write-to-delta latency grows into seconds.

## 0.1 writes per session per second

`scripts/bench-sessions.sh --sessions 100,250,500,1000 --rate 0.1 --server-cpus 8-31`
at `8b574ef2`, 2026-10-05 06:48-06:49 UTC, load average 1.0 at the start.

| Sessions | Writes/s achieved | Server CPU (cores) | Peak RSS | Write to delta, no load, p50 / p99 | Write to delta under load, p50 / p99 | Missing / stray (v1) |
|---|---|---|---|---|---|---|
| 100 | 10.0 | 0.51 | 192 MB | 25.1 / 74.1 ms | 26.6 / 50.7 ms | 0 / 0 |
| 250 | 25.1 | 1.37 | 304 MB | 38.6 / 256.3 ms | 44.2 / 71.9 ms | 0 / 0 |
| 500 | 48.9 | 2.95 | 367 MB | 64.5 / 1,482 ms | 93.8 / 180.8 ms | 0 / 0 |
| 1,000 | 99.8 | 10.03 | 476 MB | 114.5 / 4,577 ms | 266.1 / 509.3 ms | 0 / 0 |

## Missing and stray deltas at 1,000 sessions: late, not lost

The first harness (result schema `sessions/v1`) counted a probe as missing
when its delta took more than 10 s. It counted any later push as stray,
including that same delta arriving late. At 1,000 sessions and 1 write/s
the server is saturated, so "2 missing / 1 stray" could mean lost deltas or
slow ones. The harness now tells them apart (`sessions/v2`). A delta that
arrives after the probe timeout counts as late, and overdue probes get 60 s
more once the load stops. Only a delta that never arrives is missing. Same
server (`8b574ef2` plus the harness change, `512195cb`):

| Run | Writes/s | Server CPU | Peak RSS | Under-load probes on time / late | Late delta | Missing / stray |
|---|---|---|---|---|---|---|
| `--sessions 1000 --rate 1`, 06:53 UTC | 205.6 | 18.61 | 616 MB | 1 / 1 | 10.2 s | 0 / 0 |
| `--sessions 1000 --rate 1 --load-secs 60`, 06:54 UTC (load average 8.8 at the start, mostly the run before it) | 162.9 | 19.60 | 688 MB | 0 / 6 | p50 14.6 s, max 18.0 s | 0 / 0 |

Every delta arrived, and none arrived that no probe caused. The v1 failure
was the probe timeout on a saturated server, not a lost or spurious delta.
`delta_single`, `delta_fanout` and the gate's other delta fixtures check
the content of every delta exactly, on every run.
