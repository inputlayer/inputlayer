# Fuzz targets

[cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) targets for the client
input the server parses (#301). Each target runs the server's own code
through the engine's `test-support` seam (`src/fuzzing.rs`), on a thread with
the server's engine stack (`ENGINE_THREAD_STACK_BYTES`), at the highest
nesting limit an operator may configure (`MAX_NESTING_DEPTH_CEILING`).

| target | input | checks beyond "no panic, no overflow, no abort" |
|---|---|---|
| `iql_statement` | one IQL statement (`parse_statement`) | a persistent rule passes `validate_rule` and reloads unchanged from the catalog's JSON (`nested_json`) |
| `iql_program` | an `execute` program, optionally `\0` + its `params` JSON | statements split, parsed, bound and authorized for every role as the handler does; a program the connection runs read-only holds only queries; the rule-program parser |
| `ws_client_frame` | one `/ws` text frame | the pre-authentication decode and `id` probe agree; a decoded frame re-encodes to itself; replies echo the frame's `id`; an immediate reply encodes; a request run read-only holds only queries |

## Seeds

`python3 fuzz/seeds.py` writes `fuzz/seeds/<target>/` (git-ignored):

- every example program under `examples/` (up to 32 KiB) and each of its
  statements, and each program as an `execute` frame;
- one frame of every `/ws` client type, with `params` and `expect_*`;
- the #295 cases: nested calls, groups, operator chains, `list[...]` and
  records in every statement form at depths 1, 127-129, 1023-1025 and 4,000,
  and bodies of 4,095-4,097 elements (self-joins, distinct atoms, negations,
  comparisons).

`iql.dict` and `ws_frame.dict` are libFuzzer dictionaries of IQL tokens and
frame JSON.

## Running

Install once with `cargo install cargo-fuzz`. Then, from the repo root:

```sh
# Every target for 10 minutes, 4 workers each (FUZZ_FORKS).
fuzz/run.sh 600

# One target.
fuzz/run.sh 3600 iql_program
```

`run.sh` regenerates the seeds, builds with AddressSanitizer and debug
assertions, and runs the targets in parallel. Workers continue past a crash,
timeout (10 s per input) or OOM (2 GiB), so a campaign collects every
finding in `fuzz/artifacts/<target>/`, listed at the end of the run. The
corpus grows in `fuzz/corpus/<target>/` and makes the next campaign start
where this one stopped. On the shared hosts, run it under a memory cap
(`systemd-run --user --scope -p MemoryMax=16G ...`) and, on the benchmark
host, under its lock (`flock ~/perf-gate-remote/lock fuzz/run.sh ...`).

Reproduce or minimize a finding:

```sh
cd fuzz
cargo fuzz run -O iql_program artifacts/iql_program/crash-<hash>
cargo fuzz tmin -O iql_program artifacts/iql_program/crash-<hash>
```

cargo-fuzz wants a nightly toolchain. `run.sh` uses one when it is
installed and otherwise sets `RUSTC_BOOTSTRAP=1` on the stable toolchain;
do the same for the commands above.
