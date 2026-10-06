#!/usr/bin/env bash
# Views benchmark: what writes and reads cost against deployed rules as the
# graph, the rule catalog and the subscribers grow (see perf-gate/README.md).
#
# Usage: scripts/bench-views.sh [options]
#   --baseline-rev REV          also measure REV (built from git archive, cached
#                               with the perf gate's baselines) on the same sizes
#   --edges LIST                graph sizes in edges (default 10000,100000,1000000)
#   --writes N                  writes per phase (default 30)
#   --large-writes N            writes per subscriber phase at 1M edges or more
#                               (default 20)
#   --reads N                   repetitions of each idle read (default 10)
#   --keyed LIST                keyed subscribers on the non-recursive view
#                               (default 1,10,100,1000)
#   --recursive-keyed LIST      keyed subscribers on the recursive view
#                               (default 1,100)
#   --large-recursive-keyed LIST  the same at 1M edges or more (default 1,10)
#   --filler-rules LIST         unrelated rules added to the catalog (default 50,200)
#   --server-env K=V            extra server environment (repeatable)
#   --server-cpus LIST          pin servers with taskset -c LIST
#
# Exit status: 0 when every write's delta arrived and every step succeeded,
# 1 otherwise, 3 on a setup error. Latency is reported, not judged.
# result.json (every raw summary) and summary.md land in
# target/bench-views/runs/<utc-time>/; target/bench-views/latest points there.
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"
OUT=$ROOT/target/bench-views

BASELINE_REV=""
ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --baseline-rev) BASELINE_REV=$2; shift 2 ;;
        --edges|--writes|--large-writes|--reads|--keyed|--recursive-keyed|--large-recursive-keyed|--filler-rules|--server-env|--server-cpus)
            ARGS+=("$1" "$2"); shift 2 ;;
        -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
        *) echo "bench-views: unknown option $1" >&2; exit 3 ;;
    esac
done

echo "=== Build perf-gate and server (release, all features) ==="
cargo build --release --quiet --manifest-path perf-gate/Cargo.toml --target-dir target
cargo build --release --all-features --bin inputlayer-server
LABEL=$(git rev-parse HEAD)
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
    LABEL="$LABEL+dirty"
fi
RUN_DIR=$OUT/runs/$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$RUN_DIR"
# A private copy: a rebuild during the run must not swap the binary.
cp target/release/inputlayer-server "$RUN_DIR/inputlayer-server"

if [ -n "$BASELINE_REV" ]; then
    BASELINE_SHA=$(git rev-parse --verify "${BASELINE_REV}^{commit}")
    BASELINE_BIN=$(scripts/perf-gate-baseline.sh "$BASELINE_SHA")
    ARGS+=(--compare "$BASELINE_BIN" --compare-label "$BASELINE_SHA")
fi

echo "=== Benchmark ($LABEL) ==="
STATUS=0
target/release/perf-gate views \
    --server "$RUN_DIR/inputlayer-server" --server-label "$LABEL" \
    --build "$(rustc -V); cargo build --release --all-features" \
    --data-root "$RUN_DIR/servers" \
    --out "$RUN_DIR/result.json" --summary "$RUN_DIR/summary.md" \
    "${ARGS[@]}" || STATUS=$?
rm -f "$RUN_DIR/inputlayer-server"
ln -sfn "$RUN_DIR" "$OUT/latest"
echo ""
echo "Summary: $RUN_DIR/summary.md"
exit "$STATUS"
