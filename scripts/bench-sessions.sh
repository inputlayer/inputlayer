#!/usr/bin/env bash
# Session-scale benchmark: standing-query cost as sessions on one knowledge
# graph grow, on the voice-agent reference pack (see perf-gate/README.md).
#
# Usage: scripts/bench-sessions.sh [options]
#   --baseline-rev REV    also measure REV (built from git archive, cached
#                         with the perf gate's baselines) on the same sizes
#   --sessions LIST       session counts (default 1,10,25,50,100,200)
#   --load-secs N         seconds of probing under load (default 15)
#   --rate R              receipts per session per second (default 1)
#   --mode MODE           per-session (default) or shared-view
#   --server-env K=V      extra server environment (repeatable)
#   --server-cpus LIST    pin servers with taskset -c LIST
#   --max-delta-p99-ms X  fail when the working tree's loaded write->delta
#                         p99 exceeds X ms, or any delta is late, at any size
#                         up to --budget-sessions
#   --budget-sessions N   largest size the budget applies to
#
# Exit status: 0 when every probe got its delta and none was stray (and the
# budget, if any, held), 1 otherwise, 3 on a setup error. result.json (all
# raw summaries) and summary.md land in target/bench-sessions/runs/<utc-time>/;
# target/bench-sessions/latest points there.
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"
OUT=$ROOT/target/bench-sessions

BASELINE_REV=""
ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --baseline-rev) BASELINE_REV=$2; shift 2 ;;
        --sessions|--load-secs|--rate|--mode|--server-env|--server-cpus|--max-delta-p99-ms|--budget-sessions)
            ARGS+=("$1" "$2"); shift 2 ;;
        -h|--help) sed -n '2,22p' "$0"; exit 0 ;;
        *) echo "bench-sessions: unknown option $1" >&2; exit 3 ;;
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
    TOOLCHAIN_KEY=$(rustc -V | sha256sum | cut -c1-12)
    GATE_OUT=$ROOT/target/perf-gate
    BASELINE_BIN=$GATE_OUT/servers/$BASELINE_SHA-$TOOLCHAIN_KEY/inputlayer-server
    if [ ! -x "$BASELINE_BIN" ]; then
        echo "=== Build baseline server $BASELINE_SHA ==="
        SRC=$GATE_OUT/src/$BASELINE_SHA
        rm -rf "$SRC" && mkdir -p "$SRC"
        git archive --format=tar "$BASELINE_SHA" | tar -x -C "$SRC"
        if [ -f Cargo.lock ]; then cp Cargo.lock "$SRC/"; fi
        CARGO_TARGET_DIR=$GATE_OUT/build cargo build --release --all-features \
            --manifest-path "$SRC/Cargo.toml" --bin inputlayer-server
        mkdir -p "$(dirname "$BASELINE_BIN")"
        cp "$GATE_OUT/build/release/inputlayer-server" "$BASELINE_BIN"
    fi
    ARGS+=(--compare "$BASELINE_BIN" --compare-label "$BASELINE_SHA")
fi

echo "=== Benchmark ($LABEL) ==="
STATUS=0
target/release/perf-gate sessions \
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
