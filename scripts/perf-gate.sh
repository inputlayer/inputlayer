#!/usr/bin/env bash
# Performance gate: measure the working tree's server against the approved
# baseline on this host and judge it under perf-gate/policy.toml.
#
# Usage: scripts/perf-gate.sh [options]
#   --baseline-rev REV    baseline commit (default: perf-gate/baselines/approved.toml)
#   --candidate-bin PATH  candidate server binary (default: build the working tree)
#   --aa                  measure the baseline against itself (noise calibration)
#   --rounds N            rounds per arm (default 10)
#   --profile NAME        standard (default) or quick (smoke only, never passes)
#   --fixtures LIST       comma-separated fixture subset (default: all)
#   --server-cpus LIST    pin servers with taskset -c LIST
#   --gate-cpus LIST      pin the gate's own clients with taskset -c LIST
#   --no-verdict          only measure and summarize (the engine suite)
#
# Exit status: 0 pass, 1 fail, 2 inconclusive, 3 invalid (or a setup error).
# With --no-verdict: 0, or 3 when a fixture run failed.
# The run file (all raw samples), report.md, verdict.json and summary.md
# (absolute numbers) land in target/perf-gate/runs/<utc-time>/;
# target/perf-gate/latest points there.
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"
OUT=$ROOT/target/perf-gate

BASELINE_REV=""
CANDIDATE_BIN=""
AA=0
NO_VERDICT=0
RUN_ARGS=()
GATE_PIN=()
while [ $# -gt 0 ]; do
    case "$1" in
        --baseline-rev) BASELINE_REV=$2; shift 2 ;;
        --candidate-bin) CANDIDATE_BIN=$2; shift 2 ;;
        --aa) AA=1; shift ;;
        --rounds|--profile|--fixtures|--server-cpus) RUN_ARGS+=("$1" "$2"); shift 2 ;;
        --gate-cpus) GATE_PIN=(taskset -c "$2"); shift 2 ;;
        --no-verdict) NO_VERDICT=1; shift ;;
        -h|--help) sed -n '2,22p' "$0"; exit 0 ;;
        *) echo "perf-gate: unknown option $1" >&2; exit 3 ;;
    esac
done

if [ -z "$BASELINE_REV" ]; then
    BASELINE_REV=$(sed -n 's/^commit *= *"\([0-9a-f]*\)".*/\1/p' perf-gate/baselines/approved.toml)
fi
BASELINE_SHA=$(git rev-parse --verify "${BASELINE_REV}^{commit}")
RUSTC=$(rustc -V)

echo "=== Build perf-gate ==="
cargo build --release --quiet --manifest-path perf-gate/Cargo.toml --target-dir target
GATE=$ROOT/target/release/perf-gate

# Server binaries are built exactly like the release image: release profile,
# all features. Baselines are cached per commit and toolchain, each built
# from its own sources alone (scripts/perf-gate-baseline.sh).
BASELINE_BIN=$(scripts/perf-gate-baseline.sh "$BASELINE_SHA")

if [ "$AA" = 1 ]; then
    CANDIDATE_BIN=$BASELINE_BIN
    CANDIDATE_LABEL=$BASELINE_SHA
elif [ -n "$CANDIDATE_BIN" ]; then
    CANDIDATE_LABEL="binary $(realpath "$CANDIDATE_BIN")"
else
    echo "=== Build candidate server (working tree) ==="
    cargo build --release --all-features --bin inputlayer-server
    CANDIDATE_LABEL=$(git rev-parse HEAD)
    if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
        CANDIDATE_LABEL="$CANDIDATE_LABEL+dirty"
    fi
    # A private copy: a rebuild during the run must not swap the binary.
    CANDIDATE_BIN=$OUT/servers/candidate/inputlayer-server
    mkdir -p "$(dirname "$CANDIDATE_BIN")"
    cp target/release/inputlayer-server "$CANDIDATE_BIN"
fi

RUN_DIR=$OUT/runs/$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$RUN_DIR"
echo "=== Measure (baseline $BASELINE_SHA vs candidate $CANDIDATE_LABEL) ==="
"${GATE_PIN[@]}" "$GATE" run \
    --baseline "$BASELINE_BIN" --baseline-label "$BASELINE_SHA" \
    --candidate "$CANDIDATE_BIN" --candidate-label "$CANDIDATE_LABEL" \
    --data-root "$RUN_DIR/servers" \
    --build "$RUSTC; cargo build --release --all-features" \
    --out "$RUN_DIR/run.json" \
    "${RUN_ARGS[@]}"
ln -sfn "$RUN_DIR" "$OUT/latest"

STATUS=0
"$GATE" summary "$RUN_DIR/run.json" > "$RUN_DIR/summary.md" || STATUS=$?
if [ "$NO_VERDICT" = 1 ]; then
    cat "$RUN_DIR/summary.md"
    echo ""
    echo "Summary: $RUN_DIR/summary.md"
    exit "$STATUS"
fi

echo "=== Judge ==="
STATUS=0
"$GATE" compare "$RUN_DIR/run.json" --policy perf-gate/policy.toml \
    --report "$RUN_DIR/report.md" --verdict "$RUN_DIR/verdict.json" || STATUS=$?
echo ""
echo "Report: $RUN_DIR/report.md (attach to the PR record)"
exit "$STATUS"
