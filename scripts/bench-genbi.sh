#!/usr/bin/env bash
# GenBI-trust reactive agent benchmark: the working tree's server as the
# substrate for agents that subscribe to business questions over /ws.
#
# Usage: GENBI_TRUST_DIR=/path/to/genbi-trust scripts/bench-genbi.sh [options]
#   --cases SPEC          all (default), priority, representative, or ids/categories
#   --repeat N            fresh-server runs per scenario (default 1)
#   --agents N            subscribed agent connections per scenario (default 1)
#   --fault KIND          break the agents' delta path on purpose:
#                         drop-retractions | drop-inserts (QC of the checks)
#   --server-cpus LIST    pin servers with taskset -c LIST
#   --data-root DIR       server data directories (default: under the run dir)
#   --strict              fail on any check that does not pass
#
# The suite is read in place from GENBI_TRUST_DIR and never copied.
# Exit status: 0 when every agent's answers matched the engine (with
# --strict: every check passed), 1 otherwise, 3 on a setup error.
# result.json (all raw samples) and summary.md land in
# target/genbi-bench/runs/<utc-time>/; target/genbi-bench/latest points there.
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"
OUT=$ROOT/target/genbi-bench

if [ -z "${GENBI_TRUST_DIR:-}" ] || [ ! -f "$GENBI_TRUST_DIR/generated/scenario-suite.json" ]; then
    echo "bench-genbi: set GENBI_TRUST_DIR to a genbi-trust checkout" >&2
    exit 3
fi

RUN_DIR=$OUT/runs/$(date -u +%Y%m%dT%H%M%SZ)
DATA_ROOT=$RUN_DIR/servers
ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --cases|--repeat|--agents|--fault|--server-cpus) ARGS+=("$1" "$2"); shift 2 ;;
        --data-root) DATA_ROOT=$2; shift 2 ;;
        --strict) ARGS+=("$1"); shift ;;
        -h|--help) sed -n '2,19p' "$0"; exit 0 ;;
        *) echo "bench-genbi: unknown option $1" >&2; exit 3 ;;
    esac
done

echo "=== Build perf-gate and server (release, all features) ==="
cargo build --release --quiet --manifest-path perf-gate/Cargo.toml --target-dir target
cargo build --release --all-features --bin inputlayer-server
LABEL=$(git rev-parse HEAD)
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
    LABEL="$LABEL+dirty"
fi
mkdir -p "$RUN_DIR"
# A private copy: a rebuild during the run must not swap the binary.
cp target/release/inputlayer-server "$RUN_DIR/inputlayer-server"

echo "=== Benchmark ($LABEL) ==="
STATUS=0
target/release/perf-gate genbi \
    --suite "$GENBI_TRUST_DIR" \
    --server "$RUN_DIR/inputlayer-server" --server-label "$LABEL" \
    --build "$(rustc -V); cargo build --release --all-features" \
    --data-root "$DATA_ROOT" \
    --out "$RUN_DIR/result.json" --summary "$RUN_DIR/summary.md" \
    "${ARGS[@]}" || STATUS=$?
rm -f "$RUN_DIR/inputlayer-server"
ln -sfn "$RUN_DIR" "$OUT/latest"
echo ""
echo "Summary: $RUN_DIR/summary.md"
exit "$STATUS"
