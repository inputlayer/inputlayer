#!/usr/bin/env bash
# Concurrency soak: concurrent writers, rule churn and many subscribers (fast,
# slow, stalled, grouped, short-lived) against one engine, every observation
# checked against the differential oracle's reference at its revision (see
# tests/differential_oracle/soak/mod.rs).
#
# Usage: scripts/soak.sh [options]
#   --profile NAME     smoke (the test suite's few seconds) or sustained
#                      (default: 30 minutes at benchmark-host scale)
#   --secs N           seconds the writers run (overrides the profile)
#   --set KEY=VALUE    any INPUTLAYER_SOAK_<KEY> (repeatable), e.g. --set FAST=500
#   --server-cpus LIST pin the engine with taskset -c LIST
#   --client-cpus LIST pin the soak's clients and verifier with taskset -c LIST
#
# Exit status: 0 when the soak passed, 1 when it failed, 3 on a setup error.
# result.json and summary.md land in target/soak/runs/<utc-time>/;
# target/soak/latest points there. The sustained run belongs on the
# benchmark host: make soak-remote.
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

PROFILE=sustained
SECS=""
SERVER_CPUS=""
CLIENT_CPUS=""
SETS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --profile) PROFILE=$2; shift 2 ;;
        --secs) SECS=$2; shift 2 ;;
        --set) SETS+=("$2"); shift 2 ;;
        --server-cpus) SERVER_CPUS=$2; shift 2 ;;
        --client-cpus) CLIENT_CPUS=$2; shift 2 ;;
        -h|--help) sed -n '2,19p' "$0"; exit 0 ;;
        *) echo "soak: unknown option $1" >&2; exit 3 ;;
    esac
done

# Profile settings first, so --secs and --set override them.
ENVS=()
case "$PROFILE" in
    smoke) ;;
    sustained)
        # Engine defaults throughout (send timeout 30 s, notification buffer
        # 4096): stalls outlast the send timeout plus the time to fill the
        # socket buffers, so stalled consumers are disconnected and return.
        ENVS+=(
            INPUTLAYER_SOAK_SECS=1800
            INPUTLAYER_SOAK_NODES=16
            INPUTLAYER_SOAK_WRITERS=16
            INPUTLAYER_SOAK_WRITE_RATE=20
            INPUTLAYER_SOAK_CHURN_MS=1000
            INPUTLAYER_SOAK_FAST=300
            INPUTLAYER_SOAK_SLOW=40
            INPUTLAYER_SOAK_STALLED=20
            INPUTLAYER_SOAK_GROUPS=20
            INPUTLAYER_SOAK_CHURNERS=20
            INPUTLAYER_SOAK_AUDITORS=4
            INPUTLAYER_SOAK_SLOW_PAUSE_MS=10
            INPUTLAYER_SOAK_STALL_MS=120000
            INPUTLAYER_SOAK_AUDIT_MS=100
            INPUTLAYER_SOAK_QUIET_MS=3000
            INPUTLAYER_SOAK_SETTLE_SECS=600
            INPUTLAYER_SOAK_MAX_RSS_GROWTH_PCT=25
        ) ;;
    *) echo "soak: unknown profile $PROFILE (smoke or sustained)" >&2; exit 3 ;;
esac
if [ -n "$SECS" ]; then ENVS+=(INPUTLAYER_SOAK_SECS="$SECS"); fi
for set in "${SETS[@]}"; do ENVS+=(INPUTLAYER_SOAK_"$set"); done
if [ -n "$SERVER_CPUS" ]; then ENVS+=(INPUTLAYER_SOAK_SERVER_CPUS="$SERVER_CPUS"); fi

echo "=== Build the oracle and server (release, all features) ==="
BUILD_LOG=$(mktemp)
if ! cargo test --release --all-features --test differential_oracle --no-run \
    --message-format=json > "$BUILD_LOG"; then
    echo "soak: build failed" >&2
    exit 3
fi
# The test binary cargo just built (its hash is not predictable).
BIN=$(sed -n 's/.*"executable":"\([^"]*differential_oracle[^"]*\)".*/\1/p' "$BUILD_LOG" | tail -1)
rm -f "$BUILD_LOG"
if [ -z "$BIN" ] || [ ! -x "$BIN" ]; then
    echo "soak: no differential_oracle test binary found" >&2
    exit 3
fi

LABEL=$(git rev-parse HEAD)
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then LABEL="$LABEL+dirty"; fi
OUT=$ROOT/target/soak
RUN_DIR=$OUT/runs/$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$RUN_DIR"
{
    echo "commit: $LABEL"
    echo "host: $(hostname), nproc $(nproc)"
    echo "profile: $PROFILE"
    echo "server cpus: ${SERVER_CPUS:-unpinned}, client cpus: ${CLIENT_CPUS:-unpinned}"
    printf '%s\n' "${ENVS[@]}"
} > "$RUN_DIR/settings.txt"
cat "$RUN_DIR/settings.txt"

PIN=()
if [ -n "$CLIENT_CPUS" ]; then PIN=(taskset -c "$CLIENT_CPUS"); fi
# Every consumer is a socket on both sides; the engine inherits this limit.
ulimit -n "$(ulimit -Hn)" 2> /dev/null || true
echo "=== Soak ($LABEL) ==="
STATUS=0
env "${ENVS[@]}" INPUTLAYER_SOAK_REPORT_DIR="$RUN_DIR" \
    "${PIN[@]}" "$BIN" concurrent_soak_agrees_with_reference --exact --nocapture --test-threads 1 \
    || STATUS=$?
ln -sfn "$RUN_DIR" "$OUT/latest"
echo ""
echo "Results: $RUN_DIR (exit status $STATUS)"
if [ "$STATUS" -ne 0 ]; then exit 1; fi
