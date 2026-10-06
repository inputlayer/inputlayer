#!/usr/bin/env bash
# The nightly's runs on the benchmark host (nightly.yml), each through
# scripts/perf-gate-remote.sh and so behind that host's one-benchmark lock:
#   1. the concurrency soak at nightly size: 10 minutes, 200 consumers (fast,
#      slow, stalled, grouped, short-lived), every observation checked
#      against the oracle's reference (scripts/soak.sh)
#   2. the performance gate: the approved baseline against the commit. An
#      INCONCLUSIVE first attempt is retried once with more rounds.
# Both always run, so one failing does not hide the other's result.
#
# Usage: scripts/nightly-bench.sh [options]
#   --rev REV       commit to run (default HEAD)
#   --wait-lock S   seconds each run waits for the host's lock (default 21600)
#   PERF_GATE_RETRY_ROUNDS  rounds of the gate's retry (default 30)
#
# With GITHUB_STEP_SUMMARY set, the soak's summary and each gate attempt's
# report are appended to the job summary. The files stay in
# target/perf-gate/remote/<stamp>/.
#
# Exit status: 0 when the soak passed and the gate passed or stayed
# INCONCLUSIVE after its retry (a warning, not a PASS); 1 otherwise,
# including a run the host was too busy to start.
set -uo pipefail

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT" || exit 3

REV=HEAD
WAIT_LOCK=21600
while [ $# -gt 0 ]; do
    case "$1" in
        --rev) REV=$2; shift 2 ;;
        --wait-lock) WAIT_LOCK=$2; shift 2 ;;
        -h|--help) sed -n '2,22p' "$0"; exit 0 ;;
        *) echo "nightly-bench: unknown option $1" >&2; exit 3 ;;
    esac
done
RETRY_ROUNDS=${PERF_GATE_RETRY_ROUNDS:-30}
SUMMARY=${GITHUB_STEP_SUMMARY:-/dev/null}
LATEST=target/perf-gate/remote/latest
# 200 consumers: the sustained profile's mix at half its size.
SOAK_ARGS=(--secs 600 --set FAST=150 --set SLOW=20 --set STALLED=10 --set GROUPS=10 --set CHURNERS=10)

status_name() {
    case "$1" in
        0) echo PASS ;; 1) echo FAIL ;; 2) echo INCONCLUSIVE ;; 3) echo INVALID ;;
        4) echo "NOT RUN (benchmark host busy for ${WAIT_LOCK} s)" ;;
        *) echo "ERROR (exit $1)" ;;
    esac
}

# run TITLE FILE ARGS...: one perf-gate-remote.sh run, then TITLE, its verdict
# and the run's FILE in the summary. Returns the run's status.
run() {
    local title=$1 file=$2 status=0 before
    shift 2
    before=$(readlink "$LATEST" 2> /dev/null || true)
    echo "=== $title ==="
    scripts/perf-gate-remote.sh --rev "$REV" --wait-lock "$WAIT_LOCK" "$@" || status=$?
    {
        echo "## $title: $(status_name "$status")"
        echo
        # A run that never started leaves latest on an older run.
        if [ "$(readlink "$LATEST" 2> /dev/null)" != "$before" ] && [ -f "$LATEST/$file" ]; then
            # The file opens with its own title; the heading above replaces it.
            sed '1{/^#/d}' "$LATEST/$file"
        else
            echo "No $file: the run stopped before writing it. See the job log."
        fi
        echo
    } >> "$SUMMARY"
    return "$status"
}

FAILED=0

SOAK=0
run "Concurrency soak (10 min, 200 consumers)" summary.md --bench soak -- "${SOAK_ARGS[@]}" || SOAK=$?
if [ "$SOAK" != 0 ]; then
    echo "::error title=Concurrency soak $(status_name "$SOAK")::See the job summary and log."
    FAILED=1
fi

GATE=0
run "Performance gate" report.md || GATE=$?
if [ "$GATE" = 2 ]; then
    echo "INCONCLUSIVE; retrying once with $RETRY_ROUNDS rounds."
    GATE=0
    run "Performance gate, retry ($RETRY_ROUNDS rounds)" report.md --rounds "$RETRY_ROUNDS" || GATE=$?
fi
case "$GATE" in
    0) echo "Performance gate: PASS" ;;
    2)
        echo "::warning title=Performance gate INCONCLUSIVE::Still too noisy after a retry with $RETRY_ROUNDS rounds. Not a PASS: no regression was shown, and none was ruled out."
        echo "**Not a PASS.** INCONCLUSIVE after $RETRY_ROUNDS rounds: no supported regression, but the run was too noisy to rule one out." >> "$SUMMARY" ;;
    *)
        echo "::error title=Performance gate $(status_name "$GATE")::See the job summary and log."
        FAILED=1 ;;
esac

exit "$FAILED"
