#!/usr/bin/env bash
# Performance gate at a release checkpoint (full-suite.yml): the approved
# baseline against this checkout on one runner, servers pinned away from the
# gate's own clients. An INCONCLUSIVE first attempt is retried once with more
# rounds; only PASS exits zero.
#
# Usage: scripts/perf-gate-ci.sh
#   PERF_GATE_ROUNDS        rounds of the first attempt (default 20)
#   PERF_GATE_RETRY_ROUNDS  rounds of the retry after INCONCLUSIVE (default 30)
#
# Each attempt's run.json, report.md and verdict.json stay in
# target/perf-gate/runs/<utc-time>/. With GITHUB_STEP_SUMMARY set, every
# attempt's report is appended to the job summary.
set -uo pipefail

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"
ROUNDS=${PERF_GATE_ROUNDS:-20}
RETRY_ROUNDS=${PERF_GATE_RETRY_ROUNDS:-30}
SUMMARY=${GITHUB_STEP_SUMMARY:-/dev/null}

# Keep CPUs 0-1 for the gate's clients and the runner agent; the servers get
# the rest. Too few CPUs to split: leave them unpinned.
CPUS=$(nproc)
PIN=()
if [ "$CPUS" -ge 8 ]; then
    PIN=(--server-cpus "2-$((CPUS - 1))")
fi

status_name() {
    case "$1" in
        0) echo PASS ;; 1) echo FAIL ;; 2) echo INCONCLUSIVE ;; 3) echo INVALID ;;
        *) echo "ERROR (exit $1)" ;;
    esac
}

attempt() {
    local n=$1 rounds=$2 status=0 before
    before=$(readlink target/perf-gate/latest 2>/dev/null || true)
    echo "=== Attempt $n: $rounds rounds ${PIN[*]} ==="
    scripts/perf-gate.sh --rounds "$rounds" "${PIN[@]}" || status=$?
    {
        echo "## Performance gate attempt $n ($rounds rounds): $(status_name "$status")"
        echo
        # A run that stopped before judging leaves latest on an older run.
        if [ "$(readlink target/perf-gate/latest 2>/dev/null)" != "$before" ] &&
            [ -f target/perf-gate/latest/report.md ]; then
            # The report opens with its own title; the attempt heading replaces it.
            sed '1{/^## Performance gate/d}' target/perf-gate/latest/report.md
        else
            echo "No report: the gate stopped before judging. See the job log."
        fi
        echo
    } >> "$SUMMARY"
    return "$status"
}

STATUS=0
attempt 1 "$ROUNDS" || STATUS=$?
if [ "$STATUS" = 2 ]; then
    echo "INCONCLUSIVE after $ROUNDS rounds; retrying once with $RETRY_ROUNDS."
    STATUS=0
    attempt 2 "$RETRY_ROUNDS" || STATUS=$?
fi

case "$STATUS" in
    0) echo "Performance gate: PASS" ;;
    1) echo "::error title=Performance gate FAIL::A required metric regressed beyond its budget (see the job summary)." ;;
    2) echo "::error title=Performance gate INCONCLUSIVE::Still too noisy after a retry with $RETRY_ROUNDS rounds; only PASS is accepted. Re-run the job, or run make perf-gate on a quiet host." ;;
    *) echo "::error title=Performance gate $(status_name "$STATUS")::The run was invalid or did not finish (see the job log)." ;;
esac
exit "$STATUS"
