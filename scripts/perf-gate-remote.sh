#!/usr/bin/env bash
# Performance gate on the dedicated benchmark host: measure a commit there
# with scripts/perf-gate.sh and copy the results back. Heavy perf runs go to
# that host, not to the development box (perf-gate/README.md).
#
# Usage: scripts/perf-gate-remote.sh [options] [perf-gate options]
#   --host HOST     ssh destination (default $PERF_GATE_HOST, else sam-dev-benchmarks)
#   --rev REV       commit to measure (default HEAD). It need not be pushed:
#                   it is pushed straight to the host's clone when origin lacks it.
#   --dir DIR       the host's clone (default bench/inputlayer, under its home)
#   --attach STAMP  follow a run that is already going (after a dropped
#                   connection) and copy its results back
#   --wait-lock S   wait up to S seconds for another benchmark (its lock, or
#                   its running servers) to clear instead of refusing at once
#   --bench sessions -- ARGS
#                   run scripts/bench-sessions.sh ARGS (the session-scale
#                   benchmark) instead of the gate, under the same rules
#   --bench soak -- ARGS
#                   run scripts/soak.sh ARGS (the sustained concurrency soak)
#   --bench views -- ARGS
#                   run scripts/bench-views.sh ARGS (the views benchmark)
#   Everything else goes to scripts/perf-gate.sh: --aa, --rounds N,
#   --baseline-rev REV, --fixtures LIST, --profile NAME, --server-cpus LIST,
#   --gate-cpus LIST, --no-verdict. A working tree is never measured, only
#   commits: commit first (uncommitted changes get a warning).
#
# The run follows the host's rules (~/README-bench.txt): one benchmark at a
# time (a lock, and a refusal while any inputlayer-server is running), the
# host's nproc, the commit and the command recorded with every result in
# bench.txt, and no server left running afterwards. On a host with 16 or
# more CPUs the gate's (or soak's) clients get CPUs 0-7 and the servers the
# rest, both whole SMT core pairs; with 8 or more, 0-1 and the rest.
#
# The run is detached on the host, so a dropped connection does not stop it;
# rerun with --attach STAMP to collect it. Results land in
# target/perf-gate/remote/<stamp>/ (run.json, report.md, verdict.json,
# summary.md, bench.txt, gate.log); target/perf-gate/remote/latest points
# there.
#
# Exit status: the gate's (0 pass, 1 fail, 2 inconclusive, 3 invalid),
# 4 when the host is busy, or another non-zero status on a setup error.
set -euo pipefail

# One compound command: bash reads all of it before running any of it, so
# editing this file while a run is being followed cannot change that run.
{

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

HOST=${PERF_GATE_HOST:-sam-dev-benchmarks}
REV=HEAD
DIR=bench/inputlayer
ATTACH=""
WAIT_LOCK=0
BENCH=gate
GATE_ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --host) HOST=$2; shift 2 ;;
        --rev) REV=$2; shift 2 ;;
        --dir) DIR=$2; shift 2 ;;
        --attach) ATTACH=$2; shift 2 ;;
        --wait-lock) WAIT_LOCK=$2; shift 2 ;;
        --bench) BENCH=$2; shift 2 ;;
        # Everything after -- goes to the benchmark script unchanged.
        --) shift; GATE_ARGS+=("$@"); break ;;
        --baseline-rev)
            # Resolved here, so the host measures exactly this commit.
            GATE_ARGS+=("$1" "$(git rev-parse --verify "$2^{commit}")"); shift 2 ;;
        --rounds|--profile|--fixtures|--server-cpus|--gate-cpus) GATE_ARGS+=("$1" "$2"); shift 2 ;;
        --aa|--no-verdict) GATE_ARGS+=("$1"); shift ;;
        -h|--help) sed -n '2,39p' "$0"; exit 0 ;;
        *) echo "perf-gate-remote: unknown option $1" >&2; exit 3 ;;
    esac
done
case "$BENCH" in
    gate) SCRIPT=scripts/perf-gate.sh LATEST=target/perf-gate/latest ;;
    sessions) SCRIPT=scripts/bench-sessions.sh LATEST=target/bench-sessions/latest ;;
    soak) SCRIPT=scripts/soak.sh LATEST=target/soak/latest ;;
    views) SCRIPT=scripts/bench-views.sh LATEST=target/bench-views/latest ;;
    *) echo "perf-gate-remote: unknown benchmark $BENCH (gate, sessions, soak or views)" >&2; exit 3 ;;
esac

SSH=(ssh -o BatchMode=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=6 "$HOST")
# Runs, logs and the lock live next to the clone on the host.
STATE=perf-gate-remote

if [ -z "$ATTACH" ]; then
    SHA=$(git rev-parse --verify "$REV^{commit}")
    for arg in "${GATE_ARGS[@]}"; do
        if [ "$arg" = --baseline-rev ]; then BASELINE_GIVEN=1; fi
    done
    STAMP=$(date -u +%Y%m%dT%H%M%SZ)-${SHA:0:8}
    if [ "$REV" = HEAD ] && [ -n "$(git status --porcelain --untracked-files=no)" ]; then
        echo "perf-gate-remote: warning: measuring HEAD $SHA; uncommitted changes are not measured" >&2
    fi

    echo "=== $HOST: fetch $SHA ==="
    if ! "${SSH[@]}" "cd $DIR && git fetch -q origin && git cat-file -e $SHA^{commit}" 2>/dev/null; then
        echo "origin lacks $SHA; pushing it to $HOST:$DIR"
        git push -q "$HOST:$DIR" "$SHA:refs/perf-gate/$SHA"
    fi
    # The baseline must be on the host too (a --baseline-rev may be local).
    if [ "${BASELINE_GIVEN:-0}" = 1 ]; then
        for ((i = 0; i < ${#GATE_ARGS[@]}; i++)); do
            if [ "${GATE_ARGS[$i]}" = --baseline-rev ]; then
                base=${GATE_ARGS[$((i + 1))]}
                if ! "${SSH[@]}" "cd $DIR && git cat-file -e $base^{commit}" 2>/dev/null; then
                    git push -q "$HOST:$DIR" "$base:refs/perf-gate/$base"
                fi
            fi
        done
    fi

    # The host-side runner, started detached. Positional arguments: the
    # clone, the state directory, the stamp, the commit, the lock wait, the
    # benchmark script and its latest-run link, then the script's own.
    "${SSH[@]}" "mkdir -p $STATE && cat > $STATE/$STAMP.run.sh" <<'REMOTE'
#!/usr/bin/env bash
set -uo pipefail
DIR=$1 STATE=$2 STAMP=$3 SHA=$4 WAIT_LOCK=$5 SCRIPT=$6 LATEST=$7
shift 7
export PATH=$HOME/.cargo/bin:$PATH
LOG=$HOME/$STATE/$STAMP.log
STATUS_FILE=$HOME/$STATE/$STAMP.status
exec > "$LOG" 2>&1
finish() { echo "$1" > "$STATUS_FILE"; exit "$1"; }

# One benchmark at a time on this host.
exec 9> "$HOME/$STATE/lock"
if ! flock -n 9; then
    echo "perf-gate-remote: another benchmark holds $HOME/$STATE/lock; waiting up to ${WAIT_LOCK}s"
    if ! flock -w "$WAIT_LOCK" 9; then
        echo "perf-gate-remote: lock still held"; finish 4
    fi
fi
# A benchmark that does not take the lock still runs servers: wait for them
# too, within the same budget.
waited=0
while pgrep -x inputlayer-serv > /dev/null; do
    if [ "$waited" -ge "$WAIT_LOCK" ]; then
        echo "perf-gate-remote: an inputlayer-server is already running here:"
        pgrep -a -x inputlayer-serv
        finish 4
    fi
    [ "$waited" = 0 ] && echo "perf-gate-remote: an inputlayer-server is running; waiting up to ${WAIT_LOCK}s"
    sleep 10
    waited=$((waited + 10))
done
# Never leave a server running: the gate stops its own, this catches any
# left behind by a crash. Nothing else may run here meanwhile (the checks above).
trap 'pkill -x inputlayer-serv 2> /dev/null || true' EXIT

cd "$HOME/$DIR" || finish 3
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
    echo "perf-gate-remote: $HOME/$DIR has local changes; refusing to check out over them"
    finish 3
fi
git checkout -q --detach "$SHA" || finish 3

CPUS=$(nproc)
PIN=()
case " $* " in *" --server-cpus "*|*" --gate-cpus "*) ;; *)
    # bench-sessions.sh and bench-views.sh pin only servers; their clients
    # float on 0-7 too.
    if [ "$CPUS" -ge 16 ]; then
        PIN=(--server-cpus "8-$((CPUS - 1))")
        [ "$SCRIPT" = scripts/perf-gate.sh ] && PIN+=(--gate-cpus 0-7)
        [ "$SCRIPT" = scripts/soak.sh ] && PIN+=(--client-cpus 0-7)
    elif [ "$CPUS" -ge 8 ]; then
        PIN=(--server-cpus "2-$((CPUS - 1))")
        [ "$SCRIPT" = scripts/perf-gate.sh ] && PIN+=(--gate-cpus 0-1)
        [ "$SCRIPT" = scripts/soak.sh ] && PIN+=(--client-cpus 0-1)
    fi
esac
COMMAND="$SCRIPT $* ${PIN[*]}"
before=$(readlink "$LATEST" 2> /dev/null || true)
{
    echo "host: $(hostname)"
    echo "nproc: $CPUS"
    echo "commit: $(git rev-parse HEAD)"
    echo "command: $COMMAND"
    echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "load average at start: $(cat /proc/loadavg)"
} > "$HOME/$STATE/$STAMP.bench.txt"
cat "$HOME/$STATE/$STAMP.bench.txt"

STATUS=0
# Builds use every CPU; the scripts pin only the measurement.
"$SCRIPT" "$@" "${PIN[@]}" || STATUS=$?
after=$(readlink "$LATEST" 2> /dev/null || true)
{
    echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "load average at end: $(cat /proc/loadavg)"
    echo "exit status: $STATUS"
    # A run that stopped before judging leaves latest on an older run.
    if [ "$after" != "$before" ]; then echo "run dir: $after"; fi
} >> "$HOME/$STATE/$STAMP.bench.txt"
finish "$STATUS"
REMOTE

    echo "=== $HOST: run $STAMP ($SCRIPT ${GATE_ARGS[*]}) ==="
    "${SSH[@]}" "setsid nohup bash $STATE/$STAMP.run.sh $DIR $STATE $STAMP $SHA $WAIT_LOCK $SCRIPT $LATEST ${GATE_ARGS[*]} < /dev/null > /dev/null 2>&1 &"
else
    STAMP=$ATTACH
fi

# Follow the log until the status file appears. A dropped connection leaves
# the run going; collect it with --attach.
"${SSH[@]}" "cd $STATE && while [ ! -e $STAMP.log ] && [ ! -e $STAMP.status ]; do sleep 1; done;
    tail -n +1 -f $STAMP.log & tailer=\$!;
    while [ ! -e $STAMP.status ]; do sleep 5; done; sleep 1; kill \$tailer" ||
    { echo "perf-gate-remote: lost $HOST; collect later with --attach $STAMP" >&2; exit 5; }
STATUS=$("${SSH[@]}" "cat $STATE/$STAMP.status")

OUT=$ROOT/target/perf-gate/remote/$STAMP
mkdir -p "$OUT"
scp -q "$HOST:$STATE/$STAMP.bench.txt" "$OUT/bench.txt" 2> /dev/null || true
scp -q "$HOST:$STATE/$STAMP.log" "$OUT/gate.log" 2> /dev/null || true
RUN_DIR=""
if [ -s "$OUT/bench.txt" ]; then
    RUN_DIR=$(sed -n 's/^run dir: //p' "$OUT/bench.txt")
fi
if [ -n "$RUN_DIR" ]; then
    # The gate's files, or the session benchmark's, views benchmark's or
    # soak's (result.json, summary.md, settings.txt).
    for f in run.json report.md verdict.json summary.md result.json settings.txt; do
        scp -q "$HOST:$RUN_DIR/$f" "$OUT/$f" 2> /dev/null || true
    done
fi
ln -sfn "$OUT" "$ROOT/target/perf-gate/remote/latest"
echo ""
echo "Results: $OUT (exit status $STATUS)"
exit "$STATUS"
}
