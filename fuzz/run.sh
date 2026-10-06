#!/usr/bin/env bash
# Run a fuzzing campaign over the client-input targets, all at once.
#
#   fuzz/run.sh [SECONDS] [TARGET...]
#
# SECONDS per target (default 600); targets default to all three. Each
# target runs FUZZ_FORKS libFuzzer workers (default 4) that keep going past
# a crash, timeout or OOM, so one campaign collects every finding: they land
# in fuzz/artifacts/<target>/ and are listed at the end. The corpus grows in
# fuzz/corpus/<target>/; the generated seeds in fuzz/seeds/<target>/ are
# read-only inputs. Needs cargo-fuzz; uses a nightly toolchain when one is
# installed, otherwise the stable one with RUSTC_BOOTSTRAP=1.
set -euo pipefail
cd "$(dirname "$0")"

seconds=${1:-600}
shift || true
targets=("$@")
if [ ${#targets[@]} -eq 0 ]; then
    targets=(iql_statement iql_program ws_client_frame)
fi
forks=${FUZZ_FORKS:-4}

cargo=(cargo)
if rustup toolchain list 2>/dev/null | grep -q '^nightly'; then
    cargo=(cargo +nightly)
else
    export RUSTC_BOOTSTRAP=1
fi

python3 seeds.py
"${cargo[@]}" fuzz build -O

# libFuzzer takes one dictionary; frames carry IQL inside JSON.
cat iql.dict ws_frame.dict >target/ws_client_frame.dict

pids=()
for target in "${targets[@]}"; do
    dict=iql.dict
    [ "$target" = ws_client_frame ] && dict=target/ws_client_frame.dict
    mkdir -p "corpus/$target" "artifacts/$target"
    "${cargo[@]}" fuzz run -O "$target" "corpus/$target" "seeds/$target" -- \
        -dict="$dict" -max_total_time="$seconds" -timeout=10 -rss_limit_mb=2048 \
        -fork="$forks" -ignore_crashes=1 -ignore_timeouts=1 -ignore_ooms=1 \
        -print_final_stats=1 >"artifacts/$target.log" 2>&1 &
    pids+=($!)
done
status=0
for pid in "${pids[@]}"; do
    wait "$pid" || status=1
done

for target in "${targets[@]}"; do
    echo "== $target: $(grep -aE '^#[0-9]+: cov' "artifacts/$target.log" | tail -n 1)"
    find "artifacts/$target" -type f \( -name 'crash-*' -o -name 'timeout-*' -o -name 'oom-*' \) |
        sort | sed 's/^/  finding: /'
done
exit $status
