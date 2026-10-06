#!/usr/bin/env bash
# Build the baseline server of one commit for the performance gate and the
# benchmarks, and print the path of its binary. The server is built exactly
# like the release image (release profile, all features) from `git archive`
# of the commit, and cached per commit and toolchain.
#
# Usage: scripts/perf-gate-baseline.sh COMMIT
#
# Every baseline is built in a target directory of its own, removed once the
# binary is cached. Baselines must never share one: cargo names a workspace
# crate's artifacts by its path inside the workspace, not by where the
# workspace is, and rebuilds one only when a source file is newer than the
# last build. `git archive` gives every file the commit's time, which is
# older than any build already in the directory, so a second baseline would
# link the first one's crates (or the whole first server), or fail to
# compile against them.
#
# The binary lands in target/perf-gate/baseline-servers/<commit>-<toolchain>/;
# build output goes to stderr. Exit status: 0, or non-zero when the commit
# does not resolve or does not build.
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "usage: scripts/perf-gate-baseline.sh COMMIT" >&2
    exit 3
fi
ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"
OUT=$ROOT/target/perf-gate
SHA=$(git rev-parse --verify "$1^{commit}")
KEY=$SHA-$(rustc -V | sha256sum | cut -c1-12)
BIN=$OUT/baseline-servers/$KEY/inputlayer-server

if [ ! -x "$BIN" ]; then
    echo "=== Build baseline server $SHA ===" >&2
    WORK=$OUT/baseline-build/$KEY
    rm -rf "$WORK" && mkdir -p "$WORK/src"
    git archive --format=tar "$SHA" | tar -x -C "$WORK/src"
    # Same dependency versions as the candidate where the manifests allow.
    if [ -f Cargo.lock ]; then cp Cargo.lock "$WORK/src/"; fi
    CARGO_TARGET_DIR=$WORK/target cargo build --release --all-features \
        --manifest-path "$WORK/src/Cargo.toml" --bin inputlayer-server >&2
    mkdir -p "$(dirname "$BIN")"
    # Renamed into place: an interrupted copy is never taken for a cached binary.
    cp "$WORK/target/release/inputlayer-server" "$BIN.tmp"
    mv "$BIN.tmp" "$BIN"
    rm -rf "$WORK"
fi
echo "$BIN"
