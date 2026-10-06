#!/usr/bin/env bash
# Prove scripts/perf-gate-baseline.sh builds every baseline from exactly its
# own sources. Works in a throwaway repository whose workspace has the shape
# the baseline build depends on: a root package with an `inputlayer-server`
# binary and a path dependency, `ws-protocol`. Two commits differ in both
# crates, each a change to an existing file, and both are older than any
# build, as real baselines are. The server prints what it was built from:
#   - the newer commit, then the older one: each binary is its own commit's
#   - the older commit, then the newer one, in a second clone: the same
#   - a cached baseline is returned again without a rebuild
#
# Usage: scripts/perf-gate-baseline-selftest.sh   (needs cargo; no network)
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
HELPER=$ROOT/scripts/perf-gate-baseline.sh
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# The scratch repositories must see only their own config.
unset GIT_CONFIG_PARAMETERS GIT_CONFIG_COUNT
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

FAILURES=0
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; FAILURES=$((FAILURES + 1)); }

# commit <repo> <n> <date>: both crates say <n>, committed at <date>.
commit() {
    printf 'pub const ENGINE: &str = "engine-%s";\n' "$2" > "$1/src/lib.rs"
    printf 'pub const PROTOCOL: &str = "protocol-%s";\n' "$2" > "$1/ws-protocol/src/lib.rs"
    git -C "$1" add .
    GIT_AUTHOR_DATE=$3 GIT_COMMITTER_DATE=$3 git -C "$1" commit --quiet -m "baseline $2"
}

# new_repo <dir>: the fixture workspace with an older and a newer commit.
new_repo() {
    mkdir -p "$1/src/bin" "$1/ws-protocol/src"
    git -C "$1" init --quiet
    git -C "$1" config user.name perf-gate-baseline-selftest
    git -C "$1" config user.email perf-gate-baseline-selftest@invalid
    cat > "$1/Cargo.toml" <<'EOF'
[workspace]
members = ["ws-protocol"]

[package]
name = "inputlayer"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "inputlayer-server"
path = "src/bin/server.rs"

[dependencies]
inputlayer-ws-protocol = { path = "ws-protocol" }
EOF
    cat > "$1/ws-protocol/Cargo.toml" <<'EOF'
[package]
name = "inputlayer-ws-protocol"
version = "0.1.0"
edition = "2021"
EOF
    cat > "$1/src/bin/server.rs" <<'EOF'
fn main() {
    println!("{} {}", inputlayer::ENGINE, inputlayer_ws_protocol::PROTOCOL);
}
EOF
    commit "$1" older 2020-01-01T00:00:00Z
    commit "$1" newer 2020-01-02T00:00:00Z
}

# expect_baseline <repo> <rev> <n>: the baseline of <rev> says <n> for both crates.
expect_baseline() {
    local bin said
    if ! bin=$(cd "$1" && "$HELPER" "$2" 2> "$TMP/build.log"); then
        fail "the $3 baseline did not build"; cat "$TMP/build.log"
        return
    fi
    said=$("$bin")
    if [ "$said" = "engine-$3 protocol-$3" ]; then
        pass "the $3 baseline is built from its own sources"
    else
        fail "the $3 baseline was built from other sources: it says '$said'"
    fi
}

new_repo "$TMP/newer-first"
expect_baseline "$TMP/newer-first" HEAD newer
expect_baseline "$TMP/newer-first" HEAD~1 older

new_repo "$TMP/older-first"
expect_baseline "$TMP/older-first" HEAD~1 older
expect_baseline "$TMP/older-first" HEAD newer

if (cd "$TMP/older-first" && "$HELPER" HEAD~1 2>&1) | grep -q 'Build baseline server'; then
    fail "a cached baseline was built again"
else
    pass "a cached baseline is not built again"
fi
expect_baseline "$TMP/older-first" HEAD~1 older

if [ -n "$(ls "$TMP/older-first/target/perf-gate/baseline-build")" ]; then
    fail "a build directory was left behind"
else
    pass "no build directory is left behind"
fi

if [ "$FAILURES" -gt 0 ]; then
    echo "perf-gate-baseline-selftest: $FAILURES failure(s)"
    exit 1
fi
echo "perf-gate-baseline-selftest: all checks passed"
