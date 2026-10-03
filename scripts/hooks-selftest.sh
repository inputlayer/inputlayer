#!/usr/bin/env bash
# Prove the optional git hooks reject what they promise to reject. Works in
# a throwaway clone with the working tree's hook files, isolated from your
# git config, and commits there through a real `git commit`:
#   - the installer refuses to replace a foreign core.hooksPath
#   - a misformatted Rust change is rejected
#   - a staged credential is rejected and never printed in clear
#   - a formatted Rust change without credentials commits
#
# Usage: scripts/hooks-selftest.sh   (needs rustfmt and gitleaks)
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
REPO=$TMP/repo
FILES=(.githooks/pre-commit .githooks/pre-push .gitleaks.toml Makefile scripts/secret-scan.sh)

# The scratch clone must see only its own config, never a global or
# command-line core.hooksPath.
unset GIT_CONFIG_PARAMETERS GIT_CONFIG_COUNT
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

git clone --quiet --shared --no-checkout "$ROOT" "$REPO"
git -C "$REPO" checkout --quiet --detach "$(git -C "$ROOT" rev-parse HEAD)"
for f in "${FILES[@]}"; do
    mkdir -p "$REPO/$(dirname "$f")"
    cp -p "$ROOT/$f" "$REPO/$f"
done
if [ -x "$ROOT/target/tools/gitleaks" ]; then
    mkdir -p "$REPO/target/tools"
    ln -s "$ROOT/target/tools/gitleaks" "$REPO/target/tools/gitleaks"
fi
cd "$REPO"
git config user.name hooks-selftest
git config user.email hooks-selftest@invalid
git add -- "${FILES[@]}"
git commit --quiet --allow-empty -m "selftest: working-tree hook files"

FAILURES=0
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; FAILURES=$((FAILURES + 1)); }
# expect_commit <ok|rejected> <name> [reason]: commit the index and compare
# the outcome; a rejection must print <reason>, so a broken tool cannot pass.
expect_commit() {
    local before log
    before=$(git rev-parse HEAD)
    log=$TMP/$2.log
    if git commit --quiet -m "selftest: $2" >"$log" 2>&1; then
        if [ "$1" = ok ]; then pass "$2 commits"; else fail "$2 was committed"; cat "$log"; fi
    else
        if [ "$1" = rejected ] && [ "$(git rev-parse HEAD)" = "$before" ] && grep -qF "$3" "$log"; then
            pass "$2 rejected"
        else
            fail "$2 did not commit"; cat "$log"
        fi
    fi
}
reset_tree() { git reset --quiet --hard HEAD; git clean --quiet -fd; }

git config core.hooksPath elsewhere
if make --no-print-directory install-hooks >"$TMP/foreign.log" 2>&1; then
    fail "installer replaced a foreign core.hooksPath"
elif [ "$(git config core.hooksPath)" = elsewhere ]; then
    pass "installer keeps a foreign core.hooksPath"
else
    fail "installer changed core.hooksPath and failed"
fi
git config --unset core.hooksPath
make --no-print-directory install-hooks >/dev/null

printf '\nfn  hooks_selftest_misformatted ( ) { }\n' >>src/lib.rs
git add src/lib.rs
expect_commit rejected misformatted-rust 'formatting failed'
reset_tree

# Built at run time so this script never contains a credential-shaped literal.
TOKEN=ghp_$(head -c 1024 /dev/urandom | LC_ALL=C tr -dc 'A-Za-z0-9' | cut -c1-36)
printf 'GITHUB_TOKEN=%s\n' "$TOKEN" >leaked.env
git add -f leaked.env
expect_commit rejected staged-credential 'leaks found'
if grep -qF "$TOKEN" "$TMP/staged-credential.log"; then
    fail "secret scan printed the credential in clear"
else
    pass "secret scan output is redacted"
fi
reset_tree

printf '\n/// Hook self-test marker.\npub const HOOKS_SELFTEST: u8 = 0;\n' >>src/lib.rs
git add src/lib.rs
expect_commit ok formatted-rust

if [ "$FAILURES" -ne 0 ]; then
    echo "hooks self-test: $FAILURES failure(s)"
    exit 1
fi
echo "hooks self-test passed"
