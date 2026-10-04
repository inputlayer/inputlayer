#!/usr/bin/env bash
# Runs the README quick starts against a fresh container, exactly as written:
#   README.md                         <!-- quickstart:docker -->      start the
#                                     container and export its API key
#   packages/inputlayer-py/README.md  <!-- quickstart:python -->      compared
#                                     with <!-- quickstart:python-output -->
#   packages/inputlayer-js/README.md  <!-- quickstart:typescript -->  compared
#                                     with the `// ...` comment on each
#                                     console.log line
# plus packages/inputlayer-py/examples/01_quickstart.py, the GUI at / and the
# 401 hint. A marked block is the fenced block right after its marker.
#
# Needs docker, python3 (3.10+), node (18+) and npm. Port 8080 must be free.
# QUICKSTART_IMAGE runs another image in place of the documented one (CI
# builds this checkout's Dockerfile and tags it as the documented name).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d -t il-quickstart-XXXXXX)
DOC_IMAGE=ghcr.io/inputlayer/inputlayer
CONTAINER=inputlayer

step() { printf '\n== %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

# extract FILE NAME: the fenced block after `<!-- quickstart:NAME ` or
# `<!-- quickstart:NAME -->`.
extract() {
    local out
    out=$(awk -v name="$2" '
        f == 0 && (index($0, "<!-- quickstart:" name " ") == 1 || $0 == "<!-- quickstart:" name " -->") { f = 1; next }
        f == 1 && /^```/ { f = 2; next }
        f == 2 && /^```/ { exit }
        f == 2 { print }
    ' "$1")
    [ -n "$out" ] || fail "no quickstart:$2 block in $1"
    printf '%s\n' "$out"
}

compare() { # compare LABEL EXPECTED_FILE ACTUAL_FILE
    if ! diff -u "$2" "$3"; then
        fail "$1 output differs from the documented output (diff above: - documented, + actual)"
    fi
    echo "$1: output matches the docs"
}

cleanup() {
    status=$?
    if [ $status -ne 0 ]; then
        echo "--- container logs ---"
        docker logs "$CONTAINER" 2>&1 | grep -v INFO | tail -30 || true
    fi
    docker rm -f "$CONTAINER" > /dev/null 2>&1 || true
    exit $status
}
trap cleanup EXIT

step "README.md: start the container and export the API key"
extract "$ROOT/README.md" docker > "$WORK/docker.sh"
if [ -n "${QUICKSTART_IMAGE:-}" ]; then
    sed -i "s|$DOC_IMAGE\\b|$QUICKSTART_IMAGE|g" "$WORK/docker.sh"
fi
cat "$WORK/docker.sh"
docker rm -f "$CONTAINER" > /dev/null 2>&1 || true
unset INPUTLAYER_API_KEY
# shellcheck disable=SC1091
source "$WORK/docker.sh"
[ -n "${INPUTLAYER_API_KEY:-}" ] || fail "the documented commands left INPUTLAYER_API_KEY empty"
for _ in $(seq 1 60); do
    curl -sf http://localhost:8080/health > /dev/null && break
    sleep 0.5
done
curl -sf http://localhost:8080/health > /dev/null || fail "server not healthy on :8080"

step "GUI at http://localhost:8080 and the 401 hint"
code=$(curl -s -o "$WORK/index.html" -w '%{http_code}' http://localhost:8080/)
if ! { [ "$code" = 200 ] && grep -qi '<html' "$WORK/index.html"; }; then
    fail "GET / returned $code, not the GUI"
fi
echo "GET / -> 200 (GUI)"
code=$(curl -s -o "$WORK/401.txt" -w '%{http_code}' http://localhost:8080/metrics)
if ! { [ "$code" = 401 ] && grep -q credentials.toml "$WORK/401.txt"; }; then
    fail "unauthenticated GET /metrics returned $code without the credentials hint"
fi
echo "GET /metrics without a key -> 401 naming credentials.toml"
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $INPUTLAYER_API_KEY" http://localhost:8080/metrics)
[ "$code" = 200 ] || fail "GET /metrics with the exported key returned $code"
echo "GET /metrics with the exported key -> 200"

step "packages/inputlayer-py: install as documented and run the quick start"
python3 -m venv "$WORK/venv"
"$WORK/venv/bin/pip" install --quiet "$ROOT/packages/inputlayer-py"
extract "$ROOT/packages/inputlayer-py/README.md" python > "$WORK/quickstart.py"
extract "$ROOT/packages/inputlayer-py/README.md" python-output > "$WORK/python.expected"
(cd "$WORK" && "$WORK/venv/bin/python" quickstart.py) > "$WORK/python.actual"
compare "Python README quick start" "$WORK/python.expected" "$WORK/python.actual"
"$WORK/venv/bin/python" "$ROOT/packages/inputlayer-py/examples/01_quickstart.py"
echo "examples/01_quickstart.py: ran"

step "packages/inputlayer-js: build, pack and install as documented, run the quick start"
(cd "$ROOT/packages/inputlayer-js" && npm ci --silent && npm run --silent build && npm pack --silent --pack-destination "$WORK" > /dev/null)
documented=$(sed -n 's|^npm install inputlayer@file:.*/\([^/]*\.tgz\)$|\1|p' "$ROOT/packages/inputlayer-js/README.md" | head -1)
if ! { [ -n "$documented" ] && [ -f "$WORK/$documented" ]; }; then
    fail "npm pack wrote $(cd "$WORK" && ls ./*.tgz), the README installs '${documented}'"
fi
mkdir "$WORK/ts"
(cd "$WORK/ts" && npm init -y > /dev/null && npm pkg set type=module \
    && npm install --silent --no-audit --no-fund "inputlayer@file:$WORK/$documented" tsx)
extract "$ROOT/packages/inputlayer-js/README.md" typescript > "$WORK/ts/quickstart.ts"
sed -n 's|.*console\.log(.*// \(.*\)$|\1|p' "$WORK/ts/quickstart.ts" | sed 's/ *$//' > "$WORK/ts.expected"
[ -s "$WORK/ts.expected" ] || fail "the TypeScript quick start documents no console.log output"
(cd "$WORK/ts" && npx tsx quickstart.ts) > "$WORK/ts.actual"
compare "TypeScript README quick start" "$WORK/ts.expected" "$WORK/ts.actual"

step "all quick starts passed"
