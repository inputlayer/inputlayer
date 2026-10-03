#!/bin/bash
# Verified Completions false-alarm/revision gate (issue #88).
#
# Starts a throwaway engine (temp data dir and credentials, WS rate limits
# off like the snapshot harness: the corpus replay is statement-heavy), runs
# the evaluator contract self-tests, then gate.py: corpus.json scenarios -
# controls included - and benchmark.json recorded extractions through the
# real rule pack. No model calls. Exits nonzero on any false alarm, stale
# correction, finding-kind drift, missing scenario or missing metric.
#
# Usage: scripts/run_vc_gate.sh [--skip-build] [gate.py args...]
# Env:   VC_GATE_PORT (default 8093), INPUTLAYER_SERVER_BIN

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POC_DIR="$PROJECT_DIR/docs/internals/verified-completions/poc"
PORT="${VC_GATE_PORT:-8093}"
SERVER_BIN="${INPUTLAYER_SERVER_BIN:-$PROJECT_DIR/target/release/inputlayer-server}"

if [[ "${1:-}" == "--skip-build" ]]; then
    shift
else
    cargo build --release --bin inputlayer-server
fi

TEMP_DIR="$(mktemp -d -t il-vc-gate-XXXXXX)"
SERVER_PID=""
cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

cat > "$TEMP_DIR/server.toml" <<EOF
[storage]
data_dir = "$TEMP_DIR/data"
default_database = "default"

[storage.persistence]
format = "parquet"
compression = "snappy"
auto_save_interval = 0
enable_wal = true

# Throwaway store: no fsync per write, so a loaded runner's disk cannot
# stall a KG create/drop past the client's keepalive.
[storage.persist]
durability_mode = "async"

[logging]
level = "warn"
format = "text"

[http]
enabled = true
host = "127.0.0.1"
port = $PORT

[http.auth]
credentials_file = "$TEMP_DIR/credentials.toml"

[http.rate_limit]
ws_max_messages_per_sec = 0
per_ip_max_rps = 0
EOF

# --config mode rejects INPUTLAYER_* env vars that map to no config field.
(
    for v in $(compgen -e); do
        [[ "$v" == INPUTLAYER_* ]] && unset "$v"
    done
    exec "$SERVER_BIN" --config "$TEMP_DIR/server.toml" > "$TEMP_DIR/server.log" 2>&1
) &
SERVER_PID=$!

for _ in $(seq 1 60); do
    curl -sf "http://127.0.0.1:$PORT/health" > /dev/null 2>&1 && break
    sleep 0.5
done
if ! curl -sf "http://127.0.0.1:$PORT/health" > /dev/null 2>&1; then
    echo "vc-gate: engine failed to start on port $PORT" >&2
    cat "$TEMP_DIR/server.log" >&2
    exit 1
fi
API_KEY="$(grep '^api_key' "$TEMP_DIR/credentials.toml" | head -1 | cut -d'"' -f2)"
if [[ -z "$API_KEY" ]]; then
    echo "vc-gate: no api_key in $TEMP_DIR/credentials.toml" >&2
    exit 1
fi

cd "$POC_DIR"
python3 -m unittest -q test_evaluator
uv run --project "$PROJECT_DIR/packages/inputlayer-py" python gate.py \
    --server "ws://127.0.0.1:$PORT/ws" --api-key "$API_KEY" "$@"
