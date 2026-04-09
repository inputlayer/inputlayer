#!/usr/bin/env bash
set -euo pipefail

# Start InputLayer server, seed all demo knowledge graphs, verify, then keep running.
# Called by `make demo`.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SERVER="$ROOT_DIR/target/debug/inputlayer-server"
CLIENT="$ROOT_DIR/target/debug/inputlayer-client"
SEEDS_DIR="$ROOT_DIR/demo/seeds"
PREPROCESS="$SCRIPT_DIR/preprocess-idl.py"
API_KEY="demo-api-key"

export INPUTLAYER_ADMIN_PASSWORD="demo-admin"
export INPUTLAYER_BOOTSTRAP_API_KEY="$API_KEY"

# Start server
"$SERVER" > /tmp/il-demo-server.log 2>&1 &
SERVER_PID=$!
cleanup() { kill "$SERVER_PID" 2>/dev/null; exit; }
trap cleanup INT TERM EXIT

echo "Waiting for server to start..."
for i in $(seq 1 30); do
    if curl -sf http://127.0.0.1:8080/health >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

if ! curl -sf http://127.0.0.1:8080/health >/dev/null 2>&1; then
    echo "ERROR: Server failed to start. Log:"
    cat /tmp/il-demo-server.log
    exit 1
fi

echo ""
echo "Seeding demo knowledge graphs..."

SEED_FAIL=0
for seed in "$SEEDS_DIR"/*.idl; do
    KG_NAME=$(basename "$seed" .idl)
    SEED_SCRIPT=$(mktemp /tmp/il-demo-seed.XXXXXX.idl)

    echo ".kg create $KG_NAME" > "$SEED_SCRIPT"
    echo ".kg use $KG_NAME" >> "$SEED_SCRIPT"
    python3 "$PREPROCESS" "$seed" >> "$SEED_SCRIPT"

    SEED_LOG=$(mktemp /tmp/il-demo-seedlog.XXXXXX)
    "$CLIENT" --api-key "$API_KEY" --limit 0 --script "$SEED_SCRIPT" > "$SEED_LOG" 2>&1 || true

    INSERTS=$(grep -c "Inserted" "$SEED_LOG" 2>/dev/null || echo 0)
    RULES=$(grep -c "registered" "$SEED_LOG" 2>/dev/null || echo 0)

    if [ "$INSERTS" -eq 0 ] && [ "$RULES" -eq 0 ]; then
        echo "  $KG_NAME: FAILED (no data loaded)"
        cat "$SEED_LOG"
        SEED_FAIL=1
    else
        echo "  $KG_NAME: $INSERTS facts, $RULES rules"
    fi

    rm -f "$SEED_SCRIPT" "$SEED_LOG"
done

if [ "$SEED_FAIL" -eq 1 ]; then
    echo ""
    echo "ERROR: Some knowledge graphs failed to seed."
    exit 1
fi

# Verify each KG has data
echo ""
echo "Verifying..."
VERIFY_FAIL=0
for kg in default flights incremental provenance retraction rules_vectors; do
    VER_SCRIPT=$(mktemp /tmp/il-demo-ver.XXXXXX.idl)
    echo ".kg use $kg" > "$VER_SCRIPT"
    echo ".rel" >> "$VER_SCRIPT"

    VER_LOG=$("$CLIENT" --api-key "$API_KEY" --limit 0 --script "$VER_SCRIPT" 2>&1)
    RELS=$(echo "$VER_LOG" | grep -c "tuples:" 2>/dev/null || echo 0)

    if [ "$RELS" -eq 0 ]; then
        echo "  $kg: EMPTY - no relations found"
        VERIFY_FAIL=1
    else
        echo "  $kg: ok ($RELS relations)"
    fi

    rm -f "$VER_SCRIPT"
done

if [ "$VERIFY_FAIL" -eq 1 ]; then
    echo ""
    echo "ERROR: Some knowledge graphs are empty after seeding."
    exit 1
fi

echo ""
echo "Demo ready at http://localhost:8080/"
echo "API key: $API_KEY"
echo "Knowledge graphs: default, flights, incremental, provenance, retraction, rules_vectors"
echo ""
echo "Press Ctrl+C to stop."

# Keep running - remove EXIT trap so server stays until INT/TERM
trap cleanup INT TERM
wait "$SERVER_PID"
