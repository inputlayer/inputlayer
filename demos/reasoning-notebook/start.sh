#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
SERVER_BIN="$REPO_ROOT/target/release/inputlayer-server"

cleanup() {
    echo ""
    echo "Shutting down..."
    kill "$SERVER_PID" "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    echo "All processes stopped."
}
trap cleanup EXIT

if [ ! -f "$SERVER_BIN" ]; then
    echo "InputLayer server not found at $SERVER_BIN"
    echo "Build it with: cargo build --release --bin inputlayer-server"
    exit 1
fi

echo "Starting InputLayer server..."
"$SERVER_BIN" &
SERVER_PID=$!

sleep 1

echo "Starting FastAPI backend..."
cd "$ROOT/backend"
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload &
BACKEND_PID=$!

echo "Starting Vite frontend..."
cd "$ROOT/frontend"
bun run dev &
FRONTEND_PID=$!

echo ""
echo "==================================="
echo "  Reasoning Notebook is running"
echo "  Frontend:  http://localhost:5173"
echo "  Backend:   http://localhost:8000"
echo "  Engine:    ws://localhost:8080/ws"
echo "==================================="
echo ""

wait
