#!/usr/bin/env bash
# Start the ingestion stack from scratch and run the self-checking demo:
# Postgres row changes and billing webhooks update a derived answer, and a
# watching client receives each change. Leaves the stack running; stop it with
# `docker compose down -v`.
set -euo pipefail
cd "$(dirname "$0")"

docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose build adapter
docker compose up -d --wait inputlayer postgres adapter debezium
docker compose run --rm --no-deps demo
