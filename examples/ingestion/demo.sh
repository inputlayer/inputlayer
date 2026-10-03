#!/usr/bin/env bash
# Start the ingestion stack from scratch and run the self-checking demo:
# Postgres row changes and billing webhooks update a derived answer, and a
# watching client receives each change. Leaves the stack running; stop it with
# `docker compose down -v`.
set -euo pipefail
cd "$(dirname "$0")"

INGEST_ENGINE_API_KEY="$(openssl rand -hex 32)"
INGEST_CDC_WEBHOOK_SECRET="whsec_$(openssl rand -base64 32)"
INGEST_WEBHOOK_SECRET_BILLING="whsec_$(openssl rand -base64 32)"
INGEST_POSTGRES_PASSWORD="$(openssl rand -hex 32)"
export INGEST_ENGINE_API_KEY INGEST_CDC_WEBHOOK_SECRET
export INGEST_WEBHOOK_SECRET_BILLING INGEST_POSTGRES_PASSWORD
secret_file=$(mktemp ./.env.XXXXXX)
printf '%s\n' \
  "INGEST_ENGINE_API_KEY=$INGEST_ENGINE_API_KEY" \
  "INGEST_CDC_WEBHOOK_SECRET=$INGEST_CDC_WEBHOOK_SECRET" \
  "INGEST_WEBHOOK_SECRET_BILLING=$INGEST_WEBHOOK_SECRET_BILLING" \
  "INGEST_POSTGRES_PASSWORD=$INGEST_POSTGRES_PASSWORD" > "$secret_file"
mv "$secret_file" .env

docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose build adapter
docker compose up -d --wait inputlayer postgres adapter debezium
docker compose run --rm --no-deps demo
