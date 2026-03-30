#!/usr/bin/env bash
#
# Deploy the InputLayer demo stack (InputLayer + demo gateway + Caddy)
# on demo.inputlayer.ai with automatic TLS.
#
# Usage:
#   ./scripts/demo-deploy.sh          # Start the stack
#   ./scripts/demo-deploy.sh stop     # Stop the stack
#   ./scripts/demo-deploy.sh logs     # Tail logs
#   ./scripts/demo-deploy.sh status   # Show service status
#   ./scripts/demo-deploy.sh rebuild  # Rebuild and restart demo-gateway
#   ./scripts/demo-deploy.sh reseed   # Force re-seed all demo KGs
#   ./scripts/demo-deploy.sh reset    # Wipe all data and start fresh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEMO_DIR="$PROJECT_ROOT/demo"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()   { echo -e "${CYAN}[demo]${NC} $*"; }
ok()    { echo -e "${GREEN}[demo]${NC} $*"; }
warn()  { echo -e "${YELLOW}[demo]${NC} $*"; }
err()   { echo -e "${RED}[demo]${NC} $*" >&2; }

# ── Prerequisites ────────────────────────────────────────────────────────

check_prereqs() {
  local missing=0

  if ! command -v docker &>/dev/null; then
    err "docker is not installed"
    missing=1
  fi

  if ! docker compose version &>/dev/null 2>&1; then
    err "docker compose (v2) is not available"
    missing=1
  fi

  if [ $missing -ne 0 ]; then
    err "install missing dependencies and try again"
    exit 1
  fi
}

# ── .env setup ───────────────────────────────────────────────────────────

ensure_env() {
  local env_file="$DEMO_DIR/.env"

  if [ ! -f "$env_file" ]; then
    log "no .env found - creating from .env.example"
    cp "$DEMO_DIR/.env.example" "$env_file"

    # Generate a random admin password if not set
    local pw
    pw=$(openssl rand -base64 24 | tr -d '/+=' | head -c 32)
    if grep -q '^INPUTLAYER_ADMIN_PASSWORD=$' "$env_file"; then
      if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "s/^INPUTLAYER_ADMIN_PASSWORD=$/INPUTLAYER_ADMIN_PASSWORD=${pw}/" "$env_file"
      else
        sed -i "s/^INPUTLAYER_ADMIN_PASSWORD=$/INPUTLAYER_ADMIN_PASSWORD=${pw}/" "$env_file"
      fi
      ok "generated admin password (saved in .env)"
    fi

    warn "edit $env_file to configure DOMAIN and SMTP settings"
    warn "then re-run this script"
    exit 0
  fi

  # Validate required vars
  source "$env_file"
  if [ -z "${INPUTLAYER_ADMIN_PASSWORD:-}" ]; then
    err "INPUTLAYER_ADMIN_PASSWORD is not set in $env_file"
    exit 1
  fi
  if [ -z "${DOMAIN:-}" ]; then
    warn "DOMAIN is not set - defaulting to localhost (no TLS)"
  fi
}

# ── Compose helpers ──────────────────────────────────────────────────────

compose() {
  docker compose -f "$DEMO_DIR/docker-compose.yml" --env-file "$DEMO_DIR/.env" --project-name inputlayer-demo "$@"
}

wait_healthy() {
  local service=$1
  local timeout=${2:-120}
  local elapsed=0

  log "waiting for $service to be healthy..."
  while [ $elapsed -lt $timeout ]; do
    local health
    health=$(docker inspect --format='{{.State.Health.Status}}' "$(compose ps -q "$service" 2>/dev/null)" 2>/dev/null || echo "missing")

    case "$health" in
      healthy)
        ok "$service is healthy"
        return 0
        ;;
      unhealthy)
        err "$service is unhealthy"
        compose logs --tail=20 "$service"
        return 1
        ;;
    esac

    sleep 2
    elapsed=$((elapsed + 2))
  done

  err "$service did not become healthy within ${timeout}s"
  compose logs --tail=20 "$service"
  return 1
}

# ── Commands ─────────────────────────────────────────────────────────────

cmd_start() {
  check_prereqs
  ensure_env

  log "building demo-gateway..."
  compose build demo-gateway

  log "starting stack..."
  compose up -d

  wait_healthy inputlayer
  wait_healthy demo-gateway

  echo ""
  ok "demo stack is running"

  local domain
  domain=$(grep '^DOMAIN=' "$DEMO_DIR/.env" 2>/dev/null | cut -d= -f2)
  domain=${domain:-localhost}

  echo ""
  echo "  Studio:         https://$domain/query"
  echo "  Request access: https://$domain/demo/request-access"
  echo "  Health check:   https://$domain/demo/api/health"
  echo ""
  echo "  Logs:           $0 logs"
  echo "  Status:         $0 status"
  echo "  Stop:           $0 stop"
  echo ""
}

cmd_stop() {
  log "stopping stack..."
  compose down
  ok "stack stopped"
}

cmd_logs() {
  compose logs -f --tail=50 "$@"
}

cmd_status() {
  compose ps
}

cmd_rebuild() {
  check_prereqs
  ensure_env

  log "rebuilding demo-gateway..."
  compose build --no-cache demo-gateway
  compose up -d demo-gateway
  wait_healthy demo-gateway
  ok "demo-gateway rebuilt and restarted"
}

cmd_reseed() {
  check_prereqs
  ensure_env

  log "re-seeding demo knowledge graphs..."
  log "this will stop the stack, wipe InputLayer data, and re-seed from scratch"

  read -r -p "continue? [y/N] " confirm
  if [[ "$confirm" != [yY] ]]; then
    log "aborted"
    exit 0
  fi

  # Stop everything, wipe InputLayer data (but keep demo invites and Caddy certs)
  compose stop
  docker volume rm inputlayer-demo_inputlayer-data 2>/dev/null || true

  # Restart - the seeder will detect empty KGs and populate them
  compose up -d
  wait_healthy inputlayer
  wait_healthy demo-gateway
  ok "re-seed complete - all demo KGs freshly populated"
}

cmd_reset() {
  check_prereqs
  ensure_env

  warn "this will destroy ALL data (InputLayer + demo invites) and start fresh"
  read -r -p "are you sure? [y/N] " confirm
  if [[ "$confirm" != [yY] ]]; then
    log "aborted"
    exit 0
  fi

  log "stopping stack..."
  compose down -v
  ok "volumes removed"

  cmd_start
}

# ── Main ─────────────────────────────────────────────────────────────────

case "${1:-start}" in
  start)   cmd_start ;;
  stop)    cmd_stop ;;
  logs)    shift; cmd_logs "$@" ;;
  status)  cmd_status ;;
  rebuild) cmd_rebuild ;;
  reseed)  cmd_reseed ;;
  reset)   cmd_reset ;;
  *)
    echo "Usage: $0 {start|stop|logs|status|rebuild|reseed|reset}"
    exit 1
    ;;
esac
