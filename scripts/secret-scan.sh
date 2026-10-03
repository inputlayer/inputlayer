#!/usr/bin/env bash
# Scan for committed credentials with gitleaks under .gitleaks.toml.
#
# Usage: scripts/secret-scan.sh [--staged]
#   (default)  every commit reachable from HEAD (`make secret-check`, CI)
#   --staged   only the staged changes (the pre-commit hook)
#
# Uses target/tools/gitleaks (`make install-gitleaks`) or gitleaks on PATH.
# Findings are printed redacted. Exit status: 0 clean, 1 leaks found,
# 2 gitleaks missing.
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

case "${1:-}" in
    "") SCOPE=(--log-opts="--full-history --no-renames HEAD") ;;
    --staged) SCOPE=(--staged) ;;
    -h | --help) sed -n '2,10p' "$0"; exit 0 ;;
    *) echo "secret-scan: unknown option $1" >&2; exit 2 ;;
esac

if [ -x target/tools/gitleaks ]; then
    GITLEAKS=target/tools/gitleaks
elif command -v gitleaks >/dev/null 2>&1; then
    GITLEAKS=gitleaks
else
    echo "secret-scan: gitleaks not found; run 'make install-gitleaks'" >&2
    exit 2
fi

"$GITLEAKS" git . --config .gitleaks.toml --redact --verbose --no-banner --log-level warn "${SCOPE[@]}"
echo "secret-scan: no leaks found"
