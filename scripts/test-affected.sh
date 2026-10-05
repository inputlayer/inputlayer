#!/bin/bash
# Run only snapshot tests affected by source code changes.
# The map from changed files to test categories lives in
# run_snapshot_tests.sh (--affected); extra arguments pass through to it.
#
# Usage:
#   ./scripts/test-affected.sh          # Changes since HEAD (uncommitted)
#   ./scripts/test-affected.sh HEAD~3   # Changes in last 3 commits
#   ./scripts/test-affected.sh main     # Changes since main branch

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REF="${1:-HEAD}"
shift

exec "$SCRIPT_DIR/run_snapshot_tests.sh" --affected "$REF" "$@"
