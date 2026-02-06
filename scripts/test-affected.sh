#!/bin/bash
# Run only snapshot tests affected by source code changes.
# Maps changed source files to test categories and runs only those.
#
# Usage:
#   ./scripts/test-affected.sh          # Changes since HEAD (uncommitted)
#   ./scripts/test-affected.sh HEAD~3   # Changes in last 3 commits
#   ./scripts/test-affected.sh main     # Changes since main branch

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
REF="${1:-HEAD}"

# Get changed files
if [ "$REF" = "HEAD" ]; then
    # Uncommitted changes (staged + unstaged)
    CHANGED=$(cd "$PROJECT_DIR" && git diff --name-only HEAD 2>/dev/null; git diff --name-only --cached 2>/dev/null)
else
    CHANGED=$(cd "$PROJECT_DIR" && git diff --name-only "$REF")
fi

if [ -z "$CHANGED" ]; then
    echo "No changes detected."
    exit 0
fi

# Map source files to test categories
CATEGORIES=""
RUN_ALL=0

for file in $CHANGED; do
    case "$file" in
        src/join_planning/*|src/sip_rewriting/*)
            CATEGORIES="$CATEGORIES 06_joins 80_sip" ;;
        src/recursion.rs|src/code_generator/*)
            CATEGORIES="$CATEGORIES 09_recursion 18_advanced_patterns" ;;
        src/ir_builder/*)
            CATEGORIES="$CATEGORIES 06_joins 07_filters 08_negation 14_aggregations 10_edge_cases" ;;
        src/parser/*|src/statement/*)
            CATEGORIES="$CATEGORIES 12_errors 17_rule_commands 28_docs_coverage 33_meta 39_meta_complete" ;;
        src/vector_ops.rs|src/hnsw_index.rs)
            CATEGORIES="$CATEGORIES 16_vectors 30_quantization 31_lsh" ;;
        src/temporal_ops.rs)
            CATEGORIES="$CATEGORIES 29_temporal" ;;
        src/storage*|src/protocol/*)
            CATEGORIES="$CATEGORIES 01_knowledge_graph 04_session 40_load_command" ;;
        src/value/*|src/ir/*)
            # Core changes affect everything
            RUN_ALL=1 ;;
        src/lib.rs|src/config.rs)
            RUN_ALL=1 ;;
        src/optimizer/*|src/boolean_specialization/*)
            CATEGORIES="$CATEGORIES 09_recursion 06_joins 18_advanced_patterns" ;;
        src/subplan_sharing/*)
            CATEGORIES="$CATEGORIES 06_joins 09_recursion 18_advanced_patterns" ;;
        examples/datalog/*)
            # Test file changed - run that specific category
            category=$(echo "$file" | sed 's|examples/datalog/\([^/]*\)/.*|\1|')
            CATEGORIES="$CATEGORIES $category" ;;
        scripts/run_snapshot_tests.sh)
            # Test runner changed - run all
            RUN_ALL=1 ;;
    esac
done

if [ "$RUN_ALL" -eq 1 ]; then
    echo "Core files changed  - running all snapshot tests."
    exec "$SCRIPT_DIR/run_snapshot_tests.sh"
fi

# Deduplicate categories
CATEGORIES=$(echo "$CATEGORIES" | tr ' ' '\n' | sort -u | tr '\n' ' ')

if [ -z "$(echo "$CATEGORIES" | tr -d ' ')" ]; then
    echo "No test-relevant changes detected."
    exit 0
fi

echo "Changed source files affect these test categories:"
for cat in $CATEGORIES; do
    echo "  - $cat"
done
echo ""

# Build filter pattern (grep OR of categories)
FILTER=$(echo "$CATEGORIES" | tr ' ' '\n' | grep -v '^$' | paste -sd'|' -)

# Run affected tests
exec "$SCRIPT_DIR/run_snapshot_tests.sh" -f "$FILTER"
