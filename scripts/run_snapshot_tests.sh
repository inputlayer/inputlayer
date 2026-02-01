#!/bin/bash
# Snapshot Test Runner for InputLayer Datalog
# Compares actual output against expected .dl.out files

# Don't exit on error - we handle errors ourselves
# set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
EXAMPLES_DIR="$PROJECT_DIR/examples/datalog"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Counters
PASSED=0
FAILED=0
SKIPPED=0
UPDATED=0

# Mode flags
UPDATE_MODE=0

# Temporary directory for test outputs
TEMP_DIR=$(mktemp -d)

# Function to normalize output for comparison
# - Strips timestamps (e.g., "Created: 2025-12-04T...")
# - Strips "Executing script:" lines
# - Strips Rust compiler warnings
# - Strips connection header lines (server state dependent)
# - Normalizes whitespace
normalize_output() {
    local input="$1"
    echo "$input" | \
        grep -v "^Executing script:" | \
        grep -v "^Connecting to server" | \
        grep -v "^Connected!" | \
        grep -v "^Server status:" | \
        grep -v "^Current knowledge graph:" | \
        grep -v "^warning:" | \
        grep -v "^   -->" | \
        grep -v "^    |" | \
        grep -v "^   = note:" | \
        grep -v "^   = help:" | \
        grep -v "^$" | \
        grep -v "^\s*$" | \
        grep -v "Compiling" | \
        grep -v "Finished" | \
        grep -v "Running" | \
        sed -E 's/Created: [0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+-]+/Created: <timestamp>/g' | \
        sed -E 's/last_accessed: [0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+-]+/last_accessed: <timestamp>/g' | \
        sed -E 's/created_at: [0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+-]+/created_at: <timestamp>/g' | \
        sed 's/[[:space:]]*$//' # Trim trailing whitespace
}

# Function to clean knowledge graph state before each test
clean_state() {
    # Switch to default KG first (so we can delete test KGs that might be active)
    curl -s "http://127.0.0.1:8080/api/v1/knowledge-graphs/default" > /dev/null 2>&1 || true

    # Delete all non-default knowledge graphs via API
    local kgs=$(curl -s http://127.0.0.1:8080/api/v1/knowledge-graphs 2>/dev/null | grep -o '"name":"[^"]*"' | cut -d'"' -f4)
    for kg in $kgs; do
        if [[ "$kg" != "default" ]]; then
            curl -s -X DELETE "http://127.0.0.1:8080/api/v1/knowledge-graphs/$kg" >/dev/null 2>&1 || true
        fi
    done

    # Remove test knowledge graph directories from disk
    find "$PROJECT_DIR/data" -maxdepth 1 -type d -name "test_*" -exec rm -rf {} \; 2>/dev/null || true

    # Clean ALL persist state completely - delete all files in these directories
    rm -f "$PROJECT_DIR/data/persist/shards/"* 2>/dev/null || true
    rm -f "$PROJECT_DIR/data/persist/wal/"* 2>/dev/null || true
    rm -f "$PROJECT_DIR/data/persist/batches/"* 2>/dev/null || true

    # Also clean any view catalog files that might be persisted
    find "$PROJECT_DIR/data" -type f -name "views.json" -delete 2>/dev/null || true

    # Reset metadata to only have default knowledge graph
    cat > "$PROJECT_DIR/data/metadata/knowledge_graphs.json" << 'EOF'
{
  "version": "1.0",
  "knowledge_graphs": [
    {
      "name": "default",
      "created_at": "2025-12-04T12:05:31.328858+00:00",
      "last_accessed": "2025-12-04T12:05:31.329088+00:00",
      "relations_count": 0,
      "total_tuples": 0
    }
  ]
}
EOF
}

# Function to run a single test
run_test() {
    local test_file="$1"
    local expected_file="${test_file}.out"
    local test_name=$(basename "$test_file")
    local category=$(basename "$(dirname "$test_file")")

    # In update mode, we don't skip tests without .out files - we create them
    if [[ "$UPDATE_MODE" != "1" ]] && [[ ! -f "$expected_file" ]]; then
        echo -e "${YELLOW}SKIP${NC} [$category] $test_name - no .out file"
        ((SKIPPED++))
        return
    fi

    # Clean server state before each test to ensure idempotency
    clean_state

    # Run the test and capture output (stderr to /dev/null to skip warnings)
    local actual_output
    local cmd="cargo run --bin inputlayer-client --release --quiet -- --script \"$test_file\""

    if [[ "${VERBOSE:-0}" == "1" ]]; then
        echo -e "${CYAN}CMD${NC} $cmd"
    fi

    actual_output=$(cd "$PROJECT_DIR" && cargo run --bin inputlayer-client --release --quiet -- --script "$test_file" 2>/dev/null) || true

    # In update mode, write the output to the expected file
    if [[ "$UPDATE_MODE" == "1" ]]; then
        echo "$actual_output" > "$expected_file"
        echo -e "${CYAN}UPDATED${NC} [$category] $test_name"
        ((UPDATED++))
        return
    fi

    # Normalize both outputs
    local normalized_actual=$(normalize_output "$actual_output")
    local normalized_expected=$(normalize_output "$(cat "$expected_file")")

    # Compare snapshots
    if [[ "$normalized_actual" == "$normalized_expected" ]]; then
        echo -e "${GREEN}PASS${NC} [$category] $test_name"
        ((PASSED++))
    else
        echo -e "${RED}FAIL${NC} [$category] $test_name"
        ((FAILED++))

        # Show diff if verbose
        if [[ "${VERBOSE:-0}" == "1" ]]; then
            echo "--- Expected ---"
            echo "$normalized_expected" | head -20
            echo "--- Actual ---"
            echo "$normalized_actual" | head -20
            echo "--- Diff ---"
            diff <(echo "$normalized_expected") <(echo "$normalized_actual") | head -30 || true
            echo ""
        fi
    fi
}

# Parse arguments
VERBOSE=0
FILTER=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=1
            shift
            ;;
        -f|--filter)
            FILTER="$2"
            shift 2
            ;;
        -u|--update)
            UPDATE_MODE=1
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -v, --verbose    Show diff output on failures"
            echo "  -f, --filter     Only run tests matching pattern (e.g., 'negation')"
            echo "  -u, --update     Update .out files with actual output (regenerate snapshots)"
            echo "  -h, --help       Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                    # Run all tests"
            echo "  $0 -v                 # Run all tests with verbose output"
            echo "  $0 -f negation        # Run only negation tests"
            echo "  $0 --update           # Regenerate all snapshot files"
            echo "  $0 -u -f recursion    # Regenerate only recursion test snapshots"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Build the project first (release mode for speed, suppresses warnings)
echo "Building project..."
cd "$PROJECT_DIR"
if ! cargo build --bin inputlayer-client --bin inputlayer-server --release --quiet 2>/dev/null; then
    echo -e "${RED}Build failed!${NC}"
    echo "Attempting verbose build to show errors:"
    cargo build --bin inputlayer-client --bin inputlayer-server --release 2>&1 | grep -v "^warning" || true
    echo -e "${RED}Aborting tests due to build failure.${NC}"
    exit 1
fi

# Ensure server is running - start one if needed
SERVER_STARTED=0
if ! curl -s http://127.0.0.1:8080/api/v1/knowledge-graphs > /dev/null 2>&1; then
    echo "Starting server..."
    rm -rf "$PROJECT_DIR/data"
    cargo run --bin inputlayer-server --release --quiet 2>/dev/null &
    SERVER_PID=$!
    SERVER_STARTED=1

    # Wait for server to be ready (up to 15 seconds)
    for i in $(seq 1 30); do
        if curl -s http://127.0.0.1:8080/api/v1/knowledge-graphs > /dev/null 2>&1; then
            break
        fi
        sleep 0.5
    done

    if ! curl -s http://127.0.0.1:8080/api/v1/knowledge-graphs > /dev/null 2>&1; then
        echo -e "${RED}Server failed to start!${NC}"
        kill $SERVER_PID 2>/dev/null || true
        exit 1
    fi
    echo "Server started (PID $SERVER_PID)"
fi

# Cleanup function to stop server if we started it
cleanup() {
    rm -rf "$TEMP_DIR"
    if [[ "$SERVER_STARTED" == "1" ]] && [[ -n "$SERVER_PID" ]]; then
        echo "Stopping server (PID $SERVER_PID)..."
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo ""
echo "========================================"
if [[ "$UPDATE_MODE" == "1" ]]; then
    echo "  InputLayer Snapshot Update Mode"
else
    echo "  InputLayer Snapshot Tests"
fi
echo "========================================"
echo ""

# Find all test files (exclude files starting with _ which are helpers)
TEST_FILES=$(find "$EXAMPLES_DIR" -name "*.dl" -type f ! -name "_*" | sort)

for test_file in $TEST_FILES; do
    # Apply filter if specified
    if [[ -n "$FILTER" ]] && [[ ! "$test_file" == *"$FILTER"* ]]; then
        continue
    fi

    run_test "$test_file"
done

# Summary
echo ""
echo "========================================"
echo "  Summary"
echo "========================================"

if [[ "$UPDATE_MODE" == "1" ]]; then
    echo -e "Updated: ${CYAN}$UPDATED${NC}"
    echo -e "Skipped: ${YELLOW}$SKIPPED${NC}"
    echo ""
    echo -e "${GREEN}Snapshot files updated successfully!${NC}"
    echo "Run without --update to verify tests pass."
else
    echo -e "Passed:  ${GREEN}$PASSED${NC}"
    echo -e "Failed:  ${RED}$FAILED${NC}"
    echo -e "Skipped: ${YELLOW}$SKIPPED${NC}"
    echo ""

    if [[ $FAILED -gt 0 ]]; then
        echo -e "${RED}Some tests failed!${NC}"
        echo "Run with -v for detailed diff output"
        exit 1
    else
        echo -e "${GREEN}All tests passed!${NC}"
    fi
fi
