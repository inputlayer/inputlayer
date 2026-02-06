#!/bin/bash
# Snapshot Test Runner for InputLayer Datalog
# Compares actual output against expected .dl.out files
#
# Supports parallel execution (default) for speed and sequential
# mode for update/verbose/debugging.

# Don't exit on error - we handle errors ourselves
# set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
EXAMPLES_DIR="$PROJECT_DIR/examples/datalog"
SERVER_PORT="${INPUTLAYER_TEST_PORT:-8080}"
SERVER_URL="http://127.0.0.1:${SERVER_PORT}"

# Parallelism control (0 = sequential, N = N parallel jobs)
PARALLEL_JOBS=${INPUTLAYER_TEST_PARALLEL:-4}

# Colors for output (only when stdout is a terminal)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    CYAN='\033[0;36m'
    NC='\033[0m'
else
    RED=''
    GREEN=''
    YELLOW=''
    CYAN=''
    NC=''
fi

# Counters (used in sequential mode)
PASSED=0
FAILED=0
SKIPPED=0
UPDATED=0
DIRTY=0

# Mode flags
UPDATE_MODE=0

# Temporary directory for test outputs
TEMP_DIR=$(mktemp -d)

# Function to normalize output for comparison
# Strips timestamps, connection headers, compiler warnings, and blank lines
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
        grep -v "^\s*$" | \
        grep -v "^Compiling " | \
        grep -v "^   Compiling " | \
        grep -v "^Finished " | \
        grep -v "^   Finished " | \
        grep -v "^Running " | \
        grep -v "^   Running " | \
        sed -E 's/Created: [0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+-]+/Created: <timestamp>/g' | \
        sed -E 's/last_accessed: [0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+-]+/last_accessed: <timestamp>/g' | \
        sed -E 's/created_at: [0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+-]+/created_at: <timestamp>/g' | \
        sed 's/[[:space:]]*$//' # Trim trailing whitespace
}

# Function to check server health
check_server() {
    curl -s --max-time 5 "${SERVER_URL}/api/v1/knowledge-graphs" > /dev/null 2>&1
}

# Function to stop the running server
stop_server() {
    if [[ -n "$SERVER_PID" ]]; then
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
        SERVER_PID=""
    fi
}

# Function to start a fresh server
start_server() {
    # Clean up data folder for fresh start
    rm -rf "$PROJECT_DIR/data"

    # Start fresh server (direct binary, no cargo run overhead)
    "$SERVER_BIN" >/dev/null 2>&1 &
    SERVER_PID=$!

    # Wait for server to be ready (up to 15 seconds)
    for i in $(seq 1 30); do
        if check_server; then
            break
        fi
        sleep 0.5
    done

    if ! check_server; then
        echo -e "${RED}Server failed to start!${NC}"
        return 1
    fi
    echo "Server started (PID $SERVER_PID)"
    return 0
}

# Function to restart the server (stop + start)
restart_server() {
    echo ""
    echo -e "${CYAN}Restarting server to prevent resource exhaustion...${NC}"
    stop_server
    if ! start_server; then
        echo -e "${RED}Server restart failed! Aborting.${NC}"
        exit 1
    fi
}

# Assert that previous test cleaned up after itself (dirty state = test failure)
# Used only in sequential mode
assert_clean_state() {
    local prev_test="$1"
    local kgs=$(curl -s "${SERVER_URL}/api/v1/knowledge-graphs" 2>/dev/null \
        | grep -o '"name":"[^"]*"' | cut -d'"' -f4 | grep -v "^default$")

    if [[ -n "$kgs" ]]; then
        echo -e "${YELLOW}DIRTY${NC} [cleanup] Previous test left dirty state: $prev_test"
        echo "  Leaked knowledge graphs: $kgs"
        ((DIRTY++))

        # Force cleanup so the next test can proceed
        curl -s "${SERVER_URL}/api/v1/knowledge-graphs/default" > /dev/null 2>&1 || true
        for kg in $kgs; do
            curl -s -X DELETE "${SERVER_URL}/api/v1/knowledge-graphs/$kg" >/dev/null 2>&1 || true
        done
    fi
}

# Sequential test runner (used for --update, --verbose, or -j1)
run_test_sequential() {
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

    # Assert previous test cleaned up (marks FAILURE if not)
    if [[ -n "$LAST_TEST" ]]; then
        assert_clean_state "$LAST_TEST"
    fi
    LAST_TEST="[$category] $test_name"

    # Run the test and capture output (direct binary, no cargo run overhead)
    local stderr_file="$TEMP_DIR/${category}_${test_name}.stderr"
    local actual_output

    if [[ "${VERBOSE:-0}" == "1" ]]; then
        echo -e "${CYAN}CMD${NC} $CLIENT_BIN --script \"$test_file\""
    fi

    actual_output=$("$CLIENT_BIN" --script "$test_file" 2>"$stderr_file") || true

    # Show stderr on verbose if it contains real errors (not just warnings)
    if [[ -s "$stderr_file" ]] && [[ "${VERBOSE:-0}" == "1" ]]; then
        local real_errors=$(grep -v "^warning:" "$stderr_file" | grep -v "^\s*$" | head -5)
        if [[ -n "$real_errors" ]]; then
            echo -e "${YELLOW}STDERR${NC} [$category] $test_name:"
            echo "$real_errors"
        fi
    fi

    # In update mode, write the output to the expected file
    if [[ "$UPDATE_MODE" == "1" ]]; then
        printf '%s\n' "$actual_output" > "${expected_file}.tmp" && mv "${expected_file}.tmp" "$expected_file"
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

        # Always show diff on failure
        echo "  --- Diff (expected vs actual) ---"
        diff <(echo "$normalized_expected") <(echo "$normalized_actual") | head -50 || true

        if [[ "${VERBOSE:-0}" == "1" ]]; then
            echo "  --- Expected (full) ---"
            echo "$normalized_expected" | head -30
            echo "  --- Actual (full) ---"
            echo "$normalized_actual" | head -30
        fi

        # Show stderr if present
        if [[ -s "$stderr_file" ]]; then
            echo "  --- Stderr ---"
            head -10 "$stderr_file"
        fi
        echo ""
    fi
}

# Parallel test runner (default mode).
# Each test writes results to temp files, collected at the end.
run_test_parallel() {
    local test_file="$1"
    local expected_file="${test_file}.out"
    local test_name=$(basename "$test_file")
    local category=$(basename "$(dirname "$test_file")")
    local result_file="$TEMP_DIR/${category}__${test_name}.result"
    local output_file="$TEMP_DIR/${category}__${test_name}.output"
    local stderr_file="$TEMP_DIR/${category}__${test_name}.stderr"
    local diff_file="$TEMP_DIR/${category}__${test_name}.diff"

    # Skip tests without .out files
    if [[ ! -f "$expected_file" ]]; then
        echo "SKIP [$category] $test_name - no .out file" > "$result_file"
        return
    fi

    # Run the test (direct binary invocation)
    local actual_output
    actual_output=$("$CLIENT_BIN" --script "$test_file" 2>"$stderr_file") || true

    # Save raw output
    printf '%s\n' "$actual_output" > "$output_file"

    # Normalize both outputs
    local normalized_actual=$(normalize_output "$actual_output")
    local normalized_expected=$(normalize_output "$(cat "$expected_file")")

    # Compare
    if [[ "$normalized_actual" == "$normalized_expected" ]]; then
        echo "PASS [$category] $test_name" > "$result_file"
    else
        echo "FAIL [$category] $test_name" > "$result_file"
        diff <(echo "$normalized_expected") <(echo "$normalized_actual") > "$diff_file" 2>/dev/null || true
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
        -j|--jobs)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -v, --verbose    Show diff output on failures (forces sequential)"
            echo "  -f, --filter     Only run tests matching pattern (e.g., 'negation')"
            echo "  -u, --update     Update .out files with actual output (forces sequential)"
            echo "  -j, --jobs N     Parallel jobs (default: $PARALLEL_JOBS, 0 or 1 = sequential)"
            echo "  -h, --help       Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  INPUTLAYER_TEST_PARALLEL  Set parallel job count (default: 8)"
            echo "  INPUTLAYER_TEST_PORT      Server port (default: 8080)"
            echo "  INPUTLAYER_RESTART_INTERVAL  Server restart interval (default: 500)"
            echo ""
            echo "Examples:"
            echo "  $0                    # Run all tests in parallel"
            echo "  $0 -v                 # Run all tests sequentially with verbose output"
            echo "  $0 -f negation        # Run only negation tests in parallel"
            echo "  $0 -j 1              # Run all tests sequentially"
            echo "  $0 --update           # Regenerate all snapshot files (sequential)"
            echo "  $0 -u -f recursion    # Regenerate only recursion test snapshots"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Force sequential mode for update and verbose modes
if [[ "$UPDATE_MODE" == "1" ]] || [[ "$VERBOSE" == "1" ]]; then
    PARALLEL_JOBS=0
fi

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

# Detect the cargo target directory (handles workspace layouts)
TARGET_DIR=$(cargo metadata --format-version 1 --no-deps 2>/dev/null \
    | grep -o '"target_directory":"[^"]*"' | cut -d'"' -f4)
if [[ -z "$TARGET_DIR" ]]; then
    TARGET_DIR="$PROJECT_DIR/target"
fi

# Use direct binary paths (eliminates ~270ms cargo run overhead per invocation)
CLIENT_BIN="$TARGET_DIR/release/inputlayer-client"
SERVER_BIN="$TARGET_DIR/release/inputlayer-server"

if [[ ! -x "$CLIENT_BIN" ]] || [[ ! -x "$SERVER_BIN" ]]; then
    echo -e "${RED}Binaries not found after build!${NC}"
    echo "  Client: $CLIENT_BIN"
    echo "  Server: $SERVER_BIN"
    exit 1
fi

# Always start a fresh server for snapshot tests to ensure reproducibility
SERVER_PID=""

# Stop any existing server on the test port
if check_server; then
    echo "Stopping existing server..."
    pkill -f "inputlayer-server.*${SERVER_PORT}" 2>/dev/null || true
    for i in $(seq 1 10); do
        if ! check_server; then
            break
        fi
        sleep 0.5
    done
fi

echo "Starting server..."
if ! start_server; then
    exit 1
fi

# Cleanup function to stop server on exit
cleanup() {
    rm -rf "$TEMP_DIR"
    stop_server
}
trap cleanup EXIT

echo ""
echo "========================================"
if [[ "$UPDATE_MODE" == "1" ]]; then
    echo "  InputLayer Snapshot Update Mode"
elif [[ "$PARALLEL_JOBS" -gt 1 ]]; then
    echo "  InputLayer Snapshot Tests (parallel: $PARALLEL_JOBS jobs)"
else
    echo "  InputLayer Snapshot Tests (sequential)"
fi
echo "========================================"
echo ""

# Find all test files (exclude files starting with _ which are helpers)
# Also exclude _pending_* tests which are for features not yet implemented
TEST_FILES=$(find "$EXAMPLES_DIR" -name "*.dl" -type f ! -name "_*" ! -name "*_pending_*" | sort)

# Apply filter if specified
if [[ -n "$FILTER" ]]; then
    TEST_FILES=$(echo "$TEST_FILES" | grep "$FILTER")
fi

if [[ -z "$TEST_FILES" ]]; then
    TEST_TOTAL=0
else
    TEST_TOTAL=$(echo "$TEST_FILES" | wc -l | tr -d ' ')
fi

# Count pending tests for reporting
PENDING_COUNT=$(find "$EXAMPLES_DIR" -name "*_pending_*.dl" -type f | wc -l | tr -d ' ')

# Run tests
if [[ "$PARALLEL_JOBS" -le 1 ]]; then
    # --- Sequential mode ---
    LAST_TEST=""
    HEALTH_CHECK_INTERVAL=100
    SERVER_RESTART_INTERVAL=${INPUTLAYER_RESTART_INTERVAL:-500}
    TEST_COUNT=0

    for test_file in $TEST_FILES; do
        TEST_COUNT=$((TEST_COUNT + 1))

        # Periodic server restart to prevent resource exhaustion
        if [[ $((TEST_COUNT % SERVER_RESTART_INTERVAL)) -eq 0 ]]; then
            restart_server
        elif [[ $((TEST_COUNT % HEALTH_CHECK_INTERVAL)) -eq 0 ]]; then
            if ! check_server; then
                echo -e "${YELLOW}Server health check failed after $TEST_COUNT tests, restarting...${NC}"
                restart_server
            fi
        fi

        run_test_sequential "$test_file"
    done

    # Final dirty-state check for the last test
    if [[ -n "$LAST_TEST" ]]; then
        assert_clean_state "$LAST_TEST"
    fi
else
    # --- Parallel mode ---

    # Separate tests into parallel-safe and sequential-only
    # Tests using .kg list see other parallel tests' KGs and must run sequentially
    PARALLEL_FILE="$TEMP_DIR/parallel_tests.txt"
    SEQUENTIAL_FILE="$TEMP_DIR/sequential_tests.txt"

    for f in $TEST_FILES; do
        if grep -q '\.kg list' "$f" 2>/dev/null; then
            echo "$f" >> "$SEQUENTIAL_FILE"
        else
            echo "$f" >> "$PARALLEL_FILE"
        fi
    done

    # Shuffle parallel tests to minimize KG name collisions
    # (same-category tests have similar KG names; spreading them out reduces collision risk)
    if [[ -f "$PARALLEL_FILE" ]]; then
        PARALLEL_COUNT=$(wc -l < "$PARALLEL_FILE" | tr -d ' ')
        sort -R "$PARALLEL_FILE" > "${PARALLEL_FILE}.shuffled"
        mv "${PARALLEL_FILE}.shuffled" "$PARALLEL_FILE"
    else
        PARALLEL_COUNT=0
    fi
    SEQUENTIAL_COUNT=0
    [[ -f "$SEQUENTIAL_FILE" ]] && SEQUENTIAL_COUNT=$(wc -l < "$SEQUENTIAL_FILE" | tr -d ' ')

    echo "Running $PARALLEL_COUNT tests in parallel, $SEQUENTIAL_COUNT sequentially (.kg list tests)..."
    echo ""

    # Run parallel batch
    if [[ -f "$PARALLEL_FILE" ]] && [[ "$PARALLEL_COUNT" -gt 0 ]]; then
        # Export functions and variables for xargs subprocesses
        export -f run_test_parallel normalize_output
        export CLIENT_BIN TEMP_DIR SERVER_URL

        cat "$PARALLEL_FILE" | xargs -P"$PARALLEL_JOBS" -I{} bash -c 'run_test_parallel "{}"'
    fi

    # Run sequential batch (tests that use .kg list)
    if [[ -f "$SEQUENTIAL_FILE" ]] && [[ "$SEQUENTIAL_COUNT" -gt 0 ]]; then
        echo ""
        echo "Running $SEQUENTIAL_COUNT sequential tests (.kg list)..."
        LAST_TEST=""
        for test_file in $(cat "$SEQUENTIAL_FILE"); do
            # Wait for parallel tests to finish their KG cleanup
            if [[ -z "$LAST_TEST" ]]; then
                # Clean up leaked KGs from parallel batch before sequential tests
                kgs_to_clean=$(curl -s "${SERVER_URL}/api/v1/knowledge-graphs" 2>/dev/null \
                    | grep -o '"name":"[^"]*"' | cut -d'"' -f4 | grep -v "^default$")
                for kg in $kgs_to_clean; do
                    curl -s -X DELETE "${SERVER_URL}/api/v1/knowledge-graphs/$kg" >/dev/null 2>&1 || true
                done
            fi
            run_test_sequential "$test_file"
        done
        if [[ -n "$LAST_TEST" ]]; then
            assert_clean_state "$LAST_TEST"
        fi
    fi

    # Collect results from temp files
    for result_file in "$TEMP_DIR"/*.result; do
        [[ -f "$result_file" ]] || continue
        local_result=$(cat "$result_file")

        if [[ "$local_result" == PASS* ]]; then
            echo -e "${GREEN}${local_result}${NC}"
            ((PASSED++))
        elif [[ "$local_result" == FAIL* ]]; then
            echo -e "${RED}${local_result}${NC}"
            ((FAILED++))

            # Show diff for failures
            diff_file="${result_file%.result}.diff"
            if [[ -s "$diff_file" ]]; then
                echo "  --- Diff (expected vs actual) ---"
                head -30 "$diff_file"
                echo ""
            fi
        elif [[ "$local_result" == SKIP* ]]; then
            echo -e "${YELLOW}${local_result}${NC}"
            ((SKIPPED++))
        fi
    done

    # Post-run dirty-state check (parallel mode can't check per-test)
    final_kgs=$(curl -s "${SERVER_URL}/api/v1/knowledge-graphs" 2>/dev/null \
        | grep -o '"name":"[^"]*"' | cut -d'"' -f4 | grep -v "^default$")

    if [[ -n "$final_kgs" ]]; then
        echo ""
        echo -e "${YELLOW}WARNING: Tests left dirty state after parallel run${NC}"
        echo "  Leaked knowledge graphs: $final_kgs"
        # Clean up
        curl -s "${SERVER_URL}/api/v1/knowledge-graphs/default" > /dev/null 2>&1 || true
        for kg in $final_kgs; do
            curl -s -X DELETE "${SERVER_URL}/api/v1/knowledge-graphs/$kg" >/dev/null 2>&1 || true
        done
    fi
fi

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
    if [[ "$DIRTY" -gt 0 ]]; then
        echo -e "Dirty:   ${YELLOW}$DIRTY${NC} (tests that didn't clean up their KG)"
    fi
    if [[ "$PENDING_COUNT" -gt 0 ]]; then
        echo -e "Pending: ${CYAN}$PENDING_COUNT${NC} (features not yet implemented)"
    fi
    echo ""

    if [[ $FAILED -gt 0 ]]; then
        echo -e "${RED}Some tests failed!${NC}"
        echo "Run with -v for detailed diff output"
        exit 1
    else
        echo -e "${GREEN}All tests passed!${NC}"
    fi
fi
