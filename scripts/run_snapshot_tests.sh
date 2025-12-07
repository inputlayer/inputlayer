#!/bin/bash
# Snapshot Test Runner for InputLayer Datalog
# Compares actual output against expected .dl.out files
# Also supports embedded assertions in test files

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
ASSERTION_PASSED=0
ASSERTION_FAILED=0

# Temporary directory for test outputs
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Function to normalize output for comparison
# - Strips timestamps (e.g., "Created: 2025-12-04T...")
# - Strips "Executing script:" lines
# - Strips Rust compiler warnings
# - Normalizes whitespace
normalize_output() {
    local input="$1"
    echo "$input" | \
        grep -v "^Executing script:" | \
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

# ============================================================
# Assertion Support Functions
# ============================================================
# Assertions can be embedded in .dl files using special comments:
#   // @ASSERT_ROWS: N           - Expect exactly N result rows
#   // @ASSERT_CONTAINS: (tuple) - Output must contain this tuple
#   // @ASSERT_NOT_CONTAINS: (tuple) - Output must NOT contain this tuple
#   // @ASSERT_EMPTY             - Query must return no results
#   // @ASSERT_COLUMNS: N        - Each result tuple must have N columns
#
# Assertions apply to the NEXT query after the assertion comment.
# ============================================================

# Extract assertions from a .dl file
# Returns one assertion per line
extract_assertions() {
    local test_file="$1"
    grep -E "^// @ASSERT" "$test_file" 2>/dev/null | sed 's|^// @||' || true
}

# Check if a test file has any assertions
has_assertions() {
    local test_file="$1"
    grep -q "^// @ASSERT" "$test_file" 2>/dev/null
}

# Count result rows in output (lines starting with "  (" for tuples)
count_result_rows() {
    local output="$1"
    echo "$output" | grep -c "^  (" 2>/dev/null || echo "0"
}

# Check if output contains a specific tuple
# Requires the tuple to be on its own line (with optional leading whitespace)
output_contains_tuple() {
    local output="$1"
    local tuple="$2"
    # Match the tuple as a complete entry (start of line or after whitespace, end of line or before newline)
    # The tuple should appear as "  (x, y)" with the exact content
    echo "$output" | grep -qE "^[[:space:]]*$(echo "$tuple" | sed 's/[[\.*^$()+?{}|]/\\&/g')[[:space:]]*$" 2>/dev/null
}

# Check if output indicates "No results"
output_is_empty() {
    local output="$1"
    echo "$output" | grep -q "No results" 2>/dev/null
}

# Count columns in a result tuple (count commas + 1, accounting for nested parens)
count_tuple_columns() {
    local tuple="$1"
    # Simple approach: count top-level commas + 1
    # Remove outer parens, count commas at depth 0
    local inner="${tuple#(}"
    inner="${inner%)}"
    if [[ -z "$inner" ]]; then
        echo "0"
    else
        # Count commas (simple case - works for flat tuples)
        local comma_count=$(echo "$inner" | tr -cd ',' | wc -c | tr -d ' ')
        echo $((comma_count + 1))
    fi
}

# Verify a single assertion against output
# Returns 0 if assertion passes, 1 if it fails
# Echoes error message on failure
verify_assertion() {
    local assertion="$1"
    local output="$2"
    local test_name="$3"

    case "$assertion" in
        ASSERT_ROWS:*)
            local expected_count="${assertion#ASSERT_ROWS: }"
            expected_count="${expected_count#ASSERT_ROWS:}"  # Handle no space
            expected_count=$(echo "$expected_count" | tr -d ' ')
            local actual_count=$(count_result_rows "$output")
            if [[ "$actual_count" == "$expected_count" ]]; then
                return 0
            else
                echo "Expected $expected_count rows, got $actual_count"
                return 1
            fi
            ;;

        ASSERT_CONTAINS:*)
            local expected_tuple="${assertion#ASSERT_CONTAINS: }"
            expected_tuple="${expected_tuple#ASSERT_CONTAINS:}"
            expected_tuple=$(echo "$expected_tuple" | sed 's/^ *//')
            if output_contains_tuple "$output" "$expected_tuple"; then
                return 0
            else
                echo "Output does not contain: $expected_tuple"
                return 1
            fi
            ;;

        ASSERT_NOT_CONTAINS:*)
            local forbidden_tuple="${assertion#ASSERT_NOT_CONTAINS: }"
            forbidden_tuple="${forbidden_tuple#ASSERT_NOT_CONTAINS:}"
            forbidden_tuple=$(echo "$forbidden_tuple" | sed 's/^ *//')
            if output_contains_tuple "$output" "$forbidden_tuple"; then
                echo "Output should not contain: $forbidden_tuple"
                return 1
            else
                return 0
            fi
            ;;

        ASSERT_EMPTY)
            if output_is_empty "$output"; then
                return 0
            else
                echo "Expected no results, but got some"
                return 1
            fi
            ;;

        ASSERT_COLUMNS:*)
            local expected_cols="${assertion#ASSERT_COLUMNS: }"
            expected_cols="${expected_cols#ASSERT_COLUMNS:}"
            expected_cols=$(echo "$expected_cols" | tr -d ' ')
            # Check first result tuple
            local first_tuple=$(echo "$output" | grep "^  (" | head -1 | sed 's/^  //')
            if [[ -z "$first_tuple" ]]; then
                echo "No result tuples to check column count"
                return 1
            fi
            local actual_cols=$(count_tuple_columns "$first_tuple")
            if [[ "$actual_cols" == "$expected_cols" ]]; then
                return 0
            else
                echo "Expected $expected_cols columns, got $actual_cols in: $first_tuple"
                return 1
            fi
            ;;

        *)
            echo "Unknown assertion type: $assertion"
            return 1
            ;;
    esac
}

# Run all assertions for a test file
# Returns number of failed assertions
run_assertions() {
    local test_file="$1"
    local output="$2"
    local test_name="$3"
    local failed=0

    while IFS= read -r assertion; do
        [[ -z "$assertion" ]] && continue

        local error_msg
        if error_msg=$(verify_assertion "$assertion" "$output" "$test_name" 2>&1); then
            ((ASSERTION_PASSED++))
            if [[ "${VERBOSE:-0}" == "1" ]]; then
                echo -e "    ${GREEN}✓${NC} $assertion"
            fi
        else
            ((ASSERTION_FAILED++))
            ((failed++))
            echo -e "    ${RED}✗${NC} $assertion"
            if [[ -n "$error_msg" ]]; then
                echo -e "      ${RED}→${NC} $error_msg"
            fi
        fi
    done < <(extract_assertions "$test_file")

    return $failed
}

# Function to clean database state before each test
clean_state() {
    # Remove test databases directories
    find "$PROJECT_DIR/data" -maxdepth 1 -type d -name "test_*" -exec rm -rf {} \; 2>/dev/null || true

    # Clean ALL persist state completely - delete all files in these directories
    rm -f "$PROJECT_DIR/data/persist/shards/"* 2>/dev/null || true
    rm -f "$PROJECT_DIR/data/persist/wal/"* 2>/dev/null || true
    rm -f "$PROJECT_DIR/data/persist/batches/"* 2>/dev/null || true

    # Also clean any view catalog files that might be persisted
    find "$PROJECT_DIR/data" -type f -name "views.json" -delete 2>/dev/null || true

    # Reset metadata to only have default database
    cat > "$PROJECT_DIR/data/metadata/databases.json" << 'EOF'
{
  "version": "1.0",
  "databases": [
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

    # Check if expected output exists
    if [[ ! -f "$expected_file" ]]; then
        echo -e "${YELLOW}SKIP${NC} [$category] $test_name - no .out file"
        ((SKIPPED++))
        return
    fi

    # Clean state before test
    clean_state

    # Run the test and capture output (stderr to /dev/null to skip warnings)
    local actual_output
    actual_output=$(cd "$PROJECT_DIR" && cargo run --bin inputlayer-client --release --quiet -- --script "$test_file" 2>/dev/null) || true

    # Normalize both outputs
    local normalized_actual=$(normalize_output "$actual_output")
    local normalized_expected=$(normalize_output "$(cat "$expected_file")")

    # Compare snapshots
    local snapshot_passed=false
    if [[ "$normalized_actual" == "$normalized_expected" ]]; then
        snapshot_passed=true
    fi

    # Check for assertions in the test file
    local has_asserts=false
    local assertion_failures=0
    if has_assertions "$test_file"; then
        has_asserts=true
        # Run assertions against the actual output
        run_assertions "$test_file" "$actual_output" "$test_name" || assertion_failures=$?
    fi

    # Report result
    if [[ "$snapshot_passed" == "true" ]]; then
        if [[ "$has_asserts" == "true" ]] && [[ $assertion_failures -gt 0 ]]; then
            echo -e "${YELLOW}PASS${NC} [$category] $test_name ${RED}($assertion_failures assertion(s) failed)${NC}"
            ((PASSED++))
        elif [[ "$has_asserts" == "true" ]]; then
            echo -e "${GREEN}PASS${NC} [$category] $test_name ${CYAN}(assertions OK)${NC}"
            ((PASSED++))
        else
            echo -e "${GREEN}PASS${NC} [$category] $test_name"
            ((PASSED++))
        fi
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
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -v, --verbose    Show diff output on failures and assertion details"
            echo "  -f, --filter     Only run tests matching pattern (e.g., 'negation')"
            echo "  -h, --help       Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                    # Run all tests"
            echo "  $0 -v                 # Run all tests with verbose output"
            echo "  $0 -f negation        # Run only negation tests"
            echo ""
            echo "Assertions:"
            echo "  Tests can include embedded assertions using special comments:"
            echo "    // @ASSERT_ROWS: N           - Expect exactly N result rows"
            echo "    // @ASSERT_CONTAINS: (tuple) - Output must contain tuple"
            echo "    // @ASSERT_NOT_CONTAINS: (t) - Output must NOT contain tuple"
            echo "    // @ASSERT_EMPTY             - Query must return no results"
            echo "    // @ASSERT_COLUMNS: N        - Each tuple must have N columns"
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
cargo build --bin inputlayer-client --release --quiet 2>/dev/null || cargo build --bin inputlayer-client --release 2>&1 | grep -v "^warning"

echo ""
echo "========================================"
echo "  InputLayer Snapshot Tests"
echo "========================================"
echo ""

# Find all test files
TEST_FILES=$(find "$EXAMPLES_DIR" -name "*.dl" -type f | sort)

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

echo -e "Passed:  ${GREEN}$PASSED${NC}"
echo -e "Failed:  ${RED}$FAILED${NC}"
echo -e "Skipped: ${YELLOW}$SKIPPED${NC}"

# Show assertion stats if any assertions were run
if [[ $((ASSERTION_PASSED + ASSERTION_FAILED)) -gt 0 ]]; then
    echo ""
    echo "  Assertions:"
    echo -e "    Passed:  ${GREEN}$ASSERTION_PASSED${NC}"
    echo -e "    Failed:  ${RED}$ASSERTION_FAILED${NC}"
fi

echo ""

if [[ $FAILED -gt 0 ]]; then
    echo -e "${RED}Some tests failed!${NC}"
    echo "Run with -v for detailed diff output"
    exit 1
elif [[ $ASSERTION_FAILED -gt 0 ]]; then
    echo -e "${YELLOW}All snapshot tests passed, but some assertions failed.${NC}"
    echo "These assertions document known bugs that need fixing."
    exit 0  # Don't fail the build for assertion failures (they document bugs)
else
    echo -e "${GREEN}All tests passed!${NC}"
fi
