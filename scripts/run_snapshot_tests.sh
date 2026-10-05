#!/bin/bash
# Snapshot Test Runner for InputLayer IQL
# Compares actual output against expected .iql.out files
#
# Design principles:
# - 1 connection per test: each test script IS the client invocation
# - Global pre-run cleanup: drop stale KGs before tests start
# - No per-test pre/post-clean connections (tests are self-contained)
# - No fail-fast: run ALL tests, collect ALL failures
# - Per-test timeout via perl alarm (no extra processes)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
EXAMPLES_DIR="$PROJECT_DIR/examples/iql"
SERVER_PORT="${INPUTLAYER_TEST_PORT:-8080}"
SERVER_URL="http://127.0.0.1:${SERVER_PORT}"
CLIENT_SERVER_URL="${SERVER_URL}"

# Auth: the server is started with a clean environment (no INPUTLAYER_* vars,
# see start_server) and a generated test config, so it bootstraps or reuses
# credentials in .inputlayer-credentials.toml. After startup we read the API
# key from that file and export INPUTLAYER_API_KEY for the client invocations.
# Reason: `--config` mode rejects unknown INPUTLAYER_* env vars (config parse
# error), so the old INPUTLAYER_BOOTSTRAP_API_KEY approach cannot be combined
# with a config file.
CREDENTIALS_FILE="$PROJECT_DIR/.inputlayer-credentials.toml"

# Parallelism: use all CPUs
NCPU=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)
PARALLEL_JOBS=${INPUTLAYER_TEST_PARALLEL:-$NCPU}

# Per-test timeout in seconds (prevents hanging tests from blocking the harness)
TEST_TIMEOUT=60

# Colors (only when stdout is a terminal)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    CYAN='\033[0;36m'
    NC='\033[0m'
else
    RED='' GREEN='' YELLOW='' CYAN='' NC=''
fi

# Counters
PASSED=0
FAILED=0
SKIPPED=0
UPDATED=0
DIRTY=0

# Mode flags
UPDATE_MODE=0
SKIP_BUILD=0
# Cargo profile of the server and client binaries and the target directory
# it writes them to (--debug: target/debug, the binaries `cargo test` builds)
PROFILE=release
PROFILE_DIR=release
VERBOSE=0
FILTER=""
AFFECTED_REF=""

# Temp dir for all intermediate files
TEMP_DIR=$(mktemp -d)
mkdir -p "$TEMP_DIR/results"

# Normalize output for comparison
normalize_output() {
    local input="$1"
    echo "$input" | \
        grep -v "^Executing script:" | \
        grep -v "^Connecting to server" | \
        grep -v "^Connected!" | \
        grep -v "^Authenticated as:" | \
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
        awk -v allowed="${ALLOWED_KGS:-}" '
            BEGIN {
                n = split(allowed, arr, " ")
                for (i = 1; i <= n; i++) {
                    if (arr[i] != "") allow[arr[i]] = 1
                }
                in_list = 0
            }
            {
                line = $0
                if (line ~ /^Knowledge Graphs:/) {
                    in_list = 1
                    print line
                    next
                }
                if (in_list) {
                    if (line ~ /^  /) {
                        name = line
                        sub(/^  /, "", name)
                        sub(/ \*$/, "", name)
                        if (allowed == "" || allow[name]) {
                            print line
                        }
                        next
                    } else {
                        in_list = 0
                    }
                }
                print line
            }
        ' | \
        sed 's/[[:space:]]*$//'
}

# Extract KG names created in a script
extract_kgs() {
    local file="$1"
    grep -E '^\\.kg[[:space:]]+create[[:space:]]+' "$file" 2>/dev/null \
        | awk '{print $3}' \
        | grep -v '^default$' \
        || true
}

# List non-default knowledge graphs via client
ws_list_kgs() {
    local script_file="${TEMP_DIR}/_kg_list.iql"
    local output_file="${TEMP_DIR}/_kg_list.out"
    printf '%s\n' ".kg list" > "$script_file"
    perl -e 'alarm shift; exec @ARGV' 15 "$CLIENT_BIN" --server "$CLIENT_SERVER_URL" --script "$script_file" >"$output_file" 2>/dev/null || true
    cat "$output_file" 2>/dev/null \
        | awk '/^  / { sub(/^  /, "", $0); sub(/ \*$/, "", $0); print }' \
        | grep -v "^default$" \
        || true
}

# Drop knowledge graphs via client
ws_drop_kgs() {
    local kgs="$1"
    [[ -z "$kgs" ]] && return 0
    local script_file
    script_file=$(mktemp "${TEMP_DIR}/_kg_drop_XXXXXX.iql")
    {
        printf '%s\n' ".kg use default"
        for kg in $kgs; do
            printf '%s\n' ".kg drop $kg"
        done
    } > "$script_file"
    perl -e 'alarm shift; exec @ARGV' 30 "$CLIENT_BIN" --server "$CLIENT_SERVER_URL" --script "$script_file" >/dev/null 2>&1 || true
    rm -f "$script_file"
}

# Server management
check_server() {
    curl -s --max-time 10 "${SERVER_URL}/health" > /dev/null 2>&1
}

stop_server() {
    if [[ -n "$SERVER_PID" ]]; then
        kill $SERVER_PID 2>/dev/null || true
        # Wait up to 3s for graceful shutdown, then SIGKILL
        for i in 1 2 3 4 5 6; do
            if ! kill -0 $SERVER_PID 2>/dev/null; then break; fi
            sleep 0.5
        done
        kill -9 $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
        SERVER_PID=""
    fi
}

# Generate the test server config. Mirrors the server defaults except:
# - WS message and per-IP rate limits are OFF: statement-heavy test scripts
#   (e.g. rule packs loaded via .load, which the client expands into one WS
#   message per statement) legitimately exceed the production default of
#   100 msgs/sec on localhost.
# - credentials_file is pinned so we can read the API key back deterministically.
write_server_config() {
    cat > "$TEMP_DIR/server-config.toml" <<EOF
[storage]
data_dir = "$PROJECT_DIR/data"
default_database = "default"

[storage.persistence]
format = "parquet"
compression = "snappy"
auto_save_interval = 0
enable_wal = true

[optimization]
enable_join_planning = true
enable_sip_rewriting = true
enable_subplan_sharing = true
enable_boolean_specialization = true
enable_magic_sets = true

[logging]
level = "info"
format = "text"

[http]
enabled = true
host = "127.0.0.1"
port = $SERVER_PORT

[http.auth]
credentials_file = "$CREDENTIALS_FILE"

[http.rate_limit]
ws_max_messages_per_sec = 0
per_ip_max_rps = 0
EOF
}

start_server() {
    local clean_data="${1:-true}"
    # Wait for port to be free FIRST (up to 15s)
    for i in $(seq 1 30); do
        if ! lsof -ti :"${SERVER_PORT}" >/dev/null 2>&1; then break; fi
        sleep 0.5
    done
    if [[ "$clean_data" == "true" ]]; then
        rm -rf "$PROJECT_DIR/data"
    fi
    SERVER_LOG="${INPUTLAYER_SERVER_LOG:-${TEMP_DIR}/server.log}"
    if [[ -n "${INPUTLAYER_SERVER_LOG:-}" ]]; then
        : > "$SERVER_LOG"
    fi
    echo "Server log: $SERVER_LOG"
    write_server_config
    # Launch with a clean environment: in --config mode the server rejects
    # any INPUTLAYER_* env var that does not map to a config field.
    (
        cd "$PROJECT_DIR" || exit 1
        for v in $(compgen -e); do
            [[ "$v" == INPUTLAYER_* ]] && unset "$v"
        done
        exec "$SERVER_BIN" --config "$TEMP_DIR/server-config.toml" >>"$SERVER_LOG" 2>&1
    ) &
    SERVER_PID=$!
    for i in $(seq 1 30); do
        if check_server; then break; fi
        sleep 0.5
    done
    if ! check_server; then
        echo -e "${RED}Server failed to start!${NC}"
        return 1
    fi
    # Read the API key the server bootstrapped (or reused) for client auth
    if [[ ! -f "$CREDENTIALS_FILE" ]]; then
        echo -e "${RED}Server started but no credentials file at $CREDENTIALS_FILE${NC}"
        return 1
    fi
    INPUTLAYER_API_KEY="$(grep '^api_key' "$CREDENTIALS_FILE" | head -1 | cut -d'"' -f2)"
    if [[ -z "$INPUTLAYER_API_KEY" ]]; then
        echo -e "${RED}Could not parse api_key from $CREDENTIALS_FILE${NC}"
        return 1
    fi
    export INPUTLAYER_API_KEY
    echo "Server started (PID $SERVER_PID)"
    return 0
}

# Restart server without wiping data (for mid-run recovery)
restart_server_keep_data() {
    echo -e "${CYAN}Restarting server (keeping data)...${NC}"
    stop_server
    if ! start_server false; then
        echo -e "${RED}Server restart failed!${NC}"
        return 1
    fi
    return 0
}

restart_server() {
    echo -e "${CYAN}Restarting server (clean data)...${NC}"
    stop_server
    if ! start_server; then
        echo -e "${RED}Server restart failed!${NC}"
        return 1
    fi
    return 0
}

# Cleanup on exit
cleanup() {
    # Kill any lingering client processes
    pkill -f "inputlayer-client.*${SERVER_PORT}" 2>/dev/null || true
    # rm -rf "$TEMP_DIR"
    stop_server
}
trap cleanup EXIT

# Parallel worker: processes one test file, writes result to temp dir
# Runs synchronously - no background processes. Timeout via perl alarm.
run_test_parallel() {
    local test_file="$1"
    local expected_file="${test_file}.out"
    local test_name=$(basename "$test_file")
    local category=$(basename "$(dirname "$test_file")")
    local safe_name="${category}__${test_name}"
    local result_file="$TEMP_DIR/results/${safe_name}.result"
    local output_file="$TEMP_DIR/results/${safe_name}.output"
    local stderr_file="$TEMP_DIR/results/${safe_name}.stderr"
    local diff_file="$TEMP_DIR/results/${safe_name}.diff"

    # Skip tests without .out files
    if [[ ! -f "$expected_file" ]]; then
        echo "SKIP [$category] $test_name - no .out file" > "$result_file"
        return 0
    fi

    # Run the test with perl alarm timeout (synchronous, no extra processes)
    # Retry once on empty output (transient connection failure - no state was modified)
    local attempt
    for attempt in 1 2; do
        perl -e 'alarm shift; exec @ARGV' "$TEST_TIMEOUT" \
            "$CLIENT_BIN" --server "$CLIENT_SERVER_URL" --script "$test_file" \
            >"$output_file" 2>"$stderr_file" || true
        # If output is non-empty, done; otherwise retry after brief delay
        if [[ -s "$output_file" ]]; then
            break
        fi
        if [[ $attempt -eq 1 ]]; then
            sleep 2
        fi
    done
    local actual_output
    actual_output=$(cat "$output_file")

    # Normalize both outputs
    local allowed_kgs="default"
    local kgs
    kgs=$(extract_kgs "$test_file")
    if [[ -n "$kgs" ]]; then
        allowed_kgs="default $kgs"
    fi
    local normalized_actual=$(ALLOWED_KGS="$allowed_kgs" normalize_output "$actual_output")
    local normalized_expected=$(ALLOWED_KGS="$allowed_kgs" normalize_output "$(cat "$expected_file")")

    # Compare
    if [[ "$normalized_actual" == "$normalized_expected" ]]; then
        echo "PASS [$category] $test_name" > "$result_file"
    else
        echo "FAIL [$category] $test_name" > "$result_file"
        diff <(echo "$normalized_expected") <(echo "$normalized_actual") > "$diff_file" 2>/dev/null || true
        # Append stderr for debugging
        if [[ -s "$stderr_file" ]]; then
            echo "--- stderr ---" >> "$diff_file"
            head -5 "$stderr_file" >> "$diff_file"
        fi
    fi
}

# Sequential test runner
run_test_sequential() {
    local test_file="$1"
    local expected_file="${test_file}.out"
    local test_name=$(basename "$test_file")
    local category=$(basename "$(dirname "$test_file")")

    # In update mode, don't skip tests without .out files
    if [[ "$UPDATE_MODE" != "1" ]] && [[ ! -f "$expected_file" ]]; then
        echo -e "${YELLOW}SKIP${NC} [$category] $test_name - no .out file"
        ((SKIPPED++))
        return
    fi

    local stderr_file="$TEMP_DIR/${category}_${test_name}.stderr"
    local stdout_file="$TEMP_DIR/${category}_${test_name}.stdout"

    # Run the test with perl alarm timeout (synchronous, no extra processes)
    perl -e 'alarm shift; exec @ARGV' "$TEST_TIMEOUT" \
        "$CLIENT_BIN" --server "$CLIENT_SERVER_URL" --script "$test_file" \
        >"$stdout_file" 2>"$stderr_file" || true
    local actual_output
    actual_output=$(cat "$stdout_file")

    # Update mode
    if [[ "$UPDATE_MODE" == "1" ]]; then
        printf '%s\n' "$actual_output" > "${expected_file}.tmp" && mv "${expected_file}.tmp" "$expected_file"
        echo -e "${CYAN}UPDATED${NC} [$category] $test_name"
        ((UPDATED++))
        return
    fi

    # Normalize
    local allowed_kgs="default"
    local kgs
    kgs=$(extract_kgs "$test_file")
    if [[ -n "$kgs" ]]; then
        allowed_kgs="default $kgs"
    fi
    local normalized_actual=$(ALLOWED_KGS="$allowed_kgs" normalize_output "$actual_output")
    local normalized_expected=$(ALLOWED_KGS="$allowed_kgs" normalize_output "$(cat "$expected_file")")

    if [[ "$normalized_actual" == "$normalized_expected" ]]; then
        echo -e "${GREEN}PASS${NC} [$category] $test_name"
        ((PASSED++))
    else
        echo -e "${RED}FAIL${NC} [$category] $test_name"
        ((FAILED++))
        echo "  --- Diff (expected vs actual) ---"
        diff <(echo "$normalized_expected") <(echo "$normalized_actual") | head -50 || true
        if [[ "${VERBOSE:-0}" == "1" ]]; then
            echo "  --- Expected (full) ---"
            echo "$normalized_expected" | head -30
            echo "  --- Actual (full) ---"
            echo "$normalized_actual" | head -30
        fi
        if [[ -s "$stderr_file" ]]; then
            echo "  --- Stderr ---"
            head -10 "$stderr_file"
        fi
        echo ""
    fi
}

# Files changed since a ref: committed, staged and unstaged (the working tree
# against the ref)
changed_files() {
    git -C "$PROJECT_DIR" diff --name-only "$1" --
}

# Map changed files to the spec categories that exercise them. Sets
# AFFECTED (space-separated categories) and AFFECTED_ALL=1 when a change can
# reach every category. Only leaf modules have a narrow mapping; any other
# change to the engine's sources or build runs the whole corpus, so a module
# missing from this list costs time rather than coverage.
affected_categories() {
    AFFECTED=""
    AFFECTED_ALL=0
    local file categories
    for file in "$@"; do
        categories=""
        case "$file" in
            src/vector_ops.rs|src/hnsw_index.rs|src/hnsw_index_tests.rs)
                categories="16_vectors 30_quantization 31_lsh" ;;
            src/temporal_ops.rs)
                categories="29_temporal" ;;
            src/*.rs|src/*/*|Cargo.toml|Cargo.lock|config.toml|ws-protocol/*|ontology-client/*|scripts/run_snapshot_tests.sh)
                AFFECTED_ALL=1 ;;
            examples/iql/*/*)
                # A spec changed: run its category
                categories=$(echo "$file" | cut -d/ -f3) ;;
        esac
        AFFECTED="$AFFECTED $categories"
    done
    AFFECTED=$(echo "$AFFECTED" | tr ' ' '\n' | grep -v '^$' | sort -u | tr '\n' ' ' | sed 's/ $//')
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)   VERBOSE=1; shift ;;
        -f|--filter)    FILTER="$2"; shift 2 ;;
        -u|--update)    UPDATE_MODE=1; shift ;;
        --skip-build)   SKIP_BUILD=1; shift ;;
        --debug)        PROFILE=dev; PROFILE_DIR=debug; shift ;;
        --affected)     AFFECTED_REF="$2"; shift 2 ;;
        -j|--jobs)      PARALLEL_JOBS="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -v, --verbose    Show verbose diff output on failures (forces sequential)"
            echo "  -f, --filter     Only run tests matching pattern"
            echo "  -u, --update     Update .out files with actual output (forces sequential)"
            echo "  -j, --jobs N     Parallel jobs (default: $PARALLEL_JOBS, 0 or 1 = sequential)"
            echo "  --skip-build     Skip cargo build"
            echo "  --debug          Use the target/debug binaries cargo test builds (default: release)"
            echo "  --affected REF   Only run the categories the changes since REF affect"
            echo "  -h, --help       Show this help message"
            exit 0
            ;;
        *)  echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Force sequential for update/verbose
if [[ "$UPDATE_MODE" == "1" ]] || [[ "$VERBOSE" == "1" ]]; then
    PARALLEL_JOBS=0
fi

cd "$PROJECT_DIR"

# --affected: turn the changes since the ref into a category filter, or stop
# here when no spec depends on them
if [[ -n "$AFFECTED_REF" ]]; then
    if [[ -n "$FILTER" ]]; then
        echo -e "${RED}--affected and --filter cannot be combined${NC}"
        exit 1
    fi
    if ! CHANGED=$(changed_files "$AFFECTED_REF"); then
        echo -e "${RED}Cannot diff against $AFFECTED_REF${NC}"
        exit 1
    fi
    affected_categories $CHANGED
    # Keep only categories that still hold a runnable spec (a deleted category,
    # or one with only _ helpers and _pending_ specs, has nothing to run)
    RUNNABLE=""
    for category in $AFFECTED; do
        if [[ -d "$EXAMPLES_DIR/$category" ]] && [[ -n "$(find "$EXAMPLES_DIR/$category" -name "*.iql" -type f ! -name "_*" ! -name "*_pending_*" | head -1)" ]]; then
            RUNNABLE="$RUNNABLE $category"
        fi
    done
    AFFECTED="${RUNNABLE# }"
    if [[ "$AFFECTED_ALL" == "1" ]]; then
        echo "Changes since $AFFECTED_REF affect every category - running all snapshot tests."
    elif [[ -z "$AFFECTED" ]]; then
        echo "No runnable specs affected by the changes since $AFFECTED_REF."
        exit 0
    else
        echo "Changes since $AFFECTED_REF affect these categories:"
        for category in $AFFECTED; do
            echo "  - $category"
        done
        # Anchored at the category directory; basic grep, so alternation is \|
        FILTER="/\($(echo "$AFFECTED" | sed 's/ /\\|/g')\)/"
    fi
    echo ""
fi

# Build
if [[ "$SKIP_BUILD" == "1" ]]; then
    echo "Skipping build (--skip-build)..."
else
    echo "Building project..."
    if ! cargo build --bin inputlayer-client --bin inputlayer-server --profile "$PROFILE" --quiet 2>/dev/null; then
        echo -e "${RED}Build failed!${NC}"
        cargo build --bin inputlayer-client --bin inputlayer-server --profile "$PROFILE" 2>&1 | grep -v "^warning" || true
        echo -e "${RED}Aborting tests due to build failure.${NC}"
        exit 1
    fi
fi

# Detect target dir
TARGET_DIR=$(cargo metadata --format-version 1 --no-deps 2>/dev/null \
    | grep -o '"target_directory":"[^"]*"' | cut -d'"' -f4)
if [[ -z "$TARGET_DIR" ]]; then
    TARGET_DIR="$PROJECT_DIR/target"
fi

CLIENT_BIN="$TARGET_DIR/$PROFILE_DIR/inputlayer-client"
SERVER_BIN="$TARGET_DIR/$PROFILE_DIR/inputlayer-server"

if [[ ! -x "$CLIENT_BIN" ]] || [[ ! -x "$SERVER_BIN" ]]; then
    echo -e "${RED}Binaries not found after build!${NC}"
    echo "  Client: $CLIENT_BIN"
    echo "  Server: $SERVER_BIN"
    exit 1
fi

# Stop any existing server
SERVER_PID=""
if check_server; then
    echo "Stopping existing server..."
    pkill -f "inputlayer-server" 2>/dev/null || true
    for i in $(seq 1 10); do
        if ! check_server; then break; fi
        sleep 0.5
    done
    # Force kill if still alive
    if check_server; then
        pkill -9 -f "inputlayer-server" 2>/dev/null || true
        sleep 1
    fi
fi

# Start fresh server
echo "Starting server..."
if ! start_server; then
    exit 1
fi

# Global pre-clean: drop all non-default KGs from prior failed runs
echo "Pre-cleaning stale knowledge graphs..."
stale_kgs=$(ws_list_kgs)
if [[ -n "$stale_kgs" ]]; then
    echo "  Dropping: $stale_kgs"
    ws_drop_kgs "$stale_kgs"
fi

# Warmup: send a few queries to ensure the server's WS pipeline is fully ready
warmup_script="${TEMP_DIR}/_warmup.iql"
printf '%s\n' ".kg list" ".status" > "$warmup_script"
for i in 1 2 3; do
    perl -e 'alarm shift; exec @ARGV' 10 "$CLIENT_BIN" --server "$CLIENT_SERVER_URL" --script "$warmup_script" >/dev/null 2>&1 || true
done
rm -f "$warmup_script"

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

# Find test files (exclude _ prefix helpers and _pending_ tests)
TEST_FILES=$(find "$EXAMPLES_DIR" -name "*.iql" -type f ! -name "_*" ! -name "*_pending_*" | sort)

if [[ -n "$FILTER" ]]; then
    TEST_FILES=$(echo "$TEST_FILES" | grep "$FILTER")
fi

if [[ -z "$TEST_FILES" ]]; then
    TEST_TOTAL=0
else
    TEST_TOTAL=$(echo "$TEST_FILES" | wc -l | tr -d ' ')
fi

# A filter that matches nothing would pass vacuously (a renamed spec drops
# out of its gate), so fail it
if [[ -n "$FILTER" ]] && [[ "$TEST_TOTAL" == "0" ]]; then
    echo -e "${RED}No tests match filter: $FILTER${NC}"
    exit 1
fi

PENDING_COUNT=$(find "$EXAMPLES_DIR" -name "*_pending_*.iql" -type f | wc -l | tr -d ' ')

# Run tests
if [[ "$PARALLEL_JOBS" -le 1 ]]; then
    # --- Sequential mode ---
    LAST_TEST=""
    HEALTH_CHECK_INTERVAL=100
    SERVER_RESTART_INTERVAL=${INPUTLAYER_RESTART_INTERVAL:-500}
    TEST_COUNT=0

    for test_file in $TEST_FILES; do
        TEST_COUNT=$((TEST_COUNT + 1))

        if [[ $((TEST_COUNT % SERVER_RESTART_INTERVAL)) -eq 0 ]]; then
            restart_server_keep_data
        elif [[ $((TEST_COUNT % HEALTH_CHECK_INTERVAL)) -eq 0 ]]; then
            if ! check_server; then
                echo -e "${YELLOW}Server health check failed, restarting...${NC}"
                restart_server_keep_data
            fi
        fi

        run_test_sequential "$test_file"
    done
else
    # --- Parallel mode ---
    PARALLEL_FILE="$TEMP_DIR/parallel_tests.txt"
    SEQUENTIAL_FILE="$TEMP_DIR/sequential_tests.txt"

    # Classify: only tests using global commands go sequential
    for f in $TEST_FILES; do
        if grep -qE '^\.(kg list|status|compact)' "$f" 2>/dev/null; then
            echo "$f" >> "$SEQUENTIAL_FILE"
        else
            echo "$f" >> "$PARALLEL_FILE"
        fi
    done

    PARALLEL_COUNT=0
    SEQUENTIAL_COUNT=0
    [[ -f "$PARALLEL_FILE" ]] && PARALLEL_COUNT=$(wc -l < "$PARALLEL_FILE" | tr -d ' ')
    [[ -f "$SEQUENTIAL_FILE" ]] && SEQUENTIAL_COUNT=$(wc -l < "$SEQUENTIAL_FILE" | tr -d ' ')

    # Shuffle parallel tests to spread load
    if [[ -f "$PARALLEL_FILE" ]]; then
        sort -R "$PARALLEL_FILE" > "${PARALLEL_FILE}.shuffled"
        mv "${PARALLEL_FILE}.shuffled" "$PARALLEL_FILE"
    fi

    echo "Running $PARALLEL_COUNT tests in parallel ($PARALLEL_JOBS workers), $SEQUENTIAL_COUNT sequentially..."
    echo ""

    # Run parallel batch
    if [[ -f "$PARALLEL_FILE" ]] && [[ "$PARALLEL_COUNT" -gt 0 ]]; then
        export -f run_test_parallel normalize_output extract_kgs
        export CLIENT_BIN TEMP_DIR SERVER_URL CLIENT_SERVER_URL TEST_TIMEOUT

        cat "$PARALLEL_FILE" | xargs -P"$PARALLEL_JOBS" -I{} bash -c 'run_test_parallel "{}"'
    fi

    # Collect parallel results
    if ls "$TEMP_DIR"/results/*.result >/dev/null 2>&1; then
        for result_file in "$TEMP_DIR"/results/*.result; do
            [[ -f "$result_file" ]] || continue
            local_result=$(cat "$result_file")

            if [[ "$local_result" == PASS* ]]; then
                ((PASSED++))
            elif [[ "$local_result" == FAIL* ]]; then
                echo -e "${RED}${local_result}${NC}"
                ((FAILED++))
                diff_file="${result_file%.result}.diff"
                if [[ -s "$diff_file" ]]; then
                    echo "  --- Diff (expected vs actual) ---"
                    head -30 "$diff_file"
                    echo ""
                fi
            elif [[ "$local_result" == SKIP* ]]; then
                ((SKIPPED++))
            fi
        done
    fi

    # Kill any lingering client processes before cleanup phase
    pkill -f "inputlayer-client.*${SERVER_PORT}" 2>/dev/null || true
    sleep 1

    # Check server health; try restart if dead
    SERVER_ALIVE=true
    if ! check_server; then
        echo -e "${YELLOW}Server died during parallel tests, attempting restart...${NC}"
        if ! restart_server_keep_data; then
            echo -e "${RED}Could not restart server - skipping leak check and sequential tests${NC}"
            SERVER_ALIVE=false
        fi
    fi

    # Check for leaked KGs after parallel run (only if server is alive)
    if [[ "$SERVER_ALIVE" == true ]]; then
        leaked_kgs=$(ws_list_kgs)
        if [[ -n "$leaked_kgs" ]]; then
            echo ""
            echo -e "${YELLOW}WARNING: Leaked knowledge graphs after parallel run${NC}"
            echo "  Leaked: $leaked_kgs"
            ((DIRTY++))
            ws_drop_kgs "$leaked_kgs"
        fi
    fi

    # Run sequential batch (global-state tests)
    if [[ -f "$SEQUENTIAL_FILE" ]] && [[ "$SEQUENTIAL_COUNT" -gt 0 ]]; then
        if [[ "$SERVER_ALIVE" == true ]]; then
            echo ""
            echo "Running $SEQUENTIAL_COUNT sequential tests (global state)..."

            # Restart server with clean data for sequential tests (they test global state)
            if ! restart_server; then
                echo -e "${RED}Could not restart server for sequential tests${NC}"
                FAILED=$((FAILED + SEQUENTIAL_COUNT))
            else
                LAST_TEST=""
                for test_file in $(cat "$SEQUENTIAL_FILE"); do
                    run_test_sequential "$test_file"
                done
            fi
        else
            # Server is dead and couldn't restart - count sequential tests as failures
            echo ""
            echo -e "${RED}Skipping $SEQUENTIAL_COUNT sequential tests (server unavailable)${NC}"
            FAILED=$((FAILED + SEQUENTIAL_COUNT))
        fi
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
        echo -e "Dirty:   ${YELLOW}$DIRTY${NC} (leaked KGs after parallel run)"
    fi
    if [[ "$PENDING_COUNT" -gt 0 ]]; then
        echo -e "Pending: ${CYAN}$PENDING_COUNT${NC} (features not yet implemented)"
    fi
    echo ""

    if [[ $FAILED -gt 0 ]]; then
        echo -e "${RED}Some tests failed!${NC}"
        echo "Run with -v for detailed output"
        if [[ -f "$SERVER_LOG" ]]; then
            echo ""
            echo "=== Server Log (last 50 lines) ==="
            tail -50 "$SERVER_LOG"
            echo "=== End Server Log ==="
        fi
        exit 1
    else
        echo -e "${GREEN}All tests passed!${NC}"
    fi
fi
