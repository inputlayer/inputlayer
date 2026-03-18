.PHONY: all ci fmt fmt-check lint test test-fast test-release unit-test integration-test e2e-test e2e-update test-affected doc doc-check check build build-release clean fix release snapshot-test test-all ci-test-all flush-dev docker docker-run docker-deploy docker-deploy-no-tls docker-logs docker-stop deny python-test front-build front-deploy gui-build run run-server coverage view-coverage static-analysis

SHELL := /bin/bash

# Default target
all: ci

# Developer Workflow

# Fast feedback loop - unit + integration tests only (~30s)
test-fast: check unit-test
	@echo "Fast tests complete."

# Pre-commit gate - unit + integration + snapshot E2E (~60-90s)
test: check unit-test e2e-test
	@echo "All tests complete."

# Full verification (CI, pre-merge)
# Runs everything: static analysis, build, unit+integration tests, snapshot E2E tests, coverage
# All tests run in parallel. Zero ignored tests allowed. Cleanup verified.
test-all: check static-analysis
	@FAILURES=0; \
	STRIP_ANSI='s/\x1b\[[0-9;]*m//g'; \
	NCPU=$$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4); \
	rm -f /tmp/il_make_test_all.log 2>/dev/null || true; \
	exec > >(tee /tmp/il_make_test_all.log) 2>&1; \
	echo "Running all tests ($$NCPU CPUs)..."; \
	echo ""; \
	echo "=== Cleanup (pre-test) ==="; \
	rm -rf ./data 2>/dev/null || true; \
	echo "Cleaned ./data directory"; \
	echo ""; \
	rm -f /tmp/il_trace.log /tmp/il_server.log 2>/dev/null || true; \
	echo "=== Build (release) ==="; \
	if cargo build --all-features --release 2>&1; then \
		BUILD_STATUS="PASS"; \
	else \
		BUILD_STATUS="FAIL"; \
		FAILURES=$$((FAILURES + 1)); \
	fi; \
	echo ""; \
	echo "=== Unit + Integration Tests ($$NCPU threads) ==="; \
	UNIT_TMPFILE=$$(mktemp); \
	set -o pipefail; \
	RUST_TEST_THREADS=$$NCPU cargo test --all-features -- --test-threads=$$NCPU --format=pretty \
		2>&1 | tee "$$UNIT_TMPFILE"; \
	UNIT_EXIT=$${PIPESTATUS[0]}; \
	tail -5 "$$UNIT_TMPFILE"; \
	UNIT_PASSED=$$(grep -E "^test result:" "$$UNIT_TMPFILE" | awk '{sum += $$4} END {print sum+0}'); \
	UNIT_FAILED=$$(grep -E "^test result:" "$$UNIT_TMPFILE" | awk '{sum += $$6} END {print sum+0}'); \
	UNIT_IGNORED=$$(grep -E "^test result:" "$$UNIT_TMPFILE" | awk '{sum += $$8} END {print sum+0}'); \
	rm -f "$$UNIT_TMPFILE"; \
	if [ $$UNIT_EXIT -ne 0 ]; then FAILURES=$$((FAILURES + 1)); fi; \
	if [ "$$UNIT_IGNORED" -gt 0 ] 2>/dev/null; then \
		echo "ERROR: $$UNIT_IGNORED ignored test(s) detected. No tests may be ignored."; \
		FAILURES=$$((FAILURES + 1)); \
	fi; \
	echo ""; \
	echo "Settling before snapshot tests..."; \
	lsof -ti :8080 | xargs kill -9 2>/dev/null || true; \
	rm -rf ./data 2>/dev/null || true; \
	echo "Cleaning debug artifacts to free memory..."; \
	rm -rf target/debug 2>/dev/null || true; \
	sync 2>/dev/null || true; \
	sleep 8; \
	echo ""; \
	echo "=== Snapshot Tests (E2E) ==="; \
	SNAP_TMPFILE=$$(mktemp); \
	set -o pipefail; \
	IL_TRACE=1 IL_TRACE_LEVEL=trace IL_SERVER_LOG=/tmp/il_server.log IL_TRACE_FILE=/tmp/il_trace.log \
	   ./scripts/run_snapshot_tests.sh --skip-build 2>&1 | tee "$$SNAP_TMPFILE"; \
	SNAP_EXIT=$${PIPESTATUS[0]}; \
	tail -10 "$$SNAP_TMPFILE"; \
	SNAP_PASSED=$$(sed "$$STRIP_ANSI" "$$SNAP_TMPFILE" | grep -E "^Passed:" | awk '{print $$2}'); \
	SNAP_FAILED=$$(sed "$$STRIP_ANSI" "$$SNAP_TMPFILE" | grep -E "^Failed:" | awk '{print $$2}'); \
	rm -f "$$SNAP_TMPFILE"; \
	if [ $$SNAP_EXIT -ne 0 ]; then FAILURES=$$((FAILURES + 1)); fi; \
	echo ""; \
	echo "=== Python SDK Tests ==="; \
	if python -m pytest --version >/dev/null 2>&1; then \
		PY_TMPFILE=$$(mktemp); \
		set -o pipefail; \
		cd packages/inputlayer-py && python -m pytest tests/ -v 2>&1 | tee "$$PY_TMPFILE"; \
		PY_EXIT=$${PIPESTATUS[0]}; \
		cd ../..; \
		PY_LINE=$$(grep -E "passed" "$$PY_TMPFILE" | tail -1); \
		PY_PASSED=$$(echo "$$PY_LINE" | grep -oE '[0-9]+ passed' | awk '{print $$1}'); \
		PY_FAILED=$$(echo "$$PY_LINE" | grep -oE '[0-9]+ failed' | awk '{print $$1}'); \
		PY_PASSED=$${PY_PASSED:-0}; \
		PY_FAILED=$${PY_FAILED:-0}; \
		rm -f "$$PY_TMPFILE"; \
		if [ $$PY_EXIT -ne 0 ]; then FAILURES=$$((FAILURES + 1)); fi; \
	else \
		echo "SKIPPED: python/pytest not available (pip install -e 'packages/inputlayer-py[dev]')"; \
		PY_PASSED=0; PY_FAILED=0; \
	fi; \
	echo ""; \
	echo "=== Coverage Report ==="; \
	if command -v cargo-tarpaulin >/dev/null 2>&1; then \
		mkdir -p target/coverage; \
		if cargo tarpaulin --all-features --out html --out json --output-dir target/coverage \
			--exclude-files "tests/*" --exclude-files "benches/*" --exclude-files "examples/*" \
			--exclude-files "src/bin/*" --exclude-files "src/main.rs" \
			--timeout 300 --skip-clean 2>&1; then \
			COV_STATUS="PASS"; \
			COV_PCT=$$(python3 -c "import json; d=json.load(open('target/coverage/tarpaulin-report.json')); \
				tc=sum(f.get('covered',0) for f in d.get('files',[])); \
				tt=sum(f.get('coverable',0) for f in d.get('files',[])); \
				print(f'{tc/tt*100:.1f}' if tt>0 else '0.0')" 2>/dev/null || echo "?"); \
			COV_STATUS="PASS ($${COV_PCT}%)"; \
		else \
			COV_STATUS="FAIL"; \
			FAILURES=$$((FAILURES + 1)); \
		fi; \
	else \
		COV_STATUS="SKIPPED (install: cargo install cargo-tarpaulin)"; \
	fi; \
	echo ""; \
	echo "=== Cleanup Verification ==="; \
	STALE_DATA=""; \
	if [ -d "./data" ]; then \
		STALE_KGS=$$(ls -d ./data/*/ 2>/dev/null | grep -v persist | grep -v metadata | grep -v '/default/' | wc -l | tr -d ' '); \
		if [ "$$STALE_KGS" -gt 0 ]; then \
			STALE_DATA="$$STALE_KGS stale KG directories in ./data"; \
		fi; \
	fi; \
	if [ -n "$$STALE_DATA" ]; then \
		echo "WARNING: $$STALE_DATA"; \
	else \
		echo "Clean: no stale test data"; \
	fi; \
	rm -rf ./data 2>/dev/null || true; \
	echo ""; \
	TOTAL=$$(($$UNIT_PASSED + $${SNAP_PASSED:-0} + $${PY_PASSED:-0})); \
	TOTAL_FAILED=$$(($$UNIT_FAILED + $${SNAP_FAILED:-0} + $${PY_FAILED:-0})); \
	echo "==========================================="; \
	echo "              TEST SUMMARY"; \
	echo "==========================================="; \
	echo ""; \
	printf "  | %-14s | %-48s |\n" "Category" "Status"; \
	printf "  |----------------|--------------------------------------------------|\n"; \
	printf "  | %-14s | %-48s |\n" "Build" "$$BUILD_STATUS"; \
	printf "  | %-14s | %-48s |\n" "Cargo Tests" "$$UNIT_PASSED passed, $$UNIT_FAILED failed, $$UNIT_IGNORED ignored"; \
	printf "  | %-14s | %-48s |\n" "Snapshot Tests" "$${SNAP_PASSED:-0} passed, $${SNAP_FAILED:-0} failed"; \
	printf "  | %-14s | %-48s |\n" "Python Tests" "$${PY_PASSED:-0} passed, $${PY_FAILED:-0} failed"; \
	printf "  | %-14s | %-48s |\n" "Coverage" "$${COV_STATUS:-SKIPPED}"; \
	printf "  | %-14s | %-48s |\n" "TOTAL" "$$TOTAL passed, $$TOTAL_FAILED failed"; \
	echo ""; \
	echo "==========================================="; \
	if [ $$FAILURES -ne 0 ]; then \
		echo "SOME CHECKS FAILED ($$FAILURES)"; \
		exit 1; \
	fi; \
	echo "ALL CHECKS PASSED"

# CI-friendly full verification (disk/memory-constrained free-tier runners)
# Same checks as test-all but with:
#   - Thin LTO instead of full LTO (halves linker peak memory)
#   - More codegen units (reduces per-unit memory)
#   - Capped test parallelism (DD workers are memory-hungry)
#   - Capped snapshot parallelism
#   - Debug artifacts cleaned between unit and snapshot tests (saves ~11GB disk)
ci-test-all:
	@FAILURES=0; \
	STRIP_ANSI='s/\x1b\[[0-9;]*m//g'; \
	CI_JOBS=$$(nproc 2>/dev/null || echo 4); \
	echo "Running all tests (CI mode, $${CI_JOBS} parallel)..."; \
	echo ""; \
	echo "=== Build (release, thin LTO) ==="; \
	if CARGO_PROFILE_RELEASE_LTO=thin CARGO_PROFILE_RELEASE_CODEGEN_UNITS=4 \
	   cargo build --all-features --release 2>&1; then \
		BUILD_STATUS="PASS"; \
	else \
		BUILD_STATUS="FAIL"; \
		FAILURES=$$((FAILURES + 1)); \
	fi; \
	echo ""; \
	echo "=== Unit Tests ==="; \
	UNIT_TMPFILE=$$(mktemp); \
	set -o pipefail; \
	RUST_TEST_THREADS=$$CI_JOBS cargo test --all-features -- --test-threads=$$CI_JOBS --format=pretty \
		2>&1 | tee "$$UNIT_TMPFILE"; \
	UNIT_EXIT=$${PIPESTATUS[0]}; \
	tail -5 "$$UNIT_TMPFILE"; \
	UNIT_PASSED=$$(grep -E "^test result:" "$$UNIT_TMPFILE" | awk '{sum += $$4} END {print sum+0}'); \
	UNIT_FAILED=$$(grep -E "^test result:" "$$UNIT_TMPFILE" | awk '{sum += $$6} END {print sum+0}'); \
	rm -f "$$UNIT_TMPFILE"; \
	if [ $$UNIT_EXIT -ne 0 ]; then FAILURES=$$((FAILURES + 1)); fi; \
	echo ""; \
	echo "Cleaning debug artifacts to free disk space..."; \
	rm -rf target/debug 2>/dev/null || true; \
	echo ""; \
	echo "=== Snapshot Tests (E2E, $$CI_JOBS parallel) ==="; \
	SNAP_TMPFILE=$$(mktemp); \
	set -o pipefail; \
	CARGO_PROFILE_RELEASE_LTO=thin CARGO_PROFILE_RELEASE_CODEGEN_UNITS=4 \
	   ./scripts/run_snapshot_tests.sh --skip-build -j $$CI_JOBS 2>&1 | tee "$$SNAP_TMPFILE"; \
	SNAP_EXIT=$${PIPESTATUS[0]}; \
	tail -10 "$$SNAP_TMPFILE"; \
	SNAP_PASSED=$$(sed "$$STRIP_ANSI" "$$SNAP_TMPFILE" | grep -E "^Passed:" | awk '{print $$2}'); \
	SNAP_FAILED=$$(sed "$$STRIP_ANSI" "$$SNAP_TMPFILE" | grep -E "^Failed:" | awk '{print $$2}'); \
	rm -f "$$SNAP_TMPFILE"; \
	if [ $$SNAP_EXIT -ne 0 ]; then FAILURES=$$((FAILURES + 1)); fi; \
	echo ""; \
	echo "=== Python SDK Tests ==="; \
	if python -m pytest --version >/dev/null 2>&1; then \
		PY_TMPFILE=$$(mktemp); \
		set -o pipefail; \
		cd packages/inputlayer-py && python -m pytest tests/ -v 2>&1 | tee "$$PY_TMPFILE"; \
		PY_EXIT=$${PIPESTATUS[0]}; \
		cd ../..; \
		PY_LINE=$$(grep -E "passed" "$$PY_TMPFILE" | tail -1); \
		PY_PASSED=$$(echo "$$PY_LINE" | grep -oE '[0-9]+ passed' | awk '{print $$1}'); \
		PY_FAILED=$$(echo "$$PY_LINE" | grep -oE '[0-9]+ failed' | awk '{print $$1}'); \
		PY_PASSED=$${PY_PASSED:-0}; \
		PY_FAILED=$${PY_FAILED:-0}; \
		rm -f "$$PY_TMPFILE"; \
		if [ $$PY_EXIT -ne 0 ]; then FAILURES=$$((FAILURES + 1)); fi; \
	else \
		echo "SKIPPED: python/pytest not available (pip install -e 'packages/inputlayer-py[dev]')"; \
		PY_PASSED=0; PY_FAILED=0; \
	fi; \
	echo ""; \
	echo "==========================================="; \
	echo "           CI TEST SUMMARY"; \
	echo "==========================================="; \
	echo ""; \
	printf "  | %-14s | %-48s |\n" "Category" "Status"; \
	printf "  |----------------|--------------------------------------------------|\n"; \
	printf "  | %-14s | %-48s |\n" "Build" "$$BUILD_STATUS"; \
	printf "  | %-14s | %-48s |\n" "Unit Tests" "$$UNIT_PASSED passed, $$UNIT_FAILED failed"; \
	printf "  | %-14s | %-48s |\n" "Snapshot Tests" "$${SNAP_PASSED:-0} passed, $${SNAP_FAILED:-0} failed"; \
	printf "  | %-14s | %-48s |\n" "Python Tests" "$${PY_PASSED:-0} passed, $${PY_FAILED:-0} failed"; \
	echo ""; \
	echo "==========================================="; \
	if [ $$FAILURES -ne 0 ]; then \
		echo "SOME CHECKS FAILED ($$FAILURES)"; \
		exit 1; \
	fi; \
	echo "ALL CHECKS PASSED"

# CI target - runs all checks that CI performs
ci: check ci-test-all
	@echo "All CI checks passed!"

# Individual Test Tiers

# Tier 1: Unit tests (cargo test - includes all #[test] functions)
unit-test:
	cargo test --all-features

# Tier 2: Integration tests only
integration-test:
	cargo test --all-features --test '*'

# Tier 3: E2E snapshot tests (parallel, against live server)
e2e-test:
	./scripts/run_snapshot_tests.sh

# Regenerate snapshot .idl.out files (sequential mode)
e2e-update:
	./scripts/run_snapshot_tests.sh --update

# Run only tests affected by uncommitted changes
test-affected:
	./scripts/test-affected.sh

# Legacy alias for e2e-test
snapshot-test: e2e-test

# Tier 4: Python SDK tests (inputlayer-py package)
python-test:
	cd packages/inputlayer-py && python -m pytest tests/ -v

# Coverage & Static Analysis

# Generate test coverage report (requires cargo-tarpaulin)
coverage:
	@command -v cargo-tarpaulin >/dev/null 2>&1 || { echo "Install: cargo install cargo-tarpaulin"; exit 1; }
	@echo "=== Generating Coverage Report ==="
	@mkdir -p target/coverage
	cargo tarpaulin --all-features --out html --out json --output-dir target/coverage \
		--exclude-files "tests/*" --exclude-files "benches/*" --exclude-files "examples/*" \
		--exclude-files "src/bin/*" --exclude-files "src/main.rs" \
		--timeout 300 --skip-clean 2>&1
	@echo ""
	@echo "Coverage report: target/coverage/tarpaulin-report.html"
	@echo "Coverage JSON:   target/coverage/tarpaulin-report.json"

# Analyze coverage report and show untested code paths
view-coverage:
	@command -v cargo-tarpaulin >/dev/null 2>&1 || { echo "Install: cargo install cargo-tarpaulin"; exit 1; }
	@if [ ! -f target/coverage/tarpaulin-report.json ]; then \
		echo "No coverage data found. Running coverage first..."; \
		$(MAKE) coverage; \
	fi
	@echo ""
	@echo "=== Coverage Summary ==="
	@python3 -c " \
import json, sys; \
data = json.load(open('target/coverage/tarpaulin-report.json')); \
files = data.get('files', []); \
total_covered = sum(f.get('covered', 0) for f in files); \
total_coverable = sum(f.get('coverable', 0) for f in files); \
pct = (total_covered / total_coverable * 100) if total_coverable > 0 else 0; \
print(f'Overall: {total_covered}/{total_coverable} lines ({pct:.1f}%)'); \
print(); \
low = [(f['path'], f.get('covered',0), f.get('coverable',0)) for f in files \
       if f.get('coverable',0) > 10 and (f.get('covered',0)/f.get('coverable',1)*100) < 60]; \
low.sort(key=lambda x: x[1]/max(x[2],1)); \
if low: \
    print('=== Files Below 60% Coverage (need attention) ==='); \
    for path, cov, total in low[:30]: \
        print(f'  {cov/max(total,1)*100:5.1f}%  {cov:4d}/{total:4d}  {path}'); \
else: \
    print('All files above 60% coverage.'); \
print(); \
uncov = [(f['path'], f.get('covered',0), f.get('coverable',0)) for f in files \
         if f.get('coverable',0) > 0 and f.get('covered',0) == 0]; \
if uncov: \
    print('=== Completely Untested Files ==='); \
    for path, _, total in uncov: \
        print(f'  {total:4d} lines  {path}'); \
" 2>/dev/null || echo "Python3 required for coverage analysis. View HTML report: open target/coverage/tarpaulin-report.html"

# Static analysis beyond clippy (supply chain + doc verification)
static-analysis: lint doc-check
	@echo ""
	@echo "=== Static Analysis ==="
	@FAILURES=0; \
	echo "Clippy:     PASS (ran via lint target)"; \
	echo "Doc check:  PASS (ran via doc-check target)"; \
	if command -v cargo-deny >/dev/null 2>&1; then \
		if cargo deny check advisories sources licenses bans 2>&1; then \
			echo "Cargo deny: PASS"; \
		else \
			echo "Cargo deny: FAIL"; \
			FAILURES=$$((FAILURES + 1)); \
		fi; \
	else \
		echo "Cargo deny: SKIPPED (install: cargo install cargo-deny)"; \
	fi; \
	if [ $$FAILURES -ne 0 ]; then \
		echo ""; \
		echo "Static analysis FAILED"; \
		exit 1; \
	fi; \
	echo ""; \
	echo "Static analysis PASSED"

# Code Quality

# Format code
fmt:
	cargo fmt --all

# Check formatting (CI mode - fails if not formatted)
fmt-check:
	cargo fmt --all -- --check

# Run clippy lints
lint:
	cargo clippy --all-features -- -D warnings

# Check compilation + formatting + lints (quality gate)
check: fmt-check lint
	cargo check --all-features

# Fix formatting and lint issues automatically where possible
fix: fmt
	cargo clippy --all-features --fix --allow-dirty --allow-staged -- -D warnings

# Build

# Build the project (GUI + Rust)
build: gui-build
	cargo build --all-features

# Build in release mode (GUI + Rust)
build-release: gui-build
	cargo build --all-features --release

# Run unit tests in release mode
test-release:
	cargo test --all-features --release

# Clean build artifacts
clean:
	cargo clean
	rm -rf gui/dist gui/node_modules/.cache

# Documentation

# Build documentation
doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

# Check documentation (CI mode)
doc-check:
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

# Release & Maintenance

# Create a release branch with updated version
# Usage: make release VERSION=x.x.x
release:
ifndef VERSION
	$(error VERSION is not set. Usage: make release VERSION=x.x.x)
endif
	@echo "Creating release branch for version $(VERSION)..."
	@# Ensure we're on a clean working tree
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "Error: Working tree is not clean. Please commit or stash changes."; \
		exit 1; \
	fi
	@# Create and checkout release branch
	git checkout -b release/$(VERSION)
	@# Update version in Cargo.toml
	@sed -i.bak 's/^version = ".*"/version = "$(VERSION)"/' Cargo.toml && rm -f Cargo.toml.bak
	@# Update Cargo.lock
	cargo update --workspace
	@# Run CI checks to ensure everything is valid
	@$(MAKE) ci
	@# Commit changes
	git add Cargo.toml Cargo.lock
	git commit -m "chore: bump version to $(VERSION)"
	@# Push branch to origin
	git push -u origin release/$(VERSION)
	@echo ""
	@echo "Release branch 'release/$(VERSION)' created and pushed!"
	@echo "Next steps:"
	@echo "  1. Create a PR from release/$(VERSION) to main"
	@echo "  2. After merge, create and push tag: git tag v$(VERSION) && git push origin v$(VERSION)"

# Docker

# Build Docker image
docker:
	DOCKER_BUILDKIT=1 docker build -t inputlayer .

# Run Docker container
docker-run: docker
	docker run --rm -p 8080:8080 -v inputlayer-data:/var/lib/inputlayer/data inputlayer

# Deploy with TLS (production)
docker-deploy:
	docker compose up -d

# Deploy without TLS (development/behind load balancer)
docker-deploy-no-tls:
	docker compose -f docker-compose-no-tls.yml up -d

# View logs
docker-logs:
	docker compose logs -f

# Stop deployment
docker-stop:
	docker compose down

# Supply chain checks (licenses, advisories, banned crates)
deny:
	@command -v cargo-deny >/dev/null 2>&1 || { echo "Install: cargo install cargo-deny"; exit 1; }
	cargo deny check advisories sources licenses bans

# Flush development data - removes data folder to reset to empty state
flush-dev:
	@echo "Flushing development data..."
	@rm -rf ./data
	@echo "Data folder removed. Server will recreate default knowledge graph on next start."

# GUI (Web UI)

# Build the GUI dashboard (Next.js static export to gui/dist/)
gui-build:
	cd gui && npm ci && npm run build

# Build GUI + run the server (GUI served at http://localhost:8080/)
run: gui-build
	cargo run --bin inputlayer-server

# Run the server without rebuilding GUI (assumes gui/dist/ already exists)
run-server:
	cargo run --bin inputlayer-server

# Frontend Website

# Build the marketing website (static export to front/dist/)
front-build:
	cd front && npm ci && npm run build
	@echo "Frontend built to front/dist/"

# Build and deploy website to gh-pages branch
front-deploy: front-build
	@echo "Deploying frontend to gh-pages..."
	@TMPDIR=$$(mktemp -d); \
	cp -r front/dist/* "$$TMPDIR/"; \
	touch "$$TMPDIR/.nojekyll"; \
	cd "$$TMPDIR" && \
	git init && \
	git checkout -b main && \
	git add -A && \
	git commit -m "Deploy website" && \
	git remote add origin git@github.com:inputlayer/inputlayer.github.io.git && \
	git push -f origin main && \
	cd / && rm -rf "$$TMPDIR"; \
	echo "Deployed to https://inputlayer.github.io/"
