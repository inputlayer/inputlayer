.PHONY: all ci fmt fmt-check lint test test-fast test-release unit-test integration-test e2e-test e2e-update test-affected doc doc-check check build build-release clean fix release snapshot-test test-all ci-test-all flush-dev docker docker-run

# Default target
all: ci

# Developer Workflow

# Fast feedback loop - unit + integration tests only (~30s)
test-fast: unit-test
	@echo "Fast tests complete."

# Pre-commit gate - unit + integration + snapshot E2E (~60-90s)
test: unit-test e2e-test
	@echo "All tests complete."

# Full verification (CI, pre-merge)
# Optimized flow:
#   1. Build release binaries (verifies compilation + pre-builds for snapshot runner)
#   2. Unit tests (dev build, fast - deps already cached from step 1)
#   3. Snapshot tests in parallel (release binaries already built, skips rebuild)
test-all:
	@FAILURES=0; \
	STRIP_ANSI='s/\x1b\[[0-9;]*m//g'; \
	NCPU=$$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4); \
	echo "Running all tests ($$NCPU CPUs)..."; \
	echo ""; \
	echo "=== Build (release) ==="; \
	if cargo build --all-features --release 2>&1; then \
		BUILD_STATUS="PASS"; \
	else \
		BUILD_STATUS="FAIL"; \
		FAILURES=$$((FAILURES + 1)); \
	fi; \
	echo ""; \
	echo "=== Unit Tests ==="; \
	UNIT_OUTPUT=$$(cargo test --all-features 2>&1); \
	UNIT_EXIT=$$?; \
	echo "$$UNIT_OUTPUT" | tail -5; \
	UNIT_PASSED=$$(echo "$$UNIT_OUTPUT" | grep -E "^test result:" | awk '{sum += $$4} END {print sum+0}'); \
	UNIT_FAILED=$$(echo "$$UNIT_OUTPUT" | grep -E "^test result:" | awk '{sum += $$6} END {print sum+0}'); \
	if [ $$UNIT_EXIT -ne 0 ]; then FAILURES=$$((FAILURES + 1)); fi; \
	echo ""; \
	echo "=== Snapshot Tests (E2E, $$NCPU parallel) ==="; \
	SNAP_OUTPUT=$$(./scripts/run_snapshot_tests.sh -j $$NCPU 2>&1); \
	SNAP_EXIT=$$?; \
	echo "$$SNAP_OUTPUT" | tail -10; \
	SNAP_PASSED=$$(echo "$$SNAP_OUTPUT" | sed "$$STRIP_ANSI" | grep -E "^Passed:" | awk '{print $$2}'); \
	SNAP_FAILED=$$(echo "$$SNAP_OUTPUT" | sed "$$STRIP_ANSI" | grep -E "^Failed:" | awk '{print $$2}'); \
	if [ $$SNAP_EXIT -ne 0 ]; then FAILURES=$$((FAILURES + 1)); fi; \
	echo ""; \
	echo "==========================================="; \
	echo "              TEST SUMMARY"; \
	echo "==========================================="; \
	echo ""; \
	printf "  | %-14s | %-48s |\n" "Category" "Status"; \
	printf "  |----------------|--------------------------------------------------|\n"; \
	printf "  | %-14s | %-48s |\n" "Build" "$$BUILD_STATUS"; \
	printf "  | %-14s | %-48s |\n" "Unit Tests" "$$UNIT_PASSED passed, $$UNIT_FAILED failed"; \
	printf "  | %-14s | %-48s |\n" "Snapshot Tests" "$${SNAP_PASSED:-0} passed, $${SNAP_FAILED:-0} failed"; \
	echo ""; \
	echo "==========================================="; \
	if [ $$FAILURES -ne 0 ]; then \
		echo "SOME CHECKS FAILED ($$FAILURES)"; \
		exit 1; \
	fi; \
	echo "ALL CHECKS PASSED"

# CI-friendly full verification (memory-constrained runners: 16GB RAM)
# Same checks as test-all but with:
#   - Thin LTO instead of full LTO (halves linker peak memory)
#   - More codegen units (reduces per-unit memory)
#   - Capped test parallelism (DD workers are memory-hungry)
#   - Capped snapshot parallelism
ci-test-all:
	@FAILURES=0; \
	STRIP_ANSI='s/\x1b\[[0-9;]*m//g'; \
	CI_JOBS=2; \
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
	UNIT_OUTPUT=$$(RUST_TEST_THREADS=$$CI_JOBS cargo test --all-features 2>&1); \
	UNIT_EXIT=$$?; \
	echo "$$UNIT_OUTPUT" | tail -5; \
	UNIT_PASSED=$$(echo "$$UNIT_OUTPUT" | grep -E "^test result:" | awk '{sum += $$4} END {print sum+0}'); \
	UNIT_FAILED=$$(echo "$$UNIT_OUTPUT" | grep -E "^test result:" | awk '{sum += $$6} END {print sum+0}'); \
	if [ $$UNIT_EXIT -ne 0 ]; then FAILURES=$$((FAILURES + 1)); fi; \
	echo ""; \
	echo "=== Snapshot Tests (E2E, $$CI_JOBS parallel) ==="; \
	SNAP_OUTPUT=$$(CARGO_PROFILE_RELEASE_LTO=thin CARGO_PROFILE_RELEASE_CODEGEN_UNITS=4 \
	   ./scripts/run_snapshot_tests.sh -j $$CI_JOBS 2>&1); \
	SNAP_EXIT=$$?; \
	echo "$$SNAP_OUTPUT" | tail -10; \
	SNAP_PASSED=$$(echo "$$SNAP_OUTPUT" | sed "$$STRIP_ANSI" | grep -E "^Passed:" | awk '{print $$2}'); \
	SNAP_FAILED=$$(echo "$$SNAP_OUTPUT" | sed "$$STRIP_ANSI" | grep -E "^Failed:" | awk '{print $$2}'); \
	if [ $$SNAP_EXIT -ne 0 ]; then FAILURES=$$((FAILURES + 1)); fi; \
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

# Regenerate snapshot .dl.out files (sequential mode)
e2e-update:
	./scripts/run_snapshot_tests.sh --update

# Run only tests affected by uncommitted changes
test-affected:
	./scripts/test-affected.sh

# Legacy alias for e2e-test
snapshot-test: e2e-test

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

# Check compilation + formatting + lints + docs (quality gate)
check: fmt-check lint doc-check
	cargo check --all-features

# Fix formatting and lint issues automatically where possible
fix: fmt
	cargo clippy --all-features --fix --allow-dirty --allow-staged -- -D warnings

# Build

# Build the project
build:
	cargo build --all-features

# Build in release mode
build-release:
	cargo build --all-features --release

# Run unit tests in release mode
test-release:
	cargo test --all-features --release

# Clean build artifacts
clean:
	cargo clean

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

# Flush development data - removes data folder to reset to empty state
flush-dev:
	@echo "Flushing development data..."
	@rm -rf ./data
	@echo "Data folder removed. Server will recreate default knowledge graph on next start."
