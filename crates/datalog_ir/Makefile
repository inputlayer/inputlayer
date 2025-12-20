.PHONY: all ci fmt fmt-check lint test test-release doc doc-check check build clean release

# Default target
all: ci

# CI target - runs all checks that CI performs
ci: fmt-check lint test test-release doc-check check
	@echo "All CI checks passed!"

# Format code
fmt:
	cargo fmt --all

# Check formatting (CI mode - fails if not formatted)
fmt-check:
	cargo fmt --all -- --check

# Run clippy lints
lint:
	cargo clippy --all-features -- -D warnings

# Run tests
test:
	cargo test --all-features

# Run tests in release mode
test-release:
	cargo test --all-features --release

# Build documentation
doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

# Check documentation (CI mode)
doc-check:
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

# Check compilation (used for MSRV)
check:
	cargo check --all-features

# Build the project
build:
	cargo build --all-features

# Build in release mode
build-release:
	cargo build --all-features --release

# Clean build artifacts
clean:
	cargo clean

# Fix formatting and lint issues automatically where possible
fix: fmt
	cargo clippy --all-features --fix --allow-dirty --allow-staged

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
