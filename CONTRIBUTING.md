# Contributing to InputLayer

Thank you for your interest in contributing to InputLayer! This document provides guidelines for contributing to the project.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/inputlayer.git
   cd inputlayer
   ```
3. **Build the project**:
   ```bash
   cargo build
   ```
4. **Run the tests**:
   ```bash
   cargo test
   ./scripts/run_snapshot_tests.sh
   ```

## Development Workflow

### Branching

- Create a feature branch from `main`:
  ```bash
  git checkout -b feature/your-feature-name
  ```
- Keep branches focused on a single feature or fix

### Code Style

- Follow Rust standard formatting: `cargo fmt`
- Run clippy for lints: `cargo clippy`
- Write clear, descriptive commit messages

### Testing

- Add unit tests for new functionality
- Add snapshot tests for new Datalog features in `examples/datalog/`
- Ensure all existing tests pass before submitting

### Snapshot Tests

Snapshot tests compare actual output against expected `.out` files:

```bash
# Run all snapshot tests
./scripts/run_snapshot_tests.sh

# Run specific category
./scripts/run_snapshot_tests.sh -f recursion

# Update snapshots after intentional changes
./scripts/run_snapshot_tests.sh --update
```

## Pull Request Process

1. **Ensure tests pass**: Run `cargo test` and `./scripts/run_snapshot_tests.sh`
2. **Update documentation** if you've changed public APIs
3. **Write a clear PR description** explaining:
   - What the change does
   - Why it's needed
   - How to test it
4. **Request review** from maintainers

## Areas for Contribution

### Good First Issues

- Documentation improvements
- Additional snapshot tests
- Error message improvements
- Performance benchmarks

### Feature Contributions

- New aggregation functions
- Query optimization improvements
- Storage engine enhancements
- Additional vector distance functions

### Bug Fixes

- Check the [issue tracker](https://github.com/InputLayer/inputlayer/issues)
- Reproduce the bug first
- Include a test that fails without the fix

## Code of Conduct

- Be respectful and constructive
- Focus on technical merit
- Welcome newcomers

## License

By contributing to InputLayer, you agree that your contributions will be licensed under the Apache License 2.0.

## Questions?

- Open a [GitHub issue](https://github.com/InputLayer/inputlayer/issues)
- Check existing documentation

Thank you for contributing!
