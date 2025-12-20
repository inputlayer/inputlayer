# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.4] - 2025-11-25

### Changed

- Bumped MSRV to 1.74 (required for `[lints]` table in Cargo.toml)
- Fixed Cargo.lock version compatibility

## [0.1.3] - 2025-11-25

### Fixed

- Switched to `gh` CLI for creating GitHub releases (more reliable permissions)

## [0.1.2] - 2025-11-25

### Fixed

- Fixed GitHub Actions workflow permissions for automated releases

## [0.1.1] - 2025-11-25

### Fixed

- Fixed CI workflow to use correct `dtolnay/rust-toolchain` action
- Fixed release workflow permissions for GitHub releases

## [0.1.0] - 2025-11-25

### Added

- Initial release
- `IRNode` enum with operators: `Scan`, `Map`, `Filter`, `Join`, `Distinct`, `Union`
- `Predicate` enum for filter conditions with comparison and logical operators
- Schema tracking via `output_schema()` method
- Pretty printing for debugging with `pretty_print()`
- Predicate utilities:
  - `referenced_columns()` - find all columns used in a predicate
  - `simplify()` - constant folding optimization
  - `adjust_for_projection()` - reindex columns after projection

[Unreleased]: https://github.com/jsam/datalog_ir/compare/v0.1.4...HEAD
[0.1.4]: https://github.com/jsam/datalog_ir/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/jsam/datalog_ir/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/jsam/datalog_ir/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/jsam/datalog_ir/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/jsam/datalog_ir/releases/tag/v0.1.0
