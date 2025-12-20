# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-11-25

### Added

- Initial release of `datalog_ast`
- Core AST types:
  - `Term` - Variables, constants, and placeholders
  - `Atom` - Relation atoms with arguments
  - `Constraint` - Comparison constraints (equality, inequality, ordering)
  - `BodyPredicate` - Positive and negated body literals
  - `Rule` - Datalog rules with head, body, and constraints
  - `Program` - Collection of Datalog rules
- Safety analysis for rules and programs
- Relation classification (EDB vs IDB)
- Recursive rule detection
- `Display` trait implementations for all types
- Comprehensive test suite

[Unreleased]: https://github.com/jsam/datalog_ast/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/jsam/datalog_ast/releases/tag/v0.1.0
