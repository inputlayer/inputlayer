# InputLayer Internals

This section contains developer documentation for contributing to InputLayer.

## Contents

### Architecture

| Document | Description |
|----------|-------------|
| [Architecture](architecture.md) | Complete system architecture with diagrams |
| [Type System](type-system.md) | Value types, coercion rules, Abomonation |
| [Validation](validation.md) | Validation layer design |

### Development

| Document | Description |
|----------|-------------|
| [Coding Standards](coding-standards.md) | Code style, patterns, and conventions |
| [Roadmap](roadmap.md) | Feature roadmap and version history |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          StorageEngine                               │
│              DashMap<String, Arc<RwLock<KnowledgeGraph>>>           │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                      KnowledgeGraph                             │ │
│  │                                                                 │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────┐ │ │
│  │  │ DatalogEngine │  │ RuleCatalog  │  │    DDComputation      │ │ │
│  │  │              │  │              │  │     (Optional)        │ │ │
│  │  │ input_tuples │  │ rules        │  │                       │ │ │
│  │  │ HashMap      │  │ catalog.json │  │ InputSessions         │ │ │
│  │  └──────────────┘  └──────────────┘  │ TraceAgents           │ │ │
│  │                                       │ Arrangements          │ │ │
│  │  ┌──────────────────────────────────┐│ ProbeHandle<u64>      │ │ │
│  │  │ ArcSwap<KnowledgeGraphSnapshot>  ││                       │ │ │
│  │  │                                  │└───────────────────────┘ │ │
│  │  │ Immutable point-in-time views    │                          │ │
│  │  └──────────────────────────────────┘                          │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                         FilePersist                             │ │
│  │            WAL (Write-Ahead Log) + Parquet Batch Files         │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Key Source Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | DatalogEngine, public API |
| `src/ast/mod.rs` | AST types, BuiltinFunc, Display |
| `src/value/mod.rs` | Value, Tuple, Abomonation |
| `src/storage_engine/mod.rs` | StorageEngine, KnowledgeGraph |
| `src/storage_engine/snapshot.rs` | KnowledgeGraphSnapshot |
| `src/dd_computation.rs` | DDComputation, DDCommand |
| `src/derived_relations.rs` | DerivedRelationsManager |
| `src/rule_catalog.rs` | RuleCatalog, validation |
| `src/code_generator/mod.rs` | CodeGenerator, execution |
| `src/protocol/handler.rs` | Request handling |

---

## Getting Started as a Contributor

### 1. Understand the Pipeline

```
Source Code → Parser → AST → IR Builder → IR → Optimizer → CodeGenerator → DD
```

### 2. Run Tests

```bash
# All tests
make test-all

# Unit tests only
cargo test

# Snapshot tests only
./scripts/run_snapshot_tests.sh
```

### 3. Adding a New Builtin Function

1. Add variant to `BuiltinFunc` enum in `src/ast/mod.rs`
2. Implement `parse()`, `arity()`, `as_str()` in `src/ast/mod.rs`
3. Add IR variant to `BuiltinFunction` in `src/ir/mod.rs`
4. Add conversion in `src/ir_builder/mod.rs`
5. Add evaluation in `src/code_generator/mod.rs`
6. Implement function logic in `src/vector_ops.rs` or `src/temporal_ops.rs`
7. Add tests and update `docs/reference/functions.md`

### 4. Adding a New Statement Type

1. Add variant to `src/statement/mod.rs::Statement`
2. Add parsing in appropriate `src/statement/*.rs` file
3. Add handling in `src/protocol/handler.rs`
4. Add tests and snapshot tests

---

## Test Coverage

- **1567 unit tests** across all modules (as of 2026-02-08)
- **1109 snapshot tests** for end-to-end validation

Tests are organized by category in `examples/datalog/`:
- `01_basics/` - Basic queries
- `16_vectors/` - Vector operations
- `29_temporal/` - Temporal functions
- `30_quantization/` - Int8 quantization
- `31_lsh/` - LSH functions
- `32_math/` - Math functions
