# InputLayer - Datalog Compiler & Engine

Incremental structured knowledge and memory layer for AI systems.


Beyond embeddings: a live, incrementally maintain knowledge state for you AI systems.

---

## What is This?

InputLayer is a **complete Datalog engine** that demonstrates the full compilation pipeline from Datalog source to executable queries. It serves as both:
1. **A working reference implementation** - Study the complete pipeline
2. **An exercise framework** - Optimization modules are extension points for course exercises

**Pipeline Architecture**:
```
Datalog Source
    |
    v
[Parser (M04)]                 -> AST
    |
    v
[Recursion Analysis]           -> has_recursion flag + strata
    |
    v
[IR Builder (M05)]             -> IRNode tree
    |
    v
[Join Planning (M07)]          -> Optimized join order (MST algorithm)
    |
    v
[SIP Rewriting (M08)]          -> Existence check filters (Yannakakis-style)
    |
    v
[Subplan Sharing (M09)]        -> Common subexpression elimination
    |
    v
[Boolean Specialization (M10)] -> Semiring selection (boolean/counting)
    |
    v
[Basic Optimizer (M06)]        -> Fused/simplified IRNode
    |
    v
[Code Generator (M11)]         -> DD Execution
    |
    v
Results
```

---

## Quick Start

### Run Tests

```bash
# Run all tests
cargo test

# Run specific package tests
cargo test -p datalog-engine --lib
```

### Run Examples

```bash
# Simple queries (scan, filter, projection)
cargo run --example simple_query

# Join queries (multi-relation)
cargo run --example join_query

# Full pipeline demonstration
cargo run --example pipeline_demo

# Pipeline with tracing (see each stage)
cargo run --example pipeline_trace_demo
```

---

## Using as a Library

### Basic Usage

```rust
use datalog_engine::DatalogEngine;

// Create engine
let mut engine = DatalogEngine::new();

// Add base facts
engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

// Execute Datalog query
let program = "result(x, y) :- edge(x, y), x > 1.";
let results = engine.execute(program)?;
```

### With Pipeline Tracing

```rust
use datalog_engine::DatalogEngine;

let mut engine = DatalogEngine::new();
engine.add_fact("edge", vec![(1, 2), (2, 3)]);

// Execute with full trace
let (results, trace) = engine.execute_with_trace("path(x,y) :- edge(x,y).")?;

// Inspect each pipeline stage
println!("AST: {:?}", trace.ast);
println!("IR before optimization: {:?}", trace.ir_before);
println!("IR after optimization: {:?}", trace.ir_after);
```

---

## Project Structure

```
final-project/
|-- src/
|   |-- lib.rs              # Main engine, pipeline orchestration
|   |-- main.rs             # CLI (datalog-repl)
|   |
|   |   # FULLY IMPLEMENTED (study these as reference)
|   |-- parser/             # Module 04: Datalog parser
|   |-- catalog.rs          # Module 04: Schema management
|   |-- ir_builder/         # Module 05: IR construction
|   |-- optimizer/          # Module 06: Basic optimizations
|   |-- code_generator/     # Module 11: DD code generation
|   |-- recursion.rs        # Module 11: SCC detection, stratification
|   |
|   |   # OPTIMIZATION MODULES (full implementations)
|   |-- join_planning/      # Module 07: Join ordering with MST algorithm
|   |-- sip_rewriting/      # Module 08: Sideways Information Passing
|   |-- subplan_sharing/    # Module 09: Common subexpression elimination
|   |-- boolean_specialization/ # Module 10: Semiring specialization
|   |
|   |   # PRODUCTION FEATURES (bonus, not course modules)
|   |-- storage_engine/     # Multi-database persistence
|   |-- storage/            # Parquet storage implementation
|   |-- config.rs           # Configuration management
|   |-- pipeline_trace.rs   # Debug tracing
|
|-- examples/
|   |-- rust/               # Working Rust examples
|   |   |-- simple_query.rs
|   |   |-- join_query.rs
|   |   |-- pipeline_demo.rs
|   |   |-- pipeline_trace_demo.rs
|   |
|   |-- datalog/            # Benchmark datasets (11 folders)
|
|-- tests/                  # Integration tests
|-- Cargo.toml
```

---

## Course Module Mapping

This table shows exactly which course module corresponds to which final-project component:

| Course Module | Topic | Final-Project File | Implementation Status | Student Exercise |
|---------------|-------|-------------------|----------------------|------------------|
| 01-03 | Theory | - | External (DD library) | Reading & manual tracing |
| **04** | Parsing & Catalog | `parser/`, `catalog.rs` | **COMPLETE** | Build parser from scratch |
| **05** | IR Construction | `ir_builder/` | **COMPLETE** | Build IR builder from scratch |
| **06** | Logic Fusion | `optimizer/` | **COMPLETE** | Implement fusion rules |
| **07** | Join Planning | `join_planning/` | **COMPLETE** (MST, JST) | Study reference, implement variants |
| **08** | SIP Rewriting | `sip_rewriting/` | **COMPLETE** (Yannakakis) | Study reference, extend for recursion |
| **09** | Subplan Sharing | `subplan_sharing/` | **COMPLETE** (CSE) | Study reference, implement view materialization |
| **10** | Boolean Specialization | `boolean_specialization/` | **COMPLETE** (Semiring) | Study reference, add aggregations |
| **11** | Code Generation | `code_generator/`, `recursion.rs` | **COMPLETE** | Implement `.iterative()` scopes |

### Optimization Module Architecture

All optimization modules (07-10) are now fully implemented with production-quality algorithms:

```
                    FINAL-PROJECT DESIGN
+----------------------------------------------------------+
|                                                          |
|  +--------+   +--------+   +---------+   +--------+     |
|  | Parser | -> |   IR   | -> |Optimizer| -> |CodeGen|    |
|  |  M04   |   | Builder|   |   M06   |   |  M11  |       |
|  |   OK   |   |  M05   |   |   OK    |   |   OK  |       |
|  +--------+   |   OK   |   +---------+   +--------+      |
|               +--------+                                 |
|                    |                                     |
|                    v                                     |
|  +---------------------------------------------------+  |
|  |        OPTIMIZATION PIPELINE (Fully Implemented)  |  |
|  |                                                   |  |
|  |   M07         M08          M09           M10      |  |
|  |  Join      -> SIP      -> Subplan   -> Boolean    |  |
|  | Planning    Rewriting    Sharing     Specialization  |
|  |   [OK]        [OK]         [OK]          [OK]     |  |
|  |                                                   |  |
|  |  M07: MST algorithm, structural cost model        |  |
|  |  M08: Yannakakis-style semijoin filtering         |  |
|  |  M09: Canonicalization & CSE detection            |  |
|  |  M10: Semiring analysis & specialization          |  |
|  +---------------------------------------------------+  |
|                                                          |
|  OK = Fully implemented (study as reference)             |
|                                                          |
+----------------------------------------------------------+
```

---

## Module Implementation Details

### Fully Implemented Modules (Study These)

| Module | File | What's Implemented |
|--------|------|-------------------|
| 04 | `parser/mod.rs` | Complete parser: rules, atoms, terms, constraints, negation |
| 04 | `catalog.rs` | Schema management, join key inference |
| 05 | `ir_builder/mod.rs` | IR construction: scans, joins, filters, projections |
| 06 | `optimizer/mod.rs` | Identity elimination, true/false filter removal, fixpoint iteration |
| **07** | `join_planning/mod.rs` | **Join graph construction, MST via Prim's algorithm, rooted JST, structural cost model** |
| **08** | `sip_rewriting/mod.rs` | **Yannakakis-style SIP, adornment computation, existence filters, binding patterns** |
| **09** | `subplan_sharing/mod.rs` | **IR canonicalization, structural hashing, CSE detection, sharing statistics** |
| **10** | `boolean_specialization/mod.rs` | **Semiring analysis (Boolean/Counting/Min/Max), constraint propagation, annotation** |
| 11 | `code_generator/mod.rs` | IR->DD translation: scan, map, filter, join, distinct, union |
| 11 | `recursion.rs` | Tarjan's SCC algorithm, stratification, dependency graphs |

### Module 07-10 Implementation Details

| Module | Algorithm | Key Structures | Test Count |
|--------|-----------|----------------|------------|
| 07 | Maximum Spanning Tree (Prim's) | `JoinGraph`, `JoinGraphEdge`, `RootedJST` | 11 tests |
| 08 | Yannakakis-style SIP | `Adornment`, `SipTraversal`, `SipStats` | 12 tests |
| 09 | Hash-based CSE | `CanonicalSubtree`, `SharingStats` | 8 tests |
| 10 | Semiring Lattice | `SemiringType`, `SemiringAnnotation`, `SpecializationStats` | 13 tests |

---

## Configuration

The engine supports optional optimization configuration:

```rust
use datalog_engine::{DatalogEngine, OptimizationConfig};

let config = OptimizationConfig {
    enable_join_planning: true,       // Module 07
    enable_sip_rewriting: true,       // Module 08
    enable_subplan_sharing: true,     // Module 09
    enable_boolean_specialization: false, // Module 10 (opt-in)
};

let engine = DatalogEngine::with_config(config);
```

---

## Examples

### Rust Examples

| Example | Description | Status |
|---------|-------------|--------|
| `simple_query.rs` | Scan, filter, projection queries | Working |
| `join_query.rs` | Multi-relation join queries | Working |
| `pipeline_demo.rs` | Complete pipeline demonstration | Working |
| `pipeline_trace_demo.rs` | Traced execution with stage output | Working |

### Datalog Benchmark Datasets

See `examples/datalog/` for 11 benchmark folders:
- Small: `pointsto/`, `galen/`, `borrow/`
- Medium: `batik/`, `biojava/`, `cvc5/`, `z3/`, `crdt/`
- Large: `eclipse/`, `xalan/`, `zxing/`
- Generic: `generic-examples/` (11 .dl files)

---

## API Reference

### DatalogEngine

```rust
pub struct DatalogEngine { ... }

impl DatalogEngine {
    /// Create new engine with default config
    pub fn new() -> Self;

    /// Create with custom optimization config
    pub fn with_config(config: OptimizationConfig) -> Self;

    /// Add base facts for a relation
    pub fn add_fact(&mut self, relation: &str, data: Vec<(i32, i32)>);

    /// Execute full pipeline: parse -> IR -> optimize -> execute
    pub fn execute(&mut self, source: &str) -> Result<Vec<(i32, i32)>, String>;

    /// Execute with pipeline trace for debugging
    pub fn execute_with_trace(&mut self, source: &str)
        -> Result<(Vec<(i32, i32)>, PipelineTrace), String>;

    /// Check if program has recursive rules
    pub fn is_recursive(&self) -> bool;

    /// Get computed strata for evaluation order
    pub fn strata(&self) -> &[Vec<usize>];
}
```

---

## Known Limitations

### Current Implementation

1. **Non-recursive queries only** - The code generator handles non-recursive queries. Recursive queries are detected and stratified, but `.iterative()` code generation is left for student exercises.

2. **2-tuple limitation** - Current implementation uses `(i32, i32)` tuples for simplicity. Production systems would support variable arity.

3. **Optimization scope** - Modules 07-10 implement core algorithms but not all advanced features (e.g., delta rules for recursion).

### Future Extensions (Student Exercises)

Modules 07-10 are fully implemented. Students can extend them by:

- **Module 07**: Add cardinality estimation, implement alternative cost models
- **Module 08**: Extend for recursive queries with delta rules and magic sets
- **Module 09**: Add view materialization and arrangement sharing
- **Module 10**: Add custom semirings for domain-specific aggregations
- **Module 11**: Add recursive code generation with `.iterative()` scopes

---

## Troubleshooting

**Tests fail to compile**: Run `cargo clean && cargo build`

**No results returned**: Ensure you add facts with `add_fact()` before executing queries

**"Identity transform" in output**: This is expected - optimization modules pass IR unchanged by design

---

## Learning Path

1. **Start with lib.rs** - Understand the overall pipeline orchestration
2. **Study parser/** - See how Datalog is parsed (Module 04)
3. **Study ir_builder/** - See how AST becomes IR (Module 05)
4. **Study optimizer/** - See basic optimizations (Module 06)
5. **Study code_generator/** - See IR-to-DD translation (Module 11)
6. **Examine identity transforms** - Understand extension points (Modules 07-10)
7. **Run examples** - See the complete pipeline in action

---

## Project Status

**Implementation**: Complete reference implementation
**Tests**: All passing
**Examples**: All working
**Documentation**: Comprehensive

This final-project provides:
- A complete, working Datalog engine for non-recursive queries
- Clear module boundaries matching course structure
- Extension points for student exercises
- Production features (storage engine) for advanced exploration

---

**Course**: InputLayer Reference Implementation
**Last Updated**: 2025-11-25
