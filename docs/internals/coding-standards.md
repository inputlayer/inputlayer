# InputLayer Coding Standards & Principles

> **Project Status**: Production-Ready
>
> The architecture is complete and tested (1567 unit tests + 1109 snapshot tests, as of 2026-02-08). This means:
> - APIs are stable but may evolve with clear migration paths
> - Breaking changes require justification and documentation
> - Deprecated code should be removed promptly after migration period

---

## Core Principles

### 1. Single Source of Truth

**Every concept should have exactly one canonical definition.**

```rust
// BAD: Same concept defined multiple times
// ast/mod.rs
pub enum AggregateFunc { Count, Sum, ... }

// ir/mod.rs
pub enum AggregateFunction { Count, Sum, ... }  // Different name!

// statement.rs
pub enum SerializableAggregateFunc { Count, Sum, ... }  // Third copy!
```

```rust
// GOOD: Single definition, re-exported where needed
// types/aggregates.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Aggregate { Count, Sum, ... }

// ast/mod.rs
pub use crate::types::Aggregate;

// ir/mod.rs
pub use crate::types::Aggregate;
```

**Why**: Prevents drift between definitions, eliminates conversion boilerplate, makes adding new variants trivial.

---

### 2. No Legacy Compatibility Code

**Delete deprecated code immediately. We have no users to break.**

```rust
// BAD: Keeping old code "just in case" (Tuple2 was REMOVED - this is the old pattern)
pub type OldFormat = (i32, i32);   // Legacy - DON'T DO THIS
pub struct Tuple(Vec<Value>);      // New

fn execute_old(...) -> Vec<OldFormat> { ... }  // Keep for compatibility?
fn execute_new(...) -> Vec<Tuple> { ... }      // The real one
```

```rust
// GOOD: One way to do things (Tuple is now the ONLY data format)
pub struct Tuple(Vec<Value>);

fn execute(...) -> Vec<Tuple> { ... }
```

**Why**: Duplicate code paths double maintenance burden, confuse new developers, and create subtle bugs when they diverge.

---

### 3. Types Flow Through the Pipeline

**Type information should be available at every stage, not just parsing.**

```rust
// BAD: Losing type info
struct IRNode {
    Scan { relation: String, schema: Vec<String> }  // Only names, no types!
}

// Later in code generator:
fn generate_filter(...) {
    // Can't validate types here because we don't have them
}
```

```rust
// GOOD: Types preserved
struct IRNode {
    Scan { relation: String, schema: TupleSchema }  // Full type info!
}

fn generate_filter(schema: &TupleSchema, predicate: &Predicate) {
    let col_type = schema.field_type(predicate.column());
    assert!(col_type.is_comparable(), "Invalid comparison on {:?}", col_type);
}
```

**Why**: Enables better error messages, catches bugs earlier, allows type-aware optimizations.

---

### 4. Small, Focused Modules

**Each file should have a single, clear purpose. If you can't describe it in one sentence, split it.**

```rust
// BAD: Monolithic file
// statement.rs (2,860 lines)
// - Meta command parsing
// - Schema parsing
// - Data operation parsing
// - Serialization types
// - Conversion functions
// - Query parsing
```

```rust
// GOOD: Focused modules
// statement/
//   mod.rs          - Re-exports and Statement enum
//   meta.rs         - Meta command parsing (.kg, .rule, .load)
//   schema.rs       - Schema declaration parsing
//   data.rs         - Insert/Delete/Update operations
//   serialize.rs    - JSON serialization types
//   parser.rs       - Core parsing utilities
```

**Guidelines**:
- Maximum 500 lines per file (prefer 200-300)
- One `pub struct` or `pub enum` as the primary export
- Helper functions should be private unless needed elsewhere

---

### 5. Explicit Over Implicit

**Be explicit about behavior. Avoid magic and hidden side effects.**

```rust
// BAD: Hidden behavior
fn add_fact(&mut self, relation: &str, data: Vec<(i32, i32)>) {
    // Secretly infers schema if not exists
    // Secretly converts to internal format
    // Secretly persists to disk
}
```

```rust
// GOOD: Explicit operations
fn ensure_schema(&mut self, relation: &str, schema: TupleSchema) -> Result<()>
fn insert_tuples(&mut self, relation: &str, tuples: Vec<Tuple>) -> Result<()>
fn persist(&mut self) -> Result<()>

// Usage is clear
engine.ensure_schema("edge", schema![("from", Int64), ("to", Int64)])?;
engine.insert_tuples("edge", tuples)?;
engine.persist()?;
```

---

### 6. Error Messages Are User Interface

**Errors should help users fix problems, not just report them.**

```rust
// BAD: Unhelpful error
Err("parse error".to_string())
```

```rust
// GOOD: Actionable error
Err(format!(
    "Unknown relation '{}' at line {}. Did you mean '{}'? \
     Available relations: {}",
    name, line, suggestion, available.join(", ")
))
```

**Error message checklist**:
- [ ] What went wrong?
- [ ] Where did it happen? (file, line, column if applicable)
- [ ] Why is it wrong?
- [ ] How can the user fix it?

---

### 7. Composition Over Inheritance

**Build complex behavior by combining simple pieces.**

```rust
// BAD: Deep trait hierarchies
trait Queryable { ... }
trait Filterable: Queryable { ... }
trait Joinable: Filterable { ... }
trait Aggregatable: Joinable { ... }
```

```rust
// GOOD: Composable operations
struct QueryPipeline {
    stages: Vec<Stage>,
}

enum Stage {
    Scan(ScanOp),
    Filter(FilterOp),
    Join(JoinOp),
    Aggregate(AggregateOp),
}

impl QueryPipeline {
    fn add_filter(mut self, predicate: Predicate) -> Self {
        self.stages.push(Stage::Filter(FilterOp { predicate }));
        self
    }
}
```

---

## Code Style Guidelines

### Naming Conventions

| Item | Convention | Example |
|------|------------|---------|
| Types | PascalCase | `TupleSchema`, `IRNode` |
| Functions | snake_case | `parse_statement`, `build_ir` |
| Constants | SCREAMING_SNAKE | `MAX_RECURSION_DEPTH` |
| Modules | snake_case | `code_generator`, `ir_builder` |
| Type parameters | Single uppercase | `T`, `E`, `F` |

**Naming principles**:
- Be descriptive: `parse_meta_command` not `pmc`
- Be consistent: if one function is `parse_X`, all similar functions are `parse_Y`
- Avoid abbreviations unless universal: `id` ok, `idx` ok, `rel` not ok (use `relation`)

### Function Size

**Functions should fit on one screen (~50 lines max).**

If a function is longer:
1. Extract helper functions
2. Use early returns to reduce nesting
3. Consider if it's doing too many things

```rust
// BAD: Too long, deeply nested
fn execute(&mut self, program: &str) -> Result<Vec<Tuple>> {
    // 200 lines of code with 5 levels of nesting
}

// GOOD: Composed of helpers
fn execute(&mut self, program: &str) -> Result<Vec<Tuple>> {
    let compiled = self.compile(program)?;
    let context = self.prepare_context()?;
    self.execute_rules(&compiled, &context)
}
```

### Comments

**Code should be self-documenting. Use comments for "why", not "what".**

```rust
// BAD: Restating the code
// Increment counter by 1
counter += 1;

// BAD: Explaining obvious code
// Check if the relation exists in the schema
if schema.contains(relation) { ... }
```

```rust
// GOOD: Explaining non-obvious decisions
// We use a BTreeMap instead of HashMap here because we need deterministic
// iteration order for reproducible query results across runs.
let relations: BTreeMap<String, Schema> = ...;

// GOOD: Explaining edge cases
// Empty body means this is a fact, not a rule. Facts are handled specially
// because they don't need fixpoint iteration.
if rule.body.is_empty() {
    return self.handle_fact(rule);
}
```

### Documentation

**All public items must have doc comments.**

```rust
/// Parse a Datalog statement from source text.
///
/// # Arguments
///
/// * `input` - The source text to parse
///
/// # Returns
///
/// The parsed statement, or an error if parsing fails.
///
/// # Examples
///
/// ```
/// let stmt = parse_statement("+edge(1, 2).")?;
/// assert!(matches!(stmt, Statement::Insert(_)));
/// ```
pub fn parse_statement(input: &str) -> Result<Statement, ParseError> {
    // ...
}
```

### Error Handling

**Use `Result` and `?` operator. Avoid `.unwrap()` except in tests.**

```rust
// BAD: Panics on error
let schema = catalog.get(relation).unwrap();

// BAD: Ignores error
let _ = persist_to_disk();

// GOOD: Propagates error with context
let schema = catalog.get(relation)
    .ok_or_else(|| SchemaError::UnknownRelation(relation.to_string()))?;

// GOOD: Handles error explicitly
match persist_to_disk() {
    Ok(()) => log::info!("Persisted successfully"),
    Err(e) => log::warn!("Failed to persist: {}", e),
}
```

---

## Module Organization

### Directory Structure

```
src/
├── lib.rs                    # DatalogEngine, public API
├── main.rs                   # Server binary
├── config.rs                 # TOML configuration
├── catalog.rs                # System catalog
├── ast/                      # Abstract Syntax Tree
│   └── mod.rs                # AST types, BuiltinFunc, AggregateFunc, Display
├── ir/                       # Intermediate Representation
│   └── mod.rs                # IRNode, IRExpression, BuiltinFunction
├── ir_builder/               # AST to IR conversion
│   └── mod.rs
├── parser/                   # Datalog parser
│   └── mod.rs                # Lexer + recursive descent parser
├── statement/                # Statement parsing
│   ├── mod.rs                # Statement enum
│   ├── parser.rs             # Statement-level parsing
│   ├── meta.rs               # Meta command parsing (.kg, .rule, .index)
│   ├── schema.rs             # Schema declaration parsing
│   ├── data.rs               # Insert/Delete operations
│   ├── serialize.rs          # JSON serialization types
│   └── types.rs              # Statement-related types
├── optimizer/                # Query optimization passes
│   └── mod.rs
├── join_planning/            # Join order optimization
│   └── mod.rs
├── sip_rewriting/            # Sideways Information Passing
│   └── mod.rs
├── subplan_sharing/          # Common subexpression detection
│   └── mod.rs
├── boolean_specialization/   # Set/bag semantics analysis
│   └── mod.rs
├── code_generator/           # IR to DD execution
│   └── mod.rs
├── storage_engine/           # Multi-KG storage
│   ├── mod.rs                # StorageEngine, KnowledgeGraph
│   └── snapshot.rs           # KnowledgeGraphSnapshot
├── storage/                  # Persistence layer
│   └── ...                   # FilePersist, WAL, Parquet batches
├── schema/                   # Schema management
│   ├── mod.rs
│   ├── catalog.rs            # SchemaCatalog (per-KG)
│   └── validator.rs
├── value/                    # Value types and tuples
│   ├── mod.rs                # Value, Tuple, Abomonation
│   └── arrow_convert.rs      # Arrow/Parquet conversion
├── protocol/                 # HTTP/API handler
│   └── handler.rs
├── dd_computation.rs         # DDComputation, DDCommand
├── derived_relations.rs      # DerivedRelationsManager
├── rule_catalog.rs           # RuleCatalog, validation
├── recursion.rs              # Recursion detection/handling
├── vector_ops.rs             # Vector, quantization, LSH functions
├── temporal_ops.rs           # Temporal functions
├── hnsw_index.rs             # HNSW index implementation
├── index_manager.rs          # Per-KG index management
└── pipeline_trace.rs         # Pipeline tracing/debugging
```

### Module Dependencies

**Dependencies should flow downward. Lower modules should not depend on higher ones.**

```
          lib.rs (public API)
              ↓
    ┌─────────┼─────────┐
    ↓         ↓         ↓
statement  optimizer  code_generator
    ↓         ↓         ↓
    └────→ ir/ast ←─────┘
              ↓
           types/
              ↓
           value/
```

**Forbidden**:
- `types/` depending on `ast/`
- `value/` depending on `ir/`
- Circular dependencies of any kind

---

## Testing Standards

### Test Organization

```
tests/
├── integration/        # Full pipeline tests
├── snapshots/          # Golden file tests
└── unit/               # Module-specific tests (or inline in src/)
```

### Test Naming

```rust
#[test]
fn test_<module>_<scenario>_<expected_outcome>() {
    // ...
}

// Examples:
fn test_parser_empty_input_returns_error() { ... }
fn test_join_two_relations_produces_cartesian_product() { ... }
fn test_aggregation_empty_group_returns_zero() { ... }
```

### Test Coverage Requirements

- All public functions must have at least one test
- All error paths should be tested
- Edge cases: empty input, single element, max values

### Snapshot Tests

**Use for complex output that's hard to assert programmatically.**

```
examples/datalog/
├── 01_basics/
│   ├── 01_simple_query.dl      # Input
│   └── 01_simple_query.dl.out  # Expected output
```

Update snapshots with:
```bash
./scripts/run_snapshot_tests.sh --update
```

---

## Performance Guidelines

### Avoid Unnecessary Allocations

```rust
// BAD: Creates intermediate String
let name = format!("{}", relation);
if catalog.contains(&name) { ... }

// GOOD: Works with &str directly
if catalog.contains(relation) { ... }
```

### Use Iterators Instead of Collecting

```rust
// BAD: Allocates intermediate Vec
let filtered: Vec<_> = items.iter().filter(|x| x.valid).collect();
let result: Vec<_> = filtered.iter().map(|x| x.value).collect();

// GOOD: Lazy evaluation
let result: Vec<_> = items.iter()
    .filter(|x| x.valid)
    .map(|x| x.value)
    .collect();
```

### Clone Consciously

```rust
// BAD: Cloning when not needed
fn process(data: Vec<Tuple>) {
    let copy = data.clone();  // Why?
    // ...
}

// GOOD: Take ownership or borrow
fn process(data: Vec<Tuple>) { ... }  // Takes ownership
fn process(data: &[Tuple]) { ... }    // Borrows
```

---

## Git Workflow

### Commit Messages

```
<type>: <short description>

<optional longer description>

<optional footer>
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `docs`: Documentation only
- `test`: Adding or updating tests
- `chore`: Build, CI, dependencies

**Examples**:
```
feat: add TopK aggregate function

Implements top-k selection with ordering column support.
Syntax: top_k<10, name, score:desc>

refactor: split statement.rs into focused modules

Improves maintainability by separating concerns:
- meta.rs: .kg, .rule, .load commands
- schema.rs: type declarations
- data.rs: insert/delete/update
```

### Branch Naming

```
<type>/<short-description>

feat/topk-aggregate
fix/parser-empty-input
refactor/split-statement-module
```

### Pull Request Checklist

Before merging:
- [ ] All tests pass (`cargo test`)
- [ ] Snapshot tests pass (`./scripts/run_snapshot_tests.sh`)
- [ ] No clippy warnings (`cargo clippy -- -D warnings`)
- [ ] Code is formatted (`cargo fmt`)
- [ ] Public APIs are documented
- [ ] Complex logic has comments explaining "why"

---

## Anti-Patterns to Avoid

### 1. The God Module

**Don't**: Put everything in one file because "it's easier".
**Do**: Split into focused modules from the start.

### 2. Stringly Typed APIs

**Don't**:
```rust
fn execute(&mut self, operation: &str, target: &str, data: &str) -> String
```

**Do**:
```rust
fn execute(&mut self, op: Operation) -> Result<ExecutionResult>
```

### 3. Boolean Parameters

**Don't**:
```rust
fn parse(input: &str, strict: bool, allow_empty: bool, normalize: bool)
```

**Do**:
```rust
struct ParseOptions {
    strict: bool,
    allow_empty: bool,
    normalize: bool,
}

fn parse(input: &str, options: ParseOptions)
```

### 4. Defensive Cloning

**Don't**: Clone "just to be safe" without understanding ownership.
**Do**: Understand borrowing and clone only when necessary.

### 5. Comment-Driven Development

**Don't**:
```rust
// TODO: fix this later
// HACK: this works but is ugly
// XXX: not sure why this is needed
```

**Do**: Fix it now or create a tracked issue.

---

## Checklist for New Code

Before submitting code, verify:

- [ ] Single responsibility: each function/module does one thing
- [ ] No duplicate type definitions
- [ ] Types flow through the pipeline (not lost after parsing)
- [ ] Errors are descriptive and actionable
- [ ] Public items are documented
- [ ] Tests cover happy path and error cases
- [ ] No `.unwrap()` outside of tests
- [ ] No unnecessary clones
- [ ] Code is formatted (`cargo fmt`)
- [ ] Clippy is happy (`cargo clippy`)

---

## Quick Reference

### Adding a New Aggregate Function

1. Add variant to `AggregateFunc` enum in `src/ast/mod.rs`
2. Add IR variant to `AggregateFunction` in `src/ir/mod.rs`
3. Add conversion in `src/ir_builder/mod.rs`
4. Add code generation in `src/code_generator/mod.rs`
5. Add tests in appropriate test file
6. Add snapshot test in `examples/datalog/`

### Adding a New Built-in Function

1. Add variant to `BuiltinFunc` enum in `src/ast/mod.rs`
2. Implement `parse()`, `arity()`, `as_str()` in `src/ast/mod.rs`
3. Add IR variant to `BuiltinFunction` in `src/ir/mod.rs`
4. Add conversion in `src/ir_builder/mod.rs`
5. Add evaluation in `src/code_generator/mod.rs`
6. Implement function logic in `src/vector_ops.rs` or `src/temporal_ops.rs`
7. Add tests and update `docs/reference/functions.md`

### Adding a New Statement Type

1. Add variant to `src/statement/mod.rs::Statement`
2. Add parsing in appropriate `src/statement/*.rs` file
3. Add handling in `src/protocol/handler.rs`
4. Add tests and snapshot tests

---

## Getting Help

- **Architecture questions**: Read `docs/internals/architecture.md`
- **API questions**: Check doc comments with `cargo doc --open`
- **Debugging**: Use `RUST_LOG=debug cargo run` for verbose output
- **Tests failing**: Run `./scripts/run_snapshot_tests.sh -v` for diffs
