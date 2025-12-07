# Plan: Supporting Standard Prolog/Datalog Syntax

## Problem Statement

The current system doesn't support standard Prolog/Datalog queries like:
```
default> ?- grandparent(1, X).
Error: Query execution failed: Constants in rule head not yet supported
```

And doesn't support atoms (lowercase identifiers as constants):
```prolog
parent(tom, liz).     -- 'tom' and 'liz' should be atoms (constants)
?- parent(tom, X).    -- find children of tom
```

Instead, it requires:
```
+parent(1, 2).           -- prefix operator required
+edge[(1, 2), (2, 3)].   -- only integers, only 2 columns
grandparent(X, Z) := parent(X, Y), parent(Y, Z).  -- := for persistent, not :-
```

## Root Cause Analysis

### 0. Constants in Query/Rule Head - **CRITICAL BUG**

**Location**: `src/ir_builder/mod.rs:368-371`
```rust
Term::Constant(_) => {
    // Constants in head are not yet supported
    return Err("Constants in rule head not yet supported".to_string());
}
```

**Impact**: Queries like `?- grandparent(1, X).` fail because:
1. The query is transformed into a rule: `__result__(1, X) :- grandparent(1, X).`
2. The IR builder sees `1` (a constant) in the head and rejects it
3. Error: "Constants in rule head not yet supported"

**Fix Required**: The IR builder must handle constants in the head by:
1. For queries: Convert head constants into body filters (e.g., `X = 1`)
2. Generate a filter node that constrains the output

### 1. Tuple2 = (i32, i32) Constraint - **NOT FUNDAMENTAL**

**Location**: `src/code_generator/mod.rs:147`
```rust
pub type Tuple2 = (i32, i32);
```

**Where enforced**:
- `src/storage_engine/mod.rs:214, 252, 290` - All insert/delete/query methods use `Vec<Tuple2>`
- `src/bin/client.rs:340-352` - Insert handler converts all terms to `(i32, i32)` pairs
- `src/storage/persist/mod.rs` - Persist layer stores `Tuple2`

**Reality**: The codebase already has `Tuple` (arbitrary arity with `Value` types):
- `src/value/mod.rs:650-784` - Full `Tuple` implementation with arbitrary arity
- `src/code_generator/mod.rs:187-229` - `generate_and_execute_tuples()` supports full `Tuple`
- **The constraint is in the storage layer API, not in DD or the code generator**

### 2. Integer-Only Values - **NOT FUNDAMENTAL**

**Location**: `src/bin/client.rs:346`
```rust
fn term_to_i32(term: &Term) -> Option<i32> { ... }
```

**Reality**: The parser and value system fully support multiple types:
- `src/statement.rs:527-568` - Parser handles strings, floats, integers
- `src/value/mod.rs:179-203` - `Value` enum has `Int32`, `Int64`, `Float64`, `String`, etc.
- **The constraint is in the client handler, not the parser or value system**

### 3. `+` Prefix Required - **DESIGN CHOICE (INTENTIONAL)**

**Location**: `src/statement.rs:329-332`
```rust
if input.starts_with('+') {
    return parse_insert(&input[1..]).map(Statement::Insert);
}
```

**Rationale**: The system intentionally chose DD-native semantics where:
- `+fact.` → insert (diff = +1)
- `-fact.` → delete (diff = -1)

This differs from standard Datalog where bare facts are implicitly insertions.

### 4. `:=` vs `:-` Distinction - **DESIGN CHOICE (INTENTIONAL)**

The system distinguishes:
- `:-` = transient rule (computed on demand, not persisted)
- `:=` = persistent view (stored in view catalog, incrementally maintained)

Standard Datalog uses `:-` for all rules without this distinction.

---

## Implementation Plan

### Phase 0: Fix Constants in Queries (CRITICAL)

**Goal**: Allow queries like `?- grandparent(1, X).` and `?- grandparent(1, 3).` to work.

**Root Cause Analysis**:

Current `handle_query` in `src/bin/client.rs:536-582`:
```rust
let vars: Vec<String> = goal.goal.args.iter()
    .filter_map(|t| match t {
        Term::Variable(v) => Some(v.clone()),
        _ => None,  // Constants are DROPPED!
    })
    .collect();

let head_vars = if vars.is_empty() {
    "1".to_string()  // BUG: "1" becomes Constant(1) in head
} else {
    vars.join(", ")
};

let program = format!("__query__({}) :- {}.", head_vars, body_parts.join(", "));
```

For `?- grandparent(1, 3).`:
- `vars = []` (no variables)
- `head_vars = "1"` (literal string)
- Program: `__query__(1) :- grandparent(1, 3).`
- IR builder sees `Constant(1)` in head → **ERROR**

**Solution**: Transform constants into temp variables + equality constraints.

#### Step 0.1: Fix handle_query in client.rs

**File**: `src/bin/client.rs`

Transform query by replacing constants with temp variables and adding equality constraints:

```rust
fn handle_query(state: &mut ReplState, goal: QueryGoal) -> Result<(), String> {
    let mut head_vars = Vec::new();
    let mut extra_constraints = Vec::new();

    // Transform: replace constants with temp vars, add equality constraints
    let transformed_args: Vec<String> = goal.goal.args.iter().enumerate()
        .map(|(i, term)| match term {
            Term::Variable(v) => {
                head_vars.push(v.clone());
                v.clone()
            }
            Term::Constant(val) => {
                let temp = format!("_c{}", i);
                head_vars.push(temp.clone());
                extra_constraints.push(format!("{} = {}", temp, val));
                temp
            }
            // Handle other term types similarly...
        })
        .collect();

    let body_atom = format!("{}({})", goal.goal.relation, transformed_args.join(", "));

    let mut body_parts = vec![body_atom];
    body_parts.extend(extra_constraints);
    // ... add other body predicates and constraints

    let program = format!("__query__({}) :- {}.", head_vars.join(", "), body_parts.join(", "));
    // ...
}
```

**Transformations**:
| Query | Transformed Program |
|-------|---------------------|
| `?- grandparent(1, 3).` | `__query__(_c0, _c1) :- grandparent(_c0, _c1), _c0 = 1, _c1 = 3.` |
| `?- grandparent(1, X).` | `__query__(_c0, X) :- grandparent(_c0, X), _c0 = 1.` |
| `?- grandparent(X, Y).` | `__query__(X, Y) :- grandparent(X, Y).` (unchanged) |

**Why this works**:
1. Head always has variables (no "Constants in head" error)
2. Equality constraints use existing IR filter support
3. Output matches query arity
4. No changes to IR builder needed!

### Phase 1: Extend Tuple Support in Storage Layer

**Goal**: Change from `Tuple2` to `Tuple` throughout the storage layer.

#### Step 1.1: Update Persist Layer
**File**: `src/storage/persist/mod.rs`

Change `Update` struct to use `Tuple` instead of `Tuple2`:
```rust
// Before
pub struct Update {
    pub data: Tuple2,
    pub time: u64,
    pub diff: i64,
}

// After
pub struct Update {
    pub data: Tuple,  // Arbitrary arity
    pub time: u64,
    pub diff: i64,
}
```

This requires updating serialization to handle variable-length tuples with mixed types.

#### Step 1.2: Update Storage Engine API
**File**: `src/storage_engine/mod.rs`

Change method signatures:
```rust
// Before
pub fn insert(&mut self, relation: &str, tuples: Vec<Tuple2>) -> StorageResult<()>
pub fn delete(&mut self, relation: &str, tuples: Vec<Tuple2>) -> StorageResult<()>
pub fn execute_query(&mut self, program: &str) -> StorageResult<Vec<Tuple2>>

// After
pub fn insert(&mut self, relation: &str, tuples: Vec<Tuple>) -> StorageResult<()>
pub fn delete(&mut self, relation: &str, tuples: Vec<Tuple>) -> StorageResult<()>
pub fn execute_query(&mut self, program: &str) -> StorageResult<Vec<Tuple>>
```

Keep backward-compatible helpers:
```rust
pub fn insert_pairs(&mut self, relation: &str, tuples: Vec<(i32, i32)>) -> StorageResult<()> {
    let tuples = tuples.into_iter().map(|(a, b)| Tuple::from_pair(a, b)).collect();
    self.insert(relation, tuples)
}
```

#### Step 1.3: Update Code Generator Bridge
**File**: `src/code_generator/mod.rs`

Remove the `Tuple2` → `Tuple` → `Tuple2` conversion roundtrip. Use `Tuple` end-to-end.

### Phase 2: Extend Term-to-Value Conversion

**Goal**: Convert parsed `Term` values to `Value` preserving types.

#### Step 2.1: Add term_to_value Function
**File**: `src/bin/client.rs`

```rust
fn term_to_value(term: &Term) -> Value {
    match term {
        Term::Constant(n) => Value::Int64(*n),
        Term::FloatConstant(f) => Value::Float64(*f),
        Term::StringConstant(s) => Value::string(s),
        Term::Variable(_) => Value::Null,  // Should not happen for facts
        Term::Placeholder => Value::Null,
        _ => Value::Null,
    }
}
```

#### Step 2.2: Update Insert Handler
**File**: `src/bin/client.rs`

```rust
// Before
let tuples: Vec<(i32, i32)> = op.tuples
    .iter()
    .filter_map(|tuple| {
        if tuple.len() >= 2 {
            Some((term_to_i32(&tuple[0])?, term_to_i32(&tuple[1])?))
        } else { None }
    })
    .collect();

// After
let tuples: Vec<Tuple> = op.tuples
    .iter()
    .map(|args| {
        Tuple::new(args.iter().map(term_to_value).collect())
    })
    .collect();
```

### Phase 3: Support Symbols (Atoms)

**Goal**: Support Prolog-style atoms like `tom`, `liz` without quotes.

#### Step 3.1: Update Parser
**File**: `src/statement.rs`

Currently, unquoted identifiers starting with lowercase are parsed as atoms/symbols in Prolog, but our parser treats them as variables.

Add symbol detection:
```rust
fn parse_single_term(input: &str) -> Result<Term, String> {
    // ... existing code ...

    // Lowercase identifier = symbol/atom (new)
    // Uppercase or underscore start = variable
    if let Some(first_char) = input.chars().next() {
        if first_char.is_lowercase() && input.chars().all(|c| c.is_alphanumeric() || c == '_') {
            // This is a symbol/atom - treat as string constant
            return Ok(Term::StringConstant(input.to_string()));
        }
    }

    // Uppercase or _ prefix = Variable
    if input.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Ok(Term::Variable(input.to_string()));
    }
}
```

**Note**: This is a breaking change. Current system treats `foo` as a variable. Standard Prolog treats `foo` as an atom and `Foo` as a variable.

### Phase 4: Relation Schema Management

**Goal**: Track schema per relation for type checking and display.

#### Step 4.1: Add Schema Registry
**File**: `src/storage_engine/mod.rs` (or new `src/schema_registry.rs`)

```rust
pub struct SchemaRegistry {
    schemas: HashMap<String, TupleSchema>,
}

impl SchemaRegistry {
    pub fn infer_or_validate(&mut self, relation: &str, tuple: &Tuple) -> Result<(), SchemaError> {
        if let Some(schema) = self.schemas.get(relation) {
            schema.validate(tuple)?;
        } else {
            // First insert - infer schema from tuple
            let schema = TupleSchema::infer_from_tuple(tuple);
            self.schemas.insert(relation.to_string(), schema);
        }
        Ok(())
    }
}
```

---

## Migration Strategy

### Backward Compatibility
1. Keep `insert_pairs()` helper for existing `Vec<(i32, i32)>` usage
2. Keep `+fact.` syntax working (it's unambiguous and useful)

### Breaking Changes
1. Symbol detection: `foo` becomes atom (string) instead of variable
2. `Foo` or `_foo` becomes variable
3. This is **required** for correct Datalog semantics - not optional

---

## Implementation Order

1. **Phase 0** (Constants in head) - **CRITICAL** - Fix `?- grandparent(1, X).` queries
2. **Phase 1** (Tuple support) - Required foundation for multi-arity
3. **Phase 2** (Term-to-Value) - Required for multi-type support
4. **Phase 3** (Atom vs Variable) - Required for correct Datalog semantics
5. **Phase 4** (Schema registry) - Required for validation

---

## Effort Estimate

| Phase | Complexity | Files Changed |
|-------|------------|---------------|
| **0.1 Constants in queries** | **Low** | **`bin/client.rs`** (~30 lines) |
| 1.1 Persist layer | Medium | `storage/persist/mod.rs` |
| 1.2 Storage API | Medium | `storage_engine/mod.rs` |
| 1.3 Code generator | Low | `code_generator/mod.rs` |
| 2.1-2.2 Term conversion | Low | `bin/client.rs` |
| 3.1 Atom vs Variable | Medium | `statement.rs`, `datalog_ast` |
| 4.1 Schema registry | Medium | New file + integration |

---

## Risks and Considerations

1. **Breaking changes**: Atom vs variable detection changes semantics for existing programs
2. **Performance**: Variable-length tuples may have serialization overhead
3. **Schema inference**: First tuple determines schema, could cause issues
4. **DD compatibility**: Need to verify Tuple serialization works with Abomonation

---

## Decisions Made

1. ✅ **Atom vs Variable**: Use standard Prolog semantics (lowercase = atom, uppercase/underscore = variable) - **Required for correct Datalog**
2. ✅ **Bare facts**: Keep `+` prefix requirement (maps to DD diff semantics)
3. ✅ **`:=` vs `:-`**: Keep current distinction (`:=` = persistent view, `:-` = transient rule) - **This is perfect**
