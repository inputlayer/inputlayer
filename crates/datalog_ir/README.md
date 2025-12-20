# datalog_ir

[![Crates.io](https://img.shields.io/crates/v/datalog_ir.svg)](https://crates.io/crates/datalog_ir)
[![Documentation](https://docs.rs/datalog_ir/badge.svg)](https://docs.rs/datalog_ir)
[![CI](https://github.com/jsam/datalog_ir/actions/workflows/ci.yml/badge.svg)](https://github.com/jsam/datalog_ir/actions/workflows/ci.yml)
[![License](https://img.shields.io/crates/l/datalog_ir.svg)](LICENSE-MIT)

Intermediate Representation (IR) types for Datalog query plans.

This library provides a canonical IR definition for representing Datalog query execution plans, suitable for use in query optimizers and execution engines.

## Features

- **IRNode** - Query plan operators (Scan, Map, Filter, Join, Distinct, Union)
- **Predicate** - Filter conditions with support for comparisons and logical operators
- **Schema tracking** - Automatic schema propagation through the query tree
- **Predicate utilities** - Column reference tracking, simplification, and projection adjustment

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
datalog_ir = "0.1"
```

## Quick Start

```rust
use datalog_ir::{IRNode, Predicate};

// Create a scan of the "edge" relation
let scan = IRNode::Scan {
    relation: "edge".to_string(),
    schema: vec!["x".to_string(), "y".to_string()],
};

// Add a filter: x > 5
let filtered = IRNode::Filter {
    input: Box::new(scan),
    predicate: Predicate::ColumnGtConst(0, 5),
};

// Project to just the "y" column
let projected = IRNode::Map {
    input: Box::new(filtered),
    projection: vec![1],
    output_schema: vec!["y".to_string()],
};

// Get the output schema
assert_eq!(projected.output_schema(), vec!["y"]);

// Pretty print for debugging
println!("{}", projected.pretty_print(0));
```

## IR Node Types

| Node | Description |
|------|-------------|
| `Scan` | Read from a relation (EDB or IDB) |
| `Map` | Project/transform columns |
| `Filter` | Select rows matching a predicate |
| `Join` | Multi-column equi-join of two inputs |
| `Distinct` | Remove duplicate rows |
| `Union` | Combine multiple inputs |

## Predicate Types

| Predicate | Description |
|-----------|-------------|
| `ColumnEqConst` | Column equals constant |
| `ColumnNeConst` | Column not equals constant |
| `ColumnGtConst` | Column greater than constant |
| `ColumnLtConst` | Column less than constant |
| `ColumnGeConst` | Column greater or equal |
| `ColumnLeConst` | Column less or equal |
| `ColumnsEq` | Two columns are equal |
| `ColumnsNe` | Two columns are not equal |
| `And` | Logical AND |
| `Or` | Logical OR |
| `True` | Always true (optimization) |
| `False` | Always false (optimization) |

## Example: Building a Join Query

```rust
use datalog_ir::IRNode;

// Scan two relations
let edges = IRNode::Scan {
    relation: "edge".to_string(),
    schema: vec!["a".to_string(), "b".to_string()],
};

let nodes = IRNode::Scan {
    relation: "node".to_string(),
    schema: vec!["id".to_string(), "label".to_string()],
};

// Join edge.b = node.id
let joined = IRNode::Join {
    left: Box::new(edges),
    right: Box::new(nodes),
    left_keys: vec![1],   // edge.b
    right_keys: vec![0],  // node.id
    output_schema: vec![
        "a".to_string(),
        "b".to_string(),
        "id".to_string(),
        "label".to_string(),
    ],
};
```

## Example: Predicate Manipulation

```rust
use datalog_ir::Predicate;

// Build a compound predicate: (x > 5) AND (y = z)
let predicate = Predicate::And(
    Box::new(Predicate::ColumnGtConst(0, 5)),
    Box::new(Predicate::ColumnsEq(1, 2)),
);

// Find referenced columns
let columns = predicate.referenced_columns();
assert!(columns.contains(&0));
assert!(columns.contains(&1));
assert!(columns.contains(&2));

// Simplify predicates with constant folding
let with_true = Predicate::And(
    Box::new(Predicate::True),
    Box::new(Predicate::ColumnGtConst(0, 5)),
);
let simplified = with_true.simplify();
assert_eq!(simplified, Predicate::ColumnGtConst(0, 5));
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
