# datalog_ast

[![Crates.io](https://img.shields.io/crates/v/datalog_ast.svg)](https://crates.io/crates/datalog_ast)
[![Documentation](https://docs.rs/datalog_ast/badge.svg)](https://docs.rs/datalog_ast)
[![License](https://img.shields.io/crates/l/datalog_ast.svg)](https://github.com/jsam/datalog_ast#license)
[![CI](https://github.com/jsam/datalog_ast/actions/workflows/ci.yml/badge.svg)](https://github.com/jsam/datalog_ast/actions/workflows/ci.yml)

Abstract Syntax Tree types for Datalog programs.

## Overview

`datalog_ast` provides a set of Rust data structures for representing Datalog programs. It is designed as a shared, reusable component that can be used across parsers, evaluators, and analyzers for consistency in Datalog program representation.

## Features

- **Core AST Types**: `Term`, `Atom`, `Constraint`, `BodyPredicate`, `Rule`, and `Program`
- **Stratified Negation**: Support for negated literals in rule bodies
- **Comparison Constraints**: Full support for arithmetic comparisons (`<`, `<=`, `>`, `>=`, `==`, `!=`)
- **Safety Analysis**: Built-in methods to check rule and program safety
- **Relation Classification**: Automatic identification of EDB (extensional) and IDB (intensional) relations
- **Zero Dependencies**: Pure Rust implementation with no external dependencies

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
datalog_ast = "0.1"
```

## Quick Start

```rust
use datalog_ast::{Atom, BodyPredicate, Program, Rule, Term};

// Build a simple transitive closure program:
// reach(x) :- source(x).
// reach(y) :- reach(x), edge(x, y).

let mut program = Program::new();

// reach(x) :- source(x).
program.add_rule(Rule::new_simple(
    Atom::new("reach".into(), vec![Term::Variable("x".into())]),
    vec![Atom::new("source".into(), vec![Term::Variable("x".into())])],
    vec![],
));

// reach(y) :- reach(x), edge(x, y).
program.add_rule(Rule::new_simple(
    Atom::new("reach".into(), vec![Term::Variable("y".into())]),
    vec![
        Atom::new("reach".into(), vec![Term::Variable("x".into())]),
        Atom::new("edge".into(), vec![
            Term::Variable("x".into()),
            Term::Variable("y".into()),
        ]),
    ],
    vec![],
));

// Analyze the program
assert!(program.is_safe());
assert_eq!(program.idbs(), vec!["reach"]);
assert_eq!(program.edbs(), vec!["edge", "source"]);
```

## Core Types

### Term

Represents variables, constants, or placeholders in Datalog:

```rust
use datalog_ast::Term;

let var = Term::Variable("x".into());
let constant = Term::Constant(42);
let placeholder = Term::Placeholder;  // Represents "_"

assert!(var.is_variable());
assert!(constant.is_constant());
```

### Atom

Represents a relation with arguments, like `edge(x, y)`:

```rust
use datalog_ast::{Atom, Term};

let atom = Atom::new(
    "edge".into(),
    vec![Term::Variable("x".into()), Term::Variable("y".into())],
);

assert_eq!(atom.relation, "edge");
assert_eq!(atom.arity(), 2);
assert!(atom.variables().contains("x"));
```

### Constraint

Represents comparison constraints in rule bodies:

```rust
use datalog_ast::{Constraint, Term};

let constraint = Constraint::LessThan(
    Term::Variable("x".into()),
    Term::Constant(100),
);
```

Available constraint types:
- `Equal`, `NotEqual`
- `LessThan`, `LessOrEqual`
- `GreaterThan`, `GreaterOrEqual`

### BodyPredicate

Represents positive or negated atoms in rule bodies (for stratified negation):

```rust
use datalog_ast::{Atom, BodyPredicate, Term};

let positive = BodyPredicate::Positive(
    Atom::new("edge".into(), vec![Term::Variable("x".into())])
);
let negated = BodyPredicate::Negated(
    Atom::new("visited".into(), vec![Term::Variable("x".into())])
);

assert!(positive.is_positive());
assert!(negated.is_negated());
```

### Rule

Represents a Datalog rule with head, body predicates, and constraints:

```rust
use datalog_ast::{Atom, BodyPredicate, Constraint, Rule, Term};

// path(x, z) :- edge(x, y), path(y, z), x != z.
let rule = Rule::new(
    Atom::new("path".into(), vec![
        Term::Variable("x".into()),
        Term::Variable("z".into()),
    ]),
    vec![
        BodyPredicate::Positive(Atom::new("edge".into(), vec![
            Term::Variable("x".into()),
            Term::Variable("y".into()),
        ])),
        BodyPredicate::Positive(Atom::new("path".into(), vec![
            Term::Variable("y".into()),
            Term::Variable("z".into()),
        ])),
    ],
    vec![Constraint::NotEqual(
        Term::Variable("x".into()),
        Term::Variable("z".into()),
    )],
);

assert!(rule.is_safe());
assert!(rule.is_recursive());
```

### Program

Represents a complete Datalog program:

```rust
use datalog_ast::Program;

let program = Program::new();

// Program analysis methods:
// - program.idbs()           - Get IDB (derived) relations
// - program.edbs()           - Get EDB (base) relations
// - program.all_relations()  - Get all relation names
// - program.is_safe()        - Check if all rules are safe
// - program.recursive_rules() - Get recursive rules
```

## Safety

A Datalog rule is **safe** if every variable in the head appears in at least one positive body atom. This library provides built-in safety checking:

```rust
use datalog_ast::{Atom, Rule, Term};

// Safe rule: y appears in edge(x, y)
let safe_rule = Rule::new_simple(
    Atom::new("reach".into(), vec![Term::Variable("y".into())]),
    vec![Atom::new("edge".into(), vec![
        Term::Variable("x".into()),
        Term::Variable("y".into()),
    ])],
    vec![],
);
assert!(safe_rule.is_safe());

// Unsafe rule: z doesn't appear in any positive body atom
let unsafe_rule = Rule::new_simple(
    Atom::new("bad".into(), vec![Term::Variable("z".into())]),
    vec![Atom::new("edge".into(), vec![
        Term::Variable("x".into()),
        Term::Variable("y".into()),
    ])],
    vec![],
);
assert!(!unsafe_rule.is_safe());
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
