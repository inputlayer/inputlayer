//! Builder Patterns for AST Construction
//!
//! Provides fluent APIs for constructing AST nodes, particularly useful for tests.
//!
//! ## Example
//!
//! ```rust
//! use inputlayer::ast::builders::{AtomBuilder, RuleBuilder};
//!
//! // Build an atom: path(x, y)
//! let atom = AtomBuilder::new("path")
//!     .var("x")
//!     .var("y")
//!     .build();
//!
//! // Build a rule: path(x, y) :- edge(x, y).
//! let rule = RuleBuilder::new("path")
//!     .head_vars(["x", "y"])
//!     .body_atom("edge", ["x", "y"])
//!     .build();
//!
//! // Build a recursive rule: path(x, z) :- path(x, y), edge(y, z).
//! let recursive = RuleBuilder::new("path")
//!     .head_vars(["x", "z"])
//!     .body_atom("path", ["x", "y"])
//!     .body_atom("edge", ["y", "z"])
//!     .build();
//! ```

use super::{Atom, BodyPredicate, Rule, Term};

// AtomBuilder
/// Builder for constructing Atom instances
#[derive(Debug, Clone)]
