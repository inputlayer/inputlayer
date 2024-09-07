//! Shared parsing utilities for statement modules.

use crate::ast::{AggregateFunc, Atom, BodyPredicate, Rule, Term};
use crate::parser::{parse_rule, parse_term};

/// Query goal: ?- atom.
#[derive(Debug, Clone)]
pub struct QueryGoal {
    /// The goal atom to query
    pub goal: Atom,
    /// Additional body predicates (for complex queries)
    pub body: Vec<BodyPredicate>,
}

// String Utilities
/// Strip % comments, respecting string literals and % as modulo inside expressions.
