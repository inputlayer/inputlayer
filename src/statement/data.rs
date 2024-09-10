//! Data manipulation statements for `InputLayer`.
//!
//! This module handles insert, delete, and update operations:
//! - `+relation(args).` - single insert
//! - `+relation[(t1), (t2), ...]` - bulk insert
//! - `-relation(args).` - single delete
//! - `-relation(X, Y) :- condition.` - conditional delete
//! - `-old, +new :- condition.` - atomic update

use crate::ast::{Atom, BodyPredicate, Rule, Term};
use crate::parser::parse_rule;

/// Insert operation: +relation(args).
#[derive(Debug, Clone)]
pub struct InsertOp {
    /// Relation name
    pub relation: String,
    /// Tuples to insert (each inner Vec is one tuple's arguments)
    pub tuples: Vec<Vec<Term>>,
}

/// Delete operation: -relation(args). or -relation(X) :- body.
#[derive(Debug, Clone)]
pub struct DeleteOp {
    /// Relation name
    pub relation: String,
    /// Delete pattern
    pub pattern: DeletePattern,
}

/// Pattern for delete operations
#[derive(Debug, Clone)]
pub enum DeletePattern {
    /// Single tuple: -edge(1, 2).
    SingleTuple(Vec<Term>),
    /// Bulk tuples: -edge[(1, 2), (3, 4)].
    BulkTuples(Vec<Vec<Term>>),
    /// Conditional delete: -edge(X, Y) :- condition.
    Conditional {
        /// Variables in the head
        head_args: Vec<Term>,
        /// Body predicates (conditions)
        body: Vec<BodyPredicate>,
    },
}

/// Update operation: -old, +new :- condition. (atomic)
#[derive(Debug, Clone)]
pub struct UpdateOp {
    /// Deletions to perform
    pub deletes: Vec<DeleteTarget>,
    /// Insertions to perform
    pub inserts: Vec<InsertTarget>,
    /// Condition body (what to match)
    pub body: Vec<BodyPredicate>,
}

/// A single delete target in an update
#[derive(Debug, Clone)]
pub struct DeleteTarget {
    pub relation: String,
    pub args: Vec<Term>,
}

/// A single insert target in an update
#[derive(Debug, Clone)]
pub struct InsertTarget {
    pub relation: String,
    pub args: Vec<Term>,
}

// Parsing
use super::parser::{parse_atom_args, parse_single_term, split_by_comma, term_to_string};

/// Parse an insert operation: +relation(args). or +relation[(t1), (t2), ...].
