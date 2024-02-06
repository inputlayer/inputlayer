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
pub struct AtomBuilder {
    relation: String,
    args: Vec<Term>,
}

impl AtomBuilder {
    /// Create a new atom builder for the given relation
    pub fn new(relation: impl Into<String>) -> Self {
        AtomBuilder {
            relation: relation.into(),
            args: Vec::new(),
        }
    }

    /// Add a variable argument
    pub fn var(mut self, name: impl Into<String>) -> Self {
        self.args.push(Term::Variable(name.into()));
        self
    }

    /// Add an integer constant argument
    pub fn int(mut self, value: i64) -> Self {
        self.args.push(Term::Constant(value));
        self
    }

    /// Add a placeholder argument (_)
    pub fn placeholder(mut self) -> Self {
        self.args.push(Term::Placeholder);
        self
    }

    /// Add a term directly
    pub fn term(mut self, t: Term) -> Self {
        self.args.push(t);
        self
    }

    /// Add multiple variable arguments
    pub fn vars<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for name in names {
            self.args.push(Term::Variable(name.into()));
        }
        self
    }

    /// Build the atom
    pub fn build(self) -> Atom {
        Atom::new(self.relation, self.args)
    }
}

// RuleBuilder
/// Builder for constructing Rule instances
#[derive(Debug, Clone)]
pub struct RuleBuilder {
    head_relation: String,
    head_args: Vec<Term>,
    body: Vec<BodyPredicate>,
}

impl RuleBuilder {
    /// Create a new rule builder with the given head relation name
    pub fn new(head_relation: impl Into<String>) -> Self {
        RuleBuilder {
            head_relation: head_relation.into(),
            head_args: Vec::new(),
            body: Vec::new(),
        }
    }

    /// Set the head variables
    pub fn head_vars<I, S>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.head_args = vars.into_iter().map(|v| Term::Variable(v.into())).collect();
        self
    }

    /// Set the head terms (for more complex heads with constants, etc.)
    pub fn head_terms<I>(mut self, terms: I) -> Self
    where
        I: IntoIterator<Item = Term>,
    {
        self.head_args = terms.into_iter().collect();
        self
    }

    /// Add a positive body atom with variable arguments
    pub fn body_atom<I, S>(mut self, relation: impl Into<String>, vars: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let atom = AtomBuilder::new(relation).vars(vars).build();
        self.body.push(BodyPredicate::Positive(atom));
        self
    }

    /// Add a negated body atom with variable arguments
    pub fn negated_atom<I, S>(mut self, relation: impl Into<String>, vars: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let atom = AtomBuilder::new(relation).vars(vars).build();
        self.body.push(BodyPredicate::Negated(atom));
        self
    }

    /// Add a body predicate directly
    pub fn body_predicate(mut self, pred: BodyPredicate) -> Self {
        self.body.push(pred);
        self
    }

    /// Add a positive body atom built with `AtomBuilder`
    pub fn body(mut self, atom: Atom) -> Self {
        self.body.push(BodyPredicate::Positive(atom));
        self
    }

    /// Build the rule
    pub fn build(self) -> Rule {
        let head = Atom::new(self.head_relation, self.head_args);
        Rule::new(head, self.body)
    }
}

// Convenience functions
/// Create a simple fact (rule with no body): rel(args...).
