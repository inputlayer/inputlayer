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
pub fn fact<I, S>(relation: impl Into<String>, args: I) -> Rule
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    RuleBuilder::new(relation).head_vars(args).build()
}

/// Create a simple rule: head(vars...) :- body(vars...).
pub fn simple_rule<I1, S1, I2, S2>(
    head_rel: impl Into<String>,
    head_vars: I1,
    body_rel: impl Into<String>,
    body_vars: I2,
) -> Rule
where
    I1: IntoIterator<Item = S1>,
    S1: Into<String>,
    I2: IntoIterator<Item = S2>,
    S2: Into<String>,
{
    RuleBuilder::new(head_rel)
        .head_vars(head_vars)
        .body_atom(body_rel, body_vars)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atom_builder_basic() {
        let atom = AtomBuilder::new("edge").var("x").var("y").build();

        assert_eq!(atom.relation, "edge");
        assert_eq!(atom.args.len(), 2);
        assert_eq!(atom.args[0], Term::Variable("x".to_string()));
        assert_eq!(atom.args[1], Term::Variable("y".to_string()));
    }

    #[test]
    fn test_atom_builder_vars() {
        let atom = AtomBuilder::new("path").vars(["a", "b", "c"]).build();

        assert_eq!(atom.relation, "path");
        assert_eq!(atom.args.len(), 3);
    }

    #[test]
    fn test_atom_builder_mixed() {
        let atom = AtomBuilder::new("relation")
            .var("x")
            .int(42)
            .placeholder()
            .build();

        assert_eq!(atom.args.len(), 3);
        assert_eq!(atom.args[0], Term::Variable("x".to_string()));
        assert_eq!(atom.args[1], Term::Constant(42));
        assert_eq!(atom.args[2], Term::Placeholder);
    }

    #[test]
    fn test_rule_builder_basic() {
        // path(x, y) :- edge(x, y).
        let rule = RuleBuilder::new("path")
            .head_vars(["x", "y"])
            .body_atom("edge", ["x", "y"])
            .build();

        assert_eq!(rule.head.relation, "path");
        assert_eq!(rule.head.args.len(), 2);
        assert_eq!(rule.body.len(), 1);
    }

    #[test]
    fn test_rule_builder_recursive() {
        // path(x, z) :- path(x, y), edge(y, z).
        let rule = RuleBuilder::new("path")
            .head_vars(["x", "z"])
            .body_atom("path", ["x", "y"])
            .body_atom("edge", ["y", "z"])
            .build();

        assert_eq!(rule.body.len(), 2);
    }

    #[test]
    fn test_rule_builder_with_negation() {
        // result(x) :- source(x), !excluded(x).
        let rule = RuleBuilder::new("result")
            .head_vars(["x"])
            .body_atom("source", ["x"])
            .negated_atom("excluded", ["x"])
            .build();

        assert_eq!(rule.body.len(), 2);
        assert!(matches!(&rule.body[0], BodyPredicate::Positive(_)));
        assert!(matches!(&rule.body[1], BodyPredicate::Negated(_)));
    }

    #[test]
    fn test_fact_helper() {
        let rule = fact("person", ["alice"]);

        assert_eq!(rule.head.relation, "person");
        assert!(rule.body.is_empty());
    }

    #[test]
    fn test_simple_rule_helper() {
        let rule = simple_rule("path", ["x", "y"], "edge", ["x", "y"]);

        assert_eq!(rule.head.relation, "path");
        assert_eq!(rule.body.len(), 1);
    }
}
