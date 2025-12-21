//! Serializable representations of AST types for JSON storage.
//!
//! These types are used to persist rule definitions to disk.

use crate::ast::{AggregateFunc, ArithExpr, ArithOp, Atom, BodyPredicate, Constraint, Rule, Term};
use serde::{Deserialize, Serialize};

/// Rule definition for storage and serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleDef {
    /// Rule name (head relation)
    pub name: String,
    /// The rule defining this relation
    pub rule: SerializableRule,
}

/// A serializable representation of a Rule for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableRule {
    pub head_relation: String,
    pub head_args: Vec<SerializableTerm>,
    pub body: Vec<SerializableBodyPred>,
    pub constraints: Vec<SerializableConstraint>,
}

/// Serializable term for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableTerm {
    Variable(String),
    Constant(i64),
    StringConstant(String),
    FloatConstant(f64),
    Placeholder,
    /// Aggregate function with variable name (e.g., count<X>, sum<Amount>)
    Aggregate(AggregateFunc, String),
    /// Arithmetic expression (e.g., D+1, X*Y)
    Arithmetic(SerializableArithExpr),
}

/// Serializable arithmetic expression for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableArithExpr {
    Variable(String),
    Constant(i64),
    Binary {
        op: SerializableArithOp,
        left: Box<SerializableArithExpr>,
        right: Box<SerializableArithExpr>,
    },
}

/// Serializable arithmetic operator for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

/// Serializable body predicate for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableBodyPred {
    pub relation: String,
    pub args: Vec<SerializableTerm>,
    pub negated: bool,
}

/// Serializable constraint for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableConstraint {
    Equal(SerializableTerm, SerializableTerm),
    NotEqual(SerializableTerm, SerializableTerm),
    LessThan(SerializableTerm, SerializableTerm),
    LessOrEqual(SerializableTerm, SerializableTerm),
    GreaterThan(SerializableTerm, SerializableTerm),
    GreaterOrEqual(SerializableTerm, SerializableTerm),
}

// ============================================================================
// Conversion Helpers
// ============================================================================

impl SerializableRule {
    /// Convert from crate::ast::Rule
    pub fn from_rule(rule: &Rule) -> Self {
        SerializableRule {
            head_relation: rule.head.relation.clone(),
            head_args: rule.head.args.iter().map(SerializableTerm::from_term).collect(),
            body: rule.body.iter().map(SerializableBodyPred::from_body_pred).collect(),
            constraints: rule.constraints.iter().map(SerializableConstraint::from_constraint).collect(),
        }
    }

    /// Convert to crate::ast::Rule
    pub fn to_rule(&self) -> Rule {
        let head = Atom::new(
            self.head_relation.clone(),
            self.head_args.iter().map(|t| t.to_term()).collect(),
        );
        let body = self.body.iter().map(|b| b.to_body_pred()).collect();
        let constraints = self.constraints.iter().map(|c| c.to_constraint()).collect();
        Rule::new(head, body, constraints)
    }
}

impl SerializableTerm {
    pub fn from_term(term: &Term) -> Self {
        match term {
            Term::Variable(name) => SerializableTerm::Variable(name.clone()),
            Term::Constant(val) => SerializableTerm::Constant(*val),
            Term::StringConstant(s) => SerializableTerm::StringConstant(s.clone()),
            Term::FloatConstant(f) => SerializableTerm::FloatConstant(*f),
            Term::Placeholder => SerializableTerm::Placeholder,
            Term::Aggregate(func, var) => SerializableTerm::Aggregate(
                func.clone(),
                var.clone(),
            ),
            Term::Arithmetic(expr) => SerializableTerm::Arithmetic(
                SerializableArithExpr::from_arith_expr(expr),
            ),
            // For other complex terms (FunctionCall, VectorLiteral),
            // we simplify to placeholder as they're not typically used in view definitions
            _ => SerializableTerm::Placeholder,
        }
    }

    pub fn to_term(&self) -> Term {
        match self {
            SerializableTerm::Variable(name) => Term::Variable(name.clone()),
            SerializableTerm::Constant(val) => Term::Constant(*val),
            SerializableTerm::StringConstant(s) => Term::StringConstant(s.clone()),
            SerializableTerm::FloatConstant(f) => Term::FloatConstant(*f),
            SerializableTerm::Placeholder => Term::Placeholder,
            SerializableTerm::Aggregate(func, var) => Term::Aggregate(
                func.clone(),
                var.clone(),
            ),
            SerializableTerm::Arithmetic(expr) => Term::Arithmetic(
                expr.to_arith_expr(),
            ),
        }
    }
}

impl SerializableArithExpr {
    /// Convert from crate::ast::ArithExpr
    pub fn from_arith_expr(expr: &ArithExpr) -> Self {
        match expr {
            ArithExpr::Variable(name) => SerializableArithExpr::Variable(name.clone()),
            ArithExpr::Constant(val) => SerializableArithExpr::Constant(*val),
            ArithExpr::Binary { op, left, right } => SerializableArithExpr::Binary {
                op: SerializableArithOp::from_arith_op(op),
                left: Box::new(Self::from_arith_expr(left)),
                right: Box::new(Self::from_arith_expr(right)),
            },
        }
    }

    /// Convert to crate::ast::ArithExpr
    pub fn to_arith_expr(&self) -> ArithExpr {
        match self {
            SerializableArithExpr::Variable(name) => ArithExpr::Variable(name.clone()),
            SerializableArithExpr::Constant(val) => ArithExpr::Constant(*val),
            SerializableArithExpr::Binary { op, left, right } => ArithExpr::Binary {
                op: op.to_arith_op(),
                left: Box::new(left.to_arith_expr()),
                right: Box::new(right.to_arith_expr()),
            },
        }
    }
}

impl SerializableArithOp {
    /// Convert from crate::ast::ArithOp
    pub fn from_arith_op(op: &ArithOp) -> Self {
        match op {
            ArithOp::Add => SerializableArithOp::Add,
            ArithOp::Sub => SerializableArithOp::Sub,
            ArithOp::Mul => SerializableArithOp::Mul,
            ArithOp::Div => SerializableArithOp::Div,
            ArithOp::Mod => SerializableArithOp::Mod,
        }
    }

    /// Convert to crate::ast::ArithOp
    pub fn to_arith_op(&self) -> ArithOp {
        match self {
            SerializableArithOp::Add => ArithOp::Add,
            SerializableArithOp::Sub => ArithOp::Sub,
            SerializableArithOp::Mul => ArithOp::Mul,
            SerializableArithOp::Div => ArithOp::Div,
            SerializableArithOp::Mod => ArithOp::Mod,
        }
    }
}

impl SerializableBodyPred {
    pub fn from_body_pred(pred: &BodyPredicate) -> Self {
        let (atom, negated) = match pred {
            BodyPredicate::Positive(atom) => (atom, false),
            BodyPredicate::Negated(atom) => (atom, true),
        };
        SerializableBodyPred {
            relation: atom.relation.clone(),
            args: atom.args.iter().map(SerializableTerm::from_term).collect(),
            negated,
        }
    }

    pub fn to_body_pred(&self) -> BodyPredicate {
        let atom = Atom::new(
            self.relation.clone(),
            self.args.iter().map(|t| t.to_term()).collect(),
        );
        if self.negated {
            BodyPredicate::Negated(atom)
        } else {
            BodyPredicate::Positive(atom)
        }
    }
}

impl SerializableConstraint {
    pub fn from_constraint(constraint: &Constraint) -> Self {
        match constraint {
            Constraint::Equal(l, r) => SerializableConstraint::Equal(
                SerializableTerm::from_term(l),
                SerializableTerm::from_term(r),
            ),
            Constraint::NotEqual(l, r) => SerializableConstraint::NotEqual(
                SerializableTerm::from_term(l),
                SerializableTerm::from_term(r),
            ),
            Constraint::LessThan(l, r) => SerializableConstraint::LessThan(
                SerializableTerm::from_term(l),
                SerializableTerm::from_term(r),
            ),
            Constraint::LessOrEqual(l, r) => SerializableConstraint::LessOrEqual(
                SerializableTerm::from_term(l),
                SerializableTerm::from_term(r),
            ),
            Constraint::GreaterThan(l, r) => SerializableConstraint::GreaterThan(
                SerializableTerm::from_term(l),
                SerializableTerm::from_term(r),
            ),
            Constraint::GreaterOrEqual(l, r) => SerializableConstraint::GreaterOrEqual(
                SerializableTerm::from_term(l),
                SerializableTerm::from_term(r),
            ),
        }
    }

    pub fn to_constraint(&self) -> Constraint {
        match self {
            SerializableConstraint::Equal(l, r) => Constraint::Equal(l.to_term(), r.to_term()),
            SerializableConstraint::NotEqual(l, r) => Constraint::NotEqual(l.to_term(), r.to_term()),
            SerializableConstraint::LessThan(l, r) => Constraint::LessThan(l.to_term(), r.to_term()),
            SerializableConstraint::LessOrEqual(l, r) => Constraint::LessOrEqual(l.to_term(), r.to_term()),
            SerializableConstraint::GreaterThan(l, r) => Constraint::GreaterThan(l.to_term(), r.to_term()),
            SerializableConstraint::GreaterOrEqual(l, r) => Constraint::GreaterOrEqual(l.to_term(), r.to_term()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_rule;

    #[test]
    fn test_serializable_rule_roundtrip() {
        let rule_str = "path(X, Y) :- edge(X, Y).";
        let rule = parse_rule(rule_str).unwrap();
        let serializable = SerializableRule::from_rule(&rule);
        let json = serde_json::to_string(&serializable).unwrap();
        let deserialized: SerializableRule = serde_json::from_str(&json).unwrap();
        let restored = deserialized.to_rule();
        assert_eq!(restored.head.relation, "path");
    }
}
