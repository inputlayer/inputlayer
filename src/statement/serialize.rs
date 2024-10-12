//! Serializable representations of AST types for JSON storage.
//!
//! These types are used to persist rule definitions to disk.

use crate::ast::{
    AggregateFunc, ArithExpr, ArithOp, Atom, BodyPredicate, ComparisonOp, Rule, Term,
};
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
}

/// Serializable term for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableTerm {
    Variable(String),
    Constant(i64),
    StringConstant(String),
    FloatConstant(f64),
    Placeholder,
    /// Aggregate function with variable name (e.g., `count<X>`, `sum<Amount>`)
    Aggregate(AggregateFunc, String),
    /// Arithmetic expression (e.g., D+1, X*Y)
    Arithmetic(SerializableArithExpr),
}

/// Serializable arithmetic expression for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableArithExpr {
    Variable(String),
    Constant(i64),
    FloatConstant(f64),
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
#[serde(tag = "type")]
pub enum SerializableBodyPred {
    /// Atom predicate (positive or negated)
    Atom {
        relation: String,
        args: Vec<SerializableTerm>,
        negated: bool,
    },
    /// Comparison predicate (X = Y, X < 5, etc.)
    Comparison {
        left: SerializableTerm,
        op: SerializableComparisonOp,
        right: SerializableTerm,
    },
}

/// Serializable comparison operator for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableComparisonOp {
    Equal,
    NotEqual,
    LessThan,
    LessOrEqual,
    GreaterThan,
    GreaterOrEqual,
}


// Conversion Helpers
impl SerializableRule {
    /// Convert from `crate::ast::Rule`
    pub fn from_rule(rule: &Rule) -> Self {
        SerializableRule {
            head_relation: rule.head.relation.clone(),
            head_args: rule
                .head
                .args
                .iter()
                .map(SerializableTerm::from_term)
                .collect(),
            body: rule
                .body
                .iter()
                .map(SerializableBodyPred::from_body_pred)
                .collect(),
        }
    }

    /// Convert to `crate::ast::Rule`
    pub fn to_rule(&self) -> Rule {
        let head = Atom::new(
            self.head_relation.clone(),
            self.head_args
                .iter()
                .map(SerializableTerm::to_term)
                .collect(),
        );
        let body = self
            .body
            .iter()
            .map(SerializableBodyPred::to_body_pred)
            .collect();
        Rule::new(head, body)
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
            Term::Aggregate(func, var) => SerializableTerm::Aggregate(func.clone(), var.clone()),
            Term::Arithmetic(expr) => {
                SerializableTerm::Arithmetic(SerializableArithExpr::from_arith_expr(expr))
            }
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
            SerializableTerm::Aggregate(func, var) => Term::Aggregate(func.clone(), var.clone()),
            SerializableTerm::Arithmetic(expr) => Term::Arithmetic(expr.to_arith_expr()),
        }

    }
}

