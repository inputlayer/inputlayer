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

impl SerializableArithExpr {
    /// Convert from `crate::ast::ArithExpr`
    pub fn from_arith_expr(expr: &ArithExpr) -> Self {
        match expr {
            ArithExpr::Variable(name) => SerializableArithExpr::Variable(name.clone()),
            ArithExpr::Constant(val) => SerializableArithExpr::Constant(*val),
            ArithExpr::FloatConstant(bits) => {
                SerializableArithExpr::FloatConstant(f64::from_bits(*bits))
            }
            ArithExpr::Binary { op, left, right } => SerializableArithExpr::Binary {
                op: SerializableArithOp::from_arith_op(op),
                left: Box::new(Self::from_arith_expr(left)),
                right: Box::new(Self::from_arith_expr(right)),
            },
        }
    }

    /// Convert to `crate::ast::ArithExpr`
    pub fn to_arith_expr(&self) -> ArithExpr {
        match self {
            SerializableArithExpr::Variable(name) => ArithExpr::Variable(name.clone()),
            SerializableArithExpr::Constant(val) => ArithExpr::Constant(*val),
            SerializableArithExpr::FloatConstant(val) => ArithExpr::FloatConstant(val.to_bits()),
            SerializableArithExpr::Binary { op, left, right } => ArithExpr::Binary {
                op: op.to_arith_op(),
                left: Box::new(left.to_arith_expr()),
                right: Box::new(right.to_arith_expr()),
            },
        }
    }
}

impl SerializableArithOp {
    /// Convert from `crate::ast::ArithOp`
    pub fn from_arith_op(op: &ArithOp) -> Self {
        match op {
            ArithOp::Add => SerializableArithOp::Add,
            ArithOp::Sub => SerializableArithOp::Sub,
            ArithOp::Mul => SerializableArithOp::Mul,
            ArithOp::Div => SerializableArithOp::Div,
            ArithOp::Mod => SerializableArithOp::Mod,
        }
    }

    /// Convert to `crate::ast::ArithOp`
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
        match pred {
            BodyPredicate::Positive(atom) => SerializableBodyPred::Atom {
                relation: atom.relation.clone(),
                args: atom.args.iter().map(SerializableTerm::from_term).collect(),
                negated: false,
            },
            BodyPredicate::Negated(atom) => SerializableBodyPred::Atom {
                relation: atom.relation.clone(),
                args: atom.args.iter().map(SerializableTerm::from_term).collect(),
                negated: true,
            },
            BodyPredicate::Comparison(left, op, right) => SerializableBodyPred::Comparison {
                left: SerializableTerm::from_term(left),
                op: SerializableComparisonOp::from_op(op),
                right: SerializableTerm::from_term(right),
            },
            // HnswNearest is a runtime-only predicate, not serialized in rules
            BodyPredicate::HnswNearest { .. } => SerializableBodyPred::Atom {
                relation: "__hnsw_nearest__".to_string(),
                args: vec![],
                negated: false,
            },
        }
    }

    pub fn to_body_pred(&self) -> BodyPredicate {
        match self {
            SerializableBodyPred::Atom {
                relation,
                args,
                negated,
            } => {
                let atom = Atom::new(
                    relation.clone(),
                    args.iter().map(SerializableTerm::to_term).collect(),
                );
                if *negated {
                    BodyPredicate::Negated(atom)
                } else {
                    BodyPredicate::Positive(atom)
                }
            }
            SerializableBodyPred::Comparison { left, op, right } => {
                BodyPredicate::Comparison(left.to_term(), op.to_op(), right.to_term())
            }
        }
    }
}

