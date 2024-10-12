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
    /// Arithmetic expression (e.g., D+1, X*Y.clone())
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
