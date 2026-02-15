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

impl SerializableComparisonOp {
    pub fn from_op(op: &ComparisonOp) -> Self {
        match op {
            ComparisonOp::Equal => SerializableComparisonOp::Equal,
            ComparisonOp::NotEqual => SerializableComparisonOp::NotEqual,
            ComparisonOp::LessThan => SerializableComparisonOp::LessThan,
            ComparisonOp::LessOrEqual => SerializableComparisonOp::LessOrEqual,
            ComparisonOp::GreaterThan => SerializableComparisonOp::GreaterThan,
            ComparisonOp::GreaterOrEqual => SerializableComparisonOp::GreaterOrEqual,
        }
    }

    pub fn to_op(&self) -> ComparisonOp {
        match self {
            SerializableComparisonOp::Equal => ComparisonOp::Equal,
            SerializableComparisonOp::NotEqual => ComparisonOp::NotEqual,
            SerializableComparisonOp::LessThan => ComparisonOp::LessThan,
            SerializableComparisonOp::LessOrEqual => ComparisonOp::LessOrEqual,
            SerializableComparisonOp::GreaterThan => ComparisonOp::GreaterThan,
            SerializableComparisonOp::GreaterOrEqual => ComparisonOp::GreaterOrEqual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_rule;

    #[test]
    fn test_serializable_rule_roundtrip() {
        let rule_str = "path(X, Y) <- edge(X, Y)";
        let rule = parse_rule(rule_str).unwrap();
        let serializable = SerializableRule::from_rule(&rule);
        let json = serde_json::to_string(&serializable).unwrap();
        let deserialized: SerializableRule = serde_json::from_str(&json).unwrap();
        let restored = deserialized.to_rule();
        assert_eq!(restored.head.relation, "path");
    }

    #[test]
    fn test_serializable_rule_with_negation() {
        let rule_str = "safe(X) <- person(X), !banned(X)";
        let rule = parse_rule(rule_str).unwrap();
        let serializable = SerializableRule::from_rule(&rule);
        let json = serde_json::to_string(&serializable).unwrap();
        let deserialized: SerializableRule = serde_json::from_str(&json).unwrap();
        let restored = deserialized.to_rule();
        assert_eq!(restored.head.relation, "safe");
        assert_eq!(restored.body.len(), 2);
        assert!(matches!(restored.body[1], BodyPredicate::Negated(_)));
    }

    #[test]
    fn test_serializable_rule_with_comparison() {
        let rule_str = "adult(X) <- person(X, Age), Age >= 18";
        let rule = parse_rule(rule_str).unwrap();
        let serializable = SerializableRule::from_rule(&rule);
        let json = serde_json::to_string(&serializable).unwrap();
        let deserialized: SerializableRule = serde_json::from_str(&json).unwrap();
        let restored = deserialized.to_rule();
        assert_eq!(restored.body.len(), 2);
        assert!(matches!(restored.body[1], BodyPredicate::Comparison(..)));
    }

    #[test]
    fn test_serializable_term_variable() {
        let term = Term::Variable("X".to_string());
        let ser = SerializableTerm::from_term(&term);
        assert!(matches!(ser, SerializableTerm::Variable(ref n) if n == "X"));
        let back = ser.to_term();
        assert!(matches!(back, Term::Variable(ref n) if n == "X"));
    }

    #[test]
    fn test_serializable_term_constant() {
        let term = Term::Constant(42);
        let ser = SerializableTerm::from_term(&term);
        assert!(matches!(ser, SerializableTerm::Constant(42)));
        let back = ser.to_term();
        assert!(matches!(back, Term::Constant(42)));
    }

    #[test]
    fn test_serializable_term_string() {
        let term = Term::StringConstant("hello".to_string());
        let ser = SerializableTerm::from_term(&term);
        assert!(matches!(ser, SerializableTerm::StringConstant(ref s) if s == "hello"));
        let back = ser.to_term();
        assert!(matches!(back, Term::StringConstant(ref s) if s == "hello"));
    }

    #[test]
    fn test_serializable_term_float() {
        let term = Term::FloatConstant(3.14);
        let ser = SerializableTerm::from_term(&term);
        assert!(matches!(ser, SerializableTerm::FloatConstant(f) if (f - 3.14).abs() < 0.001));
    }

    #[test]
    fn test_serializable_term_placeholder() {
        let term = Term::Placeholder;
        let ser = SerializableTerm::from_term(&term);
        assert!(matches!(ser, SerializableTerm::Placeholder));
        let back = ser.to_term();
        assert!(matches!(back, Term::Placeholder));
    }

    #[test]
    fn test_serializable_term_aggregate() {
        let term = Term::Aggregate(AggregateFunc::Count, "X".to_string());
        let ser = SerializableTerm::from_term(&term);
        assert!(matches!(
            ser,
            SerializableTerm::Aggregate(AggregateFunc::Count, _)
        ));
        let back = ser.to_term();
        assert!(matches!(back, Term::Aggregate(AggregateFunc::Count, _)));
    }

    #[test]
    fn test_serializable_term_vector_becomes_placeholder() {
        // VectorLiteral is not directly serializable, becomes Placeholder
        let term = Term::VectorLiteral(vec![1.0, 2.0]);
        let ser = SerializableTerm::from_term(&term);
        assert!(matches!(ser, SerializableTerm::Placeholder));
    }

    #[test]
    fn test_serializable_arith_expr_variable() {
        let expr = ArithExpr::Variable("X".to_string());
        let ser = SerializableArithExpr::from_arith_expr(&expr);
        assert!(matches!(ser, SerializableArithExpr::Variable(ref n) if n == "X"));
        let back = ser.to_arith_expr();
        assert!(matches!(back, ArithExpr::Variable(ref n) if n == "X"));
    }

    #[test]
    fn test_serializable_arith_expr_constant() {
        let expr = ArithExpr::Constant(42);
        let ser = SerializableArithExpr::from_arith_expr(&expr);
        assert!(matches!(ser, SerializableArithExpr::Constant(42)));
    }

    #[test]
    fn test_serializable_arith_expr_float() {
        let expr = ArithExpr::FloatConstant(3.14_f64.to_bits());
        let ser = SerializableArithExpr::from_arith_expr(&expr);
        if let SerializableArithExpr::FloatConstant(f) = ser {
            assert!((f - 3.14).abs() < 0.001);
        } else {
            panic!("Expected FloatConstant");
        }
    }

    #[test]
    fn test_serializable_arith_expr_binary() {
        let expr = ArithExpr::Binary {
            op: ArithOp::Add,
            left: Box::new(ArithExpr::Variable("X".to_string())),
            right: Box::new(ArithExpr::Constant(1)),
        };
        let ser = SerializableArithExpr::from_arith_expr(&expr);
        let back = ser.to_arith_expr();
        assert!(matches!(
            back,
            ArithExpr::Binary {
                op: ArithOp::Add,
                ..
            }
        ));
    }

    #[test]
    fn test_serializable_arith_op_roundtrip() {
        let ops = [
            ArithOp::Add,
            ArithOp::Sub,
            ArithOp::Mul,
            ArithOp::Div,
            ArithOp::Mod,
        ];
        for op in &ops {
            let ser = SerializableArithOp::from_arith_op(op);
            let back = ser.to_arith_op();
            assert_eq!(std::mem::discriminant(&back), std::mem::discriminant(op));
        }
    }

    #[test]
    fn test_serializable_comparison_op_roundtrip() {
        let ops = [
            ComparisonOp::Equal,
            ComparisonOp::NotEqual,
            ComparisonOp::LessThan,
            ComparisonOp::LessOrEqual,
            ComparisonOp::GreaterThan,
            ComparisonOp::GreaterOrEqual,
        ];
        for op in &ops {
            let ser = SerializableComparisonOp::from_op(op);
            let back = ser.to_op();
            assert_eq!(std::mem::discriminant(&back), std::mem::discriminant(op));
        }
    }

    #[test]
    fn test_serializable_body_pred_positive() {
        let atom = Atom::new("edge".to_string(), vec![Term::Variable("X".to_string())]);
        let pred = BodyPredicate::Positive(atom);
        let ser = SerializableBodyPred::from_body_pred(&pred);
        let back = ser.to_body_pred();
        assert!(matches!(back, BodyPredicate::Positive(ref a) if a.relation == "edge"));
    }

    #[test]
    fn test_serializable_body_pred_negated() {
        let atom = Atom::new("banned".to_string(), vec![Term::Variable("X".to_string())]);
        let pred = BodyPredicate::Negated(atom);
        let ser = SerializableBodyPred::from_body_pred(&pred);
        let back = ser.to_body_pred();
        assert!(matches!(back, BodyPredicate::Negated(ref a) if a.relation == "banned"));
    }

    #[test]
    fn test_serializable_body_pred_comparison() {
        let pred = BodyPredicate::Comparison(
            Term::Variable("X".to_string()),
            ComparisonOp::GreaterThan,
            Term::Constant(5),
        );
        let ser = SerializableBodyPred::from_body_pred(&pred);
        let back = ser.to_body_pred();
        assert!(matches!(back, BodyPredicate::Comparison(..)));
    }

    #[test]
    fn test_rule_def_json_roundtrip() {
        let rule = parse_rule("path(X, Y) <- edge(X, Y)").unwrap();
        let def = RuleDef {
            name: "path".to_string(),
            rule: SerializableRule::from_rule(&rule),
        };
        let json = serde_json::to_string(&def).unwrap();
        let back: RuleDef = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "path");
        assert_eq!(back.rule.head_relation, "path");
    }

    #[test]
    fn test_serializable_rule_with_arithmetic() {
        let rule_str = "doubled(X, Y) <- data(X), Y = X * 2";
        let rule = parse_rule(rule_str).unwrap();
        let serializable = SerializableRule::from_rule(&rule);
        let json = serde_json::to_string(&serializable).unwrap();
        let deserialized: SerializableRule = serde_json::from_str(&json).unwrap();
        let restored = deserialized.to_rule();
        assert_eq!(restored.head.relation, "doubled");
    }
}
