//! # Provenance - Proof Trees and Explanations
//!
//! Explains why derived facts exist (proof trees) and why expected facts
//! are absent (negative explanations). Core data model for explainable
//! derivations in the IQL engine.

pub mod backward_chaining;
pub mod proof_tree;
pub mod prove_body;
pub mod unification;
pub mod why_not;

use crate::value::Value;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Configuration for proof tree construction.
#[derive(Debug, Clone)]
pub struct ProofConfig {
    /// Maximum backward-chaining depth before truncation (default: 50)
    pub max_depth: usize,
    /// Maximum number of distinct derivation paths per tuple (default: 5)
    pub max_proofs_per_tuple: usize,
    /// When true, aggregation nodes include all contributing inputs (for export)
    pub full_mode: bool,
    /// Number of sample inputs shown in aggregation summary mode (default: 10)
    pub aggregation_sample_size: usize,
}

impl Default for ProofConfig {
    fn default() -> Self {
        Self {
            max_depth: 50,
            max_proofs_per_tuple: 5,
            full_mode: false,
            aggregation_sample_size: 10,
        }
    }
}

/// Explanation of why a tuple was NOT derived.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhyNotExplanation {
    /// The target relation
    pub relation: String,
    /// The target tuple values
    pub target_values: Vec<Value>,
    /// Why each candidate rule failed
    pub rule_failures: Vec<RuleFailure>,
}

impl WhyNotExplanation {
    /// Format as a human-readable explanation.
    pub fn format_explanation(&self) -> String {
        let vals = format_values(&self.target_values);
        let mut output = format!("{}({vals}) was NOT derived:\n", self.relation);
        if self.rule_failures.is_empty() {
            output.push_str("  No rules produce this relation.\n");
            return output;
        }
        for failure in &self.rule_failures {
            output.push_str(&format!(
                "\n  Rule: {} (clause {})\n",
                failure.rule_name, failure.clause_index
            ));
            output.push_str(&format!("    {}\n", failure.clause_text));
            output.push_str(&format!("    Blocker: {}\n", failure.blocker));
        }
        output
    }

    /// Serialize to a JSON value for export.
    pub fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

impl fmt::Display for WhyNotExplanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format_explanation())
    }
}

/// Why a specific rule clause failed to derive the target tuple.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleFailure {
    /// Name of the rule (head relation)
    pub rule_name: String,
    /// Which clause (0-indexed)
    pub clause_index: usize,
    /// Human-readable clause text
    pub clause_text: String,
    /// What blocked derivation
    pub blocker: Blocker,
}

/// The specific reason a rule clause failed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Blocker {
    /// Head pattern does not unify with target tuple
    #[serde(rename = "head_unification_failed")]
    HeadUnificationFailed { reason: String },

    /// A body atom had no matching tuples
    #[serde(rename = "body_atom_failed")]
    BodyAtomFailed {
        predicate_index: usize,
        predicate_text: String,
        reason: String,
    },

    /// A comparison predicate evaluated to false
    #[serde(rename = "comparison_failed")]
    ComparisonFailed {
        comparison_text: String,
        lhs_value: String,
        rhs_value: String,
    },

    /// A negated atom unexpectedly matched (tuple exists that should be absent)
    #[serde(rename = "negation_succeeded")]
    NegationSucceeded {
        relation: String,
        matching_tuple: Vec<Value>,
    },

    /// HNSW search did not return the target in top-k
    #[serde(rename = "hnsw_not_in_topk")]
    HnswNotInTopK {
        index_name: String,
        k: usize,
        reason: String,
    },
}

impl fmt::Display for Blocker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Blocker::HeadUnificationFailed { reason } => {
                write!(f, "Head unification failed: {reason}")
            }
            Blocker::BodyAtomFailed {
                predicate_index,
                predicate_text,
                reason,
            } => write!(
                f,
                "Body predicate {predicate_index} ({predicate_text}) failed: {reason}"
            ),
            Blocker::ComparisonFailed {
                comparison_text,
                lhs_value,
                rhs_value,
            } => write!(
                f,
                "Comparison {comparison_text} failed: {lhs_value} vs {rhs_value}"
            ),
            Blocker::NegationSucceeded {
                relation,
                matching_tuple,
            } => {
                let vals = format_values(matching_tuple);
                write!(f, "Negated {relation}({vals}) exists")
            }
            Blocker::HnswNotInTopK {
                index_name,
                k,
                reason,
            } => write!(f, "Not in top-{k} results of index {index_name}: {reason}"),
        }
    }
}

/// Format a list of values for display.
fn format_values(values: &[Value]) -> String {
    values
        .iter()
        .map(|v| format!("{v}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int(v: i32) -> Value {
        Value::Int32(v)
    }

    #[test]
    fn test_proof_config_default() {
        let config = ProofConfig::default();
        assert_eq!(config.max_depth, 50);
        assert_eq!(config.max_proofs_per_tuple, 5);
        assert!(!config.full_mode);
        assert_eq!(config.aggregation_sample_size, 10);
    }

    #[test]
    fn test_why_not_format() {
        let explanation = WhyNotExplanation {
            relation: "path".to_string(),
            target_values: vec![int(1), int(99)],
            rule_failures: vec![RuleFailure {
                rule_name: "path".to_string(),
                clause_index: 0,
                clause_text: "path(X, Y) <- edge(X, Y)".to_string(),
                blocker: Blocker::BodyAtomFailed {
                    predicate_index: 0,
                    predicate_text: "edge(1, 99)".to_string(),
                    reason: "No matching tuples in edge".to_string(),
                },
            }],
        };
        let fmt = explanation.format_explanation();
        assert!(fmt.contains("path(1, 99) was NOT derived:"), "got: {fmt}");
        assert!(fmt.contains("Blocker:"), "got: {fmt}");
    }

    #[test]
    fn test_why_not_no_rules() {
        let explanation = WhyNotExplanation {
            relation: "edge".to_string(),
            target_values: vec![int(1), int(99)],
            rule_failures: vec![],
        };
        let fmt = explanation.format_explanation();
        assert!(fmt.contains("No rules produce this relation"), "got: {fmt}");
    }

    #[test]
    fn test_blocker_display() {
        let b = Blocker::NegationSucceeded {
            relation: "danger".to_string(),
            matching_tuple: vec![int(3)],
        };
        let s = format!("{b}");
        assert!(s.contains("Negated danger(3) exists"), "got: {s}");

        let b = Blocker::HnswNotInTopK {
            index_name: "doc_idx".to_string(),
            k: 10,
            reason: "distance too large".to_string(),
        };
        let s = format!("{b}");
        assert!(s.contains("top-10"), "got: {s}");
    }
}
