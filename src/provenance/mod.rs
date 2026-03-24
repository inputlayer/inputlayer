//! # Provenance - Why-Provenance and Proof Trees
//!
//! Explains why derived facts exist (proof trees) and why expected facts
//! are absent (negative explanations). Core data model for explainable
//! derivations in the Datalog engine.

pub mod backward_chaining;
pub mod prove_body;
pub mod unification;
pub mod why_not;
pub mod wire;

use crate::value::Value;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Configuration for proof tree construction.
#[derive(Debug, Clone)]
pub struct ProofConfig {
    /// Maximum backward-chaining depth before truncation (default: 50)
    pub max_depth: usize,
    /// Maximum number of distinct proofs returned per tuple (default: 5)
    pub max_proofs_per_tuple: usize,
    /// When true, aggregation proofs enumerate all contributors
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

/// A proof tree node explaining why a derived tuple exists.
///
/// Each variant captures a different kind of derivation step, from base
/// facts through rule applications, vector searches, and aggregations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "node_type")]
pub enum ProofTree {
    /// Leaf: tuple exists as a base fact in a relation
    #[serde(rename = "base_fact")]
    BaseFact {
        relation: String,
        values: Vec<Value>,
    },

    /// A rule clause was applied to derive this tuple
    #[serde(rename = "rule_application")]
    RuleApplication {
        rule_name: String,
        clause_index: usize,
        clause_text: String,
        bindings: Vec<(String, Value)>,
        children: Vec<ProofTree>,
    },

    /// Negation proof: no matching tuple exists in the negated relation
    #[serde(rename = "negation")]
    NegationProof { relation: String, pattern: String },

    /// Approximate nearest neighbor search found this result
    #[serde(rename = "vector_search")]
    VectorSearchProof {
        index_name: String,
        metric: String,
        query_vector: Vec<f32>,
        result_id: i64,
        distance: f64,
        k: usize,
        ef_search: Option<usize>,
    },

    /// Aggregation collected multiple tuples into a summary
    #[serde(rename = "aggregation")]
    AggregationProof {
        rule_name: String,
        aggregate_fn: String,
        contributing_count: usize,
        sample_inputs: Vec<Vec<Value>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        full_inputs: Option<Vec<Vec<Value>>>,
    },

    /// Recursive derivation with iteration depth
    #[serde(rename = "recursive")]
    Recursive {
        rule_name: String,
        iteration: u32,
        inner: Box<ProofTree>,
    },

    /// Proof was truncated because it exceeded the configured depth limit
    #[serde(rename = "truncated")]
    Truncated { depth_limit: usize },
}

impl ProofTree {
    /// Depth of the proof tree (longest path from root to leaf).
    pub fn depth(&self) -> usize {
        match self {
            ProofTree::BaseFact { .. }
            | ProofTree::NegationProof { .. }
            | ProofTree::VectorSearchProof { .. }
            | ProofTree::Truncated { .. } => 1,
            ProofTree::RuleApplication { children, .. } => {
                1 + children.iter().map(ProofTree::depth).max().unwrap_or(0)
            }
            ProofTree::AggregationProof { .. } => 2,
            ProofTree::Recursive { inner, .. } => 1 + inner.depth(),
        }
    }

    /// Total number of nodes in the proof tree.
    pub fn node_count(&self) -> usize {
        match self {
            ProofTree::BaseFact { .. }
            | ProofTree::NegationProof { .. }
            | ProofTree::VectorSearchProof { .. }
            | ProofTree::Truncated { .. } => 1,
            ProofTree::RuleApplication { children, .. } => {
                1 + children.iter().map(ProofTree::node_count).sum::<usize>()
            }
            ProofTree::AggregationProof { .. } => 1,
            ProofTree::Recursive { inner, .. } => 1 + inner.node_count(),
        }
    }

    /// Count of base fact leaves in the tree.
    pub fn base_fact_count(&self) -> usize {
        match self {
            ProofTree::BaseFact { .. } => 1,
            ProofTree::RuleApplication { children, .. } => {
                children.iter().map(ProofTree::base_fact_count).sum()
            }
            ProofTree::Recursive { inner, .. } => inner.base_fact_count(),
            _ => 0,
        }
    }

    /// Count of rule application nodes.
    pub fn rule_count(&self) -> usize {
        match self {
            ProofTree::RuleApplication { children, .. } => {
                1 + children.iter().map(ProofTree::rule_count).sum::<usize>()
            }
            ProofTree::Recursive { inner, .. } => inner.rule_count(),
            _ => 0,
        }
    }

    /// Count of vector search proof nodes.
    pub fn vector_search_count(&self) -> usize {
        match self {
            ProofTree::VectorSearchProof { .. } => 1,
            ProofTree::RuleApplication { children, .. } => {
                children.iter().map(ProofTree::vector_search_count).sum()
            }
            ProofTree::Recursive { inner, .. } => inner.vector_search_count(),
            _ => 0,
        }
    }

    /// Serialize to a JSON value for export and storage.
    pub fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    /// Format as a human-readable tree string with box-drawing characters.
    pub fn format_tree(&self) -> String {
        let mut output = String::new();
        self.format_node(&mut output, "", true);
        output
    }

    fn format_node(&self, output: &mut String, prefix: &str, is_last: bool) {
        let connector = if prefix.is_empty() {
            ""
        } else if is_last {
            "└── "
        } else {
            "├── "
        };
        let child_prefix = if prefix.is_empty() {
            String::new()
        } else if is_last {
            format!("{prefix}    ")
        } else {
            format!("{prefix}│   ")
        };

        match self {
            ProofTree::BaseFact { relation, values } => {
                let vals = format_values(values);
                output.push_str(&format!("{prefix}{connector}[base] {relation}({vals})\n"));
            }
            ProofTree::RuleApplication {
                rule_name,
                clause_index,
                clause_text,
                bindings,
                children,
            } => {
                output.push_str(&format!(
                    "{prefix}{connector}[rule] {rule_name} (clause {clause_index})\n"
                ));
                output.push_str(&format!("{child_prefix}  clause: {clause_text}\n"));
                if !bindings.is_empty() {
                    let bind_str: Vec<String> = bindings
                        .iter()
                        .map(|(var, val)| format!("{var}={val}"))
                        .collect();
                    output.push_str(&format!(
                        "{child_prefix}  bindings: {}\n",
                        bind_str.join(", ")
                    ));
                }
                for (i, child) in children.iter().enumerate() {
                    child.format_node(output, &child_prefix, i == children.len() - 1);
                }
            }
            ProofTree::NegationProof { relation, pattern } => {
                output.push_str(&format!(
                    "{prefix}{connector}[negation] no matching {relation}({pattern})\n"
                ));
            }
            ProofTree::VectorSearchProof {
                index_name,
                metric,
                query_vector,
                result_id,
                distance,
                k,
                ef_search,
            } => {
                let vec_str = format_vector(query_vector);
                let ef_str = ef_search
                    .map(|ef| format!(", ef_search={ef}"))
                    .unwrap_or_default();
                output.push_str(&format!(
                    "{prefix}{connector}[vector_search] index={index_name}, metric={metric}\n"
                ));
                output.push_str(&format!(
                    "{child_prefix}  query: {vec_str}, k={k}{ef_str}\n"
                ));
                output.push_str(&format!(
                    "{child_prefix}  result: id={result_id}, distance={distance:.6}\n"
                ));
            }
            ProofTree::AggregationProof {
                rule_name,
                aggregate_fn,
                contributing_count,
                sample_inputs,
                full_inputs,
            } => {
                output.push_str(&format!(
                    "{prefix}{connector}[aggregation] {rule_name}.{aggregate_fn}\n"
                ));
                let showing = full_inputs.as_ref().map_or(sample_inputs.len(), Vec::len);
                output.push_str(&format!(
                    "{child_prefix}  {contributing_count} contributing tuples (showing {showing})\n"
                ));
                let display_inputs = full_inputs.as_ref().unwrap_or(sample_inputs);
                for row in display_inputs.iter().take(20) {
                    let vals = format_values(row);
                    output.push_str(&format!("{child_prefix}    ({vals})\n"));
                }
                if display_inputs.len() > 20 {
                    output.push_str(&format!(
                        "{child_prefix}    ... and {} more\n",
                        display_inputs.len() - 20
                    ));
                }
            }
            ProofTree::Recursive {
                rule_name,
                iteration,
                inner,
            } => {
                output.push_str(&format!(
                    "{prefix}{connector}[recursive] {rule_name} (iteration {iteration})\n"
                ));
                inner.format_node(output, &child_prefix, true);
            }
            ProofTree::Truncated { depth_limit } => {
                output.push_str(&format!(
                    "{prefix}{connector}[truncated] proof exceeds depth limit ({depth_limit})\n"
                ));
            }
        }
    }
}

impl fmt::Display for ProofTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format_tree())
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

/// Format a vector for display, truncating long vectors.
fn format_vector(vec: &[f32]) -> String {
    if vec.len() <= 8 {
        let parts: Vec<String> = vec.iter().map(|v| format!("{v:.4}")).collect();
        format!("[{}]", parts.join(", "))
    } else {
        let first: Vec<String> = vec[..4].iter().map(|v| format!("{v:.4}")).collect();
        let last: Vec<String> = vec[vec.len() - 2..]
            .iter()
            .map(|v| format!("{v:.4}"))
            .collect();
        format!(
            "[{}, ... {} more ..., {}]",
            first.join(", "),
            vec.len() - 6,
            last.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn int(v: i32) -> Value {
        Value::Int32(v)
    }

    fn str_val(s: &str) -> Value {
        Value::String(Arc::from(s))
    }

    #[test]
    fn test_proof_tree_base_fact_format() {
        let tree = ProofTree::BaseFact {
            relation: "edge".to_string(),
            values: vec![int(1), int(2)],
        };
        let fmt = tree.format_tree();
        assert!(fmt.contains("[base] edge(1, 2)"), "got: {fmt}");
    }

    #[test]
    fn test_proof_tree_rule_application_format() {
        let tree = ProofTree::RuleApplication {
            rule_name: "path".to_string(),
            clause_index: 0,
            clause_text: "path(X, Y) <- edge(X, Y)".to_string(),
            bindings: vec![("X".to_string(), int(1)), ("Y".to_string(), int(2))],
            children: vec![ProofTree::BaseFact {
                relation: "edge".to_string(),
                values: vec![int(1), int(2)],
            }],
        };
        let fmt = tree.format_tree();
        assert!(fmt.contains("[rule] path (clause 0)"), "got: {fmt}");
        assert!(fmt.contains("bindings: X=1, Y=2"), "got: {fmt}");
        assert!(fmt.contains("[base] edge(1, 2)"), "got: {fmt}");
    }

    #[test]
    fn test_proof_tree_nested_format() {
        let tree = ProofTree::RuleApplication {
            rule_name: "path".to_string(),
            clause_index: 1,
            clause_text: "path(X, Z) <- edge(X, Y), path(Y, Z)".to_string(),
            bindings: vec![
                ("X".to_string(), int(1)),
                ("Y".to_string(), int(2)),
                ("Z".to_string(), int(3)),
            ],
            children: vec![
                ProofTree::BaseFact {
                    relation: "edge".to_string(),
                    values: vec![int(1), int(2)],
                },
                ProofTree::RuleApplication {
                    rule_name: "path".to_string(),
                    clause_index: 0,
                    clause_text: "path(X, Y) <- edge(X, Y)".to_string(),
                    bindings: vec![("X".to_string(), int(2)), ("Y".to_string(), int(3))],
                    children: vec![ProofTree::BaseFact {
                        relation: "edge".to_string(),
                        values: vec![int(2), int(3)],
                    }],
                },
            ],
        };
        let fmt = tree.format_tree();
        assert!(fmt.contains("[rule] path (clause 1)"), "got: {fmt}");
        assert!(fmt.contains("[base] edge(1, 2)"), "got: {fmt}");
        assert!(fmt.contains("[rule] path (clause 0)"), "got: {fmt}");
        assert!(fmt.contains("[base] edge(2, 3)"), "got: {fmt}");
    }

    #[test]
    fn test_proof_tree_vector_search_format() {
        let tree = ProofTree::VectorSearchProof {
            index_name: "doc_idx".to_string(),
            metric: "cosine".to_string(),
            query_vector: vec![1.0, 2.0, 3.0],
            result_id: 42,
            distance: 0.1523,
            k: 5,
            ef_search: Some(100),
        };
        let fmt = tree.format_tree();
        assert!(fmt.contains("index=doc_idx"), "got: {fmt}");
        assert!(fmt.contains("metric=cosine"), "got: {fmt}");
        assert!(fmt.contains("id=42"), "got: {fmt}");
        assert!(fmt.contains("0.152300"), "got: {fmt}");
        assert!(fmt.contains("k=5"), "got: {fmt}");
        assert!(fmt.contains("ef_search=100"), "got: {fmt}");
    }

    #[test]
    fn test_proof_tree_aggregation_summary() {
        let tree = ProofTree::AggregationProof {
            rule_name: "total".to_string(),
            aggregate_fn: "count".to_string(),
            contributing_count: 100,
            sample_inputs: vec![vec![int(1), int(10)], vec![int(2), int(20)]],
            full_inputs: None,
        };
        let fmt = tree.format_tree();
        assert!(
            fmt.contains("100 contributing tuples (showing 2)"),
            "got: {fmt}"
        );
        assert!(fmt.contains("(1, 10)"), "got: {fmt}");
    }

    #[test]
    fn test_proof_tree_aggregation_full() {
        let all: Vec<Vec<Value>> = (0..5).map(|i| vec![int(i)]).collect();
        let tree = ProofTree::AggregationProof {
            rule_name: "total".to_string(),
            aggregate_fn: "sum".to_string(),
            contributing_count: 5,
            sample_inputs: vec![vec![int(0)]],
            full_inputs: Some(all),
        };
        let fmt = tree.format_tree();
        assert!(
            fmt.contains("5 contributing tuples (showing 5)"),
            "got: {fmt}"
        );
    }

    #[test]
    fn test_proof_tree_depth() {
        let leaf = ProofTree::BaseFact {
            relation: "x".to_string(),
            values: vec![],
        };
        assert_eq!(leaf.depth(), 1);

        let one_level = ProofTree::RuleApplication {
            rule_name: "r".to_string(),
            clause_index: 0,
            clause_text: String::new(),
            bindings: vec![],
            children: vec![leaf.clone()],
        };
        assert_eq!(one_level.depth(), 2);

        let two_level = ProofTree::RuleApplication {
            rule_name: "r".to_string(),
            clause_index: 0,
            clause_text: String::new(),
            bindings: vec![],
            children: vec![one_level],
        };
        assert_eq!(two_level.depth(), 3);
    }

    #[test]
    fn test_proof_tree_node_count() {
        let tree = ProofTree::RuleApplication {
            rule_name: "r".to_string(),
            clause_index: 0,
            clause_text: String::new(),
            bindings: vec![],
            children: vec![
                ProofTree::BaseFact {
                    relation: "a".to_string(),
                    values: vec![],
                },
                ProofTree::BaseFact {
                    relation: "b".to_string(),
                    values: vec![],
                },
            ],
        };
        assert_eq!(tree.node_count(), 3);
    }

    #[test]
    fn test_proof_tree_truncated_display() {
        let tree = ProofTree::Truncated { depth_limit: 50 };
        let fmt = tree.format_tree();
        assert!(fmt.contains("depth limit (50)"), "got: {fmt}");
    }

    #[test]
    fn test_proof_tree_to_json_roundtrip() {
        let tree = ProofTree::RuleApplication {
            rule_name: "path".to_string(),
            clause_index: 0,
            clause_text: "path(X,Y) <- edge(X,Y)".to_string(),
            bindings: vec![("X".to_string(), int(1))],
            children: vec![ProofTree::BaseFact {
                relation: "edge".to_string(),
                values: vec![int(1), int(2)],
            }],
        };
        let json = tree.to_json().expect("serialization should succeed");
        let roundtrip: ProofTree =
            serde_json::from_value(json).expect("deserialization should succeed");
        assert_eq!(tree.depth(), roundtrip.depth());
        assert_eq!(tree.node_count(), roundtrip.node_count());
    }

    #[test]
    fn test_proof_tree_null_values() {
        let tree = ProofTree::BaseFact {
            relation: "data".to_string(),
            values: vec![Value::Null, int(1)],
        };
        let fmt = tree.format_tree();
        assert!(fmt.contains("NULL, 1"), "got: {fmt}");
    }

    #[test]
    fn test_proof_tree_vector_values() {
        let tree = ProofTree::BaseFact {
            relation: "embeddings".to_string(),
            values: vec![int(1), Value::Vector(Arc::new(vec![0.1, 0.2, 0.3]))],
        };
        let fmt = tree.format_tree();
        assert!(fmt.contains("embeddings(1,"), "got: {fmt}");
    }

    #[test]
    fn test_proof_tree_timestamp_values() {
        let tree = ProofTree::BaseFact {
            relation: "events".to_string(),
            values: vec![str_val("click"), Value::Timestamp(1700000000000)],
        };
        let fmt = tree.format_tree();
        assert!(fmt.contains("events(\"click\""), "got: {fmt}");
    }

    #[test]
    fn test_proof_tree_special_chars() {
        let tree = ProofTree::BaseFact {
            relation: "data".to_string(),
            values: vec![str_val("hello \"world\""), str_val("line\nnewline")],
        };
        let json = tree.to_json().expect("serialization should succeed");
        let _roundtrip: ProofTree =
            serde_json::from_value(json).expect("deserialization should succeed");
    }

    #[test]
    fn test_proof_tree_recursive_format() {
        let tree = ProofTree::Recursive {
            rule_name: "path".to_string(),
            iteration: 3,
            inner: Box::new(ProofTree::BaseFact {
                relation: "edge".to_string(),
                values: vec![int(1), int(2)],
            }),
        };
        let fmt = tree.format_tree();
        assert!(fmt.contains("[recursive] path (iteration 3)"), "got: {fmt}");
        assert!(fmt.contains("[base] edge(1, 2)"), "got: {fmt}");
    }

    #[test]
    fn test_proof_tree_negation_format() {
        let tree = ProofTree::NegationProof {
            relation: "danger".to_string(),
            pattern: "X=1".to_string(),
        };
        let fmt = tree.format_tree();
        assert!(
            fmt.contains("[negation] no matching danger(X=1)"),
            "got: {fmt}"
        );
    }

    #[test]
    fn test_proof_tree_base_fact_count() {
        let tree = ProofTree::RuleApplication {
            rule_name: "r".to_string(),
            clause_index: 0,
            clause_text: String::new(),
            bindings: vec![],
            children: vec![
                ProofTree::BaseFact {
                    relation: "a".to_string(),
                    values: vec![],
                },
                ProofTree::RuleApplication {
                    rule_name: "s".to_string(),
                    clause_index: 0,
                    clause_text: String::new(),
                    bindings: vec![],
                    children: vec![
                        ProofTree::BaseFact {
                            relation: "b".to_string(),
                            values: vec![],
                        },
                        ProofTree::BaseFact {
                            relation: "c".to_string(),
                            values: vec![],
                        },
                    ],
                },
            ],
        };
        assert_eq!(tree.base_fact_count(), 3);
        assert_eq!(tree.rule_count(), 2);
    }

    #[test]
    fn test_proof_tree_vector_search_count() {
        let tree = ProofTree::RuleApplication {
            rule_name: "similar".to_string(),
            clause_index: 0,
            clause_text: String::new(),
            bindings: vec![],
            children: vec![
                ProofTree::VectorSearchProof {
                    index_name: "idx".to_string(),
                    metric: "cosine".to_string(),
                    query_vector: vec![1.0],
                    result_id: 1,
                    distance: 0.1,
                    k: 5,
                    ef_search: None,
                },
                ProofTree::BaseFact {
                    relation: "doc".to_string(),
                    values: vec![int(1), str_val("title")],
                },
            ],
        };
        assert_eq!(tree.vector_search_count(), 1);
        assert_eq!(tree.base_fact_count(), 1);
    }

    #[test]
    fn test_why_not_format() {
        let explanation = WhyNotExplanation {
            relation: "path".to_string(),
            target_values: vec![int(1), int(99)],
            rule_failures: vec![
                RuleFailure {
                    rule_name: "path".to_string(),
                    clause_index: 0,
                    clause_text: "path(X, Y) <- edge(X, Y)".to_string(),
                    blocker: Blocker::BodyAtomFailed {
                        predicate_index: 0,
                        predicate_text: "edge(1, 99)".to_string(),
                        reason: "No matching tuples in edge".to_string(),
                    },
                },
                RuleFailure {
                    rule_name: "path".to_string(),
                    clause_index: 1,
                    clause_text: "path(X, Z) <- edge(X, Y), path(Y, Z)".to_string(),
                    blocker: Blocker::BodyAtomFailed {
                        predicate_index: 1,
                        predicate_text: "path(Y, 99)".to_string(),
                        reason: "No intermediate Y where path(Y, 99) holds".to_string(),
                    },
                },
            ],
        };
        let fmt = explanation.format_explanation();
        assert!(fmt.contains("path(1, 99) was NOT derived:"), "got: {fmt}");
        assert!(fmt.contains("Blocker:"), "got: {fmt}");
        assert!(fmt.contains("No matching tuples in edge"), "got: {fmt}");
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
    fn test_why_not_to_json() {
        let explanation = WhyNotExplanation {
            relation: "path".to_string(),
            target_values: vec![int(1)],
            rule_failures: vec![RuleFailure {
                rule_name: "path".to_string(),
                clause_index: 0,
                clause_text: "path(X) <- edge(X, _)".to_string(),
                blocker: Blocker::HeadUnificationFailed {
                    reason: "arity mismatch".to_string(),
                },
            }],
        };
        let json = explanation.to_json().expect("serialization should succeed");
        assert!(json["rule_failures"][0]["blocker"]["type"] == "head_unification_failed");
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

    #[test]
    fn test_proof_config_default() {
        let config = ProofConfig::default();
        assert_eq!(config.max_depth, 50);
        assert_eq!(config.max_proofs_per_tuple, 5);
        assert!(!config.full_mode);
        assert_eq!(config.aggregation_sample_size, 10);
    }

    #[test]
    fn test_format_vector_short() {
        let v = format_vector(&[1.0, 2.0, 3.0]);
        assert!(v.contains("1.0000"), "got: {v}");
        assert!(v.contains("3.0000"), "got: {v}");
    }

    #[test]
    fn test_format_vector_long() {
        let v: Vec<f32> = (0..768).map(|i| i as f32 * 0.01).collect();
        let s = format_vector(&v);
        assert!(s.contains("... 762 more ..."), "got: {s}");
    }
}
