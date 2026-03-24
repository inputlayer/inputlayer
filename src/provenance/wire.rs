//! Wire-serializable proof tree types for client-server communication.
//!
//! Converts between internal `ProofTree` / `WhyNotExplanation` and
//! flattened JSON-friendly structures for the WebSocket and REST protocols.

// WireValue used only in tests for type reference
use crate::provenance::{Blocker, ProofTree, RuleFailure, WhyNotExplanation};
use serde::{Deserialize, Serialize};

/// Wire-serializable proof tree node.
///
/// Uses a flat struct with optional fields rather than a tagged enum
/// for maximum compatibility with JavaScript/Python SDKs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireProofTree {
    /// Node type discriminator
    pub node_type: String,
    // Base fact fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<serde_json::Value>>,
    // Rule application fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clause_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clause_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bindings: Option<Vec<WireBinding>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub children: Option<Vec<WireProofTree>>,
    // Negation fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    // Vector search fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_vector: Option<Vec<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub k: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_search: Option<usize>,
    // Aggregation fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate_fn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contributing_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_inputs: Option<Vec<Vec<serde_json::Value>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full_inputs: Option<Vec<Vec<serde_json::Value>>>,
    // Recursive fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iteration: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inner: Option<Box<WireProofTree>>,
    // Truncated fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depth_limit: Option<usize>,
}

/// A variable binding as wire-serializable pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireBinding {
    pub variable: String,
    pub value: serde_json::Value,
}

/// Wire-serializable why-not explanation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireWhyNotExplanation {
    pub relation: String,
    pub target_values: Vec<serde_json::Value>,
    pub rule_failures: Vec<WireRuleFailure>,
}

/// Wire-serializable rule failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireRuleFailure {
    pub rule_name: String,
    pub clause_index: usize,
    pub clause_text: String,
    pub blocker: WireBlocker,
}

/// Wire-serializable blocker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireBlocker {
    #[serde(rename = "type")]
    pub blocker_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comparison_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lhs_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rhs_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matching_tuple: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub k: Option<usize>,
}

fn value_to_json(v: &crate::value::Value) -> serde_json::Value {
    use crate::value::Value;
    match v {
        Value::Int32(n) => serde_json::Value::Number((*n).into()),
        Value::Int64(n) => serde_json::Value::Number((*n).into()),
        Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Null => serde_json::Value::Null,
        Value::Timestamp(t) => serde_json::Value::Number((*t).into()),
        Value::Vector(v) => serde_json::Value::Array(
            v.iter()
                .map(|x| {
                    serde_json::Number::from_f64(f64::from(*x))
                        .map_or(serde_json::Value::Null, serde_json::Value::Number)
                })
                .collect(),
        ),
        Value::VectorInt8(v) => serde_json::Value::Array(
            v.iter()
                .map(|x| serde_json::Value::Number((*x).into()))
                .collect(),
        ),
    }
}

fn values_to_json(values: &[crate::value::Value]) -> Vec<serde_json::Value> {
    values.iter().map(value_to_json).collect()
}

fn values_2d_to_json(rows: &[Vec<crate::value::Value>]) -> Vec<Vec<serde_json::Value>> {
    rows.iter().map(|row| values_to_json(row)).collect()
}

impl From<&ProofTree> for WireProofTree {
    fn from(tree: &ProofTree) -> Self {
        match tree {
            ProofTree::BaseFact { relation, values } => WireProofTree {
                node_type: "base_fact".to_string(),
                relation: Some(relation.clone()),
                values: Some(values_to_json(values)),
                ..WireProofTree::empty()
            },
            ProofTree::RuleApplication {
                rule_name,
                clause_index,
                clause_text,
                bindings,
                children,
            } => WireProofTree {
                node_type: "rule_application".to_string(),
                rule_name: Some(rule_name.clone()),
                clause_index: Some(*clause_index),
                clause_text: Some(clause_text.clone()),
                bindings: Some(
                    bindings
                        .iter()
                        .map(|(var, val)| WireBinding {
                            variable: var.clone(),
                            value: value_to_json(val),
                        })
                        .collect(),
                ),
                children: Some(children.iter().map(WireProofTree::from).collect()),
                ..WireProofTree::empty()
            },
            ProofTree::NegationProof { relation, pattern } => WireProofTree {
                node_type: "negation".to_string(),
                relation: Some(relation.clone()),
                pattern: Some(pattern.clone()),
                ..WireProofTree::empty()
            },
            ProofTree::VectorSearchProof {
                index_name,
                metric,
                query_vector,
                result_id,
                distance,
                k,
                ef_search,
            } => WireProofTree {
                node_type: "vector_search".to_string(),
                index_name: Some(index_name.clone()),
                metric: Some(metric.clone()),
                query_vector: Some(query_vector.clone()),
                result_id: Some(*result_id),
                distance: Some(*distance),
                k: Some(*k),
                ef_search: *ef_search,
                ..WireProofTree::empty()
            },
            ProofTree::AggregationProof {
                rule_name,
                aggregate_fn,
                contributing_count,
                sample_inputs,
                full_inputs,
            } => WireProofTree {
                node_type: "aggregation".to_string(),
                rule_name: Some(rule_name.clone()),
                aggregate_fn: Some(aggregate_fn.clone()),
                contributing_count: Some(*contributing_count),
                sample_inputs: Some(values_2d_to_json(sample_inputs)),
                full_inputs: full_inputs.as_ref().map(|fi| values_2d_to_json(fi)),
                ..WireProofTree::empty()
            },
            ProofTree::Recursive {
                rule_name,
                iteration,
                inner,
            } => WireProofTree {
                node_type: "recursive".to_string(),
                rule_name: Some(rule_name.clone()),
                iteration: Some(*iteration),
                inner: Some(Box::new(WireProofTree::from(inner.as_ref()))),
                ..WireProofTree::empty()
            },
            ProofTree::Truncated { depth_limit } => WireProofTree {
                node_type: "truncated".to_string(),
                depth_limit: Some(*depth_limit),
                ..WireProofTree::empty()
            },
        }
    }
}

impl WireProofTree {
    /// Create an empty wire proof tree (all fields None) - public accessor.
    pub fn empty_pub() -> Self {
        Self::empty()
    }

    /// Create an empty wire proof tree (all fields None).
    fn empty() -> Self {
        Self {
            node_type: String::new(),
            relation: None,
            values: None,
            rule_name: None,
            clause_index: None,
            clause_text: None,
            bindings: None,
            children: None,
            pattern: None,
            index_name: None,
            metric: None,
            query_vector: None,
            result_id: None,
            distance: None,
            k: None,
            ef_search: None,
            aggregate_fn: None,
            contributing_count: None,
            sample_inputs: None,
            full_inputs: None,
            iteration: None,
            inner: None,
            depth_limit: None,
        }
    }
}

impl From<&WhyNotExplanation> for WireWhyNotExplanation {
    fn from(exp: &WhyNotExplanation) -> Self {
        Self {
            relation: exp.relation.clone(),
            target_values: values_to_json(&exp.target_values),
            rule_failures: exp
                .rule_failures
                .iter()
                .map(WireRuleFailure::from)
                .collect(),
        }
    }
}

impl From<&RuleFailure> for WireRuleFailure {
    fn from(rf: &RuleFailure) -> Self {
        Self {
            rule_name: rf.rule_name.clone(),
            clause_index: rf.clause_index,
            clause_text: rf.clause_text.clone(),
            blocker: WireBlocker::from(&rf.blocker),
        }
    }
}

impl From<&Blocker> for WireBlocker {
    fn from(b: &Blocker) -> Self {
        match b {
            Blocker::HeadUnificationFailed { reason } => WireBlocker {
                blocker_type: "head_unification_failed".to_string(),
                reason: Some(reason.clone()),
                ..WireBlocker::empty()
            },
            Blocker::BodyAtomFailed {
                predicate_index,
                predicate_text,
                reason,
            } => WireBlocker {
                blocker_type: "body_atom_failed".to_string(),
                predicate_index: Some(*predicate_index),
                predicate_text: Some(predicate_text.clone()),
                reason: Some(reason.clone()),
                ..WireBlocker::empty()
            },
            Blocker::ComparisonFailed {
                comparison_text,
                lhs_value,
                rhs_value,
            } => WireBlocker {
                blocker_type: "comparison_failed".to_string(),
                comparison_text: Some(comparison_text.clone()),
                lhs_value: Some(lhs_value.clone()),
                rhs_value: Some(rhs_value.clone()),
                ..WireBlocker::empty()
            },
            Blocker::NegationSucceeded {
                relation,
                matching_tuple,
            } => WireBlocker {
                blocker_type: "negation_succeeded".to_string(),
                relation: Some(relation.clone()),
                matching_tuple: Some(values_to_json(matching_tuple)),
                ..WireBlocker::empty()
            },
            Blocker::HnswNotInTopK {
                index_name,
                k,
                reason,
            } => WireBlocker {
                blocker_type: "hnsw_not_in_topk".to_string(),
                index_name: Some(index_name.clone()),
                k: Some(*k),
                reason: Some(reason.clone()),
                ..WireBlocker::empty()
            },
        }
    }
}

impl WireBlocker {
    fn empty() -> Self {
        Self {
            blocker_type: String::new(),
            reason: None,
            predicate_index: None,
            predicate_text: None,
            comparison_text: None,
            lhs_value: None,
            rhs_value: None,
            relation: None,
            matching_tuple: None,
            index_name: None,
            k: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;
    use std::sync::Arc;

    fn int(v: i32) -> Value {
        Value::Int32(v)
    }

    #[test]
    fn test_wire_proof_tree_base_fact() {
        let tree = ProofTree::BaseFact {
            relation: "edge".to_string(),
            values: vec![int(1), int(2)],
        };
        let wire = WireProofTree::from(&tree);
        assert_eq!(wire.node_type, "base_fact");
        assert_eq!(wire.relation.as_deref(), Some("edge"));
        assert_eq!(wire.values.as_ref().map(Vec::len), Some(2));
    }

    #[test]
    fn test_wire_proof_tree_rule_application() {
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
        let wire = WireProofTree::from(&tree);
        assert_eq!(wire.node_type, "rule_application");
        assert_eq!(wire.children.as_ref().map(Vec::len), Some(1));
        assert_eq!(wire.bindings.as_ref().map(Vec::len), Some(1));
    }

    #[test]
    fn test_wire_proof_tree_vector_search() {
        let tree = ProofTree::VectorSearchProof {
            index_name: "doc_idx".to_string(),
            metric: "cosine".to_string(),
            query_vector: vec![1.0, 2.0],
            result_id: 42,
            distance: 0.15,
            k: 5,
            ef_search: Some(100),
        };
        let wire = WireProofTree::from(&tree);
        assert_eq!(wire.node_type, "vector_search");
        assert_eq!(wire.index_name.as_deref(), Some("doc_idx"));
        assert_eq!(wire.metric.as_deref(), Some("cosine"));
        assert_eq!(wire.result_id, Some(42));
        assert_eq!(wire.distance, Some(0.15));
        assert_eq!(wire.k, Some(5));
        assert_eq!(wire.ef_search, Some(100));
    }

    #[test]
    fn test_wire_proof_tree_aggregation() {
        let tree = ProofTree::AggregationProof {
            rule_name: "total".to_string(),
            aggregate_fn: "count".to_string(),
            contributing_count: 42,
            sample_inputs: vec![vec![int(1)], vec![int(2)]],
            full_inputs: None,
        };
        let wire = WireProofTree::from(&tree);
        assert_eq!(wire.node_type, "aggregation");
        assert_eq!(wire.contributing_count, Some(42));
        assert!(wire.full_inputs.is_none());
    }

    #[test]
    fn test_wire_proof_tree_recursive() {
        let tree = ProofTree::Recursive {
            rule_name: "path".to_string(),
            iteration: 3,
            inner: Box::new(ProofTree::BaseFact {
                relation: "edge".to_string(),
                values: vec![int(1), int(2)],
            }),
        };
        let wire = WireProofTree::from(&tree);
        assert_eq!(wire.node_type, "recursive");
        assert_eq!(wire.iteration, Some(3));
        assert!(wire.inner.is_some());
    }

    #[test]
    fn test_wire_proof_tree_truncated() {
        let tree = ProofTree::Truncated { depth_limit: 50 };
        let wire = WireProofTree::from(&tree);
        assert_eq!(wire.node_type, "truncated");
        assert_eq!(wire.depth_limit, Some(50));
    }

    #[test]
    fn test_wire_proof_tree_negation() {
        let tree = ProofTree::NegationProof {
            relation: "danger".to_string(),
            pattern: "X=1".to_string(),
        };
        let wire = WireProofTree::from(&tree);
        assert_eq!(wire.node_type, "negation");
        assert_eq!(wire.relation.as_deref(), Some("danger"));
        assert_eq!(wire.pattern.as_deref(), Some("X=1"));
    }

    #[test]
    fn test_wire_proof_tree_json_roundtrip() {
        let tree = ProofTree::RuleApplication {
            rule_name: "path".to_string(),
            clause_index: 1,
            clause_text: "path(X,Z) <- edge(X,Y), path(Y,Z)".to_string(),
            bindings: vec![("X".to_string(), int(1)), ("Z".to_string(), int(3))],
            children: vec![
                ProofTree::BaseFact {
                    relation: "edge".to_string(),
                    values: vec![int(1), int(2)],
                },
                ProofTree::BaseFact {
                    relation: "edge".to_string(),
                    values: vec![int(2), int(3)],
                },
            ],
        };
        let wire = WireProofTree::from(&tree);
        let json = serde_json::to_string(&wire).expect("serialize");
        let roundtrip: WireProofTree = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(roundtrip.node_type, "rule_application");
        assert_eq!(roundtrip.children.as_ref().map(Vec::len), Some(2));
    }

    #[test]
    fn test_wire_why_not_explanation() {
        let exp = WhyNotExplanation {
            relation: "path".to_string(),
            target_values: vec![int(1), int(99)],
            rule_failures: vec![RuleFailure {
                rule_name: "path".to_string(),
                clause_index: 0,
                clause_text: "path(X,Y) <- edge(X,Y)".to_string(),
                blocker: Blocker::BodyAtomFailed {
                    predicate_index: 0,
                    predicate_text: "edge(1, 99)".to_string(),
                    reason: "No matching tuples".to_string(),
                },
            }],
        };
        let wire = WireWhyNotExplanation::from(&exp);
        assert_eq!(wire.relation, "path");
        assert_eq!(wire.rule_failures.len(), 1);
        assert_eq!(
            wire.rule_failures[0].blocker.blocker_type,
            "body_atom_failed"
        );
    }

    #[test]
    fn test_wire_blocker_all_variants() {
        let blockers = vec![
            Blocker::HeadUnificationFailed {
                reason: "arity mismatch".to_string(),
            },
            Blocker::BodyAtomFailed {
                predicate_index: 0,
                predicate_text: "edge(1, X)".to_string(),
                reason: "no matches".to_string(),
            },
            Blocker::ComparisonFailed {
                comparison_text: "X > 10".to_string(),
                lhs_value: "5".to_string(),
                rhs_value: "10".to_string(),
            },
            Blocker::NegationSucceeded {
                relation: "danger".to_string(),
                matching_tuple: vec![int(3)],
            },
            Blocker::HnswNotInTopK {
                index_name: "idx".to_string(),
                k: 10,
                reason: "distance too large".to_string(),
            },
        ];

        let expected_types = [
            "head_unification_failed",
            "body_atom_failed",
            "comparison_failed",
            "negation_succeeded",
            "hnsw_not_in_topk",
        ];

        for (blocker, expected_type) in blockers.iter().zip(expected_types.iter()) {
            let wire = WireBlocker::from(blocker);
            assert_eq!(wire.blocker_type, *expected_type);
            let json = serde_json::to_string(&wire).expect("serialize");
            let _roundtrip: WireBlocker = serde_json::from_str(&json).expect("deserialize");
        }
    }

    #[test]
    fn test_wire_proof_tree_with_string_values() {
        let tree = ProofTree::BaseFact {
            relation: "user".to_string(),
            values: vec![int(1), Value::String(Arc::from("alice")), Value::Null],
        };
        let wire = WireProofTree::from(&tree);
        let values = wire.values.as_ref().expect("should have values");
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], serde_json::json!(1));
        assert_eq!(values[1], serde_json::json!("alice"));
        assert!(values[2].is_null());
    }
}
