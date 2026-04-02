//! Body predicate proving and candidate enumeration for derivation graphs.
//!
//! Contains `prove_body` (left-to-right body predicate proving) and
//! `enumerate_derived_candidates` (forward candidate generation for derived relations).

use crate::ast::BodyPredicate;
use crate::provenance::backward_chaining::{build_node, ProofContext};
use crate::provenance::derivation_graph::{
    Conclusion, DerivationNode, GraphBuilder, NegationInfo, NodeId, NodeKind, VectorSearchInfo,
};
use crate::provenance::unification::{
    evaluate_comparison, find_matching_tuples, format_bound_terms, substitute_atom, Bindings,
    BoundTerm,
};
use crate::value::Value;
use std::collections::HashSet;

/// Maximum number of candidates that `enumerate_derived_candidates` will produce
/// per relation. Prevents exponential blowup with deeply nested derived relations.
pub const MAX_DERIVED_CANDIDATES: usize = 1000;

/// Prove all body predicates, propagating bindings left-to-right.
///
/// Returns all valid combinations of (final_bindings, child_node_ids).
pub fn prove_body(
    body: &[BodyPredicate],
    initial_bindings: Bindings,
    ctx: &ProofContext<'_>,
    builder: &mut GraphBuilder,
    visited: &mut HashSet<(String, Vec<Value>)>,
    depth: usize,
) -> Result<Vec<(Bindings, Vec<NodeId>)>, String> {
    let mut states: Vec<(Bindings, Vec<NodeId>)> = vec![(initial_bindings, Vec::new())];

    for (pred_idx, pred) in body.iter().enumerate() {
        let mut next_states = Vec::new();
        for (bindings, children_so_far) in &states {
            match pred {
                BodyPredicate::Positive(atom) => {
                    let bound = substitute_atom(atom, bindings);

                    // First try base data
                    let mut matches = find_matching_tuples(&atom.relation, &bound, ctx.base_data);

                    // Then try derived data
                    if matches.is_empty() && ctx.is_derived(&atom.relation) {
                        if let Some(derived_data) = ctx.derived_data {
                            matches = find_matching_tuples(&atom.relation, &bound, derived_data);
                        }
                    }

                    // Last resort: enumerate candidates
                    if matches.is_empty() && ctx.is_derived(&atom.relation) {
                        matches = enumerate_derived_candidates(
                            &atom.relation,
                            &bound,
                            ctx,
                            visited,
                            depth,
                        );
                    }

                    for (matched_tuple, new_binds) in matches {
                        let mut extended = bindings.clone();
                        extended.extend(new_binds);

                        // Recursively build derivation node for the matched tuple
                        let sub_ids = build_node(
                            &atom.relation,
                            &matched_tuple,
                            ctx,
                            builder,
                            visited,
                            depth,
                        )?;

                        if let Some(node_id) = sub_ids.into_iter().next() {
                            let mut new_children = children_so_far.clone();
                            new_children.push(node_id);
                            next_states.push((extended, new_children));
                        }
                    }
                }
                BodyPredicate::Negated(atom) => {
                    let bound = substitute_atom(atom, bindings);
                    let matches = find_matching_tuples(&atom.relation, &bound, ctx.base_data);
                    if matches.is_empty() {
                        let pattern_str = format_bound_terms(&bound);
                        let node_id = builder.insert_unique(DerivationNode {
                            kind: NodeKind::Negation,
                            conclusion: Conclusion {
                                pred: atom.relation.clone(),
                                args: bound
                                    .iter()
                                    .filter_map(|b| match b {
                                        BoundTerm::Concrete(v) => Some(v.clone()),
                                        BoundTerm::Unbound(_) => None,
                                    })
                                    .collect(),
                            },
                            rule_id: None,
                            bindings: None,
                            aggregate: None,
                            negation: Some(NegationInfo {
                                pattern: pattern_str,
                            }),
                            vector_search: None,
                            truncated: None,
                            why_not: None,
                            source: None,
                            children: vec![],
                        });
                        let mut new_children = children_so_far.clone();
                        new_children.push(node_id);
                        next_states.push((bindings.clone(), new_children));
                    }
                }
                BodyPredicate::Comparison(lhs, op, rhs) => {
                    match evaluate_comparison(lhs, op, rhs, bindings) {
                        Ok(true) => {
                            next_states.push((bindings.clone(), children_so_far.clone()));
                        }
                        Ok(false) => {}
                        Err(_) => {}
                    }
                }
                BodyPredicate::HnswNearest {
                    index_name,
                    k,
                    id_var,
                    distance_var,
                    ef_search,
                    query: query_term,
                } => {
                    let result_id = bindings.get(id_var);
                    let distance = bindings.get(distance_var);
                    if let (Some(id_val), Some(dist_val)) = (result_id, distance) {
                        let rid = match id_val {
                            Value::Int64(n) => *n,
                            Value::Int32(n) => i64::from(*n),
                            _ => continue,
                        };
                        let dist = match dist_val {
                            Value::Float64(f) => *f,
                            _ => continue,
                        };

                        let info = ctx.index_info.get(index_name);
                        let metric =
                            info.map_or_else(|| "unknown".to_string(), |i| i.metric.clone());

                        let query_vector = info
                            .map(|i| i.query_vector.clone())
                            .or_else(|| match query_term {
                                crate::ast::Term::Variable(v) => {
                                    bindings.get(v).and_then(|val| match val {
                                        Value::Vector(v) => Some(v.as_ref().clone()),
                                        _ => None,
                                    })
                                }
                                crate::ast::Term::VectorLiteral(v) => {
                                    Some(v.iter().map(|x| *x as f32).collect())
                                }
                                _ => None,
                            })
                            .unwrap_or_default();

                        let node_id = builder.insert_unique(DerivationNode {
                            kind: NodeKind::VectorSearch,
                            conclusion: Conclusion {
                                pred: index_name.clone(),
                                args: vec![Value::Int64(rid), Value::Float64(dist)],
                            },
                            rule_id: None,
                            bindings: None,
                            aggregate: None,
                            negation: None,
                            vector_search: Some(VectorSearchInfo {
                                index_name: index_name.clone(),
                                metric,
                                query_vector,
                                result_id: rid,
                                distance: dist,
                                k: *k,
                                ef_search: *ef_search,
                            }),
                            truncated: None,
                            why_not: None,
                            source: None,
                            children: vec![],
                        });

                        let mut new_children = children_so_far.clone();
                        new_children.push(node_id);
                        next_states.push((bindings.clone(), new_children));
                    }
                }
            }
        }
        states = next_states;
        if states.is_empty() {
            return Err(format!(
                "No matching tuples for body predicate {pred_idx}: {pred:?}"
            ));
        }
    }

    Ok(states)
}

/// Public accessor for testing the candidate cap.
#[cfg(test)]
pub fn enumerate_derived_candidates_pub(
    relation: &str,
    bound_terms: &[BoundTerm],
    ctx: &ProofContext<'_>,
    visited: &mut HashSet<(String, Vec<Value>)>,
    depth: usize,
) -> Vec<(crate::value::Tuple, Bindings)> {
    enumerate_derived_candidates(relation, bound_terms, ctx, visited, depth)
}

/// For a derived relation with no base data, enumerate candidate tuples
/// by forward-evaluating each rule clause's body predicates.
fn enumerate_derived_candidates(
    relation: &str,
    bound_terms: &[BoundTerm],
    ctx: &ProofContext<'_>,
    visited: &mut HashSet<(String, Vec<Value>)>,
    depth: usize,
) -> Vec<(crate::value::Tuple, Bindings)> {
    if depth >= ctx.config.max_depth {
        return Vec::new();
    }

    let rules = ctx.rules_for(relation);
    let mut candidates = Vec::new();
    // Need a temporary builder for enumeration (nodes are discarded)
    let mut temp_builder = GraphBuilder::new();

    for rule in &rules {
        if candidates.len() >= MAX_DERIVED_CANDIDATES {
            break;
        }

        let mut head_bindings = Bindings::new();
        for (bt, head_arg) in bound_terms.iter().zip(rule.head.args.iter()) {
            if let BoundTerm::Concrete(val) = bt {
                if let crate::ast::Term::Variable(var_name) = head_arg {
                    head_bindings.insert(var_name.clone(), val.clone());
                }
            }
        }

        match prove_body(
            &rule.body,
            head_bindings,
            ctx,
            &mut temp_builder,
            visited,
            depth + 1,
        ) {
            Ok(results) => {
                for (final_bindings, _) in results {
                    if candidates.len() >= MAX_DERIVED_CANDIDATES {
                        break;
                    }

                    let mut head_values = Vec::new();
                    let mut valid = true;
                    for arg in &rule.head.args {
                        match arg {
                            crate::ast::Term::Variable(name) => {
                                if let Some(val) = final_bindings.get(name) {
                                    head_values.push(val.clone());
                                } else {
                                    valid = false;
                                    break;
                                }
                            }
                            other => {
                                if let Some(val) = super::unification::term_to_value_pub(other) {
                                    head_values.push(val);
                                } else {
                                    valid = false;
                                    break;
                                }
                            }
                        }
                    }
                    if valid {
                        let tuple = crate::value::Tuple::new(head_values);
                        let mut matches_pattern = true;
                        for (i, bt) in bound_terms.iter().enumerate() {
                            if let BoundTerm::Concrete(expected) = bt {
                                if let Some(actual) = tuple.get(i) {
                                    if actual != expected {
                                        matches_pattern = false;
                                        break;
                                    }
                                }
                            }
                        }
                        if matches_pattern {
                            let mut new_binds = Bindings::new();
                            for (i, bt) in bound_terms.iter().enumerate() {
                                if let BoundTerm::Unbound(var) = bt {
                                    if let Some(val) = tuple.get(i) {
                                        new_binds.insert(var.clone(), val.clone());
                                    }
                                }
                            }
                            candidates.push((tuple, new_binds));
                        }
                    }
                }
            }
            Err(_) => continue,
        }
    }

    candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Atom, ComparisonOp, Term};
    use crate::provenance::backward_chaining::ProofContext;
    use crate::provenance::derivation_graph::NodeKind;
    use crate::provenance::ProofConfig;
    use crate::value::{Tuple, Value};
    use std::collections::HashMap;

    fn int(v: i32) -> Value {
        Value::Int32(v)
    }

    fn base_data(entries: Vec<(&str, Vec<Vec<Value>>)>) -> HashMap<String, Vec<Tuple>> {
        entries
            .into_iter()
            .map(|(name, rows)| (name.to_string(), rows.into_iter().map(Tuple::new).collect()))
            .collect()
    }

    fn pos(rel: &str, args: Vec<&str>) -> BodyPredicate {
        BodyPredicate::Positive(Atom {
            relation: rel.to_string(),
            args: args
                .into_iter()
                .map(|s| Term::Variable(s.to_string()))
                .collect(),
        })
    }

    fn neg(rel: &str, args: Vec<&str>) -> BodyPredicate {
        BodyPredicate::Negated(Atom {
            relation: rel.to_string(),
            args: args
                .into_iter()
                .map(|s| Term::Variable(s.to_string()))
                .collect(),
        })
    }

    #[test]
    fn test_positive_base_match() {
        let data = base_data(vec![("edge", vec![vec![int(1), int(2)]])]);
        let ctx = ProofContext::new(&[], &data, ProofConfig::default());
        let mut builder = GraphBuilder::new();
        let mut visited = HashSet::new();
        let mut bindings = Bindings::new();
        bindings.insert("X".into(), int(1));

        let body = vec![pos("edge", vec!["X", "Y"])];
        let results =
            prove_body(&body, bindings, &ctx, &mut builder, &mut visited, 0).expect("should match");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.len(), 1); // one child node
        assert_eq!(results[0].0.get("Y"), Some(&int(2)));
    }

    #[test]
    fn test_positive_derived_fallback() {
        let data = base_data(vec![]);
        let derived = {
            let mut m = HashMap::new();
            m.insert("path".to_string(), vec![Tuple::new(vec![int(1), int(3)])]);
            m
        };
        let rules = vec![crate::ast::Rule {
            head: Atom {
                relation: "path".into(),
                args: vec![Term::Variable("X".into()), Term::Variable("Y".into())],
            },
            body: vec![pos("edge", vec!["X", "Y"])],
        }];
        let ctx =
            ProofContext::new(&rules, &data, ProofConfig::default()).with_derived_data(&derived);
        let mut builder = GraphBuilder::new();
        let mut visited = HashSet::new();
        let bindings = Bindings::new();

        let body = vec![pos("path", vec!["X", "Y"])];
        let results = prove_body(&body, bindings, &ctx, &mut builder, &mut visited, 0)
            .expect("should match via derived_data");
        assert!(!results.is_empty());
    }

    #[test]
    fn test_negation_succeeds() {
        let data = base_data(vec![("node", vec![vec![int(1)]])]);
        let ctx = ProofContext::new(&[], &data, ProofConfig::default());
        let mut builder = GraphBuilder::new();
        let mut visited = HashSet::new();
        let mut bindings = Bindings::new();
        bindings.insert("X".into(), int(1));

        // !danger(X) should succeed since danger is empty
        let body = vec![neg("danger", vec!["X"])];
        let results = prove_body(&body, bindings, &ctx, &mut builder, &mut visited, 0)
            .expect("negation should succeed");
        assert_eq!(results.len(), 1);
        // Should have a negation child node
        let child_id = &results[0].1[0];
        let graph = builder.finish(vec![]);
        let child = graph.nodes.get(child_id).unwrap();
        assert_eq!(child.kind, NodeKind::Negation);
    }

    #[test]
    fn test_negation_fails() {
        let data = base_data(vec![("danger", vec![vec![int(1)]])]);
        let ctx = ProofContext::new(&[], &data, ProofConfig::default());
        let mut builder = GraphBuilder::new();
        let mut visited = HashSet::new();
        let mut bindings = Bindings::new();
        bindings.insert("X".into(), int(1));

        // !danger(X) should fail since danger(1) exists
        let body = vec![neg("danger", vec!["X"])];
        let result = prove_body(&body, bindings, &ctx, &mut builder, &mut visited, 0);
        assert!(result.is_err(), "negation should fail when tuple exists");
    }

    #[test]
    fn test_comparison_passes() {
        let data = base_data(vec![("item", vec![vec![int(1), int(200)]])]);
        let ctx = ProofContext::new(&[], &data, ProofConfig::default());
        let mut builder = GraphBuilder::new();
        let mut visited = HashSet::new();
        let bindings = Bindings::new();

        let body = vec![
            pos("item", vec!["X", "S"]),
            BodyPredicate::Comparison(
                Term::Variable("S".into()),
                ComparisonOp::GreaterThan,
                Term::Constant(100),
            ),
        ];
        let results = prove_body(&body, bindings, &ctx, &mut builder, &mut visited, 0)
            .expect("comparison should pass");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_comparison_fails_filters() {
        let data = base_data(vec![("item", vec![vec![int(1), int(50)]])]);
        let ctx = ProofContext::new(&[], &data, ProofConfig::default());
        let mut builder = GraphBuilder::new();
        let mut visited = HashSet::new();
        let bindings = Bindings::new();

        let body = vec![
            pos("item", vec!["X", "S"]),
            BodyPredicate::Comparison(
                Term::Variable("S".into()),
                ComparisonOp::GreaterThan,
                Term::Constant(100),
            ),
        ];
        let result = prove_body(&body, bindings, &ctx, &mut builder, &mut visited, 0);
        assert!(result.is_err(), "comparison should fail");
    }

    #[test]
    fn test_multi_body_join() {
        let data = base_data(vec![(
            "edge",
            vec![vec![int(1), int(2)], vec![int(2), int(3)]],
        )]);
        let ctx = ProofContext::new(&[], &data, ProofConfig::default());
        let mut builder = GraphBuilder::new();
        let mut visited = HashSet::new();
        let bindings = Bindings::new();

        let body = vec![pos("edge", vec!["X", "Y"]), pos("edge", vec!["Y", "Z"])];
        let results = prove_body(&body, bindings, &ctx, &mut builder, &mut visited, 0)
            .expect("join should work");
        // Should find path 1->2->3
        assert!(!results.is_empty());
        let (final_bindings, children) = &results[0];
        assert_eq!(final_bindings.get("X"), Some(&int(1)));
        assert_eq!(final_bindings.get("Z"), Some(&int(3)));
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn test_empty_states_error() {
        let data = base_data(vec![]);
        let ctx = ProofContext::new(&[], &data, ProofConfig::default());
        let mut builder = GraphBuilder::new();
        let mut visited = HashSet::new();
        let bindings = Bindings::new();

        let body = vec![pos("nonexistent", vec!["X"])];
        let result = prove_body(&body, bindings, &ctx, &mut builder, &mut visited, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_candidate_cap() {
        // Create many rules that produce many candidates
        let mut rules = Vec::new();
        let mut data_entries = Vec::new();
        for i in 0..10 {
            let base_name = format!("base_{i}");
            rules.push(crate::ast::Rule {
                head: Atom {
                    relation: "derived".into(),
                    args: vec![Term::Variable("X".into())],
                },
                body: vec![BodyPredicate::Positive(Atom {
                    relation: base_name.clone(),
                    args: vec![Term::Variable("X".into())],
                })],
            });
            let tuples: Vec<Vec<Value>> = (0..200).map(|j| vec![int(i * 200 + j)]).collect();
            data_entries.push((base_name, tuples.into_iter().map(Tuple::new).collect()));
        }

        let base_data_map: HashMap<String, Vec<Tuple>> = data_entries.into_iter().collect();
        let ctx = ProofContext::new(&rules, &base_data_map, ProofConfig::default());

        let bound_terms = vec![BoundTerm::Unbound("X".into())];
        let mut visited = HashSet::new();
        let candidates =
            enumerate_derived_candidates_pub("derived", &bound_terms, &ctx, &mut visited, 0);

        assert!(
            candidates.len() <= MAX_DERIVED_CANDIDATES,
            "got {} candidates, expected <= {}",
            candidates.len(),
            MAX_DERIVED_CANDIDATES
        );
    }
}
