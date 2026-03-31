//! Body predicate proving and candidate enumeration.
//!
//! Extracted from backward_chaining.rs to keep modules under 500 lines.
//! Contains `prove_body` (left-to-right body predicate proving) and
//! `enumerate_derived_candidates` (forward candidate generation for derived relations).

use crate::ast::BodyPredicate;
use crate::provenance::backward_chaining::{build_proofs_inner, ProofContext};
use crate::provenance::unification::{
    evaluate_comparison, find_matching_tuples, format_bound_terms, substitute_atom, Bindings,
    BoundTerm,
};
use crate::provenance::ProofTree;
use crate::value::Value;
use std::collections::HashSet;

/// Maximum number of candidates that `enumerate_derived_candidates` will produce
/// per relation. Prevents exponential blowup with deeply nested derived relations.
pub const MAX_DERIVED_CANDIDATES: usize = 1000;

/// Prove all body predicates, propagating bindings left-to-right.
///
/// Returns all valid combinations of (final_bindings, child_proof_trees).
pub fn prove_body(
    body: &[BodyPredicate],
    initial_bindings: Bindings,
    ctx: &ProofContext<'_>,
    visited: &mut HashSet<(String, Vec<Value>)>,
    depth: usize,
) -> Result<Vec<(Bindings, Vec<ProofTree>)>, String> {
    let mut states: Vec<(Bindings, Vec<ProofTree>)> = vec![(initial_bindings, Vec::new())];

    for (pred_idx, pred) in body.iter().enumerate() {
        let mut next_states = Vec::new();
        for (bindings, children_so_far) in &states {
            match pred {
                BodyPredicate::Positive(atom) => {
                    let bound = substitute_atom(atom, bindings);

                    // First try base data (includes materialized views)
                    let mut matches = find_matching_tuples(&atom.relation, &bound, ctx.base_data);
                    if matches.is_empty() && ctx.base_data.contains_key(&atom.relation) {
                        // Debug: show bound pattern and first tuple to compare
                        let bound_dbg: Vec<String> = bound.iter().map(|b| format!("{b:?}")).collect();
                        let first_tuple = ctx.base_data.get(&atom.relation).and_then(|t| t.first());
                        let tuple_dbg = first_tuple.map(|t| {
                            (0..t.arity()).filter_map(|i| t.get(i).map(|v| format!("{v:?}"))).collect::<Vec<_>>().join(", ")
                        });
                        tracing::debug!(
                            relation = %atom.relation,
                            bound = ?bound_dbg,
                            first_tuple = ?tuple_dbg,
                            base_count = ctx.base_data.get(&atom.relation).map_or(0, |t| t.len()),
                            "prove_body: 0 matches despite data present"
                        );
                    }

                    // For derived relations with no base matches, enumerate
                    // candidates by trying all rules that produce this relation
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

                        // Recursively prove the matched tuple
                        let mut sub_proofs = Vec::new();
                        build_proofs_inner(
                            &atom.relation,
                            &matched_tuple,
                            ctx,
                            visited,
                            depth,
                            &mut sub_proofs,
                        )?;

                        if let Some(proof) = sub_proofs.into_iter().next() {
                            let mut new_children = children_so_far.clone();
                            new_children.push(proof);
                            next_states.push((extended, new_children));
                        }
                    }
                }
                BodyPredicate::Negated(atom) => {
                    let bound = substitute_atom(atom, bindings);
                    let matches = find_matching_tuples(&atom.relation, &bound, ctx.base_data);
                    if matches.is_empty() {
                        // Negation succeeds: no matching tuple exists
                        let pattern_str = format_bound_terms(&bound);
                        let mut new_children = children_so_far.clone();
                        new_children.push(ProofTree::NegationProof {
                            relation: atom.relation.clone(),
                            pattern: pattern_str,
                        });
                        next_states.push((bindings.clone(), new_children));
                    }
                    // If matches is non-empty, negation fails -> this path is dead
                }
                BodyPredicate::Comparison(lhs, op, rhs) => {
                    match evaluate_comparison(lhs, op, rhs, bindings) {
                        Ok(true) => {
                            next_states.push((bindings.clone(), children_so_far.clone()));
                        }
                        Ok(false) => {} // Comparison failed, path is dead
                        Err(_) => {}    // Unbound variable, path is dead
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
                    // For HNSW, the result is already in the base data as a
                    // synthetic relation. Check if id_var and distance_var are bound.
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

                        // Resolve index metadata from context
                        let info = ctx.index_info.get(index_name);
                        let metric =
                            info.map_or_else(|| "unknown".to_string(), |i| i.metric.clone());

                        // Resolve query vector: from index info, or from
                        // bindings if the query term is a variable
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

                        let mut new_children = children_so_far.clone();
                        new_children.push(ProofTree::VectorSearchProof {
                            index_name: index_name.clone(),
                            metric,
                            query_vector,
                            result_id: rid,
                            distance: dist,
                            k: *k,
                            ef_search: *ef_search,
                        });
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
///
/// Capped at `MAX_DERIVED_CANDIDATES` to prevent exponential blowup
/// when multiple rules each produce many candidates.
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

    for rule in &rules {
        if candidates.len() >= MAX_DERIVED_CANDIDATES {
            break;
        }

        // Build initial bindings from the bound terms matching the head pattern
        let mut head_bindings = Bindings::new();
        for (bt, head_arg) in bound_terms.iter().zip(rule.head.args.iter()) {
            if let BoundTerm::Concrete(val) = bt {
                if let crate::ast::Term::Variable(var_name) = head_arg {
                    head_bindings.insert(var_name.clone(), val.clone());
                }
            }
        }

        // Try to satisfy all body predicates with these partial bindings
        match prove_body(&rule.body, head_bindings, ctx, visited, depth + 1) {
            Ok(results) => {
                for (final_bindings, _) in results {
                    if candidates.len() >= MAX_DERIVED_CANDIDATES {
                        break;
                    }

                    // Construct the head tuple from the final bindings
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
                        // Check that it matches the bound pattern
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
                            // Extract new bindings for unbound variables
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
