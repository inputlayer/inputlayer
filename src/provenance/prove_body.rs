//! Body predicate proving and candidate enumeration for proof trees.
//!
//! Contains `prove_body` (left-to-right body predicate proving) and
//! `enumerate_derived_candidates` (forward candidate generation for derived relations).

use crate::ast::BodyPredicate;
use crate::ast::{Atom, ComparisonOp, Term};
use crate::provenance::backward_chaining::{build_node, ProofContext};
use crate::provenance::proof_tree::{
    Conclusion, NegationInfo, NodeId, NodeKind, ProofNode, ProofTreeBuilder, VectorSearchInfo,
};
use crate::provenance::unification::{
    evaluate_comparison, find_matching_tuples, format_bound_terms, resolvable, resolve_value,
    substitute_atom, values_equal, Bindings, BoundTerm,
};
use crate::value::{Tuple, Value};
use std::collections::HashSet;

/// Maximum number of candidates that `enumerate_derived_candidates` will produce
/// per relation. Prevents exponential blowup with deeply nested derived relations.
pub const MAX_DERIVED_CANDIDATES: usize = 1000;

/// The order to evaluate `body` in once `bound` variables are bound: each
/// predicate as soon as it can be evaluated, earlier ones first.
///
/// A positive atom can always be evaluated. A comparison waits until both
/// sides can be evaluated, or until one side can when the other is the
/// variable an equality assigns. A negated atom waits until no other
/// predicate left can bind its variables; those still unbound then are
/// existential, like `_`. Predicates that never become ready (a comparison
/// on a variable nothing binds, say) come last, in source order, and fail
/// when evaluated.
pub fn evaluation_order(body: &[BodyPredicate], bound: &HashSet<String>) -> Vec<usize> {
    let mut bound = bound.clone();
    let mut done = vec![false; body.len()];
    let mut order = Vec::with_capacity(body.len());
    while let Some(next) = (0..body.len()).find(|&i| !done[i] && ready(body, &done, i, &bound)) {
        done[next] = true;
        order.push(next);
        bound.extend(binds(&body[next], &bound));
    }
    order.extend((0..body.len()).filter(|&i| !done[i]));
    order
}

/// Whether `body[i]` can be evaluated with `bound` variables bound, when the
/// predicates marked `done` have been.
fn ready(body: &[BodyPredicate], done: &[bool], i: usize, bound: &HashSet<String>) -> bool {
    match &body[i] {
        BodyPredicate::Positive(_) | BodyPredicate::HnswNearest { .. } => true,
        BodyPredicate::Comparison(lhs, op, rhs) => {
            (resolvable(lhs, bound) && resolvable(rhs, bound))
                || assignment(lhs, op, rhs, bound).is_some()
        }
        BodyPredicate::Negated(atom) => atom_variables(atom)
            .filter(|var| !bound.contains(*var))
            .all(|var| {
                body.iter()
                    .enumerate()
                    .filter(|&(j, _)| j != i && !done[j])
                    .all(|(_, pred)| !binds(pred, bound).contains(var))
            }),
    }
}

/// The variables evaluating `pred` binds, given `bound` ones are bound.
fn binds(pred: &BodyPredicate, bound: &HashSet<String>) -> HashSet<String> {
    match pred {
        BodyPredicate::Positive(atom) => atom_variables(atom).cloned().collect(),
        BodyPredicate::Comparison(lhs, op, rhs) => {
            let mut vars: HashSet<String> = HashSet::new();
            for (var, value) in [(lhs, rhs), (rhs, lhs)] {
                if let (Term::Variable(name), ComparisonOp::Equal) = (var, op) {
                    if !bound.contains(name) && value.variables().iter().all(|v| v != name) {
                        vars.insert(name.clone());
                    }
                }
            }
            vars
        }
        BodyPredicate::HnswNearest {
            id_var,
            distance_var,
            ..
        } => [id_var.clone(), distance_var.clone()].into(),
        BodyPredicate::Negated(_) => HashSet::new(),
    }
}

/// The named variables among an atom's arguments.
fn atom_variables(atom: &Atom) -> impl Iterator<Item = &String> {
    atom.args.iter().filter_map(|term| match term {
        Term::Variable(name) => Some(name),
        _ => None,
    })
}

/// For an equality that assigns a variable `bound` does not hold from a side
/// that can be evaluated: the variable and that side.
pub fn assignment<'t>(
    lhs: &'t Term,
    op: &ComparisonOp,
    rhs: &'t Term,
    bound: &HashSet<String>,
) -> Option<(&'t str, &'t Term)> {
    if *op != ComparisonOp::Equal {
        return None;
    }
    [(lhs, rhs), (rhs, lhs)]
        .into_iter()
        .find_map(|(var, value)| match var {
            Term::Variable(name) if !bound.contains(name) && resolvable(value, bound) => {
                Some((name.as_str(), value))
            }
            _ => None,
        })
}

/// A comparison under `bindings`: `Ok(Some(extended))` when it holds,
/// binding the variable an equality assigns; `Ok(None)` when it fails;
/// `Err` when it cannot be evaluated.
pub fn apply_comparison(
    lhs: &Term,
    op: &ComparisonOp,
    rhs: &Term,
    bindings: &Bindings,
) -> Result<Option<Bindings>, String> {
    let bound: HashSet<String> = bindings.keys().cloned().collect();
    if let Some((var, value)) = assignment(lhs, op, rhs, &bound) {
        let value = resolve_value(value, bindings)
            .ok_or_else(|| format!("Cannot evaluate {value} to assign {var}"))?;
        let mut extended = bindings.clone();
        extended.insert(var.to_string(), value);
        return Ok(Some(extended));
    }
    Ok(evaluate_comparison(lhs, op, rhs, bindings)?.then(|| bindings.clone()))
}

/// A tuple matching `bound` in the base facts or the derived relations, if
/// any: what makes a negated atom fail.
pub fn negation_witness(
    relation: &str,
    bound: &[BoundTerm],
    ctx: &ProofContext<'_>,
) -> Option<Tuple> {
    let found = |relations: &crate::provenance::proof_relations::ProofRelations<'_>| {
        find_matching_tuples(relation, bound, relations)
            .into_iter()
            .next()
            .map(|(tuple, _)| tuple)
    };
    found(&ctx.base_data).or_else(|| ctx.derived_data.as_ref().and_then(found))
}

/// Prove all body predicates, propagating bindings in
/// [`evaluation_order`].
///
/// Returns all valid combinations of (final_bindings, child_node_ids), the
/// children in source order.
pub fn prove_body(
    body: &[BodyPredicate],
    initial_bindings: Bindings,
    ctx: &ProofContext<'_>,
    builder: &mut ProofTreeBuilder,
    visited: &mut HashSet<(String, Vec<Value>)>,
    depth: usize,
) -> Result<Vec<(Bindings, Vec<NodeId>)>, String> {
    let bound: HashSet<String> = initial_bindings.keys().cloned().collect();
    let mut states: Vec<(Bindings, Vec<(usize, NodeId)>)> = vec![(initial_bindings, Vec::new())];

    for pred_idx in evaluation_order(body, &bound) {
        let pred = &body[pred_idx];
        let mut next_states = Vec::new();
        for (bindings, children_so_far) in &states {
            match pred {
                BodyPredicate::Positive(atom) => {
                    let bound = substitute_atom(atom, bindings);

                    // First try base data
                    let mut matches = find_matching_tuples(&atom.relation, &bound, &ctx.base_data);

                    // Then try derived data
                    if matches.is_empty() && ctx.is_derived(&atom.relation) {
                        if let Some(derived_data) = &ctx.derived_data {
                            matches = find_matching_tuples(&atom.relation, &bound, derived_data);
                        }
                    }

                    // Last resort: enumerate candidates of a relation whose
                    // tuples the proof cannot look up
                    if matches.is_empty()
                        && ctx.is_derived(&atom.relation)
                        && !ctx.is_complete(&atom.relation)
                    {
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
                            new_children.push((pred_idx, node_id));
                            next_states.push((extended, new_children));
                        }
                    }
                }
                BodyPredicate::Negated(atom) => {
                    let bound = substitute_atom(atom, bindings);
                    if negation_witness(&atom.relation, &bound, ctx).is_none() {
                        let pattern_str = format_bound_terms(&bound);
                        let node_id = builder.insert_unique(ProofNode {
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
                        new_children.push((pred_idx, node_id));
                        next_states.push((bindings.clone(), new_children));
                    }
                }
                BodyPredicate::Comparison(lhs, op, rhs) => {
                    if let Ok(Some(extended)) = apply_comparison(lhs, op, rhs, bindings) {
                        next_states.push((extended, children_so_far.clone()));
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

                        let node_id = builder.insert_unique(ProofNode {
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
                        new_children.push((pred_idx, node_id));
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

    Ok(states
        .into_iter()
        .map(|(bindings, mut children)| {
            children.sort_by_key(|&(pred_idx, _)| pred_idx);
            (bindings, children.into_iter().map(|(_, id)| id).collect())
        })
        .collect())
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
    let mut temp_builder = ProofTreeBuilder::new();

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

                    let head_values: Option<Vec<Value>> = rule
                        .head
                        .args
                        .iter()
                        .map(|arg| resolve_value(arg, &final_bindings))
                        .collect();
                    if let Some(head_values) = head_values {
                        let tuple = crate::value::Tuple::new(head_values);
                        let mut matches_pattern = true;
                        for (i, bt) in bound_terms.iter().enumerate() {
                            if let BoundTerm::Concrete(expected) = bt {
                                if let Some(actual) = tuple.get(i) {
                                    if !values_equal(actual, expected) {
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
    use crate::provenance::proof_tree::NodeKind;
    use crate::provenance::ProofConfig;
    use crate::value::{Relation, RelationMap, Tuple, Value};

    fn int(v: i32) -> Value {
        Value::Int32(v)
    }

    fn base_data(entries: Vec<(&str, Vec<Vec<Value>>)>) -> RelationMap {
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
        let mut builder = ProofTreeBuilder::new();
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
            let mut m = RelationMap::new();
            m.insert(
                "path".to_string(),
                Relation::from(vec![Tuple::new(vec![int(1), int(3)])]),
            );
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
        let mut builder = ProofTreeBuilder::new();
        let mut visited = HashSet::new();
        let bindings = Bindings::new();

        let body = vec![pos("path", vec!["X", "Y"])];
        let results = prove_body(&body, bindings, &ctx, &mut builder, &mut visited, 0)
            .expect("should match via derived_data");
        assert!(!results.is_empty());
    }

    /// A derived relation the evaluation computed holds all its tuples: a
    /// subgoal with no match there fails instead of being re-derived from the
    /// rules (which, for a recursive relation, re-derives its closure).
    #[test]
    fn test_computed_relation_is_not_re_derived() {
        let rules = vec![crate::ast::Rule {
            head: Atom {
                relation: "path".into(),
                args: vec![Term::Variable("X".into()), Term::Variable("Y".into())],
            },
            body: vec![pos("edge", vec!["X", "Y"])],
        }];
        let data = base_data(vec![("edge", vec![vec![int(1), int(2)]])]);
        let body = vec![pos("path", vec!["X", "Y"])];
        let bindings = || {
            let mut bindings = Bindings::new();
            bindings.insert("X".into(), int(1));
            bindings
        };

        // Without derived data, `path(1, Y)` is enumerated from the rules.
        let ctx = ProofContext::new(&rules, &data, ProofConfig::default());
        let results = prove_body(
            &body,
            bindings(),
            &ctx,
            &mut ProofTreeBuilder::new(),
            &mut HashSet::new(),
            0,
        )
        .expect("enumerated from the rules");
        assert_eq!(results[0].0.get("Y"), Some(&int(2)));

        // The evaluation computed `path` and found no `path(1, _)`.
        let derived: RelationMap = [("path".to_string(), Relation::new())].into();
        let ctx =
            ProofContext::new(&rules, &data, ProofConfig::default()).with_derived_data(&derived);
        let result = prove_body(
            &body,
            bindings(),
            &ctx,
            &mut ProofTreeBuilder::new(),
            &mut HashSet::new(),
            0,
        );
        assert!(result.is_err(), "computed path has no path(1, _)");
    }

    #[test]
    fn test_negation_succeeds() {
        let data = base_data(vec![("node", vec![vec![int(1)]])]);
        let ctx = ProofContext::new(&[], &data, ProofConfig::default());
        let mut builder = ProofTreeBuilder::new();
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
        let mut builder = ProofTreeBuilder::new();
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
        let mut builder = ProofTreeBuilder::new();
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
        let mut builder = ProofTreeBuilder::new();
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

    fn cmp(lhs: Term, op: ComparisonOp, rhs: Term) -> BodyPredicate {
        BodyPredicate::Comparison(lhs, op, rhs)
    }

    fn var(name: &str) -> Term {
        Term::Variable(name.into())
    }

    #[test]
    fn test_evaluation_order_waits_for_bindings() {
        let none = HashSet::new();
        // A comparison waits for the atom that binds its variable.
        let body = vec![
            cmp(var("S"), ComparisonOp::GreaterThan, Term::Constant(100)),
            pos("item", vec!["X", "S"]),
        ];
        assert_eq!(evaluation_order(&body, &none), [1, 0]);

        // A negation waits for every predicate that can bind its variables.
        let body = vec![neg("danger", vec!["X"]), pos("node", vec!["X"])];
        assert_eq!(evaluation_order(&body, &none), [1, 0]);

        // An assignment binds the variable a later-listed filter reads.
        let double = Term::Arithmetic(crate::ast::ArithExpr::Binary {
            op: crate::ast::ArithOp::Mul,
            left: Box::new(crate::ast::ArithExpr::Variable("N".into())),
            right: Box::new(crate::ast::ArithExpr::Constant(2)),
        });
        let body = vec![
            pos("item", vec!["X", "N"]),
            cmp(var("D"), ComparisonOp::GreaterThan, Term::Constant(10)),
            cmp(var("D"), ComparisonOp::Equal, double),
        ];
        assert_eq!(evaluation_order(&body, &none), [0, 2, 1]);

        // A comparison on a variable nothing binds comes last.
        let body = vec![
            cmp(var("Q"), ComparisonOp::Equal, var("R")),
            pos("node", vec!["X"]),
        ];
        assert_eq!(evaluation_order(&body, &none), [1, 0]);
    }

    #[test]
    fn test_comparison_before_its_atom_proves_in_source_order() {
        let data = base_data(vec![("item", vec![vec![int(1), int(200)]])]);
        let ctx = ProofContext::new(&[], &data, ProofConfig::default());
        let mut builder = ProofTreeBuilder::new();
        let mut visited = HashSet::new();
        let body = vec![
            cmp(var("S"), ComparisonOp::GreaterThan, Term::Constant(100)),
            pos("item", vec!["X", "S"]),
            neg("danger", vec!["X"]),
        ];
        let results = prove_body(&body, Bindings::new(), &ctx, &mut builder, &mut visited, 0)
            .expect("comparison should pass once S is bound");
        assert_eq!(results.len(), 1);
        let graph = builder.finish(vec![]);
        let kinds: Vec<&NodeKind> = results[0]
            .1
            .iter()
            .map(|id| &graph.nodes[id].kind)
            .collect();
        assert_eq!(kinds, [&NodeKind::Fact, &NodeKind::Negation]);
    }

    #[test]
    fn test_negation_reads_derived_relations() {
        let data = base_data(vec![("node", vec![vec![int(1)]])]);
        let derived: RelationMap = [(
            "danger".to_string(),
            Relation::from(vec![Tuple::new(vec![int(1)])]),
        )]
        .into_iter()
        .collect();
        let ctx = ProofContext::new(&[], &data, ProofConfig::default()).with_derived_data(&derived);
        let mut builder = ProofTreeBuilder::new();
        let mut visited = HashSet::new();
        let body = vec![pos("node", vec!["X"]), neg("danger", vec!["X"])];
        let result = prove_body(&body, Bindings::new(), &ctx, &mut builder, &mut visited, 0);
        assert!(result.is_err(), "danger(1) is derived, so !danger(1) fails");
    }

    #[test]
    fn test_multi_body_join() {
        let data = base_data(vec![(
            "edge",
            vec![vec![int(1), int(2)], vec![int(2), int(3)]],
        )]);
        let ctx = ProofContext::new(&[], &data, ProofConfig::default());
        let mut builder = ProofTreeBuilder::new();
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
        let mut builder = ProofTreeBuilder::new();
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

        let base_data_map: RelationMap = data_entries.into_iter().collect();
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
