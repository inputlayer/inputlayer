//! Negative explanation: why a tuple was NOT derived.
//!
//! For each rule that could produce the target relation, traces through
//! body predicates to find the specific blocker - showing which premises
//! succeeded and which one failed.

use crate::ast::BodyPredicate;
use crate::provenance::backward_chaining::ProofContext;
use crate::provenance::derivation_graph::{
    Conclusion, DerivationGraph, DerivationNode, FactSource, GraphBuilder, NodeKind, WhyNotInfo,
};
use crate::provenance::unification::{
    evaluate_comparison, find_matching_tuples, format_bound_terms, resolve_term_pub,
    substitute_atom, unify_head,
};
use crate::provenance::Blocker;
use crate::value::{Tuple, Value};

/// Explain why a specific tuple was NOT derived.
///
/// Returns a `DerivationGraph` with `WhyNot` nodes showing:
/// - A root node for the target tuple
/// - Per-clause children showing which body atoms succeeded (Fact nodes)
///   and which one failed (WhyNot node with blocker)
pub fn explain_why_not(relation: &str, target: &Tuple, ctx: &ProofContext<'_>) -> DerivationGraph {
    let target_values: Vec<Value> = (0..target.arity())
        .filter_map(|i| target.get(i).cloned())
        .collect();
    let conclusion = Conclusion {
        pred: relation.to_string(),
        args: target_values.clone(),
    };

    let mut builder = GraphBuilder::new();
    let mut clause_children = Vec::new();

    if !ctx.derived_relations.contains(relation) {
        // Base-only relation - no rules produce it
        let id = builder.insert_unique(DerivationNode {
            kind: NodeKind::WhyNot,
            conclusion: conclusion.clone(),
            source: None,
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: Some(WhyNotInfo {
                rule_name: relation.to_string(),
                clause_index: 0,
                clause_text: String::new(),
                blocker: Blocker::HeadUnificationFailed {
                    reason: "No rules produce this relation".to_string(),
                },
            }),
            children: vec![],
        });
        clause_children.push(id);
    } else {
        let rules = ctx.rules_for(relation);

        for (clause_idx, rule) in rules.iter().enumerate() {
            let clause_text = format!("{rule}");

            // Step 1: Try head unification
            let bindings = match unify_head(target, &rule.head) {
                Some(b) => b,
                None => {
                    let id = builder.insert_unique(DerivationNode {
                        kind: NodeKind::WhyNot,
                        conclusion: conclusion.clone(),
                        source: None,
                        rule_id: Some(clause_text),
                        bindings: None,
                        aggregate: None,
                        negation: None,
                        vector_search: None,
                        truncated: None,
                        why_not: Some(WhyNotInfo {
                            rule_name: relation.to_string(),
                            clause_index: clause_idx,
                            clause_text: format!("{rule}"),
                            blocker: Blocker::HeadUnificationFailed {
                                reason: format!(
                                    "Target arity {} does not match head arity {}",
                                    target.arity(),
                                    rule.head.args.len()
                                ),
                            },
                        }),
                        children: vec![],
                    });
                    clause_children.push(id);
                    continue;
                }
            };

            // Step 2: Trace body predicates - show which succeeded and which failed
            let mut current_bindings = bindings;
            let mut body_children = Vec::new();

            for (pred_idx, pred) in rule.body.iter().enumerate() {
                match pred {
                    BodyPredicate::Positive(ref atom) => {
                        let bound = substitute_atom(atom, &current_bindings);
                        let matches = find_matching_tuples(&atom.relation, &bound, ctx.base_data);

                        if matches.is_empty() {
                            // Also check derived_data
                            let derived_matches = ctx
                                .derived_data
                                .map(|d| find_matching_tuples(&atom.relation, &bound, d))
                                .unwrap_or_default();

                            if derived_matches.is_empty() {
                                // This body atom FAILED - record as WhyNot
                                let pattern_str = format_bound_terms(&bound);
                                let id = builder.insert_unique(DerivationNode {
                                    kind: NodeKind::WhyNot,
                                    conclusion: Conclusion {
                                        pred: atom.relation.clone(),
                                        args: bound
                                            .iter()
                                            .filter_map(|b| match b {
                                                crate::provenance::unification::BoundTerm::Concrete(v) => Some(v.clone()),
                                                crate::provenance::unification::BoundTerm::Unbound(_) => None,
                                            })
                                            .collect(),
                                    },
                                    source: None,
                                    rule_id: None,
                                    bindings: None,
                                    aggregate: None,
                                    negation: None,
                                    vector_search: None,
                                    truncated: None,
                                    why_not: Some(WhyNotInfo {
                                        rule_name: atom.relation.clone(),
                                        clause_index: pred_idx,
                                        clause_text: format!("{}({})", atom.relation, pattern_str),
                                        blocker: Blocker::BodyAtomFailed {
                                            predicate_index: pred_idx,
                                            predicate_text: format!(
                                                "{}({})",
                                                atom.relation, pattern_str
                                            ),
                                            reason: format!(
                                                "No matching tuples in {}",
                                                atom.relation
                                            ),
                                        },
                                    }),
                                    children: vec![],
                                });
                                body_children.push(id);
                                break;
                            }

                            // Found in derived_data
                            let (_, new_binds) = &derived_matches[0];
                            current_bindings.extend(new_binds.clone());
                            let arity = derived_matches[0].0.arity().min(atom.args.len());
                            let matched_vals: Vec<Value> = (0..arity)
                                .filter_map(|i| derived_matches[0].0.get(i).cloned())
                                .collect();
                            let id = builder.insert_unique(DerivationNode {
                                kind: NodeKind::Fact,
                                conclusion: Conclusion {
                                    pred: atom.relation.clone(),
                                    args: matched_vals,
                                },
                                source: Some(FactSource::Derived),
                                rule_id: None,
                                bindings: None,
                                aggregate: None,
                                negation: None,
                                vector_search: None,
                                truncated: None,
                                why_not: None,
                                children: vec![],
                            });
                            body_children.push(id);
                        } else {
                            // This body atom SUCCEEDED - record the matching fact
                            let (matched_tuple, new_binds) = &matches[0];
                            current_bindings.extend(new_binds.clone());
                            let matched_vals: Vec<Value> = (0..matched_tuple.arity())
                                .filter_map(|i| matched_tuple.get(i).cloned())
                                .collect();
                            let id = builder.insert_unique(DerivationNode {
                                kind: NodeKind::Fact,
                                conclusion: Conclusion {
                                    pred: atom.relation.clone(),
                                    args: matched_vals,
                                },
                                source: Some(FactSource::Edb),
                                rule_id: None,
                                bindings: None,
                                aggregate: None,
                                negation: None,
                                vector_search: None,
                                truncated: None,
                                why_not: None,
                                children: vec![],
                            });
                            body_children.push(id);
                        }
                    }
                    BodyPredicate::Negated(ref atom) => {
                        let bound = substitute_atom(atom, &current_bindings);
                        let matches = find_matching_tuples(&atom.relation, &bound, ctx.base_data);

                        if !matches.is_empty() {
                            // Negation FAILED (tuple exists that shouldn't)
                            let (matched_tuple, _) = &matches[0];
                            let matched_vals: Vec<Value> = (0..matched_tuple.arity())
                                .filter_map(|i| matched_tuple.get(i).cloned())
                                .collect();
                            let id = builder.insert_unique(DerivationNode {
                                kind: NodeKind::WhyNot,
                                conclusion: Conclusion {
                                    pred: atom.relation.clone(),
                                    args: matched_vals.clone(),
                                },
                                source: None,
                                rule_id: None,
                                bindings: None,
                                aggregate: None,
                                negation: None,
                                vector_search: None,
                                truncated: None,
                                why_not: Some(WhyNotInfo {
                                    rule_name: atom.relation.clone(),
                                    clause_index: pred_idx,
                                    clause_text: format!("!{}(...)", atom.relation),
                                    blocker: Blocker::NegationSucceeded {
                                        relation: atom.relation.clone(),
                                        matching_tuple: matched_vals,
                                    },
                                }),
                                children: vec![],
                            });
                            body_children.push(id);
                            break;
                        }
                        // Negation succeeded - no node needed, continue
                    }
                    BodyPredicate::Comparison(ref lhs, ref op, ref rhs) => {
                        match evaluate_comparison(lhs, op, rhs, &current_bindings) {
                            Ok(true) => {} // Passed, continue
                            Ok(false) => {
                                let lhs_resolved = resolve_term_pub(lhs, &current_bindings);
                                let rhs_resolved = resolve_term_pub(rhs, &current_bindings);
                                let op_str = match op {
                                    crate::ast::ComparisonOp::Equal => "==",
                                    crate::ast::ComparisonOp::NotEqual => "!=",
                                    crate::ast::ComparisonOp::LessThan => "<",
                                    crate::ast::ComparisonOp::LessOrEqual => "<=",
                                    crate::ast::ComparisonOp::GreaterThan => ">",
                                    crate::ast::ComparisonOp::GreaterOrEqual => ">=",
                                };
                                let id = builder.insert_unique(DerivationNode {
                                    kind: NodeKind::WhyNot,
                                    conclusion: conclusion.clone(),
                                    source: None,
                                    rule_id: None,
                                    bindings: None,
                                    aggregate: None,
                                    negation: None,
                                    vector_search: None,
                                    truncated: None,
                                    why_not: Some(WhyNotInfo {
                                        rule_name: relation.to_string(),
                                        clause_index: pred_idx,
                                        clause_text: format!(
                                            "{lhs_resolved} {op_str} {rhs_resolved}"
                                        ),
                                        blocker: Blocker::ComparisonFailed {
                                            comparison_text: format!(
                                                "{lhs_resolved} {op_str} {rhs_resolved}"
                                            ),
                                            lhs_value: lhs_resolved,
                                            rhs_value: rhs_resolved,
                                        },
                                    }),
                                    children: vec![],
                                });
                                body_children.push(id);
                                break;
                            }
                            Err(e) => {
                                let id = builder.insert_unique(DerivationNode {
                                    kind: NodeKind::WhyNot,
                                    conclusion: conclusion.clone(),
                                    source: None,
                                    rule_id: None,
                                    bindings: None,
                                    aggregate: None,
                                    negation: None,
                                    vector_search: None,
                                    truncated: None,
                                    why_not: Some(WhyNotInfo {
                                        rule_name: relation.to_string(),
                                        clause_index: pred_idx,
                                        clause_text: format!("{lhs:?} vs {rhs:?}"),
                                        blocker: Blocker::BodyAtomFailed {
                                            predicate_index: pred_idx,
                                            predicate_text: format!("{lhs:?} {rhs:?}"),
                                            reason: e,
                                        },
                                    }),
                                    children: vec![],
                                });
                                body_children.push(id);
                                break;
                            }
                        }
                    }
                    BodyPredicate::HnswNearest {
                        ref index_name, k, ..
                    } => {
                        let id = builder.insert_unique(DerivationNode {
                            kind: NodeKind::WhyNot,
                            conclusion: conclusion.clone(),
                            source: None,
                            rule_id: None,
                            bindings: None,
                            aggregate: None,
                            negation: None,
                            vector_search: None,
                            truncated: None,
                            why_not: Some(WhyNotInfo {
                                rule_name: relation.to_string(),
                                clause_index: pred_idx,
                                clause_text: format!("hnsw_nearest({index_name}, {k})"),
                                blocker: Blocker::HnswNotInTopK {
                                    index_name: index_name.clone(),
                                    k: *k,
                                    reason: "Target not found in HNSW search results".to_string(),
                                },
                            }),
                            children: vec![],
                        });
                        body_children.push(id);
                        break;
                    }
                }
            }

            // Clause-level node: shows the rule and its body atom results
            let clause_bindings: std::collections::HashMap<String, Value> = current_bindings
                .into_iter()
                .filter(|(name, _)| !name.starts_with("_placeholder_"))
                .collect();
            let id = builder.insert_unique(DerivationNode {
                kind: NodeKind::WhyNot,
                conclusion: conclusion.clone(),
                source: None,
                rule_id: Some(clause_text),
                bindings: if clause_bindings.is_empty() {
                    None
                } else {
                    Some(clause_bindings)
                },
                aggregate: None,
                negation: None,
                vector_search: None,
                truncated: None,
                why_not: None,
                children: body_children,
            });
            clause_children.push(id);
        }
    }

    // Root node
    let root_id = builder.insert_unique(DerivationNode {
        kind: NodeKind::WhyNot,
        conclusion,
        source: None,
        rule_id: None,
        bindings: None,
        aggregate: None,
        negation: None,
        vector_search: None,
        truncated: None,
        why_not: Some(WhyNotInfo {
            rule_name: relation.to_string(),
            clause_index: 0,
            clause_text: String::new(),
            blocker: Blocker::HeadUnificationFailed {
                reason: format!(
                    "{}({}) was NOT derived",
                    relation,
                    target_values
                        .iter()
                        .map(|v| format!("{v}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            },
        }),
        children: clause_children,
    });

    builder.finish(vec![root_id])
}

/// Format a why-not derivation graph as human-readable text for CLI output.
///
/// Derives text directly from the graph structure - no duplicated logic.
pub fn format_why_not_text(graph: &DerivationGraph) -> String {
    let root = match graph.roots.first().and_then(|id| graph.nodes.get(id)) {
        Some(r) => r,
        None => return "No explanation available.\n".to_string(),
    };

    let vals = root
        .conclusion
        .args
        .iter()
        .map(|v| format!("{v}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut output = format!("{}({vals}) was NOT derived:\n", root.conclusion.pred);

    // Check for "no rules" case: root has children but they're all WhyNot
    // nodes without rule_id (meaning no rules produce this relation)
    let has_rule_clauses = root
        .children
        .iter()
        .any(|id| graph.nodes.get(id).is_some_and(|n| n.rule_id.is_some()));

    if root.children.is_empty() || !has_rule_clauses {
        output.push_str("  No rules produce this relation.\n");
        return output;
    }

    for (clause_idx, clause_id) in root.children.iter().enumerate() {
        let clause = match graph.nodes.get(clause_id) {
            Some(c) => c,
            None => continue,
        };

        let rule_text = clause.rule_id.as_deref().unwrap_or("?");
        output.push_str(&format!(
            "\n  Rule: {} (clause {clause_idx})\n",
            clause.conclusion.pred
        ));
        output.push_str(&format!("    {rule_text}\n"));

        // If the clause itself has a blocker (e.g., head unification failed)
        if let Some(ref why_not) = clause.why_not {
            output.push_str(&format!("    Blocker: {}\n", why_not.blocker));
            continue;
        }

        // Otherwise, look at body atom children for the blocker
        for child_id in &clause.children {
            let child = match graph.nodes.get(child_id) {
                Some(c) => c,
                None => continue,
            };
            if child.kind == NodeKind::WhyNot {
                if let Some(ref why_not) = child.why_not {
                    output.push_str(&format!("    Blocker: {}\n", why_not.blocker));
                }
            }
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Atom, ComparisonOp, Term};
    use crate::provenance::derivation_graph::NodeKind;
    use crate::provenance::ProofConfig;
    use std::collections::HashMap;

    fn int(v: i32) -> Value {
        Value::Int32(v)
    }

    fn tuple(vals: Vec<Value>) -> Tuple {
        Tuple::new(vals)
    }

    fn base_data(entries: Vec<(&str, Vec<Vec<Value>>)>) -> HashMap<String, Vec<Tuple>> {
        entries
            .into_iter()
            .map(|(name, rows)| (name.to_string(), rows.into_iter().map(Tuple::new).collect()))
            .collect()
    }

    fn var(s: &str) -> Term {
        Term::Variable(s.to_string())
    }

    fn pos(rel: &str, args: Vec<&str>) -> BodyPredicate {
        BodyPredicate::Positive(Atom {
            relation: rel.to_string(),
            args: args.into_iter().map(|s| var(s)).collect(),
        })
    }

    fn neg(rel: &str, args: Vec<&str>) -> BodyPredicate {
        BodyPredicate::Negated(Atom {
            relation: rel.to_string(),
            args: args.into_iter().map(|s| var(s)).collect(),
        })
    }

    fn rule(head: &str, args: Vec<&str>, body: Vec<BodyPredicate>) -> crate::ast::Rule {
        crate::ast::Rule {
            head: Atom {
                relation: head.to_string(),
                args: args.into_iter().map(|s| var(s)).collect(),
            },
            body,
        }
    }

    #[test]
    fn test_why_not_base_fact_missing() {
        let data = base_data(vec![("edge", vec![vec![int(1), int(2)]])]);
        let ctx = ProofContext::new(&[], &data, ProofConfig::default());
        let graph = explain_why_not("edge", &tuple(vec![int(99), int(99)]), &ctx);
        assert_eq!(graph.roots.len(), 1);
        let root = &graph.nodes[&graph.roots[0]];
        assert_eq!(root.kind, NodeKind::WhyNot);
    }

    #[test]
    fn test_why_not_body_atom_fails() {
        let rules = vec![rule("derived", vec!["X"], vec![pos("base", vec!["X"])])];
        let data = base_data(vec![("base", vec![vec![int(1)]])]);
        let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

        let graph = explain_why_not("derived", &tuple(vec![int(99)]), &ctx);
        let root = &graph.nodes[&graph.roots[0]];
        assert_eq!(root.kind, NodeKind::WhyNot);
        // Root should have a clause child, which should have a WhyNot child for the failed atom
        assert!(!root.children.is_empty());
        let clause = &graph.nodes[&root.children[0]];
        assert!(!clause.children.is_empty());
        let failed_atom = &graph.nodes[&clause.children[0]];
        assert_eq!(failed_atom.kind, NodeKind::WhyNot);
        assert!(failed_atom.why_not.is_some());
    }

    #[test]
    fn test_why_not_join_shows_progress() {
        // path(X, Z) <- edge(X, Y), edge(Y, Z)
        // edge(1,2) exists but edge(2,99) doesn't
        let rules = vec![rule(
            "path",
            vec!["X", "Z"],
            vec![pos("edge", vec!["X", "Y"]), pos("edge", vec!["Y", "Z"])],
        )];
        let data = base_data(vec![(
            "edge",
            vec![vec![int(1), int(2)], vec![int(3), int(99)]],
        )]);
        let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

        let graph = explain_why_not("path", &tuple(vec![int(1), int(99)]), &ctx);
        let root = &graph.nodes[&graph.roots[0]];
        let clause = &graph.nodes[&root.children[0]];

        // Should have 2 children: first edge(1,2) succeeded, second edge(2,99) failed
        assert_eq!(clause.children.len(), 2);
        let first = &graph.nodes[&clause.children[0]];
        let second = &graph.nodes[&clause.children[1]];
        assert_eq!(first.kind, NodeKind::Fact, "first atom should succeed");
        assert_eq!(second.kind, NodeKind::WhyNot, "second atom should fail");
    }

    #[test]
    fn test_why_not_comparison_fails() {
        let rules = vec![crate::ast::Rule {
            head: Atom {
                relation: "big".to_string(),
                args: vec![var("X"), var("S")],
            },
            body: vec![
                pos("item", vec!["X", "S"]),
                BodyPredicate::Comparison(var("S"), ComparisonOp::GreaterThan, Term::Constant(100)),
            ],
        }];
        let data = base_data(vec![("item", vec![vec![int(1), int(50)]])]);
        let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

        let graph = explain_why_not("big", &tuple(vec![int(1), int(50)]), &ctx);
        let root = &graph.nodes[&graph.roots[0]];
        let clause = &graph.nodes[&root.children[0]];
        // First child: item(1,50) succeeded. Second child: comparison failed.
        assert_eq!(clause.children.len(), 2);
        let succeeded = &graph.nodes[&clause.children[0]];
        let failed = &graph.nodes[&clause.children[1]];
        assert_eq!(succeeded.kind, NodeKind::Fact);
        assert_eq!(failed.kind, NodeKind::WhyNot);
        let blocker = &failed.why_not.as_ref().unwrap().blocker;
        assert!(matches!(blocker, Blocker::ComparisonFailed { .. }));
    }

    #[test]
    fn test_why_not_negation_blocks() {
        let rules = vec![rule(
            "safe",
            vec!["X"],
            vec![pos("node", vec!["X"]), neg("danger", vec!["X"])],
        )];
        let data = base_data(vec![
            ("node", vec![vec![int(1)], vec![int(2)]]),
            ("danger", vec![vec![int(2)]]),
        ]);
        let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

        let graph = explain_why_not("safe", &tuple(vec![int(2)]), &ctx);
        let root = &graph.nodes[&graph.roots[0]];
        let clause = &graph.nodes[&root.children[0]];
        // node(2) succeeded, !danger(2) failed
        assert_eq!(clause.children.len(), 2);
        let succeeded = &graph.nodes[&clause.children[0]];
        let failed = &graph.nodes[&clause.children[1]];
        assert_eq!(succeeded.kind, NodeKind::Fact);
        assert_eq!(failed.kind, NodeKind::WhyNot);
        assert!(matches!(
            failed.why_not.as_ref().unwrap().blocker,
            Blocker::NegationSucceeded { .. }
        ));
    }

    #[test]
    fn test_why_not_multiple_rules_all_fail() {
        let rules = vec![
            rule("derived", vec!["X"], vec![pos("a", vec!["X"])]),
            rule("derived", vec!["X"], vec![pos("b", vec!["X"])]),
        ];
        let data = base_data(vec![("a", vec![vec![int(1)]]), ("b", vec![vec![int(2)]])]);
        let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

        let graph = explain_why_not("derived", &tuple(vec![int(99)]), &ctx);
        let root = &graph.nodes[&graph.roots[0]];
        // Should have 2 clause children (one per rule)
        assert_eq!(root.children.len(), 2);
    }

    #[test]
    fn test_why_not_wrong_arity() {
        let rules = vec![rule(
            "derived",
            vec!["X", "Y"],
            vec![pos("base", vec!["X", "Y"])],
        )];
        let data = base_data(vec![("base", vec![])]);
        let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

        let graph = explain_why_not("derived", &tuple(vec![int(1)]), &ctx);
        let root = &graph.nodes[&graph.roots[0]];
        assert!(!root.children.is_empty());
        let clause = &graph.nodes[&root.children[0]];
        assert!(clause.why_not.is_some());
        assert!(matches!(
            clause.why_not.as_ref().unwrap().blocker,
            Blocker::HeadUnificationFailed { .. }
        ));
    }

    #[test]
    fn test_why_not_nonexistent_relation() {
        let data = HashMap::new();
        let ctx = ProofContext::new(&[], &data, ProofConfig::default());
        let graph = explain_why_not("nonexistent", &tuple(vec![int(1)]), &ctx);
        assert_eq!(graph.roots.len(), 1);
        let root = &graph.nodes[&graph.roots[0]];
        assert_eq!(root.kind, NodeKind::WhyNot);
    }

    #[test]
    fn test_why_not_graph_json_export() {
        let rules = vec![rule("derived", vec!["X"], vec![pos("base", vec!["X"])])];
        let data = base_data(vec![("base", vec![vec![int(1)]])]);
        let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

        let graph = explain_why_not("derived", &tuple(vec![int(99)]), &ctx);
        let json = graph.to_json().expect("should serialize");
        assert_eq!(json["version"], 1);
        assert!(json["roots"].is_array());
        assert!(json["nodes"].is_object());
    }
}
