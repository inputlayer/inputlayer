//! Negative explanation: why a tuple was NOT derived.
//!
//! For each rule that could produce the target relation, identifies the
//! specific body predicate that blocked derivation.

use crate::ast::BodyPredicate;
use crate::provenance::backward_chaining::ProofContext;
use crate::provenance::unification::{
    evaluate_comparison, find_matching_tuples, format_bound_terms, resolve_term_pub,
    substitute_atom, unify_head,
};
use crate::provenance::{Blocker, RuleFailure, WhyNotExplanation};
use crate::value::{Tuple, Value};

/// Explain why a specific tuple was NOT derived in the given relation.
pub fn explain_why_not(
    relation: &str,
    target: &Tuple,
    ctx: &ProofContext<'_>,
) -> WhyNotExplanation {
    let target_values: Vec<Value> = (0..target.arity())
        .filter_map(|i| target.get(i).cloned())
        .collect();

    // If it's a base-only relation (no rules), the fact simply doesn't exist
    if !ctx.derived_relations.contains(relation) {
        return WhyNotExplanation {
            relation: relation.to_string(),
            target_values,
            rule_failures: vec![],
        };
    }

    let rules = ctx.rules_for(relation);
    let mut failures = Vec::new();

    for (clause_idx, rule) in rules.iter().enumerate() {
        let clause_text = format!("{rule}");

        // Step 1: Try head unification
        let bindings = match unify_head(target, &rule.head) {
            Some(b) => b,
            None => {
                failures.push(RuleFailure {
                    rule_name: relation.to_string(),
                    clause_index: clause_idx,
                    clause_text,
                    blocker: Blocker::HeadUnificationFailed {
                        reason: format!(
                            "Target arity {} does not match head arity {}",
                            target.arity(),
                            rule.head.args.len()
                        ),
                    },
                });
                continue;
            }
        };

        // Step 2: Try each body predicate to find the blocker
        let mut current_bindings = bindings;
        let mut found_blocker = false;

        for (pred_idx, pred) in rule.body.iter().enumerate() {
            match pred {
                BodyPredicate::Positive(ref atom) => {
                    let bound = substitute_atom(atom, &current_bindings);
                    let matches = find_matching_tuples(&atom.relation, &bound, ctx.base_data);

                    if matches.is_empty() {
                        let pattern_str = format_bound_terms(&bound);
                        failures.push(RuleFailure {
                            rule_name: relation.to_string(),
                            clause_index: clause_idx,
                            clause_text: clause_text.clone(),
                            blocker: Blocker::BodyAtomFailed {
                                predicate_index: pred_idx,
                                predicate_text: format!("{}({})", atom.relation, pattern_str),
                                reason: format!("No matching tuples in {}", atom.relation),
                            },
                        });
                        found_blocker = true;
                        break;
                    }

                    // Take first match to continue checking subsequent predicates
                    let (_, new_binds) = &matches[0];
                    current_bindings.extend(new_binds.clone());
                }
                BodyPredicate::Negated(ref atom) => {
                    let bound = substitute_atom(atom, &current_bindings);
                    let matches = find_matching_tuples(&atom.relation, &bound, ctx.base_data);

                    if !matches.is_empty() {
                        let (matched_tuple, _) = &matches[0];
                        let matched_vals: Vec<Value> = (0..matched_tuple.arity())
                            .filter_map(|i| matched_tuple.get(i).cloned())
                            .collect();
                        failures.push(RuleFailure {
                            rule_name: relation.to_string(),
                            clause_index: clause_idx,
                            clause_text: clause_text.clone(),
                            blocker: Blocker::NegationSucceeded {
                                relation: atom.relation.clone(),
                                matching_tuple: matched_vals,
                            },
                        });
                        found_blocker = true;
                        break;
                    }
                }
                BodyPredicate::Comparison(ref lhs, ref op, ref rhs) => {
                    match evaluate_comparison(lhs, op, rhs, &current_bindings) {
                        Ok(true) => {} // Comparison passed, continue
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
                            failures.push(RuleFailure {
                                rule_name: relation.to_string(),
                                clause_index: clause_idx,
                                clause_text: clause_text.clone(),
                                blocker: Blocker::ComparisonFailed {
                                    comparison_text: format!(
                                        "{lhs_resolved} {op_str} {rhs_resolved}"
                                    ),
                                    lhs_value: lhs_resolved,
                                    rhs_value: rhs_resolved,
                                },
                            });
                            found_blocker = true;
                            break;
                        }
                        Err(e) => {
                            failures.push(RuleFailure {
                                rule_name: relation.to_string(),
                                clause_index: clause_idx,
                                clause_text: clause_text.clone(),
                                blocker: Blocker::BodyAtomFailed {
                                    predicate_index: pred_idx,
                                    predicate_text: format!("{lhs:?} {rhs:?}"),
                                    reason: e,
                                },
                            });
                            found_blocker = true;
                            break;
                        }
                    }
                }
                BodyPredicate::HnswNearest {
                    ref index_name, k, ..
                } => {
                    failures.push(RuleFailure {
                        rule_name: relation.to_string(),
                        clause_index: clause_idx,
                        clause_text: clause_text.clone(),
                        blocker: Blocker::HnswNotInTopK {
                            index_name: index_name.clone(),
                            k: *k,
                            reason: "Target not found in HNSW search results".to_string(),
                        },
                    });
                    found_blocker = true;
                    break;
                }
            }
        }

        // If no predicate blocked, the rule actually succeeds - the tuple
        // should be derivable. This shouldn't happen if the tuple truly
        // doesn't exist, but handle it gracefully.
        if !found_blocker {
            // All body predicates passed but tuple wasn't derived.
            // This can happen with recursive rules that haven't reached fixpoint.
            failures.push(RuleFailure {
                rule_name: relation.to_string(),
                clause_index: clause_idx,
                clause_text,
                blocker: Blocker::BodyAtomFailed {
                    predicate_index: rule.body.len(),
                    predicate_text: "all predicates".to_string(),
                    reason: "All body predicates matched but tuple was not \
                             materialized (possibly insufficient recursive depth)"
                        .to_string(),
                },
            });
        }
    }

    WhyNotExplanation {
        relation: relation.to_string(),
        target_values,
        rule_failures: failures,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Atom, ComparisonOp, Term};
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
        let exp = explain_why_not("edge", &tuple(vec![int(99), int(99)]), &ctx);
        assert_eq!(exp.relation, "edge");
        // No rules produce edge, so rule_failures is empty
        assert!(exp.rule_failures.is_empty());
    }

    #[test]
    fn test_why_not_body_atom_fails() {
        let rules = vec![rule("derived", vec!["X"], vec![pos("base", vec!["X"])])];
        let data = base_data(vec![("base", vec![vec![int(1)]])]);
        let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

        let exp = explain_why_not("derived", &tuple(vec![int(99)]), &ctx);
        assert_eq!(exp.rule_failures.len(), 1);
        assert!(matches!(
            exp.rule_failures[0].blocker,
            Blocker::BodyAtomFailed { .. }
        ));
    }

    #[test]
    fn test_why_not_join_fails() {
        // path(X, Z) <- edge(X, Y), edge(Y, Z)
        let rules = vec![rule(
            "path",
            vec!["X", "Z"],
            vec![pos("edge", vec!["X", "Y"]), pos("edge", vec!["Y", "Z"])],
        )];
        // Edges exist but don't connect 1 to 99
        let data = base_data(vec![(
            "edge",
            vec![vec![int(1), int(2)], vec![int(3), int(99)]],
        )]);
        let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

        let exp = explain_why_not("path", &tuple(vec![int(1), int(99)]), &ctx);
        assert!(!exp.rule_failures.is_empty());
        // Should fail at the second body atom (edge(Y, 99)) since Y=2 and edge(2, 99) doesn't exist
    }

    #[test]
    fn test_why_not_comparison_fails() {
        // big(X, S) <- item(X, S), S > 100
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

        let exp = explain_why_not("big", &tuple(vec![int(1), int(50)]), &ctx);
        assert!(!exp.rule_failures.is_empty());
        assert!(matches!(
            exp.rule_failures[0].blocker,
            Blocker::ComparisonFailed { .. }
        ));
    }

    #[test]
    fn test_why_not_negation_blocks() {
        // safe(X) <- node(X), !danger(X)
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

        let exp = explain_why_not("safe", &tuple(vec![int(2)]), &ctx);
        assert!(!exp.rule_failures.is_empty());
        assert!(matches!(
            exp.rule_failures[0].blocker,
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

        let exp = explain_why_not("derived", &tuple(vec![int(99)]), &ctx);
        assert_eq!(exp.rule_failures.len(), 2);
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

        // Wrong arity: 1 value for a 2-arity rule
        let exp = explain_why_not("derived", &tuple(vec![int(1)]), &ctx);
        assert!(!exp.rule_failures.is_empty());
        assert!(matches!(
            exp.rule_failures[0].blocker,
            Blocker::HeadUnificationFailed { .. }
        ));
    }

    #[test]
    fn test_why_not_nonexistent_relation() {
        let data = HashMap::new();
        let ctx = ProofContext::new(&[], &data, ProofConfig::default());
        let exp = explain_why_not("nonexistent", &tuple(vec![int(1)]), &ctx);
        assert!(exp.rule_failures.is_empty());
        assert_eq!(exp.relation, "nonexistent");
    }
}
