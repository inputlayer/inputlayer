//! Negative explanation: why a tuple was NOT derived.
//!
//! The verdict comes from the data: a tuple the base facts or the evaluated
//! derived relations hold is derived, and the explanation is its proof.
//! Otherwise each rule clause that could produce the tuple is searched,
//! backtracking over every match of its body, for the derivation attempt
//! that gets furthest. The clause's blocker is the premise that attempt
//! failed on. Premises are read from the same base facts and derived
//! relations, so every premise shown as holding holds, and the blocker fails
//! for the bindings shown.

use crate::ast::{BodyPredicate, Rule, Term};
use crate::provenance::backward_chaining::{build_proof_tree, ProofContext};
use crate::provenance::proof_relations::ProofRelations;
use crate::provenance::proof_tree::{
    Conclusion, FactSource, NodeId, NodeKind, ProofNode, ProofTree, ProofTreeBuilder, WhyNotInfo,
};
use crate::provenance::prove_body::{apply_comparison, evaluation_order, negation_witness};
use crate::provenance::unification::{
    check_computed_head, find_matching_tuples, format_bound_terms, resolve_term_pub,
    substitute_atom, unify_head_explained, Bindings, BoundTerm, ComputedHead, PLACEHOLDER_PREFIX,
};
use crate::provenance::Blocker;
use crate::value::{Tuple, Value};
use std::collections::HashSet;

/// Body matches one clause's search may try before it settles for the
/// furthest attempt found so far.
pub const MAX_SEARCH_STEPS: usize = 100_000;

/// Explain why a specific tuple was NOT derived.
///
/// `ctx` must carry the derived relations of an evaluation that covers
/// `relation` (see [`ProofContext::with_derived_data`]): they decide whether
/// the tuple is derived, and body atoms over derived relations are matched
/// against them.
///
/// For a tuple that is derived, returns its proof tree, whose root is not a
/// `WhyNot` node. Otherwise returns a tree with `WhyNot` nodes showing:
/// - A root node for the target tuple
/// - Per-clause children showing which body atoms succeeded (Fact nodes)
///   and which one failed (WhyNot node with blocker)
pub fn explain_why_not(relation: &str, target: &Tuple, ctx: &ProofContext<'_>) -> ProofTree {
    if let Some((tuple, source)) = holding_tuple(relation, target, ctx) {
        return derived_proof(relation, &tuple, source, ctx);
    }

    let target_values = target.values().to_vec();
    let conclusion = Conclusion {
        pred: relation.to_string(),
        args: target_values.clone(),
    };

    let mut builder = ProofTreeBuilder::new();
    let mut clause_children = Vec::new();

    if !ctx.is_derived(relation) {
        // Base-only relation - no rules produce it
        let id = builder.insert_unique(why_not_node(
            conclusion.clone(),
            None,
            WhyNotInfo {
                rule_name: relation.to_string(),
                clause_index: 0,
                clause_text: String::new(),
                blocker: Blocker::HeadUnificationFailed {
                    reason: "No rules produce this relation".to_string(),
                },
            },
        ));
        clause_children.push(id);
    } else {
        for (clause_idx, rule) in ctx.rules_for(relation).into_iter().enumerate() {
            let id = explain_clause(relation, clause_idx, rule, target, ctx, &mut builder);
            clause_children.push(id);
        }
    }

    // Root node
    let mut root = why_not_node(
        conclusion,
        None,
        WhyNotInfo {
            rule_name: relation.to_string(),
            clause_index: 0,
            clause_text: String::new(),
            blocker: Blocker::HeadUnificationFailed {
                reason: format!(
                    "{relation}({}) was NOT derived",
                    format_values(&target_values)
                ),
            },
        },
    );
    root.children = clause_children;
    let root_id = builder.insert_unique(root);
    builder.finish(vec![root_id])
}

/// The tuple of `relation` equal to `target` that the base facts or the
/// derived relations hold, with where it was found.
fn holding_tuple(
    relation: &str,
    target: &Tuple,
    ctx: &ProofContext<'_>,
) -> Option<(Tuple, FactSource)> {
    let pattern: Vec<BoundTerm> = target
        .values()
        .iter()
        .cloned()
        .map(BoundTerm::Concrete)
        .collect();
    let found = |relations: &ProofRelations<'_>| {
        find_matching_tuples(relation, &pattern, relations)
            .into_iter()
            .next()
            .map(|(tuple, _)| tuple)
    };
    found(&ctx.base_data)
        .map(|tuple| (tuple, FactSource::Edb))
        .or_else(|| {
            let derived = ctx.derived_data.as_ref()?;
            found(derived).map(|tuple| (tuple, FactSource::Derived))
        })
}

/// The proof of a derived tuple; a bare fact node when backward chaining
/// cannot trace it.
fn derived_proof(
    relation: &str,
    tuple: &Tuple,
    source: FactSource,
    ctx: &ProofContext<'_>,
) -> ProofTree {
    if let Ok(tree) = build_proof_tree(relation, tuple, ctx) {
        return tree;
    }
    let mut builder = ProofTreeBuilder::new();
    let id = builder.insert(ProofNode {
        kind: NodeKind::Fact,
        conclusion: Conclusion {
            pred: relation.to_string(),
            args: tuple.values().to_vec(),
        },
        source: Some(source),
        rule_id: None,
        bindings: None,
        aggregate: None,
        negation: None,
        vector_search: None,
        truncated: None,
        why_not: None,
        children: vec![],
    });
    builder.finish(vec![id])
}

/// A `WhyNot` node with no children.
fn why_not_node(conclusion: Conclusion, rule_id: Option<String>, info: WhyNotInfo) -> ProofNode {
    ProofNode {
        kind: NodeKind::WhyNot,
        conclusion,
        source: None,
        rule_id,
        bindings: None,
        aggregate: None,
        negation: None,
        vector_search: None,
        truncated: None,
        why_not: Some(info),
        children: vec![],
    }
}

/// A body atom that held in a derivation attempt.
#[derive(Clone)]
struct Held {
    relation: String,
    tuple: Tuple,
    source: FactSource,
}

/// Where a derivation attempt stopped.
struct Failure {
    /// Body atoms that held before it stopped.
    held: Vec<Held>,
    bindings: Bindings,
    /// The `WhyNot` node of the premise that failed.
    blocker: ProofNode,
}

/// The backtracking search of one clause's body for the derivation attempt
/// that gets furthest.
struct ClauseSearch<'s, 'c> {
    relation: &'s str,
    clause_idx: usize,
    rule: &'s Rule,
    target: &'s Tuple,
    ctx: &'s ProofContext<'c>,
    order: Vec<usize>,
    steps: usize,
    /// The furthest failed attempt, and how many premises it passed.
    furthest: Option<(usize, Failure)>,
    /// Bindings under which the whole body holds, if the search found any.
    holds: Option<Bindings>,
    /// Whether the search stopped at [`MAX_SEARCH_STEPS`].
    exhausted: bool,
}

impl ClauseSearch<'_, '_> {
    /// Search from premise `pos` of the evaluation order on; `false` once the
    /// search should stop.
    fn search(&mut self, pos: usize, bindings: &Bindings, held: &mut Vec<Held>) -> bool {
        let Some(&pred_idx) = self.order.get(pos) else {
            return self.body_holds(pos, bindings, held);
        };
        match &self.rule.body[pred_idx] {
            BodyPredicate::Positive(atom) => {
                let bound = substitute_atom(atom, bindings);
                let matches = self.matches(&atom.relation, &bound);
                if matches.is_empty() {
                    let pattern = format!("{}({})", atom.relation, format_bound_terms(&bound));
                    let node = why_not_node(
                        Conclusion {
                            pred: atom.relation.clone(),
                            args: concrete(&bound),
                        },
                        None,
                        WhyNotInfo {
                            rule_name: atom.relation.clone(),
                            clause_index: pred_idx,
                            clause_text: pattern.clone(),
                            blocker: Blocker::BodyAtomFailed {
                                predicate_index: pred_idx,
                                predicate_text: pattern,
                                reason: format!("No matching tuples in {}", atom.relation),
                            },
                        },
                    );
                    self.fail(pos, bindings, held, node);
                    return true;
                }
                for (tuple, source, new_bindings) in matches {
                    self.steps += 1;
                    if self.steps > MAX_SEARCH_STEPS {
                        self.exhausted = true;
                        return false;
                    }
                    let mut extended = bindings.clone();
                    extended.extend(new_bindings);
                    held.push(Held {
                        relation: atom.relation.clone(),
                        tuple,
                        source,
                    });
                    let go_on = self.search(pos + 1, &extended, held);
                    held.pop();
                    if !go_on {
                        return false;
                    }
                }
                true
            }
            BodyPredicate::Negated(atom) => {
                let bound = substitute_atom(atom, bindings);
                match negation_witness(&atom.relation, &bound, self.ctx) {
                    None => self.search(pos + 1, bindings, held),
                    Some(witness) => {
                        let values = witness.values().to_vec();
                        let node = why_not_node(
                            Conclusion {
                                pred: atom.relation.clone(),
                                args: values.clone(),
                            },
                            None,
                            WhyNotInfo {
                                rule_name: atom.relation.clone(),
                                clause_index: pred_idx,
                                clause_text: format!(
                                    "!{}({})",
                                    atom.relation,
                                    format_bound_terms(&bound)
                                ),
                                blocker: Blocker::NegationSucceeded {
                                    relation: atom.relation.clone(),
                                    matching_tuple: values,
                                },
                            },
                        );
                        self.fail(pos, bindings, held, node);
                        true
                    }
                }
            }
            BodyPredicate::Comparison(lhs, op, rhs) => {
                match apply_comparison(lhs, op, rhs, bindings) {
                    Ok(Some(extended)) => self.search(pos + 1, &extended, held),
                    Ok(None) => {
                        let lhs_value = resolve_term_pub(lhs, bindings);
                        let rhs_value = resolve_term_pub(rhs, bindings);
                        let text = format!("{lhs_value} {op} {rhs_value}");
                        let node = self.clause_blocker(
                            pred_idx,
                            text.clone(),
                            Blocker::ComparisonFailed {
                                comparison_text: text,
                                lhs_value,
                                rhs_value,
                            },
                        );
                        self.fail(pos, bindings, held, node);
                        true
                    }
                    Err(reason) => {
                        let text = format!("{lhs} {op} {rhs}");
                        let node = self.clause_blocker(
                            pred_idx,
                            text.clone(),
                            Blocker::NotExplained {
                                reason: format!("cannot evaluate {text}: {reason}"),
                            },
                        );
                        self.fail(pos, bindings, held, node);
                        true
                    }
                }
            }
            BodyPredicate::HnswNearest { index_name, k, .. } => {
                let text = format!("hnsw_nearest({index_name}, {k})");
                let node = self.clause_blocker(
                    pred_idx,
                    text,
                    Blocker::NotExplained {
                        reason: format!(
                            "nearest-neighbour results of index {index_name} are not re-evaluated by .why_not"
                        ),
                    },
                );
                self.fail(pos, bindings, held, node);
                true
            }
        }
    }

    /// Every body premise held: check the head's computed columns.
    fn body_holds(&mut self, pos: usize, bindings: &Bindings, held: &[Held]) -> bool {
        let has_aggregate = self.rule.head.args.iter().any(Term::is_aggregate);
        let blocker = if has_aggregate {
            None
        } else {
            match check_computed_head(self.target, &self.rule.head, bindings) {
                ComputedHead::Matches => None,
                ComputedHead::Differs(reason) => Some(Blocker::HeadUnificationFailed { reason }),
                ComputedHead::Unevaluable(reason) => Some(Blocker::NotExplained { reason }),
            }
        };
        match blocker {
            Some(blocker) => {
                let node = why_not_node(
                    Conclusion {
                        pred: self.relation.to_string(),
                        args: self.target.values().to_vec(),
                    },
                    None,
                    WhyNotInfo {
                        rule_name: self.relation.to_string(),
                        clause_index: self.clause_idx,
                        clause_text: format!("{}", self.rule.head),
                        blocker,
                    },
                );
                self.fail(pos, bindings, held, node);
                true
            }
            None => {
                self.holds = Some(bindings.clone());
                false
            }
        }
    }

    /// Tuples matching a body atom's pattern, from the derived relations for
    /// a derived relation (they include its base facts) and from the base
    /// facts otherwise.
    fn matches(&self, relation: &str, bound: &[BoundTerm]) -> Vec<(Tuple, FactSource, Bindings)> {
        let derived = self
            .ctx
            .derived_data
            .as_ref()
            .filter(|derived| self.ctx.is_derived(relation) && derived.get(relation).is_some());
        match derived {
            Some(derived) => find_matching_tuples(relation, bound, derived)
                .into_iter()
                .map(|(tuple, bindings)| {
                    let source = if self.ctx.base_data.contains(relation, &tuple) {
                        FactSource::Edb
                    } else {
                        FactSource::Derived
                    };
                    (tuple, source, bindings)
                })
                .collect(),
            None => find_matching_tuples(relation, bound, &self.ctx.base_data)
                .into_iter()
                .map(|(tuple, bindings)| (tuple, FactSource::Edb, bindings))
                .collect(),
        }
    }

    /// The `WhyNot` node of a failed premise that is not an atom.
    fn clause_blocker(&self, pred_idx: usize, text: String, blocker: Blocker) -> ProofNode {
        why_not_node(
            Conclusion {
                pred: self.relation.to_string(),
                args: self.target.values().to_vec(),
            },
            None,
            WhyNotInfo {
                rule_name: self.relation.to_string(),
                clause_index: pred_idx,
                clause_text: text,
                blocker,
            },
        )
    }

    /// Record an attempt that failed at premise `pos`, if it got further than
    /// any before it.
    fn fail(&mut self, pos: usize, bindings: &Bindings, held: &[Held], blocker: ProofNode) {
        if self
            .furthest
            .as_ref()
            .is_some_and(|(furthest, _)| *furthest >= pos)
        {
            return;
        }
        self.furthest = Some((
            pos,
            Failure {
                held: held.to_vec(),
                bindings: bindings.clone(),
                blocker,
            },
        ));
    }
}

/// The values of the bound terms of a pattern.
fn concrete(bound: &[BoundTerm]) -> Vec<Value> {
    bound
        .iter()
        .filter_map(|term| match term {
            BoundTerm::Concrete(value) => Some(value.clone()),
            BoundTerm::Unbound(_) => None,
        })
        .collect()
}

/// The clause-level node explaining why `rule` does not derive `target`.
fn explain_clause(
    relation: &str,
    clause_idx: usize,
    rule: &Rule,
    target: &Tuple,
    ctx: &ProofContext<'_>,
    builder: &mut ProofTreeBuilder,
) -> NodeId {
    let clause_text = format!("{rule}");
    let clause_node = |bindings: Option<&Bindings>, children: Vec<NodeId>| {
        let bindings: std::collections::HashMap<String, Value> = bindings
            .into_iter()
            .flatten()
            .filter(|(name, _)| !name.starts_with(PLACEHOLDER_PREFIX))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        ProofNode {
            kind: NodeKind::WhyNot,
            conclusion: Conclusion {
                pred: relation.to_string(),
                args: target.values().to_vec(),
            },
            source: None,
            rule_id: Some(clause_text.clone()),
            bindings: (!bindings.is_empty()).then_some(bindings),
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            children,
        }
    };
    let clause_failure = |blocker: Blocker| {
        let mut node = clause_node(None, vec![]);
        node.why_not = Some(WhyNotInfo {
            rule_name: relation.to_string(),
            clause_index: clause_idx,
            clause_text: clause_text.clone(),
            blocker,
        });
        node
    };

    // Step 1: Head unification
    let bindings = match unify_head_explained(target, &rule.head) {
        Ok(bindings) => bindings,
        Err(reason) => {
            return builder.insert_unique(clause_failure(Blocker::HeadUnificationFailed { reason }))
        }
    };

    // Step 2: Search the body for the furthest derivation attempt
    let bound: HashSet<String> = bindings.keys().cloned().collect();
    let mut search = ClauseSearch {
        relation,
        clause_idx,
        rule,
        target,
        ctx,
        order: evaluation_order(&rule.body, &bound),
        steps: 0,
        furthest: None,
        holds: None,
        exhausted: false,
    };
    search.search(0, &bindings, &mut Vec::new());

    if let Some(bindings) = search.holds {
        let blocker = if rule.head.args.iter().any(Term::is_aggregate) {
            aggregate_blocker(relation, rule, target, ctx)
        } else {
            Blocker::NotExplained {
                reason: "the body holds for these bindings, yet the evaluation derived no such row"
                    .to_string(),
            }
        };
        let mut node = clause_failure(blocker);
        let filtered = clause_node(Some(&bindings), vec![]);
        node.bindings = filtered.bindings;
        return builder.insert_unique(node);
    }

    let Some((_, failure)) = search.furthest else {
        let reason = if search.exhausted {
            format!("search stopped after {MAX_SEARCH_STEPS} body matches")
        } else {
            "no premise failed".to_string()
        };
        return builder.insert_unique(clause_failure(Blocker::NotExplained { reason }));
    };

    let mut children: Vec<NodeId> = failure
        .held
        .into_iter()
        .map(|held| {
            builder.insert_unique(ProofNode {
                kind: NodeKind::Fact,
                conclusion: Conclusion {
                    pred: held.relation,
                    args: held.tuple.values().to_vec(),
                },
                source: Some(held.source),
                rule_id: None,
                bindings: None,
                aggregate: None,
                negation: None,
                vector_search: None,
                truncated: None,
                why_not: None,
                children: vec![],
            })
        })
        .collect();
    children.push(builder.insert_unique(failure.blocker));
    builder.insert_unique(clause_node(Some(&failure.bindings), children))
}

/// Why an aggregate clause whose group has contributing rows does not derive
/// `target`: the aggregate columns hold other values for the group.
fn aggregate_blocker(
    relation: &str,
    rule: &Rule,
    target: &Tuple,
    ctx: &ProofContext<'_>,
) -> Blocker {
    let pattern: Vec<BoundTerm> = rule
        .head
        .args
        .iter()
        .zip(target.values())
        .enumerate()
        .map(|(i, (term, value))| match term {
            Term::Aggregate(_, _) => BoundTerm::Unbound(format!("{PLACEHOLDER_PREFIX}agg{i}")),
            _ => BoundTerm::Concrete(value.clone()),
        })
        .collect();
    let group_rows = ctx
        .derived_data
        .as_ref()
        .map(|derived| find_matching_tuples(relation, &pattern, derived))
        .unwrap_or_default();
    let Some((row, _)) = group_rows.first() else {
        return Blocker::NotExplained {
            reason: "the group has contributing rows, yet the evaluation derived no row for it"
                .to_string(),
        };
    };
    let differences: Vec<String> = rule
        .head
        .args
        .iter()
        .zip(row.values().iter().zip(target.values()))
        .enumerate()
        .filter(|(_, (term, _))| term.is_aggregate())
        .map(|(i, (term, (actual, wanted)))| {
            format!("column {i}: {term} over this group is {actual}, target has {wanted}")
        })
        .collect();
    Blocker::HeadUnificationFailed {
        reason: differences.join("; "),
    }
}

fn format_values(values: &[Value]) -> String {
    values
        .iter()
        .map(|v| format!("{v}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Format a why-not proof tree as human-readable text for CLI output.
///
/// Derives text directly from the tree structure - no duplicated logic. A
/// tree whose root is not a `WhyNot` node is the proof of a derived tuple.
pub fn format_why_not_text(graph: &ProofTree) -> String {
    let root = match graph.roots.first().and_then(|id| graph.nodes.get(id)) {
        Some(r) => r,
        None => return "No explanation available.\n".to_string(),
    };

    let vals = format_values(&root.conclusion.args);
    if root.kind != NodeKind::WhyNot {
        let mut output = format!("{}({vals}) was derived:\n", root.conclusion.pred);
        for line in graph.format_tree().lines() {
            output.push_str(&format!("  {line}\n"));
        }
        return output;
    }
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
    use crate::provenance::proof_tree::NodeKind;
    use crate::provenance::ProofConfig;
    use crate::value::RelationMap;

    fn int(v: i32) -> Value {
        Value::Int32(v)
    }

    fn tuple(vals: Vec<Value>) -> Tuple {
        Tuple::new(vals)
    }

    fn base_data(entries: Vec<(&str, Vec<Vec<Value>>)>) -> RelationMap {
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
            args: args.into_iter().map(var).collect(),
        })
    }

    fn neg(rel: &str, args: Vec<&str>) -> BodyPredicate {
        BodyPredicate::Negated(Atom {
            relation: rel.to_string(),
            args: args.into_iter().map(var).collect(),
        })
    }

    fn rule(head: &str, args: Vec<&str>, body: Vec<BodyPredicate>) -> crate::ast::Rule {
        crate::ast::Rule {
            head: Atom {
                relation: head.to_string(),
                args: args.into_iter().map(var).collect(),
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
        let data = RelationMap::new();
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
