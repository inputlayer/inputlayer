//! Backward chaining proof tree construction.
//!
//! Given a derived tuple, traces backward through rules and base facts
//! to build a proof tree DAG explaining why the tuple was derived.

use crate::ast::{Rule, Term};
use crate::provenance::proof_tree::{
    AggregateInfo, Conclusion, FactSource, NodeId, NodeKind, ProofNode, ProofTree,
    ProofTreeBuilder, TruncatedInfo,
};
use crate::provenance::unification::unify_head;
use crate::provenance::ProofConfig;
use crate::value::{Tuple, Value};
use std::collections::{HashMap, HashSet};

/// Metadata about an HNSW index for proof enrichment.
#[derive(Debug, Clone)]
pub struct IndexProofInfo {
    /// Distance metric (cosine, euclidean, dot_product, manhattan)
    pub metric: String,
    /// The query vector used in the search (if known)
    pub query_vector: Vec<f32>,
}

/// Context holding all data needed for backward chaining.
pub struct ProofContext<'a> {
    /// All rules in the knowledge graph
    pub rules: &'a [Rule],
    /// Base relation data: relation_name -> list of tuples
    pub base_data: &'a HashMap<String, Vec<Tuple>>,
    /// Derived/materialized relation data from the evaluation engine.
    pub derived_data: Option<&'a HashMap<String, Vec<Tuple>>>,
    /// Names of relations that are derived (have rules defining them)
    pub derived_relations: HashSet<String>,
    /// Configuration (depth limit, full mode, etc.)
    pub config: ProofConfig,
    /// HNSW index metadata: index_name -> info
    pub index_info: HashMap<String, IndexProofInfo>,
}

impl<'a> ProofContext<'a> {
    /// Create a new proof context, computing derived relation set from rules.
    pub fn new(
        rules: &'a [Rule],
        base_data: &'a HashMap<String, Vec<Tuple>>,
        config: ProofConfig,
    ) -> Self {
        let derived_relations: HashSet<String> =
            rules.iter().map(|r| r.head.relation.clone()).collect();
        Self {
            rules,
            base_data,
            derived_data: None,
            derived_relations,
            config,
            index_info: HashMap::new(),
        }
    }

    /// Create a proof context with HNSW index metadata.
    pub fn with_index_info(
        rules: &'a [Rule],
        base_data: &'a HashMap<String, Vec<Tuple>>,
        config: ProofConfig,
        index_info: HashMap<String, IndexProofInfo>,
    ) -> Self {
        let derived_relations: HashSet<String> =
            rules.iter().map(|r| r.head.relation.clone()).collect();
        Self {
            rules,
            base_data,
            derived_data: None,
            derived_relations,
            config,
            index_info,
        }
    }

    /// Set derived/materialized relation data for candidate lookup.
    pub fn with_derived_data(mut self, derived_data: &'a HashMap<String, Vec<Tuple>>) -> Self {
        self.derived_data = Some(derived_data);
        self
    }

    /// Check if a relation is derived (has rules) vs base (only facts).
    pub fn is_derived(&self, relation: &str) -> bool {
        self.derived_relations.contains(relation)
    }

    /// Get all rules whose head matches the given relation name.
    pub fn rules_for(&self, relation: &str) -> Vec<&Rule> {
        self.rules
            .iter()
            .filter(|r| r.head.relation == relation)
            .collect()
    }
}

/// Build a proof tree explaining why a tuple was derived.
///
/// Returns a `ProofTree` with a single root node for the given tuple,
/// containing the full proof tree DAG with shared sub-proofs.
pub fn build_proof_tree(
    relation: &str,
    tuple: &Tuple,
    ctx: &ProofContext<'_>,
) -> Result<ProofTree, String> {
    // Determine expected arity from rule heads for this relation.
    // The engine may return wider tuples (e.g., 4-arity for a 2-arity relation
    // when the rule body references a wider relation). Truncate to correct arity.
    let expected_arity = ctx.rules_for(relation).first().map(|r| r.head.args.len());
    let tuple = if let Some(arity) = expected_arity {
        if tuple.arity() > arity {
            let truncated_vals: Vec<Value> =
                (0..arity).filter_map(|i| tuple.get(i).cloned()).collect();
            Tuple::new(truncated_vals)
        } else {
            tuple.clone()
        }
    } else {
        tuple.clone()
    };

    let mut builder = ProofTreeBuilder::new();
    let mut visited = HashSet::new();

    let node_ids = build_node(relation, &tuple, ctx, &mut builder, &mut visited, 0)?;

    if node_ids.is_empty() {
        return Err(format!(
            "No derivation found for {relation}({})",
            tuple_display(&tuple)
        ));
    }

    // Use first derivation as the root
    Ok(builder.finish(vec![node_ids.into_iter().next().unwrap_or_default()]))
}

/// Recursively build proof nodes, returning node IDs for this tuple.
///
/// May return multiple IDs if there are alternative proof paths
/// (capped by `max_proofs_per_tuple`).
pub(crate) fn build_node(
    relation: &str,
    tuple: &Tuple,
    ctx: &ProofContext<'_>,
    builder: &mut ProofTreeBuilder,
    visited: &mut HashSet<(String, Vec<Value>)>,
    depth: usize,
) -> Result<Vec<NodeId>, String> {
    let values = tuple_values(tuple);

    // Depth limit
    if depth >= ctx.config.max_depth {
        let id = builder.insert_unique(ProofNode {
            kind: NodeKind::Truncated,
            conclusion: Conclusion {
                pred: relation.to_string(),
                args: values,
            },
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: Some(TruncatedInfo {
                depth_limit: ctx.config.max_depth,
            }),
            why_not: None,
            source: None,
            children: vec![],
        });
        return Ok(vec![id]);
    }

    // Check if we already have a node for this (relation, values)
    if let Some(existing) = builder.get_existing(relation, &values) {
        return Ok(vec![existing.clone()]);
    }

    // Cycle detection
    let key = (relation.to_string(), values.clone());
    if visited.contains(&key) {
        return Ok(vec![]); // Cycle - skip to avoid infinite loop
    }
    visited.insert(key.clone());

    let mut result_ids = Vec::new();

    // Base relation: check if tuple exists as a fact (in base_data or derived_data)
    let in_base = tuple_exists_in(relation, tuple, ctx.base_data);
    let in_derived = ctx
        .derived_data
        .is_some_and(|d| tuple_exists_in(relation, tuple, d));

    if !ctx.is_derived(relation) {
        if in_base || in_derived {
            let id = builder.insert(ProofNode {
                kind: NodeKind::Fact,
                conclusion: Conclusion {
                    pred: relation.to_string(),
                    args: values,
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
            result_ids.push(id);
        }
        visited.remove(&key);
        return Ok(result_ids);
    }

    // If it exists as a base fact (not just derived), record it
    if in_base {
        let id = builder.insert(ProofNode {
            kind: NodeKind::Fact,
            conclusion: Conclusion {
                pred: relation.to_string(),
                args: values.clone(),
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
        result_ids.push(id);
        if result_ids.len() >= ctx.config.max_proofs_per_tuple {
            visited.remove(&key);
            return Ok(result_ids);
        }
    }

    // Try each rule clause
    let rules = ctx.rules_for(relation);
    for (clause_idx, rule) in rules.iter().enumerate() {
        if result_ids.len() >= ctx.config.max_proofs_per_tuple {
            break;
        }

        // Check for aggregate terms in the rule head
        let has_aggregate = rule
            .head
            .args
            .iter()
            .any(|t| matches!(t, Term::Aggregate(_, _)));
        if has_aggregate {
            let id = build_aggregate_node(
                relation, tuple, rule, clause_idx, ctx, builder, visited, depth,
            );
            result_ids.push(id);
            visited.remove(&key);
            return Ok(result_ids);
        }

        // Try to unify the target tuple with the rule head
        let bindings = match unify_head(tuple, &rule.head) {
            Some(b) => b,
            None => continue,
        };

        // Try to satisfy all body predicates with these bindings
        match super::prove_body::prove_body(&rule.body, bindings, ctx, builder, visited, depth + 1)
        {
            Ok(body_results) => {
                for (final_bindings, child_ids) in body_results {
                    if result_ids.len() >= ctx.config.max_proofs_per_tuple {
                        break;
                    }
                    let binding_map: HashMap<String, Value> = final_bindings
                        .into_iter()
                        .filter(|(name, _)| !name.starts_with("_placeholder_"))
                        .collect();

                    let id = builder.insert(ProofNode {
                        kind: NodeKind::Rule,
                        conclusion: Conclusion {
                            pred: relation.to_string(),
                            args: values.clone(),
                        },
                        rule_id: Some(format!("{rule}")),
                        bindings: if binding_map.is_empty() {
                            None
                        } else {
                            Some(binding_map)
                        },
                        aggregate: None,
                        negation: None,
                        vector_search: None,
                        truncated: None,
                        why_not: None,
                        source: None,
                        children: child_ids,
                    });
                    result_ids.push(id);
                }
            }
            Err(_) => continue,
        }
    }

    // Fallback: if no rule proof found but tuple exists in derived_data,
    // record it as a fact (the engine materialized it but we can't trace further)
    if result_ids.is_empty() && in_derived {
        let id = builder.insert(ProofNode {
            kind: NodeKind::Fact,
            conclusion: Conclusion {
                pred: relation.to_string(),
                args: values,
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
        result_ids.push(id);
    }

    visited.remove(&key);
    Ok(result_ids)
}

/// Build an aggregate proof node.
///
/// For rules like `reachable_count(City, count<Dest>) <- can_reach(City, Dest)`,
/// creates an Aggregate node with children pointing to the contributing tuples.
#[allow(clippy::too_many_arguments)]
fn build_aggregate_node(
    relation: &str,
    tuple: &Tuple,
    rule: &Rule,
    _clause_idx: usize,
    ctx: &ProofContext<'_>,
    builder: &mut ProofTreeBuilder,
    visited: &mut HashSet<(String, Vec<Value>)>,
    depth: usize,
) -> NodeId {
    let values = tuple_values(tuple);

    // Extract the aggregate function name and variable
    let (aggregate_fn, value_var) = rule
        .head
        .args
        .iter()
        .find_map(|t| {
            if let Term::Aggregate(func, var) = t {
                Some((format!("{func}"), var.clone()))
            } else {
                None
            }
        })
        .unwrap_or_else(|| ("unknown".to_string(), "?".to_string()));

    // Extract the aggregate result value from the tuple
    let result_value = rule
        .head
        .args
        .iter()
        .enumerate()
        .find_map(|(i, t)| {
            if matches!(t, Term::Aggregate(_, _)) {
                tuple.get(i).cloned()
            } else {
                None
            }
        })
        .unwrap_or(Value::Null);

    // Find the group-by columns and their values
    let mut group_bindings: HashMap<String, Value> = HashMap::new();
    for (i, term) in rule.head.args.iter().enumerate() {
        if let Term::Variable(name) = term {
            if let Some(val) = tuple.get(i) {
                group_bindings.insert(name.clone(), val.clone());
            }
        }
    }

    // Find contributing tuples and build child nodes
    let sample_limit = ctx.config.aggregation_sample_size;
    let mut child_ids: Vec<NodeId> = Vec::new();
    let mut all_inputs: Vec<Vec<Value>> = Vec::new();
    let mut contributing_count: usize = 0;

    if let Some(crate::ast::BodyPredicate::Positive(body_atom)) = rule.body.first() {
        let body_tuples = ctx
            .derived_data
            .and_then(|d| d.get(&body_atom.relation))
            .or_else(|| ctx.base_data.get(&body_atom.relation));

        // Determine expected arity from the body atom's args
        let body_arity = body_atom.args.len();

        if let Some(tuples) = body_tuples {
            for t in tuples {
                let mut matches = true;
                for (arg_idx, arg) in body_atom.args.iter().enumerate() {
                    if let Term::Variable(var_name) = arg {
                        if let Some(expected) = group_bindings.get(var_name) {
                            if let Some(actual) = t.get(arg_idx) {
                                if actual != expected {
                                    matches = false;
                                    break;
                                }
                            }
                        }
                    }
                }
                if matches {
                    contributing_count += 1;

                    // Truncate to body atom arity (engine may return wider tuples)
                    let truncated_vals: Vec<Value> = (0..body_arity.min(t.arity()))
                        .filter_map(|i| t.get(i).cloned())
                        .collect();

                    if ctx.config.full_mode || all_inputs.len() < sample_limit {
                        all_inputs.push(truncated_vals.clone());
                    }
                    // Build child proof nodes for contributing tuples
                    // (capped to avoid explosion on large aggregations)
                    if child_ids.len() < sample_limit {
                        let truncated_tuple = Tuple::new(truncated_vals);
                        let child_result = build_node(
                            &body_atom.relation,
                            &truncated_tuple,
                            ctx,
                            builder,
                            visited,
                            depth + 1,
                        );
                        if let Ok(ids) = child_result {
                            if let Some(id) = ids.into_iter().next() {
                                child_ids.push(id);
                            }
                        }
                    }
                }
            }
        }
    }

    let (sample_inputs, full_inputs) = if ctx.config.full_mode {
        let sample = all_inputs.iter().take(sample_limit).cloned().collect();
        (Some(sample), Some(all_inputs))
    } else if all_inputs.is_empty() {
        (None, None)
    } else {
        (Some(all_inputs), None)
    };

    builder.insert_unique(ProofNode {
        kind: NodeKind::Aggregate,
        conclusion: Conclusion {
            pred: relation.to_string(),
            args: values,
        },
        rule_id: Some(format!("{rule}")),
        bindings: if group_bindings.is_empty() {
            None
        } else {
            Some(group_bindings)
        },
        aggregate: Some(AggregateInfo {
            func: aggregate_fn,
            value_var,
            result: result_value,
            contributing_count,
            sample_inputs,
            full_inputs,
        }),
        negation: None,
        vector_search: None,
        truncated: None,
        why_not: None,
        source: None,
        children: child_ids,
    })
}

fn tuple_exists_in(relation: &str, tuple: &Tuple, base_data: &HashMap<String, Vec<Tuple>>) -> bool {
    base_data
        .get(relation)
        .is_some_and(|tuples| tuples.contains(tuple))
}

pub(crate) fn tuple_values(tuple: &Tuple) -> Vec<Value> {
    (0..tuple.arity())
        .filter_map(|i| tuple.get(i).cloned())
        .collect()
}

fn tuple_display(tuple: &Tuple) -> String {
    let vals: Vec<String> = (0..tuple.arity())
        .filter_map(|i| tuple.get(i).map(|v| format!("{v}")))
        .collect();
    vals.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Atom;
    use crate::ast::BodyPredicate;
    use crate::ast::Term;
    use crate::provenance::proof_tree::NodeKind;

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

    fn simple_rule(head_rel: &str, head_args: Vec<&str>, body: Vec<BodyPredicate>) -> Rule {
        Rule {
            head: Atom {
                relation: head_rel.to_string(),
                args: head_args
                    .into_iter()
                    .map(|s| Term::Variable(s.to_string()))
                    .collect(),
            },
            body,
        }
    }

    fn positive(rel: &str, args: Vec<&str>) -> BodyPredicate {
        BodyPredicate::Positive(Atom {
            relation: rel.to_string(),
            args: args
                .into_iter()
                .map(|s| Term::Variable(s.to_string()))
                .collect(),
        })
    }

    fn negated(rel: &str, args: Vec<&str>) -> BodyPredicate {
        BodyPredicate::Negated(Atom {
            relation: rel.to_string(),
            args: args
                .into_iter()
                .map(|s| Term::Variable(s.to_string()))
                .collect(),
        })
    }

    fn default_config() -> ProofConfig {
        ProofConfig::default()
    }

    #[test]
    fn test_base_fact() {
        let data = base_data(vec![("edge", vec![vec![int(1), int(2)]])]);
        let ctx = ProofContext::new(&[], &data, default_config());
        let graph = build_proof_tree("edge", &tuple(vec![int(1), int(2)]), &ctx)
            .expect("should find derivation");
        assert_eq!(graph.node_count(), 1);
        let root = &graph.nodes[&graph.roots[0]];
        assert_eq!(root.kind, NodeKind::Fact);
        assert_eq!(root.conclusion.pred, "edge");
        assert_eq!(root.conclusion.args, vec![int(1), int(2)]);
        assert!(root.children.is_empty());
    }

    #[test]
    fn test_base_fact_missing() {
        let data = base_data(vec![("edge", vec![vec![int(1), int(2)]])]);
        let ctx = ProofContext::new(&[], &data, default_config());
        let result = build_proof_tree("edge", &tuple(vec![int(99), int(99)]), &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_single_rule() {
        let rules = vec![simple_rule(
            "derived",
            vec!["X"],
            vec![positive("base", vec!["X"])],
        )];
        let data = base_data(vec![("base", vec![vec![int(42)]])]);
        let ctx = ProofContext::new(&rules, &data, default_config());

        let graph = build_proof_tree("derived", &tuple(vec![int(42)]), &ctx)
            .expect("should find derivation");

        assert_eq!(graph.node_count(), 2); // rule + fact
        let root = &graph.nodes[&graph.roots[0]];
        assert_eq!(root.kind, NodeKind::Rule);
        assert_eq!(root.conclusion.pred, "derived");
        assert_eq!(root.children.len(), 1);

        let child = &graph.nodes[&root.children[0]];
        assert_eq!(child.kind, NodeKind::Fact);
        assert_eq!(child.conclusion.pred, "base");
    }

    #[test]
    fn test_join_rule() {
        let rules = vec![simple_rule(
            "path",
            vec!["X", "Z"],
            vec![
                positive("edge", vec!["X", "Y"]),
                positive("edge", vec!["Y", "Z"]),
            ],
        )];
        let data = base_data(vec![(
            "edge",
            vec![vec![int(1), int(2)], vec![int(2), int(3)]],
        )]);
        let ctx = ProofContext::new(&rules, &data, default_config());

        let graph = build_proof_tree("path", &tuple(vec![int(1), int(3)]), &ctx)
            .expect("should find derivation");

        let root = &graph.nodes[&graph.roots[0]];
        assert_eq!(root.kind, NodeKind::Rule);
        assert_eq!(root.children.len(), 2); // two edge facts
    }

    #[test]
    fn test_recursive_transitive_closure() {
        let rules = vec![
            simple_rule(
                "path",
                vec!["X", "Y"],
                vec![positive("edge", vec!["X", "Y"])],
            ),
            simple_rule(
                "path",
                vec!["X", "Z"],
                vec![
                    positive("edge", vec!["X", "Y"]),
                    positive("path", vec!["Y", "Z"]),
                ],
            ),
        ];
        let data = base_data(vec![(
            "edge",
            vec![
                vec![int(1), int(2)],
                vec![int(2), int(3)],
                vec![int(3), int(4)],
            ],
        )]);

        let derived = {
            let mut m = HashMap::new();
            m.insert(
                "path".to_string(),
                vec![
                    Tuple::new(vec![int(1), int(2)]),
                    Tuple::new(vec![int(1), int(3)]),
                    Tuple::new(vec![int(1), int(4)]),
                    Tuple::new(vec![int(2), int(3)]),
                    Tuple::new(vec![int(2), int(4)]),
                    Tuple::new(vec![int(3), int(4)]),
                ],
            );
            m
        };

        let ctx = ProofContext::new(&rules, &data, default_config()).with_derived_data(&derived);

        let graph = build_proof_tree("path", &tuple(vec![int(1), int(4)]), &ctx)
            .expect("should find derivation");
        assert!(!graph.has_truncated());
        assert!(graph.max_depth() >= 3);
    }

    #[test]
    fn test_recursive_cycle_detection() {
        let rules = vec![
            simple_rule(
                "path",
                vec!["X", "Y"],
                vec![positive("edge", vec!["X", "Y"])],
            ),
            simple_rule(
                "path",
                vec!["X", "Z"],
                vec![
                    positive("edge", vec!["X", "Y"]),
                    positive("path", vec!["Y", "Z"]),
                ],
            ),
        ];
        // Cyclic: 1->2->3->1
        let data = base_data(vec![(
            "edge",
            vec![
                vec![int(1), int(2)],
                vec![int(2), int(3)],
                vec![int(3), int(1)],
            ],
        )]);
        let ctx = ProofContext::new(&rules, &data, default_config());

        // Should not hang
        let graph = build_proof_tree("path", &tuple(vec![int(1), int(2)]), &ctx)
            .expect("should find derivation");
        assert!(!graph.nodes.is_empty());
    }

    #[test]
    fn test_negation() {
        let rules = vec![simple_rule(
            "safe",
            vec!["X"],
            vec![positive("node", vec!["X"]), negated("danger", vec!["X"])],
        )];
        let data = base_data(vec![
            ("node", vec![vec![int(1)], vec![int(2)]]),
            ("danger", vec![vec![int(2)]]),
        ]);
        let ctx = ProofContext::new(&rules, &data, default_config());

        // Node 1 is safe
        let graph =
            build_proof_tree("safe", &tuple(vec![int(1)]), &ctx).expect("should find derivation");
        let root = &graph.nodes[&graph.roots[0]];
        assert_eq!(root.kind, NodeKind::Rule);
        // Should have a negation child
        let has_negation = root
            .children
            .iter()
            .any(|id| graph.nodes[id].kind == NodeKind::Negation);
        assert!(has_negation);

        // Node 2 is not safe
        let result = build_proof_tree("safe", &tuple(vec![int(2)]), &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_depth_limit() {
        let rules = vec![
            simple_rule(
                "chain",
                vec!["X", "Y"],
                vec![positive("link", vec!["X", "Y"])],
            ),
            simple_rule(
                "chain",
                vec!["X", "Z"],
                vec![
                    positive("link", vec!["X", "Y"]),
                    positive("chain", vec!["Y", "Z"]),
                ],
            ),
        ];
        let links: Vec<Vec<Value>> = (0..100).map(|i| vec![int(i), int(i + 1)]).collect();
        let data = base_data(vec![("link", links)]);

        let mut config = default_config();
        config.max_depth = 5;
        let ctx = ProofContext::new(&rules, &data, config);

        let result = build_proof_tree("chain", &tuple(vec![int(0), int(100)]), &ctx);
        // Should not panic or hang
        if let Ok(graph) = result {
            // May have truncated nodes
            if !graph.nodes.is_empty() {
                assert!(graph.has_truncated() || graph.max_depth() <= 10);
            }
        }
    }

    #[test]
    fn test_aggregation_count() {
        use crate::ast::AggregateFunc;

        let rules = vec![Rule {
            head: Atom {
                relation: "reachable_count".to_string(),
                args: vec![
                    Term::Variable("City".to_string()),
                    Term::Aggregate(AggregateFunc::Count, "Dest".to_string()),
                ],
            },
            body: vec![positive("can_reach", vec!["City", "Dest"])],
        }];

        let data = base_data(vec![]);
        let derived = {
            let mut m = HashMap::new();
            m.insert(
                "can_reach".to_string(),
                vec![
                    Tuple::new(vec![Value::string("berlin"), Value::string("dubai")]),
                    Tuple::new(vec![Value::string("berlin"), Value::string("tokyo")]),
                    Tuple::new(vec![Value::string("berlin"), Value::string("sydney")]),
                ],
            );
            m
        };

        let ctx = ProofContext::new(&rules, &data, default_config()).with_derived_data(&derived);

        let graph = build_proof_tree(
            "reachable_count",
            &tuple(vec![Value::string("berlin"), Value::Int64(3)]),
            &ctx,
        )
        .expect("should find derivation for aggregate");

        let root = &graph.nodes[&graph.roots[0]];
        assert_eq!(root.kind, NodeKind::Aggregate);
        assert_eq!(root.conclusion.pred, "reachable_count");

        let agg = root.aggregate.as_ref().expect("should have aggregate info");
        assert_eq!(agg.func, "count");
        assert_eq!(agg.value_var, "Dest");
        assert_eq!(agg.contributing_count, 3);
        // Children are the contributing can_reach derivations
        assert_eq!(root.children.len(), 3);
    }

    #[test]
    fn test_aggregation_never_truncated() {
        use crate::ast::AggregateFunc;

        let rules = vec![Rule {
            head: Atom {
                relation: "cnt".to_string(),
                args: vec![
                    Term::Variable("X".to_string()),
                    Term::Aggregate(AggregateFunc::Count, "Y".to_string()),
                ],
            },
            body: vec![positive("src", vec!["X", "Y"])],
        }];

        let data = base_data(vec![(
            "src",
            vec![vec![int(1), int(10)], vec![int(1), int(20)]],
        )]);
        let ctx = ProofContext::new(&rules, &data, default_config());

        let graph = build_proof_tree("cnt", &tuple(vec![int(1), Value::Int64(2)]), &ctx)
            .expect("aggregate should not fail");
        assert!(!graph.has_truncated());
        assert_eq!(graph.nodes[&graph.roots[0]].kind, NodeKind::Aggregate);
    }

    #[test]
    fn test_aggregation_export_mode() {
        use crate::ast::AggregateFunc;

        let rules = vec![Rule {
            head: Atom {
                relation: "cnt".to_string(),
                args: vec![
                    Term::Variable("G".to_string()),
                    Term::Aggregate(AggregateFunc::Count, "V".to_string()),
                ],
            },
            body: vec![positive("data", vec!["G", "V"])],
        }];

        let rows: Vec<Vec<Value>> = (0..25).map(|i| vec![int(1), int(i)]).collect();
        let data = base_data(vec![("data", rows)]);

        let mut config = default_config();
        config.full_mode = true;
        config.aggregation_sample_size = 10;
        let ctx = ProofContext::new(&rules, &data, config);

        let graph = build_proof_tree("cnt", &tuple(vec![int(1), Value::Int64(25)]), &ctx)
            .expect("should produce graph");
        let root = &graph.nodes[&graph.roots[0]];
        let agg = root.aggregate.as_ref().unwrap();
        assert_eq!(agg.contributing_count, 25);
        assert_eq!(agg.sample_inputs.as_ref().unwrap().len(), 10);
        let full = agg
            .full_inputs
            .as_ref()
            .expect("export mode must have full_inputs");
        assert_eq!(full.len(), 25);
    }

    #[test]
    fn test_aggregation_gui_mode_trims() {
        use crate::ast::AggregateFunc;

        let rules = vec![Rule {
            head: Atom {
                relation: "cnt".to_string(),
                args: vec![
                    Term::Variable("G".to_string()),
                    Term::Aggregate(AggregateFunc::Count, "V".to_string()),
                ],
            },
            body: vec![positive("data", vec!["G", "V"])],
        }];

        let rows: Vec<Vec<Value>> = (0..25).map(|i| vec![int(1), int(i)]).collect();
        let data = base_data(vec![("data", rows)]);

        let mut config = default_config();
        config.full_mode = false;
        config.aggregation_sample_size = 10;
        let ctx = ProofContext::new(&rules, &data, config);

        let graph = build_proof_tree("cnt", &tuple(vec![int(1), Value::Int64(25)]), &ctx)
            .expect("should produce graph");
        let root = &graph.nodes[&graph.roots[0]];
        let agg = root.aggregate.as_ref().unwrap();
        assert_eq!(agg.contributing_count, 25);
        assert_eq!(agg.sample_inputs.as_ref().unwrap().len(), 10);
        assert!(
            agg.full_inputs.is_none(),
            "GUI mode should not have full_inputs"
        );
    }

    #[test]
    fn test_comparison_rule() {
        let rules = vec![Rule {
            head: Atom {
                relation: "big".to_string(),
                args: vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("S".to_string()),
                ],
            },
            body: vec![
                positive("item", vec!["X", "S"]),
                BodyPredicate::Comparison(
                    Term::Variable("S".to_string()),
                    crate::ast::ComparisonOp::GreaterThan,
                    Term::Constant(100),
                ),
            ],
        }];
        let data = base_data(vec![(
            "item",
            vec![vec![int(1), int(200)], vec![int(2), int(50)]],
        )]);
        let ctx = ProofContext::new(&rules, &data, default_config());

        let graph = build_proof_tree("big", &tuple(vec![int(1), int(200)]), &ctx)
            .expect("should find derivation");
        assert!(!graph.nodes.is_empty());

        let result = build_proof_tree("big", &tuple(vec![int(2), int(50)]), &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_multi_level_rules() {
        let rules = vec![
            simple_rule("level1", vec!["X"], vec![positive("base", vec!["X"])]),
            simple_rule("level2", vec!["X"], vec![positive("level1", vec!["X"])]),
        ];
        let data = base_data(vec![("base", vec![vec![int(1)]])]);
        let ctx = ProofContext::new(&rules, &data, default_config());

        let graph =
            build_proof_tree("level2", &tuple(vec![int(1)]), &ctx).expect("should find derivation");
        assert!(graph.max_depth() >= 3); // level2 -> level1 -> base
    }

    #[test]
    fn test_with_derived_data() {
        let rules = vec![
            simple_rule(
                "can_reach",
                vec!["A", "B"],
                vec![positive("flight", vec!["A", "B"])],
            ),
            simple_rule(
                "can_reach",
                vec!["A", "C"],
                vec![
                    positive("flight", vec!["A", "B"]),
                    positive("can_reach", vec!["B", "C"]),
                ],
            ),
        ];
        let data = base_data(vec![(
            "flight",
            vec![
                vec![int(1), int(2)],
                vec![int(2), int(3)],
                vec![int(3), int(4)],
                vec![int(4), int(5)],
            ],
        )]);

        let derived = {
            let mut m = HashMap::new();
            m.insert(
                "can_reach".to_string(),
                vec![
                    Tuple::new(vec![int(1), int(2)]),
                    Tuple::new(vec![int(1), int(3)]),
                    Tuple::new(vec![int(1), int(4)]),
                    Tuple::new(vec![int(1), int(5)]),
                    Tuple::new(vec![int(2), int(3)]),
                    Tuple::new(vec![int(2), int(4)]),
                    Tuple::new(vec![int(2), int(5)]),
                    Tuple::new(vec![int(3), int(4)]),
                    Tuple::new(vec![int(3), int(5)]),
                    Tuple::new(vec![int(4), int(5)]),
                ],
            );
            m
        };

        let ctx = ProofContext::new(&rules, &data, default_config()).with_derived_data(&derived);

        let graph = build_proof_tree("can_reach", &tuple(vec![int(1), int(5)]), &ctx)
            .expect("should find derivation");
        assert!(!graph.has_truncated());
        assert!(graph.max_depth() <= 10);
    }

    #[test]
    fn test_json_export_shape() {
        let data = base_data(vec![("edge", vec![vec![int(1), int(2)]])]);
        let rules = vec![simple_rule(
            "path",
            vec!["X", "Y"],
            vec![positive("edge", vec!["X", "Y"])],
        )];
        let ctx = ProofContext::new(&rules, &data, default_config());

        let graph = build_proof_tree("path", &tuple(vec![int(1), int(2)]), &ctx)
            .expect("should find derivation");

        let json = graph.to_json().expect("should serialize");
        assert_eq!(json["version"], 1);
        assert!(json["roots"].is_array());
        assert!(json["nodes"].is_object());

        let root_id = json["roots"][0].as_str().unwrap();
        let root = &json["nodes"][root_id];
        assert_eq!(root["kind"], "rule");
        assert_eq!(root["conclusion"]["pred"], "path");
        assert!(root["conclusion"]["args"].is_array());
        assert!(root["children"].is_array());
    }
}
