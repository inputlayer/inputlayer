//! Backward chaining proof tree construction.
//!
//! Given a derived tuple, traces backward through rules and base facts
//! to build a proof tree explaining why the tuple was derived.

use crate::ast::Rule;
use crate::provenance::unification::unify_head;
use crate::provenance::{ProofConfig, ProofTree};
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
            derived_relations,
            config,
            index_info,
        }
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

/// Build proof trees for a specific result tuple.
///
/// Returns up to `config.max_proofs_per_tuple` distinct proof trees.
pub fn build_proofs(
    relation: &str,
    tuple: &Tuple,
    ctx: &ProofContext<'_>,
) -> Result<Vec<ProofTree>, String> {
    let mut visited = HashSet::new();
    let mut proofs = Vec::new();

    build_proofs_inner(relation, tuple, ctx, &mut visited, 0, &mut proofs)?;

    if proofs.is_empty() {
        Err(format!(
            "No proof found for {relation}({})",
            tuple_display(tuple)
        ))
    } else {
        Ok(proofs)
    }
}

pub fn build_proofs_inner(
    relation: &str,
    tuple: &Tuple,
    ctx: &ProofContext<'_>,
    visited: &mut HashSet<(String, Vec<Value>)>,
    depth: usize,
    proofs: &mut Vec<ProofTree>,
) -> Result<(), String> {
    // Depth limit
    if depth >= ctx.config.max_depth {
        proofs.push(ProofTree::Truncated {
            depth_limit: ctx.config.max_depth,
        });
        return Ok(());
    }

    // Cycle detection
    let key = (relation.to_string(), tuple_values(tuple));
    if visited.contains(&key) {
        return Ok(()); // Already being proved - skip to avoid infinite loop
    }
    visited.insert(key.clone());

    // Base relation: check if tuple exists as a fact
    if !ctx.is_derived(relation) {
        if tuple_exists_in(relation, tuple, ctx.base_data) {
            proofs.push(ProofTree::BaseFact {
                relation: relation.to_string(),
                values: tuple_values(tuple),
            });
        }
        visited.remove(&key);
        return Ok(());
    }

    // Also check if it exists as a base fact (relation can be both base and derived)
    if tuple_exists_in(relation, tuple, ctx.base_data) {
        proofs.push(ProofTree::BaseFact {
            relation: relation.to_string(),
            values: tuple_values(tuple),
        });
        if proofs.len() >= ctx.config.max_proofs_per_tuple {
            visited.remove(&key);
            return Ok(());
        }
    }

    // Try each rule clause
    let rules = ctx.rules_for(relation);
    for (clause_idx, rule) in rules.iter().enumerate() {
        if proofs.len() >= ctx.config.max_proofs_per_tuple {
            break;
        }

        // Try to unify the target tuple with the rule head
        let bindings = match unify_head(tuple, &rule.head) {
            Some(b) => b,
            None => continue,
        };

        // Try to satisfy all body predicates with these bindings
        match super::prove_body::prove_body(&rule.body, bindings, ctx, visited, depth + 1) {
            Ok(body_results) => {
                for (final_bindings, children) in body_results {
                    if proofs.len() >= ctx.config.max_proofs_per_tuple {
                        break;
                    }
                    let mut binding_pairs: Vec<(String, Value)> =
                        final_bindings.into_iter().collect();
                    binding_pairs.sort_by(|(a, _), (b, _)| a.cmp(b));
                    proofs.push(ProofTree::RuleApplication {
                        rule_name: rule.head.relation.clone(),
                        clause_index: clause_idx,
                        clause_text: format!("{rule}"),
                        bindings: binding_pairs,
                        children,
                    });
                }
            }
            Err(_) => continue,
        }
    }

    visited.remove(&key);
    Ok(())
}

fn tuple_exists_in(relation: &str, tuple: &Tuple, base_data: &HashMap<String, Vec<Tuple>>) -> bool {
    base_data
        .get(relation)
        .is_some_and(|tuples| tuples.contains(tuple))
}

fn tuple_values(tuple: &Tuple) -> Vec<Value> {
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
    fn test_proof_base_fact() {
        let data = base_data(vec![("edge", vec![vec![int(1), int(2)]])]);
        let ctx = ProofContext::new(&[], &data, default_config());
        let proofs =
            build_proofs("edge", &tuple(vec![int(1), int(2)]), &ctx).expect("should find proof");
        assert_eq!(proofs.len(), 1);
        assert!(matches!(proofs[0], ProofTree::BaseFact { .. }));
    }

    #[test]
    fn test_proof_base_fact_missing() {
        let data = base_data(vec![("edge", vec![vec![int(1), int(2)]])]);
        let ctx = ProofContext::new(&[], &data, default_config());
        let result = build_proofs("edge", &tuple(vec![int(99), int(99)]), &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_proof_single_rule_single_body() {
        // derived(X) <- base(X)
        let rules = vec![simple_rule(
            "derived",
            vec!["X"],
            vec![positive("base", vec!["X"])],
        )];
        let data = base_data(vec![("base", vec![vec![int(42)]])]);
        let ctx = ProofContext::new(&rules, &data, default_config());

        let proofs =
            build_proofs("derived", &tuple(vec![int(42)]), &ctx).expect("should find proof");
        assert_eq!(proofs.len(), 1);
        match &proofs[0] {
            ProofTree::RuleApplication { children, .. } => {
                assert_eq!(children.len(), 1);
                assert!(matches!(children[0], ProofTree::BaseFact { .. }));
            }
            other => panic!("Expected RuleApplication, got {other:?}"),
        }
    }

    #[test]
    fn test_proof_single_rule_join() {
        // path(X, Z) <- edge(X, Y), edge(Y, Z)
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

        let proofs =
            build_proofs("path", &tuple(vec![int(1), int(3)]), &ctx).expect("should find proof");
        assert_eq!(proofs.len(), 1);
        match &proofs[0] {
            ProofTree::RuleApplication { children, .. } => {
                assert_eq!(children.len(), 2);
            }
            other => panic!("Expected RuleApplication, got {other:?}"),
        }
    }

    #[test]
    fn test_proof_multi_clause() {
        // derived(X) <- a(X)     -- clause 0
        // derived(X) <- b(X)     -- clause 1
        let rules = vec![
            simple_rule("derived", vec!["X"], vec![positive("a", vec!["X"])]),
            simple_rule("derived", vec!["X"], vec![positive("b", vec!["X"])]),
        ];
        let data = base_data(vec![("a", vec![vec![int(1)]]), ("b", vec![vec![int(1)]])]);
        let mut config = default_config();
        config.max_proofs_per_tuple = 10;
        let ctx = ProofContext::new(&rules, &data, config);

        let proofs =
            build_proofs("derived", &tuple(vec![int(1)]), &ctx).expect("should find proofs");
        // Should find proofs from both clauses
        assert!(proofs.len() >= 2, "got {} proofs", proofs.len());
    }

    #[test]
    fn test_proof_recursive_transitive_closure() {
        // path(X, Y) <- edge(X, Y)
        // path(X, Z) <- edge(X, Y), path(Y, Z)
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
        let ctx = ProofContext::new(&rules, &data, default_config());

        // path(1, 4) requires 3 hops through edges
        let proofs =
            build_proofs("path", &tuple(vec![int(1), int(4)]), &ctx).expect("should find proof");
        assert!(!proofs.is_empty());
        assert!(proofs[0].depth() >= 3, "depth: {}", proofs[0].depth());
    }

    #[test]
    fn test_proof_recursive_cycle_detection() {
        // path(X, Y) <- edge(X, Y)
        // path(X, Z) <- edge(X, Y), path(Y, Z)
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
        // Cyclic data: 1->2->3->1
        let data = base_data(vec![(
            "edge",
            vec![
                vec![int(1), int(2)],
                vec![int(2), int(3)],
                vec![int(3), int(1)],
            ],
        )]);
        let ctx = ProofContext::new(&rules, &data, default_config());

        // path(1, 2) should work without infinite loop
        let proofs =
            build_proofs("path", &tuple(vec![int(1), int(2)]), &ctx).expect("should find proof");
        assert!(!proofs.is_empty());
    }

    #[test]
    fn test_proof_depth_limit_truncation() {
        // chain(X, Y) <- link(X, Y)
        // chain(X, Z) <- link(X, Y), chain(Y, Z)
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
        // Very long chain: 0->1->2->...->100
        let links: Vec<Vec<Value>> = (0..100).map(|i| vec![int(i), int(i + 1)]).collect();
        let data = base_data(vec![("link", links)]);

        let mut config = default_config();
        config.max_depth = 5;
        let ctx = ProofContext::new(&rules, &data, config);

        let proofs = build_proofs("chain", &tuple(vec![int(0), int(100)]), &ctx);
        // Should either find a truncated proof or no proof (depth exceeded)
        // The key is it doesn't infinite loop or panic
        if let Ok(ps) = &proofs {
            // If any proof was found, check for truncation
            let has_truncated = ps.iter().any(|p| contains_truncated(p));
            if !ps.is_empty() {
                assert!(has_truncated, "deep proof should contain truncation");
            }
        }
    }

    #[test]
    fn test_proof_with_negation() {
        // safe(X) <- node(X), !danger(X)
        let rules = vec![simple_rule(
            "safe",
            vec!["X"],
            vec![positive("node", vec!["X"]), negated("danger", vec!["X"])],
        )];
        let data = base_data(vec![
            ("node", vec![vec![int(1)], vec![int(2)], vec![int(3)]]),
            ("danger", vec![vec![int(2)]]),
        ]);
        let ctx = ProofContext::new(&rules, &data, default_config());

        // Node 1 is safe (not in danger)
        let proofs = build_proofs("safe", &tuple(vec![int(1)]), &ctx).expect("should find proof");
        assert!(!proofs.is_empty());
        match &proofs[0] {
            ProofTree::RuleApplication { children, .. } => {
                assert!(children
                    .iter()
                    .any(|c| matches!(c, ProofTree::NegationProof { .. })));
            }
            other => panic!("Expected RuleApplication, got {other:?}"),
        }

        // Node 2 is not safe (in danger)
        let result = build_proofs("safe", &tuple(vec![int(2)]), &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_proof_with_comparison() {
        // big(X, S) <- item(X, S), S > 100
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

        let proofs =
            build_proofs("big", &tuple(vec![int(1), int(200)]), &ctx).expect("should find proof");
        assert!(!proofs.is_empty());

        let result = build_proofs("big", &tuple(vec![int(2), int(50)]), &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_proof_multi_level_rules() {
        // level2(X) <- level1(X)
        // level1(X) <- base(X)
        let rules = vec![
            simple_rule("level1", vec!["X"], vec![positive("base", vec!["X"])]),
            simple_rule("level2", vec!["X"], vec![positive("level1", vec!["X"])]),
        ];
        let data = base_data(vec![("base", vec![vec![int(1)]])]);
        let ctx = ProofContext::new(&rules, &data, default_config());

        let proofs = build_proofs("level2", &tuple(vec![int(1)]), &ctx).expect("should find proof");
        assert!(!proofs.is_empty());
        assert!(proofs[0].depth() >= 3);
    }

    #[test]
    fn test_proof_empty_result() {
        let data = base_data(vec![("edge", vec![])]);
        let ctx = ProofContext::new(&[], &data, default_config());
        let result = build_proofs("edge", &tuple(vec![int(1), int(2)]), &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_proof_config_max_proofs() {
        // derived(X) <- a(X)
        // derived(X) <- b(X)
        // derived(X) <- c(X)
        let rules = vec![
            simple_rule("derived", vec!["X"], vec![positive("a", vec!["X"])]),
            simple_rule("derived", vec!["X"], vec![positive("b", vec!["X"])]),
            simple_rule("derived", vec!["X"], vec![positive("c", vec!["X"])]),
        ];
        let data = base_data(vec![
            ("a", vec![vec![int(1)]]),
            ("b", vec![vec![int(1)]]),
            ("c", vec![vec![int(1)]]),
        ]);

        let mut config = default_config();
        config.max_proofs_per_tuple = 2;
        let ctx = ProofContext::new(&rules, &data, config);

        let proofs =
            build_proofs("derived", &tuple(vec![int(1)]), &ctx).expect("should find proofs");
        assert_eq!(proofs.len(), 2);
    }

    #[test]
    fn test_candidate_cap_prevents_explosion() {
        use crate::provenance::prove_body::MAX_DERIVED_CANDIDATES;

        // Create 10 rules each producing 200 candidates from different base relations
        // Without cap, 2000 total candidates would be returned
        let mut rules = Vec::new();
        let mut data_entries = Vec::new();
        for i in 0..10 {
            let base_name = format!("base_{i}");
            let body = BodyPredicate::Positive(Atom {
                relation: base_name.clone(),
                args: vec![Term::Variable("X".to_string())],
            });
            rules.push(Rule {
                head: Atom {
                    relation: "derived".to_string(),
                    args: vec![Term::Variable("X".to_string())],
                },
                body: vec![body],
            });
            let tuples: Vec<Vec<Value>> = (0..200).map(|j| vec![int(i * 200 + j)]).collect();
            data_entries.push((base_name, tuples.into_iter().map(Tuple::new).collect()));
        }

        let base_data_map: HashMap<String, Vec<Tuple>> = data_entries.into_iter().collect();
        let config = default_config();
        let ctx = ProofContext::new(&rules, &base_data_map, config);

        // Try to enumerate derived candidates for "derived" relation
        let bound_terms = vec![crate::provenance::unification::BoundTerm::Unbound(
            "X".to_string(),
        )];
        let mut visited = HashSet::new();
        let candidates = crate::provenance::prove_body::enumerate_derived_candidates_pub(
            "derived",
            &bound_terms,
            &ctx,
            &mut visited,
            0,
        );

        // Should be capped at MAX_DERIVED_CANDIDATES
        assert!(
            candidates.len() <= MAX_DERIVED_CANDIDATES,
            "got {} candidates, expected <= {}",
            candidates.len(),
            MAX_DERIVED_CANDIDATES
        );
    }

    fn contains_truncated(tree: &ProofTree) -> bool {
        match tree {
            ProofTree::Truncated { .. } => true,
            ProofTree::RuleApplication { children, .. } => children.iter().any(contains_truncated),
            ProofTree::Recursive { inner, .. } => contains_truncated(inner),
            _ => false,
        }
    }
}
