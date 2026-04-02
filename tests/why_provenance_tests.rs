//! Integration tests for proof tree construction.

use inputlayer::provenance::backward_chaining::{build_proof_tree, ProofContext};
use inputlayer::provenance::proof_tree::NodeKind;
use inputlayer::provenance::ProofConfig;
use inputlayer::value::{Tuple, Value};
use std::collections::HashMap;

fn int(v: i32) -> Value {
    Value::Int32(v)
}

fn str_val(s: &str) -> Value {
    Value::string(s)
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

#[test]
fn test_proof_tree_base_fact() {
    let data = base_data(vec![(
        "edge",
        vec![vec![int(1), int(2)], vec![int(3), int(4)]],
    )]);
    let ctx = ProofContext::new(&[], &data, ProofConfig::default());

    let graph = build_proof_tree("edge", &tuple(vec![int(1), int(2)]), &ctx)
        .expect("should find derivation");
    assert_eq!(graph.roots.len(), 1);
    let root = &graph.nodes[&graph.roots[0]];
    assert_eq!(root.kind, NodeKind::Fact);
    assert_eq!(root.conclusion.pred, "edge");
    assert_eq!(root.conclusion.args, vec![int(1), int(2)]);
}

#[test]
fn test_proof_tree_derived_rule() {
    use inputlayer::ast::{Atom, BodyPredicate, Rule, Term};

    let rules = vec![Rule {
        head: Atom {
            relation: "active".to_string(),
            args: vec![Term::Variable("X".to_string())],
        },
        body: vec![BodyPredicate::Positive(Atom {
            relation: "node".to_string(),
            args: vec![Term::Variable("X".to_string())],
        })],
    }];
    let data = base_data(vec![("node", vec![vec![int(1)], vec![int(2)]])]);
    let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

    let graph =
        build_proof_tree("active", &tuple(vec![int(1)]), &ctx).expect("should find derivation");
    let root = &graph.nodes[&graph.roots[0]];
    assert_eq!(root.kind, NodeKind::Rule);
    assert_eq!(root.conclusion.pred, "active");
    assert_eq!(root.children.len(), 1);
    let child = &graph.nodes[&root.children[0]];
    assert_eq!(child.kind, NodeKind::Fact);
    assert_eq!(child.conclusion.pred, "node");
}

#[test]
fn test_proof_tree_negation() {
    use inputlayer::ast::{Atom, BodyPredicate, Rule, Term};

    let rules = vec![Rule {
        head: Atom {
            relation: "safe".to_string(),
            args: vec![Term::Variable("X".to_string())],
        },
        body: vec![
            BodyPredicate::Positive(Atom {
                relation: "node".to_string(),
                args: vec![Term::Variable("X".to_string())],
            }),
            BodyPredicate::Negated(Atom {
                relation: "danger".to_string(),
                args: vec![Term::Variable("X".to_string())],
            }),
        ],
    }];
    let data = base_data(vec![
        ("node", vec![vec![int(1)], vec![int(2)]]),
        ("danger", vec![vec![int(2)]]),
    ]);
    let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

    let graph = build_proof_tree("safe", &tuple(vec![int(1)]), &ctx).expect("should find proof");
    let root = &graph.nodes[&graph.roots[0]];
    assert_eq!(root.kind, NodeKind::Rule);
    let has_negation = root
        .children
        .iter()
        .any(|id| graph.nodes[id].kind == NodeKind::Negation);
    assert!(has_negation);

    let result = build_proof_tree("safe", &tuple(vec![int(2)]), &ctx);
    assert!(result.is_err());
}

#[test]
fn test_proof_tree_json_export() {
    let data = base_data(vec![("edge", vec![vec![int(1), int(2)]])]);
    let ctx = ProofContext::new(&[], &data, ProofConfig::default());

    let graph = build_proof_tree("edge", &tuple(vec![int(1), int(2)]), &ctx)
        .expect("should find derivation");

    let json = graph.to_json().expect("should serialize");
    assert_eq!(json["version"], 1);
    assert!(json["roots"].is_array());
    assert!(json["nodes"].is_object());

    let root_id = json["roots"][0].as_str().unwrap();
    let root = &json["nodes"][root_id];
    assert_eq!(root["kind"], "fact");
    assert_eq!(root["conclusion"]["pred"], "edge");
}

#[test]
fn test_proof_tree_format_tree() {
    let data = base_data(vec![("edge", vec![vec![int(1), int(2)]])]);
    let ctx = ProofContext::new(&[], &data, ProofConfig::default());

    let graph = build_proof_tree("edge", &tuple(vec![int(1), int(2)]), &ctx)
        .expect("should find derivation");

    let formatted = graph.format_tree();
    assert!(formatted.contains("[base] edge(1, 2)"));
}

#[test]
fn test_proof_tree_depth_50_limit() {
    use inputlayer::ast::{Atom, BodyPredicate, Rule, Term};

    let links: Vec<Vec<Value>> = (0..60).map(|i| vec![int(i), int(i + 1)]).collect();
    let rules = vec![
        Rule {
            head: Atom {
                relation: "chain".to_string(),
                args: vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Y".to_string()),
                ],
            },
            body: vec![BodyPredicate::Positive(Atom {
                relation: "link".to_string(),
                args: vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Y".to_string()),
                ],
            })],
        },
        Rule {
            head: Atom {
                relation: "chain".to_string(),
                args: vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Z".to_string()),
                ],
            },
            body: vec![
                BodyPredicate::Positive(Atom {
                    relation: "link".to_string(),
                    args: vec![
                        Term::Variable("X".to_string()),
                        Term::Variable("Y".to_string()),
                    ],
                }),
                BodyPredicate::Positive(Atom {
                    relation: "chain".to_string(),
                    args: vec![
                        Term::Variable("Y".to_string()),
                        Term::Variable("Z".to_string()),
                    ],
                }),
            ],
        },
    ];
    let data = base_data(vec![("link", links)]);

    let mut config = ProofConfig::default();
    config.max_depth = 50;
    let ctx = ProofContext::new(&rules, &data, config);

    // Should not hang or panic
    let result = build_proof_tree("chain", &tuple(vec![int(0), int(60)]), &ctx);
    if let Ok(graph) = result {
        assert!(graph.max_depth() <= 55);
    }
}

/// End-to-end test through StorageEngine (same path as the GUI's .why handler).
#[test]
fn test_proof_tree_flights_no_truncation() {
    use inputlayer::ast::{Atom, BodyPredicate, Rule, Term};
    use inputlayer::statement::{RuleDef, SerializableRule};
    use inputlayer::storage_engine::StorageEngine;
    use inputlayer::Config;

    let tmp = tempfile::tempdir().expect("temp dir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    let storage = StorageEngine::new(config).expect("storage");
    storage
        .create_knowledge_graph("flights_test")
        .expect("create kg");

    let flights: Vec<Tuple> = vec![
        ("ny", "lon", "sa", 7.0),
        ("lon", "par", "ea", 1.5),
        ("par", "tok", "ea", 12.0),
        ("tok", "syd", "pw", 9.5),
        ("lon", "dub", "sa", 7.0),
        ("dub", "sin", "pw", 7.5),
        ("sin", "tok", "pw", 6.5),
        ("sin", "syd", "pw", 8.0),
        ("par", "ber", "ea", 1.5),
        ("ny", "sao", "sj", 10.0),
        ("dub", "cpt", "sj", 9.5),
        ("ber", "dub", "ea", 5.5),
    ]
    .into_iter()
    .map(|(a, b, c, d)| Tuple::new(vec![str_val(a), str_val(b), str_val(c), Value::Float64(d)]))
    .collect();

    storage
        .insert_tuples_into("flights_test", "direct_flight", flights)
        .expect("insert flights");

    let rule1 = Rule::new(
        Atom::new(
            "can_reach".into(),
            vec![Term::Variable("A".into()), Term::Variable("B".into())],
        ),
        vec![BodyPredicate::Positive(Atom::new(
            "direct_flight".into(),
            vec![
                Term::Variable("A".into()),
                Term::Variable("B".into()),
                Term::Placeholder,
                Term::Placeholder,
            ],
        ))],
    );
    storage
        .register_rule_in(
            "flights_test",
            &RuleDef {
                name: "can_reach".into(),
                rule: SerializableRule::from_rule(&rule1),
            },
        )
        .expect("rule1");

    let rule2 = Rule::new(
        Atom::new(
            "can_reach".into(),
            vec![Term::Variable("A".into()), Term::Variable("C".into())],
        ),
        vec![
            BodyPredicate::Positive(Atom::new(
                "direct_flight".into(),
                vec![
                    Term::Variable("A".into()),
                    Term::Variable("B".into()),
                    Term::Placeholder,
                    Term::Placeholder,
                ],
            )),
            BodyPredicate::Positive(Atom::new(
                "can_reach".into(),
                vec![Term::Variable("B".into()), Term::Variable("C".into())],
            )),
        ],
    );
    storage
        .register_rule_in(
            "flights_test",
            &RuleDef {
                name: "can_reach".into(),
                rule: SerializableRule::from_rule(&rule2),
            },
        )
        .expect("rule2");

    let query = r#"can_reach(A, B) <- can_reach(A, B)"#;
    let (result_tuples, rules, base_data, derived_data, _metrics) = storage
        .execute_and_get_context("flights_test", query)
        .expect("execute_and_get_context failed");

    assert!(!result_tuples.is_empty());

    let ctx = ProofContext::new(&rules, &base_data, ProofConfig::default())
        .with_derived_data(&derived_data);

    let mut truncated_count = 0;
    let mut total_graphs = 0;
    for t in &result_tuples {
        match build_proof_tree("can_reach", t, &ctx) {
            Ok(graph) => {
                total_graphs += 1;
                if graph.has_truncated() {
                    truncated_count += 1;
                }
            }
            Err(e) => {
                let dest = t.get(1).map(|v| format!("{v}")).unwrap_or_default();
                panic!("failed for can_reach(_, {dest}): {e}");
            }
        }
    }

    assert!(total_graphs > 0);
    assert_eq!(
        truncated_count, 0,
        "no proof trees should be truncated (got {truncated_count}/{total_graphs})"
    );
}

/// End-to-end test for aggregation rules.
#[test]
fn test_proof_tree_aggregation() {
    use inputlayer::ast::{AggregateFunc, Atom, BodyPredicate, Rule, Term};
    use inputlayer::statement::{RuleDef, SerializableRule};
    use inputlayer::storage_engine::StorageEngine;
    use inputlayer::Config;

    let tmp = tempfile::tempdir().expect("temp dir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    let storage = StorageEngine::new(config).expect("storage");
    storage
        .create_knowledge_graph("agg_test")
        .expect("create kg");

    let flights: Vec<Tuple> = vec![
        ("ny", "lon", "sa", 7.0),
        ("lon", "par", "ea", 1.5),
        ("par", "tok", "ea", 12.0),
        ("tok", "syd", "pw", 9.5),
        ("lon", "dub", "sa", 7.0),
        ("dub", "sin", "pw", 7.5),
        ("par", "ber", "ea", 1.5),
        ("ber", "dub", "ea", 5.5),
    ]
    .into_iter()
    .map(|(a, b, c, d)| Tuple::new(vec![str_val(a), str_val(b), str_val(c), Value::Float64(d)]))
    .collect();

    storage
        .insert_tuples_into("agg_test", "direct_flight", flights)
        .expect("insert");

    let rule1 = Rule::new(
        Atom::new(
            "can_reach".into(),
            vec![Term::Variable("A".into()), Term::Variable("B".into())],
        ),
        vec![BodyPredicate::Positive(Atom::new(
            "direct_flight".into(),
            vec![
                Term::Variable("A".into()),
                Term::Variable("B".into()),
                Term::Placeholder,
                Term::Placeholder,
            ],
        ))],
    );
    storage
        .register_rule_in(
            "agg_test",
            &RuleDef {
                name: "can_reach".into(),
                rule: SerializableRule::from_rule(&rule1),
            },
        )
        .expect("rule1");

    let rule2 = Rule::new(
        Atom::new(
            "can_reach".into(),
            vec![Term::Variable("A".into()), Term::Variable("C".into())],
        ),
        vec![
            BodyPredicate::Positive(Atom::new(
                "direct_flight".into(),
                vec![
                    Term::Variable("A".into()),
                    Term::Variable("B".into()),
                    Term::Placeholder,
                    Term::Placeholder,
                ],
            )),
            BodyPredicate::Positive(Atom::new(
                "can_reach".into(),
                vec![Term::Variable("B".into()), Term::Variable("C".into())],
            )),
        ],
    );
    storage
        .register_rule_in(
            "agg_test",
            &RuleDef {
                name: "can_reach".into(),
                rule: SerializableRule::from_rule(&rule2),
            },
        )
        .expect("rule2");

    let agg_rule = Rule::new(
        Atom::new(
            "reachable_count".into(),
            vec![
                Term::Variable("City".into()),
                Term::Aggregate(AggregateFunc::Count, "Dest".into()),
            ],
        ),
        vec![BodyPredicate::Positive(Atom::new(
            "can_reach".into(),
            vec![Term::Variable("City".into()), Term::Variable("Dest".into())],
        ))],
    );
    storage
        .register_rule_in(
            "agg_test",
            &RuleDef {
                name: "reachable_count".into(),
                rule: SerializableRule::from_rule(&agg_rule),
            },
        )
        .expect("agg rule");

    let query = "reachable_count(City, N) <- reachable_count(City, N)";
    let (result_tuples, rules, base_data, derived_data, _metrics) = storage
        .execute_and_get_context("agg_test", query)
        .expect("execute_and_get_context failed");

    assert!(!result_tuples.is_empty());

    let ctx = ProofContext::new(&rules, &base_data, ProofConfig::default())
        .with_derived_data(&derived_data);

    let mut truncated_count = 0;
    let mut aggregate_count = 0;
    let mut errors = 0;

    for t in &result_tuples {
        match build_proof_tree("reachable_count", t, &ctx) {
            Ok(graph) => {
                if graph.has_truncated() {
                    truncated_count += 1;
                }
                let root = &graph.nodes[&graph.roots[0]];
                if root.kind == NodeKind::Aggregate {
                    aggregate_count += 1;
                    // Aggregate should have children (contributing derivations)
                    assert!(!root.children.is_empty(), "aggregate should have children");
                    // Aggregate should have metadata
                    assert!(root.aggregate.is_some(), "aggregate should have info");
                }
            }
            Err(e) => {
                errors += 1;
                eprintln!("error: {e}");
            }
        }
    }

    assert_eq!(errors, 0, "no errors");
    assert_eq!(truncated_count, 0, "aggregation must NEVER be truncated");
    assert!(aggregate_count > 0, "should have aggregate nodes");
}

/// Verify proof trees built via the handler path (same as GUI).
/// Uses the actual Handler::query_program method with .why command.
#[tokio::test]
async fn test_proof_tree_via_handler() {
    use inputlayer::protocol::Handler;
    use inputlayer::provenance::proof_tree::NodeKind;
    use inputlayer::Config;

    let tmp = tempfile::tempdir().expect("temp dir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    config.storage.auto_create_knowledge_graphs = true;
    let handler = Handler::from_config(config).expect("handler");

    // Insert flights data with FULL city names (matching the demo)
    handler
        .query_program(
            None,
            r#"+direct_flight("new_york", "london", "sky_atlantic", 7.0)"#.into(),
        )
        .await
        .expect("insert");
    handler
        .query_program(
            None,
            r#"+direct_flight("london", "paris", "euro_air", 1.5)"#.into(),
        )
        .await
        .expect("insert");
    handler
        .query_program(
            None,
            r#"+direct_flight("paris", "tokyo", "euro_air", 12.0)"#.into(),
        )
        .await
        .expect("insert");
    handler
        .query_program(
            None,
            r#"+direct_flight("london", "dubai", "sky_atlantic", 7.0)"#.into(),
        )
        .await
        .expect("insert");
    handler
        .query_program(
            None,
            r#"+direct_flight("new_york", "sao_paulo", "southern_jet", 10.0)"#.into(),
        )
        .await
        .expect("insert");
    handler
        .query_program(
            None,
            r#"+can_reach(A, B) <- direct_flight(A, B, _, _)"#.into(),
        )
        .await
        .expect("rule1");
    handler
        .query_program(
            None,
            r#"+can_reach(A, C) <- direct_flight(A, B, _, _), can_reach(B, C)"#.into(),
        )
        .await
        .expect("rule2");

    // Run .why query - EXACTLY what the GUI does
    let result = handler
        .query_program(None, r#".why ?can_reach("new_york", X)"#.into())
        .await
        .expect("why query");

    let graphs = result.proof_trees.expect("should have proof_trees");
    assert!(!graphs.is_empty(), "should have graphs");

    let mut errors: Vec<String> = Vec::new();

    for (i, graph) in graphs.iter().enumerate() {
        for (node_id, node) in &graph.nodes {
            let pred = &node.conclusion.pred;
            let arity = node.conclusion.args.len();

            // can_reach must ALWAYS have exactly 2 args
            if pred == "can_reach" && arity != 2 {
                errors.push(format!(
                    "graph {i}, node {node_id}: can_reach has {arity} args (expected 2): {:?}",
                    node.conclusion.args
                ));
            }

            // direct_flight must ALWAYS have exactly 4 args
            if pred == "direct_flight" && arity != 4 {
                errors.push(format!(
                    "graph {i}, node {node_id}: direct_flight has {arity} args (expected 4): {:?}",
                    node.conclusion.args
                ));
            }

            // Rule nodes must have children and rule_id
            if node.kind == NodeKind::Rule {
                if node.children.is_empty() {
                    errors.push(format!("graph {i}, node {node_id}: rule has no children"));
                }
                if node.rule_id.is_none() {
                    errors.push(format!("graph {i}, node {node_id}: rule has no rule_id"));
                }
            }

            // No can_reach node should be a bare fact - it's derived
            if pred == "can_reach" && node.kind == NodeKind::Fact {
                errors.push(format!(
                    "graph {i}, node {node_id}: can_reach should be Rule (not bare Fact)"
                ));
            }

            // No truncated nodes
            if node.kind == NodeKind::Truncated {
                errors.push(format!("graph {i}, node {node_id}: unexpected truncated"));
            }

            // Aggregate sample_inputs must have correct arity (not wider than body atom)
            if node.kind == NodeKind::Aggregate {
                if let Some(ref agg) = node.aggregate {
                    if let Some(ref samples) = agg.sample_inputs {
                        for (si, sample) in samples.iter().enumerate() {
                            // can_reach has 2 args, direct_flight has 4
                            // sample_inputs for reachable_count should be can_reach tuples (2 args)
                            if sample.len() > 2 {
                                errors.push(format!(
                                    "graph {i}, node {node_id}: sample_inputs[{si}] has {} args (expected <=2): {:?}",
                                    sample.len(), sample
                                ));
                            }
                        }
                    }
                }
            }

            // All children must exist
            for child in &node.children {
                if !graph.nodes.contains_key(child) {
                    errors.push(format!("graph {i}, node {node_id}: dangling child {child}"));
                }
            }
        }
    }

    if !errors.is_empty() {
        panic!(
            "Proof tree errors ({}):\n  {}",
            errors.len(),
            errors.join("\n  ")
        );
    }
}

/// Verify EVERY node in every proof tree has correct arity and structure.
/// This catches the bug where can_reach nodes get direct_flight's 4 args instead of 2.
#[test]
fn test_proof_tree_node_correctness() {
    use inputlayer::ast::{Atom, BodyPredicate, Rule, Term};
    use inputlayer::provenance::proof_tree::NodeKind;
    use inputlayer::statement::{RuleDef, SerializableRule};
    use inputlayer::storage_engine::StorageEngine;
    use inputlayer::Config;

    let tmp = tempfile::tempdir().expect("temp dir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    let storage = StorageEngine::new(config).expect("storage");
    storage
        .create_knowledge_graph("verify_test")
        .expect("create kg");

    let flights: Vec<Tuple> = vec![
        ("ny", "lon", "sa", 7.0),
        ("lon", "par", "ea", 1.5),
        ("par", "tok", "ea", 12.0),
        ("tok", "syd", "pw", 9.5),
        ("lon", "dub", "sa", 7.0),
        ("dub", "sin", "pw", 7.5),
        ("sin", "tok", "pw", 6.5),
        ("sin", "syd", "pw", 8.0),
        ("par", "ber", "ea", 1.5),
        ("ny", "sao", "sj", 10.0),
        ("dub", "cpt", "sj", 9.5),
        ("ber", "dub", "ea", 5.5),
    ]
    .into_iter()
    .map(|(a, b, c, d)| Tuple::new(vec![str_val(a), str_val(b), str_val(c), Value::Float64(d)]))
    .collect();
    storage
        .insert_tuples_into("verify_test", "direct_flight", flights)
        .expect("insert");

    let rule1 = Rule::new(
        Atom::new(
            "can_reach".into(),
            vec![Term::Variable("A".into()), Term::Variable("B".into())],
        ),
        vec![BodyPredicate::Positive(Atom::new(
            "direct_flight".into(),
            vec![
                Term::Variable("A".into()),
                Term::Variable("B".into()),
                Term::Placeholder,
                Term::Placeholder,
            ],
        ))],
    );
    storage
        .register_rule_in(
            "verify_test",
            &RuleDef {
                name: "can_reach".into(),
                rule: SerializableRule::from_rule(&rule1),
            },
        )
        .expect("rule1");

    let rule2 = Rule::new(
        Atom::new(
            "can_reach".into(),
            vec![Term::Variable("A".into()), Term::Variable("C".into())],
        ),
        vec![
            BodyPredicate::Positive(Atom::new(
                "direct_flight".into(),
                vec![
                    Term::Variable("A".into()),
                    Term::Variable("B".into()),
                    Term::Placeholder,
                    Term::Placeholder,
                ],
            )),
            BodyPredicate::Positive(Atom::new(
                "can_reach".into(),
                vec![Term::Variable("B".into()), Term::Variable("C".into())],
            )),
        ],
    );
    storage
        .register_rule_in(
            "verify_test",
            &RuleDef {
                name: "can_reach".into(),
                rule: SerializableRule::from_rule(&rule2),
            },
        )
        .expect("rule2");

    let query = r#"can_reach(A, B) <- can_reach(A, B)"#;
    let (result_tuples, rules, base_data, derived_data, _) = storage
        .execute_and_get_context("verify_test", query)
        .expect("execute failed");

    // First: verify all result tuples are 2-arity
    for t in &result_tuples {
        assert_eq!(
            t.arity(),
            2,
            "can_reach result tuple should be 2-arity, got {} for {:?}",
            t.arity(),
            (0..t.arity()).filter_map(|i| t.get(i)).collect::<Vec<_>>()
        );
    }

    let ctx = ProofContext::new(&rules, &base_data, ProofConfig::default())
        .with_derived_data(&derived_data);

    let mut errors: Vec<String> = Vec::new();

    for t in &result_tuples {
        let src = t.get(0).map(|v| format!("{v}")).unwrap_or_default();
        let dst = t.get(1).map(|v| format!("{v}")).unwrap_or_default();
        let label = format!("can_reach({src}, {dst})");

        let graph = match build_proof_tree("can_reach", t, &ctx) {
            Ok(g) => g,
            Err(e) => {
                errors.push(format!("{label}: build failed: {e}"));
                continue;
            }
        };

        // Verify every node in the graph
        for (node_id, node) in &graph.nodes {
            let pred = &node.conclusion.pred;
            let arity = node.conclusion.args.len();

            match node.kind {
                NodeKind::Fact => {
                    // Fact nodes: arity must match the relation's actual arity
                    if pred == "direct_flight" {
                        if arity != 4 {
                            errors.push(format!(
                                "{label}: node {node_id}: direct_flight fact should have 4 args, got {arity}"
                            ));
                        }
                    } else if pred == "can_reach" {
                        if arity != 2 {
                            errors.push(format!(
                                "{label}: node {node_id}: can_reach fact should have 2 args, got {arity}: {:?}",
                                node.conclusion.args
                            ));
                        }
                    }
                }
                NodeKind::Rule => {
                    // Rule nodes: conclusion must match the head relation's arity
                    if pred == "can_reach" && arity != 2 {
                        errors.push(format!(
                            "{label}: node {node_id}: can_reach rule node should have 2 args, got {arity}: {:?}",
                            node.conclusion.args
                        ));
                    }
                    // Rule nodes must have children
                    if node.children.is_empty() {
                        errors.push(format!(
                            "{label}: node {node_id}: rule node has no children"
                        ));
                    }
                    // Rule nodes must have a rule_id
                    if node.rule_id.is_none() {
                        errors.push(format!("{label}: node {node_id}: rule node has no rule_id"));
                    }
                }
                NodeKind::Truncated => {
                    errors.push(format!(
                        "{label}: node {node_id}: unexpected truncated node"
                    ));
                }
                _ => {}
            }

            // All children must reference existing nodes
            for child_id in &node.children {
                if !graph.nodes.contains_key(child_id) {
                    errors.push(format!(
                        "{label}: node {node_id}: child {child_id} does not exist in graph"
                    ));
                }
            }
        }

        // Root must exist
        for root_id in &graph.roots {
            if !graph.nodes.contains_key(root_id) {
                errors.push(format!("{label}: root {root_id} does not exist in graph"));
            }
        }

        // Root conclusion must match the queried tuple
        if let Some(root) = graph.roots.first().and_then(|id| graph.nodes.get(id)) {
            if root.conclusion.pred != "can_reach" {
                errors.push(format!(
                    "{label}: root pred is '{}', expected 'can_reach'",
                    root.conclusion.pred
                ));
            }
            if root.conclusion.args.len() != 2 {
                errors.push(format!(
                    "{label}: root has {} args, expected 2: {:?}",
                    root.conclusion.args.len(),
                    root.conclusion.args
                ));
            }
        }
    }

    if !errors.is_empty() {
        panic!(
            "Proof tree correctness errors ({}):\n  {}",
            errors.len(),
            errors.join("\n  ")
        );
    }
}

/// Test aggregation via the handler path - verifies sample_inputs arity
/// and that derived fact nodes don't leak wider tuples.
#[tokio::test]
async fn test_proof_tree_aggregation_via_handler() {
    use inputlayer::protocol::Handler;
    use inputlayer::provenance::proof_tree::NodeKind;
    use inputlayer::Config;

    let tmp = tempfile::tempdir().expect("temp dir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    config.storage.auto_create_knowledge_graphs = true;
    let handler = Handler::from_config(config).expect("handler");

    handler
        .query_program(None, r#"+direct_flight("ny", "lon", "sa", 7.0)"#.into())
        .await
        .unwrap();
    handler
        .query_program(None, r#"+direct_flight("lon", "par", "ea", 1.5)"#.into())
        .await
        .unwrap();
    handler
        .query_program(None, r#"+direct_flight("par", "tok", "ea", 12.0)"#.into())
        .await
        .unwrap();
    handler
        .query_program(None, r#"+direct_flight("lon", "dub", "sa", 7.0)"#.into())
        .await
        .unwrap();
    handler
        .query_program(
            None,
            r#"+can_reach(A, B) <- direct_flight(A, B, _, _)"#.into(),
        )
        .await
        .unwrap();
    handler
        .query_program(
            None,
            r#"+can_reach(A, C) <- direct_flight(A, B, _, _), can_reach(B, C)"#.into(),
        )
        .await
        .unwrap();
    handler
        .query_program(
            None,
            r#"+reachable_count(City, count<Dest>) <- can_reach(City, Dest)"#.into(),
        )
        .await
        .unwrap();

    let result = handler
        .query_program(None, r#".why ?reachable_count(City, N)"#.into())
        .await
        .unwrap();
    let graphs = result.proof_trees.expect("should have graphs");

    let mut errors: Vec<String> = Vec::new();

    for (i, graph) in graphs.iter().enumerate() {
        for (node_id, node) in &graph.nodes {
            let pred = &node.conclusion.pred;
            let arity = node.conclusion.args.len();

            // can_reach must have 2 args
            if pred == "can_reach" && arity != 2 {
                errors.push(format!(
                    "graph {i}, node {node_id}: can_reach has {arity} args: {:?}",
                    node.conclusion.args
                ));
            }

            // direct_flight must have 4 args
            if pred == "direct_flight" && arity != 4 {
                errors.push(format!(
                    "graph {i}, node {node_id}: direct_flight has {arity} args: {:?}",
                    node.conclusion.args
                ));
            }

            // reachable_count must have 2 args
            if pred == "reachable_count" && arity != 2 {
                errors.push(format!(
                    "graph {i}, node {node_id}: reachable_count has {arity} args: {:?}",
                    node.conclusion.args
                ));
            }

            // Aggregate sample_inputs must have correct arity
            if node.kind == NodeKind::Aggregate {
                if let Some(ref agg) = node.aggregate {
                    if let Some(ref samples) = agg.sample_inputs {
                        for (si, sample) in samples.iter().enumerate() {
                            if sample.len() != 2 {
                                errors.push(format!(
                                    "graph {i}, node {node_id}: sample_inputs[{si}] has {} args (expected 2): {:?}",
                                    sample.len(), sample
                                ));
                            }
                        }
                    }
                }
            }

            // No truncated nodes
            if node.kind == NodeKind::Truncated {
                errors.push(format!("graph {i}, node {node_id}: unexpected truncated"));
            }
        }
    }

    if !errors.is_empty() {
        panic!(
            "Aggregation proof tree errors ({}):\n  {}",
            errors.len(),
            errors.join("\n  ")
        );
    }
}

/// Test that .agent start returns the first step and .agent next advances.
#[tokio::test]
async fn test_agent_scripted_steps() {
    use inputlayer::protocol::Handler;
    use inputlayer::Config;

    let tmp = tempfile::tempdir().expect("temp dir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    config.storage.auto_create_knowledge_graphs = true;
    let handler = Handler::from_config(config).expect("handler");

    // Start the flights lesson
    let result = handler
        .query_program(None, ".agent start flights".into())
        .await
        .expect("agent start should succeed");
    assert!(!result.rows.is_empty());
    let first_msg = format!("{}", result.rows[0].values[0]);
    assert!(
        first_msg.contains("direct_flight"),
        "first step should mention inserting a flight"
    );
    assert!(
        first_msg.contains("```iql"),
        "first step should have iql code block"
    );

    // Advance to next step
    let result = handler
        .query_program(None, ".agent next".into())
        .await
        .expect("agent next should succeed");
    assert!(!result.rows.is_empty());
    let second_msg = format!("{}", result.rows[0].values[0]);
    assert!(
        second_msg.contains("direct_flight"),
        "second step should add more flights"
    );
}
