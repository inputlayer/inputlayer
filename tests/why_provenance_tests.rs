//! Integration tests for why-provenance proof tree construction.

use inputlayer::provenance::backward_chaining::{build_proofs, ProofContext};
use inputlayer::provenance::{ProofConfig, ProofTree};
use inputlayer::value::{Tuple, Value};
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

/// Create a simple engine, add data, and build proof trees end-to-end.
#[test]
fn test_why_end_to_end_base_fact() {
    let data = base_data(vec![(
        "edge",
        vec![vec![int(1), int(2)], vec![int(3), int(4)]],
    )]);
    let ctx = ProofContext::new(&[], &data, ProofConfig::default());

    let proofs = build_proofs("edge", &tuple(vec![int(1), int(2)]), &ctx)
        .expect("should find proof for existing fact");
    assert_eq!(proofs.len(), 1);
    assert!(matches!(proofs[0], ProofTree::BaseFact { .. }));
}

#[test]
fn test_why_end_to_end_derived_rule() {
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

    let proofs = build_proofs("active", &tuple(vec![int(1)]), &ctx).expect("should find proof");
    assert_eq!(proofs.len(), 1);
    match &proofs[0] {
        ProofTree::RuleApplication {
            rule_name,
            children,
            ..
        } => {
            assert_eq!(rule_name, "active");
            assert_eq!(children.len(), 1);
            assert!(matches!(children[0], ProofTree::BaseFact { .. }));
        }
        other => panic!("Expected RuleApplication, got {other:?}"),
    }
}

#[test]
fn test_why_end_to_end_negation() {
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

    // Node 1 is safe
    let proofs = build_proofs("safe", &tuple(vec![int(1)]), &ctx).expect("should find proof");
    assert!(!proofs.is_empty());
    let has_negation = match &proofs[0] {
        ProofTree::RuleApplication { children, .. } => children
            .iter()
            .any(|c| matches!(c, ProofTree::NegationProof { .. })),
        _ => false,
    };
    assert!(has_negation, "proof should contain negation node");

    // Node 2 is not safe (danger exists)
    let result = build_proofs("safe", &tuple(vec![int(2)]), &ctx);
    assert!(result.is_err());
}

#[test]
fn test_why_proof_tree_json_export() {
    let data = base_data(vec![("edge", vec![vec![int(1), int(2)]])]);
    let ctx = ProofContext::new(&[], &data, ProofConfig::default());

    let proofs =
        build_proofs("edge", &tuple(vec![int(1), int(2)]), &ctx).expect("should find proof");

    // JSON export should work
    let json = proofs[0].to_json().expect("should serialize");
    assert_eq!(json["node_type"], "base_fact");
    assert_eq!(json["relation"], "edge");
}

#[test]
fn test_why_proof_tree_format() {
    let data = base_data(vec![("edge", vec![vec![int(1), int(2)]])]);
    let ctx = ProofContext::new(&[], &data, ProofConfig::default());

    let proofs =
        build_proofs("edge", &tuple(vec![int(1), int(2)]), &ctx).expect("should find proof");

    let formatted = proofs[0].format_tree();
    assert!(formatted.contains("[base] edge(1, 2)"));
}

#[test]
fn test_why_depth_50_limit() {
    use inputlayer::ast::{Atom, BodyPredicate, Rule, Term};

    // Create a chain: link(0,1), link(1,2), ..., link(59,60)
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

    // chain(0, 60) should either truncate or fail gracefully
    let result = build_proofs("chain", &tuple(vec![int(0), int(60)]), &ctx);
    // Key assertion: does not hang or panic
    // If proof found, should contain truncation node at depth
    if let Ok(proofs) = result {
        if !proofs.is_empty() {
            // Check reasonable depth (not necessarily truncated - might find via shorter path)
            assert!(proofs[0].depth() <= 55, "depth should be bounded");
        }
    }
}
