//! Integration tests for why-not (negative explanation) using proof trees.

use inputlayer::ast::{Atom, BodyPredicate, ComparisonOp, Rule, Term};
use inputlayer::provenance::backward_chaining::ProofContext;
use inputlayer::provenance::proof_tree::NodeKind;
use inputlayer::provenance::why_not::{explain_why_not, format_why_not_text};
use inputlayer::provenance::{Blocker, ProofConfig};
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

fn var(s: &str) -> Term {
    Term::Variable(s.to_string())
}

fn simple_rule(head: &str, args: Vec<&str>, body: Vec<BodyPredicate>) -> Rule {
    Rule {
        head: Atom {
            relation: head.to_string(),
            args: args.into_iter().map(|s| var(s)).collect(),
        },
        body,
    }
}

fn pos(rel: &str, args: Vec<&str>) -> BodyPredicate {
    BodyPredicate::Positive(Atom {
        relation: rel.to_string(),
        args: args.into_iter().map(|s| var(s)).collect(),
    })
}

#[test]
fn test_why_not_base_missing_produces_graph() {
    let data = base_data(vec![("edge", vec![vec![int(1), int(2)]])]);
    let ctx = ProofContext::new(&[], &data, ProofConfig::default());

    let graph = explain_why_not("edge", &tuple(vec![int(99), int(99)]), &ctx);
    assert_eq!(graph.roots.len(), 1);
    let root = &graph.nodes[&graph.roots[0]];
    assert_eq!(root.kind, NodeKind::WhyNot);
    assert_eq!(root.conclusion.pred, "edge");
}

#[test]
fn test_why_not_join_shows_succeeded_and_failed_atoms() {
    // path(X, Z) <- edge(X, Y), edge(Y, Z)
    // edge(1,2) exists, edge(2,99) does not
    let rules = vec![simple_rule(
        "path",
        vec!["X", "Z"],
        vec![pos("edge", vec!["X", "Y"]), pos("edge", vec!["Y", "Z"])],
    )];
    let data = base_data(vec![(
        "edge",
        vec![vec![int(1), int(2)], vec![int(3), int(4)]],
    )]);
    let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

    let graph = explain_why_not("path", &tuple(vec![int(1), int(99)]), &ctx);
    let root = &graph.nodes[&graph.roots[0]];
    assert!(!root.children.is_empty());

    // The clause child should show body atom detail
    let clause = &graph.nodes[&root.children[0]];
    assert_eq!(clause.children.len(), 2, "should have 2 body atom children");
    let first = &graph.nodes[&clause.children[0]];
    let second = &graph.nodes[&clause.children[1]];
    assert_eq!(
        first.kind,
        NodeKind::Fact,
        "first atom (edge(1,Y)) succeeded"
    );
    assert_eq!(
        second.kind,
        NodeKind::WhyNot,
        "second atom (edge(2,99)) failed"
    );
}

#[test]
fn test_why_not_comparison_shows_progress() {
    // big(X, S) <- item(X, S), S > 100
    let rules = vec![Rule {
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
    // item(1,50) succeeded, S > 100 failed
    assert_eq!(clause.children.len(), 2);
    assert_eq!(graph.nodes[&clause.children[0]].kind, NodeKind::Fact);
    assert_eq!(graph.nodes[&clause.children[1]].kind, NodeKind::WhyNot);
}

#[test]
fn test_why_not_negation_shows_blocking_fact() {
    let rules = vec![simple_rule(
        "safe",
        vec!["X"],
        vec![
            pos("node", vec!["X"]),
            BodyPredicate::Negated(Atom {
                relation: "danger".to_string(),
                args: vec![var("X")],
            }),
        ],
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
    assert_eq!(graph.nodes[&clause.children[0]].kind, NodeKind::Fact);
    let neg_fail = &graph.nodes[&clause.children[1]];
    assert_eq!(neg_fail.kind, NodeKind::WhyNot);
    assert!(matches!(
        neg_fail.why_not.as_ref().unwrap().blocker,
        Blocker::NegationSucceeded { .. }
    ));
}

#[test]
fn test_why_not_wrong_arity() {
    let rules = vec![simple_rule(
        "derived",
        vec!["X", "Y"],
        vec![pos("base", vec!["X", "Y"])],
    )];
    let data = base_data(vec![("base", vec![])]);
    let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

    let graph = explain_why_not("derived", &tuple(vec![int(1)]), &ctx);
    let root = &graph.nodes[&graph.roots[0]];
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
fn test_why_not_text_format_still_works() {
    let rules = vec![simple_rule(
        "derived",
        vec!["X"],
        vec![pos("base", vec!["X"])],
    )];
    let data = base_data(vec![("base", vec![vec![int(1)]])]);
    let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

    let graph = explain_why_not("derived", &tuple(vec![int(99)]), &ctx);
    let formatted = format_why_not_text(&graph);
    assert!(
        formatted.contains("derived(99) was NOT derived:"),
        "got: {formatted}"
    );
    assert!(formatted.contains("Blocker:"), "got: {formatted}");
}

#[test]
fn test_why_not_graph_json_export() {
    let rules = vec![simple_rule(
        "derived",
        vec!["X"],
        vec![pos("base", vec!["X"])],
    )];
    let data = base_data(vec![("base", vec![vec![int(1)]])]);
    let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

    let graph = explain_why_not("derived", &tuple(vec![int(99)]), &ctx);
    let json = graph.to_json().expect("should serialize");
    assert_eq!(json["version"], 1);
    assert!(json["roots"].is_array());
    assert!(json["nodes"].is_object());
}
