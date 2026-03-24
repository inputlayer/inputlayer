//! Integration tests for why-not (negative explanation).

use inputlayer::ast::{Atom, BodyPredicate, ComparisonOp, Rule, Term};
use inputlayer::provenance::backward_chaining::ProofContext;
use inputlayer::provenance::why_not::explain_why_not;
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
fn test_why_not_end_to_end_base_missing() {
    let data = base_data(vec![("edge", vec![vec![int(1), int(2)]])]);
    let ctx = ProofContext::new(&[], &data, ProofConfig::default());

    let exp = explain_why_not("edge", &tuple(vec![int(99), int(99)]), &ctx);
    assert_eq!(exp.relation, "edge");
    assert!(exp.rule_failures.is_empty()); // No rules for base relation
    let formatted = exp.format_explanation();
    assert!(
        formatted.contains("No rules produce this relation"),
        "got: {formatted}"
    );
}

#[test]
fn test_why_not_end_to_end_join_failure() {
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

    let exp = explain_why_not("path", &tuple(vec![int(1), int(99)]), &ctx);
    assert!(!exp.rule_failures.is_empty());
    assert!(matches!(
        exp.rule_failures[0].blocker,
        Blocker::BodyAtomFailed { .. }
    ));
}

#[test]
fn test_why_not_end_to_end_comparison_failure() {
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

    let exp = explain_why_not("big", &tuple(vec![int(1), int(50)]), &ctx);
    assert!(!exp.rule_failures.is_empty());
    assert!(matches!(
        exp.rule_failures[0].blocker,
        Blocker::ComparisonFailed { .. }
    ));
}

#[test]
fn test_why_not_end_to_end_negation_block() {
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

    let exp = explain_why_not("safe", &tuple(vec![int(2)]), &ctx);
    assert!(!exp.rule_failures.is_empty());
    assert!(matches!(
        exp.rule_failures[0].blocker,
        Blocker::NegationSucceeded { .. }
    ));
}

#[test]
fn test_why_not_end_to_end_wrong_arity() {
    let rules = vec![simple_rule(
        "derived",
        vec!["X", "Y"],
        vec![pos("base", vec!["X", "Y"])],
    )];
    let data = base_data(vec![("base", vec![])]);
    let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

    let exp = explain_why_not("derived", &tuple(vec![int(1)]), &ctx);
    assert!(!exp.rule_failures.is_empty());
    assert!(matches!(
        exp.rule_failures[0].blocker,
        Blocker::HeadUnificationFailed { .. }
    ));
}

#[test]
fn test_why_not_end_to_end_nonexistent_relation() {
    let data = HashMap::new();
    let ctx = ProofContext::new(&[], &data, ProofConfig::default());

    let exp = explain_why_not("nonexistent", &tuple(vec![int(1)]), &ctx);
    assert!(exp.rule_failures.is_empty());
}

#[test]
fn test_why_not_explanation_format() {
    let rules = vec![simple_rule(
        "derived",
        vec!["X"],
        vec![pos("base", vec!["X"])],
    )];
    let data = base_data(vec![("base", vec![vec![int(1)]])]);
    let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

    let exp = explain_why_not("derived", &tuple(vec![int(99)]), &ctx);
    let formatted = exp.format_explanation();
    assert!(
        formatted.contains("derived(99) was NOT derived:"),
        "got: {formatted}"
    );
    assert!(formatted.contains("Blocker:"), "got: {formatted}");
}

#[test]
fn test_why_not_json_export() {
    let rules = vec![simple_rule(
        "derived",
        vec!["X"],
        vec![pos("base", vec!["X"])],
    )];
    let data = base_data(vec![("base", vec![vec![int(1)]])]);
    let ctx = ProofContext::new(&rules, &data, ProofConfig::default());

    let exp = explain_why_not("derived", &tuple(vec![int(99)]), &ctx);
    let json = exp.to_json().expect("should serialize");
    assert_eq!(json["relation"], "derived");
    assert!(!json["rule_failures"]
        .as_array()
        .unwrap_or(&vec![])
        .is_empty());
}
