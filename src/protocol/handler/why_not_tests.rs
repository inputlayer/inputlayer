//! `.why_not` answers on the evaluated knowledge graph: a row the query
//! `?relation(values)` returns is reported as derived, never as NOT derived,
//! and a refusal names a blocker that holds on the derived relations as well
//! as the base facts. `.why` proofs read the same derived relations.

use super::*;
use crate::provenance::proof_tree::{NodeKind, ProofTree};
use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The support demo from issue #291: `candidate` has a constant in every
/// rule head and `allowed` reads it.
const SUPPORT: &str = "+ticket[(\"s-43\", \"refund\"), (\"s-44\", \"question\")]
+blocked[(\"s-43\", \"issue_refund\")]
+candidate(S, \"issue_refund\") <- ticket(S, \"refund\")
+candidate(S, \"open_ticket\") <- ticket(S, _)
+candidate(S, \"escalate\") <- ticket(S, _)
+allowed(S, T) <- candidate(S, T), !blocked(S, T)";

/// A knowledge graph of its own on a shared handler.
struct Kg<'h> {
    handler: &'h Handler,
    name: String,
}

fn handler() -> (Handler, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    let handler = Handler::from_config(config).expect("handler creation failed");
    (handler, tmp)
}

impl<'h> Kg<'h> {
    fn new(handler: &'h Handler, fixture: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let name = format!("why_not_{}", NEXT.fetch_add(1, Ordering::Relaxed));
        handler
            .storage
            .read()
            .create_knowledge_graph(&name)
            .expect("knowledge graph creation failed");
        let kg = Self { handler, name };
        let setup = kg.run(fixture);
        assert!(setup.errors.is_empty(), "fixture: {:?}", setup.errors);
        kg
    }

    fn run(&self, program: &str) -> QueryResult {
        self.handler
            .make_query_job()
            .execute(Some(self.name.clone()), program.to_string(), None)
            .expect("program failed")
    }

    fn rows(&self, query: &str) -> usize {
        let result = self.run(query);
        assert!(result.errors.is_empty(), "{query}: {:?}", result.errors);
        result.rows.len()
    }

    fn why_not(&self, target: &str) -> QueryResult {
        let result = self.run(&format!(".why_not {target}"));
        assert!(result.errors.is_empty(), "{target}: {:?}", result.errors);
        result
    }

    /// Whether `.why_not target` reports the target as NOT derived.
    fn refuses(&self, target: &str) -> bool {
        let result = self.why_not(target);
        let refused = root(&result).kind == NodeKind::WhyNot;
        let text = text(&result);
        let says_not_derived = text.contains("was NOT derived");
        assert_eq!(refused, says_not_derived, "{target}: {text}");
        assert_eq!(!refused, text.contains("was derived"), "{target}: {text}");
        refused
    }
}

fn text(result: &QueryResult) -> String {
    result
        .rows
        .iter()
        .map(|row| match row.values.as_slice() {
            [WireValue::String(line)] => line.clone(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn tree(result: &QueryResult) -> &ProofTree {
    &result.proof_trees.as_ref().expect("proof trees")[0]
}

fn root(result: &QueryResult) -> &crate::provenance::proof_tree::ProofNode {
    let tree = tree(result);
    &tree.nodes[&tree.roots[0]]
}

/// The blockers of every clause of a refusal, as displayed.
fn blockers(result: &QueryResult) -> Vec<String> {
    let tree = tree(result);
    root(result)
        .children
        .iter()
        .map(|clause_id| {
            let clause = &tree.nodes[clause_id];
            clause
                .why_not
                .iter()
                .chain(
                    clause
                        .children
                        .iter()
                        .filter_map(|id| tree.nodes[id].why_not.as_ref()),
                )
                .map(|info| info.blocker.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        })
        .collect()
}

/// The premises each clause of a refusal shows as holding.
fn held(result: &QueryResult) -> Vec<String> {
    let tree = tree(result);
    root(result)
        .children
        .iter()
        .flat_map(|clause_id| &tree.nodes[clause_id].children)
        .map(|id| &tree.nodes[id])
        .filter(|node| node.kind == NodeKind::Fact)
        .map(|node| atom(&node.conclusion.pred, &node.conclusion.args))
        .collect()
}

fn atom(relation: &str, values: &[crate::value::Value]) -> String {
    let values: Vec<String> = values.iter().map(ToString::to_string).collect();
    format!("{relation}({})", values.join(", "))
}

#[test]
fn a_row_read_from_a_constant_head_view_is_derived() {
    let (handler, _tmp) = handler();
    let kg = Kg::new(&handler, SUPPORT);
    assert_eq!(kg.rows("?candidate(\"s-43\", T)"), 3);
    assert_eq!(kg.rows("?allowed(\"s-43\", T)"), 2);

    for derived in [
        "allowed(\"s-43\", \"open_ticket\")",
        "allowed(\"s-43\", \"escalate\")",
        "candidate(\"s-43\", \"issue_refund\")",
        "candidate(\"s-44\", \"open_ticket\")",
        "ticket(\"s-43\", \"refund\")",
    ] {
        assert!(!kg.refuses(derived), "{derived} is derived");
    }

    // The answer carries the proof `.why` gives.
    let result = kg.why_not("allowed(\"s-43\", \"open_ticket\")");
    let text = text(&result);
    assert!(
        text.starts_with("allowed(\"s-43\", \"open_ticket\") was derived:"),
        "{text}"
    );
    assert!(
        text.contains("[rule] candidate(\"s-43\", \"open_ticket\")"),
        "{text}"
    );
    assert!(
        text.contains("[negation] blocked(\"s-43\", \"open_ticket\")"),
        "{text}"
    );
}

#[test]
fn a_refusal_over_a_constant_head_view_names_the_true_blocker() {
    let (handler, _tmp) = handler();
    let kg = Kg::new(&handler, SUPPORT);
    let result = kg.why_not("allowed(\"s-43\", \"issue_refund\")");
    assert!(
        text(&result).contains("was NOT derived"),
        "{}",
        text(&result)
    );
    assert_eq!(
        blockers(&result),
        ["Negated blocked(\"s-43\", \"issue_refund\") exists"],
        "{}",
        text(&result)
    );
    assert_eq!(held(&result), ["candidate(\"s-43\", \"issue_refund\")"]);

    let result = kg.why_not("allowed(\"s-44\", \"issue_refund\")");
    assert_eq!(
        blockers(&result),
        ["Body predicate 0 (candidate(\"s-44\", \"issue_refund\")) failed: No matching tuples in candidate"],
    );
}

#[test]
fn a_constant_in_the_head_that_differs_is_named_not_reported_as_arity() {
    let (handler, _tmp) = handler();
    let kg = Kg::new(&handler, SUPPORT);
    let result = kg.why_not("candidate(\"s-43\", \"close\")");
    assert_eq!(
        blockers(&result),
        [
            "Head unification failed: column 1: rule head has \"issue_refund\", target has \"close\"",
            "Head unification failed: column 1: rule head has \"open_ticket\", target has \"close\"",
            "Head unification failed: column 1: rule head has \"escalate\", target has \"close\"",
        ]
    );

    // The clause whose head matches is explained by its body.
    let result = kg.why_not("candidate(\"s-44\", \"issue_refund\")");
    assert_eq!(
        blockers(&result)[0],
        "Body predicate 0 (ticket(\"s-44\", \"refund\")) failed: No matching tuples in ticket"
    );

    let result = kg.why_not("candidate(\"s-43\")");
    assert_eq!(
        blockers(&result)[0],
        "Head unification failed: target has 1 values, rule head has 2"
    );
}

#[test]
fn mixed_constant_and_variable_heads() {
    let (handler, _tmp) = handler();
    let kg = Kg::new(
        &handler,
        "+ticket[(\"s-1\", \"refund\", 120), (\"s-2\", \"refund\", 40), (\"s-3\", \"bug\", 10)]
+route(S, \"finance\", A) <- ticket(S, \"refund\", A), A > 50
+route(S, \"support\", A) <- ticket(S, _, A)
+escalated(S, Q) <- route(S, Q, _), Q = \"finance\"",
    );
    for (target, derived) in [
        ("route(\"s-1\", \"finance\", 120)", true),
        ("route(\"s-2\", \"finance\", 40)", false),
        ("route(\"s-3\", \"support\", 10)", true),
        ("escalated(\"s-1\", \"finance\")", true),
        ("escalated(\"s-2\", \"finance\")", false),
        ("escalated(\"s-1\", \"support\")", false),
    ] {
        assert_eq!(!kg.refuses(target), derived, "{target}");
    }

    let result = kg.why_not("route(\"s-2\", \"finance\", 40)");
    assert_eq!(
        blockers(&result),
        [
            "Comparison 40 > 50 failed: 40 vs 50",
            "Head unification failed: column 1: rule head has \"support\", target has \"finance\"",
        ]
    );

    let result = kg.why_not("escalated(\"s-2\", \"finance\")");
    assert_eq!(
        blockers(&result),
        ["Body predicate 0 (route(\"s-2\", \"finance\", _)) failed: No matching tuples in route"]
    );
    let result = kg.why_not("escalated(\"s-1\", \"support\")");
    assert_eq!(
        blockers(&result),
        ["Comparison \"support\" = \"finance\" failed: \"support\" vs \"finance\""]
    );
}

#[test]
fn a_derivation_found_past_a_dead_end_match_is_derived() {
    // The first match of `edge(1, Y)` is a dead end; the second derives it.
    let (handler, _tmp) = handler();
    let kg = Kg::new(
        &handler,
        "+edge[(1, 2), (1, 3), (3, 4)]
+two_hop(X, Z) <- edge(X, Y), edge(Y, Z)",
    );
    assert!(!kg.refuses("two_hop(1, 4)"));
    let result = kg.why_not("two_hop(1, 5)");
    assert_eq!(
        blockers(&result),
        ["Body predicate 1 (edge(2, 5)) failed: No matching tuples in edge"]
    );
}

#[test]
fn recursive_relations() {
    let (handler, _tmp) = handler();
    let kg = Kg::new(
        &handler,
        "+edge[(1, 2), (2, 3), (3, 4), (5, 6)]
+path(X, Y) <- edge(X, Y)
+path(X, Z) <- path(X, Y), edge(Y, Z)
+cut(X, \"far\") <- path(X, 4), !edge(X, 4)",
    );
    for derived in [
        "path(1, 4)",
        "path(2, 4)",
        "cut(1, \"far\")",
        "cut(2, \"far\")",
    ] {
        assert!(!kg.refuses(derived), "{derived}");
    }
    let result = kg.why_not("path(1, 6)");
    assert_eq!(
        blockers(&result),
        [
            "Body predicate 0 (edge(1, 6)) failed: No matching tuples in edge",
            "Body predicate 1 (edge(2, 6)) failed: No matching tuples in edge",
        ]
    );
    // Every premise shown as holding is a row of its relation.
    for premise in held(&result) {
        assert_eq!(kg.rows(&format!("?{premise}")), 1, "{premise}");
    }

    let result = kg.why_not("cut(3, \"far\")");
    assert_eq!(blockers(&result), ["Negated edge(3, 4) exists"]);
    assert!(kg.refuses("cut(5, \"far\")"));
}

#[test]
fn negation_over_a_derived_relation() {
    let (handler, _tmp) = handler();
    let kg = Kg::new(
        &handler,
        "+candidate[(\"s-1\", \"refund\"), (\"s-2\", \"refund\")]
+flagged[(\"s-1\", \"refund\")]
+override[(\"s-1\", \"refund\")]
+blocked(S, T) <- flagged(S, T)
+allowed(S, T) <- candidate(S, T), !blocked(S, T)
+allowed(S, T) <- override(S, T)",
    );
    assert!(!kg.refuses("allowed(\"s-2\", \"refund\")"));
    assert!(!kg.refuses("allowed(\"s-1\", \"refund\")"));

    // The proof of allowed(s-1) is the override: blocked(s-1) is derived,
    // so the negation cannot prove it.
    for proof in [
        ".why ?allowed(\"s-1\", T)",
        ".why_not allowed(\"s-1\", \"refund\")",
    ] {
        let result = kg.run(proof);
        let tree = tree(&result);
        let rendered = tree.format_tree();
        assert!(
            rendered.contains("rule: allowed(S, T) <- override(S, T)"),
            "{proof}: {rendered}"
        );
        assert!(!rendered.contains("[negation]"), "{proof}: {rendered}");
    }

    let kg = Kg::new(
        &handler,
        "+candidate[(\"s-1\", \"refund\")]
+flagged[(\"s-1\", \"refund\")]
+blocked(S, T) <- flagged(S, T)
+allowed(S, T) <- candidate(S, T), !blocked(S, T)",
    );
    let result = kg.why_not("allowed(\"s-1\", \"refund\")");
    assert_eq!(
        blockers(&result),
        ["Negated blocked(\"s-1\", \"refund\") exists"]
    );
}

#[test]
fn computed_heads_and_assignments() {
    let (handler, _tmp) = handler();
    let kg = Kg::new(
        &handler,
        "+item[(1, 3), (2, 8)]
+kind[(1, \"tool\"), (2, \"tool\")]
+next(X, N + 1) <- item(X, N)
+double(X, D) <- item(X, N), D = N * 2, D > 10
+count_items(K, count<X>) <- kind(X, K)",
    );
    assert!(!kg.refuses("next(1, 4)"));
    assert!(!kg.refuses("double(2, 16)"));
    assert!(!kg.refuses("count_items(\"tool\", 2)"));

    let result = kg.why_not("next(1, 5)");
    assert_eq!(
        blockers(&result),
        ["Head unification failed: column 1: N+1 is 4, target has 5"]
    );
    let result = kg.why_not("double(1, 6)");
    assert_eq!(blockers(&result), ["Comparison 6 > 10 failed: 6 vs 10"]);
    let result = kg.why_not("count_items(\"tool\", 5)");
    assert_eq!(
        blockers(&result),
        ["Head unification failed: column 1: count<X> over this group is 2, target has 5"]
    );

    // `.why` proves rows of computed heads instead of giving up.
    for query in ["?next(X, Y)", "?double(X, D)"] {
        let result = kg.run(&format!(".why {query}"));
        for tree in result.proof_trees.as_ref().expect("proof trees") {
            let root = &tree.nodes[&tree.roots[0]];
            assert_eq!(root.kind, NodeKind::Rule, "{query}: {}", tree.format_tree());
        }
    }
}

/// One rule template of the generated programs: its text with `C` and `D`
/// standing for the case's constants.
const TEMPLATES: &[&str] = &[
    "p(X, Y) <- e(X, Y)",
    "p(X, C) <- e(X, _)",
    "p(C, Y) <- f(Y, _)",
    "p(X, Z) <- p(X, Y), e(Y, Z)",
    "p(X, Y) <- f(X, Y), X < Y",
    "q(X, Y) <- p(X, Y), !f(X, Y)",
    "q(X, C) <- p(X, Y), Y > D",
    "q(X, Y) <- e(X, Y), !p(Y, X)",
    "q(X, Y) <- p(X, Z), p(Z, Y)",
    "q(C, D) <- g(C)",
    "r(X) <- q(X, _), !g(X)",
    "r(X) <- p(X, X)",
];

fn program(
    rules: &[bool],
    c: i32,
    d: i32,
    e: &[(i32, i32)],
    f: &[(i32, i32)],
    g: &[i32],
) -> String {
    let pairs = |rows: &[(i32, i32)]| {
        rows.iter()
            .map(|(a, b)| format!("({a}, {b})"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut lines = vec![
        "+e(a: int, b: int)".to_string(),
        "+f(a: int, b: int)".to_string(),
        "+g(a: int)".to_string(),
    ];
    for (name, rows) in [("e", pairs(e)), ("f", pairs(f))] {
        if !rows.is_empty() {
            lines.push(format!("+{name}[{rows}]"));
        }
    }
    if !g.is_empty() {
        let rows: Vec<String> = g.iter().map(|v| format!("({v})")).collect();
        lines.push(format!("+g[{}]", rows.join(", ")));
    }
    // Every derived relation has a rule, so every relation a rule reads exists.
    let mut chosen: Vec<&str> = TEMPLATES
        .iter()
        .zip(rules)
        .filter_map(|(template, &on)| on.then_some(*template))
        .collect();
    for (head, fallback) in [
        ("p(", TEMPLATES[0]),
        ("q(", "q(X, Y) <- p(X, Y)"),
        ("r(", "r(X) <- q(X, X)"),
    ] {
        if !chosen.iter().any(|rule| rule.starts_with(head)) {
            chosen.push(fallback);
        }
    }
    for rule in chosen {
        let rule = rule
            .replace('C', &c.to_string())
            .replace('D', &d.to_string());
        lines.push(format!("+{rule}"));
    }
    lines.join("\n")
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 48,
        failure_persistence: None,
        .. ProptestConfig::default()
    })]

    /// For random small programs, `.why_not x` refuses exactly when `?x`
    /// has no row, and every premise a refusal shows as holding is a row.
    #[test]
    fn why_not_refuses_exactly_the_rows_the_query_lacks(
        rules in proptest::collection::vec(any::<bool>(), TEMPLATES.len()),
        c in 1..=3i32,
        d in 1..=3i32,
        e in proptest::collection::vec((1..=3i32, 1..=3i32), 0..6),
        f in proptest::collection::vec((1..=3i32, 1..=3i32), 0..4),
        g in proptest::collection::vec(1..=3i32, 0..2),
    ) {
        let (handler, _tmp) = handler();
        let program = program(&rules, c, d, &e, &f, &g);
        let kg = Kg::new(&handler, &program);
        let mut targets = Vec::new();
        for x in 1..=3 {
            targets.push(format!("r({x})"));
            for y in 1..=3 {
                targets.push(format!("p({x}, {y})"));
                targets.push(format!("q({x}, {y})"));
            }
        }
        for target in targets {
            let has_row = kg.rows(&format!("?{target}")) > 0;
            prop_assert_eq!(kg.refuses(&target), !has_row, "{}\n{}", target, program);
            if !has_row {
                let result = kg.why_not(&target);
                for premise in held(&result) {
                    prop_assert!(kg.rows(&format!("?{premise}")) > 0, "{} for {}\n{}", premise, target, program);
                }
            }
        }
    }
}
