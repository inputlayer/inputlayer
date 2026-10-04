//! A proof that ends a program that writes is evaluated on the snapshot the
//! program's writes committed against: the state its guard passed on. A claim
//! guarded on `eligible` withdraws eligibility by succeeding, so only a proof
//! pinned to that snapshot can explain why the claim was allowed.

use super::*;
use crate::provenance::proof_tree::{NodeKind, ProofTree};

const KG: &str = "pinned_proof";

const FIXTURE: &str = "+attempt(s: string, t: string, id: string)
+need[(\"s1\", \"carrier_check\"), (\"s1\", \"say_eta\")]
+eligible(S, T) <- need(S, T), !attempt(S, T, _)";

/// Claims `carrier_check` for `s1` while it is eligible, then proves `proof`.
fn claim_then(proof: &str) -> String {
    format!(
        "-attempt(\"\", \"\", \"\"), +attempt(S, T, \"a-1\") <- \
         eligible(S, T), S = \"s1\", T = \"carrier_check\"\n{proof}"
    )
}

const WHY_ELIGIBLE: &str = ".why ?eligible(\"s1\", \"carrier_check\")";

fn handler_with_fixture() -> (Handler, tempfile::TempDir) {
    handler_with(FIXTURE)
}

fn handler_with(fixture: &str) -> (Handler, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    let handler = Handler::from_config(config).expect("handler creation failed");
    handler
        .storage
        .read()
        .create_knowledge_graph(KG)
        .expect("knowledge graph creation failed");
    let setup = run(&handler, fixture);
    assert!(setup.errors.is_empty(), "fixture: {:?}", setup.errors);
    (handler, tmp)
}

fn run(handler: &Handler, program: &str) -> QueryResult {
    handler
        .make_query_job()
        .execute(Some(KG.to_string()), program.to_string(), None)
        .expect("program failed")
}

fn messages(result: &QueryResult) -> Vec<String> {
    result
        .rows
        .iter()
        .map(|row| match row.values.as_slice() {
            [WireValue::String(message)] => message.clone(),
            other => format!("{other:?}"),
        })
        .collect()
}

fn trees(result: &QueryResult) -> &[ProofTree] {
    result
        .proof_trees
        .as_deref()
        .expect("the reply carries the proof")
}

/// Each tree's root conclusion, rendered.
fn roots(result: &QueryResult) -> Vec<String> {
    let mut roots: Vec<String> = trees(result)
        .iter()
        .flat_map(|tree| tree.roots.iter().map(|id| &tree.nodes[id]))
        .map(|node| format!("{}{:?}", node.conclusion.pred, node.conclusion.args))
        .collect();
    roots.sort();
    roots
}

/// The relations a tree's nodes of `kind` conclude, sorted.
fn leaves(tree: &ProofTree, kind: NodeKind) -> Vec<String> {
    let mut preds: Vec<String> = tree
        .nodes
        .values()
        .filter(|node| node.kind == kind)
        .map(|node| node.conclusion.pred.clone())
        .collect();
    preds.sort();
    preds
}

fn attempts(handler: &Handler) -> usize {
    run(handler, "?attempt(S, T, Id)").rows.len()
}

#[test]
fn a_won_claim_returns_the_proof_of_the_state_its_guard_passed_on() {
    let (handler, _tmp) = handler_with_fixture();
    let result = run(&handler, &claim_then(WHY_ELIGIBLE));

    assert!(result.errors.is_empty(), "{:?}", result.errors);
    assert_eq!(messages(&result), ["Update: 0 deleted, 1 inserted."]);
    assert_eq!(attempts(&handler), 1);
    assert_eq!(
        roots(&result),
        [r#"eligible[String("s1"), String("carrier_check")]"#]
    );
    let tree = &trees(&result)[0];
    assert_eq!(leaves(tree, NodeKind::Fact), ["need"]);
    assert_eq!(leaves(tree, NodeKind::Negation), ["attempt"]);
    assert!(tree.revision.is_some(), "the proof names its snapshot");

    // The claim withdrew eligibility: a proof after it explains nothing.
    let after = run(&handler, WHY_ELIGIBLE);
    assert!(after.rows.is_empty());
    assert!(after.proof_trees.is_none());
}

#[test]
fn a_refused_claim_returns_an_empty_proof() {
    let (handler, _tmp) = handler_with_fixture();
    run(&handler, &claim_then(WHY_ELIGIBLE));
    let refused = run(&handler, &claim_then(WHY_ELIGIBLE));

    assert!(refused.errors.is_empty(), "{:?}", refused.errors);
    assert_eq!(messages(&refused), ["Update: 0 deleted, 0 inserted."]);
    assert!(trees(&refused).is_empty());
    assert_eq!(attempts(&handler), 1);
}

#[test]
fn the_proof_revision_is_the_snapshot_before_the_program_changes() {
    let (handler, _tmp) = handler_with_fixture();
    let first = run(&handler, &claim_then(".why ?need(S, T)"));
    let second = run(&handler, "+need(\"s2\", \"say_eta\")\n.why ?need(S, T)");

    let revision = |result: &QueryResult| trees(result)[0].revision.expect("revision");
    assert!(revision(&second) > revision(&first));
    // The second program's proof does not see its own insert.
    assert_eq!(trees(&second).len(), 2);
    assert_eq!(run(&handler, "?need(S, T)").rows.len(), 3);
}

#[test]
fn writes_after_the_commit_do_not_change_the_proof() {
    let (handler, _tmp) = handler_with_fixture();
    let writer = handler.make_query_job();
    test_hook::set(test_hook::Point::ProofSearch, move || {
        let result = writer
            .execute(
                Some(KG.to_string()),
                "-need(\"s1\", \"say_eta\")".to_string(),
                None,
            )
            .expect("mutation failed");
        assert!(result.errors.is_empty(), "mutation: {:?}", result.errors);
    });
    let result = run(&handler, &claim_then(".why ?eligible(S, T)"));

    assert_eq!(messages(&result), ["Update: 0 deleted, 1 inserted."]);
    assert_eq!(
        roots(&result),
        [
            r#"eligible[String("s1"), String("carrier_check")]"#,
            r#"eligible[String("s1"), String("say_eta")]"#,
        ]
    );
    assert_eq!(
        run(&handler, "?need(S, T)").rows.len(),
        1,
        "the write landed"
    );
}

/// A conflicting write between staging and commit makes the program stage
/// again: the guard then refuses, and the proof explains the state of that
/// refusal, not the state the first staging saw.
#[test]
fn the_proof_follows_the_guard_through_a_restage() {
    let (handler, _tmp) = handler_with_fixture();
    let writer = handler.make_query_job();
    test_hook::set(test_hook::Point::Commit, move || {
        let result = writer
            .execute(
                Some(KG.to_string()),
                "+attempt(\"s1\", \"carrier_check\", \"a-0\")".to_string(),
                None,
            )
            .expect("conflicting claim failed");
        assert!(result.errors.is_empty(), "conflict: {:?}", result.errors);
    });
    let result = run(&handler, &claim_then(WHY_ELIGIBLE));

    assert!(result.errors.is_empty(), "{:?}", result.errors);
    assert_eq!(messages(&result), ["Update: 0 deleted, 0 inserted."]);
    assert!(trees(&result).is_empty());
    assert_eq!(attempts(&handler), 1);
}

#[test]
fn why_full_and_why_not_can_end_a_program_that_writes() {
    let (handler, _tmp) = handler_with_fixture();
    let full = run(&handler, &claim_then(".why full ?eligible(S, T)"));
    assert_eq!(messages(&full), ["Update: 0 deleted, 1 inserted."]);
    assert_eq!(trees(&full).len(), 2);

    let why_not = run(
        &handler,
        "+need(\"s2\", \"escalate\")\n.why_not eligible(\"s2\", \"escalate\")",
    );
    assert!(why_not.errors.is_empty(), "{:?}", why_not.errors);
    assert_eq!(messages(&why_not), ["Inserted 1 fact(s) into 'need'."]);
    let tree = &trees(&why_not)[0];
    assert_eq!(
        tree.query.as_deref(),
        Some(".why_not eligible(\"s2\", \"escalate\")")
    );
    assert!(tree.revision.is_some());
    // The proof ran before the insert: `need` was missing then.
    let text = crate::provenance::why_not::format_why_not_text(tree);
    assert!(text.contains("No matching tuples in need"), "{text}");
    let after = messages(&run(&handler, ".why_not eligible(\"s2\", \"escalate\")"));
    assert!(
        !after.join("\n").contains("No matching tuples in need"),
        "{after:?}"
    );
}

#[test]
fn a_failed_program_returns_no_proof() {
    let (handler, _tmp) = handler_with_fixture();
    let result = run(
        &handler,
        &format!("+need(\"s3\", \"say_eta\")\n+attempt(1, 2)\n{WHY_ELIGIBLE}"),
    );
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].index, 1);
    assert!(result.proof_trees.is_none());
    assert_eq!(run(&handler, "?need(S, T)").rows.len(), 2, "rolled back");
}

#[test]
fn a_proof_that_does_not_end_the_program_rejects_it() {
    let (handler, _tmp) = handler_with_fixture();
    for (program, index) in [
        (format!("{WHY_ELIGIBLE}\n+need(\"s3\", \"say_eta\")"), 0),
        (
            format!("+need(\"s3\", \"say_eta\")\n?need(S, T)\n{WHY_ELIGIBLE}"),
            1,
        ),
    ] {
        let result = run(&handler, &program);
        assert_eq!(result.errors.len(), 1, "{program}");
        assert_eq!(result.errors[0].index, index, "{program}");
        assert_eq!(result.errors[0].code, ErrorCode::Unsupported, "{program}");
        assert!(result.proof_trees.is_none(), "{program}");
    }
    assert_eq!(
        run(&handler, "?need(S, T)").rows.len(),
        2,
        "nothing applied"
    );
}

/// The example in the explainability guide's "Proofs of a Guarded Write".
#[test]
fn the_documented_example_runs() {
    let (handler, _tmp) = handler_with(
        "+attempt(session: string, tool: string, id: string)\n\
         +need[(\"s1\", \"carrier_check\")]\n\
         +eligible(S, T) <- need(S, T), !attempt(S, T, _)",
    );
    let result = run(&handler, &claim_then(WHY_ELIGIBLE));
    assert_eq!(messages(&result), ["Update: 0 deleted, 1 inserted."]);
    assert_eq!(trees(&result).len(), 1);
}

#[test]
fn a_failed_proof_fails_its_statement_and_keeps_the_writes() {
    let (handler, _tmp) = handler_with_fixture();
    let result = run(&handler, &claim_then(".why ?need(S, T), X > 1"));

    assert_eq!(messages(&result)[0], "Update: 0 deleted, 1 inserted.");
    assert_eq!(result.errors.len(), 1, "{:?}", result.errors);
    assert_eq!(result.errors[0].index, 1);
    assert!(
        result.errors[0].message.starts_with("Why error: "),
        "{:?}",
        result.errors
    );
    assert!(result.proof_trees.is_none());
    assert_eq!(attempts(&handler), 1, "the claim stays committed");
}
