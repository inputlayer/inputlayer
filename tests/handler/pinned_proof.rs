//! A proof at the end of a program that writes is evaluated on the snapshot
//! the program's writes committed against, the state its guard passed on, and
//! returned in the program's reply beside the statements' messages.
//!
//! The decision programs here are the token form of a guarded claim: the
//! guard inserts a token once, every conditioned statement lands with it or
//! none does, and the claim withdraws the candidate it was guarded on. Only a
//! proof pinned to the commit snapshot can explain why such a claim won.

use inputlayer::protocol::{ErrorCode, Handler, ProgramError, QueryResult, WireValue};
use inputlayer::provenance::proof_tree::{NodeKind, ProofTree};
use inputlayer::{Config, StorageEngine};
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

const KG: &str = "default";

const PACK: &str = "+il_txn(t: string)
+il_ghost(n: int)
+attempt(candidate: string, claim: string)
+decision(id: string, candidate: string)
+offer[(\"s-42\", \"a\"), (\"s-42\", \"c\")]
+tool_ready[(\"a\"), (\"c\")]
+candidate(S, K) <- offer(S, K), tool_ready(K), !attempt(K, _)";

fn handler(dir: &Path) -> Handler {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    config.storage.persist.durability_mode = inputlayer::config::DurabilityMode::Immediate;
    Handler::new(StorageEngine::new(config).expect("storage"))
}

async fn run(handler: &Handler, program: &str) -> Result<QueryResult, ProgramError> {
    handler
        .execute_program_status(
            None,
            Some(KG.to_string()),
            program.to_string(),
            None,
            &handler.request_control(None),
        )
        .await
}

async fn ok(handler: &Handler, program: &str) -> QueryResult {
    let result = run(handler, program).await.expect(program);
    assert!(result.errors.is_empty(), "{program}: {:?}", result.errors);
    result
}

/// The decision `id` claiming candidate `k` for session `s-42`, guarded on the
/// candidate, ending with the proof of that candidate.
fn decide(id: &str, k: &str) -> String {
    format!(
        "-il_txn(\"\"), +il_txn(\"t-{id}\") <- candidate(S, K), S = \"s-42\", K = \"{k}\"
-il_ghost(0), +attempt(\"{k}\", \"claim-{id}\") <- il_txn(\"t-{id}\")
-il_ghost(0), +decision(\"{id}\", \"{k}\") <- il_txn(\"t-{id}\")
-il_txn(T) <- il_txn(T), T = \"t-{id}\"
.why ?candidate(\"s-42\", \"{k}\")"
    )
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

/// Whether the token landed, read from the guard statement's message.
fn won(result: &QueryResult) -> bool {
    match messages(result).first().map(String::as_str) {
        Some("Update: 0 deleted, 1 inserted.") => true,
        Some("Update: 0 deleted, 0 inserted.") => false,
        other => panic!("unexpected guard message {other:?}"),
    }
}

/// The relations the proof's base facts and absences name, sorted.
fn support(tree: &ProofTree) -> Vec<String> {
    let mut support: Vec<String> = tree
        .nodes
        .values()
        .filter(|node| matches!(node.kind, NodeKind::Fact | NodeKind::Negation))
        .map(|node| format!("{:?} {}", node.kind, node.conclusion.pred))
        .collect();
    support.sort();
    support
}

async fn count(handler: &Handler, query: &str) -> usize {
    ok(handler, query).await.rows.len()
}

#[tokio::test]
async fn a_decision_returns_the_proof_of_the_candidate_it_claimed() {
    let dir = TempDir::new().unwrap();
    let handler = handler(dir.path());
    ok(&handler, PACK).await;

    let result = ok(&handler, &decide("d-1", "c")).await;
    assert!(won(&result));
    let [tree] = trees(&result) else {
        panic!("one proof tree: {:?}", result.proof_trees)
    };
    assert_eq!(
        support(tree),
        ["Fact offer", "Fact tool_ready", "Negation attempt"]
    );
    assert!(tree.revision.is_some());

    // The claim withdrew the candidate: proved afterwards, it is gone.
    let after = ok(&handler, ".why ?candidate(\"s-42\", \"c\")").await;
    assert!(after.rows.is_empty());
    assert_eq!(count(&handler, "?decision(Id, K)").await, 1);

    // The reply crosses the wire with both the messages and the proof.
    let wire = serde_json::to_value(&result).unwrap();
    assert_eq!(wire["proof_trees"].as_array().map(Vec::len), Some(1));
    assert_eq!(wire["rows"].as_array().map(Vec::len), Some(4));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_decisions_one_wins_and_only_the_winner_has_a_proof() {
    let dir = TempDir::new().unwrap();
    let handler = Arc::new(handler(dir.path()));
    ok(&handler, PACK).await;

    let racers: Vec<_> = (0..8)
        .map(|i| {
            let handler = Arc::clone(&handler);
            tokio::spawn(async move { ok(&handler, &decide(&format!("d-{i}"), "c")).await })
        })
        .collect();
    // (won, proof trees) per racer: the winner's proof explains its
    // candidate; a refusal's snapshot had none.
    let mut outcomes = Vec::new();
    for racer in racers {
        let result = racer.await.unwrap();
        outcomes.push((won(&result), trees(&result).len()));
    }
    outcomes.sort_unstable();
    let mut expected = vec![(false, 0); 7];
    expected.push((true, 1));
    assert_eq!(outcomes, expected);
    assert_eq!(count(&handler, "?attempt(K, C)").await, 1);
    assert_eq!(count(&handler, "?decision(Id, K)").await, 1);
}

#[tokio::test]
async fn the_proof_rejects_a_program_it_cannot_end_and_survives_restart() {
    let dir = TempDir::new().unwrap();
    {
        let handler = handler(dir.path());
        ok(&handler, PACK).await;

        let misplaced = run(
            &handler,
            "+tool_ready(\"z\")\n.why ?candidate(S, K)\n+offer(\"s-42\", \"z\")",
        )
        .await
        .unwrap();
        assert_eq!(misplaced.errors.len(), 1);
        assert_eq!(misplaced.errors[0].index, 1);
        assert_eq!(misplaced.errors[0].code, ErrorCode::Unsupported);
        assert_eq!(
            count(&handler, "?tool_ready(T)").await,
            2,
            "nothing applied"
        );

        assert!(won(&ok(&handler, &decide("d-1", "c")).await));
    }
    // The decision and its claim are durable like any other write.
    let handler = handler(dir.path());
    assert_eq!(count(&handler, "?decision(Id, K)").await, 1);
    let refused = ok(&handler, &decide("d-2", "c")).await;
    assert!(!won(&refused));
    assert!(trees(&refused).is_empty());
}
