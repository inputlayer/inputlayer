//! A proof explains the snapshot it captured. Writes that commit after the
//! capture, while proof search runs without the storage guard, must not
//! change the proof's rows or DAG.

use super::*;

const KG: &str = "proof_snapshot";

/// `path(1, 3)` has two derivations: `edge(1, 3)` and `edge(1, 2), edge(2, 3)`.
const FIXTURE: &str = "+edge[(1, 2), (2, 3), (1, 3)]
+path(X, Y) <- edge(X, Y)
+path(X, Z) <- path(X, Y), edge(Y, Z)";

/// Removes both derivations of `path(1, 3)` and derives `path(1, 4)`.
const MUTATION: &str = "-edge(1, 3)\n-edge(2, 3)\n+edge[(1, 4)]";

fn handler_with_fixture() -> (Handler, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    let handler = Handler::from_config(config).expect("handler creation failed");
    handler
        .storage
        .read()
        .create_knowledge_graph(KG)
        .expect("knowledge graph creation failed");
    execute(&handler, FIXTURE);
    (handler, tmp)
}

fn execute(handler: &Handler, program: &str) -> QueryResult {
    let result = handler
        .make_query_job()
        .execute(Some(KG.to_string()), program.to_string(), None)
        .expect("program failed");
    assert!(result.errors.is_empty(), "{program}: {:?}", result.errors);
    result
}

/// The rows and proof DAGs of a proof result. Revisions count publishes
/// across knowledge graphs, so they are left out.
fn proof_of(result: &QueryResult) -> serde_json::Value {
    let mut trees = result.proof_trees.clone();
    for tree in trees.iter_mut().flatten() {
        tree.revision = None;
    }
    serde_json::json!({
        "rows": result.rows,
        "proof_trees": trees,
    })
}

/// Runs `program` with `MUTATION` committed after the proof captured its
/// snapshot and released the storage guard, before proof search.
fn execute_with_mutation_during_search(handler: &Handler, program: &str) -> QueryResult {
    let writer = handler.make_query_job();
    test_hook::set(test_hook::Point::ProofSearch, move || {
        let result = writer
            .execute(Some(KG.to_string()), MUTATION.to_string(), None)
            .expect("mutation failed");
        assert!(result.errors.is_empty(), "mutation: {:?}", result.errors);
    });
    let result = execute(handler, program);
    let edges = execute(handler, "?edge(X, Y)");
    assert_eq!(edges.rows.len(), 2, "the mutation committed during search");
    result
}

#[test]
fn why_explains_the_captured_snapshot_despite_concurrent_writes() {
    for program in [".why ?path(1, Y)", ".why full ?path(X, Y)"] {
        let (handler, _tmp) = handler_with_fixture();
        let unmutated = proof_of(&execute(&handler, program));

        let (handler, _tmp) = handler_with_fixture();
        let mutated_during_search = execute_with_mutation_during_search(&handler, program);
        assert_eq!(proof_of(&mutated_during_search), unmutated, "{program}");

        // The next proof sees the mutation.
        assert_ne!(
            proof_of(&execute(&handler, program)),
            unmutated,
            "{program}"
        );
    }
}

#[test]
fn why_not_explains_the_captured_snapshot_despite_concurrent_writes() {
    let program = ".why_not path(1, 4)";
    let (handler, _tmp) = handler_with_fixture();
    let unmutated = proof_of(&execute(&handler, program));

    let (handler, _tmp) = handler_with_fixture();
    let mutated_during_search = execute_with_mutation_during_search(&handler, program);
    assert_eq!(proof_of(&mutated_during_search), unmutated);

    assert_ne!(proof_of(&execute(&handler, program)), unmutated);
}
