//! `expect_revision` on `execute`: a program's writes commit only while the
//! state in scope is as it was at the expected revision. A refused program
//! applies nothing and fails with `precondition_failed`; staging it again
//! cannot help, so it is never retried. Only a program that writes may carry
//! the precondition.

use super::*;
use crate::storage_engine::Precondition;

const KG: &str = "expect_revision";

fn handler() -> (Arc<Handler>, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    let handler = Handler::from_config(config).expect("handler creation failed");
    handler
        .storage
        .read()
        .create_knowledge_graph(KG)
        .expect("knowledge graph creation failed");
    (Arc::new(handler), tmp)
}

fn revision(handler: &Handler) -> u64 {
    handler
        .storage
        .read()
        .get_snapshot_for(KG)
        .expect("snapshot")
        .revision
}

async fn run(
    handler: &Handler,
    program: &str,
    precondition: Option<Precondition>,
) -> Result<QueryResult, ProgramError> {
    let control = handler.request_control_expecting(None, precondition);
    handler
        .execute_program_status(
            None,
            Some(KG.to_string()),
            program.to_string(),
            None,
            &control,
        )
        .await
}

fn expect(revision: u64, relations: &[&str]) -> Option<Precondition> {
    Some(Precondition {
        revision,
        relations: Some(relations.iter().map(ToString::to_string).collect()),
    })
}

async fn count(handler: &Handler, query: &str) -> usize {
    run(handler, query, None).await.expect("query").rows.len()
}

#[tokio::test]
async fn a_write_commits_while_its_scope_is_unchanged() {
    let (handler, _tmp) = handler();
    run(&handler, "+eta(\"s1\", 3)", None).await.unwrap();
    let seen = revision(&handler);
    run(&handler, "+noise(1)", None).await.unwrap();

    let result = run(&handler, "+claim(\"s1\", \"a\")", expect(seen, &["eta"]))
        .await
        .unwrap();
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    assert_eq!(count(&handler, "?claim(S, A)").await, 1);
}

#[tokio::test]
async fn a_change_in_scope_refuses_the_program_with_precondition_failed() {
    let (handler, _tmp) = handler();
    run(&handler, "+eta(\"s1\", 3)", None).await.unwrap();
    let seen = revision(&handler);
    run(&handler, "+eta(\"s2\", 5)", None).await.unwrap();

    let error = run(&handler, "+claim(\"s1\", \"a\")", expect(seen, &["eta"]))
        .await
        .unwrap_err();
    assert_eq!(error.code, Some(ErrorCode::PreconditionFailed));
    assert!(error.message.contains("relation 'eta' changed"), "{error}");
    assert_eq!(count(&handler, "?claim(S, A)").await, 0);

    // Several writes: none applies, and the failure says so.
    let result = run(
        &handler,
        "+claim(\"s1\", \"a\")\n+attempt(\"s1\", \"a\")",
        expect(seen, &["eta"]),
    )
    .await
    .unwrap();
    let [failure] = result.errors.as_slice() else {
        panic!("one failure expected: {:?}", result.errors);
    };
    assert_eq!(failure.code, ErrorCode::PreconditionFailed);
    assert!(
        failure.message.contains("rolled back"),
        "{}",
        failure.message
    );
    assert_eq!(count(&handler, "?claim(S, A)").await, 0);
    assert_eq!(count(&handler, "?attempt(S, A)").await, 0);
}

#[tokio::test]
async fn a_guarded_write_is_checked_against_the_expected_revision() {
    let (handler, _tmp) = handler();
    run(
        &handler,
        "+eta(\"s1\", 3)\n+late(S) <- eta(S, D), D > 2",
        None,
    )
    .await
    .unwrap();
    let seen = revision(&handler);
    // The guard still holds after this write, but the window changed.
    run(&handler, "+eta(\"s1\", 4)", None).await.unwrap();
    let claim = "-claim(\"\", \"\"), +claim(S, \"a\") <- late(S)";
    let error = run(&handler, claim, expect(seen, &["late"]))
        .await
        .unwrap_err();
    assert_eq!(error.code, Some(ErrorCode::PreconditionFailed));

    let fresh = revision(&handler);
    let result = run(&handler, claim, expect(fresh, &["late"]))
        .await
        .unwrap();
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    assert_eq!(count(&handler, "?claim(S, A)").await, 1);
}

#[tokio::test]
async fn programs_that_do_not_write_cannot_carry_a_precondition() {
    let (handler, _tmp) = handler();
    run(&handler, "+eta(\"s1\", 3)", None).await.unwrap();
    let seen = revision(&handler);
    for program in ["?eta(S, D)", ".rel", "session_rule(X) <- eta(X, _)"] {
        let error = run(&handler, program, expect(seen, &["eta"]))
            .await
            .unwrap_err();
        assert_eq!(error.code, Some(ErrorCode::InvalidRequest), "{program}");
    }
}

#[tokio::test]
async fn an_unknown_scope_relation_is_a_validation_error() {
    let (handler, _tmp) = handler();
    run(&handler, "+eta(\"s1\", 3)", None).await.unwrap();
    let seen = revision(&handler);
    let error = run(&handler, "+claim(\"s1\", \"a\")", expect(seen, &["etaa"]))
        .await
        .unwrap_err();
    assert_eq!(error.code, Some(ErrorCode::Validation));
    assert!(error.message.contains("'etaa'"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn of_concurrent_programs_expecting_one_revision_exactly_one_commits() {
    const WRITERS: usize = 8;
    let (handler, _tmp) = handler();
    run(&handler, "+slot(0)", None).await.unwrap();
    for round in 0..10 {
        let seen = revision(&handler);
        let writers: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let handler = Arc::clone(&handler);
                let value = round * WRITERS + writer + 1;
                tokio::spawn(async move {
                    let program = format!("+slot({value})");
                    run(&handler, &program, expect(seen, &["slot"])).await
                })
            })
            .collect();
        let mut committed = 0;
        for writer in writers {
            match writer.await.unwrap() {
                Ok(result) => {
                    assert!(result.errors.is_empty(), "{:?}", result.errors);
                    committed += 1;
                }
                Err(error) => {
                    assert_eq!(error.code, Some(ErrorCode::PreconditionFailed), "{error}");
                }
            }
        }
        assert_eq!(committed, 1, "round {round}");
        assert_eq!(count(&handler, "?slot(X)").await, round + 2);
    }
}
