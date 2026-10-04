//! Per-statement counts: a result lists every fact statement its program
//! committed with the tuples it inserted and deleted, so a client reads
//! whether a write landed from data instead of message text.

use inputlayer::protocol::{
    ErrorCode, Handler, ProgramError, QueryResult, StatementCounts, StatementKind,
};
use inputlayer::{Config, StorageEngine};
use tempfile::TempDir;

fn handler() -> (Handler, TempDir) {
    let temp = TempDir::new().expect("create temp dir");
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    let storage = StorageEngine::new(config).expect("create storage engine");
    (Handler::new(storage), temp)
}

async fn run(handler: &Handler, program: &str) -> Result<QueryResult, ProgramError> {
    handler
        .execute_program_status(
            None,
            Some("default".to_string()),
            program.to_string(),
            None,
            &handler.request_control(None),
        )
        .await
}

fn counts(index: usize, kind: StatementKind, inserted: usize, deleted: usize) -> StatementCounts {
    StatementCounts {
        index,
        kind,
        inserted,
        deleted,
    }
}

#[tokio::test]
async fn every_fact_statement_reports_its_effective_counts() {
    let (handler, _tmp) = handler();
    let program = "\
+item[(1,), (2,), (3,)]
+item(1)
+big(X) <- item(X), X > 5
-item(2)
-item(9)
-item(X) <- item(X), X > 2
-item(1), +item(10) <- item(1)";
    let result = run(&handler, program).await.expect("program result");
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    assert_eq!(
        result.statements,
        [
            counts(0, StatementKind::Insert, 3, 0),
            // Already present: committed, but changed nothing.
            counts(1, StatementKind::Insert, 0, 0),
            // The rule (statement 2) changes no facts and is not listed.
            counts(3, StatementKind::Delete, 0, 1),
            counts(4, StatementKind::Delete, 0, 0),
            counts(5, StatementKind::Delete, 0, 1),
            counts(6, StatementKind::Update, 1, 1),
        ]
    );
}

#[tokio::test]
async fn a_guarded_insert_reports_whether_its_token_landed() {
    let (handler, _tmp) = handler();
    run(&handler, "+task(\"t1\")\n+held(\"none\")")
        .await
        .expect("seed");
    // The update form with a never-present delete anchor inserts the token
    // only while the guard holds.
    let claim = "-il_ghost(0), +held(T) <- task(T), T = \"t1\", !held(T)";
    for (attempt, inserted) in [(1, 1), (2, 0)] {
        let result = run(&handler, claim).await.expect("claim");
        assert!(result.errors.is_empty(), "{:?}", result.errors);
        assert_eq!(
            result.statements,
            [counts(0, StatementKind::Update, inserted, 0)],
            "attempt {attempt}"
        );
    }
}

#[tokio::test]
async fn counts_survive_a_trailing_query() {
    let (handler, _tmp) = handler();
    let result = run(&handler, "+item[(1,), (2,)]\n-item(1)\n?item(X)")
        .await
        .expect("program result");
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    assert_eq!(result.rows.len(), 1, "the query's rows: {result:?}");
    assert_eq!(
        result.statements,
        [
            counts(0, StatementKind::Insert, 2, 0),
            counts(1, StatementKind::Delete, 0, 1),
        ]
    );
}

#[tokio::test]
async fn counts_survive_a_failed_trailing_query() {
    let (handler, _tmp) = handler();
    let result = run(&handler, "+item(1)\n?item(X), Y > 1")
        .await
        .expect("program result");
    assert_eq!(result.errors.len(), 1, "{result:?}");
    assert_eq!(result.errors[0].index, 1);
    assert_eq!(result.statements, [counts(0, StatementKind::Insert, 1, 0)]);
}

#[tokio::test]
async fn a_rolled_back_program_lists_no_counts() {
    let (handler, _tmp) = handler();
    let result = run(&handler, "+a(1)\n.rule drop missing")
        .await
        .expect("program result");
    assert_eq!(result.errors.len(), 1, "{:?}", result.errors);
    assert_eq!(result.errors[0].code, ErrorCode::NotFound);
    assert!(result.statements.is_empty(), "{:?}", result.statements);
    assert!(run(&handler, "?a(X)").await.expect("read").rows.is_empty());
}

#[tokio::test]
async fn a_read_lists_no_counts() {
    let (handler, _tmp) = handler();
    run(&handler, "+item(1)").await.expect("seed");
    let result = run(&handler, "?item(X)").await.expect("read");
    assert_eq!(result.rows.len(), 1);
    assert!(result.statements.is_empty(), "{:?}", result.statements);
}
