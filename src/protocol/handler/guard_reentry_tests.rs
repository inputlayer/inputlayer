//! Meta commands must not re-acquire the storage read guard that
//! `QueryJob::execute` already holds. parking_lot's `RwLock` prefers writers:
//! once a writer queues behind the outer guard, a second `read()` on the same
//! thread waits for that writer, which waits for the outer guard, so the
//! query deadlocks.

use super::*;
use std::sync::mpsc;
use std::time::Duration;

/// Generous bound for a command that must not block at all.
const DEADLINE: Duration = Duration::from_secs(10);

const KG: &str = "guard_reentry";

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
    let setup = handler
        .make_query_job()
        .execute(
            Some(KG.to_string()),
            "+edge[(1, 2), (2, 3)]\n+path(X, Y) <- edge(X, Y)".to_string(),
            None,
        )
        .expect("fixture setup failed");
    assert!(
        setup.errors.is_empty(),
        "fixture errors: {:?}",
        setup.errors
    );
    (handler, tmp)
}

/// Starts a writer on `storage` and returns once it is queued: a writer that
/// holds the write intent makes every new `try_read` fail.
fn queue_writer(storage: Arc<RwLock<StorageEngine>>) {
    let writer = Arc::clone(&storage);
    std::thread::spawn(move || drop(writer.write()));
    let start = std::time::Instant::now();
    while storage.try_read().is_some() {
        assert!(start.elapsed() < DEADLINE, "writer never queued");
        std::thread::yield_now();
    }
}

/// Runs `program` through `QueryJob::execute` with a writer queued between
/// execute's storage read and the dispatch of its meta command.
fn execute_with_queued_writer(handler: &Handler, program: &str) -> QueryResult {
    let job = handler.make_query_job();
    let storage = Arc::clone(&handler.storage);
    let program = program.to_string();
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        test_hook::set(test_hook::Point::MetaDispatch, move || {
            queue_writer(storage);
        });
        let _ = done_tx.send(job.execute(Some(KG.to_string()), program, None));
    });
    done_rx
        .recv_timeout(DEADLINE)
        .expect("command re-acquired the storage read guard and deadlocked")
        .expect("command failed")
}

fn message_text(result: &QueryResult) -> String {
    result
        .rows
        .iter()
        .flat_map(|row| row.values.iter())
        .map(|value| format!("{value:?}"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn why_completes_with_queued_writer() {
    let (handler, _tmp) = handler_with_fixture();
    let result = execute_with_queued_writer(&handler, ".why ?path(1, Y)");
    assert!(result.errors.is_empty(), "errors: {:?}", result.errors);
    assert_eq!(result.proof_trees.map(|trees| trees.len()), Some(1));
}

#[test]
fn why_full_completes_with_queued_writer() {
    let (handler, _tmp) = handler_with_fixture();
    let result = execute_with_queued_writer(&handler, ".why full ?path(X, Y)");
    assert!(result.errors.is_empty(), "errors: {:?}", result.errors);
    assert_eq!(result.proof_trees.map(|trees| trees.len()), Some(2));
}

#[test]
fn why_not_completes_with_queued_writer() {
    let (handler, _tmp) = handler_with_fixture();
    let result = execute_with_queued_writer(&handler, ".why_not path(3, 1)");
    assert!(result.errors.is_empty(), "errors: {:?}", result.errors);
    assert_eq!(result.proof_trees.map(|trees| trees.len()), Some(1));
}

#[test]
fn debug_completes_with_queued_writer() {
    let (handler, _tmp) = handler_with_fixture();
    let result = execute_with_queued_writer(&handler, ".debug ?path(X, Y)");
    assert!(result.errors.is_empty(), "errors: {:?}", result.errors);
    assert!(message_text(&result).contains("Query Plan:"));
}

#[test]
fn index_commands_complete_with_queued_writer() {
    let (handler, _tmp) = handler_with_fixture();
    let list = execute_with_queued_writer(&handler, ".index list");
    assert!(list.errors.is_empty(), "errors: {:?}", list.errors);
    assert!(message_text(&list).contains("No indexes."));

    // Each command reaches its storage call and fails there: the index or
    // vector column does not exist.
    for program in [
        ".index create e_idx on edge(col0) metric cosine",
        ".index stats missing_idx",
        ".index rebuild missing_idx",
        ".index drop missing_idx",
    ] {
        let result = execute_with_queued_writer(&handler, program);
        assert_eq!(result.errors.len(), 1, "{program}: {:?}", result.errors);
        assert!(
            result.errors[0].message.starts_with("Index error:"),
            "{program}: {}",
            result.errors[0].message
        );
    }
}

#[test]
fn failed_proof_reacquires_storage_for_later_statements() {
    let (handler, _tmp) = handler_with_fixture();
    // The target fails to parse only after the guard was released for proof
    // search; the following query must still run.
    let result = execute_with_queued_writer(&handler, ".why_not nonsense\n?edge(X, Y)");
    assert_eq!(result.errors.len(), 1, "errors: {:?}", result.errors);
    assert!(result.errors[0].message.starts_with("Why-not error:"));
    assert_eq!(result.rows.len(), 2);
}
