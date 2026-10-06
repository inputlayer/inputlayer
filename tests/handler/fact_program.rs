//! Fact statements of a program commit as one transaction (issue #169): every
//! statement takes effect at one revision, or none does; subscribers and a
//! restarted server see only the final state.

use std::sync::Arc;
use std::time::Duration;

use inputlayer::protocol::handler::Notification;
use inputlayer::protocol::{ErrorCode, Handler, ProgramError, QueryResult, WireValue};
use inputlayer::value::Tuple;
use inputlayer::{Config, StorageEngine};
use tempfile::TempDir;

const KG: &str = "default";

fn config(dir: &TempDir) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_path_buf();
    config.storage.persist.durability_mode = inputlayer::config::DurabilityMode::Immediate;
    config
}

fn handler(dir: &TempDir) -> Handler {
    Handler::new(StorageEngine::new(config(dir)).expect("storage"))
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

/// Rows of `query`, rendered and sorted.
async fn rows(handler: &Handler, query: &str) -> Vec<String> {
    let result = run(handler, query).await.expect(query);
    let mut rows: Vec<String> = result
        .rows
        .iter()
        .map(|row| format!("{:?}", row.values))
        .collect();
    rows.sort();
    rows
}

fn messages(result: &QueryResult) -> Vec<String> {
    result
        .rows
        .iter()
        .map(|row| match row.values.as_slice() {
            [WireValue::String(message)] => message.clone(),
            other => panic!("not a message row: {other:?}"),
        })
        .collect()
}

fn errors(result: &QueryResult) -> Vec<(usize, ErrorCode)> {
    result.errors.iter().map(|e| (e.index, e.code)).collect()
}

#[tokio::test]
async fn delete_then_invalid_replacement_keeps_the_old_fact_across_restart() {
    let dir = TempDir::new().unwrap();
    {
        let handler = handler(&dir);
        run(&handler, "+person(name: string, age: int)")
            .await
            .unwrap();
        run(&handler, "+person(\"ann\", 30)").await.unwrap();

        let result = run(
            &handler,
            "-person(\"ann\", 30)\n+person(\"ann\", \"thirty\")",
        )
        .await
        .expect("program result");
        assert_eq!(errors(&result), [(1, ErrorCode::Validation)]);
        let message = &result.errors[0].message;
        assert!(
            message.starts_with("Insert rejected for 'person': "),
            "{message}"
        );
        assert!(message.contains("rolled back"), "{message}");
        // Neither statement reports success.
        assert_eq!(messages(&result), std::slice::from_ref(message));
        assert_eq!(rows(&handler, "?person(N, A)").await.len(), 1);
    }
    let handler = handler(&dir);
    assert_eq!(rows(&handler, "?person(N, A)").await.len(), 1);
}

#[tokio::test]
async fn late_arity_error_rolls_back_every_earlier_statement() {
    let dir = TempDir::new().unwrap();
    let handler = handler(&dir);
    run(&handler, "+e(1, 2)").await.unwrap();

    let result = run(&handler, "-e(1, 2)\n+e(3, 4)\n+f(9)\n+e(5, 6, 7)")
        .await
        .expect("program result");
    assert_eq!(errors(&result), [(3, ErrorCode::Validation)]);
    assert!(result.errors[0]
        .message
        .starts_with("Arity mismatch for relation 'e'"));
    assert_eq!(rows(&handler, "?e(X, Y)").await.len(), 1);
    assert!(rows(&handler, "?f(X)").await.is_empty());
}

#[tokio::test]
async fn single_statement_errors_keep_their_messages_and_codes() {
    let dir = TempDir::new().unwrap();
    let handler = handler(&dir);
    run(&handler, "+e(1, 2)").await.unwrap();
    let err = run(&handler, "+e(5, 6, 7)").await.unwrap_err();
    assert_eq!(err.code, Some(ErrorCode::Validation));
    assert_eq!(
        err.message,
        "Arity mismatch for relation 'e': existing arity is 2, but trying to insert tuples with arity 3"
    );
}

#[tokio::test]
async fn duplicate_facts_are_counted_per_statement() {
    let dir = TempDir::new().unwrap();
    let handler = handler(&dir);
    let result = run(&handler, "+d(1)\n+d(1)\n+d[(2), (2)]\n-d(2)\n-d(2)")
        .await
        .expect("program result");
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    assert_eq!(
        messages(&result),
        [
            "Inserted 1 fact(s) into 'd'.",
            "Inserted 0 fact(s) into 'd'.",
            "Inserted 1 fact(s) into 'd'.",
            "Deleted 1 facts from 'd'.",
            "Deleted 0 facts from 'd'.",
        ]
    );
    assert_eq!(rows(&handler, "?d(X)").await.len(), 1);
}

#[tokio::test]
async fn conditional_delete_and_update_see_earlier_statements() {
    let dir = TempDir::new().unwrap();
    let handler = handler(&dir);
    let result = run(
        &handler,
        "+n(1)\n+n(2)\n+n(3)\n-n(X) <- n(X), X > 2\n\
         +ctr(1, 0)\n-ctr(K, V), +ctr(K, W) <- ctr(K, V), W = V + 1",
    )
    .await
    .expect("program result");
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    assert_eq!(
        messages(&result)[3..],
        [
            "Conditional delete: 1 fact(s) deleted from 'n'.".to_string(),
            "Inserted 1 fact(s) into 'ctr'.".to_string(),
            "Update: 1 deleted, 1 inserted.".to_string(),
        ]
    );
    assert_eq!(rows(&handler, "?n(X)").await.len(), 2);
    assert_eq!(rows(&handler, "?ctr(K, V)").await, ["[Int64(1), Int64(1)]"]);
}

#[tokio::test]
async fn subscribers_get_one_notification_per_relation_after_the_commit() {
    let dir = TempDir::new().unwrap();
    let handler = handler(&dir);
    run(&handler, "+p(1, 10)").await.unwrap();
    let mut notifications = handler.subscribe_notifications();

    run(&handler, "-p(1, 10)\n+p(1, 11)\n+q(5)\n-q(6)")
        .await
        .unwrap();
    let mut updates = Vec::new();
    while let Ok(notification) = notifications.try_recv() {
        if let Notification::PersistentUpdate {
            relation,
            operation,
            count,
            ..
        } = notification
        {
            updates.push((relation, operation, count));
        }
    }
    assert_eq!(
        updates,
        [
            ("p".to_string(), "update".to_string(), 2),
            ("q".to_string(), "insert".to_string(), 1),
        ]
    );

    // Updates name the relation they change.
    run(&handler, "-p(K, V), +p(K, W) <- p(K, V), W = V + 1")
        .await
        .unwrap();
    match notifications.try_recv() {
        Ok(Notification::PersistentUpdate {
            relation,
            operation,
            ..
        }) => assert_eq!((relation.as_str(), operation.as_str()), ("p", "update")),
        other => panic!("expected an update of p, got {other:?}"),
    }

    // A failed program changes nothing and notifies nobody.
    let result = run(&handler, "-p(1, 12)\n+p(1, 2, 3)").await.unwrap();
    assert_eq!(errors(&result), [(1, ErrorCode::Validation)]);
    assert!(notifications.try_recv().is_err());
    assert_eq!(rows(&handler, "?p(K, V)").await, ["[Int64(1), Int64(12)]"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_key_updates_serialize() {
    const WRITERS: usize = 8;
    const ROUNDS: usize = 10;
    let dir = TempDir::new().unwrap();
    let handler = Arc::new(handler(&dir));
    run(&handler, "+ctr(1, 0)").await.unwrap();

    let tasks: Vec<_> = (0..WRITERS)
        .map(|_| {
            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                let mut applied = 0;
                for _ in 0..ROUNDS {
                    match run(&handler, "-ctr(1, V), +ctr(1, W) <- ctr(1, V), W = V + 1").await {
                        Ok(result) => {
                            assert!(result.errors.is_empty(), "{:?}", result.errors);
                            applied += 1;
                        }
                        // Bounded retries ran out: the update was not applied.
                        Err(e) => assert_eq!(e.code, Some(ErrorCode::Conflict), "{e:?}"),
                    }
                }
                applied
            })
        })
        .collect();
    let mut applied = 0;
    for task in tasks {
        applied += task.await.unwrap();
    }

    assert!(applied > 0);
    // Every applied update incremented the counter it read: none was lost
    // and the key never held two values.
    assert_eq!(
        rows(&handler, "?ctr(K, V)").await,
        [format!("[Int64(1), Int64({applied})]")]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_program_applies_nothing() {
    let dir = TempDir::new().unwrap();
    let mut config = config(&dir);
    config.storage.performance.query_timeout_ms = 1;
    let handler = Handler::new(StorageEngine::new(config).expect("storage"));
    let big: Vec<Tuple> = (0..4000).map(|i| Tuple::from_pair(i, i + 1)).collect();
    handler
        .get_storage()
        .insert_tuples_into(KG, "big", big)
        .unwrap();
    let count = || {
        let storage = handler.get_storage();
        let snapshot = storage.get_snapshot_for(KG).unwrap();
        (
            snapshot.input_tuples.get("big").map_or(0, |r| r.len()),
            snapshot.input_tuples.get("marker").map_or(0, |r| r.len()),
        )
    };

    let err = run(&handler, "+marker(1)\n-big(X, Y) <- big(X, Y), big(Y, Z)")
        .await
        .expect_err("the program must time out");
    assert_eq!(
        err.code,
        Some(ErrorCode::DeadlineExceeded),
        "{}",
        err.message
    );
    // The abandoned task stops at its deadline; whatever it reached, it
    // never commits.
    for _ in 0..40 {
        assert_eq!(count(), (4000, 0));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
