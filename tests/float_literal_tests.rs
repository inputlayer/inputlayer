//! Float literals are finite. JSON has no infinity or NaN, so a persistent
//! rule holding one was stored as `null` and could not be read back: the
//! write-ahead log failed to decode and the engine would not start (found by
//! the `iql_statement` fuzz target, #301).

use inputlayer::protocol::handler::VALIDATION_ERROR_PREFIX;
use inputlayer::protocol::{ErrorCode, Handler, ProgramError, QueryResult};
use inputlayer::{Config, StorageEngine};
use std::path::Path;
use tempfile::TempDir;

fn open(dir: &Path) -> Handler {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    Handler::new(StorageEngine::new(config).expect("create storage engine"))
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

async fn ok(handler: &Handler, program: &str) -> QueryResult {
    let result = run(handler, program)
        .await
        .unwrap_or_else(|e| panic!("{program}: {e:?}"));
    assert!(result.errors.is_empty(), "{program}: {:?}", result.errors);
    result
}

fn assert_refused(result: Result<QueryResult, ProgramError>, program: &str) {
    let err = match result {
        Ok(result) if !result.errors.is_empty() => {
            panic!("{program}: refused at run time, not parse: {result:?}")
        }
        Ok(result) => panic!("{program}: must be refused, got {result:?}"),
        Err(err) => err,
    };
    assert!(
        err.code == Some(ErrorCode::Validation) || err.message.starts_with(VALIDATION_ERROR_PREFIX),
        "{program}: {err:?}"
    );
}

/// Literals that are not finite numbers, in every place a float literal
/// can stand.
const NON_FINITE: &[&str] = &[
    "+p(X) <- r(Y), X = Y + 1e400",
    "+p(X) <- r(Y), X = Y * -1.0e309",
    "+p(X) <- r(Y), X = 1e400",
    "+p(X) <- r(Y), X = - 1e400",
    "+p(X) <- r(Y), Y < 1e999",
    "+p(X) <- r(X), X = inf",
    "+p(X) <- r(X), X = Y + NaN",
    "+p(X) <- r(Y), X = -infinity",
    "+p(X) <- r(Y), X = [1.0, 1e400]",
    "+p(X) <- r(Y), X = [NaN]",
    "+r(1e400)",
    "+r(inf)",
    "+r(- 1e400)",
    "+v([1e400, 0.5])",
    "?r(X), X < 1e400",
    "q(X) <- r(X), X > -1e400",
];

#[tokio::test]
async fn non_finite_float_literals_are_refused() {
    let temp = TempDir::new().expect("create temp dir");
    let handler = open(temp.path());
    ok(&handler, "+r(1)").await;
    for program in NON_FINITE {
        assert_refused(run(&handler, program).await, program);
    }
}

/// The engine restarts after every literal above was offered, with the
/// finite extremes still stored exactly.
#[tokio::test]
async fn rules_with_float_literals_survive_a_restart() {
    let temp = TempDir::new().expect("create temp dir");
    {
        let handler = open(temp.path());
        ok(&handler, "+r(1)").await;
        for program in NON_FINITE {
            let _ = run(&handler, program).await;
        }
        ok(&handler, "+big(X) <- r(Y), X = Y * 1.7976931348623157e308").await;
        ok(&handler, "+tiny(X) <- r(Y), X = Y * 5e-324").await;
        ok(&handler, "+under(X) <- r(Y), X = Y * 1e-400").await;
    }
    let handler = open(temp.path());
    for (relation, expected) in [
        ("big", "Float64(1.7976931348623157e308)"),
        ("tiny", "Float64(5e-324)"),
        ("under", "Float64(0.0)"),
    ] {
        let result = ok(&handler, &format!("?{relation}(X)")).await;
        assert_eq!(result.rows.len(), 1, "{relation}: {result:?}");
        assert_eq!(format!("{:?}", result.rows[0].values[0]), expected);
    }
}
