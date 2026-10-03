//! Per-statement status: failed statements are reported structurally, never
//! as success message rows.

use inputlayer::protocol::{ErrorCode, Handler, ProgramError, QueryResult, StatementError};
use inputlayer::{Config, StorageEngine};
use tempfile::TempDir;

const MAX_STRING_BYTES: usize = 10;

fn handler() -> (Handler, TempDir) {
    let temp = TempDir::new().expect("create temp dir");
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.max_string_value_bytes = MAX_STRING_BYTES;
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

fn too_long() -> String {
    "a".repeat(MAX_STRING_BYTES + 1)
}

async fn count(handler: &Handler, query: &str) -> usize {
    run(handler, query).await.expect("query").rows.len()
}

#[tokio::test]
async fn oversized_string_insert_is_a_validation_error() {
    let (handler, _tmp) = handler();
    let err = run(&handler, &format!("+r(\"{}\")", too_long()))
        .await
        .expect_err("oversized insert must fail");
    assert_eq!(err.code, Some(ErrorCode::Validation));
    assert!(err.message.starts_with("String value too long"), "{err:?}");

    let err = handler
        .execute_program(None, None, format!("+r(\"{}\")", too_long()), None)
        .await
        .expect_err("execute_program must fail too");
    assert!(err.starts_with("String value too long"), "{err}");
}

#[tokio::test]
async fn second_statement_failure_is_reported_by_index_and_rolls_back_the_program() {
    let (handler, _tmp) = handler();
    let program = format!("+a(1)\n+b(\"{}\")\n+c(3)", too_long());
    let result = run(&handler, &program).await.expect("program result");
    assert_eq!(result.errors.len(), 1, "{:?}", result.errors);
    let StatementError {
        index,
        code,
        message,
    } = &result.errors[0];
    assert_eq!((*index, *code), (1, ErrorCode::Validation));
    assert!(message.starts_with("String value too long"), "{message}");
    assert!(
        message
            .ends_with("(rolled back: none of the program's 3 write statements 0-2 was applied)"),
        "{message}"
    );
    // The program's writes are one transaction: none of them took effect,
    // and none reports success.
    assert_eq!(
        result.rows.len(),
        1,
        "only the failure is reported: {result:?}"
    );
    assert_eq!(count(&handler, "?a(X)").await, 0);
    assert_eq!(count(&handler, "?c(X)").await, 0);
}

#[tokio::test]
async fn failure_before_a_query_survives_the_query_result() {
    let (handler, _tmp) = handler();
    run(&handler, "+r(\"ok\")").await.expect("insert");
    let result = run(&handler, &format!("+r(\"{}\")\n?r(X)", too_long()))
        .await
        .expect("query result");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].index, 0);
    assert_eq!(result.errors[0].code, ErrorCode::Validation);
}

#[tokio::test]
async fn missing_rule_and_relation_are_not_found() {
    let (handler, _tmp) = handler();
    for program in [
        ".rule drop missing",
        ".rule def missing",
        ".rel drop missing",
        ".rel missing",
        ".index drop missing",
        ".index stats missing",
        ".index rebuild missing",
    ] {
        let err = run(&handler, program).await.expect_err(program);
        assert_eq!(err.code, Some(ErrorCode::NotFound), "{program}: {err:?}");
    }
}

#[tokio::test]
async fn dropping_the_current_kg_is_a_conflict() {
    let (handler, _tmp) = handler();
    let err = run(&handler, ".kg drop default")
        .await
        .expect_err("drop current");
    assert_eq!(err.code, Some(ErrorCode::Conflict));
}

#[tokio::test]
async fn statements_of_a_program_without_writes_fail_independently() {
    let (handler, _tmp) = handler();
    let result = run(&handler, "?a(X)\n.rel drop missing\n.index drop missing")
        .await
        .expect("program result");
    let errors: Vec<(usize, ErrorCode)> = result.errors.iter().map(|e| (e.index, e.code)).collect();
    assert_eq!(
        errors,
        vec![(1, ErrorCode::NotFound), (2, ErrorCode::NotFound)]
    );
}

#[tokio::test]
async fn first_failure_of_a_writing_program_rolls_back_all_its_writes() {
    let (handler, _tmp) = handler();
    let result = run(&handler, "+a(1)\n.rule drop missing\n+b(2)")
        .await
        .expect("program result");
    let errors: Vec<(usize, ErrorCode)> = result.errors.iter().map(|e| (e.index, e.code)).collect();
    assert_eq!(errors, vec![(1, ErrorCode::NotFound)]);
    assert!(
        result.errors[0]
            .message
            .ends_with("(rolled back: none of the program's 3 write statements 0-2 was applied)"),
        "{:?}",
        result.errors
    );
    assert_eq!(count(&handler, "?a(X)").await, 0);
    assert_eq!(count(&handler, "?b(X)").await, 0);
}

#[tokio::test]
async fn command_outside_the_transaction_fails_a_writing_program_before_any_write() {
    let (handler, _tmp) = handler();
    run(&handler, "+keep(1)").await.expect("insert");
    let result = run(&handler, "-keep(1)\n+a(1)\n.rel drop keep\n+b(2)")
        .await
        .expect("program result");
    let errors: Vec<(usize, ErrorCode)> = result.errors.iter().map(|e| (e.index, e.code)).collect();
    assert_eq!(errors, vec![(2, ErrorCode::Unsupported)]);
    assert!(
        result.errors[0]
            .message
            .starts_with("'.rel drop keep' cannot run"),
        "{:?}",
        result.errors
    );
    assert_eq!(count(&handler, "?keep(X)").await, 1);
    assert_eq!(count(&handler, "?a(X)").await, 0);
    assert_eq!(count(&handler, "?b(X)").await, 0);
}

#[tokio::test]
async fn successful_statements_report_no_errors() {
    let (handler, _tmp) = handler();
    let result = run(&handler, "+a(1)\n+a(2)\n+ok(X) <- a(X)\n.rule drop ok")
        .await
        .expect("program result");
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    let result = run(&handler, "+a(3)").await.expect("insert");
    assert!(result.errors.is_empty());
}

#[tokio::test]
async fn invalid_rule_mid_program_rolls_back_the_whole_program() {
    let (handler, _tmp) = handler();
    let result = run(&handler, "+a(1)\n+p(X) <- q(Y)\n+c(3)")
        .await
        .expect("program result");
    let errors: Vec<(usize, ErrorCode)> = result.errors.iter().map(|e| (e.index, e.code)).collect();
    assert_eq!(errors, vec![(1, ErrorCode::Validation)]);
    assert_eq!(count(&handler, "?a(X)").await, 0);
    assert_eq!(count(&handler, "?c(X)").await, 0);
}

#[tokio::test]
async fn invalid_rules_alone_are_validation_errors() {
    let (handler, _tmp) = handler();
    run(&handler, "+a(1)").await.expect("insert");
    for program in ["+p(X) <- q(Y)", "__s(X) <- a(X)", "s(X) <- a(Y)"] {
        let err = run(&handler, program).await.expect_err(program);
        assert_eq!(err.code, Some(ErrorCode::Validation), "{program}: {err:?}");
    }
}

#[tokio::test]
async fn failed_query_after_writes_is_listed() {
    let (handler, _tmp) = handler();
    let result = run(&handler, "+a(1)\n?a(X), X > Y")
        .await
        .expect("program result");
    let errors: Vec<(usize, ErrorCode)> = result.errors.iter().map(|e| (e.index, e.code)).collect();
    assert_eq!(errors, vec![(1, ErrorCode::Validation)], "{result:?}");
    assert_eq!(count(&handler, "?a(X)").await, 1);
}

#[tokio::test]
async fn ontology_remove_tolerates_an_already_dropped_rule() {
    let (handler, _tmp) = handler();
    run(
        &handler,
        "+pack_meta(name: string, version: string, digest: string)\n\
         +pack_item(pack: string, kind: string, item: string)\n\
         +pack_meta[(\"p\", \"1.0.0\", \"d\")]\n\
         +pack_item[(\"p\", \"rule\", \"gone\")]",
    )
    .await
    .expect("record pack");
    run(&handler, ".ontology remove p").await.expect("remove");
    assert_eq!(count(&handler, "?pack_item(P, K, I)").await, 0);
    assert_eq!(count(&handler, "?pack_meta(N, V, D)").await, 0);
}

#[tokio::test]
async fn ontology_upgrade_tolerates_an_already_dropped_rule() {
    let (handler, tmp) = handler();
    // No registry: the upgrade must get past dropping old rules and fail
    // only at install.
    std::env::set_var("INPUTLAYER_REGISTRY", tmp.path().join("no-registry"));
    run(
        &handler,
        "+pack_meta(name: string, version: string, digest: string)\n\
         +pack_item(pack: string, kind: string, item: string)\n\
         +a(1)\n\
         +live(X) <- a(X)\n\
         +pack_meta[(\"p\", \"1.0.0\", \"d\")]\n\
         +pack_item[(\"p\", \"rule\", \"gone\"), (\"p\", \"rule\", \"live\")]",
    )
    .await
    .expect("record pack");
    let err = run(&handler, ".ontology upgrade p")
        .await
        .expect_err("install has no registry");
    assert!(err.message.contains("WITHOUT rules"), "{err:?}");
    assert_eq!(count(&handler, "?pack_meta(N, V, D)").await, 0);
}

mod ws {
    use super::{too_long, MAX_STRING_BYTES};
    use futures_util::{SinkExt, StreamExt};
    use inputlayer::protocol::rest::create_router;
    use inputlayer::protocol::Handler;
    use inputlayer::Config;
    use serde_json::{json, Value};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio_tungstenite::tungstenite::Message;

    const PASSWORD: &str = "statement-status-test-pw";

    /// Runs `programs` over `/ws`, returning each `result`/`error` frame.
    async fn execute_all(programs: &[String]) -> Vec<Value> {
        let tmp = TempDir::new().expect("tempdir");
        let mut config = Config::default();
        config.storage.data_dir = tmp.path().join("data");
        config.storage.performance.max_string_value_bytes = MAX_STRING_BYTES;
        config.http.auth.bootstrap_admin_password = Some(PASSWORD.to_string());
        config.http.auth.credentials_file = Some(tmp.path().join("credentials.toml"));
        config.http.rate_limit.ws_max_messages_per_sec = 0;
        config.http.gui.enabled = false;
        let handler = Arc::new(Handler::from_config(config).expect("handler"));
        handler.bootstrap_auth();
        let app = create_router(Arc::clone(&handler), &handler.config().http);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });

        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
            .await
            .expect("connect");
        let mut frames = Vec::new();
        let login = json!({"type": "login", "username": "admin", "password": PASSWORD});
        let requests = std::iter::once(login).chain(
            programs
                .iter()
                .map(|program| json!({"type": "execute", "program": program})),
        );
        for request in requests {
            socket
                .send(Message::Text(request.to_string()))
                .await
                .expect("send");
            loop {
                let frame = tokio::time::timeout(Duration::from_secs(20), socket.next())
                    .await
                    .expect("timed out")
                    .expect("closed")
                    .expect("frame");
                let Message::Text(text) = frame else { continue };
                let value: Value = serde_json::from_str(&text).expect("json");
                if let Some("authenticated" | "result" | "error") = value["type"].as_str() {
                    frames.push(value);
                    break;
                }
            }
        }
        server.abort();
        assert_eq!(frames[0]["type"], "authenticated", "{}", frames[0]);
        frames.split_off(1)
    }

    #[tokio::test]
    async fn statement_failures_reach_the_client_structured() {
        let frames = execute_all(&[
            format!("+r(\"{}\")", too_long()),
            format!("+a(1)\n+b(\"{}\")", too_long()),
            ".rule drop missing".to_string(),
            "+a(2)".to_string(),
            "__s(X) <- a(X)".to_string(),
        ])
        .await;

        assert_eq!(frames[0]["type"], "error", "{}", frames[0]);
        assert_eq!(frames[0]["code"], "validation", "{}", frames[0]);

        assert_eq!(frames[1]["type"], "result", "{}", frames[1]);
        let errors = frames[1]["errors"].as_array().expect("errors");
        assert_eq!(errors.len(), 1, "{}", frames[1]);
        assert_eq!(errors[0]["index"], 1);
        assert_eq!(errors[0]["code"], "validation");

        assert_eq!(frames[2]["type"], "error", "{}", frames[2]);
        assert_eq!(frames[2]["code"], "not_found", "{}", frames[2]);

        assert_eq!(frames[3]["type"], "result", "{}", frames[3]);
        assert_eq!(frames[3]["errors"], json!([]), "{}", frames[3]);

        assert_eq!(frames[4]["type"], "error", "{}", frames[4]);
        assert_eq!(frames[4]["code"], "validation", "{}", frames[4]);
    }
}
