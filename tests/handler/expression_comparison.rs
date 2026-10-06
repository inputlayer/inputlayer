//! Comparisons and assignments over function-call expressions, end to end
//! through `Handler`, for session rules, persistent rules and persistent
//! rules reloaded after a restart (IQL1).
//!
//! Two independent defects used to break these rules:
//! - a persistent rule stored its function calls as `_`, so
//!   `C = concat(..)` failed the head-safety check and
//!   `concat(..) != concat(..)` lowered to `Placeholder NotEqual Placeholder`;
//! - lowering rejected any comparison with a function-call operand (and
//!   arithmetic on both sides), and treated `Y = f(X)` with `Y` already bound
//!   as an assignment instead of a filter.

use crate::harness::handler_at;
use inputlayer::protocol::Handler;
use tempfile::TempDir;

async fn exec(handler: &Handler, program: &str) -> inputlayer::protocol::wire::QueryResult {
    handler
        .query_program(None, program.to_string())
        .await
        .unwrap_or_else(|e| panic!("Failed to execute '{program}': {e}"))
}

async fn rows(handler: &Handler, query: &str) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = exec(handler, query)
        .await
        .rows
        .iter()
        .map(|row| row.values.iter().map(|v| format!("{v}")).collect())
        .collect();
    rows.sort();
    rows
}

fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = rows
        .iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect();
    out.sort();
    out
}

/// The GenBI seed-gaps minimal reproducer: two events share
/// (source, sequence, entity) but differ in content.
async fn setup_events(handler: &Handler) {
    exec(
        handler,
        "+adapter_event(source_id: string, event_id: string, sequence: int, \
         entity_key: string, op: string, payload_json: string)",
    )
    .await;
    exec(
        handler,
        r#"+adapter_event[("erp","first",2,"entity","delete","{}"),("erp","conflict",2,"entity","insert","{}")]"#,
    )
    .await;
}

const EVENT_CONTENT: &str = r#"+event_content(S,K,N,Content) <- adapter_event(S,_,N,K,Op,Payload), Content = concat(Op,"|",Payload)"#;
const REPLAY_CONFLICT: &str = r#"+replay_conflict_direct(S,K,N) <- adapter_event(S,_,N,K,OpA,PayloadA), adapter_event(S,_,N,K,OpB,PayloadB), concat(OpA,"|",PayloadA) != concat(OpB,"|",PayloadB)"#;

fn expected_event_content() -> Vec<Vec<String>> {
    table(&[
        &[r#""erp""#, r#""entity""#, "2", r#""delete|{}""#],
        &[r#""erp""#, r#""entity""#, "2", r#""insert|{}""#],
    ])
}

fn expected_replay_conflict() -> Vec<Vec<String>> {
    table(&[&[r#""erp""#, r#""entity""#, "2"]])
}

#[tokio::test]
async fn persistent_rule_binds_a_computed_head_variable() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    setup_events(&handler).await;

    let registered = exec(&handler, EVENT_CONTENT).await;
    assert!(
        format!("{registered:?}").contains("registered"),
        "{registered:?}"
    );
    assert_eq!(
        rows(&handler, "?event_content(S,K,N,C)").await,
        expected_event_content()
    );
}

#[tokio::test]
async fn persistent_rule_compares_two_function_calls() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    setup_events(&handler).await;

    exec(&handler, REPLAY_CONFLICT).await;
    assert_eq!(
        rows(&handler, "?replay_conflict_direct(S,K,N)").await,
        expected_replay_conflict()
    );
}

#[tokio::test]
async fn persistent_function_rules_survive_restart() {
    let temp = TempDir::new().unwrap();
    {
        let handler = handler_at(temp.path());
        setup_events(&handler).await;
        exec(&handler, EVENT_CONTENT).await;
        exec(&handler, REPLAY_CONFLICT).await;
    }
    let handler = handler_at(temp.path());
    assert_eq!(
        rows(&handler, "?event_content(S,K,N,C)").await,
        expected_event_content()
    );
    assert_eq!(
        rows(&handler, "?replay_conflict_direct(S,K,N)").await,
        expected_replay_conflict()
    );
}

#[tokio::test]
async fn session_rule_compares_two_function_calls() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    setup_events(&handler).await;

    // Equal content never conflicts: only the differing pair qualifies.
    assert_eq!(
        rows(
            &handler,
            r#"?adapter_event(S,_,N,K,OpA,PA), adapter_event(S,_,N,K,OpB,PB), concat(OpA,"|",PA) != concat(OpB,"|",PB)"#,
        )
        .await
        .len(),
        2
    );
    assert_eq!(
        rows(
            &handler,
            r#"?adapter_event(S,E,N,K,OpA,PA), adapter_event(S,F,N,K,OpB,PB), concat(OpA,"|",PA) = concat(OpB,"|",PB)"#,
        )
        .await
        .len(),
        2,
        "each event equals only itself"
    );
}

#[tokio::test]
async fn bound_variable_equated_to_function_call_filters() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    exec(&handler, r#"+word[("a", "A"), ("b", "x")]"#).await;

    let only_a = table(&[&[r#""a""#, r#""A""#]]);
    assert_eq!(rows(&handler, "?word(X, Y), Y = upper(X)").await, only_a);
    assert_eq!(rows(&handler, "?word(X, Y), upper(X) = Y").await, only_a);
    exec(&handler, "+upper_pair(X, Y) <- word(X, Y), Y = upper(X)").await;
    assert_eq!(rows(&handler, "?upper_pair(X, Y)").await, only_a);
}

#[tokio::test]
async fn function_call_and_arithmetic_operands_filter() {
    let temp = TempDir::new().unwrap();
    let handler = handler_at(temp.path());
    exec(&handler, r#"+word[("a", "A"), ("bb", "x")]"#).await;
    exec(&handler, "+num[1, 2, 3]").await;

    assert_eq!(
        rows(&handler, r#"?word(X, Y), upper(X) = "A""#).await,
        table(&[&[r#""a""#, r#""A""#]])
    );
    assert_eq!(
        rows(&handler, "?word(X, Y), len(X) < 2").await,
        table(&[&[r#""a""#, r#""A""#]])
    );
    // N + 1 = N * 2 holds only for N = 1.
    assert_eq!(
        rows(&handler, "?num(N), N + 1 != N * 2").await,
        table(&[&["2"], &["3"]])
    );
    assert_eq!(
        rows(&handler, "?num(N), N + 1 = N * 2").await,
        table(&[&["1"]])
    );
}
