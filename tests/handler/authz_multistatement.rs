//! Authorization of multi-statement programs: every statement is checked
//! against the KG in effect at that statement.

// Test setup aborts on failure; `unwrap` is the intended behavior.
#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicU64, Ordering};

use inputlayer::auth::{Principal, Role};
use inputlayer::protocol::wire::{ErrorCode, QueryResult, WireValue};
use inputlayer::protocol::Handler;
use inputlayer::{Config, StorageEngine};
use tempfile::TempDir;

fn admin(handler: &Handler) -> Principal {
    user(handler, "admin")
}

/// A principal for `name`, through a fresh API key.
fn user(handler: &Handler, name: &str) -> Principal {
    static KEYS: AtomicU64 = AtomicU64::new(0);
    let label = format!("test-{}", KEYS.fetch_add(1, Ordering::Relaxed));
    let key = handler.create_api_key(&label, name, None).unwrap();
    handler.authenticate_api_key(&key).unwrap()
}

/// `secret` holds a `creds` row; `public` grants `mallory` (global viewer)
/// and `eve` (global editor) viewer access only.
async fn setup() -> (Handler, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.http.auth.credentials_file = Some(temp.path().join("credentials.toml"));
    let handler = Handler::new(StorageEngine::new(config).unwrap());
    handler.bootstrap_auth().unwrap();
    handler
        .handle_user_create("mallory", "password-mallory", "viewer")
        .unwrap();
    handler
        .handle_user_create("eve", "password-eve", "editor")
        .unwrap();

    let a = admin(&handler);
    for (kg, prog) in [
        ("secret", "+creds[(\"alice\", \"hunter2\")]"),
        ("public", "+pub_data[(1,)]"),
    ] {
        handler.get_storage().create_knowledge_graph(kg).unwrap();
        handler
            .execute_program(None, Some(kg.to_string()), prog.to_string(), Some(&a))
            .await
            .unwrap();
    }
    handler
        .handle_kg_acl_grant("public", "mallory", "viewer")
        .unwrap();
    handler
        .handle_kg_acl_grant("public", "eve", "viewer")
        .unwrap();
    (handler, temp)
}

async fn run(
    handler: &Handler,
    kg: &str,
    program: &str,
    who: &Principal,
) -> Result<QueryResult, inputlayer::protocol::ProgramError> {
    handler
        .execute_program(None, Some(kg.to_string()), program.to_string(), Some(who))
        .await
}

fn assert_denied(result: Result<QueryResult, inputlayer::protocol::ProgramError>) {
    match result {
        Err(e) => {
            assert!(
                e.message.contains("Access denied") || e.message.contains("Permission denied"),
                "unexpected error: {e}"
            );
            assert_eq!(e.code, Some(ErrorCode::AccessDenied), "{e}");
        }
        Ok(r) => panic!("expected denial, got {:?}", r.rows),
    }
}

async fn creds_rows(handler: &Handler) -> usize {
    run(handler, "secret", "?creds(U, P)", &admin(handler))
        .await
        .unwrap()
        .rows
        .len()
}

#[tokio::test]
async fn single_read_without_acl_is_denied() {
    let (h, _t) = setup().await;
    let m = user(&h, "mallory");
    assert_denied(run(&h, "secret", "?creds(U, P)", &m).await);
}

#[tokio::test]
async fn multi_statement_read_without_acl_is_denied() {
    let (h, _t) = setup().await;
    let m = user(&h, "mallory");
    assert_denied(run(&h, "secret", "?creds(U, P)\n?creds(U, P)", &m).await);
}

#[tokio::test]
async fn multi_statement_write_on_viewer_kg_is_denied() {
    let (h, _t) = setup().await;
    let e = user(&h, "eve");
    assert_denied(run(&h, "public", "+pub_data[(2,)]\n?pub_data(X)", &e).await);
    let rows = run(&h, "public", "?pub_data(X)", &admin(&h))
        .await
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1, "write must not have been applied");
}

#[tokio::test]
async fn multi_statement_write_without_acl_is_denied() {
    let (h, _t) = setup().await;
    let m = user(&h, "mallory");
    assert_denied(
        run(
            &h,
            "secret",
            "+creds[(\"mallory\", \"x\")]\n?creds(U, P)",
            &m,
        )
        .await,
    );
    assert_eq!(creds_rows(&h).await, 1);
}

#[tokio::test]
async fn kg_use_switch_to_unauthorized_kg_is_denied() {
    let (h, _t) = setup().await;
    let m = user(&h, "mallory");
    assert_denied(
        run(
            &h,
            "public",
            "?pub_data(X)\n.kg use secret\n?creds(U, P)",
            &m,
        )
        .await,
    );
}

#[tokio::test]
async fn kg_use_switch_to_internal_is_denied() {
    let (h, _t) = setup().await;
    let m = user(&h, "mallory");
    assert_denied(
        run(
            &h,
            "public",
            "?pub_data(X)\n.kg use _internal\n?users(U, H, R)",
            &m,
        )
        .await,
    );
    assert_denied(
        run(
            &h,
            "public",
            "?pub_data(X)\n.kg use _internal\n+users[(\"m2\", \"h\", \"admin\")]",
            &m,
        )
        .await,
    );
}

#[tokio::test]
async fn internal_kg_is_refused_even_for_admin() {
    let (h, _t) = setup().await;
    let a = admin(&h);
    assert!(run(
        &h,
        "public",
        "?pub_data(X)\n.kg use _internal\n?users(U, H, R)",
        &a
    )
    .await
    .is_err());
    assert!(run(&h, "_internal", "?users(U, H, R)", &a).await.is_err());
    assert!(h
        .query_program(Some("_internal".to_string()), "?users(U, H, R)".to_string())
        .await
        .is_err());
    assert!(h
        .query_program(
            Some("public".to_string()),
            "?pub_data(X)\n.kg use _internal\n?users(U, H, R)".to_string()
        )
        .await
        .is_err());
}

#[tokio::test]
async fn later_statement_checked_against_switched_kg() {
    let (h, _t) = setup().await;
    let a = admin(&h);
    run(&h, "secret", ".kg create other", &a).await.unwrap();
    h.handle_kg_acl_grant("other", "mallory", "viewer").unwrap();
    let m = user(&h, "mallory");
    // Allowed on `public`, but `secret` comes after the switch.
    assert_denied(run(&h, "public", ".kg use secret\n?creds(U, P)", &m).await);
    // Both KGs readable: allowed.
    run(&h, "public", "?pub_data(X)\n.kg use other\n.kg", &m)
        .await
        .unwrap();
}

#[tokio::test]
async fn admin_multi_statement_programs_work() {
    let (h, _t) = setup().await;
    let a = admin(&h);
    let r = run(&h, "secret", "+creds[(\"bob\", \"pw\")]\n?creds(U, P)", &a)
        .await
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    run(
        &h,
        "secret",
        "?creds(U, P)\n.kg use public\n?pub_data(X)",
        &a,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn authorized_viewer_multi_statement_read_works() {
    let (h, _t) = setup().await;
    let m = user(&h, "mallory");
    let r = run(&h, "public", "?pub_data(X)\n?pub_data(X)", &m)
        .await
        .unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[tokio::test]
async fn statements_after_switch_are_checked_against_previous_kg_too() {
    let (h, _t) = setup().await;
    // If the switch fails at run time, the write lands on `public`, so it
    // needs write access there too.
    h.get_storage().create_knowledge_graph("ghost").unwrap();
    h.handle_kg_acl_grant("ghost", "eve", "owner").unwrap();
    let e = user(&h, "eve");
    assert_denied(run(&h, "public", ".kg use ghost\n+pub_data[(3,)]", &e).await);
    let rows = run(&h, "public", "?pub_data(X)", &admin(&h))
        .await
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn unparseable_program_without_acl_is_denied() {
    let (h, _t) = setup().await;
    let m = user(&h, "mallory");
    assert_denied(run(&h, "secret", "?creds(U, P)\n?creds(U,", &m).await);
}

async fn run_in_session(
    handler: &Handler,
    sid: &String,
    program: &str,
    who: &Principal,
) -> Result<QueryResult, inputlayer::protocol::ProgramError> {
    handler
        .execute_program(Some(sid), None, program.to_string(), Some(who))
        .await
}

#[tokio::test]
async fn session_bound_switch_is_denied_and_binding_kept() {
    let (h, _t) = setup().await;
    let m = user(&h, "mallory");
    let sid = h.create_session_with_auth("public", &m).unwrap();
    for program in [
        "?pub_data(X)\n.kg use secret\n?creds(U, P)",
        "?pub_data(X)\n.kg use _internal\n?users(U, H, R)",
        ".kg use _internal",
    ] {
        assert_denied(run_in_session(&h, &sid, program, &m).await);
        assert_eq!(h.session_manager().session_kg(&sid).unwrap(), "public");
    }
    let r = run_in_session(&h, &sid, "?pub_data(X)\n?pub_data(X)", &m)
        .await
        .unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[tokio::test]
async fn storage_default_kg_is_checked() {
    let (h, _t) = setup().await;
    let m = user(&h, "mallory");
    let a = admin(&h);
    let no_kg = |program: &str, who| h.execute_program(None, None, program.to_string(), who);
    assert_denied(no_kg("?pub_data(X)\n?pub_data(X)", Some(&m)).await);
    assert_denied(no_kg(".kg use public\n?pub_data(X)", Some(&m)).await);
    no_kg("+d[(1,)]\n?d(X)", Some(&a)).await.unwrap();
}

#[tokio::test]
async fn reaped_session_runs_on_checked_kg() {
    let (h, _t) = setup().await;
    let m = user(&h, "mallory");
    let sid = h.create_session_with_auth("public", &m).unwrap();
    h.close_session(&sid).unwrap();
    // The session's KG is gone; the storage default is checked instead.
    assert_denied(run_in_session(&h, &sid, "?pub_data(X)", &m).await);
    h.handle_kg_acl_grant("default", "mallory", "viewer")
        .unwrap();
    let r = run_in_session(&h, &sid, "?pub_data(X)", &m).await.unwrap();
    assert!(r.rows.is_empty(), "must run on default, not public");
    assert!(run_in_session(&h, &sid, "+d[(1,)]", &admin(&h))
        .await
        .is_err());
}

#[tokio::test]
async fn kg_create_and_drop_must_be_single_statement() {
    let (h, _t) = setup().await;
    let a = admin(&h);
    let e = user(&h, "eve");
    assert!(run(&h, "public", ".kg create fresh\n.kg use public", &e)
        .await
        .is_err());
    assert!(run(&h, "public", ".kg drop secret\n.kg list", &a)
        .await
        .is_err());
    let kgs = h.get_storage().list_knowledge_graphs();
    assert!(!kgs.contains(&"fresh".to_string()));
    assert!(kgs.contains(&"secret".to_string()));

    run(&h, "public", ".kg create fresh", &e).await.unwrap();
    assert!(h
        .get_kg_role_for_user("fresh", "eve", &Role::Editor)
        .is_some());
    run(&h, "public", ".kg drop fresh", &e).await.unwrap();
    assert!(h
        .get_kg_role_for_user("fresh", "eve", &Role::Editor)
        .is_none());
}

/// `program` on `kg` as `who` is refused with exactly `message` and the code
/// `access_denied`.
async fn assert_refused(
    handler: &Handler,
    who: &Principal,
    kg: &str,
    program: &str,
    message: &str,
) {
    let error = run(handler, kg, program, who)
        .await
        .expect_err(&format!("{program} on {kg} was not refused"));
    assert_eq!(error.message, message, "{program} on {kg}");
    assert_eq!(
        error.code,
        Some(ErrorCode::AccessDenied),
        "{program} on {kg}: {error}"
    );
}

/// A principal for the key `.apikey create {args}` returns.
async fn scoped_key(handler: &Handler, args: &str) -> Principal {
    let created = run(
        handler,
        "public",
        &format!(".apikey create {args}"),
        &admin(handler),
    )
    .await
    .unwrap();
    match &created.rows[0].values[1] {
        WireValue::String(key) => handler.authenticate_api_key(key).unwrap(),
        other => panic!("expected the key, got {other:?}"),
    }
}

#[tokio::test]
async fn global_role_refusals_carry_access_denied() {
    let (h, _t) = setup().await;
    let viewer = user(&h, "mallory");
    for (program, message) in [
        (
            ".kg create mine",
            "Permission denied: viewers cannot create knowledge graphs",
        ),
        (
            ".ontology install core",
            "Permission denied: viewers cannot manage ontologies",
        ),
    ] {
        assert_refused(&h, &viewer, "public", program, message).await;
    }
}

#[tokio::test]
async fn admin_only_commands_carry_access_denied() {
    let (h, _t) = setup().await;
    let editor = user(&h, "eve");
    for (program, message) in [
        (".compact", "Permission denied: only admins can compact"),
        (
            ".backup",
            "Permission denied: only admins can back up the server",
        ),
        (
            ".backup status",
            "Permission denied: only admins can back up the server",
        ),
        (
            ".user list",
            "Permission denied: only admins can manage users",
        ),
        (
            ".user create zed password-zed viewer",
            "Permission denied: only admins can manage users",
        ),
        (
            ".apikey list",
            "Permission denied: only admins can manage API keys",
        ),
        (
            ".apikey create mine",
            "Permission denied: only admins can manage API keys",
        ),
    ] {
        assert_refused(&h, &editor, "public", program, message).await;
    }
}

#[tokio::test]
async fn knowledge_graph_role_refusals_carry_access_denied() {
    let (h, _t) = setup().await;
    let eve = user(&h, "eve");
    // No role on the knowledge graph, and the system knowledge graph.
    assert_refused(&h, &eve, "secret", "?creds(U, P)", "Access denied").await;
    assert_refused(
        &h,
        &eve,
        "public",
        ".kg use _internal",
        "Access denied: '_internal' is a system knowledge graph",
    )
    .await;
    // Viewer of `public`.
    for program in ["+pub_data[(2,)]", "+pub_data(x: int)", ".rel drop pub_data"] {
        assert_refused(
            &h,
            &eve,
            "public",
            program,
            "Permission denied: you have viewer access to this knowledge graph",
        )
        .await;
    }
    // Editor of `secret`: owners alone drop it and manage its access.
    h.handle_kg_acl_grant("secret", "eve", "editor").unwrap();
    let eve = user(&h, "eve");
    assert_refused(
        &h,
        &eve,
        "secret",
        ".kg drop secret",
        "Permission denied: only KG owners can drop this knowledge graph",
    )
    .await;
    for program in [
        ".kg acl grant secret mallory viewer",
        ".kg acl revoke secret mallory",
    ] {
        assert_refused(
            &h,
            &eve,
            "secret",
            program,
            "Permission denied: only KG owners can manage ACLs",
        )
        .await;
    }
    assert_eq!(creds_rows(&h).await, 1);
}

#[tokio::test]
async fn write_grant_and_key_scope_refusals_carry_access_denied() {
    let (h, _t) = setup().await;
    let agent = scoped_key(&h, "agent role decider on public relations decision").await;
    for (kg, program, message) in [
        (
            "public",
            "+pub_data[(2,)]",
            "Permission denied: the decider role on 'public' has no write grant for relation \
             'pub_data'",
        ),
        (
            "public",
            "+open(X) <- pub_data(X)",
            "Permission denied: registering a rule needs the editor role on 'public'; the \
             decider role may only write facts",
        ),
        (
            "public",
            ".kg drop public",
            "Permission denied: only owners of 'public' can drop it or manage its access",
        ),
        (
            "public",
            ".kg create mine",
            "Permission denied: this API key is scoped to knowledge graph 'public' and cannot \
             create one",
        ),
        (
            "public",
            ".kg use secret",
            "Access denied: this API key is scoped to knowledge graph 'public'",
        ),
        (
            "secret",
            "?creds(U, P)",
            "Access denied: this API key is scoped to knowledge graph 'public'",
        ),
        (
            "public",
            ".compact",
            "Permission denied: only admins can compact",
        ),
    ] {
        assert_refused(&h, &agent, kg, program, message).await;
    }
    // What it was granted still works.
    run(&h, "public", "+decision[(1,)]", &agent).await.unwrap();
}

#[tokio::test]
async fn a_revoked_credential_is_refused_with_access_denied() {
    let (h, _t) = setup().await;
    let key = h.create_api_key("short-lived", "eve", None).unwrap();
    let eve = h.authenticate_api_key(&key).unwrap();
    run(&h, "public", "?pub_data(X)", &eve).await.unwrap();
    run(&h, "public", ".apikey revoke short-lived", &admin(&h))
        .await
        .unwrap();
    assert_refused(
        &h,
        &eve,
        "public",
        "?pub_data(X)",
        "Access denied: credential revoked",
    )
    .await;
}
