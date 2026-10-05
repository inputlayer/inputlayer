//! Scoped write roles end to end: `writer` and `decider` grants and API keys
//! created with a role write the facts they were granted and nothing else,
//! never policy, on every path a program takes, and across a restart.

use super::*;
use crate::auth::{Principal, INTERNAL_KG};

const KG: &str = "shop";

/// A handler with KG `shop` holding a kill-switched policy: `eligible` is
/// empty while `kill_switch("o1")` holds.
async fn fixture() -> (Arc<Handler>, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    config.http.auth.bootstrap_admin_password = Some("admin-password".to_string());
    config.http.auth.credentials_file = Some(tmp.path().join("credentials.toml"));
    let handler = Arc::new(Handler::from_config(config).unwrap());
    handler.bootstrap_auth().unwrap();
    for kg in [KG, "other"] {
        handler.storage.read().create_knowledge_graph(kg).unwrap();
    }
    admin(
        &handler,
        "+order[(\"o1\")]\n\
         +kill_switch[(\"o1\")]\n\
         +eligible(O) <- order(O), !kill_switch(O)",
    )
    .await
    .unwrap();
    (handler, tmp)
}

fn restart(handler: Arc<Handler>) -> Arc<Handler> {
    let config = handler.config().clone();
    handler.shutdown();
    drop(handler);
    let handler = Arc::new(Handler::from_config(config).unwrap());
    handler.bootstrap_auth().unwrap();
    handler
}

async fn admin(handler: &Handler, program: &str) -> Result<QueryResult, String> {
    let result = handler
        .execute_program(None, Some(KG.to_string()), program.to_string(), None)
        .await
        .map_err(|e| e.message)?;
    match result.errors.first() {
        Some(error) => Err(error.message.clone()),
        None => Ok(result),
    }
}

async fn run_on(
    handler: &Handler,
    principal: &Principal,
    kg: &str,
    program: &str,
) -> Result<QueryResult, String> {
    let result = handler
        .execute_program(
            None,
            Some(kg.to_string()),
            program.to_string(),
            Some(principal),
        )
        .await
        .map_err(|e| e.message)?;
    match result.errors.first() {
        Some(error) => Err(error.message.clone()),
        None => Ok(result),
    }
}

async fn run(
    handler: &Handler,
    principal: &Principal,
    program: &str,
) -> Result<QueryResult, String> {
    run_on(handler, principal, KG, program).await
}

/// `.apikey create` as admin; returns the plaintext key.
async fn create_key(handler: &Handler, args: &str) -> String {
    let created = admin(handler, &format!(".apikey create {args}"))
        .await
        .unwrap();
    match &created.rows[0].values[1] {
        WireValue::String(key) => key.clone(),
        other => panic!("expected the key, got {other:?}"),
    }
}

async fn rows(handler: &Handler, query: &str) -> usize {
    admin(handler, query).await.unwrap().rows.len()
}

/// The review's three bypasses with an agent credential, plus a schema change:
/// each refused naming what is missing, and the policy still holds.
async fn assert_cannot_change_policy(handler: &Handler, principal: &Principal) {
    for (program, why) in [
        (
            "+eligible(O) <- order(O)",
            "registering a rule needs the editor role on 'shop'",
        ),
        (
            "-kill_switch(\"o1\")",
            "has no write grant for relation 'kill_switch'",
        ),
        (
            ".rel drop kill_switch",
            "dropping a relation needs the editor role on 'shop'",
        ),
        (
            ".rule drop eligible",
            "changing a rule needs the editor role on 'shop'",
        ),
        (
            "+kill_switch(name: string)",
            "declaring a schema needs the editor role on 'shop'",
        ),
    ] {
        let refused = run(handler, principal, program).await.unwrap_err();
        assert!(refused.contains(why), "{program}: {refused}");
    }
    assert_eq!(rows(handler, "?kill_switch(X)").await, 1);
    assert_eq!(rows(handler, "?eligible(O)").await, 0);
}

#[tokio::test]
async fn decider_key_writes_only_its_relations() {
    let (handler, _tmp) = fixture().await;
    let key = create_key(
        &handler,
        "agent role decider on shop relations attempt, decision",
    )
    .await;
    let agent = handler.authenticate_api_key(&key).unwrap();
    // Stored where a server from before scopes never reads it as a key.
    let snapshot = handler
        .storage
        .read()
        .get_snapshot_for(INTERNAL_KG)
        .unwrap();
    let labelled = |relation: &str| {
        snapshot.input_tuples.get(relation).is_some_and(|rows| {
            rows.iter()
                .any(|row| row.values()[0].as_str() == Some("agent"))
        })
    };
    assert!(labelled(crate::auth::stored::SCOPED_API_KEYS));
    assert!(!labelled(crate::auth::stored::API_KEYS));

    // Its own relations: a guarded claim, a decision, a release.
    run(
        &handler,
        &agent,
        "-attempt(\"\", -1), +attempt(O, 1) <- order(O), O = \"o1\"",
    )
    .await
    .unwrap();
    assert_eq!(rows(&handler, "?attempt(O, N)").await, 1);
    run(&handler, &agent, "+decision[(\"d1\", \"o1\")]")
        .await
        .unwrap();
    run(&handler, &agent, "-attempt(\"o1\", 1)").await.unwrap();
    assert_eq!(rows(&handler, "?decision(D, O)").await, 1);
    // It reads everything on its KG.
    assert_eq!(
        run(&handler, &agent, "?order(O)").await.unwrap().rows.len(),
        1
    );

    assert_cannot_change_policy(&handler, &agent).await;
    let refused = run(&handler, &agent, "+order(\"o2\")").await.unwrap_err();
    assert!(refused.contains("decider role on 'shop' has no write grant for relation 'order'"));

    // Nothing outside its KG, and nothing system-wide.
    let elsewhere = run_on(&handler, &agent, "other", "?order(O)")
        .await
        .unwrap_err();
    assert!(
        elsewhere.contains("scoped to knowledge graph 'shop'"),
        "{elsewhere}"
    );
    let switched = run(&handler, &agent, ".kg use other").await.unwrap_err();
    assert!(
        switched.contains("scoped to knowledge graph 'shop'"),
        "{switched}"
    );
    let created = run(&handler, &agent, ".kg create mine").await.unwrap_err();
    assert!(created.contains("cannot create one"), "{created}");
    for program in [".apikey list", ".user list", ".kg drop shop", ".compact"] {
        assert!(run(&handler, &agent, program).await.is_err(), "{program}");
    }
    assert!(handler.create_session_with_auth("other", &agent).is_err());
    assert!(handler.create_session_with_auth(KG, &agent).is_ok());

    let listed = admin(&handler, ".apikey list").await.unwrap();
    let scope = listed
        .schema
        .iter()
        .position(|c| c.name == "scope")
        .unwrap();
    assert_eq!(
        listed.rows.iter().find_map(|row| match &row.values[scope] {
            WireValue::String(scope) => Some(scope.clone()),
            _ => None,
        }),
        Some("decider on shop (relations attempt, decision)".to_string())
    );
}

#[tokio::test]
async fn scoped_key_lists_only_its_own_kgs_acl() {
    let (handler, _tmp) = fixture().await;
    handler
        .handle_user_create("bob", "bob-password", "viewer")
        .unwrap();
    admin(&handler, ".kg acl grant other bob editor")
        .await
        .unwrap();
    let key = create_key(&handler, "agent role decider on shop relations attempt").await;
    let agent = handler.authenticate_api_key(&key).unwrap();

    for (kg, program) in [("other", ".kg acl list"), (KG, ".kg acl list other")] {
        let refused = run_on(&handler, &agent, kg, program).await.unwrap_err();
        assert!(
            refused.contains("scoped to knowledge graph 'shop'"),
            "{kg}: {program}: {refused}"
        );
    }
    let listed = run(&handler, &agent, ".kg acl list").await.unwrap();
    let WireValue::String(listed) = &listed.rows[0].values[0] else {
        panic!("expected a message, got {listed:?}");
    };
    assert!(listed.contains("'shop'"), "{listed}");
    assert!(!listed.contains("bob"), "{listed}");
}

/// `.status` as `principal` on `kg`, its lines joined.
async fn status(handler: &Handler, principal: Option<&Principal>, kg: &str) -> String {
    let result = handler
        .execute_program(None, Some(kg.to_string()), ".status".to_string(), principal)
        .await
        .unwrap();
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    result
        .rows
        .iter()
        .map(|row| match &row.values[0] {
            WireValue::String(line) => line.clone(),
            other => panic!("expected a message, got {other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn status_shows_only_the_knowledge_graphs_a_caller_can_see() {
    let (handler, _tmp) = fixture().await;
    handler
        .handle_user_create("bob", "bob-password", "viewer")
        .unwrap();
    admin(&handler, ".kg acl grant other bob viewer")
        .await
        .unwrap();
    let bob = handler.authenticate_user("bob", "bob-password").unwrap();
    let key = create_key(&handler, "reader role viewer on shop").await;
    let reader = handler.authenticate_api_key(&key).unwrap();
    let root = handler
        .authenticate_user("admin", "admin-password")
        .unwrap();
    let all = handler.get_storage().list_knowledge_graphs().len();
    assert!(all > 2, "{all}");

    for (who, principal, kg) in [("bob", &bob, "other"), ("scoped viewer key", &reader, KG)] {
        let shown = status(&handler, Some(principal), kg).await;
        assert!(shown.contains("Knowledge graphs: 1"), "{who}: {shown}");
        assert!(!shown.contains("Total queries"), "{who}: {shown}");
    }
    for shown in [
        status(&handler, Some(&root), KG).await,
        status(&handler, None, KG).await,
    ] {
        assert!(
            shown.contains(&format!("Knowledge graphs: {all}")),
            "{shown}"
        );
        assert!(shown.contains("Total queries: "), "{shown}");
    }
}

#[tokio::test]
async fn writer_key_writes_any_facts_but_no_policy() {
    let (handler, _tmp) = fixture().await;
    let key = create_key(&handler, "ingest role writer on shop").await;
    let writer = handler.authenticate_api_key(&key).unwrap();

    run(&handler, &writer, "+order(\"o2\")\n+eta[(\"o2\", 3)]")
        .await
        .unwrap();
    assert_eq!(rows(&handler, "?order(O)").await, 2);
    for (program, why) in [
        ("+eligible(O) <- order(O)", "registering a rule"),
        (".rel drop kill_switch", "dropping a relation"),
        ("-kill_switch", "dropping a relation or rule"),
        (".clear prefix kill", "clearing relations"),
    ] {
        let refused = run(&handler, &writer, program).await.unwrap_err();
        assert!(refused.contains(why), "{program}: {refused}");
    }
    assert_eq!(rows(&handler, "?eligible(O)").await, 1);
    // A writer may flip the switch: it is a fact. Grants are what keep an
    // agent off it.
    run(&handler, &writer, "-kill_switch(\"o1\")")
        .await
        .unwrap();
    assert_eq!(rows(&handler, "?eligible(O)").await, 2);
}

#[tokio::test]
async fn acl_grants_carry_relations_and_survive_restart() {
    let (handler, _tmp) = fixture().await;
    handler
        .handle_user_create("agent", "agent-password", "viewer")
        .unwrap();
    admin(
        &handler,
        ".kg acl grant shop agent decider relations attempt",
    )
    .await
    .unwrap();
    let key = handler.create_api_key("agent-key", "agent", None).unwrap();

    let listed = admin(&handler, ".kg acl list shop").await.unwrap();
    assert!(
        format!("{:?}", listed.rows).contains("agent: decider (relations attempt)"),
        "{:?}",
        listed.rows
    );

    let handler = restart(handler);
    let agent = handler.authenticate_api_key(&key).unwrap();
    run(&handler, &agent, "+attempt(\"o1\", 1)").await.unwrap();
    assert_cannot_change_policy(&handler, &agent).await;
    let password = handler
        .authenticate_user("agent", "agent-password")
        .unwrap();
    assert!(run(&handler, &password, "+decision(\"d1\", \"o1\")")
        .await
        .is_err());

    // A new grant replaces the old one with its relations.
    admin(&handler, ".kg acl grant shop agent writer")
        .await
        .unwrap();
    run(&handler, &agent, "+order(\"o3\")").await.unwrap();
    admin(
        &handler,
        ".kg acl grant shop agent decider relations decision",
    )
    .await
    .unwrap();
    assert!(run(&handler, &agent, "+attempt(\"o1\", 2)").await.is_err());
    run(&handler, &agent, "+decision(\"d1\", \"o1\")")
        .await
        .unwrap();

    // Revoking removes the relations with the grant.
    admin(&handler, ".kg acl revoke shop agent").await.unwrap();
    assert!(run(&handler, &agent, "?order(O)").await.is_err());
    let snapshot = handler
        .storage
        .read()
        .get_snapshot_for(INTERNAL_KG)
        .unwrap();
    assert!(snapshot
        .input_tuples
        .get(KG_ACL_RELATIONS)
        .is_none_or(|rows| rows.is_empty()));
}

#[tokio::test]
async fn grants_refuse_relation_lists_that_do_not_fit_the_role() {
    let (handler, _tmp) = fixture().await;
    handler
        .handle_user_create("agent", "agent-password", "viewer")
        .unwrap();
    for (program, why) in [
        (
            ".kg acl grant shop agent decider",
            "needs the relations it may write",
        ),
        (
            ".kg acl grant shop agent editor relations attempt",
            "Only the writer and decider",
        ),
        (
            ".apikey create k role owner on shop",
            "cannot carry the owner role",
        ),
        (
            ".apikey create k role writer on nowhere",
            "Knowledge graph 'nowhere' not found",
        ),
    ] {
        let refused = admin(&handler, program).await.unwrap_err();
        assert!(refused.contains(why), "{program}: {refused}");
    }
}

#[tokio::test]
async fn a_scope_never_widens_its_owner() {
    let (handler, _tmp) = fixture().await;
    handler
        .handle_user_create("bob", "bob-password", "editor")
        .unwrap();
    handler.handle_kg_acl_grant(KG, "bob", "viewer").unwrap();
    let writer = crate::auth::KeyScope::new(KG, crate::auth::KgRole::Writer.into()).unwrap();
    let created = handler
        .handle_apikey_create("bob-writer", "bob", None, Some(writer))
        .unwrap();
    let WireValue::String(key) = &created.rows[0].values[1] else {
        panic!("expected the key");
    };
    let bob = handler.authenticate_api_key(key).unwrap();
    assert_eq!(
        run(&handler, &bob, "?order(O)").await.unwrap().rows.len(),
        1
    );
    let refused = run(&handler, &bob, "+order(\"o2\")").await.unwrap_err();
    assert!(refused.contains("viewer access"), "{refused}");

    // An admin owner's scoped key is not an admin.
    let key = create_key(&handler, "reader role viewer on shop").await;
    let reader = handler.authenticate_api_key(&key).unwrap();
    assert_eq!(reader.role(), Ok(crate::auth::Role::Editor));
    assert!(run(&handler, &reader, "+order(\"o2\")").await.is_err());
    assert!(run(&handler, &reader, ".user list").await.is_err());
}

#[tokio::test]
async fn scoped_keys_survive_restart_and_end_with_their_kg() {
    let (handler, _tmp) = fixture().await;
    let key = create_key(&handler, "agent role decider on shop relations attempt").await;
    let other = create_key(&handler, "elsewhere role writer on other").await;

    let handler = restart(handler);
    let agent = handler.authenticate_api_key(&key).unwrap();
    run(&handler, &agent, "+attempt(\"o1\", 1)").await.unwrap();
    assert_cannot_change_policy(&handler, &agent).await;

    for program in [".kg drop shop", ".kg create shop"] {
        let result = handler
            .execute_program(None, Some("other".to_string()), program.to_string(), None)
            .await
            .unwrap();
        assert!(result.errors.is_empty(), "{program}: {:?}", result.errors);
        assert!(
            agent.ended().is_some(),
            "a dropped KG's scoped keys are revoked"
        );
    }
    assert!(handler.authenticate_api_key(&key).is_err());
    let handler = restart(handler);
    assert!(handler.authenticate_api_key(&key).is_err());
    assert!(handler.authenticate_api_key(&other).is_ok());
}

#[tokio::test]
async fn an_unreadable_stored_scope_disables_its_key() {
    let (handler, _tmp) = fixture().await;
    let key = create_key(&handler, "agent role decider on shop relations attempt").await;
    let hash = crate::auth::hash_api_key(&key);
    // A second, wider scope row for the same key, as corruption could leave.
    handler
        .storage
        .read()
        .insert_tuples_into(
            INTERNAL_KG,
            crate::auth::stored::API_KEY_SCOPES,
            vec![Tuple::new(vec![
                Value::string(&hash),
                Value::string(KG),
                Value::string("writer"),
                Value::string("*"),
            ])],
        )
        .unwrap();
    let handler = restart(handler);
    assert!(handler.authenticate_api_key(&key).is_err());
}
