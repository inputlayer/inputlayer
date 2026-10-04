use super::*;
use crate::auth::{CredentialEnded, Role, INTERNAL_KG};
use crate::schema::SchemaType;
use crate::{ColumnSchema, RelationSchema};
use std::sync::mpsc;
use std::time::Duration;

fn fixture() -> (Arc<Handler>, tempfile::TempDir) {
    let temp = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.http.auth.bootstrap_admin_password = Some("pw".into());
    let handler = Arc::new(Handler::from_config(config).unwrap());
    handler.bootstrap_auth();
    handler.handle_user_create("bob", "pw", "editor").unwrap();
    handler
        .storage
        .read()
        .create_knowledge_graph("finance")
        .unwrap();
    (handler, temp)
}

/// Runs credential mutation `operation` on a thread; returns its result channel.
fn spawn_mutation(
    handler: &Arc<Handler>,
    operation: usize,
) -> (
    mpsc::Receiver<Result<(), ProgramError>>,
    std::thread::JoinHandle<()>,
) {
    let (done_tx, done_rx) = mpsc::channel();
    let worker = Arc::clone(handler);
    let thread = std::thread::spawn(move || {
        let result = match operation {
            0 => worker.create_api_key("new_key", "bob", None).map(|_| ()),
            1 => worker.handle_apikey_revoke("key").map(|_| ()),
            2 => worker
                .handle_user_create("alice", "pw", "viewer")
                .map(|_| ()),
            3 => worker.handle_user_drop("bob").map(|_| ()),
            4 => worker.handle_user_password("bob", "new_pw").map(|_| ()),
            5 => worker.handle_user_role("bob", "viewer").map(|_| ()),
            _ => worker
                .handle_kg_acl_grant("finance", "bob", "viewer")
                .map(|_| ()),
        };
        done_tx.send(result).unwrap();
    });
    (done_rx, thread)
}

#[test]
fn credential_mutations_are_serialized_with_each_other() {
    for operation in 0..7 {
        let (handler, _temp) = fixture();
        handler.create_api_key("key", "bob", None).unwrap();
        let in_progress = handler.credential_writes.lock();
        let (done_rx, thread) = spawn_mutation(&handler, operation);
        let early = done_rx.recv_timeout(Duration::from_millis(200));
        drop(in_progress);
        assert!(
            early.is_err(),
            "mutation {operation} ran during another credential mutation"
        );
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
        thread.join().unwrap();
    }
}

#[test]
fn credential_mutations_do_not_wait_for_readers() {
    for operation in 0..7 {
        let (handler, _temp) = fixture();
        handler.create_api_key("key", "bob", None).unwrap();
        // A query, proof or replay holding the storage read guard.
        let reader = handler.storage.read();
        let (done_rx, thread) = spawn_mutation(&handler, operation);
        let result = done_rx.recv_timeout(Duration::from_secs(10));
        drop(reader);
        result
            .unwrap_or_else(|_| panic!("mutation {operation} waited for a reader"))
            .unwrap();
        thread.join().unwrap();
    }
}

/// Make every insert into `relation` fail: its schema has the wrong arity.
fn break_relation(handler: &Handler, relation: &str) {
    handler
        .storage
        .read()
        .register_schema_in(
            INTERNAL_KG,
            RelationSchema::new(relation)
                .with_column(ColumnSchema::new("name", SchemaType::String)),
        )
        .unwrap();
}

fn internal_rows(handler: &Handler, relation: &str) -> Vec<Tuple> {
    handler
        .storage
        .read()
        .get_snapshot_for(INTERNAL_KG)
        .unwrap()
        .input_tuples
        .get(relation)
        .map(|rows| rows.iter().cloned().collect())
        .unwrap_or_default()
}

/// Delete `relation` rows directly, as a server from before atomic
/// replacement could leave them.
fn delete_internal_rows(handler: &Handler, relation: &str, matches: impl Fn(&Tuple) -> bool) {
    let rows = internal_rows(handler, relation)
        .into_iter()
        .filter(|t| matches(t))
        .collect();
    handler
        .storage
        .read()
        .delete_tuples_from(INTERNAL_KG, relation, rows)
        .unwrap();
}

fn owned_by(column: usize, username: &str) -> impl Fn(&Tuple) -> bool + '_ {
    move |t| t.values()[column].as_str() == Some(username)
}

fn bootstrap_key(config: &Config) -> String {
    std::env::var("INPUTLAYER_BOOTSTRAP_API_KEY")
        .ok()
        .filter(|key| !key.is_empty())
        .or(crate::auth::PersistedCredentials::load(
            &config.storage.data_dir.join("credentials.toml"),
        )
        .unwrap()
        .api_key)
        .unwrap()
}

fn restart(handler: Arc<Handler>) -> Arc<Handler> {
    let config = handler.config().clone();
    handler.shutdown();
    drop(handler);
    let handler = Arc::new(Handler::from_config(config).unwrap());
    handler.bootstrap_auth();
    handler
}

#[test]
fn failed_user_replacement_changes_nothing() {
    for password_change in [true, false] {
        let (handler, _temp) = fixture();
        let password = handler.authenticate_user("bob", "pw").unwrap();
        let key = handler.create_api_key("key", "bob", None).unwrap();
        let principal = handler.authenticate_api_key(&key).unwrap();
        let users = internal_rows(&handler, "users");
        let keys = internal_rows(&handler, "api_keys");
        break_relation(&handler, "users");
        let result = if password_change {
            handler.handle_user_password("bob", "new_pw")
        } else {
            handler.handle_user_role("bob", "viewer")
        };
        assert!(result
            .unwrap_err()
            .message
            .contains("Insert rejected for 'users'"));
        assert_eq!(internal_rows(&handler, "users"), users);
        assert_eq!(internal_rows(&handler, "api_keys"), keys);
        assert!(password.ended().is_none());
        assert!(principal.ended().is_none());
        assert_eq!(principal.role().unwrap(), Role::Editor);
        assert!(handler.authenticate_api_key(&key).is_ok());
        assert!(handler.authenticate_user("bob", "pw").is_ok());
    }
}

#[test]
fn recreated_user_does_not_inherit_dropped_access() {
    let (handler, _temp) = fixture();
    handler
        .handle_kg_acl_grant("finance", "bob", "owner")
        .unwrap();
    let key = handler.create_api_key("old-key", "bob", None).unwrap();
    handler.handle_user_drop("bob").unwrap();
    handler.handle_user_create("bob", "pw", "viewer").unwrap();
    let inherits_nothing = |handler: &Handler| {
        assert!(handler.authenticate_api_key(&key).is_err());
        assert_eq!(
            handler.get_kg_role_for_user("finance", "bob", &Role::Viewer),
            None
        );
    };
    inherits_nothing(&handler);
    inherits_nothing(&restart(handler));
}

#[test]
fn access_cannot_be_given_to_an_unknown_user() {
    let (handler, _temp) = fixture();
    let grant = handler.handle_kg_acl_grant("finance", "alice", "viewer");
    assert_eq!(grant.unwrap_err().message, "User 'alice' not found");
    let key = handler.create_api_key("alice-key", "alice", None);
    assert_eq!(key.unwrap_err().message, "User 'alice' not found");
    assert!(!internal_rows(&handler, "kg_acls")
        .iter()
        .any(owned_by(1, "alice")));
    assert!(!internal_rows(&handler, "api_keys")
        .iter()
        .any(owned_by(2, "alice")));

    handler.handle_user_create("alice", "pw", "viewer").unwrap();
    handler
        .handle_kg_acl_grant("finance", "alice", "viewer")
        .unwrap();
    let key = handler.create_api_key("alice-key", "alice", None).unwrap();
    assert!(handler.authenticate_api_key(&key).is_ok());
    assert_eq!(
        handler.get_kg_role_for_user("finance", "alice", &Role::Viewer),
        Some(crate::auth::KgRole::Viewer)
    );
}

#[test]
fn bootstrap_key_that_fails_to_store_is_neither_recorded_nor_saved() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    let handler = Handler::from_config(config.clone()).unwrap();
    handler
        .storage
        .read()
        .create_knowledge_graph(INTERNAL_KG)
        .unwrap();
    break_relation(&handler, "api_keys");
    handler.bootstrap_auth();

    let saved =
        crate::auth::PersistedCredentials::load(&temp.path().join("credentials.toml")).unwrap();
    assert_eq!(saved.api_key, None);
    let password = saved.admin_password.unwrap();
    assert!(handler.authenticate_user("admin", &password).is_ok());
    assert!(internal_rows(&handler, crate::auth::stored::BOOTSTRAP_KEYS).is_empty());
    assert!(internal_rows(&handler, crate::auth::stored::API_KEY_TIMES).is_empty());
}

#[test]
fn upgraded_data_dir_never_reissues_a_revoked_bootstrap_key() {
    let (handler, _temp) = fixture();
    let key = bootstrap_key(handler.config());
    delete_internal_rows(&handler, crate::auth::stored::BOOTSTRAP_KEYS, |_| true);
    let handler = restart(handler);
    handler.handle_apikey_revoke("bootstrap").unwrap();
    handler.handle_user_drop("bob").unwrap();
    delete_internal_rows(&handler, "users", owned_by(0, "admin"));

    let handler = restart(handler);
    assert!(handler.authenticate_user("admin", "pw").is_ok());
    assert!(handler.authenticate_api_key(&key).is_err());
    let handler = restart(handler);
    assert!(handler.authenticate_api_key(&key).is_err());
}

#[test]
fn partial_first_boot_issues_a_working_key_on_the_next_boot() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.http.auth.bootstrap_admin_password = Some("pw".into());
    let handler = Handler::from_config(config.clone()).unwrap();
    handler
        .storage
        .read()
        .create_knowledge_graph(INTERNAL_KG)
        .unwrap();
    break_relation(&handler, "users");
    handler.bootstrap_auth();
    assert!(handler.authenticate_user("admin", "pw").is_err());
    handler
        .storage
        .read()
        .remove_schema_in(INTERNAL_KG, "users")
        .unwrap();
    handler.shutdown();
    drop(handler);

    let handler = Handler::from_config(config.clone()).unwrap();
    handler.bootstrap_auth();
    let key = bootstrap_key(&config);
    assert!(handler.authenticate_api_key(&key).is_ok());
    assert!(handler.authenticate_user("admin", "pw").is_ok());
}

#[test]
fn successful_credential_mutations_preserve_live_identity() {
    let (handler, _temp) = fixture();
    let password = handler.authenticate_user("bob", "pw").unwrap();
    let key = handler.create_api_key("key", "bob", None).unwrap();
    let principal = handler.authenticate_api_key(&key).unwrap();
    handler.handle_user_role("bob", "viewer").unwrap();
    assert_eq!(password.role().unwrap(), Role::Viewer);
    assert_eq!(principal.role().unwrap(), Role::Viewer);
    handler.handle_user_password("bob", "new_pw").unwrap();
    assert_eq!(password.ended(), Some(CredentialEnded::Revoked));
    assert!(principal.ended().is_none());
    assert!(handler.authenticate_user("bob", "new_pw").is_ok());
    handler.handle_apikey_revoke("key").unwrap();
    let replacement = handler.create_api_key("key", "bob", None).unwrap();
    assert_eq!(principal.ended(), Some(CredentialEnded::Revoked));
    assert!(handler.authenticate_api_key(&key).is_err());
    assert!(handler.authenticate_api_key(&replacement).is_ok());
    handler.handle_user_drop("bob").unwrap();
    assert!(handler.authenticate_api_key(&replacement).is_err());
}
