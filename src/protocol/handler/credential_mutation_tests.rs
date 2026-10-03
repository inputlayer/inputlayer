use super::*;
use crate::auth::{Role, INTERNAL_KG};
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
    (handler, temp)
}

#[test]
fn credential_mutations_wait_for_exclusive_storage_access() {
    for operation in 0..6 {
        let (handler, _temp) = fixture();
        handler.create_api_key("key", "bob").unwrap();
        let snapshot_guard = handler.storage.read();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = Arc::clone(&handler);
        let thread = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = match operation {
                0 => worker.create_api_key("new_key", "bob").map(|_| ()),
                1 => worker.handle_apikey_revoke("key").map(|_| ()),
                2 => worker
                    .handle_user_create("alice", "pw", "viewer")
                    .map(|_| ()),
                3 => worker.handle_user_drop("bob").map(|_| ()),
                4 => worker.handle_user_password("bob", "new_pw").map(|_| ()),
                _ => worker.handle_user_role("bob", "viewer").map(|_| ()),
            };
            done_tx.send(result).unwrap();
        });
        started_rx.recv().unwrap();
        let early = done_rx.recv_timeout(Duration::from_millis(200));
        drop(snapshot_guard);
        assert!(
            early.is_err(),
            "mutation {operation} bypassed exclusive access"
        );
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
        thread.join().unwrap();
    }
}

#[test]
fn failed_user_replacement_revokes_password_and_keys() {
    for password_change in [true, false] {
        let (handler, _temp) = fixture();
        let password = handler.authenticate_user("bob", "pw").unwrap();
        let key = handler.create_api_key("key", "bob").unwrap();
        let principal = handler.authenticate_api_key(&key).unwrap();
        handler
            .storage
            .read()
            .register_schema_in(
                INTERNAL_KG,
                RelationSchema::new("users")
                    .with_column(ColumnSchema::new("name", SchemaType::String)),
            )
            .unwrap();
        let result = if password_change {
            handler.handle_user_password("bob", "new_pw")
        } else {
            handler.handle_user_role("bob", "viewer")
        };
        assert!(result.unwrap_err().contains("Insert rejected for 'users'"));
        assert!(password.is_revoked());
        assert!(principal.is_revoked());
        assert!(handler.authenticate_api_key(&key).is_err());
        assert!(handler.authenticate_user("bob", "pw").is_err());
        assert!(!handler
            .storage
            .read()
            .get_snapshot_for(INTERNAL_KG)
            .unwrap()
            .input_tuples["users"]
            .iter()
            .any(|tuple| tuple.values()[0].as_str() == Some("bob")));
    }
}

#[test]
fn successful_credential_mutations_preserve_live_identity() {
    let (handler, _temp) = fixture();
    let password = handler.authenticate_user("bob", "pw").unwrap();
    let key = handler.create_api_key("key", "bob").unwrap();
    let principal = handler.authenticate_api_key(&key).unwrap();
    handler.handle_user_role("bob", "viewer").unwrap();
    assert_eq!(password.role().unwrap(), Role::Viewer);
    assert_eq!(principal.role().unwrap(), Role::Viewer);
    handler.handle_user_password("bob", "new_pw").unwrap();
    assert!(password.is_revoked());
    assert!(!principal.is_revoked());
    assert!(handler.authenticate_user("bob", "new_pw").is_ok());
    handler.handle_apikey_revoke("key").unwrap();
    let replacement = handler.create_api_key("key", "bob").unwrap();
    assert!(principal.is_revoked());
    assert!(handler.authenticate_api_key(&key).is_err());
    assert!(handler.authenticate_api_key(&replacement).is_ok());
    handler.handle_user_drop("bob").unwrap();
    assert!(handler.authenticate_api_key(&replacement).is_err());
}
