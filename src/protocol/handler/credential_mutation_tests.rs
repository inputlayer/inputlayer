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
fn orphaned_keys_cannot_rebind_after_recreation_or_bootstrap() {
    for (username, revoke_first) in [("bob", false), ("admin", false), ("admin", true)] {
        for password_change in [true, false] {
            let (handler, _temp) = fixture();
            let config = handler.config().clone();
            let key = handler.create_api_key("old-key", username).unwrap();
            let mut revoked_keys = vec![key];
            let keeper = if username == "admin" {
                handler.handle_user_drop("bob").unwrap();
                let persisted = crate::auth::PersistedCredentials::load(
                    &config.storage.data_dir.join("credentials.toml"),
                )
                .unwrap();
                revoked_keys.push(
                    std::env::var("INPUTLAYER_BOOTSTRAP_API_KEY")
                        .ok()
                        .filter(|key| !key.is_empty())
                        .or(persisted.api_key)
                        .unwrap(),
                );
                None
            } else {
                Some(handler.create_api_key("keeper", "admin").unwrap())
            };
            if revoke_first {
                handler.handle_apikey_revoke("old-key").unwrap();
                handler.handle_apikey_revoke("bootstrap").unwrap();
            }
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
                handler.handle_user_password(username, "new_pw")
            } else {
                handler.handle_user_role(username, "admin")
            };
            assert!(result.is_err());
            assert!(handler.handle_user_create(username, "pw", "admin").is_err());
            let snapshot = handler
                .storage
                .read()
                .get_snapshot_for(INTERNAL_KG)
                .unwrap();
            assert_eq!(
                snapshot.input_tuples["api_keys"]
                    .iter()
                    .any(|tuple| tuple.values()[2].as_str() == Some(username)),
                !revoke_first
            );
            handler
                .storage
                .read()
                .remove_schema_in(INTERNAL_KG, "users")
                .unwrap();
            handler.shutdown();
            drop(handler);

            let handler = Handler::from_config(config.clone()).unwrap();
            handler.bootstrap_auth();
            if username == "bob" {
                handler.handle_user_create(username, "pw", "admin").unwrap();
            }
            for key in &revoked_keys {
                assert!(handler.authenticate_api_key(key).is_err());
            }
            assert!(handler.authenticate_user(username, "pw").is_ok());
            let snapshot = handler
                .storage
                .read()
                .get_snapshot_for(INTERNAL_KG)
                .unwrap();
            assert!(!snapshot
                .input_tuples
                .get("api_keys")
                .into_iter()
                .flatten()
                .any(|tuple| tuple.values()[2].as_str() == Some(username)));
            let new_key = handler.create_api_key("old-key", username).unwrap();
            handler.shutdown();
            drop(handler);

            let handler = Handler::from_config(config).unwrap();
            handler.bootstrap_auth();
            for key in &revoked_keys {
                assert!(handler.authenticate_api_key(key).is_err());
            }
            assert!(handler.authenticate_api_key(&new_key).is_ok());
            if let Some(key) = keeper {
                assert!(handler.authenticate_api_key(&key).is_ok());
            }
        }
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
