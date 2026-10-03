#![allow(clippy::unwrap_used)]

use std::time::Duration;

use super::*;

fn user(name: &str, role: Role) -> UserRecord {
    UserRecord {
        username: name.to_string(),
        password_hash: format!("hash-{name}"),
        role,
    }
}

fn key(label: &str, owner: &str) -> ApiKeyRecord {
    ApiKeyRecord {
        label: label.to_string(),
        key_hash: format!("sha-{label}"),
        username: owner.to_string(),
    }
}

fn registry() -> CredentialRegistry {
    let registry = CredentialRegistry::default();
    registry.load(
        vec![user("alice", Role::Editor), user("bob", Role::Viewer)],
        vec![key("a1", "alice"), key("a2", "alice"), key("b1", "bob")],
    );
    registry
}

fn login(registry: &CredentialRegistry, name: &str) -> Principal {
    registry.password_candidate(name).unwrap().accept().unwrap()
}

#[test]
fn revoking_one_key_leaves_other_credentials_live() {
    let registry = registry();
    let a1 = registry.authenticate_key("sha-a1").unwrap();
    let a2 = registry.authenticate_key("sha-a2").unwrap();
    let password = login(&registry, "alice");
    let bob = registry.authenticate_key("sha-b1").unwrap();

    assert!(registry.revoke_key("a1"));

    assert_eq!(a1.identity(), Err(CredentialRevoked));
    assert_eq!(
        registry.authenticate_key("sha-a1").unwrap_err(),
        ApiKeyRejected::Unknown
    );
    for live in [&a2, &password, &bob] {
        assert!(live.identity().is_ok(), "{}", live.credential());
    }
    assert!(!registry.revoke_key("a1"), "already revoked");
}

#[test]
fn password_change_revokes_only_the_old_generation() {
    let registry = registry();
    let old = login(&registry, "alice");
    let in_flight = registry.password_candidate("alice").unwrap();
    let via_key = registry.authenticate_key("sha-a1").unwrap();

    assert!(registry.set_password("alice", "new-hash".to_string()));

    assert!(old.is_revoked());
    assert_eq!(
        in_flight.accept().unwrap_err(),
        CredentialRevoked,
        "a login verified against the old hash must not succeed"
    );
    assert!(via_key.identity().is_ok());
    let candidate = registry.password_candidate("alice").unwrap();
    assert_eq!(candidate.password_hash, "new-hash");
    let new = candidate.accept().unwrap();
    assert_ne!(new.credential(), old.credential());
    assert!(!registry.set_password("nobody", String::new()));
}

#[test]
fn role_changes_reach_every_live_session_of_the_user() {
    let registry = registry();
    let password = login(&registry, "alice");
    let via_key = registry.authenticate_key("sha-a1").unwrap();
    registry.set_password("alice", "rotated".to_string());
    let rotated = login(&registry, "alice");

    assert!(registry.set_role("alice", Role::Admin));

    assert_eq!(via_key.identity().unwrap().role, Role::Admin);
    assert_eq!(rotated.identity().unwrap().role, Role::Admin);
    assert!(password.identity().is_err());
    assert_eq!(
        login(&registry, "bob").identity().unwrap().role,
        Role::Viewer
    );
}

#[test]
fn dropping_a_user_revokes_its_password_and_keys_only() {
    let registry = registry();
    let password = login(&registry, "alice");
    let via_key = registry.authenticate_key("sha-a2").unwrap();
    let bob = login(&registry, "bob");

    registry.remove_user("alice");

    assert!(password.is_revoked() && via_key.is_revoked());
    assert!(registry.password_candidate("alice").is_none());
    assert!(registry.authenticate_key("sha-a2").is_err());
    assert!(bob.identity().is_ok());

    registry.put_user(user("alice", Role::Viewer));
    assert!(
        password.is_revoked(),
        "re-creating the name revives nothing"
    );
}

#[test]
fn keys_resolve_their_owner_when_used() {
    let registry = CredentialRegistry::default();
    registry.put_key(key("early", "carol"));
    assert_eq!(
        registry.authenticate_key("sha-early").unwrap_err(),
        ApiKeyRejected::OwnerNotFound
    );
    registry.put_user(user("carol", Role::Editor));
    let principal = registry.authenticate_key("sha-early").unwrap();
    assert_eq!(principal.username(), "carol");
}

#[test]
fn reused_label_gets_a_new_credential_id() {
    let registry = registry();
    let old = registry.authenticate_key("sha-a1").unwrap();
    registry.revoke_key("a1");
    registry.put_key(ApiKeyRecord {
        key_hash: "sha-a1-v2".to_string(),
        ..key("a1", "alice")
    });
    let new = registry.authenticate_key("sha-a1-v2").unwrap();
    assert_ne!(old.credential(), new.credential());
    assert!(old.is_revoked() && !new.is_revoked());
}

#[test]
fn reload_revokes_everything_previously_issued() {
    let registry = registry();
    let principal = registry.authenticate_key("sha-b1").unwrap();
    registry.load(vec![user("bob", Role::Viewer)], vec![key("b1", "bob")]);
    assert!(principal.is_revoked());
    assert!(registry.authenticate_key("sha-b1").is_ok());
}

#[tokio::test]
async fn revocation_signal_fires_once_revoked_and_after_the_fact() {
    let registry = registry();
    let principal = registry.authenticate_key("sha-a1").unwrap();
    let other = registry.authenticate_key("sha-a2").unwrap();
    let waiter = tokio::spawn(principal.revocation());
    let mut unrelated = other.revocation();
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());

    registry.revoke_key("a1");

    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), principal.revocation())
        .await
        .unwrap();
    assert!(
        futures_util::poll!(&mut unrelated).is_pending(),
        "another key's signal must not fire"
    );
}

#[test]
fn dropped_signals_are_pruned() {
    let registry = registry();
    let principal = registry.authenticate_key("sha-a1").unwrap();
    for _ in 0..100 {
        drop(principal.revocation());
    }
    let _live = principal.revocation();
    assert_eq!(principal.credential.watchers.lock().len(), 1);
}
