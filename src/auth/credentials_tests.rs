#![allow(clippy::unwrap_used)]

use std::time::Duration;

use super::*;
use crate::auth::principal::now_ms;

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
        times: ApiKeyTimes::default(),
    }
}

/// A key for `owner` that expires `in_ms` from now (negative: already has).
fn expiring_key(label: &str, owner: &str, in_ms: i64) -> ApiKeyRecord {
    ApiKeyRecord {
        times: ApiKeyTimes {
            expires_at: Some(now_ms().saturating_add_signed(in_ms)),
            ..ApiKeyTimes::default()
        },
        ..key(label, owner)
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

    assert_eq!(a1.identity(), Err(CredentialEnded::Revoked));
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

    assert_eq!(old.ended(), Some(CredentialEnded::Revoked));
    assert_eq!(
        in_flight.accept().unwrap_err(),
        CredentialEnded::Revoked,
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

    assert!(password.ended().is_some() && via_key.ended().is_some());
    assert!(registry.password_candidate("alice").is_none());
    assert!(registry.authenticate_key("sha-a2").is_err());
    assert!(bob.identity().is_ok());

    registry.put_user(user("alice", Role::Viewer));
    assert!(
        password.ended().is_some(),
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
    assert!(old.ended().is_some() && new.ended().is_none());
}

#[test]
fn reload_revokes_everything_previously_issued() {
    let registry = registry();
    let principal = registry.authenticate_key("sha-b1").unwrap();
    registry.load(vec![user("bob", Role::Viewer)], vec![key("b1", "bob")]);
    assert!(principal.ended().is_some());
    assert!(registry.authenticate_key("sha-b1").is_ok());
}

#[tokio::test]
async fn revocation_signal_fires_once_revoked_and_after_the_fact() {
    let registry = registry();
    let principal = registry.authenticate_key("sha-a1").unwrap();
    let other = registry.authenticate_key("sha-a2").unwrap();
    let waiter = tokio::spawn(principal.end_signal());
    let mut unrelated = other.end_signal();
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());

    registry.revoke_key("a1");

    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
    let mut fired = principal.end_signal();
    tokio::time::timeout(Duration::from_secs(5), &mut fired)
        .await
        .unwrap();
    assert!(
        futures_util::poll!(&mut fired).is_ready(),
        "a fired signal stays fired"
    );
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
        drop(principal.end_signal());
    }
    let _live = principal.end_signal();
    assert_eq!(principal.watchers(), 1);
}

#[test]
fn an_expired_key_stops_authenticating_and_its_sessions_end() {
    let registry = registry();
    registry.put_key(expiring_key("soon", "alice", 50));
    let session = registry.authenticate_key("sha-soon").unwrap();
    let unrelated = registry.authenticate_key("sha-a1").unwrap();
    assert!(session.identity().is_ok());

    std::thread::sleep(Duration::from_millis(60));

    assert_eq!(
        session.identity(),
        Err(CredentialEnded::Expired),
        "checks fail from the expiry instant, before any sweep"
    );
    assert_eq!(
        registry.authenticate_key("sha-soon").unwrap_err(),
        ApiKeyRejected::Expired
    );
    assert!(unrelated.identity().is_ok());
    let (expired, next) = registry.expire_due();
    assert_eq!(expired, vec!["soon".to_string()]);
    assert_eq!(next, None);
    assert_eq!(
        registry.expire_due().0,
        Vec::<String>::new(),
        "expired once"
    );
}

#[test]
fn a_key_already_past_its_expiry_loads_expired() {
    let registry = CredentialRegistry::default();
    registry.load(
        vec![user("alice", Role::Editor)],
        vec![expiring_key("old", "alice", -1_000), key("new", "alice")],
    );
    assert_eq!(
        registry.authenticate_key("sha-old").unwrap_err(),
        ApiKeyRejected::Expired
    );
    assert!(registry.authenticate_key("sha-new").is_ok());
    let listed = registry.api_keys();
    let status: Vec<_> = listed
        .iter()
        .map(|k| (k.label.as_str(), k.expired))
        .collect();
    assert_eq!(status, vec![("new", false), ("old", true)]);
}

#[test]
fn rotation_grace_keeps_both_keys_until_the_old_one_expires() {
    let registry = registry();
    let old = registry.authenticate_key("sha-a1").unwrap();
    registry.put_key(key("a1-next", "alice"));

    let at = now_ms() + 50;
    let hash = registry.check_expire_key("a1", at).unwrap();
    assert_eq!(hash, "sha-a1");
    registry.expire_key(&hash, at);

    // Grace period: the old key and its sessions still work, as does the new.
    assert!(old.identity().is_ok());
    assert!(registry.authenticate_key("sha-a1").is_ok());
    let new = registry.authenticate_key("sha-a1-next").unwrap();
    let (_, next) = registry.expire_due();
    assert!(next.is_some_and(|wait| wait <= Duration::from_millis(50)));

    std::thread::sleep(Duration::from_millis(60));

    assert_eq!(old.identity(), Err(CredentialEnded::Expired));
    assert_eq!(
        registry.authenticate_key("sha-a1").unwrap_err(),
        ApiKeyRejected::Expired
    );
    assert!(new.identity().is_ok());
}

#[test]
fn an_expiry_can_only_be_brought_forward() {
    let registry = registry();
    let first = now_ms() + 60_000;
    registry.expire_key(&registry.check_expire_key("a1", first).unwrap(), first);

    assert_eq!(
        registry.check_expire_key("a1", first + 1),
        Err(ExpireRejected::ExpiresSooner(first))
    );
    registry.expire_key("sha-a1", first + 1);
    assert_eq!(registry.api_keys()[0].times.expires_at, Some(first));
    assert!(registry.check_expire_key("a1", first - 1).is_ok());
    assert_eq!(
        registry.check_expire_key("nope", first),
        Err(ExpireRejected::Unknown)
    );

    registry.expire_key("sha-a1", 0);
    assert_eq!(
        registry.check_expire_key("a1", 0),
        Err(ExpireRejected::Expired)
    );
}

#[tokio::test]
async fn expiry_completes_end_signals_of_that_key_only() {
    let registry = registry();
    registry.put_key(expiring_key("soon", "alice", 30));
    let session = registry.authenticate_key("sha-soon").unwrap();
    let mut signal = session.end_signal();
    let mut unrelated = registry.authenticate_key("sha-a1").unwrap().end_signal();

    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(futures_util::poll!(&mut signal).is_pending(), "until swept");
    registry.expire_due();

    tokio::time::timeout(Duration::from_secs(5), signal)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), session.end_signal())
        .await
        .unwrap();
    assert!(futures_util::poll!(&mut unrelated).is_pending());
}

#[test]
fn key_use_is_recorded_throttled_and_persisted_once() {
    let registry = registry();
    assert!(registry.unpersisted_usage().is_empty());

    registry.authenticate_key("sha-a1").unwrap();
    let first = registry.unpersisted_usage();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].key_hash, "sha-a1");

    // Uses within the resolution do not move the timestamp.
    registry.authenticate_key("sha-a1").unwrap();
    assert_eq!(registry.unpersisted_usage(), first);

    registry.usage_persisted(&first);
    assert!(registry.unpersisted_usage().is_empty());
    let listed = registry.api_keys();
    assert_eq!(listed[0].times.last_used_at, Some(first[0].last_used_at));
    assert_eq!(listed[1].times.last_used_at, None);

    // Usage loaded from storage counts as persisted.
    let used = ApiKeyRecord {
        times: ApiKeyTimes {
            last_used_at: Some(1),
            ..ApiKeyTimes::default()
        },
        ..key("loaded", "bob")
    };
    registry.put_key(used);
    assert!(registry.unpersisted_usage().is_empty());
}

#[test]
fn rejected_and_expired_authentications_record_no_use() {
    let registry = registry();
    registry.put_key(expiring_key("gone", "alice", -1));
    assert!(registry.authenticate_key("sha-gone").is_err());
    assert!(registry.authenticate_key("sha-unknown").is_err());
    assert!(registry.unpersisted_usage().is_empty());
}
