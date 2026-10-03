#![allow(clippy::unwrap_used)]

use super::*;
use crate::auth::INTERNAL_KG;
use crate::Config;

fn config(dir: &std::path::Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = dir.join("data");
    config.http.auth.bootstrap_admin_password = Some("api-keys-admin".to_string());
    config.http.auth.credentials_file = Some(dir.join("credentials.toml"));
    config
}

/// A bootstrapped handler on `dir`; call again on the same `dir` to restart.
fn open(dir: &std::path::Path) -> Arc<Handler> {
    let handler = Arc::new(Handler::from_config(config(dir)).unwrap());
    handler.bootstrap_auth();
    handler
}

/// `(field, at)` of every stored time row of the key labelled `label`.
fn stored_times(handler: &Handler, label: &str) -> Vec<(String, i64)> {
    let snapshot = handler.get_storage().get_snapshot_for(INTERNAL_KG).unwrap();
    let hash = read_api_keys(&snapshot)
        .into_iter()
        .find(|key| key.label == label)
        .map(|key| key.key_hash);
    let mut rows: Vec<_> = snapshot
        .input_tuples
        .get(API_KEY_TIMES)
        .into_iter()
        .flatten()
        .filter_map(|tuple| match tuple.values() {
            [h, field, at] if h.as_str().map(str::to_string) == hash => {
                Some((field.as_str()?.to_string(), at.as_timestamp()?))
            }
            _ => None,
        })
        .collect();
    rows.sort();
    rows
}

fn listed(handler: &Handler, label: &str) -> Vec<WireValue> {
    handler
        .handle_apikey_list()
        .rows
        .into_iter()
        .find(|row| row.values[0].as_str() == Some(label))
        .unwrap()
        .values
}

#[test]
fn key_times_survive_a_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let handler = open(tmp.path());
    let before = now_ms();
    let created = handler
        .handle_apikey_create("ci", "admin", Some(Duration::from_secs(3_600)))
        .unwrap();
    let key = created.rows[0].values[1].as_str().unwrap().to_string();
    let WireValue::Timestamp(expires_at) = created.rows[0].values[2] else {
        panic!("no expiry in {:?}", created.rows[0]);
    };
    assert!(expires_at >= (before + 3_600_000) as i64);
    handler.authenticate_api_key(&key).unwrap();
    handler.persist_api_key_usage();
    let row = listed(&handler, "ci");
    drop(handler);

    let handler = open(tmp.path());
    assert_eq!(listed(&handler, "ci"), row);
    assert!(matches!(row[2], WireValue::Timestamp(_)), "created_at");
    assert_eq!(row[3], WireValue::Timestamp(expires_at));
    assert!(matches!(row[4], WireValue::Timestamp(_)), "last_used_at");
    assert_eq!(row[5], WireValue::String("active".to_string()));
    assert!(handler.authenticate_api_key(&key).is_ok());
}

#[test]
fn an_expiry_set_before_a_restart_still_holds_after_it() {
    let tmp = tempfile::tempdir().unwrap();
    let handler = open(tmp.path());
    let key = handler.create_api_key("old", "admin", None).unwrap();
    handler
        .handle_apikey_expire("old", Duration::from_secs(3_600))
        .unwrap();
    handler.handle_apikey_expire("old", Duration::ZERO).unwrap();
    assert_eq!(
        handler.authenticate_api_key(&key).unwrap_err(),
        "API key expired"
    );
    let fields: Vec<_> = stored_times(&handler, "old")
        .into_iter()
        .map(|(field, _)| field)
        .collect();
    assert_eq!(fields, ["created_at", "expires_at"], "one row per field");
    drop(handler);

    let handler = open(tmp.path());
    assert_eq!(
        handler.authenticate_api_key(&key).unwrap_err(),
        "API key expired"
    );
    assert_eq!(
        listed(&handler, "old")[5],
        WireValue::String("expired".to_string())
    );
    assert!(handler
        .handle_apikey_expire("old", Duration::from_secs(60))
        .unwrap_err()
        .message
        .contains("already expired"));
}

#[test]
fn an_expiry_cannot_be_pushed_back() {
    let tmp = tempfile::tempdir().unwrap();
    let handler = open(tmp.path());
    handler
        .create_api_key("short", "admin", Some(Duration::from_secs(60)))
        .unwrap();
    let err = handler
        .handle_apikey_expire("short", Duration::from_secs(3_600))
        .unwrap_err();
    assert!(
        err.message.contains("can only be brought forward"),
        "{}",
        err.message
    );
    assert!(handler
        .handle_apikey_expire("missing", Duration::ZERO)
        .unwrap_err()
        .message
        .contains("not found"));
}

#[test]
fn a_crash_between_writes_leaves_the_earliest_expiry() {
    let tmp = tempfile::tempdir().unwrap();
    let handler = open(tmp.path());
    let key = handler.create_api_key("k", "admin", None).unwrap();
    let hash = auth::hash_api_key(&key);
    // As if a crash hit between inserting the new expiry and deleting the
    // old one: both rows are stored.
    let past = now_ms() - 1_000;
    handler
        .get_storage()
        .insert_tuples_into(
            INTERNAL_KG,
            API_KEY_TIMES,
            vec![
                time_row(&hash, EXPIRES_AT, past),
                time_row(&hash, EXPIRES_AT, past + 3_600_000),
            ],
        )
        .unwrap();
    drop(handler);

    let handler = open(tmp.path());
    assert_eq!(
        handler.authenticate_api_key(&key).unwrap_err(),
        "API key expired"
    );
}

#[test]
fn a_key_stored_without_times_never_expires() {
    let tmp = tempfile::tempdir().unwrap();
    let handler = open(tmp.path());
    let row = listed(&handler, "bootstrap");
    assert!(matches!(row[2], WireValue::Timestamp(_)), "{row:?}");
    // A key stored before key times existed.
    handler
        .get_storage()
        .insert_tuples_into(
            INTERNAL_KG,
            API_KEYS,
            vec![Tuple::new(vec![
                Value::string("legacy"),
                Value::string(&auth::hash_api_key("legacy-secret")),
                Value::string("admin"),
            ])],
        )
        .unwrap();
    drop(handler);

    let handler = open(tmp.path());
    assert!(handler.authenticate_api_key("legacy-secret").is_ok());
    let row = listed(&handler, "legacy");
    assert_eq!(row[2], WireValue::Null, "created_at unknown");
    assert_eq!(row[3], WireValue::Null, "never expires");
    assert_eq!(row[5], WireValue::String("active".to_string()));
}

#[test]
fn usage_is_persisted_in_one_row_per_key() {
    let tmp = tempfile::tempdir().unwrap();
    let handler = open(tmp.path());
    let key = handler.create_api_key("busy", "admin", None).unwrap();
    for _ in 0..2 {
        handler.authenticate_api_key(&key).unwrap();
        handler.persist_api_key_usage();
        std::thread::sleep(Duration::from_millis(1_100));
    }
    handler.authenticate_api_key(&key).unwrap();
    handler.persist_api_key_usage();

    let used: Vec<_> = stored_times(&handler, "busy")
        .into_iter()
        .filter(|(field, _)| field == LAST_USED_AT)
        .collect();
    assert_eq!(used.len(), 1, "{used:?}");
    assert_eq!(listed(&handler, "busy")[4], WireValue::Timestamp(used[0].1));
}

#[test]
fn revoking_or_dropping_the_owner_deletes_key_times() {
    let tmp = tempfile::tempdir().unwrap();
    let handler = open(tmp.path());
    handler
        .handle_user_create("carol", "carol-pw", "editor")
        .unwrap();
    handler
        .create_api_key("mine", "admin", Some(Duration::from_secs(60)))
        .unwrap();
    handler.create_api_key("hers", "carol", None).unwrap();
    assert!(!stored_times(&handler, "mine").is_empty());

    handler.handle_apikey_revoke("mine").unwrap();
    handler.handle_user_drop("carol").unwrap();

    let snapshot = handler.get_storage().get_snapshot_for(INTERNAL_KG).unwrap();
    let hashes: Vec<_> = snapshot
        .input_tuples
        .get(API_KEY_TIMES)
        .into_iter()
        .flatten()
        .filter_map(|tuple| tuple.values()[0].as_str().map(str::to_string))
        .collect();
    let live: Vec<_> = read_api_keys(&snapshot)
        .into_iter()
        .map(|key| key.key_hash)
        .collect();
    assert!(hashes.iter().all(|hash| live.contains(hash)), "{hashes:?}");
    assert_eq!(live.len(), 1, "only the bootstrap key remains");
}

#[tokio::test]
async fn upkeep_expires_keys_on_time() {
    let tmp = tempfile::tempdir().unwrap();
    let handler = open(tmp.path());
    let key = handler
        .create_api_key("brief", "admin", Some(Duration::from_millis(200)))
        .unwrap();
    let principal = handler.authenticate_api_key(&key).unwrap();
    let upkeep = tokio::spawn(Arc::clone(&handler).credential_upkeep());

    tokio::time::timeout(Duration::from_secs(5), principal.end_signal())
        .await
        .unwrap();
    assert_eq!(
        principal.ended(),
        Some(crate::auth::CredentialEnded::Expired)
    );

    // An expiry set while the sweep sleeps wakes it.
    let key = handler.create_api_key("later", "admin", None).unwrap();
    let later = handler.authenticate_api_key(&key).unwrap();
    handler
        .handle_apikey_expire("later", Duration::from_millis(100))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), later.end_signal())
        .await
        .unwrap();
    upkeep.abort();
}

#[test]
fn humanize_uses_the_largest_whole_unit() {
    assert_eq!(humanize(Duration::from_secs(86_400 * 90)), "90d");
    assert_eq!(humanize(Duration::from_secs(5_400)), "90m");
    assert_eq!(humanize(Duration::from_millis(1_500)), "1500ms");
}
