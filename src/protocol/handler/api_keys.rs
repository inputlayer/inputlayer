//! API keys: `.apikey` commands, their storage in `_internal`, and the
//! credential upkeep task that expires keys and persists their last use.
//!
//! A key is stored as `api_keys(label, key_hash, owner)` plus rows in
//! `api_key_times(key_hash, field, at)`, one per `created_at`, `expires_at`
//! and `last_used_at`. Every write is ordered so a crash can only leave a key
//! stricter than intended, never laxer: times are written before the key,
//! a replacement time before the row it replaces is deleted, and when a field
//! has several rows the earliest expiry and the latest use win.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use super::{now_ms, Handler};
use crate::auth::{self, ApiKeyRecord, ApiKeyTimes, ExpireRejected, KeyUsage};
use crate::protocol::wire::{ColumnDef, QueryResult, WireDataType, WireTuple, WireValue};
use crate::storage_engine::{KnowledgeGraphSnapshot, StorageEngine};
use crate::value::{Tuple, Value};

use crate::auth::stored::{API_KEYS, API_KEY_TIMES, CREATED_AT, EXPIRES_AT, LAST_USED_AT};

/// How often key uses are persisted: at most one `_internal` write per
/// interval, however many requests the keys served.
const USAGE_FLUSH_INTERVAL: Duration = Duration::from_secs(60);

fn time_row(key_hash: &str, field: &str, at: u64) -> Tuple {
    Tuple::new(vec![
        Value::string(key_hash),
        Value::string(field),
        Value::timestamp(i64::try_from(at).unwrap_or(i64::MAX)),
    ])
}

fn key_row(record: &ApiKeyRecord) -> Tuple {
    Tuple::new(vec![
        Value::string(&record.label),
        Value::string(&record.key_hash),
        Value::string(&record.username),
    ])
}

fn timestamp(at: Option<u64>) -> WireValue {
    at.and_then(|at| i64::try_from(at).ok())
        .map_or(WireValue::Null, WireValue::Timestamp)
}

fn column(name: &str, data_type: WireDataType) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type,
    }
}

/// Every API key stored in `_internal`, with its times.
pub(super) fn read_api_keys(snapshot: &KnowledgeGraphSnapshot) -> Vec<ApiKeyRecord> {
    auth::stored_credentials(&snapshot.input_tuples).1
}

/// Persist a new key: its times first, so a crash never leaves it without
/// its expiry.
pub(super) fn store_api_key(storage: &StorageEngine, record: &ApiKeyRecord) -> Result<(), String> {
    let times: Vec<Tuple> = [
        (CREATED_AT, record.times.created_at),
        (EXPIRES_AT, record.times.expires_at),
    ]
    .into_iter()
    .filter_map(|(field, at)| Some(time_row(&record.key_hash, field, at?)))
    .collect();
    storage
        .insert_tuples_into(auth::INTERNAL_KG, API_KEY_TIMES, times)
        .map_err(|e| e.to_string())?;
    storage
        .insert_tuples_into(auth::INTERNAL_KG, API_KEYS, vec![key_row(record)])
        .map(drop)
        .map_err(|e| e.to_string())
}

/// Delete the stored keys `matches` selects, then their times. Returns how
/// many keys were deleted.
pub(super) fn delete_api_keys(
    storage: &StorageEngine,
    snapshot: &KnowledgeGraphSnapshot,
    matches: impl Fn(&ApiKeyRecord) -> bool,
) -> Result<usize, String> {
    let doomed: Vec<ApiKeyRecord> = read_api_keys(snapshot)
        .into_iter()
        .filter(matches)
        .collect();
    let keys = doomed.iter().map(key_row).collect();
    storage
        .delete_tuples_from(auth::INTERNAL_KG, API_KEYS, keys)
        .map_err(|e| e.to_string())?;
    let times = snapshot
        .input_tuples
        .get(API_KEY_TIMES)
        .into_iter()
        .flatten()
        .filter(|tuple| {
            let hash = tuple.values().first().and_then(Value::as_str);
            doomed.iter().any(|key| hash == Some(key.key_hash.as_str()))
        })
        .cloned()
        .collect();
    // The keys are gone; their times are only clutter now.
    if let Err(e) = storage.delete_tuples_from(auth::INTERNAL_KG, API_KEY_TIMES, times) {
        warn!(error = %e, "apikey_times_cleanup_failed");
    }
    Ok(doomed.len())
}

/// Set `field` of each key to its new time: insert the new rows, then delete
/// the rows they replace. Each step is one commit.
fn replace_times(
    storage: &StorageEngine,
    field: &str,
    updates: &[(&str, u64)],
) -> Result<(), String> {
    let new: Vec<Tuple> = updates
        .iter()
        .map(|&(hash, at)| time_row(hash, field, at))
        .collect();
    storage
        .insert_tuples_into(auth::INTERNAL_KG, API_KEY_TIMES, new.clone())
        .map_err(|e| e.to_string())?;
    let snapshot = storage
        .get_snapshot_for(auth::INTERNAL_KG)
        .map_err(|e| e.to_string())?;
    let replaced: Vec<Tuple> = snapshot
        .input_tuples
        .get(API_KEY_TIMES)
        .into_iter()
        .flatten()
        .filter(|tuple| match tuple.values() {
            [hash, f, _] => {
                f.as_str() == Some(field)
                    && updates.iter().any(|(h, _)| hash.as_str() == Some(h))
                    && !new.contains(tuple)
            }
            _ => false,
        })
        .cloned()
        .collect();
    storage
        .delete_tuples_from(auth::INTERNAL_KG, API_KEY_TIMES, replaced)
        .map(drop)
        .map_err(|e| e.to_string())
}

impl Handler {
    /// `.apikey create`: the plaintext key as a result row (shown only once).
    pub fn handle_apikey_create(
        &self,
        label: &str,
        owner: &str,
        ttl: Option<Duration>,
    ) -> Result<QueryResult, String> {
        let (plaintext_key, times) = self.create_api_key_with_times(label, owner, ttl)?;
        let row = WireTuple {
            values: vec![
                WireValue::String(label.to_string()),
                WireValue::String(plaintext_key),
                timestamp(times.expires_at),
            ],
            provenance: None,
        };
        let schema = vec![
            column("label", WireDataType::String),
            column("api_key", WireDataType::String),
            column("expires_at", WireDataType::Timestamp),
        ];
        Ok(QueryResult::new(vec![row], schema, 0))
    }

    /// Create an API key for `owner` that expires `ttl` from now, or never.
    /// Returns the plaintext key, which is not stored and cannot be recovered.
    pub fn create_api_key(
        &self,
        label: &str,
        owner: &str,
        ttl: Option<Duration>,
    ) -> Result<String, String> {
        self.create_api_key_with_times(label, owner, ttl)
            .map(|(key, _)| key)
    }

    fn create_api_key_with_times(
        &self,
        label: &str,
        owner: &str,
        ttl: Option<Duration>,
    ) -> Result<(String, ApiKeyTimes), String> {
        let storage = self.storage.write();
        let snapshot = storage
            .get_snapshot_for(auth::INTERNAL_KG)
            .map_err(|e| format!("Auth storage error: {e}"))?;
        if read_api_keys(&snapshot)
            .iter()
            .any(|key| key.label == label)
        {
            return Err(format!("API key with label '{label}' already exists"));
        }

        let plaintext_key = auth::generate_api_key();
        let created_at = now_ms();
        let ttl_ms = ttl.map(|ttl| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX));
        let record = ApiKeyRecord {
            label: label.to_string(),
            key_hash: auth::hash_api_key(&plaintext_key),
            username: owner.to_string(),
            times: ApiKeyTimes {
                created_at: Some(created_at),
                expires_at: ttl_ms.map(|ttl| created_at.saturating_add(ttl)),
                last_used_at: None,
            },
        };
        store_api_key(&storage, &record).map_err(|e| format!("Failed to create API key: {e}"))?;
        let times = record.times;
        self.credentials.put_key(record);

        info!(label, owner, expires_at = ?times.expires_at, "audit_apikey_created");
        Ok((plaintext_key, times))
    }

    /// `.apikey list`: every key with its owner, times and status, never its
    /// hash.
    pub fn handle_apikey_list(&self) -> QueryResult {
        let rows = self
            .credentials
            .api_keys()
            .into_iter()
            .map(|key| WireTuple {
                values: vec![
                    WireValue::String(key.label),
                    WireValue::String(key.owner),
                    timestamp(key.times.created_at),
                    timestamp(key.times.expires_at),
                    timestamp(key.times.last_used_at),
                    WireValue::String(if key.expired { "expired" } else { "active" }.to_string()),
                ],
                provenance: None,
            })
            .collect();
        let schema = vec![
            column("label", WireDataType::String),
            column("owner", WireDataType::String),
            column(CREATED_AT, WireDataType::Timestamp),
            column(EXPIRES_AT, WireDataType::Timestamp),
            column(LAST_USED_AT, WireDataType::Timestamp),
            column("status", WireDataType::String),
        ];
        QueryResult::new(rows, schema, 0)
    }

    /// `.apikey expire`: bring a key's expiry forward to `ttl` from now. Its
    /// sessions end when that passes; a zero `ttl` ends them now.
    pub fn handle_apikey_expire(&self, label: &str, ttl: Duration) -> Result<QueryResult, String> {
        let storage = self.storage.write();
        let at = now_ms().saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX));
        let key_hash = self
            .credentials
            .check_expire_key(label, at)
            .map_err(|rejected| match rejected {
                ExpireRejected::Unknown => format!("API key '{label}' not found"),
                ExpireRejected::Expired => format!("API key '{label}' has already expired"),
                ExpireRejected::ExpiresSooner(current) => format!(
                    "API key '{label}' already expires sooner (at {current} ms); \
                     an expiry can only be brought forward"
                ),
            })?;
        replace_times(&storage, EXPIRES_AT, &[(&key_hash, at)])
            .map_err(|e| format!("Failed to set API key expiry: {e}"))?;
        self.credentials.expire_key(&key_hash, at);

        info!(label, expires_at = at, "audit_apikey_expiry_set");
        Ok(self.message_result(&if ttl.is_zero() {
            format!("API key '{label}' expired.")
        } else {
            format!("API key '{label}' expires in {}.", humanize(ttl))
        }))
    }

    /// `.apikey revoke`: delete a key and end its sessions now.
    pub fn handle_apikey_revoke(&self, label: &str) -> Result<QueryResult, String> {
        let storage = self.storage.write();
        let snapshot = storage
            .get_snapshot_for(auth::INTERNAL_KG)
            .map_err(|e| format!("Auth storage error: {e}"))?;
        let deleted = delete_api_keys(&storage, &snapshot, |key| key.label == label)
            .map_err(|e| format!("Failed to revoke API key: {e}"))?;
        if deleted == 0 {
            return Err(format!("API key '{label}' not found"));
        }
        self.credentials.revoke_key(label);
        info!(label, "audit_apikey_revoked");
        Ok(self.message_result(&format!("API key '{label}' revoked.")))
    }

    /// Persist key uses not yet persisted, in one batch.
    pub fn persist_api_key_usage(&self) {
        let usage = self.credentials.unpersisted_usage();
        if usage.is_empty() {
            return;
        }
        let updates: Vec<(&str, u64)> = usage
            .iter()
            .map(
                |KeyUsage {
                     key_hash,
                     last_used_at,
                 }| (key_hash.as_str(), *last_used_at),
            )
            .collect();
        match replace_times(&self.storage.read(), LAST_USED_AT, &updates) {
            Ok(()) => self.credentials.usage_persisted(&usage),
            Err(e) => warn!(keys = usage.len(), error = %e, "apikey_usage_persist_failed"),
        }
    }

    /// Run until dropped: expire API keys as their expiry passes, ending
    /// their sessions, and persist key uses every `USAGE_FLUSH_INTERVAL`.
    pub async fn credential_upkeep(self: Arc<Self>) {
        let mut flush = tokio::time::interval(USAGE_FLUSH_INTERVAL);
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        flush.tick().await;
        loop {
            let (expired, next) = self.credentials.expire_due();
            for label in expired {
                info!(label, "audit_apikey_expired");
            }
            let next_expiry = async {
                match next {
                    Some(wait) => tokio::time::sleep(wait).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                () = next_expiry => {}
                () = self.credentials.expiry_changed() => {}
                _ = flush.tick() => {
                    let handler = Arc::clone(&self);
                    let persisted =
                        tokio::task::spawn_blocking(move || handler.persist_api_key_usage()).await;
                    if let Err(e) = persisted {
                        warn!(error = %e, "apikey_usage_persist_panicked");
                    }
                }
            }
        }
    }
}

/// `ttl` in its largest whole unit, as `.apikey` accepts it.
fn humanize(ttl: Duration) -> String {
    let ms = ttl.as_millis();
    [
        (86_400_000, "d"),
        (3_600_000, "h"),
        (60_000, "m"),
        (1_000, "s"),
    ]
    .into_iter()
    .find(|&(unit, _)| ms >= unit && ms.is_multiple_of(unit))
    .map_or_else(
        || format!("{ms}ms"),
        |(unit, name)| format!("{}{name}", ms / unit),
    )
}

#[cfg(test)]
#[path = "api_keys_tests.rs"]
mod tests;
