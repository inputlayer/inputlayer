//! API keys: `.apikey` commands, their storage in `_internal`, and the
//! credential upkeep task that expires keys and persists their last use.
//!
//! A key is stored as `api_keys(label, key_hash, owner)` plus rows in
//! `api_key_times(key_hash, field, at)`, one per `created_at`, `expires_at`
//! and `last_used_at`. A key created with a role is a
//! `scoped_api_keys(label, key_hash, owner)` row instead, with one
//! `api_key_scopes(key_hash, kg, role, relations)` row (see
//! [`crate::auth::stored`]). A new key's rows are
//! one commit. Every later write is ordered so a crash can only leave a key
//! stricter than intended, never laxer: a replacement time is written before
//! the row it replaces is deleted, a key is deleted before its scope, and
//! when a field has several rows the earliest expiry and the latest use win.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use super::{now_ms, user_exists, Handler, ProgramError};
use crate::auth::{self, ApiKeyRecord, ApiKeyTimes, ExpireRejected, KeyScope, KeyUsage};
use crate::protocol::wire::{ColumnDef, QueryResult, WireDataType, WireTuple, WireValue};
use crate::storage::StorageError;
use crate::storage_engine::{FactChange, KnowledgeGraphSnapshot, StorageEngine};
use crate::value::{Tuple, Value};

use crate::auth::stored::{
    API_KEYS, API_KEY_SCOPES, API_KEY_TIMES, CREATED_AT, EXPIRES_AT, LAST_USED_AT, SCOPED_API_KEYS,
};

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

/// The relation that holds `record`'s key row.
fn key_relation(record: &ApiKeyRecord) -> &'static str {
    if record.scope.is_some() {
        SCOPED_API_KEYS
    } else {
        API_KEYS
    }
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

/// The stored rows of a new key's creation and expiry times.
fn initial_time_rows(record: &ApiKeyRecord) -> Vec<Tuple> {
    [
        (CREATED_AT, record.times.created_at),
        (EXPIRES_AT, record.times.expires_at),
    ]
    .into_iter()
    .filter_map(|(field, at)| Some(time_row(&record.key_hash, field, at?)))
    .collect()
}

/// A new key's rows as changes for a write program: its times and scope,
/// then the key.
pub(super) fn api_key_inserts(record: &ApiKeyRecord) -> Vec<FactChange> {
    let scope_rows: Vec<Tuple> = record
        .scope
        .iter()
        .map(|scope| {
            Tuple::new(vec![
                Value::string(&record.key_hash),
                Value::string(&scope.kg),
                Value::string(&scope.access.role().to_string()),
                Value::string(&auth::encode_relations(scope.access.relations())),
            ])
        })
        .collect();
    [
        (API_KEY_TIMES, initial_time_rows(record)),
        (API_KEY_SCOPES, scope_rows),
        (key_relation(record), vec![key_row(record)]),
    ]
    .into_iter()
    .filter(|(_, tuples)| !tuples.is_empty())
    .map(|(relation, tuples)| FactChange::Insert {
        relation: relation.to_string(),
        tuples,
    })
    .collect()
}

/// Persist a new key with its times and scope, as one commit: a key is
/// never stored without its expiry or its scope.
pub(super) fn store_api_key(
    storage: &StorageEngine,
    record: &ApiKeyRecord,
) -> Result<(), StorageError> {
    super::commit_internal(storage, api_key_inserts(record))
}

/// Delete the stored keys `matches` selects, then their times and scopes.
/// Returns the labels of the deleted keys.
pub(super) fn delete_api_keys(
    storage: &StorageEngine,
    snapshot: &KnowledgeGraphSnapshot,
    matches: impl Fn(&ApiKeyRecord) -> bool,
) -> Result<Vec<String>, StorageError> {
    let doomed: Vec<ApiKeyRecord> = read_api_keys(snapshot)
        .into_iter()
        .filter(matches)
        .collect();
    for relation in [API_KEYS, SCOPED_API_KEYS] {
        let keys: Vec<Tuple> = doomed
            .iter()
            .filter(|key| key_relation(key) == relation)
            .map(key_row)
            .collect();
        if !keys.is_empty() {
            storage.delete_tuples_from(auth::INTERNAL_KG, relation, keys)?;
        }
    }
    let owned_rows = |relation: &str| -> Vec<Tuple> {
        snapshot
            .input_tuples
            .get(relation)
            .into_iter()
            .flatten()
            .filter(|tuple| {
                let hash = tuple.values().first().and_then(Value::as_str);
                doomed.iter().any(|key| hash == Some(key.key_hash.as_str()))
            })
            .cloned()
            .collect()
    };
    // The keys are gone; their times and scopes are only clutter now, unless
    // the cleanup's outcome is unknown.
    for relation in [API_KEY_TIMES, API_KEY_SCOPES] {
        match storage.delete_tuples_from(auth::INTERNAL_KG, relation, owned_rows(relation)) {
            Err(e @ StorageError::OutcomeUnknown { .. }) => return Err(e),
            Err(e) => warn!(relation, error = %e, "apikey_rows_cleanup_failed"),
            Ok(_) => {}
        }
    }
    Ok(doomed.into_iter().map(|key| key.label).collect())
}

/// Set `field` of each key to its new time: insert the new rows, then delete
/// the rows they replace. Each step is one commit.
fn replace_times(
    storage: &StorageEngine,
    field: &str,
    updates: &[(&str, u64)],
) -> Result<(), StorageError> {
    let new: Vec<Tuple> = updates
        .iter()
        .map(|&(hash, at)| time_row(hash, field, at))
        .collect();
    storage.insert_tuples_into(auth::INTERNAL_KG, API_KEY_TIMES, new.clone())?;
    let snapshot = storage.get_snapshot_for(auth::INTERNAL_KG)?;
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
}

impl Handler {
    /// `.apikey create`: the plaintext key as a result row (shown only once).
    pub fn handle_apikey_create(
        &self,
        label: &str,
        owner: &str,
        ttl: Option<Duration>,
        scope: Option<KeyScope>,
    ) -> Result<QueryResult, ProgramError> {
        let (plaintext_key, times) = self.create_api_key_with_times(label, owner, ttl, scope)?;
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
    ) -> Result<String, ProgramError> {
        self.create_api_key_with_times(label, owner, ttl, None)
            .map(|(key, _)| key)
    }

    fn create_api_key_with_times(
        &self,
        label: &str,
        owner: &str,
        ttl: Option<Duration>,
        scope: Option<KeyScope>,
    ) -> Result<(String, ApiKeyTimes), ProgramError> {
        let _credential_writes = self.credential_writes.lock();
        let storage = self.storage.read();
        if let Some(scope) = &scope {
            storage
                .get_snapshot_for(&scope.kg)
                .map_err(|_| format!("Knowledge graph '{}' not found", scope.kg))?;
        }
        let snapshot = storage.get_snapshot_for(auth::INTERNAL_KG)?;
        if !user_exists(&snapshot, owner) {
            return Err(format!("User '{owner}' not found").into());
        }
        if read_api_keys(&snapshot)
            .iter()
            .any(|key| key.label == label)
        {
            return Err(format!("API key with label '{label}' already exists").into());
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
            scope,
        };
        store_api_key(&storage, &record)?;
        let times = record.times;
        let scope = record.scope.as_ref().map(ToString::to_string);
        self.credentials.put_key(record);

        info!(label, owner, expires_at = ?times.expires_at, scope, "audit_apikey_created");
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
                    key.scope.map_or(WireValue::Null, |scope| {
                        WireValue::String(scope.to_string())
                    }),
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
            column("scope", WireDataType::String),
        ];
        QueryResult::new(rows, schema, 0)
    }

    /// `.apikey expire`: bring a key's expiry forward to `ttl` from now. Its
    /// sessions end when that passes; a zero `ttl` ends them now.
    pub fn handle_apikey_expire(
        &self,
        label: &str,
        ttl: Duration,
    ) -> Result<QueryResult, ProgramError> {
        let _credential_writes = self.credential_writes.lock();
        let storage = self.storage.read();
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
        replace_times(&storage, EXPIRES_AT, &[(&key_hash, at)])?;
        self.credentials.expire_key(&key_hash, at);

        info!(label, expires_at = at, "audit_apikey_expiry_set");
        Ok(self.message_result(&if ttl.is_zero() {
            format!("API key '{label}' expired.")
        } else {
            format!("API key '{label}' expires in {}.", humanize(ttl))
        }))
    }

    /// `.apikey revoke`: delete a key and end its sessions now.
    pub fn handle_apikey_revoke(&self, label: &str) -> Result<QueryResult, ProgramError> {
        let _credential_writes = self.credential_writes.lock();
        let storage = self.storage.read();
        let snapshot = storage.get_snapshot_for(auth::INTERNAL_KG)?;
        let deleted = delete_api_keys(&storage, &snapshot, |key| key.label == label)?;
        if deleted.is_empty() {
            return Err(format!("API key '{label}' not found").into());
        }
        self.credentials.revoke_key(label);
        info!(label, "audit_apikey_revoked");
        Ok(self.message_result(&format!("API key '{label}' revoked.")))
    }

    /// Persist key uses not yet persisted, in one batch.
    pub fn persist_api_key_usage(&self) {
        // A follower writes nothing; key uses there stay in memory.
        if self.storage.read().is_replica() {
            return;
        }
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
