//! The credentials `_internal` stores durably, decoded into the records
//! [`CredentialRegistry::load`](super::CredentialRegistry::load) indexes.
//!
//! `users` rows are `(username, password_hash, role)`; `api_keys` rows are
//! `(label, key_hash, username)`, with `api_key_times(key_hash, field, at)`
//! rows holding each key's `created_at`, `expires_at` and `last_used_at`. A
//! key created with a role is a `scoped_api_keys(label, key_hash, username)`
//! row instead, with one `api_key_scopes(key_hash, kg, role, relations)` row
//! (`relations` as [`decode_relations`] reads it): a server from before scopes
//! reads only `api_keys`, so it never mistakes a scoped key for an unscoped
//! one. A row too short or not made of strings is not a credential and is
//! ignored; a user whose role does not parse is skipped with a warning, so it
//! cannot authenticate, and so is a key whose scope rows do not decode to
//! exactly one scope, or a scoped key with none. When a time field has several rows the earliest creation and
//! expiry and the latest use win, so a crash between writes can only leave a
//! key stricter than intended.

use std::collections::HashMap;

use tracing::warn;

use crate::value::{RelationMap, Value};

use super::{decode_relations, ApiKeyRecord, ApiKeyTimes, KeyScope, KgAccess, UserRecord};

pub(crate) const API_KEYS: &str = "api_keys";
pub(crate) const API_KEY_TIMES: &str = "api_key_times";
pub(crate) const SCOPED_API_KEYS: &str = "scoped_api_keys";
pub(crate) const API_KEY_SCOPES: &str = "api_key_scopes";
pub(crate) const CREATED_AT: &str = "created_at";
pub(crate) const EXPIRES_AT: &str = "expires_at";
pub(crate) const LAST_USED_AT: &str = "last_used_at";
/// `bootstrap_keys(label)`: a row once admin bootstrap has issued its API
/// key, recorded at startup for a data directory that has users or a
/// bootstrap key but predates this record. It outlives the key, so bootstrap
/// never issues a key a second time.
pub(crate) const BOOTSTRAP_KEYS: &str = "bootstrap_keys";

/// Every user and API key in `_internal`'s relations.
pub fn stored_credentials(relations: &RelationMap) -> (Vec<UserRecord>, Vec<ApiKeyRecord>) {
    let users = string_rows(relations, "users")
        .filter_map(|(username, hash, role)| match role.parse() {
            Ok(role) => Some(UserRecord {
                username: username.to_string(),
                password_hash: hash.to_string(),
                role,
            }),
            Err(e) => {
                warn!(username, error = %e, "auth_user_skipped");
                None
            }
        })
        .collect();
    let times = key_times(relations);
    let mut scopes = key_scopes(relations);
    let keys = string_rows(relations, API_KEYS)
        .map(|row| (row, false))
        .chain(string_rows(relations, SCOPED_API_KEYS).map(|row| (row, true)))
        .filter_map(|((label, key_hash, username), scoped)| {
            let scope = match scopes.remove(key_hash) {
                None if scoped => {
                    warn!(label, error = "scope missing", "auth_api_key_skipped");
                    return None;
                }
                None => None,
                Some(Ok(scope)) => Some(scope),
                Some(Err(error)) => {
                    warn!(label, error = %error, "auth_api_key_skipped");
                    return None;
                }
            };
            Some(ApiKeyRecord {
                label: label.to_string(),
                key_hash: key_hash.to_string(),
                username: username.to_string(),
                times: times.get(key_hash).copied().unwrap_or_default(),
                scope,
            })
        })
        .collect();
    (users, keys)
}

/// Each scoped key's scope, by key hash, or why it is unusable.
fn key_scopes(relations: &RelationMap) -> HashMap<&str, Result<KeyScope, String>> {
    let mut scopes: HashMap<&str, Result<KeyScope, String>> = HashMap::new();
    for tuple in relations.get(API_KEY_SCOPES).into_iter().flatten() {
        let values = tuple.values();
        let Some(hash) = values.first().and_then(Value::as_str) else {
            continue;
        };
        // A row naming a key but not readable as a scope makes that key
        // unusable: read as unscoped, it would act with its owner's rights.
        let scope = match values {
            [_, kg, role, stored] => match (kg.as_str(), role.as_str(), stored.as_str()) {
                (Some(kg), Some(role), Some(stored)) => role
                    .parse()
                    .and_then(|role| KgAccess::new(role, decode_relations(stored)?))
                    .and_then(|access| KeyScope::new(kg, access)),
                _ => Err("malformed scope row".to_string()),
            },
            _ => Err("malformed scope row".to_string()),
        };
        let entry = scopes.entry(hash).or_insert_with(|| scope.clone());
        if entry.as_ref().ok() != scope.as_ref().ok() {
            *entry = Err("several scopes stored".to_string());
        }
    }
    scopes
}

/// Each key's times, by key hash.
fn key_times(relations: &RelationMap) -> HashMap<&str, ApiKeyTimes> {
    let mut times: HashMap<&str, ApiKeyTimes> = HashMap::new();
    for tuple in relations.get(API_KEY_TIMES).into_iter().flatten() {
        let [hash, field, at] = tuple.values() else {
            continue;
        };
        let (Some(hash), Some(field), Some(at)) = (
            hash.as_str(),
            field.as_str(),
            at.as_timestamp().and_then(|at| u64::try_from(at).ok()),
        ) else {
            continue;
        };
        let entry = times.entry(hash).or_default();
        let (slot, keep_min) = match field {
            CREATED_AT => (&mut entry.created_at, true),
            EXPIRES_AT => (&mut entry.expires_at, true),
            LAST_USED_AT => (&mut entry.last_used_at, false),
            _ => continue,
        };
        *slot = Some(match *slot {
            Some(current) if keep_min => current.min(at),
            Some(current) => current.max(at),
            None => at,
        });
    }
    times
}

/// The first three columns of each row of `relation` that are all strings.
fn string_rows<'a>(
    relations: &'a RelationMap,
    relation: &str,
) -> impl Iterator<Item = (&'a str, &'a str, &'a str)> {
    relations
        .get(relation)
        .into_iter()
        .flatten()
        .filter_map(|tuple| match tuple.values() {
            [a, b, c, ..] => Some((a.as_str()?, b.as_str()?, c.as_str()?)),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Role;
    use crate::value::{Relation, Tuple, Value};

    fn row(values: &[Value]) -> Tuple {
        Tuple::new(values.to_vec())
    }

    fn strings(values: &[&str]) -> Tuple {
        row(&values.iter().map(|s| Value::string(s)).collect::<Vec<_>>())
    }

    #[test]
    fn decodes_users_and_keys() {
        let relations = RelationMap::from([
            (
                "users".to_string(),
                Relation::from(vec![
                    strings(&["alice", "hash-a", "editor"]),
                    strings(&["root", "hash-r", "ADMIN"]),
                ]),
            ),
            (
                "api_keys".to_string(),
                Relation::from(vec![strings(&["ci", "sha-ci", "alice"])]),
            ),
        ]);
        let (users, keys) = stored_credentials(&relations);
        let users: Vec<_> = users
            .iter()
            .map(|u| (u.username.as_str(), u.password_hash.as_str(), u.role))
            .collect();
        assert_eq!(
            users,
            [
                ("alice", "hash-a", Role::Editor),
                ("root", "hash-r", Role::Admin)
            ]
        );
        let keys: Vec<_> = keys
            .iter()
            .map(|k| (k.label.as_str(), k.key_hash.as_str(), k.username.as_str()))
            .collect();
        assert_eq!(keys, [("ci", "sha-ci", "alice")]);
    }

    #[test]
    fn rows_that_are_not_credentials_are_skipped() {
        let relations = RelationMap::from([
            (
                "users".to_string(),
                Relation::from(vec![
                    strings(&["mallory", "hash-m", "superuser"]),
                    strings(&["short", "hash-s"]),
                    row(&[
                        Value::string("typed"),
                        Value::Int64(1),
                        Value::string("viewer"),
                    ]),
                    strings(&["bob", "hash-b", "viewer"]),
                ]),
            ),
            (
                "api_keys".to_string(),
                Relation::from(vec![
                    row(&[Value::Int64(7), Value::string("sha"), Value::string("bob")]),
                    strings(&["k", "sha-k", "bob", "extra"]),
                ]),
            ),
        ]);
        let (users, keys) = stored_credentials(&relations);
        assert_eq!(
            users
                .iter()
                .map(|u| u.username.as_str())
                .collect::<Vec<_>>(),
            ["bob"]
        );
        assert_eq!(
            keys.iter().map(|k| k.label.as_str()).collect::<Vec<_>>(),
            ["k"]
        );
    }

    #[test]
    fn scoped_keys_decode_and_unreadable_scopes_disable_their_key() {
        use crate::auth::KgRole;
        let relations = RelationMap::from([
            (
                "api_keys".to_string(),
                Relation::from(vec![
                    strings(&["plain", "sha-p", "admin"]),
                    strings(&["ops", "sha-o", "admin"]),
                ]),
            ),
            (
                SCOPED_API_KEYS.to_string(),
                Relation::from(vec![
                    strings(&["agent", "sha-a", "admin"]),
                    strings(&["twice", "sha-t", "admin"]),
                    strings(&["bad-role", "sha-b", "admin"]),
                    strings(&["short", "sha-s", "admin"]),
                    strings(&["unscoped", "sha-u", "admin"]),
                ]),
            ),
            (
                API_KEY_SCOPES.to_string(),
                Relation::from(vec![
                    strings(&["sha-a", "shop", "decider", "attempt,decision"]),
                    strings(&["sha-o", "shop", "writer", "*"]),
                    strings(&["sha-t", "shop", "decider", "attempt"]),
                    strings(&["sha-t", "shop", "writer", "*"]),
                    strings(&["sha-b", "shop", "superuser", "*"]),
                    strings(&["sha-s", "shop", "writer"]),
                ]),
            ),
        ]);
        let (_, keys) = stored_credentials(&relations);
        let scopes: Vec<_> = keys
            .iter()
            .map(|k| (k.label.as_str(), k.scope.as_ref().map(ToString::to_string)))
            .collect();
        assert_eq!(
            scopes,
            [
                ("plain", None),
                ("ops", Some("writer on shop".to_string())),
                (
                    "agent",
                    Some("decider on shop (relations attempt, decision)".to_string())
                ),
            ]
        );
        assert_eq!(
            keys[2].scope.as_ref().map(|s| s.access.role()),
            Some(KgRole::Decider)
        );
    }

    #[test]
    fn missing_relations_hold_no_credentials() {
        let (users, keys) = stored_credentials(&RelationMap::new());
        assert!(users.is_empty() && keys.is_empty());
    }
}
