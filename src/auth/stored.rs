//! The credentials `_internal` stores durably, decoded into the records
//! [`CredentialRegistry::load`](super::CredentialRegistry::load) indexes.
//!
//! `users` rows are `(username, password_hash, role)`; `api_keys` rows are
//! `(label, key_hash, username)`, with `api_key_times(key_hash, field, at)`
//! rows holding each key's `created_at`, `expires_at` and `last_used_at`. A
//! row too short or not made of strings is not a credential and is ignored; a
//! user whose role does not parse is skipped with a warning, so it cannot
//! authenticate. When a time field has several rows the earliest creation and
//! expiry and the latest use win, so a crash between writes can only leave a
//! key stricter than intended.

use std::collections::HashMap;

use tracing::warn;

use crate::value::RelationMap;

use super::{ApiKeyRecord, ApiKeyTimes, UserRecord};

pub(crate) const API_KEYS: &str = "api_keys";
pub(crate) const API_KEY_TIMES: &str = "api_key_times";
pub(crate) const CREATED_AT: &str = "created_at";
pub(crate) const EXPIRES_AT: &str = "expires_at";
pub(crate) const LAST_USED_AT: &str = "last_used_at";

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
    let keys = string_rows(relations, API_KEYS)
        .map(|(label, key_hash, username)| ApiKeyRecord {
            label: label.to_string(),
            key_hash: key_hash.to_string(),
            username: username.to_string(),
            times: times.get(key_hash).copied().unwrap_or_default(),
        })
        .collect();
    (users, keys)
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
    fn missing_relations_hold_no_credentials() {
        let (users, keys) = stored_credentials(&RelationMap::new());
        assert!(users.is_empty() && keys.is_empty());
    }
}
