//! Live credentials: the in-memory index of users and API keys.
//!
//! `_internal` stores users and API keys durably. [`CredentialRegistry`] is
//! their in-memory index: loaded at startup, then updated by every credential
//! mutation right after it is persisted. Authenticating against the registry
//! returns a [`Principal`] bound to exactly one credential: one API key, or one
//! generation of a user's password.
//!
//! Revoking a credential (`.apikey revoke`, `.user password`, `.user drop`)
//! ends it before the command returns. An API key may also carry an expiry,
//! which ends it once passed: sessions bound to it fail their checks from that
//! instant, and [`CredentialRegistry::expire_due`] completes their signals.
//! Other credentials, including other keys of the same user, are untouched.
//!
//! The registry lock is taken only to authenticate, to mutate credentials and
//! by the periodic upkeep; never per request or per frame.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::Notify;

use super::principal::{now_ms, Credential, CredentialEnded, CredentialId, Principal, RoleCell};
use super::Role;

/// A user as stored in `_internal.users`.
#[derive(Debug, Clone)]
pub struct UserRecord {
    pub username: String,
    pub password_hash: String,
    pub role: Role,
}

/// When an API key was created, expires and was last used, in Unix ms.
/// `None` is unknown (`created_at`), never (`expires_at`) or not yet
/// (`last_used_at`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApiKeyTimes {
    pub created_at: Option<u64>,
    pub expires_at: Option<u64>,
    pub last_used_at: Option<u64>,
}

/// An API key as stored in `_internal.api_keys` and `_internal.api_key_times`.
#[derive(Debug, Clone)]
pub struct ApiKeyRecord {
    pub label: String,
    pub key_hash: String,
    pub username: String,
    pub times: ApiKeyTimes,
}

/// An API key as listed: everything but its hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyInfo {
    pub label: String,
    pub owner: String,
    pub times: ApiKeyTimes,
    pub expired: bool,
}

/// A key's last use, to persist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyUsage {
    pub key_hash: String,
    pub last_used_at: u64,
}

/// A password login in progress: verify `password_hash` off the async
/// workers, then call [`PasswordCandidate::accept`].
#[derive(Debug)]
pub struct PasswordCandidate {
    pub password_hash: String,
    principal: Principal,
}

impl PasswordCandidate {
    /// The principal for a verified password. Fails if the password was
    /// changed or the user dropped while it was being verified.
    pub fn accept(self) -> Result<Principal, CredentialEnded> {
        self.principal.identity()?;
        Ok(self.principal)
    }
}

/// Why an API key did not authenticate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyRejected {
    Unknown,
    Expired,
    OwnerNotFound,
}

impl fmt::Display for ApiKeyRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unknown => "Invalid API key",
            Self::Expired => "API key expired",
            Self::OwnerNotFound => "API key owner not found",
        })
    }
}

/// Why an API key's expiry was not changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireRejected {
    Unknown,
    /// It has already expired.
    Expired,
    /// It already expires at this earlier time (Unix ms).
    ExpiresSooner(u64),
}

#[derive(Debug)]
struct User {
    password_hash: String,
    role: Arc<RoleCell>,
    password: Arc<Credential>,
}

#[derive(Debug)]
struct ApiKey {
    credential: Arc<Credential>,
    created_at: Option<u64>,
    /// `last_used_at` as persisted; the credential holds the live value.
    persisted_last_used_at: Option<u64>,
}

#[derive(Debug, Default)]
struct Credentials {
    users: HashMap<String, User>,
    /// By key hash.
    keys: HashMap<String, ApiKey>,
    last_generation: u64,
}

impl Credentials {
    fn next_generation(&mut self) -> u64 {
        self.last_generation += 1;
        self.last_generation
    }

    /// Add a user, or give an existing one a new password generation and
    /// role. The role cell is kept: sessions on the user's API keys share it.
    fn upsert_user(&mut self, record: UserRecord) {
        let username = record.username;
        let id = CredentialId::Password {
            username: username.clone(),
            generation: self.next_generation(),
        };
        let password = Credential::new(id, &username, None, None);
        match self.users.get_mut(&username) {
            Some(user) => {
                user.password.end(CredentialEnded::Revoked);
                user.password = password;
                user.password_hash = record.password_hash;
                user.role.set(record.role);
            }
            None => {
                let user = User {
                    password_hash: record.password_hash,
                    role: RoleCell::new(record.role),
                    password,
                };
                self.users.insert(username, user);
            }
        }
    }

    fn insert_key(&mut self, record: ApiKeyRecord) {
        let id = CredentialId::ApiKey {
            label: record.label,
            generation: self.next_generation(),
        };
        let times = record.times;
        let key = ApiKey {
            credential: Credential::new(id, &record.username, times.expires_at, times.last_used_at),
            created_at: times.created_at,
            persisted_last_used_at: times.last_used_at,
        };
        if let Some(previous) = self.keys.insert(record.key_hash, key) {
            previous.credential.end(CredentialEnded::Revoked);
        }
    }

    /// Remove and revoke every key matching `matches`; returns how many.
    fn revoke_keys(&mut self, mut matches: impl FnMut(&Credential) -> bool) -> usize {
        let before = self.keys.len();
        self.keys.retain(|_, key| {
            let hit = matches(&key.credential);
            if hit {
                key.credential.end(CredentialEnded::Revoked);
            }
            !hit
        });
        before - self.keys.len()
    }

    fn key_labelled(&self, label: &str) -> Option<(&String, &ApiKey)> {
        self.keys
            .iter()
            .find(|(_, key)| key_label(&key.credential) == label)
    }
}

fn key_label(credential: &Credential) -> &str {
    match &credential.id {
        CredentialId::ApiKey { label, .. } => label,
        CredentialId::Password { .. } => "",
    }
}

/// In-memory index of every live credential. See the module docs.
#[derive(Debug, Default)]
pub struct CredentialRegistry {
    state: RwLock<Credentials>,
    /// Woken when an API key's expiry is set or brought forward.
    expiry_changed: Notify,
}

impl CredentialRegistry {
    /// Replace the whole index, revoking every credential it held.
    pub fn load(&self, users: Vec<UserRecord>, keys: Vec<ApiKeyRecord>) {
        let mut state = self.state.write();
        for user in state.users.values() {
            user.password.end(CredentialEnded::Revoked);
        }
        state.revoke_keys(|_| true);
        state.users.clear();
        for user in users {
            state.upsert_user(user);
        }
        for key in keys {
            state.insert_key(key);
        }
        drop(state);
        self.expiry_changed.notify_one();
    }

    /// Start a password login. `None` for an unknown user; callers still
    /// spend a dummy verification so unknown users cost the same.
    pub fn password_candidate(&self, username: &str) -> Option<PasswordCandidate> {
        let state = self.state.read();
        let user = state.users.get(username)?;
        Some(PasswordCandidate {
            password_hash: user.password_hash.clone(),
            principal: Principal::new(Arc::clone(&user.password), Arc::clone(&user.role)),
        })
    }

    /// Authenticate the API key whose SHA-256 is `key_hash`, recording the use.
    pub fn authenticate_key(&self, key_hash: &str) -> Result<Principal, ApiKeyRejected> {
        let state = self.state.read();
        let key = state.keys.get(key_hash).ok_or(ApiKeyRejected::Unknown)?;
        if key.credential.ended().is_some() {
            return Err(ApiKeyRejected::Expired);
        }
        let owner = state
            .users
            .get(&key.credential.username)
            .ok_or(ApiKeyRejected::OwnerNotFound)?;
        key.credential.touch(now_ms());
        Ok(Principal::new(
            Arc::clone(&key.credential),
            Arc::clone(&owner.role),
        ))
    }

    /// Add a user, or replace one of the same name (revoking its password).
    pub fn put_user(&self, record: UserRecord) {
        self.state.write().upsert_user(record);
    }

    /// Start a new password generation; sessions on the old one are revoked.
    /// Returns `false` for an unknown user.
    pub fn set_password(&self, username: &str, password_hash: String) -> bool {
        let mut state = self.state.write();
        let Some(role) = state.users.get(username).map(|user| user.role.get()) else {
            return false;
        };
        state.upsert_user(UserRecord {
            username: username.to_string(),
            password_hash,
            role,
        });
        true
    }

    /// Change a user's global role for every live session. Returns `false`
    /// for an unknown user.
    pub fn set_role(&self, username: &str, role: Role) -> bool {
        match self.state.read().users.get(username) {
            Some(user) => {
                user.role.set(role);
                true
            }
            None => false,
        }
    }

    /// Remove a user, revoking its password and every API key it owns.
    pub fn remove_user(&self, username: &str) {
        let mut state = self.state.write();
        if let Some(user) = state.users.remove(username) {
            user.password.end(CredentialEnded::Revoked);
        }
        state.revoke_keys(|key| key.username == username);
    }

    /// Add an API key.
    pub fn put_key(&self, record: ApiKeyRecord) {
        let expires = record.times.expires_at.is_some();
        self.state.write().insert_key(record);
        if expires {
            self.expiry_changed.notify_one();
        }
    }

    /// Revoke the API key labelled `label`; `false` if there is none.
    pub fn revoke_key(&self, label: &str) -> bool {
        let mut state = self.state.write();
        state.revoke_keys(|key| key_label(key) == label) > 0
    }

    /// The hash and expiry of the live key labelled `label`, if `at` would
    /// bring its expiry forward. Validates an [`Self::expire_key`] before
    /// it is persisted.
    pub fn check_expire_key(&self, label: &str, at: u64) -> Result<String, ExpireRejected> {
        let state = self.state.read();
        let (key_hash, key) = state.key_labelled(label).ok_or(ExpireRejected::Unknown)?;
        if key.credential.ended().is_some() {
            return Err(ExpireRejected::Expired);
        }
        match key.credential.expires_at() {
            Some(current) if current < at => Err(ExpireRejected::ExpiresSooner(current)),
            _ => Ok(key_hash.clone()),
        }
    }

    /// Bring the expiry of the key whose hash is `key_hash` forward to `at`
    /// (Unix ms). A later `at` than its current expiry changes nothing.
    pub fn expire_key(&self, key_hash: &str, at: u64) {
        if let Some(key) = self.state.read().keys.get(key_hash) {
            key.credential.expire_at(at);
        }
        self.expiry_changed.notify_one();
    }

    /// Every API key, by label.
    pub fn api_keys(&self) -> Vec<ApiKeyInfo> {
        let state = self.state.read();
        let mut keys: Vec<_> = state
            .keys
            .values()
            .map(|key| ApiKeyInfo {
                label: key_label(&key.credential).to_string(),
                owner: key.credential.username.clone(),
                times: ApiKeyTimes {
                    created_at: key.created_at,
                    expires_at: key.credential.expires_at(),
                    last_used_at: key.credential.last_used_at(),
                },
                expired: key.credential.ended().is_some(),
            })
            .collect();
        keys.sort_by(|a, b| a.label.cmp(&b.label));
        keys
    }

    /// End every API key whose expiry has passed, completing its sessions'
    /// signals. Returns the labels just expired and the time until the next
    /// expiry, if any key has one to come.
    pub fn expire_due(&self) -> (Vec<String>, Option<Duration>) {
        let now = now_ms();
        let state = self.state.read();
        let mut expired = Vec::new();
        let mut next = None;
        for key in state.keys.values() {
            let Some(at) = key.credential.expires_at() else {
                continue;
            };
            if at > now {
                next = Some(next.map_or(at, |next: u64| next.min(at)));
            } else if key.credential.end(CredentialEnded::Expired) {
                expired.push(key_label(&key.credential).to_string());
            }
        }
        (expired, next.map(|at| Duration::from_millis(at - now)))
    }

    /// Wait until an API key's expiry is set or brought forward.
    pub async fn expiry_changed(&self) {
        self.expiry_changed.notified().await;
    }

    /// Key uses newer than what was last persisted.
    pub fn unpersisted_usage(&self) -> Vec<KeyUsage> {
        let state = self.state.read();
        state
            .keys
            .iter()
            .filter_map(|(key_hash, key)| {
                let last_used_at = key.credential.last_used_at()?;
                (Some(last_used_at) > key.persisted_last_used_at).then(|| KeyUsage {
                    key_hash: key_hash.clone(),
                    last_used_at,
                })
            })
            .collect()
    }

    /// Record that `usage` was persisted.
    pub fn usage_persisted(&self, usage: &[KeyUsage]) {
        let mut state = self.state.write();
        for used in usage {
            if let Some(key) = state.keys.get_mut(&used.key_hash) {
                key.persisted_last_used_at =
                    key.persisted_last_used_at.max(Some(used.last_used_at));
            }
        }
    }
}

#[cfg(test)]
#[path = "credentials_tests.rs"]
mod tests;
