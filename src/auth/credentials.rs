//! Live credentials and the sessions bound to them.
//!
//! `_internal` stores users and API keys durably. [`CredentialRegistry`] is
//! their in-memory index: loaded at startup, then updated by every credential
//! mutation right after it is persisted. Authenticating against the registry
//! returns a [`Principal`] bound to exactly one credential: one API key, or one
//! generation of a user's password.
//!
//! Revoking a credential (`.apikey revoke`, `.user password`, `.user drop`)
//! trips that credential's revocation flag before the command returns. Every
//! session bound to it fails its next [`Principal::identity`] check, and every
//! [`RevocationSignal`] taken from it completes. Other credentials, including
//! other keys of the same user, are untouched.
//!
//! A check is one atomic load on state the session already holds, and a
//! signal is a private one-shot channel per connection: no shared lock,
//! lookup or storage access per request or per frame. The registry lock is
//! taken only to authenticate and to mutate credentials; a credential's
//! watcher list only to register a signal and to revoke.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use parking_lot::{Mutex, RwLock};
use tokio::sync::oneshot;

use super::{AuthIdentity, Role};

/// Canonical identity of one credential. `generation` is unique per process
/// and increases with every credential issued, so a replaced password or a
/// re-created key label never shares an id with its predecessor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CredentialId {
    /// One generation of a user's password.
    Password { username: String, generation: u64 },
    /// One API key, named by its label.
    ApiKey { label: String, generation: u64 },
}

impl fmt::Display for CredentialId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Password {
                username,
                generation,
            } => write!(f, "password:{username}#{generation}"),
            Self::ApiKey { label, generation } => write!(f, "apikey:{label}#{generation}"),
        }
    }
}

/// The credential behind a principal was revoked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialRevoked;

impl fmt::Display for CredentialRevoked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Access denied: credential revoked")
    }
}

impl std::error::Error for CredentialRevoked {}

impl From<CredentialRevoked> for String {
    fn from(e: CredentialRevoked) -> Self {
        e.to_string()
    }
}

/// A user's current global role, shared by all of that user's credentials.
#[derive(Debug)]
struct RoleCell(AtomicU8);

impl RoleCell {
    fn new(role: Role) -> Arc<Self> {
        Arc::new(Self(AtomicU8::new(role as u8)))
    }

    fn get(&self) -> Role {
        match self.0.load(Ordering::Acquire) {
            r if r == Role::Admin as u8 => Role::Admin,
            r if r == Role::Editor as u8 => Role::Editor,
            _ => Role::Viewer,
        }
    }

    fn set(&self, role: Role) {
        self.0.store(role as u8, Ordering::Release);
    }
}

/// One issued credential. Revocation is one-way.
#[derive(Debug)]
struct Credential {
    id: CredentialId,
    username: String,
    revoked: AtomicBool,
    /// One sender per live [`RevocationSignal`].
    watchers: Mutex<Vec<oneshot::Sender<()>>>,
}

impl Credential {
    fn new(id: CredentialId, username: &str) -> Arc<Self> {
        Arc::new(Self {
            id,
            username: username.to_string(),
            revoked: AtomicBool::new(false),
            watchers: Mutex::new(Vec::new()),
        })
    }

    fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
        for watcher in self.watchers.lock().drain(..) {
            let _ = watcher.send(());
        }
    }

    fn watch(&self) -> RevocationSignal {
        let (tx, rx) = oneshot::channel();
        let mut watchers = self.watchers.lock();
        // Checked under the lock `revoke` drains with, so no revocation is missed.
        if self.is_revoked() {
            let _ = tx.send(());
        } else {
            watchers.retain(|watcher| !watcher.is_closed());
            watchers.push(tx);
        }
        RevocationSignal(rx)
    }

    fn is_revoked(&self) -> bool {
        self.revoked.load(Ordering::Acquire)
    }
}

/// An authenticated session's binding to one credential.
///
/// Cheap to clone; clones share the credential's revocation state.
#[derive(Debug, Clone)]
pub struct Principal {
    credential: Arc<Credential>,
    role: Arc<RoleCell>,
}

impl Principal {
    pub fn username(&self) -> &str {
        &self.credential.username
    }

    pub fn credential(&self) -> &CredentialId {
        &self.credential.id
    }

    /// The permissions this principal holds right now, as an immutable
    /// snapshot for one request or one outbound message.
    pub fn identity(&self) -> Result<AuthIdentity, CredentialRevoked> {
        Ok(AuthIdentity {
            role: self.role()?,
            username: self.credential.username.clone(),
        })
    }

    /// The user's current global role.
    pub fn role(&self) -> Result<Role, CredentialRevoked> {
        if self.is_revoked() {
            return Err(CredentialRevoked);
        }
        Ok(self.role.get())
    }

    pub fn is_revoked(&self) -> bool {
        self.credential.is_revoked()
    }

    /// A future that completes once the credential is revoked (immediately
    /// if it already is). Take one per connection and poll it for its
    /// lifetime: polling is lock-free, registering is not.
    pub fn revocation(&self) -> RevocationSignal {
        self.credential.watch()
    }
}

/// Completes when its credential is revoked. See [`Principal::revocation`].
#[derive(Debug)]
pub struct RevocationSignal(oneshot::Receiver<()>);

impl Future for RevocationSignal {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // The sender is dropped only after sending, by `revoke`.
        Pin::new(&mut self.0).poll(cx).map(|_| ())
    }
}

/// A user as stored in `_internal.users`.
#[derive(Debug, Clone)]
pub struct UserRecord {
    pub username: String,
    pub password_hash: String,
    pub role: Role,
}

/// An API key as stored in `_internal.api_keys`.
#[derive(Debug, Clone)]
pub struct ApiKeyRecord {
    pub label: String,
    pub key_hash: String,
    pub username: String,
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
    pub fn accept(self) -> Result<Principal, CredentialRevoked> {
        self.principal.identity()?;
        Ok(self.principal)
    }
}

/// Why an API key did not authenticate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyRejected {
    Unknown,
    OwnerNotFound,
}

impl fmt::Display for ApiKeyRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unknown => "Invalid API key",
            Self::OwnerNotFound => "API key owner not found",
        })
    }
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
}

#[derive(Debug, Default)]
struct Credentials {
    users: HashMap<String, User>,
    /// By key hash.
    keys: HashMap<String, ApiKey>,
    last_generation: u64,
}

impl Credentials {
    fn issue(&mut self, id: impl FnOnce(u64) -> CredentialId, username: &str) -> Arc<Credential> {
        self.last_generation += 1;
        Credential::new(id(self.last_generation), username)
    }

    /// Add a user, or give an existing one a new password generation and
    /// role. The role cell is kept: sessions on the user's API keys share it.
    fn upsert_user(&mut self, record: UserRecord) {
        let username = record.username;
        let password = self.issue(
            |generation| CredentialId::Password {
                username: username.clone(),
                generation,
            },
            &username,
        );
        match self.users.get_mut(&username) {
            Some(user) => {
                user.password.revoke();
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
        let label = record.label.clone();
        let credential = self.issue(
            |generation| CredentialId::ApiKey { label, generation },
            &record.username,
        );
        if let Some(previous) = self.keys.insert(record.key_hash, ApiKey { credential }) {
            previous.credential.revoke();
        }
    }

    /// Remove and revoke every key matching `matches`; returns how many.
    fn revoke_keys(&mut self, mut matches: impl FnMut(&Credential) -> bool) -> usize {
        let before = self.keys.len();
        self.keys.retain(|_, key| {
            let hit = matches(&key.credential);
            if hit {
                key.credential.revoke();
            }
            !hit
        });
        before - self.keys.len()
    }
}

/// In-memory index of every live credential. See the module docs.
#[derive(Debug, Default)]
pub struct CredentialRegistry {
    state: RwLock<Credentials>,
}

impl CredentialRegistry {
    /// Replace the whole index, revoking every credential it held.
    pub fn load(&self, users: Vec<UserRecord>, keys: Vec<ApiKeyRecord>) {
        let mut state = self.state.write();
        for user in state.users.values() {
            user.password.revoke();
        }
        state.revoke_keys(|_| true);
        state.users.clear();
        for user in users {
            state.upsert_user(user);
        }
        for key in keys {
            state.insert_key(key);
        }
    }

    /// Start a password login. `None` for an unknown user; callers still
    /// spend a dummy verification so unknown users cost the same.
    pub fn password_candidate(&self, username: &str) -> Option<PasswordCandidate> {
        let state = self.state.read();
        let user = state.users.get(username)?;
        Some(PasswordCandidate {
            password_hash: user.password_hash.clone(),
            principal: Principal {
                credential: Arc::clone(&user.password),
                role: Arc::clone(&user.role),
            },
        })
    }

    /// Authenticate the API key whose SHA-256 is `key_hash`.
    pub fn authenticate_key(&self, key_hash: &str) -> Result<Principal, ApiKeyRejected> {
        let state = self.state.read();
        let key = state.keys.get(key_hash).ok_or(ApiKeyRejected::Unknown)?;
        let owner = state
            .users
            .get(&key.credential.username)
            .ok_or(ApiKeyRejected::OwnerNotFound)?;
        Ok(Principal {
            credential: Arc::clone(&key.credential),
            role: Arc::clone(&owner.role),
        })
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
            user.password.revoke();
        }
        state.revoke_keys(|key| key.username == username);
    }

    /// Add an API key.
    pub fn put_key(&self, record: ApiKeyRecord) {
        self.state.write().insert_key(record);
    }

    /// Revoke the API key labelled `label`; `false` if there is none.
    pub fn revoke_key(&self, label: &str) -> bool {
        let mut state = self.state.write();
        state.revoke_keys(
            |key| matches!(&key.id, CredentialId::ApiKey { label: l, .. } if l == label),
        ) > 0
    }
}

#[cfg(test)]
#[path = "credentials_tests.rs"]
mod tests;
