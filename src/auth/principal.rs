//! One issued credential and the sessions bound to it.
//!
//! A [`Principal`] is an authenticated session's binding to exactly one
//! [`Credential`]: one API key, or one generation of a user's password. A
//! credential ends when it is revoked, or when its expiry passes. Ending is
//! one-way, and every session bound to the credential fails its next
//! [`Principal::identity`] check.
//!
//! A check is one atomic load on state the session already holds, plus one
//! clock read for a credential that has an expiry. An [`EndSignal`] is a
//! private one-shot channel per connection: no shared lock, lookup or storage
//! access per request or per frame.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use parking_lot::Mutex;
use tokio::sync::oneshot;

use super::{AuthIdentity, KeyScope, Role, ScopedKey};

/// Wall-clock time in Unix milliseconds.
pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

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

/// Why a credential no longer authorizes anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialEnded {
    /// Revoked: key revoked, password changed or user dropped.
    Revoked,
    /// Its expiry passed.
    Expired,
}

impl fmt::Display for CredentialEnded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Revoked => "Access denied: credential revoked",
            Self::Expired => "Access denied: credential expired",
        })
    }
}

impl std::error::Error for CredentialEnded {}

impl From<CredentialEnded> for String {
    fn from(e: CredentialEnded) -> Self {
        e.to_string()
    }
}

/// A user's current global role, shared by all of that user's credentials.
#[derive(Debug)]
pub(super) struct RoleCell(AtomicU8);

impl RoleCell {
    pub(super) fn new(role: Role) -> Arc<Self> {
        Arc::new(Self(AtomicU8::new(role as u8)))
    }

    pub(super) fn get(&self) -> Role {
        match self.0.load(Ordering::Acquire) {
            r if r == Role::Admin as u8 => Role::Admin,
            r if r == Role::Editor as u8 => Role::Editor,
            _ => Role::Viewer,
        }
    }

    pub(super) fn set(&self, role: Role) {
        self.0.store(role as u8, Ordering::Release);
    }
}

const LIVE: u8 = 0;
const REVOKED: u8 = 1;
const EXPIRED: u8 = 2;
/// `expires_at` of a credential that never expires.
const NEVER: u64 = u64::MAX;
/// `last_used_at` resolution: a busy key is written at most this often.
const TOUCH_RESOLUTION_MS: u64 = 1_000;

/// One issued credential.
#[derive(Debug)]
pub(super) struct Credential {
    pub(super) id: CredentialId,
    pub(super) username: String,
    /// `LIVE`, or why it ended.
    state: AtomicU8,
    /// Unix ms, or `NEVER`. Only ever moves earlier.
    expires_at: AtomicU64,
    /// Unix ms of the last authentication with it; 0 if never.
    last_used_at: AtomicU64,
    /// An API key's scope: the one KG it may use and its access there.
    scope: Option<Arc<KeyScope>>,
    /// One sender per live [`EndSignal`].
    watchers: Mutex<Vec<oneshot::Sender<()>>>,
}

impl Credential {
    pub(super) fn new(
        id: CredentialId,
        username: &str,
        expires_at: Option<u64>,
        last_used_at: Option<u64>,
        scope: Option<KeyScope>,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            username: username.to_string(),
            state: AtomicU8::new(LIVE),
            expires_at: AtomicU64::new(expires_at.unwrap_or(NEVER)),
            last_used_at: AtomicU64::new(last_used_at.unwrap_or(0)),
            scope: scope.map(Arc::new),
            watchers: Mutex::new(Vec::new()),
        })
    }

    /// End the credential for `reason`, unless it already ended. Completes
    /// every [`EndSignal`]; `false` if it had already ended.
    pub(super) fn end(&self, reason: CredentialEnded) -> bool {
        let state = match reason {
            CredentialEnded::Revoked => REVOKED,
            CredentialEnded::Expired => EXPIRED,
        };
        let mut watchers = self.watchers.lock();
        if self
            .state
            .compare_exchange(LIVE, state, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        for watcher in watchers.drain(..) {
            let _ = watcher.send(());
        }
        true
    }

    /// Why the credential no longer authorizes anything, if it doesn't.
    pub(super) fn ended(&self) -> Option<CredentialEnded> {
        match self.state.load(Ordering::Acquire) {
            LIVE => self
                .expires_at()
                .is_some_and(|expires_at| now_ms() >= expires_at)
                .then_some(CredentialEnded::Expired),
            REVOKED => Some(CredentialEnded::Revoked),
            _ => Some(CredentialEnded::Expired),
        }
    }

    pub(super) fn expires_at(&self) -> Option<u64> {
        Some(self.expires_at.load(Ordering::Acquire)).filter(|&at| at != NEVER)
    }

    /// Bring the expiry forward to `at`; a later `at` changes nothing.
    /// Waits for any [`Self::admit`] in progress, like [`Self::end`].
    pub(super) fn expire_at(&self, at: u64) {
        let _watchers = self.watchers.lock();
        self.expires_at.fetch_min(at, Ordering::AcqRel);
    }

    /// Run `enqueue` only while the credential is live, holding the lock
    /// [`Self::end`] and [`Self::expire_at`] take: neither completes until
    /// an `enqueue` already admitted has returned.
    fn admit<T>(&self, enqueue: impl FnOnce() -> T) -> Result<T, CredentialEnded> {
        let _watchers = self.watchers.lock();
        match self.ended() {
            Some(reason) => Err(reason),
            None => Ok(enqueue()),
        }
    }

    pub(super) fn scope(&self) -> Option<&KeyScope> {
        self.scope.as_deref()
    }

    pub(super) fn last_used_at(&self) -> Option<u64> {
        Some(self.last_used_at.load(Ordering::Relaxed)).filter(|&at| at != 0)
    }

    /// Record a use at `now`. Throttled: a credential used continuously is
    /// written once per `TOUCH_RESOLUTION_MS`, not once per use.
    pub(super) fn touch(&self, now: u64) {
        if now >= self.last_used_at.load(Ordering::Relaxed) + TOUCH_RESOLUTION_MS {
            self.last_used_at.fetch_max(now, Ordering::Relaxed);
        }
    }

    fn watch(&self) -> EndSignal {
        let (tx, rx) = oneshot::channel();
        let mut watchers = self.watchers.lock();
        // Checked under the lock `end` drains with, so no ending is missed.
        // An expiry nobody has ended yet is ended by the registry's sweep.
        if self.state.load(Ordering::Acquire) == LIVE {
            watchers.retain(|watcher| !watcher.is_closed());
            watchers.push(tx);
        } else {
            let _ = tx.send(());
        }
        EndSignal(Some(rx))
    }
}

/// An authenticated session's binding to one credential.
///
/// Cheap to clone; clones share the credential's state.
#[derive(Debug, Clone)]
pub struct Principal {
    credential: Arc<Credential>,
    role: Arc<RoleCell>,
}

impl Principal {
    pub(super) fn new(credential: Arc<Credential>, role: Arc<RoleCell>) -> Self {
        Self { credential, role }
    }

    pub fn username(&self) -> &str {
        &self.credential.username
    }

    pub fn credential(&self) -> &CredentialId {
        &self.credential.id
    }

    /// The permissions this principal holds right now, as an immutable
    /// snapshot for one request or one outbound message.
    pub fn identity(&self) -> Result<AuthIdentity, CredentialEnded> {
        Ok(AuthIdentity {
            role: self.role()?,
            username: self.credential.username.clone(),
            key_scope: self.credential.scope.clone().map(|scope| ScopedKey {
                scope,
                owner_role: self.role.get(),
            }),
        })
    }

    /// The user's current global role. A scoped API key never acts as an
    /// admin: its owner's admin role counts as editor, so its scope, not the
    /// owner's implicit ownership of every KG, decides what it may do.
    pub fn role(&self) -> Result<Role, CredentialEnded> {
        match self.ended() {
            Some(reason) => Err(reason),
            None => Ok(match self.role.get() {
                Role::Admin if self.credential.scope.is_some() => Role::Editor,
                role => role,
            }),
        }
    }

    /// For a scoped API key, its owner's own global role; `None` otherwise.
    pub fn key_owner_role(&self) -> Option<Role> {
        self.credential.scope.as_ref().map(|_| self.role.get())
    }

    /// Why the credential no longer authorizes anything, if it doesn't.
    pub fn ended(&self) -> Option<CredentialEnded> {
        self.credential.ended()
    }

    /// Run `enqueue` only while the credential is live, atomically with
    /// respect to revoking it or bringing its expiry forward: those wait
    /// until an admitted `enqueue` returns. Keep `enqueue` short and
    /// non-blocking.
    pub(crate) fn admit<T>(&self, enqueue: impl FnOnce() -> T) -> Result<T, CredentialEnded> {
        self.credential.admit(enqueue)
    }

    /// A future that completes once the credential is revoked or expired
    /// (immediately if it already is). Take one per connection and poll it
    /// for its lifetime: polling is lock-free, registering is not.
    pub fn end_signal(&self) -> EndSignal {
        self.credential.watch()
    }

    /// Registered end signals not yet completed or pruned.
    #[cfg(test)]
    pub(super) fn watchers(&self) -> usize {
        self.credential.watchers.lock().len()
    }
}

/// Completes when its credential ends. See [`Principal::end_signal`].
/// Once complete it stays complete: polling it again is `Ready`.
#[derive(Debug)]
pub struct EndSignal(Option<oneshot::Receiver<()>>);

impl Future for EndSignal {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let Some(receiver) = self.0.as_mut() else {
            return Poll::Ready(());
        };
        // The sender is dropped only after sending, by `end`.
        ready!(Pin::new(receiver).poll(cx)).ok();
        self.0 = None;
        Poll::Ready(())
    }
}
