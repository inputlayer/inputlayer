//! Authentication and Role-Based Access Control (RBAC)
//!
//! Provides role-based authorization for all IQL operations,
//! password hashing (argon2id), and API key management (SHA-256).

use crate::statement::{MetaCommand, Statement};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::{IpAddr, Ipv6Addr};
use std::path::Path;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

mod credentials;
mod principal;
pub(crate) mod stored;

pub use credentials::{
    ApiKeyInfo, ApiKeyRecord, ApiKeyRejected, ApiKeyTimes, CredentialRegistry, ExpireRejected,
    KeyUsage, PasswordCandidate, UserRecord,
};
pub use principal::{CredentialEnded, CredentialId, EndSignal, Principal};
pub use stored::stored_credentials;

/// Name of the internal knowledge graph used for auth data.
pub const INTERNAL_KG: &str = "_internal";

/// User roles with hierarchical permissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Admin,
    Editor,
    Viewer,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Role::Admin => write!(f, "admin"),
            Role::Editor => write!(f, "editor"),
            Role::Viewer => write!(f, "viewer"),
        }
    }
}

impl FromStr for Role {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "admin" => Ok(Role::Admin),
            "editor" => Ok(Role::Editor),
            "viewer" => Ok(Role::Viewer),
            _ => Err(format!(
                "Unknown role '{s}'. Valid roles: admin, editor, viewer"
            )),
        }
    }
}

/// Immutable permission snapshot: who is acting and with which global role,
/// taken from a live [`Principal`] for one request or one outbound message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthIdentity {
    pub username: String,
    pub role: Role,
}

// ── Password Hashing (argon2id) ─────────────────────────────────────────────

/// Hash a password using argon2id with a random salt.
pub fn hash_password(password: &str) -> Result<String, String> {
    use argon2::{
        password_hash::{rand_core::OsRng, SaltString},
        Argon2, PasswordHasher,
    };
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    argon2
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| format!("Password hashing failed: {e}"))
}

/// Verify a password against an argon2id hash.
pub fn verify_password(password: &str, hash: &str) -> bool {
    use argon2::{password_hash::PasswordHash, Argon2, PasswordVerifier};
    let parsed = match PasswordHash::new(hash) {
        Ok(h) => h,
        Err(_) => return false,
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

/// Verify `password` against `hash`, or against a dummy hash when there is
/// none, so unknown users cost the same as known ones. `false` without a hash.
pub fn verify_password_or_dummy(password: &str, hash: Option<&str>) -> bool {
    static DUMMY_HASH: LazyLock<String> =
        LazyLock::new(|| hash_password(&generate_api_key()).unwrap_or_default());
    let verified = verify_password(password, hash.unwrap_or(&DUMMY_HASH));
    verified && hash.is_some()
}

// ── Login Throttling ────────────────────────────────────────────────────────

/// Throttling key for `ip`. IPv6 addresses share their /64, which a single
/// host usually controls.
pub fn ip_bucket(ip: IpAddr) -> IpAddr {
    match ip.to_canonical() {
        IpAddr::V6(v6) => IpAddr::V6(Ipv6Addr::from(u128::from(v6) & !u128::from(u64::MAX))),
        v4 => v4,
    }
}

/// Failed logins allowed per IP or username before backoff starts.
const LOGIN_FREE_FAILURES: u32 = 5;
/// Longest login backoff.
const LOGIN_MAX_BACKOFF: Duration = Duration::from_secs(15 * 60);
/// Failures older than this are forgotten.
const LOGIN_FAILURE_TTL: Duration = Duration::from_secs(15 * 60);
/// How long a successful login exempts its IP from that user's backoff.
const LOGIN_KNOWN_IP_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);
/// Usernames are tracked by at most this many characters.
const LOGIN_USERNAME_KEY_CHARS: usize = 128;
/// Interval between sweeps of expired entries.
const LOGIN_PRUNE_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ThrottleKey {
    Ip(IpAddr),
    User(String),
}

#[derive(Debug, Clone, Copy)]
struct Failures {
    count: u32,
    last: Instant,
}

impl Failures {
    fn live(&self, now: Instant) -> bool {
        now.duration_since(self.last) < LOGIN_FAILURE_TTL
    }

    fn retry_after(&self, now: Instant) -> Option<Duration> {
        if !self.live(now) || self.count < LOGIN_FREE_FAILURES {
            return None;
        }
        let doublings = (self.count - LOGIN_FREE_FAILURES).min(20);
        let backoff = Duration::from_secs(1 << doublings).min(LOGIN_MAX_BACKOFF);
        (self.last + backoff)
            .checked_duration_since(now)
            .filter(|d| !d.is_zero())
    }

    fn record(&mut self, now: Instant) {
        if !self.live(now) {
            self.count = 0;
        }
        self.count += 1;
        self.last = now;
    }
}

/// Per-IP and per-username login failure counters with exponential backoff.
///
/// Every attempt counts as a failure until [`LoginThrottle::succeed`] is
/// called, so parallel attempts cannot slip past the limit. IPs are keyed
/// by [`ip_bucket`].
///
/// Lockout policy: a username's backoff applies from every IP except those
/// it logged in from successfully within the last 30 days, so failures
/// elsewhere cannot lock a user out of their usual address. API-key auth is
/// not throttled here and is the recovery path for a locked-out account.
#[derive(Debug)]
pub struct LoginThrottle {
    failures: dashmap::DashMap<ThrottleKey, Failures>,
    known_ips: dashmap::DashMap<(String, IpAddr), Instant>,
    last_prune: parking_lot::Mutex<Instant>,
}

impl Default for LoginThrottle {
    fn default() -> Self {
        Self {
            failures: dashmap::DashMap::new(),
            known_ips: dashmap::DashMap::new(),
            last_prune: parking_lot::Mutex::new(Instant::now()),
        }
    }
}

/// A begun login attempt. Settle it with [`LoginThrottle::fail`],
/// [`LoginThrottle::succeed`] or [`LoginThrottle::abort`].
#[derive(Debug)]
pub struct LoginAttempt {
    ip: IpAddr,
    username: String,
    at: Instant,
    prev_ip: Failures,
    /// `None` when the IP is known for this user and skips its backoff.
    prev_user: Option<Failures>,
}

impl LoginThrottle {
    /// Record a login attempt, or return how long `ip` / `username` must wait.
    pub fn begin(&self, ip: IpAddr, username: &str) -> Result<LoginAttempt, Duration> {
        let at = Instant::now();
        self.prune(at);
        let ip = ip_bucket(ip);
        let username: String = username.chars().take(LOGIN_USERNAME_KEY_CHARS).collect();
        let known = self
            .known_ips
            .get(&(username.clone(), ip))
            .is_some_and(|t| at.duration_since(*t) < LOGIN_KNOWN_IP_TTL);
        let prev_ip = self.try_record(ThrottleKey::Ip(ip), at)?;
        let prev_user = if known {
            None
        } else {
            match self.try_record(ThrottleKey::User(username.clone()), at) {
                Ok(prev) => Some(prev),
                Err(wait) => {
                    self.restore(&ThrottleKey::Ip(ip), prev_ip, at);
                    return Err(wait);
                }
            }
        };
        Ok(LoginAttempt {
            ip,
            username,
            at,
            prev_ip,
            prev_user,
        })
    }

    /// The attempt failed: its backoff runs from now.
    pub fn fail(&self, attempt: &LoginAttempt) {
        let now = Instant::now();
        for (key, _) in Self::keys(attempt) {
            if let Some(mut f) = self.failures.get_mut(&key) {
                f.last = now;
            }
        }
    }

    /// The attempt succeeded: clear the username's failures, refund the IP
    /// failure and exempt the IP from this user's future backoff.
    pub fn succeed(&self, attempt: &LoginAttempt) {
        self.failures
            .remove(&ThrottleKey::User(attempt.username.clone()));
        self.restore(&ThrottleKey::Ip(attempt.ip), attempt.prev_ip, attempt.at);
        self.known_ips
            .insert((attempt.username.clone(), attempt.ip), Instant::now());
    }

    /// The attempt never checked a password: undo it.
    pub fn abort(&self, attempt: &LoginAttempt) {
        for (key, prev) in Self::keys(attempt) {
            self.restore(&key, prev, attempt.at);
        }
    }

    fn keys(attempt: &LoginAttempt) -> impl Iterator<Item = (ThrottleKey, Failures)> + '_ {
        std::iter::once((ThrottleKey::Ip(attempt.ip), attempt.prev_ip)).chain(
            attempt
                .prev_user
                .map(|prev| (ThrottleKey::User(attempt.username.clone()), prev)),
        )
    }

    /// Record a failure under `key`, returning the previous state.
    fn try_record(&self, key: ThrottleKey, now: Instant) -> Result<Failures, Duration> {
        let mut entry = self.failures.entry(key).or_insert(Failures {
            count: 0,
            last: now,
        });
        if let Some(wait) = entry.retry_after(now) {
            return Err(wait);
        }
        let prev = *entry;
        entry.record(now);
        Ok(prev)
    }

    /// Undo a failure recorded at `at`.
    fn restore(&self, key: &ThrottleKey, prev: Failures, at: Instant) {
        if let Some(mut f) = self.failures.get_mut(key) {
            f.count = f.count.saturating_sub(1);
            if f.last == at {
                f.last = prev.last;
            }
        }
    }

    fn prune(&self, now: Instant) {
        let Some(mut last) = self.last_prune.try_lock() else {
            return;
        };
        if now.duration_since(*last) >= LOGIN_PRUNE_INTERVAL {
            *last = now;
            drop(last);
            self.failures.retain(|_, f| f.live(now) && f.count > 0);
            self.known_ips
                .retain(|_, t| now.duration_since(*t) < LOGIN_KNOWN_IP_TTL);
        }
    }

    #[cfg(test)]
    fn backdate(&self, by: Duration) {
        for mut f in self.failures.iter_mut() {
            f.last -= by;
        }
    }
}

// ── API Key Hashing (SHA-256) ───────────────────────────────────────────────

/// Hash an API key using SHA-256 for fast lookup.
pub fn hash_api_key(key: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Generate a random API key (32 bytes → 64 hex characters).
pub fn generate_api_key() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
    use std::fmt::Write;
    let mut hex = String::with_capacity(64);
    for b in &bytes {
        let _ = write!(hex, "{b:02x}");
    }
    hex
}

// ── Credential Persistence ──────────────────────────────────────────────────

/// Generated credentials persisted to a TOML file for reuse across restarts.
#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedCredentials {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
}

impl PersistedCredentials {
    /// Load credentials from a TOML file. Returns `None` if the file doesn't exist.
    pub fn load(path: &Path) -> Option<Self> {
        let contents = std::fs::read_to_string(path).ok()?;
        toml::from_str(&contents).ok()
    }

    /// Atomically replace the file. On Unix it is owner-only (0600) from
    /// creation: a fresh temp file is created with `create_new` and renamed.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        use std::io::Write;

        let contents =
            toml::to_string_pretty(self).map_err(|e| std::io::Error::other(e.to_string()))?;
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)?;
        }
        let mut tmp_name = path.file_name().unwrap_or_default().to_os_string();
        tmp_name.push(".tmp");
        let tmp = path.with_file_name(tmp_name);
        let _ = std::fs::remove_file(&tmp);

        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let result = options.open(&tmp).and_then(|mut file| {
            file.write_all(contents.as_bytes())?;
            file.sync_all()?;
            std::fs::rename(&tmp, path)
        });
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        result
    }
}

// ── Per-KG Authorization (ACLs) ─────────────────────────────────────────────

/// Per-KG role controlling access to a specific knowledge graph.
/// Separate from the global `Role` - both must pass for an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KgRole {
    /// Full control: read, write, schema, drop, grant/revoke access
    Owner,
    /// Write access: read, write, schema modifications
    Editor,
    /// Read-only access: queries only
    Viewer,
}

impl fmt::Display for KgRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KgRole::Owner => write!(f, "owner"),
            KgRole::Editor => write!(f, "editor"),
            KgRole::Viewer => write!(f, "viewer"),
        }
    }
}

impl FromStr for KgRole {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "owner" => Ok(KgRole::Owner),
            "editor" => Ok(KgRole::Editor),
            "viewer" => Ok(KgRole::Viewer),
            _ => Err(format!(
                "Unknown KG role '{s}'. Valid roles: owner, editor, viewer"
            )),
        }
    }
}

/// Check whether a KG role permits a given statement on that KG.
/// Called AFTER the global `authorize_statement()` check passes.
pub fn authorize_kg_operation(kg_role: &KgRole, stmt: &Statement) -> Result<(), String> {
    match kg_role {
        KgRole::Owner => Ok(()), // Owner can do everything on their KG
        KgRole::Editor => authorize_kg_editor(stmt),
        KgRole::Viewer => authorize_kg_viewer(stmt),
    }
}

fn authorize_kg_editor(stmt: &Statement) -> Result<(), String> {
    match stmt {
        // KG editors can read, write, and manage schema
        Statement::Query(_)
        | Statement::Insert(_)
        | Statement::Delete(_)
        | Statement::Update(_)
        | Statement::PersistentRule(_)
        | Statement::SessionRule(_)
        | Statement::Fact(_)
        | Statement::SchemaDecl(_)
        | Statement::TypeDecl(_)
        | Statement::DeleteRelationOrRule(_) => Ok(()),

        Statement::Meta(cmd) => match cmd {
            // KG editors cannot drop KGs or manage ACLs (Owner only)
            MetaCommand::KgDrop(_) => {
                Err("Permission denied: only KG owners can drop this knowledge graph".to_string())
            }
            MetaCommand::KgAclGrant { .. } | MetaCommand::KgAclRevoke { .. } => {
                Err("Permission denied: only KG owners can manage ACLs".to_string())
            }
            // KG navigation
            MetaCommand::KgShow
            | MetaCommand::KgList
            | MetaCommand::KgUse(_)
            | MetaCommand::KgCreate(_) => Ok(()),
            // Relation/rule management
            MetaCommand::RelList
            | MetaCommand::RelDescribe(_)
            | MetaCommand::RelDrop(_)
            | MetaCommand::RuleList
            | MetaCommand::RuleQuery(_)
            | MetaCommand::RuleShowDef(_)
            | MetaCommand::RuleDrop(_)
            | MetaCommand::RuleDropPrefix(_)
            | MetaCommand::RuleEdit { .. }
            | MetaCommand::RuleClear(_)
            | MetaCommand::RuleRemove { .. } => Ok(()),
            // Index management
            MetaCommand::IndexList
            | MetaCommand::IndexCreate(_)
            | MetaCommand::IndexDrop(_)
            | MetaCommand::IndexStats(_)
            | MetaCommand::IndexRebuild(_) => Ok(()),
            // Data loading/clearing
            MetaCommand::ClearPrefix(_) | MetaCommand::Load { .. } => Ok(()),
            // Ontology lifecycle: rule/relation deployment, editor-level
            MetaCommand::OntologyInstall(_)
            | MetaCommand::OntologyRemove(_)
            | MetaCommand::OntologyUpgrade(_) => Ok(()),
            // ACL list (read-only)
            MetaCommand::KgAclList(_) => Ok(()),
            // Session commands (ephemeral)
            MetaCommand::SessionList
            | MetaCommand::SessionClear
            | MetaCommand::SessionDrop(_)
            | MetaCommand::SessionDropName(_) => Ok(()),
            // Read-only system commands
            MetaCommand::Debug(_)
            | MetaCommand::Why(_)
            | MetaCommand::WhyFull(_)
            | MetaCommand::WhyNot(_)
            | MetaCommand::Subscribe { .. }
            | MetaCommand::Unsubscribe(_)
            | MetaCommand::Status
            | MetaCommand::Help
            | MetaCommand::Quit => Ok(()),
            // System administration (admin only, should not reach per-KG check)
            MetaCommand::Compact
            | MetaCommand::Backup(_)
            | MetaCommand::BackupStatus
            | MetaCommand::UserList
            | MetaCommand::UserCreate { .. }
            | MetaCommand::UserDrop(_)
            | MetaCommand::UserPassword { .. }
            | MetaCommand::UserRole { .. }
            | MetaCommand::ApiKeyCreate { .. }
            | MetaCommand::ApiKeyList
            | MetaCommand::ApiKeyRevoke(_)
            | MetaCommand::ApiKeyExpire { .. } => {
                Err("Permission denied: only admins can perform this operation".to_string())
            }
        },
    }
}

fn authorize_kg_viewer(stmt: &Statement) -> Result<(), String> {
    match stmt {
        Statement::Query(_) | Statement::SessionRule(_) => Ok(()),

        Statement::Insert(_)
        | Statement::Delete(_)
        | Statement::Update(_)
        | Statement::PersistentRule(_)
        | Statement::Fact(_)
        | Statement::SchemaDecl(_)
        | Statement::TypeDecl(_)
        | Statement::DeleteRelationOrRule(_) => {
            Err("Permission denied: you have viewer access to this knowledge graph".to_string())
        }

        Statement::Meta(cmd) => match cmd {
            // Read-only operations
            MetaCommand::KgShow
            | MetaCommand::KgList
            | MetaCommand::KgUse(_)
            | MetaCommand::RelList
            | MetaCommand::RelDescribe(_)
            | MetaCommand::RuleList
            | MetaCommand::RuleQuery(_)
            | MetaCommand::RuleShowDef(_)
            | MetaCommand::IndexList
            | MetaCommand::IndexStats(_)
            | MetaCommand::Debug(_)
            | MetaCommand::Why(_)
            | MetaCommand::WhyFull(_)
            | MetaCommand::WhyNot(_)
            | MetaCommand::Subscribe { .. }
            | MetaCommand::Unsubscribe(_)
            | MetaCommand::Status
            | MetaCommand::Help
            | MetaCommand::Quit
            | MetaCommand::KgAclList(_) => Ok(()),
            // Session commands (ephemeral, per-connection)
            MetaCommand::SessionList
            | MetaCommand::SessionClear
            | MetaCommand::SessionDrop(_)
            | MetaCommand::SessionDropName(_) => Ok(()),
            _ => {
                Err("Permission denied: you have viewer access to this knowledge graph".to_string())
            }
        },
    }
}

// ── Authorization ───────────────────────────────────────────────────────────
//
// Two-layer authorization model:
//
//   Layer 1 - Global role (`authorize_statement`):
//     Gates system-level operations only: user management, API keys, compaction,
//     and KG creation (editors can create, viewers cannot).
//     All data operations (insert, delete, rules, schema, ACL) pass through
//     to Layer 2 regardless of global role.
//
//   Layer 2 - Per-KG role (`authorize_kg_operation`):
//     Gates all operations on a specific knowledge graph. The KG role (Owner,
//     Editor, Viewer) determines what the user can do within that KG.
//     This is the authority for data access - not the global role.
//
// This separation means a global Viewer who is a KG Owner can fully manage
// their KG, and a global Editor who is a KG Viewer can only read that KG.

/// Check whether a global role is authorized to execute a given statement.
/// This only gates system-level operations. Data/KG-scoped operations are
/// always passed through here and gated by per-KG authorization instead.
pub fn authorize_statement(role: &Role, stmt: &Statement) -> Result<(), String> {
    match role {
        Role::Admin => Ok(()),
        Role::Editor | Role::Viewer => authorize_non_admin(role, stmt),
    }
}

/// Authorization for non-admin users (editors and viewers).
/// Only blocks system-level operations. Data operations are deferred to per-KG auth.
fn authorize_non_admin(role: &Role, stmt: &Statement) -> Result<(), String> {
    match stmt {
        // All data operations are deferred to per-KG authorization.
        // The per-KG role (Owner/Editor/Viewer) determines access.
        Statement::Query(_)
        | Statement::Insert(_)
        | Statement::Delete(_)
        | Statement::Update(_)
        | Statement::PersistentRule(_)
        | Statement::SessionRule(_)
        | Statement::Fact(_)
        | Statement::SchemaDecl(_)
        | Statement::TypeDecl(_)
        | Statement::DeleteRelationOrRule(_) => Ok(()),

        Statement::Meta(cmd) => authorize_non_admin_meta(role, cmd),
    }
}

/// Meta command authorization for non-admin users.
fn authorize_non_admin_meta(role: &Role, cmd: &MetaCommand) -> Result<(), String> {
    match cmd {
        // KG lifecycle: editors can create, viewers cannot.
        // Drop is deferred to per-KG auth (requires Owner).
        MetaCommand::KgCreate(_) => {
            if *role == Role::Viewer {
                Err("Permission denied: viewers cannot create knowledge graphs".to_string())
            } else {
                Ok(())
            }
        }
        MetaCommand::KgDrop(_) => Ok(()), // per-KG Owner check enforces this

        // KG navigation - all roles
        MetaCommand::KgShow | MetaCommand::KgList | MetaCommand::KgUse(_) => Ok(()),

        // Data operations on relations/rules - deferred to per-KG auth
        MetaCommand::RelList
        | MetaCommand::RelDescribe(_)
        | MetaCommand::RelDrop(_)
        | MetaCommand::RuleList
        | MetaCommand::RuleQuery(_)
        | MetaCommand::RuleShowDef(_)
        | MetaCommand::RuleDrop(_)
        | MetaCommand::RuleDropPrefix(_)
        | MetaCommand::RuleEdit { .. }
        | MetaCommand::RuleClear(_)
        | MetaCommand::RuleRemove { .. } => Ok(()),

        // Ontology lifecycle writes rules and relations: viewers denied,
        // editors and above deferred to per-KG auth.
        MetaCommand::OntologyInstall(_)
        | MetaCommand::OntologyRemove(_)
        | MetaCommand::OntologyUpgrade(_) => {
            if *role == Role::Viewer {
                Err("Permission denied: viewers cannot manage ontologies".to_string())
            } else {
                Ok(())
            }
        }

        // Index management - deferred to per-KG auth
        MetaCommand::IndexList
        | MetaCommand::IndexCreate(_)
        | MetaCommand::IndexDrop(_)
        | MetaCommand::IndexStats(_)
        | MetaCommand::IndexRebuild(_) => Ok(()),

        // Data loading/clearing - deferred to per-KG auth
        MetaCommand::ClearPrefix(_) | MetaCommand::Load { .. } => Ok(()),

        // ACL management - deferred to per-KG auth (requires Owner)
        MetaCommand::KgAclList(_)
        | MetaCommand::KgAclGrant { .. }
        | MetaCommand::KgAclRevoke { .. } => Ok(()),

        // Session commands - always allowed (ephemeral, per-connection)
        MetaCommand::SessionList
        | MetaCommand::SessionClear
        | MetaCommand::SessionDrop(_)
        | MetaCommand::SessionDropName(_) => Ok(()),

        // Read-only system commands - all roles
        MetaCommand::Debug(_)
        | MetaCommand::Why(_)
        | MetaCommand::WhyFull(_)
        | MetaCommand::WhyNot(_)
        | MetaCommand::Subscribe { .. }
        | MetaCommand::Unsubscribe(_)
        | MetaCommand::Status
        | MetaCommand::Help
        | MetaCommand::Quit => Ok(()),

        // System administration - admin only
        MetaCommand::Compact => Err("Permission denied: only admins can compact".to_string()),
        MetaCommand::Backup(_) | MetaCommand::BackupStatus => {
            Err("Permission denied: only admins can back up the server".to_string())
        }
        MetaCommand::UserList
        | MetaCommand::UserCreate { .. }
        | MetaCommand::UserDrop(_)
        | MetaCommand::UserPassword { .. }
        | MetaCommand::UserRole { .. } => {
            Err("Permission denied: only admins can manage users".to_string())
        }
        MetaCommand::ApiKeyCreate { .. }
        | MetaCommand::ApiKeyList
        | MetaCommand::ApiKeyRevoke(_)
        | MetaCommand::ApiKeyExpire { .. } => {
            Err("Permission denied: only admins can manage API keys".to_string())
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_role_display() {
        assert_eq!(Role::Admin.to_string(), "admin");
        assert_eq!(Role::Editor.to_string(), "editor");
        assert_eq!(Role::Viewer.to_string(), "viewer");
    }

    #[test]
    fn test_role_from_str() {
        assert_eq!(Role::from_str("admin").unwrap(), Role::Admin);
        assert_eq!(Role::from_str("EDITOR").unwrap(), Role::Editor);
        assert_eq!(Role::from_str("Viewer").unwrap(), Role::Viewer);
        assert!(Role::from_str("unknown").is_err());
    }

    #[test]
    fn test_role_serde_roundtrip() {
        let json = serde_json::to_string(&Role::Editor).unwrap();
        assert_eq!(json, "\"editor\"");
        let back: Role = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Role::Editor);
    }

    #[test]
    fn test_hash_and_verify_password() {
        let hash = hash_password("mypassword").unwrap();
        assert!(verify_password("mypassword", &hash));
        assert!(!verify_password("wrongpassword", &hash));
    }

    #[test]
    fn test_hash_password_unique_salts() {
        let h1 = hash_password("same").unwrap();
        let h2 = hash_password("same").unwrap();
        assert_ne!(h1, h2); // Different salts
        assert!(verify_password("same", &h1));
        assert!(verify_password("same", &h2));
    }

    #[test]
    fn test_verify_password_invalid_hash() {
        assert!(!verify_password("any", "not-a-valid-hash"));
    }

    #[test]
    fn test_hash_api_key_deterministic() {
        let h1 = hash_api_key("my-key-123");
        let h2 = hash_api_key("my-key-123");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_api_key_different_keys() {
        let h1 = hash_api_key("key-a");
        let h2 = hash_api_key("key-b");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_generate_api_key_length() {
        let key = generate_api_key();
        assert_eq!(key.len(), 64); // 32 bytes * 2 hex chars
    }

    #[test]
    fn test_generate_api_key_uniqueness() {
        let k1 = generate_api_key();
        let k2 = generate_api_key();
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_admin_can_do_everything() {
        use crate::statement::parse_statement;
        let stmts = vec![
            "?edge(X, Y)",
            "+edge(1, 2)",
            "-edge(1, 2)",
            ".kg create test",
            ".kg drop test",
            ".compact",
            ".backup",
            ".backup status",
            ".user list",
            ".apikey list",
            ".apikey expire mykey 1h",
        ];
        for s in stmts {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_statement(&Role::Admin, &stmt).is_ok(),
                "Admin should be allowed: {s}"
            );
        }
    }

    #[test]
    fn test_editor_global_auth() {
        use crate::statement::parse_statement;
        // Global auth passes all data/KG ops through to per-KG auth
        let allowed = vec![
            ".kg create test",
            ".kg drop test",
            "+edge(1, 2)",
            "-edge(1, 2)",
            ".kg acl grant mykg bob editor",
            ".kg acl revoke mykg bob",
        ];
        for s in allowed {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_statement(&Role::Editor, &stmt).is_ok(),
                "Editor should pass global auth: {s}"
            );
        }
        // System operations remain admin-only
        let denied = vec![
            ".compact",
            ".backup",
            ".backup status",
            ".user list",
            ".user create bob pass editor",
            ".apikey create mykey",
            ".apikey create mykey 30d",
            ".apikey expire mykey 1h",
        ];
        for s in denied {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_statement(&Role::Editor, &stmt).is_err(),
                "Editor should be denied at global level: {s}"
            );
        }
    }

    #[test]
    fn test_viewer_global_auth() {
        use crate::statement::parse_statement;
        // Data ops pass global auth for viewers (per-KG auth is the authority)
        let allowed = vec![
            "?edge(X, Y)",
            "+edge(1, 2)",
            "-edge(1, 2)",
            ".rel",
            ".rule",
            ".kg list",
            ".kg drop test",
            ".status",
            ".session",
            ".kg acl grant mykg bob editor",
        ];
        for s in allowed {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_statement(&Role::Viewer, &stmt).is_ok(),
                "Viewer should pass global auth: {s}"
            );
        }

        // Only KG creation and system ops are blocked at global level
        let denied = vec![
            ".kg create test",
            ".compact",
            ".backup",
            ".backup nightly",
            ".backup status",
            ".user list",
            ".apikey list",
            ".apikey expire mykey 0s",
        ];
        for s in denied {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_statement(&Role::Viewer, &stmt).is_err(),
                "Viewer should be denied at global level: {s}"
            );
        }
    }

    // ── KG Role tests ─────────────────────────────────────────────────

    #[test]
    fn test_kg_role_display() {
        assert_eq!(KgRole::Owner.to_string(), "owner");
        assert_eq!(KgRole::Editor.to_string(), "editor");
        assert_eq!(KgRole::Viewer.to_string(), "viewer");
    }

    #[test]
    fn test_kg_role_from_str() {
        assert_eq!(KgRole::from_str("owner").unwrap(), KgRole::Owner);
        assert_eq!(KgRole::from_str("EDITOR").unwrap(), KgRole::Editor);
        assert_eq!(KgRole::from_str("Viewer").unwrap(), KgRole::Viewer);
        assert!(KgRole::from_str("unknown").is_err());
    }

    #[test]
    fn test_kg_owner_can_do_everything() {
        use crate::statement::parse_statement;
        let stmts = vec![
            "?edge(X, Y)",
            "+edge(1, 2)",
            "-edge(1, 2)",
            ".kg drop test",
            ".rel drop edges",
        ];
        for s in stmts {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_kg_operation(&KgRole::Owner, &stmt).is_ok(),
                "KG Owner should be allowed: {s}"
            );
        }
    }

    #[test]
    fn test_kg_editor_cannot_drop_or_manage_acls() {
        use crate::statement::parse_statement;
        let denied = vec![".kg drop mykg"];
        for s in denied {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_kg_operation(&KgRole::Editor, &stmt).is_err(),
                "KG Editor should be denied: {s}"
            );
        }
    }

    #[test]
    fn test_kg_editor_can_write() {
        use crate::statement::parse_statement;
        let allowed = vec!["?edge(X, Y)", "+edge(1, 2)", "-edge(1, 2)", ".rel"];
        for s in allowed {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_kg_operation(&KgRole::Editor, &stmt).is_ok(),
                "KG Editor should be allowed: {s}"
            );
        }
    }

    #[test]
    fn test_kg_viewer_cannot_write() {
        use crate::statement::parse_statement;
        let denied = vec!["+edge(1, 2)", "-edge(1, 2)", ".rel drop edges"];
        for s in denied {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_kg_operation(&KgRole::Viewer, &stmt).is_err(),
                "KG Viewer should be denied: {s}"
            );
        }
    }

    #[test]
    fn test_kg_viewer_can_read() {
        use crate::statement::parse_statement;
        let allowed = vec!["?edge(X, Y)", ".rel", ".rule"];
        for s in allowed {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_kg_operation(&KgRole::Viewer, &stmt).is_ok(),
                "KG Viewer should be allowed: {s}"
            );
        }
    }

    #[test]
    fn test_acl_commands_global_auth() {
        use crate::statement::parse_statement;
        // ACL ops pass global auth for all roles (per-KG Owner check is the authority)
        let acl_ops = vec![
            ".kg acl list mykg",
            ".kg acl grant mykg bob editor",
            ".kg acl revoke mykg bob",
        ];
        for s in &acl_ops {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_statement(&Role::Admin, &stmt).is_ok(),
                "Admin: {s}"
            );
            assert!(
                authorize_statement(&Role::Editor, &stmt).is_ok(),
                "Editor: {s}"
            );
            assert!(
                authorize_statement(&Role::Viewer, &stmt).is_ok(),
                "Viewer: {s}"
            );
        }
        // But per-KG auth still gates these (tested in KG role tests)
    }

    #[test]
    fn test_persisted_credentials_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds.toml");

        let creds = PersistedCredentials {
            admin_password: Some("test-pass-123".to_string()),
            api_key: Some("test-key-456".to_string()),
        };
        creds.save(&path).unwrap();

        let loaded = PersistedCredentials::load(&path).unwrap();
        assert_eq!(loaded, creds);
    }

    #[cfg(unix)]
    #[test]
    fn test_persisted_credentials_owner_only_without_temp_leftover() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("creds.toml");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path.with_file_name("creds.toml.tmp"), "stale").unwrap();
        let creds = PersistedCredentials {
            admin_password: None,
            api_key: Some("k".to_string()),
        };
        creds.save(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        assert!(!path.with_file_name("creds.toml.tmp").exists());
        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(!contents.contains("admin_password"), "{contents}");
        assert_eq!(PersistedCredentials::load(&path).unwrap(), creds);
    }

    #[test]
    fn test_verify_password_or_dummy() {
        let hash = hash_password("pw").unwrap();
        assert!(verify_password_or_dummy("pw", Some(&hash)));
        assert!(!verify_password_or_dummy("nope", Some(&hash)));
        assert!(!verify_password_or_dummy("pw", None));
        assert!(!verify_password_or_dummy("", None));
    }

    fn ip(last: u8) -> IpAddr {
        IpAddr::from([192, 0, 2, last])
    }

    #[test]
    fn test_login_throttle_backs_off_per_ip() {
        let throttle = LoginThrottle::default();
        for i in 0..LOGIN_FREE_FAILURES {
            throttle.begin(ip(1), &format!("user{i}")).unwrap();
        }
        let wait = throttle.begin(ip(1), "another").unwrap_err();
        assert!(wait <= Duration::from_secs(1), "{wait:?}");
        // Other IPs are unaffected.
        throttle.begin(ip(2), "another").unwrap();
        throttle.backdate(Duration::from_secs(2));
        throttle.begin(ip(1), "another").unwrap();
        let wait = throttle.begin(ip(1), "another").unwrap_err();
        assert!(wait > Duration::from_secs(1), "backoff doubles: {wait:?}");
    }

    #[test]
    fn test_login_throttle_backs_off_per_username() {
        let throttle = LoginThrottle::default();
        for i in 0..LOGIN_FREE_FAILURES {
            throttle.begin(ip(i as u8), "admin").unwrap();
        }
        assert!(throttle.begin(ip(100), "admin").is_err());
        // The rejected attempt does not count against the new IP.
        for i in 0..LOGIN_FREE_FAILURES {
            throttle.begin(ip(100), &format!("u{i}")).unwrap();
        }
    }

    #[test]
    fn test_login_throttle_success_resets_username_and_refunds_ip() {
        let throttle = LoginThrottle::default();
        for _ in 0..10 {
            let attempt = throttle.begin(ip(1), "admin").unwrap();
            throttle.succeed(&attempt);
        }
        throttle.begin(ip(1), "admin").unwrap();
    }

    #[test]
    fn test_login_throttle_known_ip_skips_username_backoff() {
        let throttle = LoginThrottle::default();
        let attempt = throttle.begin(ip(1), "admin").unwrap();
        throttle.succeed(&attempt);
        for i in 0..LOGIN_FREE_FAILURES {
            let attempt = throttle.begin(ip(10 + i as u8), "admin").unwrap();
            throttle.fail(&attempt);
        }
        assert!(throttle.begin(ip(100), "admin").is_err());
        let attempt = throttle.begin(ip(1), "admin").unwrap();
        throttle.fail(&attempt);
        assert!(
            throttle.begin(ip(100), "admin").is_err(),
            "known-IP failures do not extend the username backoff"
        );
        throttle.begin(ip(1), "admin").unwrap();
    }

    #[test]
    fn test_login_throttle_username_rejection_keeps_ip_backoff() {
        let throttle = LoginThrottle::default();
        let failures = |count| Failures {
            count,
            last: Instant::now(),
        };
        throttle
            .failures
            .insert(ThrottleKey::Ip(ip(1)), failures(LOGIN_FREE_FAILURES));
        throttle.backdate(Duration::from_secs(2));
        throttle
            .failures
            .insert(ThrottleKey::User("admin".into()), failures(10));
        assert!(throttle.begin(ip(1), "admin").is_err());
        throttle.begin(ip(1), "other").unwrap();
    }

    #[test]
    fn test_login_throttle_abort_undoes_attempt() {
        let throttle = LoginThrottle::default();
        for _ in 0..LOGIN_FREE_FAILURES * 2 {
            let attempt = throttle.begin(ip(1), "admin").unwrap();
            throttle.abort(&attempt);
        }
        throttle.begin(ip(1), "admin").unwrap();
    }

    #[test]
    fn test_ip_bucket_groups_ipv6_by_64() {
        let v6 = |s: &str| s.parse::<IpAddr>().unwrap();
        assert_eq!(ip_bucket(v6("2001:db8::1")), v6("2001:db8::"));
        assert_eq!(
            ip_bucket(v6("2001:db8::ffff:1")),
            ip_bucket(v6("2001:db8::abcd"))
        );
        assert_ne!(
            ip_bucket(v6("2001:db8::1")),
            ip_bucket(v6("2001:db8:0:1::1"))
        );
        assert_eq!(ip_bucket(v6("::ffff:192.0.2.7")), ip(7));
        assert_eq!(ip_bucket(ip(7)), ip(7));

        let throttle = LoginThrottle::default();
        for i in 0..LOGIN_FREE_FAILURES {
            throttle
                .begin(v6(&format!("2001:db8::{i}")), &format!("u{i}"))
                .unwrap();
        }
        assert!(throttle.begin(v6("2001:db8::99"), "other").is_err());
    }

    #[test]
    fn test_login_throttle_forgets_old_failures() {
        let throttle = LoginThrottle::default();
        for _ in 0..LOGIN_FREE_FAILURES {
            let _ = throttle.begin(ip(1), "admin");
        }
        assert!(throttle.begin(ip(1), "admin").is_err());
        throttle.backdate(LOGIN_FAILURE_TTL);
        *throttle.last_prune.lock() -= LOGIN_PRUNE_INTERVAL;
        throttle.begin(ip(1), "admin").unwrap();
        assert_eq!(throttle.failures.len(), 2, "expired entries are pruned");
    }

    #[test]
    fn test_login_throttle_bounds_username_keys() {
        let throttle = LoginThrottle::default();
        throttle.begin(ip(1), &"x".repeat(10_000)).unwrap();
        let longest = throttle
            .failures
            .iter()
            .filter_map(|e| match e.key() {
                ThrottleKey::User(u) => Some(u.len()),
                ThrottleKey::Ip(_) => None,
            })
            .max();
        assert_eq!(longest, Some(LOGIN_USERNAME_KEY_CHARS));
    }

    #[test]
    fn test_persisted_credentials_load_nonexistent() {
        let result = PersistedCredentials::load(Path::new("/nonexistent/creds.toml"));
        assert!(result.is_none());
    }

    #[test]
    fn test_persisted_credentials_load_invalid_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, "not valid { toml }").unwrap();

        let result = PersistedCredentials::load(&path);
        assert!(result.is_none());
    }
}

#[cfg(test)]
mod ontology_auth_tests {
    use super::*;

    #[test]
    fn viewers_cannot_manage_ontologies() {
        let cmd = MetaCommand::OntologyInstall("x".to_string());
        assert!(authorize_non_admin_meta(&Role::Viewer, &cmd).is_err());
        assert!(authorize_non_admin_meta(&Role::Editor, &cmd).is_ok());
        let cmd = MetaCommand::OntologyRemove("x".to_string());
        assert!(authorize_non_admin_meta(&Role::Viewer, &cmd).is_err());
    }
}
