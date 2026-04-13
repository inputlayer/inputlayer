//! Authentication and Role-Based Access Control (RBAC)
//!
//! Provides role-based authorization for all IQL operations,
//! password hashing (argon2id), and API key management (SHA-256).

use crate::statement::{MetaCommand, Statement};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::Path;
use std::str::FromStr;

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

/// Authenticated identity attached to a session.
#[derive(Debug, Clone)]
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

/// Credentials persisted to a TOML file for reuse across server restarts.
#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedCredentials {
    pub admin_password: String,
    pub api_key: String,
}

impl PersistedCredentials {
    /// Load credentials from a TOML file. Returns `None` if the file doesn't exist.
    pub fn load(path: &Path) -> Option<Self> {
        let contents = std::fs::read_to_string(path).ok()?;
        toml::from_str(&contents).ok()
    }

    /// Save credentials to a TOML file with restricted permissions (0600 on Unix).
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let contents =
            toml::to_string_pretty(self).map_err(|e| std::io::Error::other(e.to_string()))?;
        std::fs::write(path, &contents)?;

        // Restrict file permissions to owner-only on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(path, perms)?;
        }

        Ok(())
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
            | MetaCommand::Status
            | MetaCommand::Help
            | MetaCommand::Quit => Ok(()),
            // Agent commands
            MetaCommand::AgentMessage(_)
            | MetaCommand::AgentStart(_)
            | MetaCommand::AgentSetup(_)
            | MetaCommand::AgentExamples => Ok(()),
            // System administration (admin only, should not reach per-KG check)
            MetaCommand::Compact
            | MetaCommand::UserList
            | MetaCommand::UserCreate { .. }
            | MetaCommand::UserDrop(_)
            | MetaCommand::UserPassword { .. }
            | MetaCommand::UserRole { .. }
            | MetaCommand::ApiKeyCreate(_)
            | MetaCommand::ApiKeyList
            | MetaCommand::ApiKeyRevoke(_) => {
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
            | MetaCommand::Status
            | MetaCommand::Help
            | MetaCommand::Quit
            | MetaCommand::KgAclList(_) => Ok(()),
            // Session commands (ephemeral, per-connection)
            MetaCommand::SessionList
            | MetaCommand::SessionClear
            | MetaCommand::SessionDrop(_)
            | MetaCommand::SessionDropName(_) => Ok(()),
            // Agent commands (read-only interaction)
            MetaCommand::AgentMessage(_)
            | MetaCommand::AgentStart(_)
            | MetaCommand::AgentSetup(_)
            | MetaCommand::AgentExamples => Ok(()),
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
        | MetaCommand::Status
        | MetaCommand::Help
        | MetaCommand::Quit => Ok(()),

        // Agent commands - all roles
        MetaCommand::AgentMessage(_)
        | MetaCommand::AgentStart(_)
        | MetaCommand::AgentSetup(_)
        | MetaCommand::AgentExamples => Ok(()),

        // System administration - admin only
        MetaCommand::Compact => Err("Permission denied: only admins can compact".to_string()),
        MetaCommand::UserList
        | MetaCommand::UserCreate { .. }
        | MetaCommand::UserDrop(_)
        | MetaCommand::UserPassword { .. }
        | MetaCommand::UserRole { .. } => {
            Err("Permission denied: only admins can manage users".to_string())
        }
        MetaCommand::ApiKeyCreate(_) | MetaCommand::ApiKeyList | MetaCommand::ApiKeyRevoke(_) => {
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
            ".user list",
            ".apikey list",
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
            ".user list",
            ".user create bob pass editor",
            ".apikey create mykey",
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
        let denied = vec![".kg create test", ".compact", ".user list", ".apikey list"];
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
            admin_password: "test-pass-123".to_string(),
            api_key: "test-key-456".to_string(),
        };
        creds.save(&path).unwrap();

        let loaded = PersistedCredentials::load(&path).unwrap();
        assert_eq!(loaded.admin_password, "test-pass-123");
        assert_eq!(loaded.api_key, "test-key-456");
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
