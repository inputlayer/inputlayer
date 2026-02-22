//! Authentication and Role-Based Access Control (RBAC)
//!
//! Provides role-based authorization for all Datalog operations,
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
pub fn hash_password(password: &str) -> String {
    use argon2::{
        password_hash::{rand_core::OsRng, SaltString},
        Argon2, PasswordHasher,
    };
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    argon2
        .hash_password(password.as_bytes(), &salt)
        .expect("argon2 hashing should not fail")
        .to_string()
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

    /// Save credentials to a TOML file.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let contents =
            toml::to_string_pretty(self).map_err(|e| std::io::Error::other(e.to_string()))?;
        std::fs::write(path, contents)
    }
}

// ── Authorization ───────────────────────────────────────────────────────────

/// Check whether a role is authorized to execute a given statement.
/// Returns `Ok(())` if allowed, `Err(message)` if denied.
pub fn authorize_statement(role: &Role, stmt: &Statement) -> Result<(), String> {
    match role {
        Role::Admin => Ok(()), // Admin can do everything
        Role::Editor => authorize_editor(stmt),
        Role::Viewer => authorize_viewer(stmt),
    }
}

fn authorize_editor(stmt: &Statement) -> Result<(), String> {
    match stmt {
        // Editors can query, insert, delete, define rules
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

        Statement::Meta(cmd) => authorize_editor_meta(cmd),
    }
}

fn authorize_editor_meta(cmd: &MetaCommand) -> Result<(), String> {
    match cmd {
        // Editors can view/use KGs, but not create/drop
        MetaCommand::KgShow | MetaCommand::KgList | MetaCommand::KgUse(_) => Ok(()),
        MetaCommand::KgCreate(_) | MetaCommand::KgDrop(_) => {
            Err("Permission denied: only admins can create/drop knowledge graphs".to_string())
        }

        // Editors can view and manipulate relations/rules
        MetaCommand::RelList
        | MetaCommand::RelDescribe(_)
        | MetaCommand::RuleList
        | MetaCommand::RuleQuery(_)
        | MetaCommand::RuleShowDef(_)
        | MetaCommand::RuleDrop(_)
        | MetaCommand::RuleDropPrefix(_)
        | MetaCommand::RuleEdit { .. }
        | MetaCommand::RuleClear(_)
        | MetaCommand::RuleRemove { .. } => Ok(()),

        // Session commands are always allowed
        MetaCommand::SessionList
        | MetaCommand::SessionClear
        | MetaCommand::SessionDrop(_)
        | MetaCommand::SessionDropName(_) => Ok(()),

        // Index management
        MetaCommand::IndexList
        | MetaCommand::IndexCreate(_)
        | MetaCommand::IndexDrop(_)
        | MetaCommand::IndexStats(_)
        | MetaCommand::IndexRebuild(_) => Ok(()),

        // Editors can clear and load
        MetaCommand::ClearPrefix(_) | MetaCommand::Load { .. } => Ok(()),

        // Read-only system commands
        MetaCommand::Explain(_) | MetaCommand::Status | MetaCommand::Help | MetaCommand::Quit => {
            Ok(())
        }

        // Admin-only commands
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

fn authorize_viewer(stmt: &Statement) -> Result<(), String> {
    match stmt {
        // Viewers can only query and use session rules/facts
        Statement::Query(_) | Statement::SessionRule(_) | Statement::Fact(_) => Ok(()),

        // Viewers cannot modify data
        Statement::Insert(_) => Err("Permission denied: viewers cannot insert data".to_string()),
        Statement::Delete(_) => Err("Permission denied: viewers cannot delete data".to_string()),
        Statement::Update(_) => Err("Permission denied: viewers cannot update data".to_string()),
        Statement::PersistentRule(_) => {
            Err("Permission denied: viewers cannot create persistent rules".to_string())
        }
        Statement::SchemaDecl(_) => {
            Err("Permission denied: viewers cannot define schemas".to_string())
        }
        Statement::TypeDecl(_) => Err("Permission denied: viewers cannot define types".to_string()),
        Statement::DeleteRelationOrRule(_) => {
            Err("Permission denied: viewers cannot delete relations/rules".to_string())
        }

        Statement::Meta(cmd) => authorize_viewer_meta(cmd),
    }
}

fn authorize_viewer_meta(cmd: &MetaCommand) -> Result<(), String> {
    match cmd {
        // Viewers can view KG info
        MetaCommand::KgShow | MetaCommand::KgList | MetaCommand::KgUse(_) => Ok(()),
        MetaCommand::KgCreate(_) | MetaCommand::KgDrop(_) => {
            Err("Permission denied: viewers cannot create/drop knowledge graphs".to_string())
        }

        // Viewers can list/describe relations and rules
        MetaCommand::RelList
        | MetaCommand::RelDescribe(_)
        | MetaCommand::RuleList
        | MetaCommand::RuleQuery(_)
        | MetaCommand::RuleShowDef(_) => Ok(()),

        // Viewers cannot modify rules
        MetaCommand::RuleDrop(_)
        | MetaCommand::RuleDropPrefix(_)
        | MetaCommand::RuleEdit { .. }
        | MetaCommand::RuleClear(_)
        | MetaCommand::RuleRemove { .. } => {
            Err("Permission denied: viewers cannot modify rules".to_string())
        }

        // Session commands are always allowed (they're ephemeral)
        MetaCommand::SessionList
        | MetaCommand::SessionClear
        | MetaCommand::SessionDrop(_)
        | MetaCommand::SessionDropName(_) => Ok(()),

        // Viewers cannot manage indexes
        MetaCommand::IndexList | MetaCommand::IndexStats(_) => Ok(()),
        MetaCommand::IndexCreate(_) | MetaCommand::IndexDrop(_) | MetaCommand::IndexRebuild(_) => {
            Err("Permission denied: viewers cannot manage indexes".to_string())
        }

        // Viewers cannot clear or load
        MetaCommand::ClearPrefix(_) => {
            Err("Permission denied: viewers cannot clear data".to_string())
        }
        MetaCommand::Load { .. } => Err("Permission denied: viewers cannot load files".to_string()),

        // Read-only system commands
        MetaCommand::Explain(_) | MetaCommand::Status | MetaCommand::Help | MetaCommand::Quit => {
            Ok(())
        }

        // Admin-only
        MetaCommand::Compact => Err("Permission denied: viewers cannot compact".to_string()),
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
        let hash = hash_password("mypassword");
        assert!(verify_password("mypassword", &hash));
        assert!(!verify_password("wrongpassword", &hash));
    }

    #[test]
    fn test_hash_password_unique_salts() {
        let h1 = hash_password("same");
        let h2 = hash_password("same");
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
    fn test_editor_cannot_kg_create_drop() {
        use crate::statement::parse_statement;
        let denied = vec![".kg create test", ".kg drop test", ".compact"];
        for s in denied {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_statement(&Role::Editor, &stmt).is_err(),
                "Editor should be denied: {s}"
            );
        }
    }

    #[test]
    fn test_editor_can_insert_delete_query() {
        use crate::statement::parse_statement;
        let allowed = vec!["?edge(X, Y)", "+edge(1, 2)", "-edge(1, 2)", ".rel"];
        for s in allowed {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_statement(&Role::Editor, &stmt).is_ok(),
                "Editor should be allowed: {s}"
            );
        }
    }

    #[test]
    fn test_viewer_cannot_insert_delete() {
        use crate::statement::parse_statement;
        let denied = vec![
            "+edge(1, 2)",
            "-edge(1, 2)",
            ".kg create test",
            ".kg drop test",
            ".compact",
            ".rule drop path",
        ];
        for s in denied {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_statement(&Role::Viewer, &stmt).is_err(),
                "Viewer should be denied: {s}"
            );
        }
    }

    #[test]
    fn test_viewer_can_query_and_session() {
        use crate::statement::parse_statement;
        let allowed = vec![
            "?edge(X, Y)",
            ".rel",
            ".rule",
            ".kg list",
            ".status",
            ".session",
        ];
        for s in allowed {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_statement(&Role::Viewer, &stmt).is_ok(),
                "Viewer should be allowed: {s}"
            );
        }
    }

    #[test]
    fn test_viewer_cannot_manage_users() {
        use crate::statement::parse_statement;
        let denied = vec![".user list", ".apikey list"];
        for s in denied {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_statement(&Role::Viewer, &stmt).is_err(),
                "Viewer should be denied: {s}"
            );
        }
    }

    #[test]
    fn test_editor_cannot_manage_users() {
        use crate::statement::parse_statement;
        let denied = vec![
            ".user list",
            ".user create bob pass editor",
            ".apikey create mykey",
        ];
        for s in denied {
            let stmt = parse_statement(s).unwrap();
            assert!(
                authorize_statement(&Role::Editor, &stmt).is_err(),
                "Editor should be denied: {s}"
            );
        }
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
