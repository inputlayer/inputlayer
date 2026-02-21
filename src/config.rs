//! Configuration System
//!
//! Provides hierarchical configuration loading from:
//! - config.toml (default configuration)
//! - config.local.toml (git-ignored local overrides)
//! - Environment variables (INPUTLAYER_* prefix)
//!
//! ## Example
//!
//! ```toml
//! # config.toml
//! [storage]
//! data_dir = "/var/lib/inputlayer/data"
//! default_knowledge_graph = "default"
//!
//! [storage.persistence]
//! format = "parquet"
//! compression = "snappy"
//! ```
//!
//! Environment variable overrides:
//! ```bash
//! INPUTLAYER_STORAGE__DATA_DIR=/custom/path
//! INPUTLAYER_STORAGE__PERSISTENCE__FORMAT=csv
//! ```

use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Main configuration struct
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub storage: StorageConfig,
    pub optimization: OptimizationConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub http: HttpConfig,
}

/// Storage engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Base directory for all knowledge graph storage
    pub data_dir: PathBuf,

    /// Default knowledge graph (created on startup if missing)
    #[serde(alias = "default_database")]
    pub default_knowledge_graph: String,

    /// Automatically create knowledge graphs if they don't exist
    #[serde(default, alias = "auto_create_databases")]
    pub auto_create_knowledge_graphs: bool,

    /// Persistence settings (legacy, for compatibility)
    pub persistence: PersistenceConfig,

    /// DD-native persist layer settings
    #[serde(default)]
    pub persist: PersistLayerConfig,

    /// Performance settings
    #[serde(default)]
    pub performance: PerformanceConfig,

    /// Maximum number of knowledge graphs allowed (0 = unlimited)
    #[serde(default = "default_max_knowledge_graphs")]
    pub max_knowledge_graphs: usize,
}

/// Persistence configuration (legacy)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    /// Storage format (parquet, csv, bincode)
    pub format: StorageFormat,

    /// Compression type
    pub compression: CompressionType,

    /// Auto-save interval in seconds (0 = manual only)
    #[serde(default)]
    pub auto_save_interval: u64,

    /// Enable write-ahead logging for durability
    #[serde(default)]
    pub enable_wal: bool,
}

/// DD-native persist layer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistLayerConfig {
    /// Enable the DD-native persist layer
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Buffer size before flushing to batch file
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,

    /// Durability mode for writes (immediate, batched, or async)
    #[serde(default)]
    pub durability_mode: DurabilityMode,

    /// Compaction window: how much history to retain (0 = keep all)
    #[serde(default)]
    pub compaction_window: u64,
}

fn default_buffer_size() -> usize {
    10000
}

impl Default for PersistLayerConfig {
    fn default() -> Self {
        PersistLayerConfig {
            enabled: true,
            buffer_size: 10000,
            durability_mode: DurabilityMode::Immediate,
            compaction_window: 0,
        }
    }
}

/// Storage format options
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageFormat {
    /// Apache Parquet (columnar, compressed, recommended)
    Parquet,
    /// CSV (human-readable, uncompressed)
    Csv,
    /// Bincode (binary, Rust-specific)
    Bincode,
}

/// Compression options
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    /// Snappy compression (fast, good ratio)
    Snappy,
    /// Gzip compression (slower, better ratio)
    Gzip,
    /// No compression
    None,
}

/// Write durability mode - controls when writes are considered durable
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DurabilityMode {
    /// Sync to disk immediately after each write (safest, slowest)
    /// Data is guaranteed durable when write returns
    #[default]
    Immediate,

    /// Buffer writes and sync periodically (balanced)
    /// Some data may be lost if crash occurs between syncs
    Batched,

    /// Fire-and-forget async writes (fastest, least safe)
    /// In-memory update completes immediately, persistence is async
    /// Can lose data on crash, but guarantees ordering within session
    Async,
}

/// Performance tuning options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Initial capacity for in-memory collections
    #[serde(default = "default_initial_capacity")]
    pub initial_capacity: usize,

    /// Batch size for bulk operations
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Enable async I/O
    #[serde(default = "default_async_io")]
    pub async_io: bool,

    /// Number of worker threads for parallel query execution
    /// 0 = use all available CPU cores
    #[serde(default)]
    pub num_threads: usize,

    /// Query execution timeout in milliseconds. 0 = no timeout.
    #[serde(default = "default_query_timeout_ms")]
    pub query_timeout_ms: u64,

    /// Maximum query program size in bytes. 0 = no limit.
    #[serde(default = "default_max_query_size_bytes")]
    pub max_query_size_bytes: usize,

    /// Maximum number of tuples in a single insert. 0 = no limit.
    #[serde(default = "default_max_insert_tuples")]
    pub max_insert_tuples: usize,

    /// Maximum string value length in bytes. 0 = no limit.
    #[serde(default = "default_max_string_value_bytes")]
    pub max_string_value_bytes: usize,
}

/// Optimization configuration (re-use existing from lib.rs)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationConfig {
    /// NOTE: Disabled by default - code generator only supports 2-tuples
    #[serde(default)]
    pub enable_join_planning: bool,

    /// SIP (Sideways Information Passing) - semijoin reduction
    #[serde(default)]
    pub enable_sip_rewriting: bool,

    #[serde(default = "default_true")]
    pub enable_subplan_sharing: bool,

    #[serde(default = "default_true")]
    pub enable_boolean_specialization: bool,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error)
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Log format (text, json)
    #[serde(default = "default_log_format")]
    pub format: String,
}

/// HTTP server configuration for WebSocket API and GUI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    /// Enable HTTP server (WebSocket API + optional GUI)
    #[serde(default)]
    pub enabled: bool,

    /// HTTP server bind address
    #[serde(default = "default_http_host")]
    pub host: String,

    /// HTTP server port
    #[serde(default = "default_http_port")]
    pub port: u16,

    /// Allowed CORS origins (empty = same-origin only, unless cors_allow_all is true)
    #[serde(default)]
    pub cors_origins: Vec<String>,

    /// Explicitly allow all CORS origins (dev mode opt-in)
    #[serde(default)]
    pub cors_allow_all: bool,

    /// GUI static file serving configuration
    #[serde(default)]
    pub gui: GuiConfig,

    /// Authentication configuration
    #[serde(default)]
    pub auth: AuthConfig,

    /// WebSocket idle timeout in milliseconds. 0 = disabled.
    #[serde(default = "default_ws_idle_timeout_ms")]
    pub ws_idle_timeout_ms: u64,
}

/// GUI static file serving configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuiConfig {
    /// Enable GUI dashboard serving
    #[serde(default)]
    pub enabled: bool,

    /// Directory containing GUI static files (e.g., "./gui/dist")
    #[serde(default = "default_gui_static_dir")]
    pub static_dir: String,
}

/// Authentication configuration for HTTP API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Enable authentication (JWT-based)
    #[serde(default)]
    pub enabled: bool,

    /// JWT signing secret (MUST be changed in production)
    #[serde(default = "default_jwt_secret")]
    pub jwt_secret: String,

    /// Session timeout in seconds (default: 24 hours)
    #[serde(default = "default_session_timeout")]
    pub session_timeout_secs: u64,
}

// Default value functions
fn default_initial_capacity() -> usize {
    10000
}
fn default_batch_size() -> usize {
    1000
}
fn default_async_io() -> bool {
    true
}
fn default_true() -> bool {
    true
}
fn default_query_timeout_ms() -> u64 {
    30_000
}
fn default_max_query_size_bytes() -> usize {
    1_048_576 // 1 MB
}
fn default_max_insert_tuples() -> usize {
    10_000
}
fn default_max_string_value_bytes() -> usize {
    65_536 // 64 KB
}
fn default_max_knowledge_graphs() -> usize {
    1000
}
fn default_ws_idle_timeout_ms() -> u64 {
    300_000 // 5 minutes
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_log_format() -> String {
    "text".to_string()
}
fn default_http_host() -> String {
    "127.0.0.1".to_string()
}
fn default_http_port() -> u16 {
    8080
}
fn default_gui_static_dir() -> String {
    "./gui/dist".to_string()
}
fn default_jwt_secret() -> String {
    uuid::Uuid::new_v4().to_string()
}
fn default_session_timeout() -> u64 {
    86400
} // 24 hours

impl Config {
    /// Load configuration from default locations
    ///
    /// Merges in order:
    /// 1. config.toml (base configuration)
    /// 2. config.local.toml (local overrides, git-ignored)
    /// 3. Environment variables (INPUTLAYER_* prefix)
    pub fn load() -> Result<Self, figment::Error> {
        Figment::new()
            .merge(Toml::file("config.toml"))
            .merge(Toml::file("config.local.toml"))
            .merge(Env::prefixed("INPUTLAYER_").split("__"))
            .extract()
    }

    /// Load configuration from specific file path
    pub fn from_file(path: &str) -> Result<Self, figment::Error> {
        Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("INPUTLAYER_").split("__"))
            .extract()
    }

    /// Create default configuration
    pub fn default() -> Self {
        Config {
            storage: StorageConfig {
                data_dir: PathBuf::from("./data"),
                default_knowledge_graph: "default".to_string(),
                auto_create_knowledge_graphs: false,
                persistence: PersistenceConfig {
                    format: StorageFormat::Parquet,
                    compression: CompressionType::Snappy,
                    auto_save_interval: 0, // Manual save only
                    enable_wal: false,
                },
                persist: PersistLayerConfig::default(),
                performance: PerformanceConfig {
                    initial_capacity: 10000,
                    batch_size: 1000,
                    async_io: true,
                    num_threads: 0,
                    query_timeout_ms: 30_000,
                    max_query_size_bytes: 1_048_576,
                    max_insert_tuples: 10_000,
                    max_string_value_bytes: 65_536,
                },
                max_knowledge_graphs: 1000,
            },
            optimization: OptimizationConfig {
                enable_join_planning: true,
                enable_sip_rewriting: true,
                enable_subplan_sharing: true,
                enable_boolean_specialization: true,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: "text".to_string(),
            },
            http: HttpConfig::default(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::default()
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        PerformanceConfig {
            initial_capacity: default_initial_capacity(),
            batch_size: default_batch_size(),
            async_io: default_async_io(),
            num_threads: 0, // 0 = use all available CPU cores
            query_timeout_ms: default_query_timeout_ms(),
            max_query_size_bytes: default_max_query_size_bytes(),
            max_insert_tuples: default_max_insert_tuples(),
            max_string_value_bytes: default_max_string_value_bytes(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        LoggingConfig {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        HttpConfig {
            enabled: true,
            host: default_http_host(),
            port: default_http_port(),
            cors_origins: Vec::new(),
            cors_allow_all: false,
            gui: GuiConfig::default(),
            auth: AuthConfig::default(),
            ws_idle_timeout_ms: default_ws_idle_timeout_ms(),
        }
    }
}

impl Default for GuiConfig {
    fn default() -> Self {
        GuiConfig {
            enabled: true,
            static_dir: default_gui_static_dir(),
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        AuthConfig {
            enabled: false,
            jwt_secret: default_jwt_secret(),
            session_timeout_secs: default_session_timeout(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.storage.default_knowledge_graph, "default");
        assert_eq!(config.storage.data_dir, PathBuf::from("./data"));
        assert!(matches!(
            config.storage.persistence.format,
            StorageFormat::Parquet
        ));
        assert!(matches!(
            config.storage.persistence.compression,
            CompressionType::Snappy
        ));
    }

    #[test]
    fn test_config_serialization() {
        let config = Config::default();
        let toml_str = toml::to_string(&config).unwrap();

        // Verify it contains expected sections
        assert!(toml_str.contains("[storage]"));
        assert!(toml_str.contains("[storage.persistence]"));
        assert!(toml_str.contains("[optimization]"));
    }

    #[test]
    fn test_default_storage_config() {
        let config = Config::default();
        assert!(!config.storage.auto_create_knowledge_graphs);
        assert_eq!(config.storage.persistence.auto_save_interval, 0);
        assert!(!config.storage.persistence.enable_wal);
    }

    #[test]
    fn test_default_optimization_config() {
        let config = Config::default();
        assert!(config.optimization.enable_join_planning);
        assert!(config.optimization.enable_sip_rewriting);
        assert!(config.optimization.enable_subplan_sharing);
        assert!(config.optimization.enable_boolean_specialization);
    }

    #[test]
    fn test_default_logging_config() {
        let config = Config::default();
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.logging.format, "text");
    }

    #[test]
    fn test_default_http_config() {
        let config = Config::default();
        assert!(config.http.enabled);
        assert_eq!(config.http.host, "127.0.0.1");
        assert_eq!(config.http.port, 8080);
        assert!(config.http.cors_origins.is_empty());
    }

    #[test]
    fn test_default_gui_config() {
        let gui = GuiConfig::default();
        assert!(gui.enabled);
        assert_eq!(gui.static_dir, "./gui/dist");
    }

    #[test]
    fn test_default_auth_config() {
        let auth = AuthConfig::default();
        assert!(!auth.enabled);
        assert_eq!(auth.session_timeout_secs, 86400);
        // JWT secret should be a UUID
        assert!(!auth.jwt_secret.is_empty());
    }

    #[test]
    fn test_default_performance_config() {
        let perf = PerformanceConfig::default();
        assert_eq!(perf.initial_capacity, 10000);
        assert_eq!(perf.batch_size, 1000);
        assert!(perf.async_io);
        assert_eq!(perf.num_threads, 0);
    }

    #[test]
    fn test_default_persist_layer_config() {
        let persist = PersistLayerConfig::default();
        assert!(persist.enabled);
        assert_eq!(persist.buffer_size, 10000);
        assert_eq!(persist.durability_mode, DurabilityMode::Immediate);
        assert_eq!(persist.compaction_window, 0);
    }

    #[test]
    fn test_durability_mode_default() {
        let mode = DurabilityMode::default();
        assert_eq!(mode, DurabilityMode::Immediate);
    }

    #[test]
    fn test_config_toml_roundtrip() {
        let config = Config::default();
        let toml_str = toml::to_string(&config).unwrap();
        let back: Config = toml::from_str(&toml_str).unwrap();
        assert_eq!(back.storage.default_knowledge_graph, "default");
        assert_eq!(back.storage.data_dir, PathBuf::from("./data"));
        assert_eq!(back.logging.level, "info");
        assert_eq!(back.http.port, 8080);
    }

    #[test]
    fn test_config_json_roundtrip() {
        let config = Config::default();
        let json = serde_json::to_string(&config).unwrap();
        let back: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(back.storage.default_knowledge_graph, "default");
    }

    #[test]
    fn test_storage_format_serde() {
        let json = serde_json::to_string(&StorageFormat::Parquet).unwrap();
        assert_eq!(json, "\"parquet\"");
        let json = serde_json::to_string(&StorageFormat::Csv).unwrap();
        assert_eq!(json, "\"csv\"");
        let json = serde_json::to_string(&StorageFormat::Bincode).unwrap();
        assert_eq!(json, "\"bincode\"");
    }

    #[test]
    fn test_compression_type_serde() {
        let json = serde_json::to_string(&CompressionType::Snappy).unwrap();
        assert_eq!(json, "\"snappy\"");
        let json = serde_json::to_string(&CompressionType::Gzip).unwrap();
        assert_eq!(json, "\"gzip\"");
        let json = serde_json::to_string(&CompressionType::None).unwrap();
        assert_eq!(json, "\"none\"");
    }

    #[test]
    fn test_durability_mode_serde() {
        let json = serde_json::to_string(&DurabilityMode::Immediate).unwrap();
        assert_eq!(json, "\"immediate\"");
        let json = serde_json::to_string(&DurabilityMode::Batched).unwrap();
        assert_eq!(json, "\"batched\"");
        let json = serde_json::to_string(&DurabilityMode::Async).unwrap();
        assert_eq!(json, "\"async\"");
    }

    #[test]
    fn test_auth_jwt_secret_is_unique() {
        let auth1 = AuthConfig::default();
        let auth2 = AuthConfig::default();
        // Each call generates a new UUID
        assert_ne!(auth1.jwt_secret, auth2.jwt_secret);
    }
}
