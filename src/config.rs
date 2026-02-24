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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct PersistenceConfig {
    /// Storage format (parquet, csv, bincode)
    pub format: StorageFormat,

    /// Compression type
    pub compression: CompressionType,

    /// Auto-save interval in seconds (0 = manual only)
    #[serde(default)]
    pub auto_save_interval: u64,

    /// Enable write-ahead logging for durability
    #[serde(default = "default_true")]
    pub enable_wal: bool,
}

/// DD-native persist layer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

    /// Maximum WAL file size in bytes before forcing a flush of all dirty shards.
    /// Prevents unbounded WAL growth and slow startup replay. 0 = unlimited.
    #[serde(default = "default_max_wal_size_bytes")]
    pub max_wal_size_bytes: u64,

    /// Auto-compaction: maximum number of batch files per shard before triggering
    /// background compaction. 0 = disabled (manual `.compact` only).
    #[serde(default = "default_auto_compact_threshold")]
    pub auto_compact_threshold: usize,

    /// Auto-compaction check interval in seconds. 0 = disabled.
    #[serde(default = "default_auto_compact_interval_secs")]
    pub auto_compact_interval_secs: u64,
}

fn default_buffer_size() -> usize {
    10000
}

fn default_max_wal_size_bytes() -> u64 {
    67_108_864 // 64 MB
}

fn default_auto_compact_threshold() -> usize {
    10 // Compact when a shard has 10+ batch files
}

fn default_auto_compact_interval_secs() -> u64 {
    300 // Check every 5 minutes
}

impl Default for PersistLayerConfig {
    fn default() -> Self {
        PersistLayerConfig {
            enabled: true,
            buffer_size: 10000,
            durability_mode: DurabilityMode::Immediate,
            compaction_window: 0,
            max_wal_size_bytes: default_max_wal_size_bytes(),
            auto_compact_threshold: default_auto_compact_threshold(),
            auto_compact_interval_secs: default_auto_compact_interval_secs(),
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
#[serde(deny_unknown_fields)]
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

    /// Maximum number of result rows returned by a query. 0 = no limit.
    #[serde(default)]
    pub max_result_rows: usize,

    /// Slow query warning threshold in milliseconds. Queries exceeding this
    /// are logged at WARN level. 0 = disabled.
    #[serde(default = "default_slow_query_log_ms")]
    pub slow_query_log_ms: u64,

    /// Maximum query cost score. Queries exceeding this are rejected before
    /// execution. Cost is estimated from the IR tree (joins, aggregations,
    /// negation, recursion). 0 = no limit.
    #[serde(default)]
    pub max_query_cost: u64,
}

/// Optimization configuration (re-use existing from lib.rs)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

    /// Magic Sets demand-driven rewriting for recursive queries
    #[serde(default = "default_true")]
    pub enable_magic_sets: bool,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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

    /// Graceful shutdown timeout in seconds. If the storage lock cannot be acquired
    /// within this time during shutdown, WAL flush is skipped (safe — replayed on restart).
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,

    /// Stats endpoint computation timeout in seconds. Large deployments with many
    /// KGs/relations may need a higher value.
    #[serde(default = "default_stats_timeout_secs")]
    pub stats_timeout_secs: u64,

    /// Rate limiting configuration
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
}

/// GUI static file serving configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// Initial admin password (set via INPUTLAYER_ADMIN_PASSWORD env var or config).
    /// If unset on first boot, a random password is generated and printed to stderr.
    #[serde(default)]
    pub bootstrap_admin_password: Option<String>,

    /// Session timeout in seconds (default: 24 hours)
    #[serde(default = "default_session_timeout")]
    pub session_timeout_secs: u64,

    /// Path to persist generated credentials (admin password + API key).
    /// Default: `.inputlayer-credentials.toml` in the working directory.
    /// Credentials are generated on first boot and reused across restarts,
    /// even when the data directory is wiped.
    #[serde(default)]
    pub credentials_file: Option<PathBuf>,
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitConfig {
    /// Maximum concurrent connections (0 = unlimited)
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Maximum concurrent WebSocket connections (0 = unlimited)
    #[serde(default = "default_max_ws_connections")]
    pub max_ws_connections: usize,

    /// Maximum WebSocket messages per second per connection (0 = unlimited)
    #[serde(default = "default_ws_max_messages_per_sec")]
    pub ws_max_messages_per_sec: u32,

    /// Maximum WebSocket connection lifetime in seconds (0 = unlimited)
    #[serde(default = "default_ws_max_lifetime_secs")]
    pub ws_max_lifetime_secs: u64,

    /// Notification broadcast channel buffer size (per-subscriber queue depth)
    #[serde(default = "default_notification_buffer_size")]
    pub notification_buffer_size: usize,

    /// Maximum HTTP requests per second per IP address (0 = unlimited) (#27)
    #[serde(default = "default_per_ip_max_rps")]
    pub per_ip_max_rps: u32,
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
fn default_slow_query_log_ms() -> u64 {
    5000 // 5 seconds
}
fn default_max_knowledge_graphs() -> usize {
    1000
}
fn default_ws_idle_timeout_ms() -> u64 {
    300_000 // 5 minutes
}
fn default_shutdown_timeout_secs() -> u64 {
    30
}
fn default_stats_timeout_secs() -> u64 {
    5
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
fn default_session_timeout() -> u64 {
    86400
} // 24 hours
fn default_max_connections() -> usize {
    10_000
}
fn default_max_ws_connections() -> usize {
    5_000
}
fn default_ws_max_messages_per_sec() -> u32 {
    1000
}
fn default_ws_max_lifetime_secs() -> u64 {
    86400
} // 24 hours
fn default_notification_buffer_size() -> usize {
    4096
}
fn default_per_ip_max_rps() -> u32 {
    0 // unlimited by default
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        RateLimitConfig {
            max_connections: default_max_connections(),
            max_ws_connections: default_max_ws_connections(),
            ws_max_messages_per_sec: default_ws_max_messages_per_sec(),
            ws_max_lifetime_secs: default_ws_max_lifetime_secs(),
            notification_buffer_size: default_notification_buffer_size(),
            per_ip_max_rps: default_per_ip_max_rps(),
        }
    }
}

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
        let config: Self = Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("INPUTLAYER_").split("__"))
            .extract()?;
        config.warn_unsafe_defaults();
        Ok(config)
    }

    /// Validate configuration values and auto-correct safe-to-fix issues.
    /// Returns Err for fatal misconfigurations that would cause panics.
    pub fn validate(&mut self) -> Result<(), String> {
        // notification_buffer_size=0 causes broadcast::channel(0) panic
        if self.http.rate_limit.notification_buffer_size == 0 {
            tracing::warn!("notification_buffer_size = 0 is invalid, auto-correcting to 4096");
            self.http.rate_limit.notification_buffer_size = 4096;
        }

        // notification_buffer_size too large wastes memory per subscriber
        if self.http.rate_limit.notification_buffer_size > 100_000 {
            tracing::warn!(
                value = self.http.rate_limit.notification_buffer_size,
                "notification_buffer_size is very large, capping at 100000"
            );
            self.http.rate_limit.notification_buffer_size = 100_000;
        }

        // persist buffer_size=0 would cause infinite flush loops
        if self.storage.persist.buffer_size == 0 {
            tracing::warn!("persist.buffer_size = 0 is invalid, auto-correcting to 1000");
            self.storage.persist.buffer_size = 1000;
        }

        // Warn about very long query timeouts (> 10 minutes)
        if self.storage.performance.query_timeout_ms > 600_000 {
            tracing::warn!(
                value_ms = self.storage.performance.query_timeout_ms,
                "query_timeout_ms exceeds 10 minutes — queries may appear hung"
            );
        }

        // Warn about extremely high WS connection limits
        if self.http.rate_limit.max_ws_connections > 100_000 {
            tracing::warn!(
                value = self.http.rate_limit.max_ws_connections,
                "max_ws_connections > 100000 may exhaust file descriptors"
            );
        }

        Ok(())
    }

    /// Log warnings for configuration values that may be unsafe for production.
    pub fn warn_unsafe_defaults(&self) {
        if self.storage.performance.max_result_rows == 0 {
            eprintln!(
                "WARNING: max_result_rows = 0 (unlimited). \
                 Unbounded queries may exhaust server memory."
            );
        }
        if self.http.cors_allow_all {
            eprintln!(
                "WARNING: cors_allow_all = true. \
                 Any origin can access the API. Disable for production."
            );
        }
        if self.http.rate_limit.notification_buffer_size == 0 {
            eprintln!(
                "WARNING: notification_buffer_size = 0. \
                 Using default of 4096."
            );
        }
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
                    enable_wal: true,
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
                    max_result_rows: 100_000,
                    slow_query_log_ms: 5000,
                    max_query_cost: 0,
                },
                max_knowledge_graphs: 1000,
            },
            optimization: OptimizationConfig {
                enable_join_planning: true,
                enable_sip_rewriting: true,
                enable_subplan_sharing: true,
                enable_boolean_specialization: true,
                enable_magic_sets: true,
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
            max_result_rows: 100_000, // match Config::default()
            slow_query_log_ms: default_slow_query_log_ms(),
            max_query_cost: 0, // 0 = unlimited
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
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            stats_timeout_secs: default_stats_timeout_secs(),
            rate_limit: RateLimitConfig::default(),
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
            bootstrap_admin_password: None,
            session_timeout_secs: default_session_timeout(),
            credentials_file: None,
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
        assert!(config.storage.persistence.enable_wal);
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
        assert!(auth.bootstrap_admin_password.is_none());
        assert_eq!(auth.session_timeout_secs, 86400);
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
        assert_eq!(persist.auto_compact_threshold, 10);
        assert_eq!(persist.auto_compact_interval_secs, 300);
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

    // === Regression tests for P1 security config ===

    #[test]
    fn test_default_auth_has_no_bootstrap_password() {
        let auth = AuthConfig::default();
        assert!(auth.bootstrap_admin_password.is_none());
    }

    #[test]
    fn test_default_rate_limit_config() {
        let rl = RateLimitConfig::default();
        assert_eq!(rl.max_connections, 10_000);
        assert_eq!(rl.max_ws_connections, 5_000);
        assert_eq!(rl.ws_max_messages_per_sec, 1000);
        assert_eq!(rl.ws_max_lifetime_secs, 86400);
    }

    #[test]
    fn test_rate_limit_config_serialization() {
        let config = Config::default();
        let toml_str = toml::to_string(&config).unwrap();
        assert!(toml_str.contains("[http.rate_limit]"));
    }

    #[test]
    fn test_auth_with_bootstrap_password() {
        let auth = AuthConfig {
            bootstrap_admin_password: Some("secret123".to_string()),
            session_timeout_secs: 3600,
            credentials_file: None,
        };
        assert_eq!(auth.bootstrap_admin_password.as_deref(), Some("secret123"));
    }

    #[test]
    fn test_http_config_has_rate_limit() {
        let config = HttpConfig::default();
        assert_eq!(config.rate_limit.max_connections, 10_000);
        assert_eq!(config.rate_limit.ws_max_messages_per_sec, 1000);
    }

    /// Regression: WS rate limit and lifetime fields must roundtrip through TOML.
    #[test]
    fn test_ws_rate_limit_config_roundtrip() {
        let mut config = Config::default();
        config.http.rate_limit.ws_max_messages_per_sec = 500;
        config.http.rate_limit.ws_max_lifetime_secs = 7200;
        config.http.rate_limit.max_ws_connections = 100;

        let toml_str = toml::to_string(&config).unwrap();
        let parsed: Config = toml::from_str(&toml_str).unwrap();

        assert_eq!(parsed.http.rate_limit.ws_max_messages_per_sec, 500);
        assert_eq!(parsed.http.rate_limit.ws_max_lifetime_secs, 7200);
        assert_eq!(parsed.http.rate_limit.max_ws_connections, 100);
    }

    /// Regression: WS idle timeout config defaults and survives serialization.
    #[test]
    fn test_ws_idle_timeout_config() {
        let config = HttpConfig::default();
        assert_eq!(config.ws_idle_timeout_ms, 300_000);

        let full = Config::default();
        let toml_str = toml::to_string(&full).unwrap();
        let parsed: Config = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.http.ws_idle_timeout_ms, 300_000);
    }

    // === Regression tests for Config::validate() auto-correction ===

    #[test]
    fn test_validate_auto_corrects_zero_notification_buffer() {
        let mut config = Config::default();
        config.http.rate_limit.notification_buffer_size = 0;
        config.validate().unwrap();
        assert_eq!(config.http.rate_limit.notification_buffer_size, 4096);
    }

    #[test]
    fn test_validate_caps_large_notification_buffer() {
        let mut config = Config::default();
        config.http.rate_limit.notification_buffer_size = 200_000;
        config.validate().unwrap();
        assert_eq!(config.http.rate_limit.notification_buffer_size, 100_000);
    }

    #[test]
    fn test_validate_auto_corrects_zero_persist_buffer_size() {
        let mut config = Config::default();
        config.storage.persist.buffer_size = 0;
        config.validate().unwrap();
        assert_eq!(config.storage.persist.buffer_size, 1000);
    }

    #[test]
    fn test_validate_accepts_normal_values() {
        let mut config = Config::default();
        let original_notification = config.http.rate_limit.notification_buffer_size;
        let original_buffer = config.storage.persist.buffer_size;
        config.validate().unwrap();
        // Normal defaults should not be changed
        assert_eq!(
            config.http.rate_limit.notification_buffer_size,
            original_notification
        );
        assert_eq!(config.storage.persist.buffer_size, original_buffer);
    }

    // === Regression tests for new config field defaults ===

    #[test]
    fn test_default_max_wal_size_bytes() {
        let persist = PersistLayerConfig::default();
        assert_eq!(persist.max_wal_size_bytes, 67_108_864); // 64 MB
    }

    #[test]
    fn test_default_slow_query_log_ms() {
        let perf = PerformanceConfig::default();
        assert_eq!(perf.slow_query_log_ms, 5000);
    }

    #[test]
    fn test_default_shutdown_timeout_secs() {
        let http = HttpConfig::default();
        assert_eq!(http.shutdown_timeout_secs, 30);
    }

    #[test]
    fn test_default_stats_timeout_secs() {
        let http = HttpConfig::default();
        assert_eq!(http.stats_timeout_secs, 5);
    }

    #[test]
    fn test_default_notification_buffer_size() {
        let rl = RateLimitConfig::default();
        assert_eq!(rl.notification_buffer_size, 4096);
    }

    /// Regression: the manual Config::default() impl sets max_result_rows = 100_000.
    #[test]
    fn test_config_default_max_result_rows() {
        let config = Config::default();
        assert_eq!(config.storage.performance.max_result_rows, 100_000);
    }

    #[test]
    fn test_config_default_slow_query_log_ms() {
        let config = Config::default();
        assert_eq!(config.storage.performance.slow_query_log_ms, 5000);
    }

    /// Regression: new config fields must survive TOML roundtrip.
    #[test]
    fn test_new_config_fields_toml_roundtrip() {
        let mut config = Config::default();
        config.storage.persist.max_wal_size_bytes = 123_456;
        config.storage.performance.slow_query_log_ms = 2000;
        config.http.shutdown_timeout_secs = 60;
        config.http.stats_timeout_secs = 10;
        config.http.rate_limit.notification_buffer_size = 8192;

        let toml_str = toml::to_string(&config).unwrap();
        let parsed: Config = toml::from_str(&toml_str).unwrap();

        assert_eq!(parsed.storage.persist.max_wal_size_bytes, 123_456);
        assert_eq!(parsed.storage.performance.slow_query_log_ms, 2000);
        assert_eq!(parsed.http.shutdown_timeout_secs, 60);
        assert_eq!(parsed.http.stats_timeout_secs, 10);
        assert_eq!(parsed.http.rate_limit.notification_buffer_size, 8192);
    }

    /// Regression: Zero values for rate limit fields mean "unlimited".
    #[test]
    fn test_rate_limit_zero_means_unlimited() {
        let mut rl = RateLimitConfig::default();
        rl.max_connections = 0;
        rl.max_ws_connections = 0;
        rl.ws_max_messages_per_sec = 0;
        rl.ws_max_lifetime_secs = 0;

        let config = Config {
            http: HttpConfig {
                rate_limit: rl,
                ..Default::default()
            },
            ..Default::default()
        };
        let toml_str = toml::to_string(&config).unwrap();
        let parsed: Config = toml::from_str(&toml_str).unwrap();

        assert_eq!(parsed.http.rate_limit.max_connections, 0);
        assert_eq!(parsed.http.rate_limit.max_ws_connections, 0);
        assert_eq!(parsed.http.rate_limit.ws_max_messages_per_sec, 0);
        assert_eq!(parsed.http.rate_limit.ws_max_lifetime_secs, 0);
    }
}
