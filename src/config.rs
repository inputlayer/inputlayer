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

    /// Whether to sync WAL immediately on each write (DEPRECATED: use `durability_mode` instead)
    #[serde(default = "default_true")]
    pub immediate_sync: bool,

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
            immediate_sync: true,
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

/// HTTP server configuration for REST API and GUI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    /// Enable HTTP server (REST API + optional GUI)
    #[serde(default)]
    pub enabled: bool,

    /// HTTP server bind address
    #[serde(default = "default_http_host")]
    pub host: String,

    /// HTTP server port
    #[serde(default = "default_http_port")]
    pub port: u16,

    /// Allowed CORS origins (empty = allow all in dev mode)
    #[serde(default)]
    pub cors_origins: Vec<String>,

    /// GUI static file serving configuration
    #[serde(default)]
    pub gui: GuiConfig,

    /// Authentication configuration
    #[serde(default)]
    pub auth: AuthConfig,
}

/// GUI static file serving configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
