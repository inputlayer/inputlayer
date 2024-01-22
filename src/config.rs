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
