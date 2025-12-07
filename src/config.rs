//! Configuration System
//!
//! Provides hierarchical configuration loading from:
//! - config.toml (default configuration)
//! - config.local.toml (git-ignored local overrides)
//! - Environment variables (FLOWLOG_* prefix)
//!
//! ## Example
//!
//! ```toml
//! # config.toml
//! [storage]
//! data_dir = "/var/lib/inputlayer/data"
//! default_database = "default"
//!
//! [storage.persistence]
//! format = "parquet"
//! compression = "snappy"
//! ```
//!
//! Environment variable overrides:
//! ```bash
//! FLOWLOG_STORAGE__DATA_DIR=/custom/path
//! FLOWLOG_STORAGE__PERSISTENCE__FORMAT=csv
//! ```

use figment::{Figment, providers::{Env, Format, Toml}};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Main configuration struct
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub storage: StorageConfig,
    pub optimization: OptimizationConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

/// Storage engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Base directory for all database storage
    pub data_dir: PathBuf,

    /// Default database (created on startup if missing)
    pub default_database: String,

    /// Automatically create databases if they don't exist
    #[serde(default)]
    pub auto_create_databases: bool,

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

    /// Whether to sync WAL immediately on each write
    #[serde(default = "default_true")]
    pub immediate_sync: bool,

    /// Compaction window: how much history to retain (0 = keep all)
    #[serde(default)]
    pub compaction_window: u64,
}

fn default_buffer_size() -> usize { 10000 }

impl Default for PersistLayerConfig {
    fn default() -> Self {
        PersistLayerConfig {
            enabled: true,
            buffer_size: 10000,
            immediate_sync: true,
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

    /// NOTE: Disabled by default - code generator only supports 2-tuples
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

// Default value functions
fn default_initial_capacity() -> usize { 10000 }
fn default_batch_size() -> usize { 1000 }
fn default_async_io() -> bool { true }
fn default_true() -> bool { true }
fn default_log_level() -> String { "info".to_string() }
fn default_log_format() -> String { "text".to_string() }

impl Config {
    /// Load configuration from default locations
    ///
    /// Merges in order:
    /// 1. config.toml (base configuration)
    /// 2. config.local.toml (local overrides, git-ignored)
    /// 3. Environment variables (FLOWLOG_* prefix)
    pub fn load() -> Result<Self, figment::Error> {
        Figment::new()
            .merge(Toml::file("config.toml"))
            .merge(Toml::file("config.local.toml"))
            .merge(Env::prefixed("FLOWLOG_").split("__"))
            .extract()
    }

    /// Load configuration from specific file path
    pub fn from_file(path: &str) -> Result<Self, figment::Error> {
        Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("FLOWLOG_").split("__"))
            .extract()
    }

    /// Create default configuration
    pub fn default() -> Self {
        Config {
            storage: StorageConfig {
                data_dir: PathBuf::from("./data"),
                default_database: "default".to_string(),
                auto_create_databases: false,
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
                },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.storage.default_database, "default");
        assert_eq!(config.storage.data_dir, PathBuf::from("./data"));
        assert!(matches!(config.storage.persistence.format, StorageFormat::Parquet));
        assert!(matches!(config.storage.persistence.compression, CompressionType::Snappy));
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
}
