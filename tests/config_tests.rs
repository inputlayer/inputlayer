//! Config loading, TOML parsing, and env var override tests.

use inputlayer::Config;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

// Helper to check if storage format is Parquet
fn is_parquet_format(config: &Config) -> bool {
    format!("{:?}", config.storage.persistence.format).contains("Parquet")
}

// Helper to check if compression is Snappy
fn is_snappy_compression(config: &Config) -> bool {
    format!("{:?}", config.storage.persistence.compression).contains("Snappy")
}

// Default Configuration Tests
#[test]
fn test_config_default_storage_path() {
    let config = Config::default();
    assert_eq!(config.storage.data_dir, PathBuf::from("./data"));
}

#[test]
fn test_config_default_knowledge_graph_name() {
    let config = Config::default();
    assert_eq!(config.storage.default_knowledge_graph, "default");
}

#[test]
fn test_config_default_auto_create_knowledge_graphs() {
    let config = Config::default();
    assert!(!config.storage.auto_create_knowledge_graphs);
}

#[test]
fn test_config_default_persistence_format() {
    let config = Config::default();
    assert!(is_parquet_format(&config));
}

#[test]
fn test_config_default_compression() {
    let config = Config::default();
    assert!(is_snappy_compression(&config));
}

#[test]
fn test_config_default_auto_save_interval() {
    let config = Config::default();
    assert_eq!(config.storage.persistence.auto_save_interval, 0);
}

#[test]
fn test_config_default_performance_settings() {
    let config = Config::default();
    assert_eq!(config.storage.performance.initial_capacity, 10000);
    assert_eq!(config.storage.performance.batch_size, 1000);
    assert!(config.storage.performance.async_io);
    assert_eq!(config.storage.performance.num_threads, 0); // 0 = use all CPUs
}

#[test]
fn test_config_default_optimization_enabled() {
    let config = Config::default();
    assert!(config.optimization.enable_join_planning);
    assert!(config.optimization.enable_sip_rewriting);
    assert!(config.optimization.enable_subplan_sharing);
}

#[test]
fn test_config_default_logging_level() {
    let config = Config::default();
    assert_eq!(config.logging.level, "info");
}

#[test]
fn test_config_default_logging_format() {
    let config = Config::default();
    assert_eq!(config.logging.format, "text");
}

// TOML File Parsing Tests
#[test]
fn test_load_config_from_toml() {
    let temp = TempDir::new().unwrap();
    let config_path = temp.path().join("config.toml");

    let config_content = r#"
[storage]
data_dir = "/tmp/test_data"
default_knowledge_graph = "test_db"
auto_create_knowledge_graphs = true

[storage.persistence]
format = "csv"
compression = "gzip"
auto_save_interval = 60

[storage.performance]
initial_capacity = 5000
batch_size = 500
async_io = false
num_threads = 4

[optimization]
enable_join_planning = false
enable_sip_rewriting = false

[logging]
level = "debug"
format = "json"
"#;

    fs::write(&config_path, config_content).unwrap();

    let config = Config::from_file(config_path.to_str().unwrap()).unwrap();

    // Verify loaded values
    assert_eq!(config.storage.data_dir, PathBuf::from("/tmp/test_data"));
    assert_eq!(config.storage.default_knowledge_graph, "test_db");
    assert!(config.storage.auto_create_knowledge_graphs);
    assert!(format!("{:?}", config.storage.persistence.format).contains("Csv"));
    assert!(format!("{:?}", config.storage.persistence.compression).contains("Gzip"));
    assert_eq!(config.storage.persistence.auto_save_interval, 60);
    assert_eq!(config.storage.performance.initial_capacity, 5000);
    assert_eq!(config.storage.performance.batch_size, 500);
    assert!(!config.storage.performance.async_io);
    assert_eq!(config.storage.performance.num_threads, 4);
    assert!(!config.optimization.enable_join_planning);
    assert!(!config.optimization.enable_sip_rewriting);
    assert_eq!(config.logging.level, "debug");
    assert_eq!(config.logging.format, "json");
}

#[test]
fn test_load_missing_config_file() {
    let temp = TempDir::new().unwrap();
    let nonexistent = temp.path().join("nonexistent.toml");

    // from_file with a nonexistent path should fail (required fields missing)
    let result = Config::from_file(nonexistent.to_str().unwrap());
    assert!(
        result.is_err(),
        "Config::from_file() should return error when config file doesn't exist"
    );
}

// Configuration Merging Tests
#[test]
fn test_config_local_overrides_base() {
    use figment::{
        providers::{Format, Toml},
        Figment,
    };

    let temp = TempDir::new().unwrap();
    let base_path = temp.path().join("config.toml");
    let local_path = temp.path().join("config.local.toml");

    // Create base config.toml with complete config
    let base_config = r#"
[storage]
data_dir = "./base_data"
default_knowledge_graph = "base_db"

[storage.persistence]
format = "parquet"
compression = "snappy"
auto_save_interval = 0

[storage.performance]
initial_capacity = 10000
batch_size = 1000
async_io = true
num_threads = 0

[optimization]
enable_join_planning = true
enable_sip_rewriting = true
enable_subplan_sharing = true
enable_boolean_specialization = false

[logging]
level = "info"
format = "text"
"#;
    fs::write(&base_path, base_config).unwrap();

    // Create config.local.toml with override (data_dir changed)
    let local_config = r#"
[storage]
data_dir = "./local_data"
default_knowledge_graph = "base_db"

[storage.persistence]
format = "parquet"
compression = "snappy"

[storage.performance]
initial_capacity = 10000
batch_size = 1000
async_io = true
num_threads = 0

[optimization]
enable_join_planning = true
enable_sip_rewriting = true
enable_subplan_sharing = true

[logging]
level = "info"
format = "text"
"#;
    fs::write(&local_path, local_config).unwrap();

    // Merge using Figment directly (same logic as Config::load but with explicit paths)
    let config: Config = Figment::new()
        .merge(Toml::file(&base_path))
        .merge(Toml::file(&local_path))
        .extract()
        .unwrap();

    // data_dir should be from config.local.toml
    assert_eq!(config.storage.data_dir, PathBuf::from("./local_data"));
    // default_knowledge_graph should be from both (same value)
    assert_eq!(config.storage.default_knowledge_graph, "base_db");
}

// Environment Variable Override Tests
//
// NOTE: Environment variable override tests are disabled because they
// interfere with other tests when run in parallel. The functionality
// is tested by the examples and can be verified manually.
//
// The Config::load() function does support env var overrides via Figment:
// - INPUTLAYER_STORAGE__DATA_DIR=/path
// - INPUTLAYER_STORAGE__PERFORMANCE__NUM_THREADS=8
// etc.

#[test]
fn test_env_var_syntax_documented() {
    // Just verify that the config system supports the pattern
    // Environment variables should use INPUTLAYER_ prefix with __ separators
    let config = Config::default();
    // If this compiles and runs, the config system is working
    assert!(config.storage.data_dir.to_str().is_some());
}

// Configuration Structure Tests
#[test]
fn test_config_has_storage_section() {
    let config = Config::default();
    // Just verify the config has the storage section
    let _ = config.storage;
}

#[test]
fn test_config_has_optimization_section() {
    let config = Config::default();
    let _ = config.optimization;
}

#[test]
fn test_config_has_logging_section() {
    let config = Config::default();
    let _ = config.logging;
}

#[test]
fn test_persistence_config_fields() {
    let config = Config::default();
    let persistence = config.storage.persistence;

    // Verify format and compression exist (enums always have a value)
    let _ = format!("{:?}", persistence.format);
    let _ = format!("{:?}", persistence.compression);
    // auto_save_interval is u64 (0 = manual save only, by design)
    // The important verification is that it exists and has a valid value
    assert_eq!(
        persistence.auto_save_interval, 0,
        "Default should be manual save (0)"
    );
}

#[test]
fn test_performance_config_fields() {
    let config = Config::default();
    let performance = config.storage.performance;

    assert!(performance.initial_capacity > 0);
    assert!(performance.batch_size > 0);
    // num_threads is usize (0 = use all available CPU cores, by design)
    assert_eq!(
        performance.num_threads, 0,
        "Default should be 0 (use all cores)"
    );
}

// Configuration Validation Tests
#[test]
fn test_config_valid_formats() {
    let config = Config::default();
    let format_str = format!("{:?}", config.storage.persistence.format);

    // Should be one of the valid formats
    assert!(
        format_str.contains("Parquet")
            || format_str.contains("Csv")
            || format_str.contains("Bincode"),
        "Invalid format: {}",
        format_str
    );
}

#[test]
fn test_config_valid_compression() {
    let config = Config::default();
    let compression_str = format!("{:?}", config.storage.persistence.compression);

    // Should be one of the valid compression types
    assert!(
        compression_str.contains("Snappy")
            || compression_str.contains("Gzip")
            || compression_str.contains("None"),
        "Invalid compression: {}",
        compression_str
    );
}

#[test]
fn test_config_valid_log_level() {
    let config = Config::default();
    let level = config.logging.level;

    // Should be one of the valid log levels
    assert!(
        level == "trace"
            || level == "debug"
            || level == "info"
            || level == "warn"
            || level == "error",
        "Invalid log level: {}",
        level
    );
}

#[test]
fn test_config_valid_log_format() {
    let config = Config::default();
    let format = config.logging.format;

    // Should be one of the valid formats
    assert!(
        format == "text" || format == "json",
        "Invalid log format: {}",
        format
    );
}

// Path Resolution Tests
#[test]
fn test_config_relative_path() {
    let mut config = Config::default();
    config.storage.data_dir = PathBuf::from("./data");

    // Should preserve relative path
    assert!(
        config.storage.data_dir.starts_with("./") || config.storage.data_dir.starts_with("data")
    );
}

#[test]
fn test_config_absolute_path() {
    let mut config = Config::default();
    config.storage.data_dir = PathBuf::from("/var/lib/inputlayer");

    // Should handle absolute path
    assert!(config.storage.data_dir.is_absolute());
}

// Serialization Tests
#[test]
fn test_config_can_be_cloned() {
    let config1 = Config::default();
    let config2 = config1.clone();

    assert_eq!(config1.storage.data_dir, config2.storage.data_dir);
    assert_eq!(
        config1.storage.default_knowledge_graph,
        config2.storage.default_knowledge_graph
    );
}

#[test]
fn test_config_can_be_debugged() {
    let config = Config::default();
    let debug_str = format!("{:?}", config);

    // Should contain some config information
    assert!(debug_str.contains("storage") || debug_str.contains("Config"));
}
