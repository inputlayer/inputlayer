//! Config loading, TOML parsing, and env var override tests.
//!
//! Some tests are `#[ignore]` (they chdir and conflict in parallel).
//! Run them with: `cargo test --test config_tests -- --ignored --test-threads=1`

use inputlayer::Config;
use std::env;
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
#[ignore = "Requires --test-threads=1 due to directory change"]
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

    // Change to temp directory to load config
    let original_dir = env::current_dir().unwrap();
    env::set_current_dir(temp.path()).unwrap();

    let config = Config::load().unwrap();

    // Restore original directory
    env::set_current_dir(original_dir).unwrap();

    // Verify loaded values
    assert_eq!(config.storage.data_dir, PathBuf::from("/tmp/test_data"));
    assert_eq!(config.storage.default_knowledge_graph, "test_db");
    assert!(config.storage.auto_create_knowledge_graphs);
    // Check format and compression via Debug string
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
    let original_dir = env::current_dir().unwrap();
    env::set_current_dir(temp.path()).unwrap();

    // Config::load() returns an error when no config file exists
    // (it doesn't have built-in defaults, so at least one config source is required)
    let result = Config::load();

    env::set_current_dir(original_dir).unwrap();

    // Config::load() requires at least one config source - returns error if missing
    // Use Config::default() if you need defaults without a config file
    assert!(
        result.is_err(),
        "Config::load() should return error when no config file exists"
    );
}

// Configuration Merging Tests
#[test]
#[ignore = "Requires --test-threads=1 due to directory change"]
fn test_config_local_overrides_base() {
    let temp = TempDir::new().unwrap();

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
    fs::write(temp.path().join("config.toml"), base_config).unwrap();

    // Create config.local.toml with partial override (just data_dir)
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
    fs::write(temp.path().join("config.local.toml"), local_config).unwrap();

    let original_dir = env::current_dir().unwrap();
    env::set_current_dir(temp.path()).unwrap();

    let config = Config::load().unwrap();

    env::set_current_dir(original_dir).unwrap();

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
