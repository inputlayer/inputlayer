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
