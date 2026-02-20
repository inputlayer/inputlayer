//! WI-06: Server startup error handling tests.
//! Tests that Handler::from_config returns Err on invalid config
//! (instead of panicking via .expect()).

use inputlayer::protocol::Handler;
use inputlayer::Config;
use std::path::PathBuf;
use tempfile::TempDir;

#[test]
fn test_handler_from_config_valid_config_succeeds() {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    assert!(Handler::from_config(config).is_ok());
}

#[test]
fn test_handler_from_config_invalid_data_dir_returns_err() {
    let mut config = Config::default();
    config.storage.data_dir = PathBuf::from("/dev/null/impossible");
    // Should return Err, not panic
    let result = Handler::from_config(config);
    assert!(
        result.is_err(),
        "Handler::from_config with impossible data_dir should return Err"
    );
}

#[test]
fn test_handler_from_config_error_message_is_human_readable() {
    let mut config = Config::default();
    config.storage.data_dir = PathBuf::from("/dev/null/impossible");
    let result = Handler::from_config(config);
    let err = result.err().unwrap();
    // The error message should be a string, not empty
    assert!(!err.is_empty(), "Error message should not be empty");
    // Should contain useful context
    assert!(
        err.contains("storage")
            || err.contains("create")
            || err.contains("failed")
            || err.contains("Failed"),
        "Error message should contain context about what failed, got: {err}"
    );
}
