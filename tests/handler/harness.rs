//! Helpers the handler modules share.

use inputlayer::protocol::Handler;
use inputlayer::{Config, StorageEngine};
use std::path::Path;
use tempfile::TempDir;

/// A storage engine of the default configuration on its own data directory.
pub fn storage() -> (StorageEngine, TempDir) {
    let temp = TempDir::new().expect("create temp dir");
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    let storage = StorageEngine::new(config).expect("create storage engine");
    (storage, temp)
}

/// A handler of the default configuration on its own data directory.
pub fn handler() -> (Handler, TempDir) {
    let (storage, temp) = storage();
    (Handler::new(storage), temp)
}

/// A handler of the default configuration on `dir`.
pub fn handler_at(dir: &Path) -> Handler {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    Handler::new(StorageEngine::new(config).expect("create storage engine"))
}

/// Held by a test for as long as it relies on `INPUTLAYER_REGISTRY`: the
/// variable is the process's, and every module shares this binary.
pub async fn registry_env() -> tokio::sync::MutexGuard<'static, ()> {
    static REGISTRY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    REGISTRY.lock().await
}
