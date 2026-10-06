//! Helpers the ws modules share: the configuration of a test server and
//! serving a handler on a loopback port.

// Test setup aborts on failure; `unwrap` is the intended behavior.
#![allow(clippy::unwrap_used)]

use inputlayer::protocol::rest::create_router;
use inputlayer::protocol::Handler;
use inputlayer::Config;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

/// The configuration of an engine on `dir`. Its computations use 4 threads,
/// as on CI, so the timing budgets hold on a larger host: the first engine
/// in this binary sizes the pool, so every module builds on this.
pub fn engine_config(dir: &Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    config.storage.performance.num_threads = 4;
    config
}

/// The configuration of a server in its own directory: `admin` signs in with
/// `password`, credentials are saved next to the data, and neither the
/// message rate limit nor the GUI is on.
pub fn config(password: &str) -> (Config, TempDir) {
    let tmp = TempDir::new().unwrap();
    let mut config = engine_config(&tmp.path().join("data"));
    config.http.auth.bootstrap_admin_password = Some(password.to_string());
    config.http.auth.credentials_file = Some(tmp.path().join("credentials.toml"));
    config.http.rate_limit.ws_max_messages_per_sec = 0;
    config.http.gui.enabled = false;
    (config, tmp)
}

/// Serve `handler` on a free loopback port until the task is aborted.
pub async fn serve(handler: &Arc<Handler>) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let app = create_router(Arc::clone(handler), &handler.config().http);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, task)
}
