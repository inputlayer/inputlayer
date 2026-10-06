//! Helpers the ws modules share: the configuration of a test server and
//! serving a handler on a loopback port.

// Test setup aborts on failure; `unwrap` is the intended behavior.
#![allow(clippy::unwrap_used)]

use inputlayer::protocol::rest::create_router;
use inputlayer::protocol::Handler;
use inputlayer::{Config, StorageEngine};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Once};
use tempfile::TempDir;
use tracing_subscriber::prelude::*;

/// The configuration of an engine on `dir`. The process's computations use
/// 4 threads, as on CI, so the timing budgets hold on a larger host: the
/// pool is sized here once, so every module builds on this.
pub fn engine_config(dir: &Path) -> Config {
    static POOL: Once = Once::new();
    POOL.call_once(|| StorageEngine::set_num_threads(4).unwrap());
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
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

thread_local! {
    /// What receives the log lines of this thread, while a test captures them.
    static LOG_SINK: RefCell<Option<Rc<dyn Fn(&str)>>> = const { RefCell::new(None) };
}

/// Hands each formatted event to the sink of the thread that emitted it.
struct ThreadLogs;

impl std::io::Write for ThreadLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let sink = LOG_SINK.try_with(|sink| sink.borrow().clone());
        if let Ok(Some(sink)) = sink {
            sink(&String::from_utf8_lossy(buf));
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Ends the capture of its thread's log lines when dropped.
pub struct LogCapture(());

impl Drop for LogCapture {
    fn drop(&mut self) {
        LOG_SINK.with(|sink| sink.borrow_mut().take());
    }
}

/// Pass every INFO-and-above log line this thread emits to `sink`, as it is
/// emitted, until the guard drops. A test on the current-thread runtime also
/// receives its server task's lines. The subscriber is the process's and is
/// installed here once: a thread-local one would be the only dispatcher, so
/// another test's thread reaching an event first would cache it as disabled
/// everywhere. Threads that capture nothing stay untraced.
pub fn capture_logs(sink: impl Fn(&str) + 'static) -> LogCapture {
    static SUBSCRIBER: Once = Once::new();
    SUBSCRIBER.call_once(|| {
        let capturing = tracing_subscriber::filter::dynamic_filter_fn(|metadata, _| {
            *metadata.level() <= tracing::Level::INFO
                && LOG_SINK
                    .try_with(|sink| sink.borrow().is_some())
                    .unwrap_or(false)
        })
        .with_max_level_hint(tracing::Level::INFO);
        let lines = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(|| ThreadLogs)
            .with_filter(capturing);
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(lines))
            .expect("no other global subscriber");
    });
    LOG_SINK.with(|slot| *slot.borrow_mut() = Some(Rc::new(sink)));
    LogCapture(())
}
