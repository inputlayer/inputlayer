//! A real `inputlayer-server` process per test, as installed per tenant.
//!
//! Each [`Engine`] owns a private data directory, a generated config file and
//! a free localhost port. Nothing is shared between engines, so tests run in
//! parallel without coordinating ports or data.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

/// How long a fresh or restarted engine may take to answer `/health`.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
/// Attempts at binding a free port (another process may grab it first).
const PORT_ATTEMPTS: usize = 3;
/// Admin password written into the generated config.
const ADMIN_PASSWORD: &str = "testkit-admin-password";

/// Engine settings a scenario may change; everything else is the shipped default.
#[derive(Debug, Clone, Default)]
pub struct EngineSettings {
    /// `storage.performance.max_result_rows`; `None` keeps the default cap.
    pub max_result_rows: Option<usize>,
    /// `http.rate_limit.ws_max_subscriptions`; `None` keeps the default.
    pub ws_max_subscriptions: Option<usize>,
    /// `http.rate_limit.notification_buffer_size`; `None` keeps the default.
    pub notification_buffer_size: Option<usize>,
}

/// Configures and starts an [`Engine`].
#[derive(Debug, Clone)]
pub struct EngineBuilder {
    binary: PathBuf,
    settings: EngineSettings,
}

impl EngineBuilder {
    /// Engine from the `inputlayer-server` binary at `binary`.
    ///
    /// Integration tests and benches of the engine crate get the path from
    /// `env!("CARGO_BIN_EXE_inputlayer-server")`.
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
            settings: EngineSettings::default(),
        }
    }

    /// Cap every final query result at `rows` (see `max_result_rows`).
    #[must_use]
    pub fn max_result_rows(mut self, rows: usize) -> Self {
        self.settings.max_result_rows = Some(rows);
        self
    }

    /// Allow `limit` standing queries per connection (0 = unlimited).
    #[must_use]
    pub fn ws_max_subscriptions(mut self, limit: usize) -> Self {
        self.settings.ws_max_subscriptions = Some(limit);
        self
    }

    /// Buffer `size` notifications per connection; a connection that falls
    /// further behind is told it missed some (then, past `size` in total,
    /// disconnected).
    #[must_use]
    pub fn notification_buffer_size(mut self, size: usize) -> Self {
        self.settings.notification_buffer_size = Some(size);
        self
    }

    /// Start the engine and wait until it serves `/health`.
    pub async fn start(self) -> Result<Engine> {
        let dir = TempDir::new().context("create engine directory")?;
        let mut engine = Engine {
            binary: self.binary,
            settings: self.settings,
            dir,
            port: 0,
            child: None,
            api_key: String::new(),
        };
        engine.launch().await?;
        Ok(engine)
    }
}

/// A running engine process. Killed on drop.
pub struct Engine {
    binary: PathBuf,
    settings: EngineSettings,
    dir: TempDir,
    port: u16,
    child: Option<Child>,
    api_key: String,
}

impl Engine {
    /// WebSocket URL of a session bound to `knowledge_graph`.
    pub fn ws_url(&self, knowledge_graph: &str) -> String {
        format!("ws://127.0.0.1:{}/ws?kg={knowledge_graph}", self.port)
    }

    /// The admin API key the engine bootstrapped.
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Server log, for diagnosing a failed scenario.
    pub fn log_path(&self) -> PathBuf {
        self.dir.path().join("server.log")
    }

    /// Kill the process without a clean shutdown and start it again on the
    /// same data directory (a crash-restart). The port changes; reconnect via
    /// [`Engine::ws_url`].
    pub async fn crash_restart(&mut self) -> Result<()> {
        self.kill().await?;
        self.launch().await
    }

    async fn kill(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            child.kill().await.context("kill engine")?;
        }
        Ok(())
    }

    async fn launch(&mut self) -> Result<()> {
        let mut last_error = None;
        for _ in 0..PORT_ATTEMPTS {
            self.port = free_port()?;
            self.write_config()?;
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.log_path())
                .context("open server log")?;
            let mut command = Command::new(&self.binary);
            command
                .arg("--config")
                .arg(self.config_path())
                .current_dir(self.dir.path())
                .stdin(Stdio::null())
                .stdout(log.try_clone().context("clone log handle")?)
                .stderr(log)
                .kill_on_drop(true);
            // In --config mode unknown INPUTLAYER_* variables are rejected and
            // known ones would override the generated config.
            for (key, _) in std::env::vars_os() {
                if key.to_string_lossy().starts_with("INPUTLAYER_") {
                    command.env_remove(key);
                }
            }
            let child = command
                .spawn()
                .with_context(|| format!("spawn {}", self.binary.display()))?;
            self.child = Some(child);
            match self.wait_ready().await {
                Ok(()) => {
                    self.api_key = read_api_key(&self.credentials_path())?;
                    return Ok(());
                }
                Err(e) => {
                    self.kill().await?;
                    last_error = Some(e);
                }
            }
        }
        let log = std::fs::read_to_string(self.log_path()).unwrap_or_default();
        let tail: String = log.lines().rev().take(20).collect::<Vec<_>>().join("\n");
        Err(last_error
            .unwrap_or_else(|| anyhow::anyhow!("engine did not start"))
            .context(format!("engine log (last lines, reversed):\n{tail}")))
    }

    async fn wait_ready(&mut self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
        loop {
            if let Some(child) = self.child.as_mut() {
                if let Some(status) = child.try_wait().context("poll engine")? {
                    bail!("engine exited during startup: {status}");
                }
            }
            if health_ok(self.port).await {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("engine not healthy after {STARTUP_TIMEOUT:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn config_path(&self) -> PathBuf {
        self.dir.path().join("engine.toml")
    }

    fn credentials_path(&self) -> PathBuf {
        self.dir.path().join("credentials.toml")
    }

    fn write_config(&self) -> Result<()> {
        let mut storage = toml::Table::new();
        storage.insert(
            "data_dir".into(),
            path_value(&self.dir.path().join("data"))?,
        );
        if let Some(rows) = self.settings.max_result_rows {
            let mut performance = toml::Table::new();
            performance.insert("max_result_rows".into(), integer(rows)?);
            storage.insert("performance".into(), performance.into());
        }
        let mut auth = toml::Table::new();
        auth.insert(
            "bootstrap_admin_password".into(),
            ADMIN_PASSWORD.to_string().into(),
        );
        auth.insert(
            "credentials_file".into(),
            path_value(&self.credentials_path())?,
        );
        // Writers and auditors send faster than the per-connection defaults allow.
        let mut rate_limit = toml::Table::new();
        rate_limit.insert("ws_max_messages_per_sec".into(), 0.into());
        rate_limit.insert("per_ip_max_rps".into(), 0.into());
        if let Some(limit) = self.settings.ws_max_subscriptions {
            rate_limit.insert("ws_max_subscriptions".into(), integer(limit)?);
        }
        if let Some(size) = self.settings.notification_buffer_size {
            rate_limit.insert("notification_buffer_size".into(), integer(size)?);
        }
        let mut http = toml::Table::new();
        http.insert("enabled".into(), true.into());
        http.insert("host".into(), "127.0.0.1".into());
        http.insert("port".into(), i64::from(self.port).into());
        http.insert("auth".into(), auth.into());
        http.insert("rate_limit".into(), rate_limit.into());
        let mut logging = toml::Table::new();
        logging.insert("level".into(), "warn".into());

        let mut config = toml::Table::new();
        config.insert("storage".into(), storage.into());
        config.insert("http".into(), http.into());
        config.insert("logging".into(), logging.into());
        std::fs::write(self.config_path(), config.to_string()).context("write engine config")
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
    }
}

fn free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind a free port")?;
    Ok(listener.local_addr()?.port())
}

fn path_value(path: &Path) -> Result<toml::Value> {
    let path = path
        .to_str()
        .with_context(|| format!("non-UTF-8 path {}", path.display()))?;
    Ok(path.to_string().into())
}

fn integer(value: usize) -> Result<toml::Value> {
    Ok(i64::try_from(value)
        .context("config value too large")?
        .into())
}

async fn health_ok(port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)).await else {
        return false;
    };
    let request = "GET /health HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n";
    if stream.write_all(request.as_bytes()).await.is_err() {
        return false;
    }
    let mut response = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut response));
    matches!(read.await, Ok(Ok(_)))
        && (response.starts_with(b"HTTP/1.1 200") || response.starts_with(b"HTTP/1.0 200"))
}

fn read_api_key(credentials: &Path) -> Result<String> {
    let text = std::fs::read_to_string(credentials)
        .with_context(|| format!("read {}", credentials.display()))?;
    let table: toml::Table = text.parse().context("parse credentials file")?;
    table
        .get("api_key")
        .and_then(toml::Value::as_str)
        .map(str::to_string)
        .context("credentials file has no api_key")
}
