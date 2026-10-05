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
    /// `[replication]`; `None` runs standalone.
    pub replication: Option<Replication>,
    /// `http.ws_send_timeout_ms`; `None` keeps the default.
    pub ws_send_timeout_ms: Option<u64>,
    /// `http.rate_limit.max_ws_connections` and `max_connections`; `None`
    /// keeps the defaults.
    pub max_connections: Option<usize>,
    /// `http.rate_limit.ws_max_preauth_per_ip`; `None` keeps the default.
    pub ws_max_preauth_per_ip: Option<usize>,
    /// Run the process under `taskset -c <cpus>`; `None` runs it unpinned.
    pub cpus: Option<String>,
}

/// The `[replication]` section of an engine's config.
#[derive(Debug, Clone)]
pub struct Replication {
    /// `"primary"` or `"follower"`.
    pub role: &'static str,
    /// Shared stream token (at least 16 characters).
    pub token: String,
    /// A follower's primary, e.g. [`Engine::http_url`].
    pub primary_url: Option<String>,
    /// `retain_bytes`; `None` keeps the default.
    pub retain_bytes: Option<usize>,
    /// `heartbeat_ms`; `None` keeps the default.
    pub heartbeat_ms: Option<u64>,
    /// `timeout_ms`; `None` keeps the default.
    pub timeout_ms: Option<u64>,
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

    /// Fail a connection whose client has not taken a frame within `ms`
    /// (`http.ws_send_timeout_ms`).
    #[must_use]
    pub fn ws_send_timeout_ms(mut self, ms: u64) -> Self {
        self.settings.ws_send_timeout_ms = Some(ms);
        self
    }

    /// Accept up to `limit` connections (HTTP and `/ws`).
    #[must_use]
    pub fn max_connections(mut self, limit: usize) -> Self {
        self.settings.max_connections = Some(limit);
        self
    }

    /// Allow `limit` unauthenticated `/ws` connections per client IP at once
    /// (0 = unlimited). Every test client connects from 127.0.0.1.
    #[must_use]
    pub fn ws_max_preauth_per_ip(mut self, limit: usize) -> Self {
        self.settings.ws_max_preauth_per_ip = Some(limit);
        self
    }

    /// Pin the process to `cpus` (a `taskset -c` list such as `8-31`).
    #[must_use]
    pub fn cpus(mut self, cpus: impl Into<String>) -> Self {
        self.settings.cpus = Some(cpus.into());
        self
    }

    /// Run with this `[replication]` section.
    #[must_use]
    pub fn replication(mut self, replication: Replication) -> Self {
        self.settings.replication = Some(replication);
        self
    }

    /// Start the engine and wait until it serves `/health`.
    ///
    /// A follower bootstraps no admin key of its own (its users come from
    /// its primary); give it one with [`Engine::set_api_key`].
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
        engine.launch(None).await?;
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

    /// Use `key` to authenticate (a follower takes its primary's keys).
    pub fn set_api_key(&mut self, key: &str) {
        self.api_key = key.to_string();
    }

    /// The running process's id, e.g. to sample its memory.
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(Child::id)
    }

    /// The port the engine listens on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Base HTTP URL, e.g. `http://127.0.0.1:4321`.
    pub fn http_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// The engine's data directory.
    pub fn data_dir(&self) -> PathBuf {
        self.dir.path().join("data")
    }

    /// Kill the process (SIGKILL: no clean shutdown).
    pub async fn stop(&mut self) -> Result<()> {
        self.kill().await
    }

    /// Start a stopped engine again on the same data directory and port.
    pub async fn restart(&mut self) -> Result<()> {
        self.kill().await?;
        self.launch(Some(self.port)).await
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
        self.launch(None).await
    }

    async fn kill(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            child.kill().await.context("kill engine")?;
        }
        Ok(())
    }

    async fn launch(&mut self, port: Option<u16>) -> Result<()> {
        let mut last_error = None;
        for _ in 0..PORT_ATTEMPTS {
            self.port = match port {
                Some(port) => port,
                None => free_port()?,
            };
            self.write_config()?;
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.log_path())
                .context("open server log")?;
            // `taskset` execs the engine, so the process id stays the engine's.
            let mut command = match &self.settings.cpus {
                Some(cpus) => {
                    let mut command = Command::new("taskset");
                    command.arg("-c").arg(cpus).arg(&self.binary);
                    command
                }
                None => Command::new(&self.binary),
            };
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
                    if !self.is_follower() {
                        self.api_key = read_api_key(&self.credentials_path())?;
                    }
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

    fn is_follower(&self) -> bool {
        self.settings
            .replication
            .as_ref()
            .is_some_and(|r| r.role == "follower")
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
        if let Some(limit) = self.settings.ws_max_preauth_per_ip {
            rate_limit.insert("ws_max_preauth_per_ip".into(), integer(limit)?);
        }
        if let Some(limit) = self.settings.max_connections {
            rate_limit.insert("max_connections".into(), integer(limit)?);
            rate_limit.insert("max_ws_connections".into(), integer(limit)?);
        }
        let mut http = toml::Table::new();
        http.insert("enabled".into(), true.into());
        http.insert("host".into(), "127.0.0.1".into());
        http.insert("port".into(), i64::from(self.port).into());
        http.insert("auth".into(), auth.into());
        http.insert("rate_limit".into(), rate_limit.into());
        if let Some(ms) = self.settings.ws_send_timeout_ms {
            http.insert("ws_send_timeout_ms".into(), integer_u64(ms)?);
        }
        let mut logging = toml::Table::new();
        logging.insert("level".into(), "warn".into());

        let mut config = toml::Table::new();
        if let Some(replication) = &self.settings.replication {
            let mut section = toml::Table::new();
            section.insert("role".into(), replication.role.into());
            section.insert("token".into(), replication.token.clone().into());
            if let Some(url) = &replication.primary_url {
                section.insert("primary_url".into(), url.clone().into());
            }
            if let Some(bytes) = replication.retain_bytes {
                section.insert("retain_bytes".into(), integer(bytes)?);
            }
            if let Some(ms) = replication.heartbeat_ms {
                section.insert("heartbeat_ms".into(), integer_u64(ms)?);
            }
            if let Some(ms) = replication.timeout_ms {
                section.insert("timeout_ms".into(), integer_u64(ms)?);
            }
            config.insert("replication".into(), section.into());
        }
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

fn integer_u64(value: u64) -> Result<toml::Value> {
    Ok(i64::try_from(value)
        .context("config value too large")?
        .into())
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
