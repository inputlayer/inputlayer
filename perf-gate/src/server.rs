//! Starting and stopping one `inputlayer-server` process per fixture run.
//!
//! Each run gets a fresh data directory on the gate's data root (a real disk,
//! so durable inserts pay their fsync) and a fresh port. The server runs with
//! production defaults except for [`SERVER_OVERRIDES`].

use std::collections::BTreeMap;
use std::fs::File;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};

use crate::client::Client;

/// Admin password of every server the gate starts (loopback only).
pub const ADMIN_PASSWORD: &str = "perf-gate-admin";

/// How long a server may take to accept its first login.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

/// Environment overrides applied to every server, with the reason for each.
/// Everything else is the default configuration.
pub const SERVER_OVERRIDES: [(&str, &str, &str); 3] = [
    (
        "INPUTLAYER_HTTP__RATE_LIMIT__WS_MAX_MESSAGES_PER_SEC",
        "0",
        "per-connection message cap would bound the measured throughput",
    ),
    (
        "INPUTLAYER_HTTP__RATE_LIMIT__PER_IP_MAX_RPS",
        "0",
        "every gate client shares one loopback IP",
    ),
    (
        "INPUTLAYER_HTTP__GUI__ENABLED",
        "false",
        "the GUI is not under test",
    ),
];

/// Overrides as recorded in the run file.
pub fn recorded_overrides() -> BTreeMap<String, String> {
    SERVER_OVERRIDES
        .iter()
        .map(|(key, value, why)| ((*key).to_string(), format!("{value} ({why})")))
        .collect()
}

/// How to launch a server binary.
#[derive(Debug, Clone)]
pub struct ServerSpec {
    pub binary: PathBuf,
    /// `taskset -c` CPU list, if the servers should be pinned.
    pub cpus: Option<String>,
}

/// A running server; killed and its data removed on drop.
pub struct RunningServer {
    child: Child,
    pub addr: SocketAddr,
    dir: PathBuf,
}

impl RunningServer {
    /// Start `spec` in a fresh directory under `root` and wait until it
    /// accepts a login.
    pub async fn start(spec: &ServerSpec, root: &Path, name: &str) -> Result<Self> {
        let dir = root.join(name);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).with_context(|| format!("clear {}", dir.display()))?;
        }
        std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        let port = free_port()?;
        let log = File::create(dir.join("server.log")).context("create server log")?;
        let mut command = match &spec.cpus {
            Some(cpus) => {
                let mut command = Command::new("taskset");
                command.arg("-c").arg(cpus).arg(&spec.binary);
                command
            }
            None => Command::new(&spec.binary),
        };
        command
            .arg("--port")
            .arg(port.to_string())
            .arg("--data-dir")
            .arg(dir.join("store"))
            // An empty cwd: no stray config.toml / config.local.toml.
            .current_dir(&dir)
            .env("INPUTLAYER_ADMIN_PASSWORD", ADMIN_PASSWORD)
            .stdin(Stdio::null())
            .stdout(log.try_clone().context("clone log handle")?)
            .stderr(log);
        for (key, value, _) in SERVER_OVERRIDES {
            command.env(key, value);
        }
        let child = command
            .spawn()
            .with_context(|| format!("spawn {}", spec.binary.display()))?;
        let mut server = Self {
            child,
            addr: SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
            dir,
        };
        server.wait_ready().await?;
        Ok(server)
    }

    /// Peak resident set size so far, from `/proc/<pid>/status`.
    pub fn peak_rss_kb(&self) -> Option<u64> {
        let status = std::fs::read_to_string(format!("/proc/{}/status", self.child.id())).ok()?;
        status
            .lines()
            .find_map(|line| line.strip_prefix("VmHWM:"))
            .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse().ok())
    }

    /// Connect a logged-in client bound to `kg`.
    pub async fn client(&self, kg: &str) -> Result<Client> {
        Client::connect(self.addr, kg, ADMIN_PASSWORD).await
    }

    async fn wait_ready(&mut self) -> Result<()> {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait()? {
                bail!(
                    "server exited during startup ({status}); see {}",
                    self.dir.join("server.log").display()
                );
            }
            if self.client("default").await.is_ok() {
                return Ok(());
            }
            if Instant::now() > deadline {
                bail!("server not ready within {STARTUP_TIMEOUT:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // Logs are kept for diagnosis; the data directory is not.
        let _ = std::fs::remove_dir_all(self.dir.join("store"));
    }
}

fn free_port() -> Result<u16> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).context("probe free port")?;
    Ok(listener.local_addr()?.port())
}
