//! Supplied bootstrap secrets at server startup: a blank one counts as unset
//! (the server generates and saves one, as on a first boot with none), a
//! too-short one refuses first-boot startup with an error naming its source,
//! once the admin exists a too-short one is ignored with a warning, and a
//! config password overridden by the environment is never checked.

use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

const READY: &str = "Storage engine initialized";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

/// The server on `dir`, with `env` as its only `INPUTLAYER_*` variables.
fn server(dir: &Path, env: &[(&str, &str)]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_inputlayer-server"));
    cmd.current_dir(dir)
        .arg("--data-dir")
        .arg(dir.join("data"))
        .args(["--host", "127.0.0.1", "--port", "0"]);
    for (key, _) in std::env::vars() {
        if key.starts_with("INPUTLAYER_") {
            cmd.env_remove(key);
        }
    }
    cmd.env("INPUTLAYER_TRACE", "0").envs(env.iter().copied());
    cmd
}

/// Start the server, wait until its storage is up, then stop it. Returns
/// its stderr.
fn boot(dir: &Path, env: &[(&str, &str)]) -> String {
    let mut child = server(dir, env)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn server");
    let stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let stderr = thread::spawn(move || {
        let mut text = String::new();
        let _ = stderr.read_to_string(&mut text);
        text
    });
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if line.contains(READY) {
                let _ = tx.send(());
            }
        }
    });
    let ready = rx.recv_timeout(STARTUP_TIMEOUT).is_ok();
    let _ = child.kill();
    let _ = child.wait();
    let stderr = stderr.join().expect("stderr reader");
    assert!(ready, "server never became ready with {env:?}: {stderr}");
    stderr
}

/// Run a server that is expected to exit on its own.
fn run_to_exit(dir: &Path, env: &[(&str, &str)]) -> Output {
    let mut child = server(dir, env)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn server");
    for _ in 0..600 {
        if child.try_wait().expect("poll server").is_some() {
            return child.wait_with_output().expect("collect output");
        }
        thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    panic!("server with {env:?} did not exit");
}

fn saved_credentials(dir: &Path) -> toml::Table {
    let text = std::fs::read_to_string(dir.join("data").join("credentials.toml"))
        .expect("credentials.toml written");
    toml::from_str(&text).expect("credentials.toml parses")
}

#[test]
fn blank_supplied_secrets_are_generated_and_saved() {
    for blank in ["", "   ", "\t"] {
        let tmp = TempDir::new().unwrap();
        boot(
            tmp.path(),
            &[
                ("INPUTLAYER_ADMIN_PASSWORD", blank),
                ("INPUTLAYER_BOOTSTRAP_API_KEY", blank),
            ],
        );
        let saved = saved_credentials(tmp.path());
        let password = saved["admin_password"].as_str().unwrap();
        assert_eq!(password.len(), 64, "generated password for {blank:?}");
        assert_eq!(saved["api_key"].as_str().unwrap().len(), 64);
    }
}

#[test]
fn short_supplied_secret_refuses_startup() {
    for (var, other) in [
        ("INPUTLAYER_ADMIN_PASSWORD", "INPUTLAYER_BOOTSTRAP_API_KEY"),
        ("INPUTLAYER_BOOTSTRAP_API_KEY", "INPUTLAYER_ADMIN_PASSWORD"),
    ] {
        let tmp = TempDir::new().unwrap();
        let out = run_to_exit(tmp.path(), &[(var, "eleven-char"), (other, "")]);
        assert!(!out.status.success(), "{var}: server started");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains(var) && stderr.contains("12 characters"),
            "{var}: {stderr}"
        );
        assert!(
            !tmp.path().join("data").join("credentials.toml").exists(),
            "{var}: bootstrap ran"
        );
    }
}

#[test]
fn short_supplied_secret_is_ignored_with_a_warning_once_admin_exists() {
    for var in ["INPUTLAYER_ADMIN_PASSWORD", "INPUTLAYER_BOOTSTRAP_API_KEY"] {
        let tmp = TempDir::new().unwrap();
        let strong = [
            ("INPUTLAYER_ADMIN_PASSWORD", "a-strong-enough-secret"),
            ("INPUTLAYER_BOOTSTRAP_API_KEY", "a-strong-enough-api-key"),
        ];
        boot(tmp.path(), &strong);
        let weak = strong.map(|(key, value)| (key, if key == var { "admin" } else { value }));
        let stderr = boot(tmp.path(), &weak);
        assert!(
            stderr.contains(&format!("WARNING: {var}")) && stderr.contains("12 characters"),
            "{var}: {stderr}"
        );
    }
}

#[test]
fn config_password_overridden_by_env_is_ignored_on_first_boot() {
    const ENV_PASSWORD: &str = "a-strong-enough-secret";
    let tmp = TempDir::new().unwrap();
    std::fs::write(
        tmp.path().join("config.toml"),
        "[http.auth]\nbootstrap_admin_password = \"admin\"\n",
    )
    .unwrap();
    let stderr = boot(
        tmp.path(),
        &[
            ("INPUTLAYER_ADMIN_PASSWORD", ENV_PASSWORD),
            ("INPUTLAYER_BOOTSTRAP_API_KEY", ""),
        ],
    );
    assert!(
        stderr.contains("bootstrap_admin_password is ignored"),
        "{stderr}"
    );

    let mut config = inputlayer::Config::default();
    config.storage.data_dir = tmp.path().join("data");
    let handler = inputlayer::protocol::Handler::from_config(config).unwrap();
    handler.bootstrap_auth().unwrap();
    assert!(handler.authenticate_user("admin", ENV_PASSWORD).is_ok());
    assert!(handler.authenticate_user("admin", "admin").is_err());
}
