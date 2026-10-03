//! Cross-process data_dir exclusivity (#142): a second server on a locked
//! directory refuses to start without touching it, a SIGKILLed owner does not
//! leave the directory locked, and distinct directories stay independent.

use inputlayer::storage::DataDirLock;
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

const READY: &str = "Storage engine initialized";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

fn server(data_dir: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_inputlayer-server"));
    cmd.arg("--data-dir")
        .arg(data_dir)
        .args(["--host", "127.0.0.1", "--port", "0"])
        .env("INPUTLAYER_TRACE", "0");
    cmd
}

/// Start a server and block until its storage engine (and lock) is up.
fn start_owner(data_dir: &Path) -> Child {
    let mut child = server(data_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn server");
    let stdout = child.stdout.take().expect("piped stdout");
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if line.contains(READY) {
                let _ = tx.send(());
            }
        }
    });
    if rx.recv_timeout(STARTUP_TIMEOUT).is_err() {
        let _ = child.kill();
        panic!("server on {} never became ready", data_dir.display());
    }
    child
}

/// Run a server that is expected to exit on its own.
fn run_to_exit(data_dir: &Path) -> Output {
    let mut child = server(data_dir)
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
    panic!("second server on {} did not exit", data_dir.display());
}

fn kill(mut child: Child) {
    child.kill().expect("SIGKILL server");
    child.wait().expect("reap server");
}

/// Every file under `dir` with its exact bytes.
fn tree(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(dir: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(dir).expect("read_dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                out.insert(path.clone(), Vec::new());
                walk(&path, out);
            } else {
                out.insert(path.clone(), fs::read(&path).expect("read file"));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, &mut out);
    out
}

fn assert_refused(output: &Output, data_dir: &Path) {
    assert!(!output.status.success(), "second server must not start");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("in use by another InputLayer process")
            && stderr.contains(&data_dir.display().to_string()),
        "unclear refusal: {stderr}"
    );
    assert!(
        !String::from_utf8_lossy(&output.stdout).contains(READY),
        "refused server must not report readiness"
    );
}

#[test]
fn second_server_on_same_dir_is_refused_and_owner_crash_releases() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("data");

    let owner = start_owner(&dir);
    assert_refused(&run_to_exit(&dir), &dir);

    kill(owner);
    kill(start_owner(&dir));
}

#[test]
fn refused_server_leaves_populated_dir_byte_identical() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("data");
    // Populate a realistic directory, then hold the lock from this process.
    kill(start_owner(&dir));
    let _held = DataDirLock::acquire(&dir).unwrap();
    let before = tree(&dir);

    assert_refused(&run_to_exit(&dir), &dir);

    assert_eq!(tree(&dir), before, "refused server modified the data dir");
}

#[test]
fn servers_on_distinct_dirs_run_side_by_side() {
    let tmp = TempDir::new().unwrap();
    let a = start_owner(&tmp.path().join("a"));
    let b = start_owner(&tmp.path().join("b"));
    kill(a);
    kill(b);
}
