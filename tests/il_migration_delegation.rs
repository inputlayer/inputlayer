//! Integration tests for `il migration` delegation.
//!
//! The il CLI owns the migration vocabulary (generate/apply/revert/status)
//! and translates each verb to the Python tool's subcommand. These tests
//! pin the translation, flag passthrough, exit-code propagation, and the
//! missing-tool error, using a fake `inputlayer-migrate` on PATH.

#![cfg(unix)]

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

struct FakeTool {
    dir: tempfile::TempDir,
}

impl FakeTool {
    /// A fake inputlayer-migrate that records its argv and exits with
    /// FAKE_EXIT (default 0).
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("inputlayer-migrate");
        let mut f = std::fs::File::create(&path).expect("create fake tool");
        writeln!(
            f,
            "#!/bin/sh\necho \"$@\" > \"$FAKE_OUT\"\nexit \"${{FAKE_EXIT:-0}}\""
        )
        .expect("write fake tool");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("chmod fake tool");
        Self { dir }
    }

    fn run(&self, il_args: &[&str], fake_exit: &str) -> (std::process::ExitStatus, String) {
        let out_file = self.dir.path().join("argv.txt");
        let status = Command::new(env!("CARGO_BIN_EXE_il"))
            .args(il_args)
            .env("PATH", self.dir.path())
            .env("FAKE_OUT", &out_file)
            .env("FAKE_EXIT", fake_exit)
            .status()
            .expect("run il");
        let argv = std::fs::read_to_string(&out_file).unwrap_or_default();
        (status, argv.trim().to_string())
    }
}

#[test]
fn generate_translates_to_makemigrations_with_passthrough_flags() {
    let tool = FakeTool::new();
    let (status, argv) = tool.run(
        &[
            "migration",
            "generate",
            "--models",
            "myapp.models",
            "--migrations-dir",
            "m",
        ],
        "0",
    );
    assert!(status.success());
    assert_eq!(
        argv,
        "makemigrations --models myapp.models --migrations-dir m"
    );
}

#[test]
fn apply_translates_to_migrate() {
    let tool = FakeTool::new();
    let (status, argv) = tool.run(
        &["migration", "apply", "--url", "ws://x/ws", "--kg", "prod"],
        "0",
    );
    assert!(status.success());
    assert_eq!(argv, "migrate --url ws://x/ws --kg prod");
}

#[test]
fn status_translates_to_showmigrations() {
    let tool = FakeTool::new();
    let (status, argv) = tool.run(&["migration", "status", "--url", "ws://x/ws"], "0");
    assert!(status.success());
    assert_eq!(argv, "showmigrations --url ws://x/ws");
}

#[test]
fn revert_passes_target_through() {
    let tool = FakeTool::new();
    let (status, argv) = tool.run(&["migration", "revert", "0001_initial"], "0");
    assert!(status.success());
    assert_eq!(argv, "revert 0001_initial");
}

#[test]
fn leaf_help_passes_through_to_the_tool() {
    let tool = FakeTool::new();
    let (status, argv) = tool.run(&["migration", "generate", "--help"], "0");
    assert!(status.success());
    assert_eq!(argv, "makemigrations --help");
}

#[test]
fn exit_code_propagates() {
    let tool = FakeTool::new();
    let (status, _) = tool.run(&["migration", "apply"], "3");
    assert_eq!(status.code(), Some(3));
}

#[test]
fn missing_tool_gives_install_hint() {
    let empty = tempfile::tempdir().expect("tempdir");
    let output = Command::new(env!("CARGO_BIN_EXE_il"))
        .args(["migration", "status"])
        .env("PATH", empty.path())
        .output()
        .expect("run il");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("inputlayer-migrate not found"),
        "stderr: {stderr}"
    );
    assert!(stderr.contains("pip install"), "stderr: {stderr}");
}
