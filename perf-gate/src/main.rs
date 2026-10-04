//! `perf-gate`: InputLayer's same-host performance gate.
//!
//! `run` measures a baseline and a candidate server binary in interleaved
//! rounds and writes every raw sample to a run file; `compare` judges a run
//! file under the policy and exits zero only on a pass; `genbi` benchmarks
//! one server as the substrate for reactive agents on the genbi-trust suite;
//! `sessions` measures standing-query cost as the sessions on one knowledge
//! graph grow.
//! See `perf-gate/README.md`.

mod client;
mod compare;
mod dataset;
mod environment;
mod fixtures;
mod genbi;
mod policy;
mod profile;
mod report;
mod runner;
mod schema;
mod server;
mod sessions;
mod stats;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use sha2::{Digest, Sha256};

use crate::compare::Status;
use crate::fixtures::Fixture;
use crate::policy::Policy;
use crate::profile::Profile;
use crate::runner::{ArmSpec, Plan};
use crate::schema::{Arm, RunRecord};
use crate::server::ServerSpec;

#[derive(Parser)]
#[command(name = "perf-gate", about = "InputLayer same-host performance gate")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Measure baseline and candidate servers; write a run file.
    Run(RunArgs),
    /// Judge a run file; exit 0 pass, 1 fail, 2 inconclusive, 3 invalid.
    Compare(CompareArgs),
    /// Benchmark one server as reactive-agent substrate on genbi-trust.
    Genbi(genbi::GenbiArgs),
    /// Measure standing-query cost as sessions grow (voice-agent pack).
    Sessions(sessions::SessionsArgs),
}

#[derive(clap::Args)]
struct RunArgs {
    /// Baseline `inputlayer-server` binary.
    #[arg(long)]
    baseline: PathBuf,
    /// Provenance of the baseline (commit).
    #[arg(long)]
    baseline_label: String,
    /// Candidate `inputlayer-server` binary.
    #[arg(long)]
    candidate: PathBuf,
    #[arg(long)]
    candidate_label: String,
    /// Workload profile: standard (acceptance) or quick (smoke only).
    #[arg(long, default_value = "standard")]
    profile: String,
    /// Rounds per arm.
    #[arg(long, default_value_t = 10)]
    rounds: u32,
    /// Comma-separated fixture subset (default: all).
    #[arg(long, value_delimiter = ',')]
    fixtures: Vec<String>,
    /// Pin servers to this `taskset -c` CPU list.
    #[arg(long)]
    server_cpus: Option<String>,
    /// Directory for server data and logs; must be on a real disk.
    #[arg(long)]
    data_root: PathBuf,
    /// Toolchain/build notes recorded in the run file.
    #[arg(long, default_value = "")]
    build: String,
    /// Run file to write.
    #[arg(long)]
    out: PathBuf,
}

#[derive(clap::Args)]
struct CompareArgs {
    run: PathBuf,
    #[arg(long, default_value = "perf-gate/policy.toml")]
    policy: PathBuf,
    /// Markdown report to write.
    #[arg(long)]
    report: Option<PathBuf>,
    /// JSON verdict to write.
    #[arg(long)]
    verdict: Option<PathBuf>,
}

fn main() -> ExitCode {
    let result = match Cli::parse().command {
        Command::Run(args) => run(&args).map(|()| ExitCode::SUCCESS),
        Command::Compare(args) => compare(&args),
        Command::Genbi(args) => genbi::run(&args),
        Command::Sessions(args) => sessions::run(&args),
    };
    result.unwrap_or_else(|e| {
        eprintln!("perf-gate: {e:#}");
        ExitCode::from(3)
    })
}

fn run(args: &RunArgs) -> Result<()> {
    let profile = Profile::named(&args.profile)
        .with_context(|| format!("unknown profile '{}'", args.profile))?;
    let fixtures = if args.fixtures.is_empty() {
        Fixture::ALL.to_vec()
    } else {
        args.fixtures
            .iter()
            .map(|name| Fixture::parse(name))
            .collect::<Result<_>>()?
    };
    std::fs::create_dir_all(&args.data_root)
        .with_context(|| format!("create {}", args.data_root.display()))?;
    // Servers run in their own directories: every path must be absolute.
    let data_root = args.data_root.canonicalize()?;
    let arm = |name: &str, binary: &Path, label: &str| -> Result<ArmSpec> {
        let binary = binary
            .canonicalize()
            .with_context(|| format!("server binary {}", binary.display()))?;
        Ok(ArmSpec {
            arm: Arm {
                name: name.to_string(),
                label: label.to_string(),
                binary: binary.display().to_string(),
                binary_sha256: sha256_of(&binary)?,
            },
            server: ServerSpec {
                binary,
                cpus: args.server_cpus.clone(),
                env: Vec::new(),
            },
        })
    };
    let plan = Plan {
        profile,
        rounds: args.rounds,
        fixtures,
        arms: [
            arm("baseline", &args.baseline, &args.baseline_label)?,
            arm("candidate", &args.candidate, &args.candidate_label)?,
        ],
        data_root,
    };
    let environment = environment::capture(
        &plan.data_root,
        args.server_cpus.clone(),
        args.build.clone(),
    );
    let runtime = tokio::runtime::Runtime::new().context("start runtime")?;
    // On interrupt the run future is dropped, which kills its server.
    let record = runtime.block_on(async {
        tokio::select! {
            record = runner::run(&plan, environment) => record,
            signal = interrupted() => {
                signal?;
                anyhow::bail!("interrupted; no run file written")
            }
        }
    })?;
    let json = serde_json::to_string(&record)?;
    std::fs::write(&args.out, json).with_context(|| format!("write {}", args.out.display()))?;
    eprintln!("run file: {}", args.out.display());
    Ok(())
}

fn compare(args: &CompareArgs) -> Result<ExitCode> {
    let text = std::fs::read_to_string(&args.run)
        .with_context(|| format!("read {}", args.run.display()))?;
    let record: RunRecord =
        serde_json::from_str(&text).with_context(|| format!("parse {}", args.run.display()))?;
    let policy = Policy::load(&args.policy)?;
    let verdict = compare::judge(&record, &policy);
    let markdown = report::markdown(&record, &policy, &verdict);
    print!("{markdown}");
    if let Some(path) = &args.report {
        std::fs::write(path, &markdown).with_context(|| format!("write {}", path.display()))?;
    }
    if let Some(path) = &args.verdict {
        let json = serde_json::to_string_pretty(&verdict)?;
        std::fs::write(path, json).with_context(|| format!("write {}", path.display()))?;
    }
    Ok(ExitCode::from(match verdict.status {
        Status::Pass => 0,
        Status::Fail => 1,
        Status::Inconclusive => 2,
        Status::Invalid => 3,
    }))
}

/// Resolves on SIGINT or SIGTERM.
async fn interrupted() -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = terminate.recv() => {}
    }
    Ok(())
}

fn sha256_of(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
