//! `perf-gate genbi`: the engine as the substrate for reactive AI agents,
//! measured on the genbi-trust scenario suite.
//!
//! For every selected scenario a fresh server loads the scenario's IQL seed,
//! agents subscribe to its business questions over `/ws`, and an independent
//! writer applies the ordered mutations. The run file records raw latency
//! samples, throughput, memory and per-checkpoint correctness; the summary
//! groups them by category. See `perf-gate/README.md`.

mod agent;
mod binding;
mod checkpoint;
mod record;
mod report;
mod scenario;
mod score;
mod sql;
mod suite;
mod translate;

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{bail, Context, Result};

use self::agent::Fault;
use self::record::{BenchRecord, Config, SuiteInfo, SCHEMA};
use self::suite::{Case, Suite};
use crate::server::{recorded_overrides, ServerSpec};

/// Categories exercising the reactive agent path (subscription snapshot plus
/// inserted/retracted deltas as facts change), run and reported first.
pub const PRIORITY: [&str; 11] = [
    "R02", "R07", "R10", "A05", "A09", "C03", "C04", "R01", "R03", "R04", "R05",
];

#[derive(clap::Args)]
pub struct GenbiArgs {
    /// genbi-trust checkout (read-only), e.g. `$GENBI_TRUST_DIR`.
    #[arg(long)]
    suite: PathBuf,
    /// `inputlayer-server` binary under test.
    #[arg(long)]
    server: PathBuf,
    /// Provenance of the server (commit), recorded in the result.
    #[arg(long, default_value = "")]
    server_label: String,
    /// Toolchain/build notes recorded in the result.
    #[arg(long, default_value = "")]
    build: String,
    /// `all`, `priority`, `representative` (first case per category), or a
    /// comma-separated list of case ids and category ids.
    #[arg(long, default_value = "all")]
    cases: String,
    /// Runs of every selected scenario, each on a fresh server.
    #[arg(long, default_value_t = 1)]
    repeat: u32,
    /// Agent connections, each subscribed to every question.
    #[arg(long, default_value_t = 1)]
    agents: usize,
    /// Agents are settled once no push arrives for this long.
    #[arg(long, default_value_t = 150)]
    quiet_ms: u64,
    /// Longest wait from a write for agents to converge.
    #[arg(long, default_value_t = 5000)]
    deadline_ms: u64,
    /// Break the agents' delta path on purpose (QC of the checks).
    #[arg(long, value_enum)]
    fault: Option<Fault>,
    /// Pin servers to this `taskset -c` CPU list.
    #[arg(long)]
    server_cpus: Option<String>,
    /// Directory for server data and logs.
    #[arg(long)]
    data_root: PathBuf,
    /// Result file (JSON) to write.
    #[arg(long)]
    out: PathBuf,
    /// Summary (Markdown) to write; it is also printed.
    #[arg(long)]
    summary: Option<PathBuf>,
    /// Fail on any check that does not pass, not only on delta-path failures.
    #[arg(long)]
    strict: bool,
}

/// Run the benchmark. Exit 0 when every agent's answers matched the engine
/// (or, with `--strict`, every check passed); 1 otherwise.
pub fn run(args: &GenbiArgs) -> Result<ExitCode> {
    if args.repeat == 0 || args.agents == 0 {
        bail!("--repeat and --agents must be at least 1");
    }
    let suite = Suite::open(&args.suite)
        .with_context(|| format!("open genbi-trust suite at {}", args.suite.display()))?;
    let cases = select(&suite, &args.cases)?;
    std::fs::create_dir_all(&args.data_root)
        .with_context(|| format!("create {}", args.data_root.display()))?;
    let data_root = args.data_root.canonicalize()?;
    let binary = args
        .server
        .canonicalize()
        .with_context(|| format!("server binary {}", args.server.display()))?;
    let spec = ServerSpec {
        binary: binary.clone(),
        cpus: args.server_cpus.clone(),
    };
    let options = scenario::Options {
        quiet: Duration::from_millis(args.quiet_ms),
        deadline: Duration::from_millis(args.deadline_ms),
        agents: args.agents,
        fault: args.fault,
    };
    let mut environment =
        crate::environment::capture(&data_root, args.server_cpus.clone(), args.build.clone());
    let runtime = tokio::runtime::Runtime::new().context("start runtime")?;
    let mut scenarios = Vec::new();
    for repetition in 1..=args.repeat {
        for case in &cases {
            let run = runtime.block_on(scenario::run(
                &spec, &data_root, &suite, case, repetition, options,
            ));
            eprintln!("{}", report::progress_line(&run));
            scenarios.push(run);
        }
    }
    environment.loadavg_end = crate::environment::loadavg();
    let record = BenchRecord {
        schema: SCHEMA,
        created_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
        environment,
        suite: SuiteInfo {
            dir: suite.dir.display().to_string(),
            version: suite.version.clone(),
            cases_total: suite.cases.len(),
            cases_selected: cases.iter().map(|c| c.id.clone()).collect(),
        },
        config: Config {
            server_label: args.server_label.clone(),
            server_binary: binary.display().to_string(),
            server_sha256: crate::sha256_of(&binary)?,
            server_overrides: recorded_overrides(),
            repeat: args.repeat,
            agents: args.agents,
            quiet_ms: args.quiet_ms,
            deadline_ms: args.deadline_ms,
            fault: args.fault,
        },
        scenarios,
    };
    std::fs::write(&args.out, serde_json::to_string(&record)?)
        .with_context(|| format!("write {}", args.out.display()))?;
    let verdict = report::verdict(&record, args.strict);
    let markdown = report::markdown(&record, &suite, &verdict);
    print!("{markdown}");
    if let Some(path) = &args.summary {
        std::fs::write(path, &markdown).with_context(|| format!("write {}", path.display()))?;
    }
    eprintln!("result file: {}", args.out.display());
    Ok(if verdict.passed {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

/// Cases to run, priority categories first.
fn select(suite: &Suite, spec: &str) -> Result<Vec<Case>> {
    let rank = |case: &Case| {
        PRIORITY
            .iter()
            .position(|p| *p == case.category)
            .unwrap_or(PRIORITY.len())
    };
    let mut cases: Vec<Case> = match spec {
        "all" => suite.cases.clone(),
        "priority" => suite
            .cases
            .iter()
            .filter(|c| PRIORITY.contains(&c.category.as_str()))
            .cloned()
            .collect(),
        "representative" => suite.representative(),
        list => {
            let wanted: Vec<&str> = list.split(',').map(str::trim).collect();
            for item in &wanted {
                if !suite
                    .cases
                    .iter()
                    .any(|c| c.id == *item || c.category == *item)
                {
                    bail!("no case or category '{item}' in the suite");
                }
            }
            suite
                .cases
                .iter()
                .filter(|c| {
                    wanted.contains(&c.id.as_str()) || wanted.contains(&c.category.as_str())
                })
                .cloned()
                .collect()
        }
    };
    cases.sort_by_key(|c| rank(c)); // stable: suite order within a rank
    if cases.is_empty() {
        bail!("no cases selected");
    }
    Ok(cases)
}
