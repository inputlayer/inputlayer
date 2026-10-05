//! `perf-gate sessions`: what standing queries cost as sessions grow.
//!
//! The workload is the voice-agent reference architecture
//! (`content/blog/building-a-voice-agent-that-knows.mdx`, Appendix A pack,
//! verbatim in `pack.iql`) on one tenant graph, as its hostile review
//! measured it:
//!
//! - every session has the article's world and session facts (an order, its
//!   shipment with an on-time ETA, an open `delivery_status` goal) and, on its
//!   own connection, the article's speech subscription
//!   `?speech("s-i", ...), speech_owner("s-i", "sp-i", 1)`;
//! - one executor connection subscribes to `work` for all sessions;
//! - every session commits a receipt (`playback` and `playback_time`) about
//!   once per `1/rate` seconds on its own connection. The receipts name a goal
//!   no claim reads: the write is in every speech query's dependencies yet
//!   changes no result, which isolates the cost sessions impose on each other;
//! - an adapter connection probes: it replaces a random session's ETA and
//!   times the commit reply and the arrival of that session's delta, which
//!   must carry the new ETA as a `state` row.
//!
//! Each size runs on a fresh server: 40 probes without background load, then
//! the receipt load with a probe every 150 ms. Server CPU is measured over the
//! loaded window after a 2 s warm-up. A probe whose delta never arrives, a
//! delta no probe caused, or an error makes the run fail. `--mode shared-view`
//! replaces the per-session subscriptions with one unbound view whose rows the
//! client routes by session, the review's comparison point.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{mpsc, watch};

use crate::client::{Client, Stamped};
use crate::schema::Environment;
use crate::server::{recorded_overrides, RunningServer, ServerSpec};

/// The article's Appendix A rule pack.
const PACK: &str = include_str!("pack.iql");

const KG: &str = "voice";

/// A time far in the future (2100-01-01), for leases and speech windows.
const FAR: u64 = 4_102_444_800_000;

/// Longest wait for a probe's delta before the next probe; a delta that
/// arrives later still counts, as late.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long, after the load stops, overdue probes may still deliver their
/// delta before they count as missing.
const LATE_DRAIN: Duration = Duration::from_secs(60);

const WARM_UP: Duration = Duration::from_secs(2);
const PROBE_INTERVAL: Duration = Duration::from_millis(150);
const IDLE_PROBE_GAP: Duration = Duration::from_millis(20);

/// Connections opened at once while setting up sessions.
const CONNECT_BATCH: usize = 16;

/// Server settings every run needs: hundreds of connections from one address.
const SESSION_OVERRIDES: [(&str, &str); 1] =
    [("INPUTLAYER_HTTP__RATE_LIMIT__WS_MAX_PREAUTH_PER_IP", "0")];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    /// One bound speech subscription per session (the article's design).
    PerSession,
    /// One unbound speech subscription, rows routed by session in the client.
    SharedView,
}

#[derive(clap::Args)]
pub struct SessionsArgs {
    /// `inputlayer-server` binary under test.
    #[arg(long)]
    server: PathBuf,
    /// Provenance of the server (commit), recorded in the result.
    #[arg(long, default_value = "")]
    server_label: String,
    /// A second binary measured on the same sizes, e.g. the baseline.
    #[arg(long)]
    compare: Option<PathBuf>,
    #[arg(long, default_value = "")]
    compare_label: String,
    /// Session counts, one fresh server each.
    #[arg(long, value_delimiter = ',', default_value = "1,10,25,50,100,200")]
    sessions: Vec<usize>,
    /// Seconds of probing under receipt load.
    #[arg(long, default_value_t = 15)]
    load_secs: u64,
    /// Receipts per session per second.
    #[arg(long, default_value_t = 1.0)]
    rate: f64,
    /// Probes before the load starts.
    #[arg(long, default_value_t = 40)]
    idle_probes: usize,
    #[arg(long, value_enum, default_value = "per-session")]
    mode: Mode,
    /// Extra server environment, `KEY=VALUE`; repeatable.
    #[arg(long = "server-env")]
    server_env: Vec<String>,
    /// Pin servers to this `taskset -c` CPU list.
    #[arg(long)]
    server_cpus: Option<String>,
    /// Toolchain/build notes recorded in the result.
    #[arg(long, default_value = "")]
    build: String,
    /// Directory for server data and logs.
    #[arg(long)]
    data_root: PathBuf,
    /// Result file (JSON) to write.
    #[arg(long)]
    out: PathBuf,
    /// Summary (Markdown) to write; it is also printed.
    #[arg(long)]
    summary: Option<PathBuf>,
    /// Fail when the loaded write-to-delta p99 of `--server` exceeds this
    /// (milliseconds) at any size up to `--budget-sessions`.
    #[arg(long)]
    max_delta_p99_ms: Option<f64>,
    #[arg(long, default_value_t = usize::MAX)]
    budget_sessions: usize,
}

/// Latency percentiles in milliseconds.
#[derive(Debug, Default, Serialize)]
pub struct Summary {
    pub n: usize,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

impl Summary {
    fn of(samples: &[Duration]) -> Self {
        let mut ms: Vec<f64> = samples.iter().map(|d| d.as_secs_f64() * 1e3).collect();
        ms.sort_by(f64::total_cmp);
        // Ceiling rank, as the review's harness computed it.
        let pick = |p: f64| {
            if ms.is_empty() {
                return f64::NAN;
            }
            let rank = (p / 100.0 * ms.len() as f64).ceil() as usize;
            ms[rank.clamp(1, ms.len()) - 1]
        };
        Self {
            n: ms.len(),
            p50: pick(50.0),
            p95: pick(95.0),
            p99: pick(99.0),
            max: ms.last().copied().unwrap_or(f64::NAN),
        }
    }
}

/// One size on one server.
#[derive(Debug, Serialize)]
pub struct Run {
    pub arm: String,
    pub sessions: usize,
    pub receipts_per_sec: f64,
    pub server_cpu_cores: f64,
    pub idle_commit_ms: Summary,
    pub idle_delta_ms: Summary,
    pub load_commit_ms: Summary,
    pub load_delta_ms: Summary,
    pub receipt_commit_ms: Summary,
    /// Probes whose delta never arrived, even after the load stopped and the
    /// late drain ran out.
    pub missing_deltas: usize,
    /// Probes whose delta arrived after the probe timeout (they are not in
    /// the latency summaries; see `late_delta_ms`).
    pub late_deltas: usize,
    pub late_delta_ms: Summary,
    /// Deltas no probe caused (receipts change no result).
    pub stray_deltas: usize,
    pub peak_rss_kb: Option<u64>,
    /// Why the run could not complete, if it could not.
    pub error: Option<String>,
}

impl Run {
    fn passed(&self) -> bool {
        self.error.is_none() && self.missing_deltas == 0 && self.stray_deltas == 0
    }
}

#[derive(Serialize)]
struct Record {
    schema: &'static str,
    created_unix: u64,
    environment: Environment,
    mode: Mode,
    rate: f64,
    load_secs: u64,
    idle_probes: usize,
    arms: Vec<(String, String)>,
    server_overrides: std::collections::BTreeMap<String, String>,
    server_env: Vec<String>,
    runs: Vec<Run>,
}

pub fn run(args: &SessionsArgs) -> Result<ExitCode> {
    if args.sessions.contains(&0) || args.rate <= 0.0 {
        bail!("--sessions must be positive and --rate above zero");
    }
    std::fs::create_dir_all(&args.data_root)
        .with_context(|| format!("create {}", args.data_root.display()))?;
    let data_root = args.data_root.canonicalize()?;
    let mut env: Vec<(String, String)> = SESSION_OVERRIDES
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    for item in &args.server_env {
        let (key, value) = item
            .split_once('=')
            .with_context(|| format!("--server-env {item}: expected KEY=VALUE"))?;
        env.push((key.to_string(), value.to_string()));
    }
    let mut arms = vec![(
        "candidate".to_string(),
        args.server.clone(),
        args.server_label.clone(),
    )];
    if let Some(compare) = &args.compare {
        arms.insert(
            0,
            (
                "baseline".to_string(),
                compare.clone(),
                args.compare_label.clone(),
            ),
        );
    }
    let options = Options {
        mode: args.mode,
        rate: args.rate,
        load: Duration::from_secs(args.load_secs),
        idle_probes: args.idle_probes,
    };
    let mut environment =
        crate::environment::capture(&data_root, args.server_cpus.clone(), args.build.clone());
    let runtime = tokio::runtime::Runtime::new().context("start runtime")?;
    let mut runs = Vec::new();
    for &sessions in &args.sessions {
        for (arm, binary, _) in &arms {
            let spec = ServerSpec {
                binary: binary
                    .canonicalize()
                    .with_context(|| format!("server binary {}", binary.display()))?,
                cpus: args.server_cpus.clone(),
                env: env.clone(),
            };
            let name = format!("sessions-{arm}-{sessions}");
            let run = runtime.block_on(measure(&spec, &data_root, &name, arm, sessions, &options));
            eprintln!("{}", progress_line(&run));
            runs.push(run);
        }
    }
    environment.loadavg_end = crate::environment::loadavg();
    let record = Record {
        schema: "inputlayer-perf-gate/sessions/v2",
        created_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
        environment,
        mode: args.mode,
        rate: args.rate,
        load_secs: args.load_secs,
        idle_probes: args.idle_probes,
        arms: arms
            .iter()
            .map(|(arm, binary, label)| (arm.clone(), format!("{label} ({})", binary.display())))
            .collect(),
        server_overrides: recorded_overrides(),
        server_env: env.iter().map(|(k, v)| format!("{k}={v}")).collect(),
        runs,
    };
    std::fs::write(&args.out, serde_json::to_string(&record)?)
        .with_context(|| format!("write {}", args.out.display()))?;
    let markdown = markdown(&record);
    print!("{markdown}");
    if let Some(path) = &args.summary {
        std::fs::write(path, &markdown).with_context(|| format!("write {}", path.display()))?;
    }
    eprintln!("result file: {}", args.out.display());

    Ok(if verdict(&record, args) {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

/// Whether every run completed correctly and within the budget, if any.
fn verdict(record: &Record, args: &SessionsArgs) -> bool {
    let mut passed = true;
    for run in &record.runs {
        if !run.passed() {
            eprintln!(
                "FAIL {} at {} sessions: {}",
                run.arm,
                run.sessions,
                failure(run)
            );
            passed = false;
        }
    }
    if let Some(budget) = args.max_delta_p99_ms {
        for run in record
            .runs
            .iter()
            .filter(|r| r.arm == "candidate" && r.sessions <= args.budget_sessions)
        {
            if run.load_delta_ms.p99.is_nan() || run.load_delta_ms.p99 > budget {
                eprintln!(
                    "FAIL over budget at {} sessions: write-to-delta p99 {:.1} ms > {budget} ms",
                    run.sessions, run.load_delta_ms.p99
                );
                passed = false;
            }
        }
    }
    passed
}

#[derive(Clone, Copy)]
struct Options {
    mode: Mode,
    rate: f64,
    load: Duration,
    idle_probes: usize,
}

/// One size on a fresh server; an error is recorded in the run.
async fn measure(
    spec: &ServerSpec,
    root: &Path,
    name: &str,
    arm: &str,
    sessions: usize,
    options: &Options,
) -> Run {
    let mut run = Run {
        arm: arm.to_string(),
        sessions,
        receipts_per_sec: 0.0,
        server_cpu_cores: f64::NAN,
        idle_commit_ms: Summary::default(),
        idle_delta_ms: Summary::default(),
        load_commit_ms: Summary::default(),
        load_delta_ms: Summary::default(),
        receipt_commit_ms: Summary::default(),
        missing_deltas: 0,
        late_deltas: 0,
        late_delta_ms: Summary::of(&[]),
        stray_deltas: 0,
        peak_rss_kb: None,
        error: None,
    };
    let server = match RunningServer::start(spec, root, name).await {
        Ok(server) => server,
        Err(e) => {
            run.error = Some(format!("{e:#}"));
            return run;
        }
    };
    if let Err(e) = Bench::run(&server, sessions, options, &mut run).await {
        run.error = Some(format!("{e:#}"));
    }
    run.peak_rss_kb = server.peak_rss_kb();
    run
}

/// A delta for one session, as a session connection received it.
struct Push {
    session: usize,
    at: Instant,
    inserted: Vec<Vec<Value>>,
}

struct Bench {
    adapter: Client,
    pushes: mpsc::UnboundedReceiver<Push>,
    /// Each session's current ETA, toggled by probes.
    etas: HashMap<usize, &'static str>,
    revision: u64,
    rng: u64,
    sessions: usize,
    /// Probes that timed out, still waiting for their delta.
    overdue: Vec<Overdue>,
    /// Write-to-delta latency of the probes whose delta came late.
    late: Vec<Duration>,
}

/// A timed-out probe: its session, the ETA its delta must carry, its start.
struct Overdue {
    session: usize,
    eta: &'static str,
    start: Instant,
}

/// Whether `push` is `session`'s delta carrying `eta` as a `state` row.
fn carries(push: &Push, session: usize, eta: &str) -> bool {
    push.session == session
        && push.inserted.iter().any(|row| {
            row.get(4).and_then(Value::as_str) == Some(eta)
                && row.get(5).and_then(Value::as_str) == Some("state")
        })
}

struct Probe {
    commit: Duration,
    delta: Option<Duration>,
}

/// When each receipt was sent, and how long its commit took.
type Receipts = Vec<(Instant, Duration)>;

/// The session tasks and their controls.
struct Rig {
    tasks: Vec<tokio::task::JoinHandle<Result<Receipts>>>,
    stop: watch::Sender<bool>,
    /// Session tasks commit receipts while set.
    load: Arc<std::sync::atomic::AtomicBool>,
}

/// Create the knowledge graph with the pack and `sessions` sessions' facts;
/// returns an API key for the session connections.
async fn load_world(server: &RunningServer, sessions: usize) -> Result<String> {
    let mut admin = server.client("default").await?;
    admin.execute(&format!(".kg create {KG}")).await?;
    let mut admin = server.client(KG).await?;
    admin.execute(PACK).await.context("load the pack")?;
    admin
        .execute(&format!(
            "+source_health(\"carrier\", \"ok\", {FAR})\n+source_health(\"orders\", \"ok\", {FAR})"
        ))
        .await?;
    let all: Vec<usize> = (1..=sessions).collect();
    for chunk in all.chunks(100) {
        let program: Vec<String> = chunk.iter().map(|&i| seed(i)).collect();
        admin.execute(&program.join("\n")).await.context("seed")?;
    }
    let (_, key) = admin.query(".apikey create sessions-bench").await?;
    key.rows
        .first()
        .and_then(|row| row.get(1))
        .and_then(Value::as_str)
        .map(str::to_string)
        .context("API key in `.apikey create` reply")
}

impl Bench {
    async fn run(
        server: &RunningServer,
        sessions: usize,
        options: &Options,
        out: &mut Run,
    ) -> Result<()> {
        let key = load_world(server, sessions).await?;
        let (mut bench, rig) = Self::connect(server, &key, sessions, options).await?;
        let mut idle = Vec::new();
        for _ in 0..options.idle_probes {
            idle.push(bench.probe(&mut out.stray_deltas).await?);
            tokio::time::sleep(IDLE_PROBE_GAP).await;
        }

        rig.load.store(true, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(WARM_UP).await;
        let cpu_start = server.cpu_seconds();
        let started = Instant::now();
        let mut loaded = Vec::new();
        while started.elapsed() < options.load {
            loaded.push(bench.probe(&mut out.stray_deltas).await?);
            tokio::time::sleep(PROBE_INTERVAL).await;
        }
        let window = started.elapsed();
        let cpu_end = server.cpu_seconds();
        // Receipts stop; sessions keep forwarding deltas for the late drain.
        rig.load.store(false, std::sync::atomic::Ordering::SeqCst);
        bench.drain_overdue(&mut out.stray_deltas).await;
        rig.stop.send(true).ok();
        let mut receipts = Vec::new();
        for task in rig.tasks {
            receipts.extend(task.await.context("session task")??);
        }
        let in_window: Vec<Duration> = receipts
            .iter()
            .filter(|(at, _)| *at >= started && at.duration_since(started) <= window)
            .map(|(_, latency)| *latency)
            .collect();

        out.receipts_per_sec = in_window.len() as f64 / window.as_secs_f64();
        out.server_cpu_cores = match (cpu_start, cpu_end) {
            (Some(a), Some(b)) => (b - a) / window.as_secs_f64(),
            _ => f64::NAN,
        };
        let commits =
            |probes: &[Probe]| Summary::of(&probes.iter().map(|p| p.commit).collect::<Vec<_>>());
        let deltas = |probes: &[Probe]| {
            Summary::of(&probes.iter().filter_map(|p| p.delta).collect::<Vec<_>>())
        };
        out.idle_commit_ms = commits(&idle);
        out.idle_delta_ms = deltas(&idle);
        out.load_commit_ms = commits(&loaded);
        out.load_delta_ms = deltas(&loaded);
        out.receipt_commit_ms = Summary::of(&in_window);
        out.missing_deltas = bench.overdue.len();
        out.late_deltas = bench.late.len();
        out.late_delta_ms = Summary::of(&bench.late);
        Ok(())
    }

    /// Open every session's connection and subscription, the executor's and
    /// the adapter's; session tasks start forwarding deltas at once.
    async fn connect(
        server: &RunningServer,
        key: &str,
        sessions: usize,
        options: &Options,
    ) -> Result<(Self, Rig)> {
        let (push_tx, pushes) = mpsc::unbounded_channel();
        let (stop, stopped) = watch::channel(false);
        let load = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut tasks = Vec::with_capacity(sessions);
        let all: Vec<usize> = (1..=sessions).collect();
        for chunk in all.chunks(CONNECT_BATCH) {
            let connected = futures_util::future::try_join_all(chunk.iter().map(|&i| async move {
                let mut client = Client::connect_with_key(server.addr, KG, key).await?;
                if options.mode == Mode::PerSession {
                    let (_, snapshot) = client.query(&speech_subscription(i)).await?;
                    if !snapshot.errors.is_empty() || snapshot.rows.len() != 2 {
                        bail!("session {i}: unexpected snapshot {:?}", snapshot.rows);
                    }
                }
                Ok::<_, anyhow::Error>((i, client))
            }))
            .await?;
            for (i, client) in connected {
                let session = Session {
                    index: i,
                    client,
                    pushes: push_tx.clone(),
                    load: Arc::clone(&load),
                    stop: stopped.clone(),
                    period: Duration::from_secs_f64(1.0 / options.rate),
                    rng: 0x9E37_79B9_7F4A_7C15 ^ (i as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9),
                };
                tasks.push(tokio::spawn(session.run()));
            }
        }
        let mut executor = Client::connect_with_key(server.addr, KG, key).await?;
        let (_, work) = executor
            .query(
                r#".subscribe work ?work(K, S, G, T, Subj, Arg, Ep, Id), lease_owner(S, "exec-1", _)"#,
            )
            .await?;
        if !work.errors.is_empty() {
            bail!("executor subscription: {:?}", work.errors);
        }
        tokio::spawn(drain(executor, stopped.clone()));
        if options.mode == Mode::SharedView {
            let mut shared = Client::connect_with_key(server.addr, KG, key).await?;
            let (_, subscribed) = shared
                .query(".subscribe all ?speech(S, G, T, Subj, V, K, P), speech_owner(S, O, E)")
                .await?;
            if !subscribed.errors.is_empty() || subscribed.rows.len() != 2 * sessions {
                bail!(
                    "shared subscription: {:?}, {} snapshot rows",
                    subscribed.errors,
                    subscribed.rows.len()
                );
            }
            tokio::spawn(route(shared, push_tx.clone(), stopped.clone()));
        }
        let bench = Self {
            adapter: Client::connect_with_key(server.addr, KG, key).await?,
            pushes,
            etas: HashMap::new(),
            revision: 1000,
            rng: 0x2545_F491_4F6C_DD1D,
            sessions,
            overdue: Vec::new(),
            late: Vec::new(),
        };
        Ok((bench, Rig { tasks, stop, load }))
    }

    /// Replace a random session's ETA (the article's section 4.1 revision,
    /// without the unshipped `.require`) and wait for that session's delta.
    async fn probe(&mut self, stray: &mut usize) -> Result<Probe> {
        let session = 1 + (next_random(&mut self.rng) % self.sessions as u64) as usize;
        let eta = self.etas.entry(session).or_insert("2026-10-08");
        *eta = if *eta == "2026-10-08" {
            "2026-10-10"
        } else {
            "2026-10-08"
        };
        let eta = *eta;
        self.revision += 1;
        let program = format!(
            "-eta(\"S-{session}\", D, R, At) <- eta(\"S-{session}\", D, R, At)\n\
             +eta(\"S-{session}\", \"{eta}\", {rev}, {at})",
            rev = self.revision,
            at = 1_791_068_000_000 + self.revision,
        );
        let (start, reply) = self.adapter.execute(&program).await?;
        let deadline = tokio::time::Instant::from_std(start + PROBE_TIMEOUT);
        loop {
            match tokio::time::timeout_at(deadline, self.pushes.recv()).await {
                Ok(Some(push)) => {
                    if carries(&push, session, eta) {
                        return Ok(Probe {
                            commit: reply.at - start,
                            delta: Some(push.at - start),
                        });
                    }
                    self.settle(&push, stray);
                }
                Ok(None) => bail!("every session connection closed"),
                Err(_) => {
                    self.overdue.push(Overdue {
                        session,
                        eta,
                        start,
                    });
                    return Ok(Probe {
                        commit: reply.at - start,
                        delta: None,
                    });
                }
            }
        }
    }
}

impl Bench {
    /// A push that is not the current probe's: an overdue probe's delta
    /// (late), or stray.
    fn settle(&mut self, push: &Push, stray: &mut usize) {
        let owner = self
            .overdue
            .iter()
            .position(|o| carries(push, o.session, o.eta));
        match owner {
            Some(index) => {
                let overdue = self.overdue.swap_remove(index);
                self.late.push(push.at - overdue.start);
            }
            None => *stray += 1,
        }
    }

    /// With the load stopped, wait up to [`LATE_DRAIN`] for every overdue
    /// probe's delta. What is still overdue afterwards is missing.
    async fn drain_overdue(&mut self, stray: &mut usize) {
        let deadline = tokio::time::Instant::now() + LATE_DRAIN;
        while !self.overdue.is_empty() {
            match tokio::time::timeout_at(deadline, self.pushes.recv()).await {
                Ok(Some(push)) => self.settle(&push, stray),
                Ok(None) | Err(_) => return,
            }
        }
    }
}

/// One session's connection: forwards its deltas and commits receipts.
struct Session {
    index: usize,
    client: Client,
    pushes: mpsc::UnboundedSender<Push>,
    load: Arc<std::sync::atomic::AtomicBool>,
    stop: watch::Receiver<bool>,
    period: Duration,
    rng: u64,
}

impl Session {
    /// Runs until stopped; returns each receipt's commit time and latency.
    async fn run(mut self) -> Result<Receipts> {
        let mut receipts = Vec::new();
        let mut playback = 100_u64;
        let mut due = tokio::time::Instant::now() + self.jittered(0.0);
        loop {
            tokio::select! {
                biased;
                _ = self.stop.changed() => return Ok(receipts),
                pushed = tokio::time::timeout_at(due, self.client.next_push()) => {
                    if let Ok(stamped) = pushed {
                        self.forward(stamped?)?;
                        continue;
                    }
                    if self.load.load(std::sync::atomic::Ordering::SeqCst) {
                        playback += 1;
                        let (start, reply) = self.client.execute(&receipt(self.index, playback)).await?;
                        receipts.push((start, reply.at - start));
                    }
                    due = tokio::time::Instant::now() + self.jittered(0.5);
                },
            }
        }
    }

    /// `period`, scaled by a uniform factor in `[low, low + 1)`.
    fn jittered(&mut self, low: f64) -> Duration {
        let unit = (next_random(&mut self.rng) >> 11) as f64 / (1_u64 << 53) as f64;
        self.period.mul_f64(low + unit)
    }

    fn forward(&mut self, stamped: Stamped) -> Result<()> {
        match stamped.frame {
            crate::client::Frame::SubscriptionDelta { inserted, .. } => {
                // The receiver is gone only once the bench stopped probing.
                let _ = self.pushes.send(Push {
                    session: self.index,
                    at: stamped.at,
                    inserted,
                });
                Ok(())
            }
            crate::client::Frame::SubscriptionError { message, .. } => {
                bail!("session {}: subscription error: {message}", self.index)
            }
            _ => Ok(()),
        }
    }
}

/// Read and discard `client`'s pushes until stopped.
async fn drain(mut client: Client, mut stop: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            _ = stop.changed() => return,
            push = client.next_push() => if push.is_err() { return },
        }
    }
}

/// The shared view's pushes, split into one delta per session by row[0].
async fn route(
    mut client: Client,
    pushes: mpsc::UnboundedSender<Push>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        let stamped = tokio::select! {
            _ = stop.changed() => return,
            push = client.next_push() => match push {
                Ok(stamped) => stamped,
                Err(_) => return,
            },
        };
        let crate::client::Frame::SubscriptionDelta { inserted, .. } = stamped.frame else {
            continue;
        };
        let mut by_session: HashMap<usize, Vec<Vec<Value>>> = HashMap::new();
        for row in inserted {
            let session = row
                .first()
                .and_then(Value::as_str)
                .and_then(|s| s.strip_prefix("s-"))
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            by_session.entry(session).or_default().push(row);
        }
        for (session, inserted) in by_session {
            let _ = pushes.send(Push {
                session,
                at: stamped.at,
                inserted,
            });
        }
    }
}

fn seed(i: usize) -> String {
    [
        format!("+customer_order(\"u-{i}\", \"ORD-{i}\", 1)"),
        format!("+shipment(\"ORD-{i}\", \"S-{i}\", 1)"),
        format!("+promised(\"ORD-{i}\", \"2026-10-09\", 1)"),
        format!("+eta(\"S-{i}\", \"2026-10-08\", 1, 1791068000000)"),
        format!("+session(\"s-{i}\", \"u-{i}\", \"open\")"),
        format!("+utterance_cursor(\"s-{i}\", 1)"),
        format!("+speech_owner(\"s-{i}\", \"sp-{i}\", 1)"),
        format!("+speech_until(\"s-{i}\", {FAR})"),
        format!("+goal(\"s-{i}\", \"g-{i}\", \"delivery_status\", \"ORD-{i}\")"),
        format!("+lease_owner(\"s-{i}\", \"exec-1\", 1)"),
        format!("+lease_until(\"s-{i}\", {FAR})"),
    ]
    .join("\n")
}

/// The article's section 7.1 speech subscription for session `i`.
fn speech_subscription(i: usize) -> String {
    format!(
        ".subscribe sp{i} ?speech(\"s-{i}\", G, Topic, Subj, Value, Kind, Prio), \
         speech_owner(\"s-{i}\", \"sp-{i}\", 1)"
    )
}

/// A completed playback for a goal no claim reads.
fn receipt(i: usize, playback: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    format!(
        "+playback(\"s-{i}\", {playback}, \"g-x\", \"eta\", \"ORD-{i}\", \"2026-10-08\", 1, \"completed\")\n\
         +playback_time(\"s-{i}\", {playback}, {now})"
    )
}

/// xorshift64*: a fixed, dependency-free sequence.
fn next_random(state: &mut u64) -> u64 {
    *state ^= *state >> 12;
    *state ^= *state << 25;
    *state ^= *state >> 27;
    state.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

fn failure(run: &Run) -> String {
    match &run.error {
        Some(error) => error.clone(),
        None => format!(
            "{} missing deltas, {} stray deltas",
            run.missing_deltas, run.stray_deltas
        ),
    }
}

fn progress_line(run: &Run) -> String {
    if let Some(error) = &run.error {
        return format!("{} {:>4} sessions: ERROR {error}", run.arm, run.sessions);
    }
    format!(
        "{} {:>4} sessions: cpu {:.2} cores, write->delta p50/p99 {:.1}/{:.1} ms under load, {:.1} receipts/s, {} late (max {:.0} ms), {} missing, {} stray",
        run.arm,
        run.sessions,
        run.server_cpu_cores,
        run.load_delta_ms.p50,
        run.load_delta_ms.p99,
        run.receipts_per_sec,
        run.late_deltas,
        run.late_delta_ms.max,
        run.missing_deltas,
        run.stray_deltas,
    )
}

fn markdown(record: &Record) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# Session scale ({:?})\n", record.mode);
    for (arm, binary) in &record.arms {
        let _ = writeln!(out, "- {arm}: {binary}");
    }
    let _ = writeln!(
        out,
        "- load: {} receipt(s)/s per session for {} s after a {} s warm-up; host {} ({} CPUs), load average {} -> {}\n",
        record.rate,
        record.load_secs,
        WARM_UP.as_secs(),
        record.environment.hostname,
        record.environment.logical_cpus,
        record.environment.loadavg_start,
        record.environment.loadavg_end,
    );
    out.push_str(
        "| Arm | Sessions | Receipts/s | Server CPU (cores) | No load: write->delta p50 / p99 | Under load: write->delta p50 / p95 / p99 | Receipt commit p50 / p99 | Late (max) / missing / stray |\n\
         |---|---|---|---|---|---|---|---|\n",
    );
    for run in &record.runs {
        if let Some(error) = &run.error {
            let _ = writeln!(
                out,
                "| {} | {} | error: {error} | | | | | |",
                run.arm, run.sessions
            );
            continue;
        }
        let _ = writeln!(
            out,
            "| {} | {} | {:.1} | {:.2} | {:.1} / {:.1} ms | {:.1} / {:.1} / {:.1} ms | {:.1} / {:.1} ms | {} ({:.0} ms) / {} / {} |",
            run.arm,
            run.sessions,
            run.receipts_per_sec,
            run.server_cpu_cores,
            run.idle_delta_ms.p50,
            run.idle_delta_ms.p99,
            run.load_delta_ms.p50,
            run.load_delta_ms.p95,
            run.load_delta_ms.p99,
            run.receipt_commit_ms.p50,
            run.receipt_commit_ms.p99,
            run.late_deltas,
            run.late_delta_ms.max,
            run.missing_deltas,
            run.stray_deltas,
        );
    }
    out
}
