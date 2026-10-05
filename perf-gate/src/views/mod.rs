//! `perf-gate views`: what writes and reads cost against deployed rules, as
//! the graph, the rule catalog and the subscribers grow (issue #308).
//!
//! The workload is the incremental-views review's baseline (its report,
//! Section 4), on one knowledge graph per size, each on a fresh server:
//!
//! - the graph is chains of four nodes (three edges each); 1% of the chain
//!   heads are labelled hot;
//! - three deployed rules: `r1(X,Y) <- edge(X,Y), label(X,"hot")`
//!   (non-recursive), `path` (the right-recursive transitive closure, two rows
//!   per edge) and `hot_reach(X,Y) <- label(X,"hot"), path(X,Y)` (recursive
//!   dependency, three rows per hot head);
//! - idle reads of each view, bound and unbound, and a base point read;
//! - write phases: each write inserts an edge from a hot head to a fresh node,
//!   which changes every view, and is followed by the agent's ad-hoc bound
//!   read `?r1(head,Y)` on the writing connection. A phase times the write's
//!   acknowledgement, its delta at the subscribers that must see it, the
//!   ad-hoc read, and the server CPU per write (that read included):
//!   - P0 no subscribers; P1 one and P2 100 unbound subscribers of `r1`;
//!   - P3 one unbound subscriber of the recursive `hot_reach`;
//!   - P4 keyed subscribers `?r1(head_i,Y)`, one per hot head (each write
//!     changes one of them), 1/10/100/1,000;
//!   - P5 keyed subscribers of the recursive view `?hot_reach(head_i,Y)`;
//!   - P6 one unbound `r1` subscriber after 50 and 200 unrelated rules join
//!     the catalog (with the idle reads of `r1` repeated).
//!
//! Each read and phase also records the engine's view work counters from
//! `/metrics/prometheus` (`rule_evaluations`, `view_reads`,
//! `view_maintenance_us`), where the server exports them. A delta that does
//! not arrive within 20 s of its write is late; a late delta, a subscription
//! error or a failed step fails the run. Latency is reported, not judged.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;

use crate::client::{Client, Frame};
use crate::schema::Environment;
use crate::server::{recorded_overrides, RunningServer, ServerSpec};
use crate::sessions::Summary;

const KG: &str = "views";

/// Graphs this large or larger run fewer writes per subscriber phase, smaller
/// recursive keyed counts and fewer unbound recursive reads: each costs
/// seconds today.
const LARGE_EDGES: usize = 1_000_000;

/// Tuples per bulk insert (the engine's cap is 10,000 per statement).
const LOAD_BATCH: usize = 9_990;

/// Longest wait for a write's delta; a later one is late.
const DELTA_TIMEOUT: Duration = Duration::from_secs(20);

/// Subscriber connections opened at once.
const SUBSCRIBE_BATCH: usize = 25;

/// Pause after closing a phase's subscribers, so the server has dropped them
/// before the next phase starts.
const CLOSE_SETTLE: Duration = Duration::from_millis(1_500);

/// Server settings every run needs: up to 1,000 subscriber connections from
/// one address.
const VIEWS_OVERRIDES: [(&str, &str); 3] = [
    ("INPUTLAYER_HTTP__RATE_LIMIT__WS_MAX_PREAUTH_PER_IP", "0"),
    ("INPUTLAYER_HTTP__RATE_LIMIT__MAX_WS_CONNECTIONS", "5000"),
    ("INPUTLAYER_HTTP__RATE_LIMIT__MAX_CONNECTIONS", "5000"),
];

#[derive(clap::Args)]
pub struct ViewsArgs {
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
    /// Graph sizes in edges, one fresh server each.
    #[arg(long, value_delimiter = ',', default_value = "10000,100000,1000000")]
    edges: Vec<usize>,
    /// Writes per phase.
    #[arg(long, default_value_t = 30)]
    writes: usize,
    /// Writes per subscriber phase on graphs of 1M edges or more.
    #[arg(long, default_value_t = 20)]
    large_writes: usize,
    /// Repetitions of each idle read.
    #[arg(long, default_value_t = 10)]
    reads: usize,
    /// Keyed subscriber counts on the non-recursive view (P4); a count above
    /// the hot heads is skipped.
    #[arg(long, value_delimiter = ',', default_value = "1,10,100,1000")]
    keyed: Vec<usize>,
    /// Keyed subscriber counts on the recursive view (P5).
    #[arg(long, value_delimiter = ',', default_value = "1,100")]
    recursive_keyed: Vec<usize>,
    /// P5 counts on graphs of 1M edges or more.
    #[arg(long, value_delimiter = ',', default_value = "1,10")]
    large_recursive_keyed: Vec<usize>,
    /// Unrelated rules in the catalog for P6, cumulative.
    #[arg(long, value_delimiter = ',', default_value = "50,200")]
    filler_rules: Vec<usize>,
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
}

/// The engine's view work counters over a read or a phase, per read or per
/// write; `None` when the server does not export them.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct Work {
    pub rule_evaluations: Option<f64>,
    pub view_reads: Option<f64>,
    pub view_maintenance_us: Option<f64>,
}

/// The engine's view work counters at one scrape.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Counters {
    rule_evaluations: Option<u64>,
    view_reads: Option<u64>,
    view_maintenance_us: Option<u64>,
}

impl Counters {
    /// Parse a Prometheus text exposition; unlabelled samples only.
    fn parse(text: &str) -> Self {
        let value = |name: &str| {
            text.lines()
                .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
                .and_then(|v| v.trim().parse().ok())
        };
        Self {
            rule_evaluations: value("inputlayer_rule_evaluations_total"),
            view_reads: value("inputlayer_view_reads_total"),
            view_maintenance_us: value("inputlayer_view_maintenance_us_total"),
        }
    }

    /// Work per operation between `before` and `self`, over `operations`.
    fn per(self, before: Self, operations: usize) -> Work {
        let per = |a: Option<u64>, b: Option<u64>| {
            Some(a?.checked_sub(b?)? as f64 / operations.max(1) as f64)
        };
        Work {
            rule_evaluations: per(self.rule_evaluations, before.rule_evaluations),
            view_reads: per(self.view_reads, before.view_reads),
            view_maintenance_us: per(self.view_maintenance_us, before.view_maintenance_us),
        }
    }
}

/// Loading the graph.
#[derive(Debug, Default, Serialize)]
pub struct Load {
    pub edges: usize,
    pub hot_heads: usize,
    pub ms: f64,
    pub server_cpu_ms: f64,
    pub rss_mb: Option<u64>,
}

/// One idle read, repeated.
#[derive(Debug, Serialize)]
pub struct Read {
    pub name: String,
    pub query: String,
    /// Rule clauses in the catalog.
    pub rules: usize,
    pub rows: usize,
    pub truncated: bool,
    pub latency_ms: Summary,
    /// Server CPU per read.
    pub server_cpu_ms: f64,
    /// View work per read.
    pub work: Work,
}

/// One write phase.
#[derive(Debug, Serialize)]
pub struct Phase {
    pub name: String,
    pub subscribers: usize,
    /// Rule clauses in the catalog.
    pub rules: usize,
    pub writes: usize,
    /// Write sent to its reply.
    pub ack_ms: Summary,
    /// Write sent to the last delta it must cause.
    pub write_to_delta_ms: Summary,
    /// The ad-hoc `?r1(head,Y)` right after each write.
    pub read_during_ms: Summary,
    /// Writes whose delta did not arrive within the timeout.
    pub late: usize,
    /// Subscriptions that failed during the phase.
    pub subscription_errors: usize,
    pub server_cpu_ms_per_write: f64,
    /// Frames pushed to the subscribers (deltas and notifications).
    pub frames_per_write: f64,
    pub work_per_write: Work,
    pub rss_mb: Option<u64>,
}

/// Opening subscribers.
#[derive(Debug, Serialize)]
pub struct Subscribe {
    pub query: String,
    pub count: usize,
    pub ms: f64,
}

/// One size on one server.
#[derive(Debug, Default, Serialize)]
pub struct Run {
    pub arm: String,
    pub edges: usize,
    pub load: Load,
    /// Milliseconds to register each deployed rule clause, and the fillers.
    pub registration_ms: BTreeMap<String, f64>,
    pub reads: Vec<Read>,
    pub phases: Vec<Phase>,
    pub subscribes: Vec<Subscribe>,
    pub notes: Vec<String>,
    pub peak_rss_kb: Option<u64>,
    /// Why the run could not complete, if it could not.
    pub error: Option<String>,
}

impl Run {
    fn passed(&self) -> bool {
        self.error.is_none()
            && self
                .phases
                .iter()
                .all(|p| p.late == 0 && p.subscription_errors == 0)
    }
}

#[derive(Serialize)]
struct Record {
    schema: &'static str,
    created_unix: u64,
    environment: Environment,
    options: Options,
    arms: Vec<(String, String)>,
    server_overrides: BTreeMap<String, String>,
    server_env: Vec<String>,
    runs: Vec<Run>,
}

#[derive(Debug, Clone, Serialize)]
struct Options {
    writes: usize,
    large_writes: usize,
    reads: usize,
    keyed: Vec<usize>,
    recursive_keyed: Vec<usize>,
    large_recursive_keyed: Vec<usize>,
    filler_rules: Vec<usize>,
}

pub fn run(args: &ViewsArgs) -> Result<ExitCode> {
    if args.edges.contains(&0) || args.writes == 0 || args.large_writes == 0 || args.reads == 0 {
        bail!("--edges, --writes, --large-writes and --reads must be positive");
    }
    std::fs::create_dir_all(&args.data_root)
        .with_context(|| format!("create {}", args.data_root.display()))?;
    let data_root = args.data_root.canonicalize()?;
    let mut env: Vec<(String, String)> = VIEWS_OVERRIDES
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
        writes: args.writes,
        large_writes: args.large_writes,
        reads: args.reads,
        keyed: args.keyed.clone(),
        recursive_keyed: args.recursive_keyed.clone(),
        large_recursive_keyed: args.large_recursive_keyed.clone(),
        filler_rules: args.filler_rules.clone(),
    };
    let mut environment =
        crate::environment::capture(&data_root, args.server_cpus.clone(), args.build.clone());
    let runtime = tokio::runtime::Runtime::new().context("start runtime")?;
    let mut runs = Vec::new();
    for &edges in &args.edges {
        for (arm, binary, _) in &arms {
            let spec = ServerSpec {
                binary: binary
                    .canonicalize()
                    .with_context(|| format!("server binary {}", binary.display()))?,
                cpus: args.server_cpus.clone(),
                env: env.clone(),
            };
            let name = format!("views-{arm}-{edges}");
            let run = runtime.block_on(measure(&spec, &data_root, &name, arm, edges, &options));
            eprintln!("{}", progress_line(&run));
            runs.push(run);
        }
    }
    environment.loadavg_end = crate::environment::loadavg();
    let record = Record {
        schema: "inputlayer-perf-gate/views/v1",
        created_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
        environment,
        options,
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

    let mut passed = true;
    for run in &record.runs {
        if !run.passed() {
            eprintln!("FAIL {} at {} edges: {}", run.arm, run.edges, failure(run));
            passed = false;
        }
    }
    Ok(if passed {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

/// One size on a fresh server; an error is recorded in the run.
async fn measure(
    spec: &ServerSpec,
    root: &Path,
    name: &str,
    arm: &str,
    edges: usize,
    options: &Options,
) -> Run {
    let mut run = Run {
        arm: arm.to_string(),
        edges,
        ..Run::default()
    };
    let server = match RunningServer::start(spec, root, name).await {
        Ok(server) => server,
        Err(e) => {
            run.error = Some(format!("{e:#}"));
            return run;
        }
    };
    if let Err(e) = Bench::run(&server, edges, options, &mut run).await {
        run.error = Some(format!("{e:#}"));
    }
    run.peak_rss_kb = server.peak_rss_kb();
    run
}

/// The graph's shape for one size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Graph {
    /// Four-node chains, three edges each.
    chains: usize,
    /// Chain heads labelled hot: every hundredth.
    hot: usize,
}

impl Graph {
    fn of(edges: usize) -> Self {
        let chains = edges.div_ceil(3);
        Self {
            chains,
            hot: chains.div_ceil(100),
        }
    }

    /// The `i`-th hot chain head.
    fn hot_head(i: usize) -> u64 {
        4 * 100 * i as u64
    }

    /// First node id no chain uses.
    fn first_fresh(self) -> u64 {
        4 * self.chains as u64 + 1_000
    }

    /// `+edge[...]` and `+label[...]` programs that load the graph.
    fn load_programs(self) -> Vec<String> {
        let mut programs = Vec::new();
        let mut batch = Vec::with_capacity(LOAD_BATCH);
        let mut flush = |relation: &str, batch: &mut Vec<String>| {
            if !batch.is_empty() {
                programs.push(format!("+{relation}[{}]", batch.join(",")));
                batch.clear();
            }
        };
        for chain in 0..self.chains as u64 {
            let b = 4 * chain;
            for (from, to) in [(b, b + 1), (b + 1, b + 2), (b + 2, b + 3)] {
                batch.push(format!("({from},{to})"));
            }
            if batch.len() >= LOAD_BATCH {
                flush("edge", &mut batch);
            }
        }
        flush("edge", &mut batch);
        for i in 0..self.hot {
            batch.push(format!("({},\"hot\")", Self::hot_head(i)));
            if batch.len() >= LOAD_BATCH {
                flush("label", &mut batch);
            }
        }
        flush("label", &mut batch);
        programs
    }
}

/// The deployed rules, by registration name.
const RULES: [(&str, &str); 4] = [
    ("r1", r#"+r1(X,Y) <- edge(X,Y), label(X,"hot")"#),
    ("path", "+path(X,Y) <- edge(X,Y)"),
    ("path_step", "+path(X,Z) <- edge(X,Y), path(Y,Z)"),
    (
        "hot_reach",
        r#"+hot_reach(X,Y) <- label(X,"hot"), path(X,Y)"#,
    ),
];

/// A delta's revision and when it arrived (applied, for a streamed one).
#[derive(Debug, Clone, Copy)]
struct Arrival {
    revision: u64,
    at: Instant,
}

/// From a write sent at `start` to the last of its waiters' delta
/// `arrivals`, which may come before the write's reply: `None` when a delta
/// did not arrive in time, `Some(None)` without waiters.
#[allow(clippy::option_option)]
fn write_to_delta(
    start: Instant,
    arrivals: impl IntoIterator<Item = Option<Instant>>,
) -> Option<Option<Duration>> {
    let arrivals: Vec<Instant> = arrivals.into_iter().collect::<Option<_>>()?;
    Some(arrivals.into_iter().max().map(|last| last - start))
}

/// One subscriber connection: a task reads its frames and publishes the
/// latest delta it applied.
struct Subscriber {
    arrivals: watch::Receiver<Arrival>,
    frames: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl Subscriber {
    /// The arrival of the first delta at `revision` or later, if it comes
    /// before `deadline`.
    async fn reached(&self, revision: u64, deadline: tokio::time::Instant) -> Option<Instant> {
        let mut arrivals = self.arrivals.clone();
        let reached =
            tokio::time::timeout_at(deadline, arrivals.wait_for(|a| a.revision >= revision)).await;
        match reached {
            Ok(Ok(arrival)) => Some(arrival.at),
            _ => None,
        }
    }
}

/// Read every frame of a subscribed connection until it closes.
async fn follow(
    mut client: Client,
    arrivals: watch::Sender<Arrival>,
    frames: Arc<AtomicU64>,
) -> Result<()> {
    let mut streaming = None;
    loop {
        let stamped = client.next_frame().await?;
        frames.fetch_add(1, Ordering::Relaxed);
        let applied = match stamped.frame {
            Frame::SubscriptionDelta { revision, .. } => Some(revision),
            Frame::SubscriptionDeltaStart { revision, .. } => {
                streaming = Some(revision);
                None
            }
            Frame::SubscriptionDeltaEnd {} => streaming.take(),
            Frame::SubscriptionError { message, .. } => bail!("subscription error: {message}"),
            Frame::Error { message } => bail!("server error: {message}"),
            _ => None,
        };
        if let Some(revision) = applied {
            arrivals.send_replace(Arrival {
                revision,
                at: stamped.at,
            });
        }
    }
}

struct Bench<'a> {
    server: &'a RunningServer,
    admin: Client,
    /// API key for the subscriber connections and the metrics scrape.
    key: String,
    graph: Graph,
    next_fresh: u64,
    /// Rule clauses in the catalog.
    rules: usize,
    large: bool,
    options: &'a Options,
}

impl<'a> Bench<'a> {
    async fn run(
        server: &'a RunningServer,
        edges: usize,
        options: &'a Options,
        out: &mut Run,
    ) -> Result<()> {
        let graph = Graph::of(edges);
        let mut admin = server.client("default").await?;
        admin.execute(&format!(".kg create {KG}")).await?;
        let mut admin = server.client(KG).await?;
        let (_, key) = admin.query(".apikey create views-bench").await?;
        let key = key
            .rows
            .first()
            .and_then(|row| row.get(1))
            .and_then(Value::as_str)
            .map(str::to_string)
            .context("API key in `.apikey create` reply")?;
        let mut bench = Bench {
            server,
            admin,
            key,
            graph,
            next_fresh: graph.first_fresh(),
            rules: 0,
            large: edges >= LARGE_EDGES,
            options,
        };
        if bench.scrape().await == Counters::default() {
            out.notes.push(
                "the server exports no view work counters; work columns are empty".to_string(),
            );
        }
        bench.load(out).await.context("load")?;
        for (name, rule) in RULES {
            let (start, reply) = bench.admin.execute(rule).await.context(name)?;
            out.registration_ms
                .insert(name.to_string(), ms(reply.at - start));
            bench.rules += 1;
        }
        bench.idle_reads("", out).await?;
        bench.phases(out).await
    }

    async fn load(&mut self, out: &mut Run) -> Result<()> {
        let cpu = self.server.cpu_seconds();
        let started = Instant::now();
        for program in self.graph.load_programs() {
            self.admin.execute(&program).await?;
        }
        out.load = Load {
            edges: 3 * self.graph.chains,
            hot_heads: self.graph.hot,
            ms: ms(started.elapsed()),
            server_cpu_ms: cpu_ms_since(self.server, cpu),
            rss_mb: self.server.rss_kb().map(|kb| kb / 1024),
        };
        Ok(())
    }

    /// The idle reads; `suffix` names a repeat after the catalog grew.
    async fn idle_reads(&mut self, suffix: &str, out: &mut Run) -> Result<()> {
        let head = Graph::hot_head(1);
        let all: [(&str, String, bool); 6] = [
            ("edge_point", format!("?edge({head},Y)"), false),
            ("r1_unbound", "?r1(X,Y)".to_string(), true),
            ("r1_bound", format!("?r1({head},Y)"), true),
            ("path_bound", format!("?path({head},Y)"), false),
            ("hot_reach_unbound", "?hot_reach(X,Y)".to_string(), false),
            ("hot_reach_bound", format!("?hot_reach({head},Y)"), false),
        ];
        for (name, query, repeated) in all {
            if !suffix.is_empty() && !repeated {
                continue;
            }
            let reps = if self.large && name == "hot_reach_unbound" {
                self.options.reads.min(3)
            } else {
                self.options.reads
            };
            let read = self.read(&format!("{name}{suffix}"), &query, reps).await?;
            out.reads.push(read);
        }
        Ok(())
    }

    async fn read(&mut self, name: &str, query: &str, reps: usize) -> Result<Read> {
        let before = self.scrape().await;
        let cpu = self.server.cpu_seconds();
        let mut latency = Vec::with_capacity(reps);
        let (mut rows, mut truncated) = (0, false);
        for _ in 0..reps {
            let (start, answer) = self.admin.query(query).await?;
            if let Some(error) = answer.errors.first() {
                bail!("{name}: {query}: {error}");
            }
            latency.push(answer.at - start);
            rows = answer.rows.len();
            truncated = answer.truncated;
        }
        let server_cpu_ms = cpu_ms_since(self.server, cpu) / reps as f64;
        Ok(Read {
            name: name.to_string(),
            query: query.to_string(),
            rules: self.rules,
            rows,
            truncated,
            latency_ms: Summary::of(&latency),
            server_cpu_ms,
            work: self.scrape().await.per(before, reps),
        })
    }

    async fn phases(&mut self, out: &mut Run) -> Result<()> {
        let hot = self.graph.hot;
        let every_hot = move |i: usize| Graph::hot_head(i % hot);
        let writes = if self.large {
            self.options.large_writes
        } else {
            self.options.writes
        };

        self.phase(
            "P0_no_subs",
            &[],
            every_hot,
            |_| vec![],
            self.options.writes,
            out,
        )
        .await?;
        for (name, count, query) in [
            ("P1_r1_unbound_x1", 1, "?r1(X,Y)"),
            ("P2_r1_unbound_x100", 100, "?r1(X,Y)"),
            ("P3_hot_reach_unbound_x1", 1, "?hot_reach(X,Y)"),
        ] {
            let subs = self.subscribe(count, |_| query.to_string(), out).await?;
            let everyone: Vec<usize> = (0..count).collect();
            self.phase(name, &subs, every_hot, |_| everyone.clone(), writes, out)
                .await?;
            close(subs, out).await;
        }
        let recursive = if self.large {
            self.options.large_recursive_keyed.clone()
        } else {
            self.options.recursive_keyed.clone()
        };
        let keyed_phases = self
            .options
            .keyed
            .iter()
            .map(|&n| ("r1", n))
            .chain(recursive.into_iter().map(|n| ("hot_reach", n)));
        for (view, count) in keyed_phases {
            if count > hot {
                out.notes.push(format!(
                    "{view} keyed x{count} skipped: the graph has {hot} hot heads"
                ));
                continue;
            }
            let prefix = if view == "r1" { "P4" } else { "P5" };
            let subs = self
                .subscribe(count, |i| format!("?{view}({},Y)", Graph::hot_head(i)), out)
                .await?;
            // Write i changes key i % count only: its one subscriber waits.
            self.phase(
                &format!("{prefix}_{view}_keyed_x{count}"),
                &subs,
                move |i| Graph::hot_head(i % count),
                move |i| vec![i % count],
                writes,
                out,
            )
            .await?;
            close(subs, out).await;
        }
        let mut fillers = 0;
        for &target in &self.options.filler_rules {
            let started = Instant::now();
            while fillers < target {
                self.admin
                    .execute(&format!("+f_{fillers}(X,L) <- label(X,L), X > {fillers}"))
                    .await
                    .context("filler rule")?;
                fillers += 1;
                self.rules += 1;
            }
            out.registration_ms
                .insert(format!("fillers_to_{target}"), ms(started.elapsed()));
            self.idle_reads(&format!("_rules{}", self.rules), out)
                .await?;
            let subs = self.subscribe(1, |_| "?r1(X,Y)".to_string(), out).await?;
            self.phase(
                &format!("P6_r1_unbound_x1_rules{}", self.rules),
                &subs,
                every_hot,
                |_| vec![0],
                writes,
                out,
            )
            .await?;
            close(subs, out).await;
        }
        Ok(())
    }

    /// Open `count` subscriber connections, subscriber `i` to `query(i)`.
    async fn subscribe(
        &self,
        count: usize,
        query: impl Fn(usize) -> String,
        out: &mut Run,
    ) -> Result<Vec<Subscriber>> {
        let started = Instant::now();
        let mut subs = Vec::with_capacity(count);
        let all: Vec<usize> = (0..count).collect();
        for chunk in all.chunks(SUBSCRIBE_BATCH) {
            let opened = futures_util::future::try_join_all(chunk.iter().map(|&i| {
                let query = query(i);
                async move {
                    let mut client =
                        Client::connect_with_key(self.server.addr, KG, &self.key).await?;
                    let (_, answer) = client.query(&format!(".subscribe s {query}")).await?;
                    if !answer.errors.is_empty() {
                        bail!("subscribe {query}: {:?}", answer.errors);
                    }
                    let (sender, arrivals) = watch::channel(Arrival {
                        revision: 0,
                        at: Instant::now(),
                    });
                    let frames = Arc::new(AtomicU64::new(0));
                    let task = tokio::spawn(follow(client, sender, Arc::clone(&frames)));
                    Ok::<_, anyhow::Error>(Subscriber {
                        arrivals,
                        frames,
                        task,
                    })
                }
            }))
            .await?;
            subs.extend(opened);
        }
        out.subscribes.push(Subscribe {
            query: query(0),
            count,
            ms: ms(started.elapsed()),
        });
        Ok(subs)
    }

    /// `writes` closed-loop writes: write `i` adds an edge from `head(i)` to
    /// a fresh node, then reads `?r1(head(i),Y)`, then waits for the deltas
    /// of the subscribers `waiters(i)`.
    async fn phase(
        &mut self,
        name: &str,
        subs: &[Subscriber],
        head: impl Fn(usize) -> u64,
        waiters: impl Fn(usize) -> Vec<usize>,
        writes: usize,
        out: &mut Run,
    ) -> Result<()> {
        let before = self.scrape().await;
        let cpu = self.server.cpu_seconds();
        let frames_before: u64 = subs.iter().map(|s| s.frames.load(Ordering::Relaxed)).sum();
        let (mut ack, mut delta, mut read) = (Vec::new(), Vec::new(), Vec::new());
        let mut late = 0;
        for i in 0..writes {
            let (h, fresh) = (head(i), self.next_fresh);
            self.next_fresh += 1;
            let (start, reply) = self.admin.execute(&format!("+edge({h},{fresh})")).await?;
            ack.push(reply.at - start);
            let revision = reply.revision.context("a write reply without a revision")?;
            let (read_start, read_reply) = self.admin.execute(&format!("?r1({h},Y)")).await?;
            read.push(read_reply.at - read_start);
            let deadline = tokio::time::Instant::from_std(start + DELTA_TIMEOUT);
            let mut arrivals = Vec::new();
            for waiter in waiters(i) {
                arrivals.push(subs[waiter].reached(revision, deadline).await);
            }
            match write_to_delta(start, arrivals) {
                Some(Some(took)) => delta.push(took),
                Some(None) => {}
                None => late += 1,
            }
        }
        let server_cpu_ms_per_write = cpu_ms_since(self.server, cpu) / writes as f64;
        let frames: u64 = subs.iter().map(|s| s.frames.load(Ordering::Relaxed)).sum();
        let subscription_errors = subs.iter().filter(|s| s.task.is_finished()).count();
        let phase = Phase {
            name: name.to_string(),
            subscribers: subs.len(),
            rules: self.rules,
            writes,
            ack_ms: Summary::of(&ack),
            write_to_delta_ms: Summary::of(&delta),
            read_during_ms: Summary::of(&read),
            late,
            subscription_errors,
            server_cpu_ms_per_write,
            frames_per_write: (frames - frames_before) as f64 / writes as f64,
            work_per_write: self.scrape().await.per(before, writes),
            rss_mb: self.server.rss_kb().map(|kb| kb / 1024),
        };
        eprintln!("  {}", phase_line(&phase));
        out.phases.push(phase);
        Ok(())
    }

    /// The view work counters now; empty when the scrape fails or the server
    /// does not export them.
    async fn scrape(&self) -> Counters {
        match http_get(self.server.addr, "/metrics/prometheus", &self.key).await {
            Ok(body) => Counters::parse(&body),
            Err(_) => Counters::default(),
        }
    }
}

/// Close the subscriber connections, recording why any of them failed.
async fn close(subs: Vec<Subscriber>, out: &mut Run) {
    for sub in subs {
        if sub.task.is_finished() {
            if let Ok(Err(e)) = sub.task.await {
                out.notes.push(format!("subscriber failed: {e:#}"));
            }
        } else {
            sub.task.abort();
        }
    }
    tokio::time::sleep(CLOSE_SETTLE).await;
}

/// `GET path` with a bearer key; the body of a 200 reply.
async fn http_get(addr: SocketAddr, path: &str, key: &str) -> Result<String> {
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    let request =
        format!("GET {path} HTTP/1.0\r\nHost: {addr}\r\nAuthorization: Bearer {key}\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut response = String::new();
    tokio::time::timeout(
        Duration::from_secs(30),
        stream.read_to_string(&mut response),
    )
    .await
    .context("metrics scrape timed out")??;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .context("malformed HTTP reply")?;
    let status = head.split_whitespace().nth(1).unwrap_or_default();
    if status != "200" {
        bail!("GET {path}: HTTP {status}");
    }
    Ok(body.to_string())
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e3
}

/// Server CPU milliseconds since `start` (a [`RunningServer::cpu_seconds`]).
fn cpu_ms_since(server: &RunningServer, start: Option<f64>) -> f64 {
    match (start, server.cpu_seconds()) {
        (Some(a), Some(b)) => (b - a) * 1e3,
        _ => f64::NAN,
    }
}

fn failure(run: &Run) -> String {
    if let Some(error) = &run.error {
        return error.clone();
    }
    run.phases
        .iter()
        .filter(|p| p.late > 0 || p.subscription_errors > 0)
        .map(|p| {
            format!(
                "{}: {} late deltas, {} failed subscriptions",
                p.name, p.late, p.subscription_errors
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn progress_line(run: &Run) -> String {
    match &run.error {
        Some(error) => format!("{} {} edges: ERROR {error}", run.arm, run.edges),
        None => format!(
            "{} {} edges: loaded in {:.0} ms, {} reads, {} phases, peak RSS {} MB",
            run.arm,
            run.edges,
            run.load.ms,
            run.reads.len(),
            run.phases.len(),
            run.peak_rss_kb.map_or(0, |kb| kb / 1024),
        ),
    }
}

fn phase_line(phase: &Phase) -> String {
    format!(
        "{}: ack p50 {:.1} ms, write->delta p50/p99 {:.1}/{:.1} ms, read p50 {:.1} ms, cpu {:.1} ms/write, {} late",
        phase.name,
        phase.ack_ms.p50,
        phase.write_to_delta_ms.p50,
        phase.write_to_delta_ms.p99,
        phase.read_during_ms.p50,
        phase.server_cpu_ms_per_write,
        phase.late,
    )
}

/// `value` with one decimal, or `-` when unknown.
fn opt(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |v| format!("{v:.1}"))
}

/// `p50 / p99`, or `-` without samples.
fn p50_p99(summary: &Summary) -> String {
    if summary.n == 0 {
        return "-".to_string();
    }
    format!("{:.1} / {:.1}", summary.p50, summary.p99)
}

fn markdown(record: &Record) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# Views: cost of reads and writes against deployed rules\n"
    );
    for (arm, binary) in &record.arms {
        let _ = writeln!(out, "- {arm}: {binary}");
    }
    let _ = writeln!(
        out,
        "- host {} ({} CPUs, server CPUs {}), load average {} -> {}\n",
        record.environment.hostname,
        record.environment.logical_cpus,
        record.environment.server_cpus.as_deref().unwrap_or("any"),
        record.environment.loadavg_start,
        record.environment.loadavg_end,
    );
    for run in &record.runs {
        let _ = writeln!(out, "## {} edges ({})\n", run.edges, run.arm);
        if let Some(error) = &run.error {
            let _ = writeln!(out, "error: {error}\n");
        }
        let _ = writeln!(
            out,
            "Load: {} edges, {} hot heads in {:.0} ms (server CPU {:.0} ms), RSS {} MB. Peak RSS {} MB.\n",
            run.load.edges,
            run.load.hot_heads,
            run.load.ms,
            run.load.server_cpu_ms,
            run.load.rss_mb.unwrap_or(0),
            run.peak_rss_kb.map_or(0, |kb| kb / 1024),
        );
        let registration: Vec<String> = run
            .registration_ms
            .iter()
            .map(|(name, ms)| format!("{name} {ms:.0} ms"))
            .collect();
        let _ = writeln!(out, "Registration: {}.\n", registration.join(", "));
        out.push_str(
            "| read | rules | rows | p50 / p99 ms | server CPU ms/read | rule evaluations/read | view reads/read |\n\
             |---|---|---|---|---|---|---|\n",
        );
        for read in &run.reads {
            let _ = writeln!(
                out,
                "| `{}` | {} | {}{} | {} | {:.1} | {} | {} |",
                read.query,
                read.rules,
                read.rows,
                if read.truncated { " (truncated)" } else { "" },
                p50_p99(&read.latency_ms),
                read.server_cpu_ms,
                opt(read.work.rule_evaluations),
                opt(read.work.view_reads),
            );
        }
        out.push('\n');
        out.push_str(
            "| phase | subs | ack p50 / p99 ms | write->delta p50 / p99 ms | read during p50 ms | server CPU ms/write | frames/write | rule evaluations/write | maintenance us/write | RSS MB | late |\n\
             |---|---|---|---|---|---|---|---|---|---|---|\n",
        );
        for phase in &run.phases {
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} | {:.1} | {:.1} | {:.1} | {} | {} | {} | {} |",
                phase.name,
                phase.subscribers,
                p50_p99(&phase.ack_ms),
                p50_p99(&phase.write_to_delta_ms),
                phase.read_during_ms.p50,
                phase.server_cpu_ms_per_write,
                phase.frames_per_write,
                opt(phase.work_per_write.rule_evaluations),
                opt(phase.work_per_write.view_maintenance_us),
                phase.rss_mb.unwrap_or(0),
                phase.late,
            );
        }
        out.push('\n');
        let subscribes: Vec<String> = run
            .subscribes
            .iter()
            .map(|s| format!("{} x `{}` {:.0} ms", s.count, s.query, s.ms))
            .collect();
        let _ = writeln!(out, "Subscribe: {}.\n", subscribes.join(", "));
        for note in &run.notes {
            let _ = writeln!(out, "- {note}");
        }
        if !run.notes.is_empty() {
            out.push('\n');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_graph_matches_the_review_harness() {
        let small = Graph::of(10_000);
        assert_eq!(
            small,
            Graph {
                chains: 3_334,
                hot: 34
            }
        );
        assert_eq!(Graph::hot_head(1), 400);
        assert_eq!(small.first_fresh(), 4 * 3_334 + 1_000);
        let programs = small.load_programs();
        let tuples: usize = programs.iter().map(|p| p.matches('(').count()).sum();
        assert_eq!(tuples, 3 * 3_334 + 34);
        assert!(programs[0].starts_with("+edge[(0,1),(1,2),(2,3),(4,5)"));
        assert!(programs
            .last()
            .is_some_and(|p| p.starts_with("+label[(0,\"hot\"),(400,\"hot\")")));
        assert!(programs
            .iter()
            .all(|p| p.matches('(').count() <= LOAD_BATCH + 2));
        assert_eq!(
            Graph::of(1_000_000),
            Graph {
                chains: 333_334,
                hot: 3_334
            }
        );
    }

    #[test]
    fn counters_parse_unlabelled_samples_and_divide_per_operation() {
        let before = Counters::parse(
            "# TYPE inputlayer_rule_evaluations_total counter\n\
             inputlayer_rule_evaluations_total 10\n\
             inputlayer_rule_evaluations_total{kg=\"x\"} 99\n\
             inputlayer_view_reads_total 0\n\
             inputlayer_view_maintenance_us_total 0\n",
        );
        assert_eq!(before.rule_evaluations, Some(10));
        let after = Counters {
            rule_evaluations: Some(70),
            view_reads: Some(0),
            view_maintenance_us: Some(300),
        };
        let work = after.per(before, 30);
        assert_eq!(work.rule_evaluations, Some(2.0));
        assert_eq!(work.view_reads, Some(0.0));
        assert_eq!(work.view_maintenance_us, Some(10.0));
        let none = Counters::parse("inputlayer_queries_total 4\n");
        assert_eq!(none, Counters::default());
        assert_eq!(after.per(none, 30).rule_evaluations, None);
    }

    #[test]
    fn write_to_delta_is_the_last_arrival_and_not_floored_at_the_ack() {
        let start = Instant::now();
        let at = |ms| Some(start + Duration::from_millis(ms));
        assert_eq!(
            write_to_delta(start, [at(3), at(7), at(5)]),
            Some(Some(Duration::from_millis(7)))
        );
        assert_eq!(
            write_to_delta(start, [at(1)]),
            Some(Some(Duration::from_millis(1)))
        );
        assert_eq!(write_to_delta(start, [at(3), None]), None);
        assert_eq!(write_to_delta(start, []), Some(None));
    }

    #[test]
    fn late_deltas_and_subscription_errors_fail_a_run() {
        let phase = |late, subscription_errors| Phase {
            name: "P1".to_string(),
            subscribers: 1,
            rules: 4,
            writes: 1,
            ack_ms: Summary::default(),
            write_to_delta_ms: Summary::default(),
            read_during_ms: Summary::default(),
            late,
            subscription_errors,
            server_cpu_ms_per_write: 0.0,
            frames_per_write: 0.0,
            work_per_write: Work::default(),
            rss_mb: None,
        };
        let run = |phases| Run {
            phases,
            ..Run::default()
        };
        assert!(run(vec![phase(0, 0)]).passed());
        assert!(!run(vec![phase(1, 0)]).passed());
        assert!(!run(vec![phase(0, 1)]).passed());
        let failed = Run {
            error: Some("boom".to_string()),
            ..Run::default()
        };
        assert!(!failed.passed());
    }
}
