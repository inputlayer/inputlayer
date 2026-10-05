//! Sustained concurrency soak, judged by the differential oracle's reference.
//!
//! The rest of the oracle replays one history at a time, in process. The
//! soak runs a real `inputlayer-server` under concurrent load and holds every
//! answer the engine gives to the same reference evaluator:
//!
//! - writer connections commit concurrently. Each owns a disjoint share of
//!   the base facts, so it knows exactly what each of its programs must
//!   change, and checks the effective counts the engine reports;
//! - a rule churner atomically replaces rules (recursion shape, negation,
//!   aggregates) while the writers run;
//! - many subscriber connections maintain standing queries from pushed
//!   deltas: fast consumers, slow consumers reading behind a one-frame inbox
//!   and a tiny socket buffer, consumers that stall outright, subscription
//!   groups, and short-lived connections attaching to and leaving shared
//!   views;
//! - auditors `read` every query at one revision, over and over.
//!
//! Every write reply names the revision it committed at, and every snapshot,
//! read and delta names the revision it is exact at. A verifier thread
//! replays the commits in revision order through the reference and checks
//! each observation against the reference at its revision ([`verify`]), so
//! concurrency cannot hide a wrong answer behind timing. When the writers
//! stop, every consumer must settle on the reference's final state. Slow and
//! stalled consumers may be disconnected only the ways the protocol
//! documents; fast ones never. The server's memory is sampled throughout.
//!
//! The default is a short smoke that runs with the test suite. Scale it with
//! `INPUTLAYER_SOAK_*` variables ([`Config::from_env`]); `scripts/soak.sh`
//! runs it in release with a report, and `make soak-remote` runs the
//! sustained soak on the benchmark host.

mod consumers;
mod verify;
mod workload;

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use inputlayer_testkit::{EngineBuilder, WsClient};
use serde_json::json;

use consumers::{ConsumerStats, Ctx};
use verify::{Class, Histogram, Verdict, Verifier};
use workload::WriterStats;

const KG: &str = "soak";

/// The soak's shape. Every field has an `INPUTLAYER_SOAK_<FIELD>` variable.
#[derive(Debug, Clone)]
pub struct Config {
    /// Seconds the writers run.
    pub secs: f64,
    pub seed: u64,
    /// Nodes of the graph the writers edit.
    pub nodes: i64,
    pub writers: usize,
    /// Programs per second per writer; 0 commits as fast as replies come.
    pub write_rate: f64,
    /// Milliseconds between rule replacements; 0 disables the churner.
    pub churn_ms: u64,
    pub fast: usize,
    pub slow: usize,
    pub stalled: usize,
    pub groups: usize,
    pub churners: usize,
    pub auditors: usize,
    /// A slow consumer pauses up to twice this after each delta.
    pub slow_pause_ms: u64,
    /// How long a stalled consumer stops reading, and about how long it reads
    /// between stalls.
    pub stall_ms: u64,
    /// Milliseconds between an auditor's reads.
    pub audit_ms: u64,
    /// A consumer has settled once no push arrived for this long after the
    /// writers stopped.
    pub quiet_ms: u64,
    /// Longest the consumers may take to settle.
    pub settle_secs: u64,
    /// The engine's `http.ws_send_timeout_ms`; unset keeps its default.
    pub send_timeout_ms: Option<u64>,
    /// The engine's `notification_buffer_size`; unset keeps its default.
    pub notification_buffer: Option<usize>,
    /// Pin the engine with `taskset -c`.
    pub server_cpus: Option<String>,
    /// Committed revisions the verifier keeps reference digests for.
    pub horizon: usize,
    /// Fail when the server's resident memory at the end exceeds the level
    /// after warm-up by more than this percentage.
    pub max_rss_growth_pct: Option<f64>,
    /// Fail when a fast consumer's p99 commit-to-delta lag exceeds this.
    pub max_fast_p99_ms: Option<f64>,
    /// Write `result.json` and `summary.md` here.
    pub report_dir: Option<PathBuf>,
}

impl Default for Config {
    /// The smoke: a few seconds of every kind of load.
    fn default() -> Self {
        Self {
            secs: 4.0,
            seed: 1,
            nodes: 10,
            writers: 4,
            write_rate: 25.0,
            churn_ms: 400,
            fast: 6,
            slow: 2,
            stalled: 2,
            groups: 2,
            churners: 2,
            auditors: 1,
            slow_pause_ms: 4,
            stall_ms: 1000,
            audit_ms: 50,
            quiet_ms: 500,
            settle_secs: 120,
            send_timeout_ms: None,
            notification_buffer: None,
            server_cpus: None,
            horizon: 200_000,
            max_rss_growth_pct: None,
            max_fast_p99_ms: None,
            report_dir: None,
        }
    }
}

fn var<T: FromStr>(name: &str) -> Option<T> {
    let raw = std::env::var(format!("INPUTLAYER_SOAK_{name}")).ok()?;
    match raw.parse() {
        Ok(value) => Some(value),
        Err(_) => panic!("INPUTLAYER_SOAK_{name}={raw:?} does not parse"),
    }
}

impl Config {
    /// The smoke, with any `INPUTLAYER_SOAK_*` overrides.
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            secs: var("SECS").unwrap_or(d.secs),
            seed: var("SEED").unwrap_or(d.seed),
            nodes: var("NODES").unwrap_or(d.nodes),
            writers: var("WRITERS").unwrap_or(d.writers),
            write_rate: var("WRITE_RATE").unwrap_or(d.write_rate),
            churn_ms: var("CHURN_MS").unwrap_or(d.churn_ms),
            fast: var("FAST").unwrap_or(d.fast),
            slow: var("SLOW").unwrap_or(d.slow),
            stalled: var("STALLED").unwrap_or(d.stalled),
            groups: var("GROUPS").unwrap_or(d.groups),
            churners: var("CHURNERS").unwrap_or(d.churners),
            auditors: var("AUDITORS").unwrap_or(d.auditors),
            slow_pause_ms: var("SLOW_PAUSE_MS").unwrap_or(d.slow_pause_ms),
            stall_ms: var("STALL_MS").unwrap_or(d.stall_ms),
            audit_ms: var("AUDIT_MS").unwrap_or(d.audit_ms),
            quiet_ms: var("QUIET_MS").unwrap_or(d.quiet_ms),
            settle_secs: var("SETTLE_SECS").unwrap_or(d.settle_secs),
            send_timeout_ms: var("SEND_TIMEOUT_MS").or(d.send_timeout_ms),
            notification_buffer: var("NOTIFICATION_BUFFER").or(d.notification_buffer),
            server_cpus: var("SERVER_CPUS").or(d.server_cpus),
            horizon: var("HORIZON").unwrap_or(d.horizon),
            max_rss_growth_pct: var("MAX_RSS_GROWTH_PCT").or(d.max_rss_growth_pct),
            max_fast_p99_ms: var("MAX_FAST_P99_MS").or(d.max_fast_p99_ms),
            report_dir: var("REPORT_DIR").or(d.report_dir),
        }
    }

    fn validate(&self) {
        assert!(
            self.writers >= 1,
            "INPUTLAYER_SOAK_WRITERS must be at least 1"
        );
        assert!(
            self.nodes >= 2 && self.nodes * self.nodes >= self.writers as i64,
            "INPUTLAYER_SOAK_NODES too small for {} writers",
            self.writers
        );
    }
}

/// State every task of the run shares.
pub struct Shared {
    pub ws_url: String,
    pub api_key: String,
    stop_writes: AtomicBool,
    aborted: AtomicBool,
    /// The revision every consumer must settle on; 0 until the writers stop.
    final_revision: AtomicU64,
    failures: Mutex<Vec<String>>,
}

impl Shared {
    /// Record a failure and stop the run.
    pub fn fail(&self, message: String) {
        self.failures.lock().expect("failures lock").push(message);
        self.aborted.store(true, Ordering::SeqCst);
    }

    pub fn aborted(&self) -> bool {
        self.aborted.load(Ordering::SeqCst)
    }

    pub fn writes_stopped(&self) -> bool {
        self.stop_writes.load(Ordering::SeqCst) || self.aborted()
    }

    pub fn final_revision(&self) -> Option<u64> {
        match self.final_revision.load(Ordering::SeqCst) {
            0 => None,
            revision => Some(revision),
        }
    }
}

/// Everything a run measured, and why it failed if it did.
pub struct Report {
    pub config: Config,
    pub failures: Vec<String>,
    pub verdict: Verdict,
    pub elapsed_secs: f64,
    pub write_secs: f64,
    pub writes: WriterStats,
    pub rule_replacements: u64,
    pub final_revision: u64,
    pub consumers: Vec<(Class, ConsumerStats)>,
    /// `(seconds since start, resident kB)` of the server, once a second.
    pub rss: Vec<(f64, u64)>,
    /// Server log lines worth reading: send timeouts, slow-consumer
    /// disconnects, errors and panics.
    pub server_log: Vec<(String, u64)>,
}

/// Run the soak described by `config` against `server`.
pub async fn run(server: &str, config: Config) -> Report {
    config.validate();
    let config = Arc::new(config);
    let started = Instant::now();
    let mut builder = EngineBuilder::new(server)
        // Every consumer is a connection, plus writers and auditors, and all
        // of them connect from 127.0.0.1 at once.
        .max_connections(4096)
        .ws_max_preauth_per_ip(0)
        .ws_max_subscriptions(0);
    if let Some(ms) = config.send_timeout_ms {
        builder = builder.ws_send_timeout_ms(ms);
    }
    if let Some(size) = config.notification_buffer {
        builder = builder.notification_buffer_size(size);
    }
    if let Some(cpus) = &config.server_cpus {
        builder = builder.cpus(cpus.clone());
    }
    let engine = builder.start().await.expect("start the engine");

    // The knowledge graph and every rule, before anything else runs.
    let mut admin = WsClient::connect(&engine, "default")
        .await
        .expect("connect admin");
    admin
        .commit(&format!(".kg create {KG}"))
        .await
        .expect("create the knowledge graph");
    let mut setup_client = WsClient::connect(&engine, KG).await.expect("connect setup");
    let mut setup = Vec::new();
    for program in workload::setup_rules() {
        let result = setup_client
            .execute(&program)
            .await
            .expect("install a rule");
        assert!(result.errors.is_empty(), "{program}: {:?}", result.errors);
        let revision = result.revision.expect("a rule install names its revision");
        setup.push((revision, program));
    }

    let shared = Arc::new(Shared {
        ws_url: engine.ws_url(KG),
        api_key: engine.api_key().to_string(),
        stop_writes: AtomicBool::new(false),
        aborted: AtomicBool::new(false),
        final_revision: AtomicU64::new(0),
        failures: Mutex::new(Vec::new()),
    });
    let mut queries = crate::generate::queries();
    let core = queries.len();
    queries.extend((1..config.nodes).map(|k| format!("?reach({k}, Y)")));
    let queries = Arc::new(queries);

    let churn = config.churn_ms > 0;
    let actors = config.writers + usize::from(churn);
    let (events, inbox) = mpsc::channel();
    let verifier = Verifier::new(Arc::clone(&queries), &setup, actors, config.horizon);
    let judge = std::thread::spawn(move || verifier.run(&inbox));

    let sampling = Arc::new(AtomicBool::new(true));
    let sampler = tokio::spawn(sample_rss(engine.pid(), Arc::clone(&sampling), started));

    let ctx = Ctx {
        config: Arc::clone(&config),
        shared: Arc::clone(&shared),
        queries: Arc::clone(&queries),
        core,
        events: events.clone(),
    };
    let mut consumers = Vec::new();
    let kinds = [
        (Class::Fast, config.fast),
        (Class::Slow, config.slow),
        (Class::Stalled, config.stalled),
        (Class::Churn, config.churners),
    ];
    for (class, count) in kinds {
        for index in 0..count {
            let task = tokio::spawn(consumers::subscriber(class, index, ctx.clone()));
            consumers.push((class, task));
        }
    }
    for index in 0..config.groups {
        consumers.push((
            Class::Group,
            tokio::spawn(consumers::group(index, ctx.clone())),
        ));
    }
    for index in 0..config.auditors {
        consumers.push((
            Class::Read,
            tokio::spawn(consumers::auditor(index, ctx.clone())),
        ));
    }
    drop(ctx);

    let write_started = Instant::now();
    let mut writers = Vec::new();
    for actor in 0..config.writers {
        writers.push(tokio::spawn(workload::writer(
            actor,
            Arc::clone(&config),
            Arc::clone(&shared),
            events.clone(),
        )));
    }
    let churner = churn.then(|| {
        tokio::spawn(workload::churner(
            config.writers,
            Arc::clone(&config),
            Arc::clone(&shared),
            events.clone(),
        ))
    });
    drop(events);

    let deadline = write_started + Duration::from_secs_f64(config.secs);
    while Instant::now() < deadline && !shared.aborted() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    shared.stop_writes.store(true, Ordering::SeqCst);
    let mut writes = WriterStats::default();
    for writer in writers {
        let stats = writer.await.expect("writer task");
        writes.commits += stats.commits;
        writes.latency.merge(&stats.latency);
    }
    let rule_replacements = match churner {
        Some(task) => task.await.expect("churner task"),
        None => 0,
    };
    let write_secs = write_started.elapsed().as_secs_f64();

    // Everything committed; every consumer must now settle on this revision.
    let final_revision = admin
        .read(&[("final", "?edge(X, Y)")])
        .await
        .map(|snapshot| snapshot.revision)
        .unwrap_or_else(|e| {
            shared.fail(format!("final read: {e}"));
            0
        });
    shared
        .final_revision
        .store(final_revision.max(1), Ordering::SeqCst);
    let settle_by = Instant::now() + Duration::from_secs(config.settle_secs);
    let mut stats = Vec::new();
    for (class, task) in consumers {
        let remaining = settle_by.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, task).await {
            Ok(result) => stats.push((class, result.expect("consumer task"))),
            Err(_) => shared.fail(format!(
                "a {} consumer did not settle within {}s of the writers stopping",
                class.name(),
                config.settle_secs
            )),
        }
    }
    // A consumer that did not settle still holds a sender: stop everything.
    shared.aborted.store(true, Ordering::SeqCst);

    // The engine must still answer after all of it.
    if let Err(e) = admin.ping().await {
        shared.fail(format!("the engine stopped answering: {e}"));
    }
    sampling.store(false, Ordering::SeqCst);
    let rss = sampler.await.expect("sampler task");
    let verdict = tokio::task::spawn_blocking(move || judge.join().expect("verifier thread"))
        .await
        .expect("join verifier");
    let server_log = scan_log(&engine.log_path());
    drop(engine);

    let failures = shared.failures.lock().expect("failures lock").clone();
    Report {
        config: (*config).clone(),
        failures,
        verdict,
        elapsed_secs: started.elapsed().as_secs_f64(),
        write_secs,
        writes,
        rule_replacements,
        final_revision,
        consumers: stats,
        rss,
        server_log,
    }
}

/// The server's resident memory once a second, until `running` clears.
async fn sample_rss(
    pid: Option<u32>,
    running: Arc<AtomicBool>,
    started: Instant,
) -> Vec<(f64, u64)> {
    let mut samples = Vec::new();
    let Some(pid) = pid else {
        return samples;
    };
    let path = format!("/proc/{pid}/status");
    while running.load(Ordering::SeqCst) {
        if let Some(kb) = std::fs::read_to_string(&path).ok().and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmRSS:"))
                .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse().ok())
        }) {
            samples.push((started.elapsed().as_secs_f64(), kb));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    samples
}

/// Counts of the server log lines a soak cares about.
fn scan_log(path: &std::path::Path) -> Vec<(String, u64)> {
    let log = std::fs::read_to_string(path).unwrap_or_default();
    [
        "ws_send_timeout",
        "ws_slow_subscriber_disconnected",
        "ERROR",
        "panicked",
    ]
    .iter()
    .map(|needle| {
        let n = log.lines().filter(|line| line.contains(needle)).count() as u64;
        ((*needle).to_string(), n)
    })
    .collect()
}

impl Report {
    /// Per class, the consumers' merged counters.
    fn consumer_totals(&self) -> Vec<(Class, usize, ConsumerStats)> {
        Class::ALL
            .iter()
            .filter_map(|class| {
                let all: Vec<&ConsumerStats> = self
                    .consumers
                    .iter()
                    .filter(|(c, _)| c == class)
                    .map(|(_, s)| s)
                    .collect();
                if all.is_empty() {
                    return None;
                }
                let mut total = ConsumerStats::default();
                for stats in &all {
                    total.merge(stats);
                }
                Some((*class, all.len(), total))
            })
            .collect()
    }

    /// Resident memory after warm-up (the mean of the samples between 20%
    /// and 30% of the run) and at the end (the mean of the last 10%), in kB.
    pub fn rss_levels(&self) -> Option<(u64, u64)> {
        let n = self.rss.len();
        if n < 10 {
            return None;
        }
        let mean =
            |slice: &[(f64, u64)]| slice.iter().map(|(_, kb)| kb).sum::<u64>() / slice.len() as u64;
        Some((
            mean(&self.rss[n * 2 / 10..n * 3 / 10]),
            mean(&self.rss[n * 9 / 10..]),
        ))
    }

    /// Every way the run failed, the run's own and the limits' together.
    pub fn all_failures(&self) -> Vec<String> {
        let mut failures = self.failures.clone();
        failures.extend(self.verdict.failures.iter().cloned());
        if self.verdict.commits == 0 {
            failures.push("nothing was committed".into());
        }
        for (class, count, _) in self.consumer_totals() {
            let verified = self.verdict.classes.get(&class).map_or(0, |s| s.verified);
            if count > 0 && verified == 0 {
                failures.push(format!("no {} observation was verified", class.name()));
            }
        }
        if self.config.churn_ms > 0
            && self.rule_replacements == 0
            && self.config.secs * 1000.0 > self.config.churn_ms as f64 * 2.0
        {
            failures.push("the churner replaced no rule".into());
        }
        if let Some((name, n)) = self
            .server_log
            .iter()
            .find(|(name, n)| name == "panicked" && *n > 0)
        {
            failures.push(format!("the server log has {n} line(s) with '{name}'"));
        }
        if let (Some(limit), Some((warm, end))) =
            (self.config.max_rss_growth_pct, self.rss_levels())
        {
            let growth = (end as f64 - warm as f64) * 100.0 / warm.max(1) as f64;
            if growth > limit {
                failures.push(format!(
                    "server memory grew {growth:.1}% after warm-up ({warm} kB to {end} kB), \
                     over the {limit}% limit"
                ));
            }
        }
        if let (Some(limit), Some(fast)) = (
            self.config.max_fast_p99_ms,
            self.verdict.classes.get(&Class::Fast),
        ) {
            let p99 = fast.lag.quantile_ms(0.99);
            if p99 > limit {
                failures.push(format!(
                    "fast consumers' p99 commit-to-delta lag is {p99:.1} ms, over {limit} ms"
                ));
            }
        }
        failures
    }

    pub fn summary(&self) -> String {
        let c = &self.config;
        let mut out = String::new();
        out.push_str("# Concurrency soak\n\n");
        out.push_str(&format!(
            "{} writers at {} programs/s each for {:.0} s, rule churn every {} ms, {} nodes; \
             consumers: {} fast, {} slow, {} stalled, {} group, {} churn, {} read. \
             Seed {}.\n\n",
            c.writers,
            c.write_rate,
            self.write_secs,
            c.churn_ms,
            c.nodes,
            c.fast,
            c.slow,
            c.stalled,
            c.groups,
            c.churners,
            c.auditors,
            c.seed
        ));
        let failures = self.all_failures();
        out.push_str(&format!(
            "**Verdict: {}** ({} failure(s)).\n\n",
            if failures.is_empty() { "PASS" } else { "FAIL" },
            failures.len()
        ));
        out.push_str(&format!(
            "- commits: {} ({:.0}/s), {} rule replacements, final revision {}\n",
            self.verdict.commits,
            self.verdict.commits as f64 / self.write_secs.max(0.001),
            self.rule_replacements,
            self.final_revision
        ));
        out.push_str(&format!(
            "- write latency: p50 {:.1} ms, p99 {:.1} ms, max {:.1} ms\n",
            self.writes.latency.quantile_ms(0.5),
            self.writes.latency.quantile_ms(0.99),
            self.writes.latency.max_ms()
        ));
        out.push_str(&format!(
            "- reference: {} revisions judged, slowest {:.1} ms, most observations waiting {}\n",
            self.verdict.revisions_judged, self.verdict.max_judge_ms, self.verdict.max_backlog
        ));
        if let Some((warm, end)) = self.rss_levels() {
            let peak = self.rss.iter().map(|(_, kb)| *kb).max().unwrap_or(0);
            out.push_str(&format!(
                "- server memory: {} MiB after warm-up, {} MiB at the end, peak {} MiB\n",
                warm / 1024,
                end / 1024,
                peak / 1024
            ));
        }
        let log: Vec<String> = self
            .server_log
            .iter()
            .map(|(k, n)| format!("{k} {n}"))
            .collect();
        out.push_str(&format!("- server log: {}\n\n", log.join(", ")));
        out.push_str("| consumers | n | connections | deltas/reads | verified | lag p50 ms | lag p99 ms | lag max ms | stalls | disconnects | notifications missed |\n");
        out.push_str("|---|---|---|---|---|---|---|---|---|---|---|\n");
        for (class, count, stats) in self.consumer_totals() {
            let verdict = self
                .verdict
                .classes
                .get(&class)
                .cloned()
                .unwrap_or_default();
            let lag = |q: f64| {
                if verdict.lag.count() == 0 {
                    "-".to_string()
                } else {
                    format!("{:.1}", verdict.lag.quantile_ms(q))
                }
            };
            let disconnects: Vec<String> = stats
                .disconnects
                .iter()
                .map(|(reason, n)| format!("{reason} {n}"))
                .collect();
            out.push_str(&format!(
                "| {} | {count} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                class.name(),
                stats.connections,
                stats.deltas,
                verdict.verified,
                lag(0.5),
                lag(0.99),
                if verdict.lag.count() == 0 {
                    "-".into()
                } else {
                    format!("{:.1}", verdict.lag.max_ms())
                },
                stats.stalls,
                if disconnects.is_empty() {
                    "0".into()
                } else {
                    disconnects.join(", ")
                },
                stats.notifications_missed
            ));
        }
        if !failures.is_empty() {
            out.push_str("\n## Failures\n\n");
            for failure in &failures {
                out.push_str(&format!("- {failure}\n"));
            }
        }
        out
    }

    pub fn to_json(&self) -> serde_json::Value {
        let c = &self.config;
        let classes: serde_json::Map<String, serde_json::Value> = self
            .consumer_totals()
            .into_iter()
            .map(|(class, count, stats)| {
                let verdict = self
                    .verdict
                    .classes
                    .get(&class)
                    .cloned()
                    .unwrap_or_default();
                (
                    class.name().to_string(),
                    json!({
                        "consumers": count,
                        "connections": stats.connections,
                        "deltas": stats.deltas,
                        "verified": verdict.verified,
                        "stalls": stats.stalls,
                        "disconnects": stats.disconnects,
                        "notifications_missed": stats.notifications_missed,
                        "lag_ms": histogram_json(&verdict.lag),
                    }),
                )
            })
            .collect();
        json!({
            "config": {
                "secs": c.secs, "seed": c.seed, "nodes": c.nodes, "writers": c.writers,
                "write_rate": c.write_rate, "churn_ms": c.churn_ms, "fast": c.fast,
                "slow": c.slow, "stalled": c.stalled, "groups": c.groups,
                "churners": c.churners, "auditors": c.auditors,
                "slow_pause_ms": c.slow_pause_ms, "stall_ms": c.stall_ms,
                "send_timeout_ms": c.send_timeout_ms,
                "notification_buffer": c.notification_buffer,
                "server_cpus": c.server_cpus,
            },
            "failures": self.all_failures(),
            "elapsed_secs": self.elapsed_secs,
            "write_secs": self.write_secs,
            "commits": self.verdict.commits,
            "rule_replacements": self.rule_replacements,
            "final_revision": self.final_revision,
            "write_latency_ms": histogram_json(&self.writes.latency),
            "reference": {
                "revisions_judged": self.verdict.revisions_judged,
                "max_judge_ms": self.verdict.max_judge_ms,
                "max_backlog": self.verdict.max_backlog,
                "judge_secs": self.verdict.judge_secs,
            },
            "consumers": classes,
            "rss_kb": self.rss,
            "server_log": self.server_log.iter().cloned().collect::<std::collections::BTreeMap<_, _>>(),
        })
    }

    /// Write `result.json` and `summary.md` to the configured directory.
    pub fn write(&self) {
        let Some(dir) = &self.config.report_dir else {
            return;
        };
        std::fs::create_dir_all(dir).expect("create the report directory");
        let json = serde_json::to_string_pretty(&self.to_json()).expect("serialize the report");
        std::fs::write(dir.join("result.json"), json).expect("write result.json");
        std::fs::write(dir.join("summary.md"), self.summary()).expect("write summary.md");
    }
}

fn histogram_json(histogram: &Histogram) -> serde_json::Value {
    json!({
        "count": histogram.count(),
        "p50": histogram.quantile_ms(0.5),
        "p99": histogram.quantile_ms(0.99),
        "p999": histogram.quantile_ms(0.999),
        "max": histogram.max_ms(),
    })
}
