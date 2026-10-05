//! The soak's judge: the reference evaluator, replayed in revision order.
//!
//! Writers report every commit with the revision its reply named; consumers
//! report every result they hold with the revision it claims to be exact at.
//! The verifier applies commits to the reference in revision order and, for
//! each committed revision, records a digest of every query's reference
//! result. An observation at revision `R` must equal the digests at the
//! newest commit at or below `R`.
//!
//! Commits are reported when their replies arrive, which can be after a
//! consumer already saw their revision. Each actor commits one program at a
//! time, so its next commit names a higher revision than its last
//! acknowledged one: every commit at or below the lowest of the actors' last
//! acknowledged revisions (the watermark) is known, and observations wait
//! until the watermark reaches them.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::adapter::Adapter;
use crate::model::{AdapterError, Cell, Observation, Revision, Row};
use crate::reference::ReferenceAdapter;

/// An order-independent digest of a result set: its size and the wrapping
/// sum of its rows' hashes, kept current from deltas in O(delta).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Digest {
    rows: u64,
    sum: u64,
}

impl Digest {
    /// Digest of rows given by their canonical keys (compact JSON).
    pub fn of<'a>(keys: impl IntoIterator<Item = &'a String>) -> Self {
        let mut digest = Self::default();
        for key in keys {
            digest.insert(key);
        }
        digest
    }

    pub fn insert(&mut self, key: &str) {
        self.rows += 1;
        self.sum = self.sum.wrapping_add(row_hash(key));
    }

    pub fn retract(&mut self, key: &str) {
        self.rows = self.rows.wrapping_sub(1);
        self.sum = self.sum.wrapping_sub(row_hash(key));
    }

    pub fn rows(self) -> u64 {
        self.rows
    }
}

fn row_hash(key: &str) -> u64 {
    // `DefaultHasher::new` uses fixed keys: equal rows hash equally everywhere.
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// A reference row as the engine sends it: a compact JSON array.
fn row_key(row: &Row) -> String {
    let cells: Vec<Value> = row
        .iter()
        .map(|cell| match cell {
            Cell::Int(n) => Value::from(*n),
            Cell::Str(s) => Value::from(s.clone()),
            Cell::Bool(b) => Value::from(*b),
            Cell::Other(json) => serde_json::from_str(json).unwrap_or(Value::Null),
        })
        .collect();
    Value::Array(cells).to_string()
}

/// Who observed a result, for reports and per-class latency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Class {
    /// Reads every push as it comes.
    Fast,
    /// Reads behind a one-frame inbox and a tiny socket buffer, pausing
    /// after every delta.
    Slow,
    /// Stops reading altogether for a while, then resumes.
    Stalled,
    /// Subscription groups: every member exact at each push's revision.
    Group,
    /// Connections that attach to shared views briefly, then leave.
    Churn,
    /// `read` of every query at one revision.
    Read,
}

impl Class {
    pub const ALL: [Self; 6] = [
        Self::Fast,
        Self::Slow,
        Self::Stalled,
        Self::Group,
        Self::Churn,
        Self::Read,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Fast => "fast",
            Self::Slow => "slow",
            Self::Stalled => "stalled",
            Self::Group => "group",
            Self::Churn => "churn",
            Self::Read => "read",
        }
    }
}

/// What an observation is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A subscription's (or group's) initial result.
    Snapshot,
    /// The result after applying a pushed delta.
    Delta,
    /// A `read` result.
    Read,
    /// The result held once the writers stopped and the pushes settled,
    /// claimed exact at the final revision.
    Final,
}

/// One result a consumer holds, with the revision it claims.
#[derive(Debug, Clone)]
pub struct Seen {
    pub who: Arc<str>,
    pub class: Class,
    /// Index into the soak's query list.
    pub query: usize,
    pub revision: u64,
    pub digest: Digest,
    pub kind: Kind,
    /// When the result arrived.
    pub at: Instant,
}

pub enum Event {
    /// Actor `actor`'s write was acknowledged at `revision`. `program` is
    /// what it committed there, or `None` when it changed nothing.
    Commit {
        actor: usize,
        revision: u64,
        program: Option<String>,
        acked_at: Instant,
    },
    /// Actor `actor` will commit nothing more.
    Done {
        actor: usize,
    },
    Seen(Seen),
}

/// Latencies in microseconds, in buckets about 2% wide.
#[derive(Debug, Clone, Default)]
pub struct Histogram {
    buckets: BTreeMap<u32, u64>,
    count: u64,
    max: u64,
}

impl Histogram {
    pub fn record(&mut self, micros: u64) {
        let bucket = if micros == 0 {
            0
        } else {
            ((micros as f64).ln() / 1.02_f64.ln()) as u32 + 1
        };
        *self.buckets.entry(bucket).or_default() += 1;
        self.count += 1;
        self.max = self.max.max(micros);
    }

    pub fn merge(&mut self, other: &Self) {
        for (bucket, n) in &other.buckets {
            *self.buckets.entry(*bucket).or_default() += n;
        }
        self.count += other.count;
        self.max = self.max.max(other.max);
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn max_ms(&self) -> f64 {
        self.max as f64 / 1000.0
    }

    /// The `q` quantile in milliseconds (upper edge of its bucket).
    pub fn quantile_ms(&self, q: f64) -> f64 {
        let rank = ((self.count as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (bucket, n) in &self.buckets {
            seen += n;
            if seen >= rank {
                let micros = if *bucket == 0 {
                    0.0
                } else {
                    1.02_f64.powi(*bucket as i32)
                };
                return (micros / 1000.0).min(self.max_ms());
            }
        }
        self.max_ms()
    }
}

/// Per consumer class: observations checked and push latency.
#[derive(Debug, Clone, Default)]
pub struct ClassStats {
    pub verified: u64,
    /// Commit acknowledgement to the arrival of a delta at its revision.
    pub lag: Histogram,
}

#[derive(Debug, Default)]
pub struct Verdict {
    pub failures: Vec<String>,
    pub commits: u64,
    pub revisions_judged: u64,
    pub classes: BTreeMap<Class, ClassStats>,
    /// Most observations waiting for the watermark at once.
    pub max_backlog: usize,
    /// Longest the reference took to judge one committed revision.
    pub max_judge_ms: f64,
    pub judge_secs: f64,
}

/// Detailed messages (with the reference rows) for at most this many
/// mismatches; the rest are counted.
const DETAILED: usize = 5;
/// Failures kept verbatim.
const MAX_FAILURES: usize = 50;

pub struct Verifier {
    queries: Arc<Vec<String>>,
    reference: ReferenceAdapter,
    /// Every committed program by revision, for replays in failure reports.
    log: BTreeMap<u64, String>,
    /// Revisions committed but not yet applied to the reference.
    unapplied: BTreeMap<u64, String>,
    /// Reference digests of every query at recent commits.
    expected: BTreeMap<u64, Vec<Digest>>,
    acked_at: BTreeMap<u64, Instant>,
    /// Last acknowledged revision per actor; `None` once it is done.
    acks: Vec<Option<u64>>,
    backlog: VecDeque<Seen>,
    horizon: usize,
    verdict: Verdict,
    mismatches: usize,
}

impl Verifier {
    /// `actors` commit after the setup committed at `setup_revision`.
    pub fn new(
        queries: Arc<Vec<String>>,
        setup: &[(u64, String)],
        actors: usize,
        horizon: usize,
    ) -> Self {
        let mut verifier = Self {
            queries,
            reference: ReferenceAdapter::new(),
            log: BTreeMap::new(),
            unapplied: BTreeMap::new(),
            expected: BTreeMap::new(),
            acked_at: BTreeMap::new(),
            acks: Vec::new(),
            backlog: VecDeque::new(),
            horizon: horizon.max(16),
            verdict: Verdict::default(),
            mismatches: 0,
        };
        let now = Instant::now();
        for (revision, program) in setup {
            verifier.commit(*revision, program.clone(), now);
        }
        let setup_revision = setup.iter().map(|(r, _)| *r).max().unwrap_or(0);
        verifier.acks = vec![Some(setup_revision); actors];
        verifier.advance();
        verifier
    }

    /// Judge events until every sender is gone.
    pub fn run(mut self, events: &Receiver<Event>) -> Verdict {
        let started = Instant::now();
        loop {
            match events.recv_timeout(Duration::from_millis(50)) {
                Ok(event) => {
                    self.take(event);
                    // Drain what is already queued before judging.
                    while let Ok(event) = events.try_recv() {
                        self.take(event);
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            self.advance();
        }
        // Nothing more can commit.
        self.acks.fill(None);
        self.advance();
        if !self.backlog.is_empty() {
            self.fail(format!(
                "{} observation(s) were never judged",
                self.backlog.len()
            ));
        }
        self.verdict.judge_secs = started.elapsed().as_secs_f64();
        self.verdict
    }

    fn take(&mut self, event: Event) {
        match event {
            Event::Commit {
                actor,
                revision,
                program,
                acked_at,
            } => {
                if let Some(Some(last)) = self.acks.get(actor) {
                    if revision < *last {
                        self.fail(format!(
                            "actor {actor}: write acknowledged at revision {revision} after one \
                             at {last}"
                        ));
                    }
                }
                if let Some(ack) = self.acks.get_mut(actor) {
                    *ack = Some(revision.max(ack.unwrap_or(0)));
                }
                if let Some(program) = program {
                    self.commit(revision, program, acked_at);
                }
            }
            Event::Done { actor } => {
                if let Some(ack) = self.acks.get_mut(actor) {
                    *ack = None;
                }
            }
            Event::Seen(seen) => {
                self.backlog.push_back(seen);
                self.verdict.max_backlog = self.verdict.max_backlog.max(self.backlog.len());
            }
        }
    }

    fn commit(&mut self, revision: u64, program: String, acked_at: Instant) {
        if self.log.contains_key(&revision) {
            self.fail(format!(
                "two commits were acknowledged at revision {revision}: {:?} and {program:?}",
                self.log[&revision]
            ));
            return;
        }
        self.verdict.commits += 1;
        self.log.insert(revision, program.clone());
        self.unapplied.insert(revision, program);
        self.acked_at.insert(revision, acked_at);
    }

    /// Every commit at or below this revision is known.
    fn watermark(&self) -> u64 {
        self.acks
            .iter()
            .flatten()
            .copied()
            .min()
            .unwrap_or(u64::MAX)
    }

    fn advance(&mut self) {
        let watermark = self.watermark();
        while let Some((&revision, _)) = self.unapplied.first_key_value() {
            if revision > watermark {
                break;
            }
            let program = self.unapplied.remove(&revision).expect("first key");
            let started = Instant::now();
            if let Err(e) = apply(&mut self.reference, &program) {
                self.fail(format!(
                    "the reference cannot follow the commit at revision {revision} \
                     ({program:?}): {e}"
                ));
                return;
            }
            let digests = match self.digests() {
                Ok(digests) => digests,
                Err(e) => {
                    self.fail(format!(
                        "the reference cannot answer at revision {revision}: {e:?}"
                    ));
                    return;
                }
            };
            self.expected.insert(revision, digests);
            self.verdict.revisions_judged += 1;
            let ms = started.elapsed().as_secs_f64() * 1000.0;
            self.verdict.max_judge_ms = self.verdict.max_judge_ms.max(ms);
            while self.expected.len() > self.horizon {
                self.expected.pop_first();
            }
            while self.acked_at.len() > self.horizon {
                self.acked_at.pop_first();
            }
        }
        let mut waiting = VecDeque::with_capacity(self.backlog.len());
        while let Some(seen) = self.backlog.pop_front() {
            if seen.revision > watermark {
                waiting.push_back(seen);
            } else {
                self.judge(&seen);
            }
        }
        self.backlog = waiting;
    }

    fn digests(&mut self) -> Result<Vec<Digest>, AdapterError> {
        let queries = Arc::clone(&self.queries);
        queries
            .iter()
            .map(|query| {
                let observation = self.reference.observe(query, Revision(0))?;
                Ok(digest_of(&observation))
            })
            .collect()
    }

    fn judge(&mut self, seen: &Seen) {
        // The state at `seen.revision` is the one the newest commit at or
        // below it left.
        let at = self
            .log
            .range(..=seen.revision)
            .next_back()
            .map(|(r, _)| *r);
        let Some(digests) = at.and_then(|at| self.expected.get(&at)) else {
            self.fail(format!(
                "{} ({}): {} at revision {} is older than the verifier keeps \
                 (raise INPUTLAYER_SOAK_HORIZON)",
                seen.who,
                seen.class.name(),
                self.queries[seen.query],
                seen.revision
            ));
            return;
        };
        let at = at.expect("found above");
        let expected = digests[seen.query];
        let stats = self.verdict.classes.entry(seen.class).or_default();
        stats.verified += 1;
        if seen.kind == Kind::Delta {
            if let Some(acked) = self.acked_at.get(&seen.revision) {
                let lag = seen.at.saturating_duration_since(*acked);
                stats
                    .lag
                    .record(u64::try_from(lag.as_micros()).unwrap_or(u64::MAX));
            }
        }
        if expected == seen.digest {
            return;
        }
        self.mismatches += 1;
        let mut message = format!(
            "{} ({}, {:?}): {} at revision {} holds {} row(s) that differ from the \
             reference's {} row(s) at commit {at}",
            seen.who,
            seen.class.name(),
            seen.kind,
            self.queries[seen.query],
            seen.revision,
            seen.digest.rows(),
            expected.rows()
        );
        if self.mismatches <= DETAILED {
            message.push_str(&format!(
                "\nreference rows: {}\ncommits up to it (last 20):\n{}",
                self.replay_rows(seen.revision, &self.queries[seen.query].clone()),
                self.log
                    .range(..=seen.revision)
                    .rev()
                    .take(20)
                    .map(|(r, p)| format!("  @{r}: {}", p.replace('\n', "; ")))
                    .collect::<Vec<_>>()
                    .join("\n")
            ));
        }
        self.fail(message);
    }

    /// The reference's rows for `query` at `revision`, from a fresh replay.
    fn replay_rows(&self, revision: u64, query: &str) -> String {
        let mut reference = ReferenceAdapter::new();
        for program in self.log.range(..=revision).map(|(_, p)| p) {
            if let Err(e) = apply(&mut reference, program) {
                return format!("(replay failed: {e})");
            }
        }
        match reference.observe(query, Revision(0)) {
            Ok(observation) => {
                let rows: Vec<String> = observation.support().take(40).map(row_key).collect();
                format!("{{{}}}", rows.join(", "))
            }
            Err(e) => format!("(replay failed: {e:?})"),
        }
    }

    fn fail(&mut self, message: String) {
        if self.verdict.failures.len() < MAX_FAILURES {
            self.verdict.failures.push(message);
        }
    }
}

/// Apply every statement of `program` (one per line) to the reference.
fn apply(reference: &mut ReferenceAdapter, program: &str) -> Result<(), String> {
    for statement in program.lines().filter(|l| !l.trim().is_empty()) {
        let outcome = reference
            .execute(statement, Revision(0))
            .map_err(|e| format!("{e:?}"))?;
        if !outcome.accepted() {
            return Err(format!("{statement:?} rejected: {outcome:?}"));
        }
    }
    Ok(())
}

fn digest_of(observation: &Observation) -> Digest {
    let mut digest = Digest::default();
    for row in observation.support() {
        digest.insert(&row_key(row));
    }
    digest
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seen(query: usize, revision: u64, rows: &[&str]) -> Event {
        let keys: Vec<String> = rows.iter().map(ToString::to_string).collect();
        Event::Seen(Seen {
            who: "test".into(),
            class: Class::Fast,
            query,
            revision,
            digest: Digest::of(&keys),
            kind: Kind::Delta,
            at: Instant::now(),
        })
    }

    fn commit(actor: usize, revision: u64, program: &str) -> Event {
        Event::Commit {
            actor,
            revision,
            program: Some(program.to_string()),
            acked_at: Instant::now(),
        }
    }

    /// Judge `events` with two writers after a setup at revision 1.
    fn judge(events: Vec<Event>) -> Verdict {
        let queries = Arc::new(vec!["?edge(X, Y)".to_string(), "?reach(X, Y)".to_string()]);
        let setup = [(
            1,
            "+reach(X, Y) <- edge(X, Y)\n+reach(X, Z) <- reach(X, Y), edge(Y, Z)".to_string(),
        )];
        let verifier = Verifier::new(queries, &setup, 2, 100);
        let (tx, rx) = std::sync::mpsc::channel();
        for event in events {
            tx.send(event).unwrap();
        }
        drop(tx);
        verifier.run(&rx)
    }

    #[test]
    fn results_are_judged_at_their_revision_once_every_commit_below_is_known() {
        let verdict = judge(vec![
            // Writer 1's commit at 3 is seen before writer 0's at 2 is acknowledged.
            seen(1, 3, &["[1,2]", "[2,3]", "[1,3]"]),
            commit(1, 3, "+edge(2, 3)"),
            commit(0, 2, "+edge(1, 2)"),
            seen(1, 2, &["[1,2]"]),
            // No commit at 4: the state is the one revision 3 left.
            seen(0, 4, &["[1,2]", "[2,3]"]),
        ]);
        assert!(verdict.failures.is_empty(), "{:?}", verdict.failures);
        assert_eq!(verdict.classes[&Class::Fast].verified, 3);
    }

    #[test]
    fn a_result_that_differs_from_the_reference_fails_with_its_rows() {
        let verdict = judge(vec![
            commit(0, 2, "+edge(1, 2)"),
            commit(1, 3, "+edge(2, 3)\n-edge(1, 2)"),
            // Still holds the retracted edge's consequence.
            seen(1, 3, &["[1,3]", "[2,3]"]),
        ]);
        assert_eq!(verdict.failures.len(), 1, "{:?}", verdict.failures);
        let failure = &verdict.failures[0];
        assert!(failure.contains("?reach(X, Y) at revision 3"), "{failure}");
        assert!(failure.contains("reference rows: {[2,3]}"), "{failure}");
    }

    #[test]
    fn two_commits_at_one_revision_fail() {
        let verdict = judge(vec![
            commit(0, 2, "+edge(1, 2)"),
            commit(1, 2, "+edge(2, 3)"),
        ]);
        assert!(
            verdict.failures.iter().any(|f| f.contains("two commits")),
            "{:?}",
            verdict.failures
        );
    }
}
