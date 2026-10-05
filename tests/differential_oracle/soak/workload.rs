//! The soak's writers and rule churner.
//!
//! Each writer owns a disjoint share of the base facts (`edge` pairs and
//! `blocked` nodes), so it knows exactly what each of its programs must
//! change however the writers interleave, and checks the effective counts
//! the engine reports. Writers commit one program at a time; the verifier's
//! watermark depends on it.

use std::collections::BTreeSet;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::{Duration, Instant};

use inputlayer_testkit::WsClient;
use serde_json::Value;

use super::verify::{Event, Histogram};
use super::{Config, Shared};
use crate::generate::{Rng, RULES};

/// What a program must commit: per fact statement, `(inserted, deleted)`.
struct Program {
    text: String,
    counts: Vec<(usize, usize)>,
}

struct Facts {
    /// Every `edge` pair this writer owns.
    edges: Vec<(i64, i64)>,
    present: BTreeSet<(i64, i64)>,
    /// `blocked` nodes this writer owns.
    nodes: Vec<i64>,
    blocked: BTreeSet<i64>,
    /// Edges to keep, on average.
    target: usize,
}

impl Facts {
    fn new(writer: usize, writers: usize, nodes: i64) -> Self {
        let edges: Vec<(i64, i64)> = (0..nodes)
            .flat_map(|a| (0..nodes).map(move |b| (a, b)))
            .filter(|&(a, b)| owner(a * nodes + b, writers) == writer)
            .collect();
        let owned: Vec<i64> = (0..nodes)
            .filter(|&n| owner(n, writers) == writer)
            .collect();
        // About 1.5 edges per node over all writers: paths are long enough to
        // make recursion work, short enough to keep closures small.
        let target = ((nodes as usize * 3 / 2) / writers).max(1);
        Self {
            edges,
            present: BTreeSet::new(),
            nodes: owned,
            blocked: BTreeSet::new(),
            target,
        }
    }

    fn absent(&self) -> Vec<(i64, i64)> {
        self.edges
            .iter()
            .copied()
            .filter(|e| !self.present.contains(e))
            .collect()
    }

    /// The next program, applied to the model on the assumption it commits.
    fn next(&mut self, rng: &mut Rng) -> Option<Program> {
        let absent = self.absent();
        let present: Vec<(i64, i64)> = self.present.iter().copied().collect();
        let grow = self.present.len() < self.target;
        let roll = rng.below(100);
        let program = if roll < 8 && !self.nodes.is_empty() {
            let node = *rng.pick(&self.nodes);
            if self.blocked.remove(&node) {
                Program {
                    text: format!("-blocked({node})"),
                    counts: vec![(0, 1)],
                }
            } else {
                self.blocked.insert(node);
                Program {
                    text: format!("+blocked({node})"),
                    counts: vec![(1, 0)],
                }
            }
        } else if roll < 18 && !absent.is_empty() && !present.is_empty() {
            // One insert and one delete, committed together.
            let add = *rng.pick(&absent);
            let remove = *rng.pick(&present);
            self.present.insert(add);
            self.present.remove(&remove);
            Program {
                text: format!(
                    "+edge({}, {})\n-edge({}, {})",
                    add.0, add.1, remove.0, remove.1
                ),
                counts: vec![(1, 0), (0, 1)],
            }
        } else if (grow || present.is_empty()) && !absent.is_empty() {
            let n = 1 + rng.below(3);
            let picked = pick_distinct(rng, &absent, n);
            self.present.extend(picked.iter().copied());
            Program {
                text: format!("+edge[{}]", tuples(&picked)),
                counts: vec![(picked.len(), 0)],
            }
        } else if !present.is_empty() {
            let n = 1 + rng.below(2);
            let picked = pick_distinct(rng, &present, n);
            for edge in &picked {
                self.present.remove(edge);
            }
            let text = if picked.len() == 1 {
                format!("-edge({}, {})", picked[0].0, picked[0].1)
            } else {
                format!("-edge[{}]", tuples(&picked))
            };
            Program {
                text,
                counts: vec![(0, picked.len())],
            }
        } else {
            return None;
        };
        Some(program)
    }
}

fn owner(key: i64, writers: usize) -> usize {
    // Spread neighbouring keys over writers.
    (key.unsigned_abs().wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize % writers
}

fn pick_distinct(rng: &mut Rng, from: &[(i64, i64)], n: usize) -> Vec<(i64, i64)> {
    let mut pool = from.to_vec();
    let mut picked = Vec::new();
    while picked.len() < n && !pool.is_empty() {
        picked.push(pool.swap_remove(rng.below(pool.len())));
    }
    picked
}

fn tuples(edges: &[(i64, i64)]) -> String {
    edges
        .iter()
        .map(|(a, b)| format!("({a}, {b})"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The engine's effective `(inserted, deleted)` per fact statement.
fn counts(statements: &[Value]) -> Vec<(usize, usize)> {
    statements
        .iter()
        .map(|s| {
            let n = |field: &str| {
                s[field]
                    .as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .unwrap_or(usize::MAX)
            };
            (n("inserted"), n("deleted"))
        })
        .collect()
}

/// Write latency and volume of one writer.
#[derive(Debug, Default)]
pub struct WriterStats {
    pub commits: u64,
    pub latency: Histogram,
}

/// Commit programs from `rng` until the writers are told to stop.
pub async fn writer(
    actor: usize,
    config: Arc<Config>,
    shared: Arc<Shared>,
    events: Sender<Event>,
) -> WriterStats {
    let mut stats = WriterStats::default();
    let result = write_loop(actor, &config, &shared, &events, &mut stats).await;
    if let Err(message) = result {
        shared.fail(format!("writer {actor}: {message}"));
    }
    let _ = events.send(Event::Done { actor });
    stats
}

async fn write_loop(
    actor: usize,
    config: &Config,
    shared: &Shared,
    events: &Sender<Event>,
    stats: &mut WriterStats,
) -> Result<(), String> {
    let mut client = WsClient::connect_url(&shared.ws_url, &shared.api_key)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let mut rng = Rng::new(
        config
            .seed
            .wrapping_mul(1_000_003)
            .wrapping_add(actor as u64),
    );
    let mut facts = Facts::new(actor, config.writers, config.nodes);
    let mut pace = pacer(config.write_rate);
    while !shared.writes_stopped() {
        if let Some(pace) = pace.as_mut() {
            pace.tick().await;
        }
        let Some(program) = facts.next(&mut rng) else {
            continue;
        };
        let sent = Instant::now();
        let result = client
            .execute(&program.text)
            .await
            .map_err(|e| format!("{:?}: {e}", program.text))?;
        let acked_at = Instant::now();
        stats
            .latency
            .record(u64::try_from((acked_at - sent).as_micros()).unwrap_or(u64::MAX));
        if !result.errors.is_empty() {
            return Err(format!("{:?} failed: {:?}", program.text, result.errors));
        }
        let got = counts(&result.statements);
        if got != program.counts {
            return Err(format!(
                "{:?} committed (inserted, deleted) {got:?}, but this writer owns these facts \
                 and expected {:?}",
                program.text, program.counts
            ));
        }
        let revision = result
            .revision
            .ok_or_else(|| format!("{:?}: the reply names no revision", program.text))?;
        stats.commits += 1;
        let _ = events.send(Event::Commit {
            actor,
            revision,
            program: Some(program.text),
            acked_at,
        });
    }
    Ok(())
}

/// A ticker for `rate` programs per second, or `None` for as fast as acks.
fn pacer(rate: f64) -> Option<tokio::time::Interval> {
    (rate > 0.0).then(|| {
        let mut interval = tokio::time::interval(Duration::from_secs_f64(1.0 / rate));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval
    })
}

/// The rule names the churner replaces, with their interchangeable variants.
fn churned() -> Vec<(&'static str, &'static [&'static [&'static str]])> {
    RULES
        .iter()
        .filter(|(_, variants)| variants.len() > 1)
        .copied()
        .collect()
}

/// The setup program: every rule of the generator's universe, first variant.
pub fn setup_rules() -> Vec<String> {
    RULES
        .iter()
        .map(|(_, variants)| variants[0].join("\n"))
        .collect()
}

/// Replace one rule with another of its variants, atomically, every
/// `config.churn_ms`, until the writers are told to stop.
pub async fn churner(
    actor: usize,
    config: Arc<Config>,
    shared: Arc<Shared>,
    events: Sender<Event>,
) -> u64 {
    let mut replaced = 0;
    if let Err(message) = churn_loop(actor, &config, &shared, &events, &mut replaced).await {
        shared.fail(format!("rule churner: {message}"));
    }
    let _ = events.send(Event::Done { actor });
    replaced
}

async fn churn_loop(
    actor: usize,
    config: &Config,
    shared: &Shared,
    events: &Sender<Event>,
    replaced: &mut u64,
) -> Result<(), String> {
    let mut client = WsClient::connect_url(&shared.ws_url, &shared.api_key)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let mut rng = Rng::new(config.seed ^ 0xC0FF_EE00);
    let rules = churned();
    // The variant in place per churned rule (setup installs the first).
    let mut current = vec![0usize; rules.len()];
    let mut pace = tokio::time::interval(Duration::from_millis(config.churn_ms.max(1)));
    pace.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    pace.tick().await;
    while !shared.writes_stopped() {
        pace.tick().await;
        let which = rng.below(rules.len());
        let (name, variants) = rules[which];
        let next = (current[which] + 1 + rng.below(variants.len() - 1)) % variants.len();
        let program = format!(".rule drop {name}\n{}", variants[next].join("\n"));
        let result = client
            .execute(&program)
            .await
            .map_err(|e| format!("{program:?}: {e}"))?;
        let acked_at = Instant::now();
        if !result.errors.is_empty() {
            return Err(format!("{program:?} failed: {:?}", result.errors));
        }
        let revision = result
            .revision
            .ok_or_else(|| format!("{program:?}: the reply names no revision"))?;
        current[which] = next;
        *replaced += 1;
        let _ = events.send(Event::Commit {
            actor,
            revision,
            program: Some(program),
            acked_at,
        });
    }
    Ok(())
}
