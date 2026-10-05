//! Parameterized views: standing queries that differ only in bound constants
//! share one evaluation per change.
//!
//! `?speech("s-1", G, T), speech_owner("s-1", "sp-1", 1)` and the same query for
//! `"s-2"` are different views, each with its own subscribers, result and
//! publications. Without sharing, every commit they read re-evaluates both,
//! and a bound query costs about as much as the unbound one whenever the
//! engine cannot restrict the rules it reads to the constants. Instead, their
//! constants are *lifted* to parameters, `?speech(_L0, G, T),
//! speech_owner(_L0, _L1, 1)`, and views of the same lifted query form a
//! [`Family`]. The family evaluates the lifted query once per revision (a
//! round), partitions its rows by parameter values, and hands each view the
//! rows of its own constants, projected to its own columns. Each view diffs
//! that against its previous result as before, so subscribers, deltas,
//! revisions, authorization and the shared-view state machine are unchanged.
//!
//! The partition is exact, not a re-implementation of the query: a parameter
//! is matched the way the engine matches the constant it replaces.
//!
//! - String constants are lifted, and equal ones share a parameter. The
//!   engine matches a string constant only against an equal string, so a
//!   join on the shared parameter keeps exactly the rows the separate
//!   constants kept.
//! - An integer constant is lifted only when no other positive atom repeats
//!   its value: the engine matches it numerically (an `Int32`, `Int64`,
//!   timestamp or float within tolerance), while a join would compare values
//!   exactly. A row's partition key reproduces the numeric match.
//! - Other constants, negated atoms and comparisons stay as written: they are
//!   part of the shape.
//!
//! A view evaluates its own query when it starts and after every rule change,
//! so its errors are exactly its own. It also evaluates its own query when the
//! round fails, is truncated, or holds more rows for it than
//! `max_result_rows` allows. A lifted query computes every binding's rows,
//! subscribed or not, and every view waits for it. A family therefore never
//! shares while its views' own evaluations fit on the compute permits at
//! once, and otherwise shares only while a round is, on average, no slower
//! than those evaluations run in parallel on the permits. Costs leave out
//! waiting for a permit and compiling a plan, and a view's own cost counts
//! only evaluations that reused a compiled plan. Costs hold under the rules
//! they were measured with: a rule change stops sharing and starts them
//! over. A family never evaluates its lifted query while a parameter binds
//! an atom that reads, under the current rules, a recursive relation: the
//! constant lets Magic Sets restrict the work, and the lifted query may
//! compute the whole closure. Otherwise, once it has a view's own cost to
//! compare with, a family probes: it evaluates the round of that revision,
//! which no view waits for, under the server's probe permit rather than a
//! compute permit, and starts sharing only when that round is fast enough.
//! Views that then share at the probe's revision read the probe's round, so
//! the lifted query is evaluated at most once per revision. With too few
//! compute permits for that, a family shares without a probe. It stops when
//! rounds are slower on average, and probes (or decides) again after some
//! commits, waiting longer after each failure. While sharing, a view
//! evaluates its own query now and then to keep that cost current.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::OnceCell;
use tracing::debug;

use crate::ast::dependencies::DependencyClosure;
use crate::ast::{Atom, BodyPredicate, Program, Rule, Term};
use crate::protocol::handler::{extract_column_names_from_query, transform_query_shorthand};
use crate::protocol::Handler;
use crate::statement::{parse_query, QueryGoal};
use crate::storage_engine::{KnowledgeGraphSnapshot, PersistentRules};

use super::reevaluate::{run_query, Evaluated};
use super::SubscriptionMetrics;
use super::{Dependencies, ReevaluatingQuery, Refresh, ResultSet, Row, StandingQuery};

/// Name prefix of lifted parameters; queries using it are not lifted.
const PARAM_PREFIX: &str = "_L";

/// Commits before a family that stopped sharing tries again.
const PROBE_AFTER: u64 = 256;

/// Fewest compute permits with which families probe: with fewer, a probe
/// would take CPU views need, so families share without one.
const MIN_PERMITS_FOR_PROBES: usize = 3;

/// A family that keeps failing its probes waits at most this many doublings
/// of [`PROBE_AFTER`] between them.
const MAX_PROBE_BACKOFF: u64 = 6;

/// Rounds between a view's own evaluations while sharing.
const SAMPLE_EVERY: u64 = 64;

/// Whether a family keeps sharing with rounds of `shared_us`: unless they
/// are slower than its `bindings` views' own evaluations of `own_us` each,
/// run `permits` at a time.
fn keeps_sharing(shared_us: u64, own_us: u64, bindings: u64, permits: u64) -> bool {
    let unshared = own_us.saturating_mul(bindings.div_ceil(permits.max(1)).max(1));
    shared_us <= unshared
}

/// Fold `cost` into the running `average` of costs in microseconds (0: none
/// yet), a new cost weighing an eighth, and return the new average.
fn smooth(average: &AtomicU64, cost: Duration) -> u64 {
    let cost = (cost.as_micros() as u64).max(1);
    let previous = average.load(Ordering::Relaxed);
    let next = if previous == 0 {
        cost
    } else {
        (previous.saturating_mul(7) + cost) / 8
    };
    average.store(next, Ordering::Relaxed);
    next
}

/// Whether a parameter of `shape` binds an atom that reads, through
/// `rules`, a recursive relation: its constant lets Magic Sets restrict the
/// work, which lifting it would lose.
fn binds_recursion(shape: &Shape, rules: &[Rule]) -> bool {
    let mut closure = DependencyClosure::default();
    let positive = shape.goal.body.iter().filter_map(|pred| match pred {
        BodyPredicate::Positive(atom) => Some(atom),
        _ => None,
    });
    for atom in shape.goal.goal.iter().chain(positive) {
        let bound = atom
            .args
            .iter()
            .any(|term| matches!(term, Term::Variable(v) if v.starts_with(PARAM_PREFIX)));
        if bound {
            closure.add_relation(&atom.relation);
        }
    }
    closure.close_over(rules);
    let recursive = crate::recursion::recursive_relations(&Program {
        rules: rules.to_vec(),
    });
    let binds = closure
        .relations()
        .any(|relation| recursive.contains(relation));
    binds
}

/// Own evaluations, summed over a family's `bindings` views, before a family
/// that stopped sharing `stops` times in a row probes again: every commit
/// evaluates every view, so [`PROBE_AFTER`] commits, doubled after each stop
/// past the first, up to [`MAX_PROBE_BACKOFF`] times.
fn probe_after(bindings: u64, stops: u64) -> u64 {
    PROBE_AFTER
        .saturating_mul(bindings.max(1))
        .saturating_mul(1 << stops.saturating_sub(1).min(MAX_PROBE_BACKOFF))
}

/// How the engine matches a lifted constant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ParamKind {
    /// Equal strings only.
    Str,
    /// Integers, timestamps and floats within tolerance.
    Int,
}

/// A view's value for one parameter.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ParamValue {
    Str(String),
    Int(i64),
}

impl ParamValue {
    fn kind(&self) -> ParamKind {
        match self {
            ParamValue::Str(_) => ParamKind::Str,
            ParamValue::Int(_) => ParamKind::Int,
        }
    }

    fn variable(index: usize) -> String {
        format!("{PARAM_PREFIX}{index}")
    }
}

/// A view's values for its family's parameters, in order.
pub type Binding = Vec<ParamValue>;

/// What every view of a family shares: the lifted query and how its rows
/// map to each view's rows.
#[derive(Debug)]
pub struct Shape {
    /// The lifted query (`?...`).
    query: String,
    goal: QueryGoal,
    /// The lifted query as a `__query__` program.
    program: String,
    kinds: Vec<ParamKind>,
    /// The lifted result's column of each parameter.
    key_columns: Vec<usize>,
    /// For each column of a view's result, the lifted result's column.
    projection: Vec<usize>,
}

/// A query split into its family's shape and its own constants.
#[derive(Debug)]
pub struct Lifted {
    pub shape: Shape,
    pub binding: Binding,
}

/// Lift the constants of `query` (parsed: `goal`), or `None` when it has none
/// to lift or a form this module does not lift.
pub fn lift(query: &str, goal: &QueryGoal) -> Option<Lifted> {
    let first = goal.goal.as_ref()?;
    if !goal.order_by.is_empty() || goal.limit.is_some() || goal.offset.is_some() {
        return None;
    }
    let positive: Vec<&Atom> = std::iter::once(first)
        .chain(goal.body.iter().filter_map(|pred| match pred {
            BodyPredicate::Positive(atom) => Some(atom),
            _ => None,
        }))
        .collect();
    for pred in &goal.body {
        if matches!(pred, BodyPredicate::HnswNearest { .. }) {
            return None;
        }
    }
    let mut int_uses: HashMap<i64, usize> = HashMap::new();
    for atom in &positive {
        for term in &atom.args {
            match term {
                Term::Variable(v) if v.starts_with(PARAM_PREFIX) => return None,
                Term::Variable(_)
                | Term::Placeholder
                | Term::StringConstant(_)
                | Term::FloatConstant(_)
                | Term::BoolConstant(_) => {}
                Term::Constant(value) => *int_uses.entry(*value).or_default() += 1,
                _ => return None,
            }
        }
    }

    let mut binding: Binding = Vec::new();
    let mut lift_term = |term: &Term| -> Term {
        let value = match term {
            Term::StringConstant(s) => ParamValue::Str(s.clone()),
            Term::Constant(i) if int_uses.get(i) == Some(&1) => ParamValue::Int(*i),
            other => return other.clone(),
        };
        let index = binding.iter().position(|v| *v == value).unwrap_or_else(|| {
            binding.push(value);
            binding.len() - 1
        });
        Term::Variable(ParamValue::variable(index))
    };
    let lift_atom = |atom: &Atom, lift_term: &mut dyn FnMut(&Term) -> Term| Atom {
        relation: atom.relation.clone(),
        args: atom.args.iter().map(&mut *lift_term).collect(),
    };
    let lifted_first = lift_atom(first, &mut lift_term);
    let lifted_body: Vec<BodyPredicate> = goal
        .body
        .iter()
        .map(|pred| match pred {
            BodyPredicate::Positive(atom) => {
                BodyPredicate::Positive(lift_atom(atom, &mut lift_term))
            }
            other => other.clone(),
        })
        .collect();
    if binding.is_empty() {
        return None;
    }

    let lifted_query = std::iter::once(lifted_first.to_string())
        .chain(lifted_body.iter().map(ToString::to_string))
        .collect::<Vec<_>>()
        .join(", ");
    let lifted_query = format!("?{lifted_query}");
    let lifted_goal = parse_query(&lifted_query[1..]).ok()?;
    let own = transform_query_shorthand(query).ok()?;
    let shared = transform_query_shorthand(&lifted_query).ok()?;

    // The first atom's columns line up one to one; after it, the lifted
    // result has the view's variables in the same order, with parameters
    // first seen in the body interleaved.
    let width = first.args.len();
    let params: Vec<String> = (0..binding.len()).map(ParamValue::variable).collect();
    if own.columns.len() < width || shared.columns.len() < width {
        return None;
    }
    let body_columns: Vec<usize> = (width..shared.columns.len())
        .filter(|&c| !params.contains(&shared.columns[c]))
        .collect();
    if body_columns.len() != own.columns.len() - width {
        return None;
    }
    let mut projection: Vec<usize> = (0..width).collect();
    for (j, &c) in body_columns.iter().enumerate() {
        if shared.columns[c] != own.columns[width + j] {
            return None;
        }
        projection.push(c);
    }
    let key_columns = params
        .iter()
        .map(|p| shared.columns.iter().position(|c| c == p))
        .collect::<Option<Vec<usize>>>()?;

    Some(Lifted {
        shape: Shape {
            query: lifted_query,
            goal: lifted_goal,
            program: shared.query,
            kinds: binding.iter().map(ParamValue::kind).collect(),
            key_columns,
            projection,
        },
        binding,
    })
}

/// The parameter value a row holds for a parameter of `kind`, as the engine
/// would match the constant: `None` when no constant of that kind matches
/// it, `Err` when more than one could.
fn row_value(kind: ParamKind, value: &Value) -> Result<Option<ParamValue>, ()> {
    Ok(match kind {
        ParamKind::Str => value.as_str().map(|s| ParamValue::Str(s.to_string())),
        ParamKind::Int => match (value.as_i64(), value.as_f64()) {
            (Some(i), _) => Some(ParamValue::Int(i)),
            (None, Some(f)) if f.is_finite() => {
                // Integers near `f` all convert to the same float past 2^52.
                if f.abs() >= 4_503_599_627_370_496.0 {
                    return Err(());
                }
                let nearest = f.round();
                ((f - nearest).abs() < crate::value::arith::FLOAT_EQ_TOLERANCE)
                    .then_some(ParamValue::Int(nearest as i64))
            }
            _ => None,
        },
    })
}

/// One binding's rows of a round.
struct Part {
    result: Arc<ResultSet>,
    /// Rows before set semantics merged equal JSON: what the view's own
    /// evaluation would count against `max_result_rows`, at most.
    rows: usize,
}

/// A round's outcome: the lifted result by binding.
struct Partitions {
    revision: u64,
    rules: Arc<PersistentRules>,
    dependencies: Dependencies,
    /// Names of the source relation's schema columns, if it has a schema.
    schema_columns: Option<Vec<String>>,
    parts: HashMap<Binding, Part>,
}

impl Partitions {
    fn build(shape: &Shape, rows: Vec<Row>) -> Result<HashMap<Binding, Part>, String> {
        let mut grouped: HashMap<Binding, Vec<Row>> = HashMap::new();
        'rows: for row in rows {
            let mut binding = Vec::with_capacity(shape.kinds.len());
            for (&column, &kind) in shape.key_columns.iter().zip(&shape.kinds) {
                let value = row
                    .get(column)
                    .ok_or("lifted result row too short")
                    .map_err(str::to_string)?;
                match row_value(kind, value) {
                    Ok(Some(value)) => binding.push(value),
                    Ok(None) => continue 'rows,
                    Err(()) => return Err("ambiguous parameter value".to_string()),
                }
            }
            let projected: Row = shape
                .projection
                .iter()
                .map(|&c| row.get(c).cloned().unwrap_or(Value::Null))
                .collect();
            grouped.entry(binding).or_default().push(projected);
        }
        Ok(grouped
            .into_iter()
            .map(|(binding, rows)| {
                let count = rows.len();
                let part = Part {
                    result: Arc::new(rows.into_iter().collect()),
                    rows: count,
                };
                (binding, part)
            })
            .collect())
    }
}

/// One evaluation of a family's lifted query, at one revision.
///
/// A round starts pending and takes its snapshot only when it starts
/// evaluating, after the family's previous round finished. A view that needs
/// a newer revision than the round evaluating joins the pending round, so
/// however many revisions views ask for, a family evaluates one round at a
/// time and at most one more waits.
#[derive(Default)]
struct Round {
    state: Mutex<RoundState>,
    outcome: OnceCell<Result<Arc<Partitions>, String>>,
}

#[derive(Debug, Clone, Copy)]
enum RoundState {
    /// Not evaluating yet: its snapshot will be no older than `needs`, the
    /// newest revision a view that joined it holds.
    Pending { needs: u64 },
    /// Evaluating, or evaluated, the snapshot at `revision`.
    Started { revision: u64 },
}

impl Default for RoundState {
    fn default() -> Self {
        Self::Pending { needs: 0 }
    }
}

impl Round {
    /// Whether a view holding a snapshot at `revision` may read this round:
    /// it is pending (joining it, the view makes it see `revision`) or
    /// evaluates `revision` or later.
    fn serves(&self, revision: u64) -> bool {
        let mut state = self.state.lock();
        match &mut *state {
            RoundState::Pending { needs } => {
                *needs = (*needs).max(revision);
                true
            }
            RoundState::Started {
                revision: evaluated,
            } => *evaluated >= revision,
        }
    }

    /// Start evaluating: the snapshot `current` returns, which is no older
    /// than any view that joined the round holds, since each joined after
    /// taking its own.
    fn start(
        &self,
        current: impl FnOnce() -> Result<Arc<KnowledgeGraphSnapshot>, String>,
    ) -> Result<Arc<KnowledgeGraphSnapshot>, String> {
        let mut state = self.state.lock();
        let snapshot = current()?;
        debug_assert!(
            !matches!(*state, RoundState::Pending { needs } if needs > snapshot.revision),
            "a round's snapshot is older than a view that joined it"
        );
        *state = RoundState::Started {
            revision: snapshot.revision,
        };
        Ok(snapshot)
    }
}

/// The views sharing one lifted query.
pub struct Family {
    handler: Arc<Handler>,
    knowledge_graph: String,
    shape: Shape,
    metrics: Arc<SubscriptionMetrics>,
    /// Views per binding; changes only when views come and go.
    members: Mutex<HashMap<Binding, usize>>,
    bindings: AtomicUsize,
    /// The newest round, evaluating, evaluated or pending.
    latest: ArcSwapOption<Round>,
    /// Held by the round evaluating: one at a time.
    evaluating: tokio::sync::Mutex<()>,
    sharing: AtomicBool,
    /// Whether a probe is evaluating.
    probing: AtomicBool,
    /// Held by a test to keep a probe from evaluating.
    #[cfg(test)]
    probe_gate: tokio::sync::RwLock<()>,
    /// Held by a test to keep a started round from evaluating.
    #[cfg(test)]
    round_gate: tokio::sync::RwLock<()>,
    /// Own evaluations since sharing stopped.
    own_since_stop: AtomicU64,
    /// Times sharing stopped since a round last kept it.
    stops: AtomicU64,
    /// Recent cost of a view's own evaluation, in microseconds (0: unknown).
    own_cost_us: AtomicU64,
    /// Recent cost of a round since sharing last stopped, in microseconds
    /// (0: none yet).
    shared_cost_us: AtomicU64,
    /// Rounds evaluated.
    rounds: AtomicU64,
    /// Whether the next view to refresh evaluates its own query.
    sample_due: AtomicBool,
    /// The rules the costs were measured under, and the shape's verdict
    /// under them.
    judged: ArcSwapOption<Judged>,
}

/// A family's verdict under one set of rules.
struct Judged {
    rules: Arc<PersistentRules>,
    /// The revision of the snapshot judged.
    revision: u64,
    /// Whether a parameter binds an atom that reads a recursive relation.
    binds_recursion: bool,
}

impl Family {
    /// Whether views should read rounds now: more bindings than compute
    /// permits, and sharing pays.
    fn shares(&self) -> bool {
        self.outnumbers_permits() && self.sharing.load(Ordering::Relaxed)
    }

    /// Whether the family has more bindings than compute permits. With no
    /// more, its views' own evaluations all run at once, and a round, which
    /// computes every binding's rows, is no faster.
    fn outnumbers_permits(&self) -> bool {
        self.bindings.load(Ordering::Relaxed) > self.handler.compute_permits().max(1)
    }

    fn join(&self, binding: &Binding) {
        let mut members = self.members.lock();
        *members.entry(binding.clone()).or_default() += 1;
        self.bindings.store(members.len(), Ordering::Relaxed);
    }

    fn leave(&self, binding: &Binding) {
        let mut members = self.members.lock();
        if let Some(count) = members.get_mut(binding) {
            *count -= 1;
            if *count == 0 {
                members.remove(binding);
            }
        }
        self.bindings.store(members.len(), Ordering::Relaxed);
    }

    /// Whether, under `snapshot`'s rules, a parameter binds an atom that
    /// reads a recursive relation. The first call under newer rules starts
    /// the costs over and stops sharing, without counting a failure; a
    /// snapshot older than the judged rules changes nothing.
    fn binds_recursion(&self, snapshot: &KnowledgeGraphSnapshot) -> bool {
        let rules = snapshot.persistent_rules();
        let mut current = self.judged.load_full();
        let mut binds = None;
        let verdict = loop {
            if let Some(judged) = &current {
                if Arc::ptr_eq(&judged.rules, rules) {
                    return judged.binds_recursion;
                }
                if judged.revision > snapshot.revision {
                    return binds_recursion(&self.shape, &snapshot.rules);
                }
            }
            let verdict =
                *binds.get_or_insert_with(|| binds_recursion(&self.shape, &snapshot.rules));
            let next = Arc::new(Judged {
                rules: Arc::clone(rules),
                revision: snapshot.revision,
                binds_recursion: verdict,
            });
            let previous = self.judged.compare_and_swap(&current, Some(next));
            let won = match (&*previous, &current) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            };
            if won {
                break verdict;
            }
            current = arc_swap::Guard::into_inner(previous);
        };
        self.sharing.store(false, Ordering::Relaxed);
        self.own_cost_us.store(0, Ordering::Relaxed);
        self.shared_cost_us.store(0, Ordering::Relaxed);
        self.own_since_stop.store(0, Ordering::Relaxed);
        self.stops.store(0, Ordering::Relaxed);
        verdict
    }

    /// The round to read for a view holding a snapshot at `revision`: the
    /// latest round when it [serves](Round::serves) it, else a new pending
    /// round, which starts once the latest one has finished.
    fn round_for(&self, revision: u64) -> Arc<Round> {
        let mut current = self.latest.load_full();
        loop {
            if let Some(round) = current.as_ref().filter(|r| r.serves(revision)) {
                return Arc::clone(round);
            }
            let fresh = Arc::new(Round::default());
            fresh.serves(revision);
            let previous = self
                .latest
                .compare_and_swap(&current, Some(Arc::clone(&fresh)));
            let won = match (&*previous, &current) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            };
            if won {
                return fresh;
            }
            current = arc_swap::Guard::into_inner(previous);
        }
    }

    /// The outcome of `round`, evaluating it, as a `probe` or for views
    /// waiting on it, unless another caller did or is doing so. A round the
    /// family does not share once judged, too slow or failed, is let go: no
    /// view reads it, so its rows must not stay around until the next probe.
    async fn outcome(&self, round: &Arc<Round>, probe: bool) -> Result<Arc<Partitions>, String> {
        let outcome = round
            .outcome
            .get_or_init(|| async {
                let _turn = self.evaluating.lock().await;
                // An evaluation cancelled after it started is started again
                // by another reader, on a newer snapshot: it serves the
                // round's readers as well.
                let snapshot = round.start(|| {
                    self.handler
                        .get_storage()
                        .get_snapshot_for(&self.knowledge_graph)
                        .map_err(|e| format!("Knowledge graph '{}': {e}", self.knowledge_graph))
                })?;
                #[cfg(test)]
                let _gate = self.round_gate.read().await;
                self.evaluate(snapshot, probe).await
            })
            .await
            .clone();
        if !self.sharing.load(Ordering::Relaxed) {
            let _ = self.latest.compare_and_swap(&Some(Arc::clone(round)), None);
        }
        outcome
    }

    /// Evaluate a round at `snapshot`, as a `probe` or for views waiting on
    /// it, and judge whether views share: only while the family's costs are
    /// still those of `snapshot`'s rules and a view's own cost is known.
    async fn evaluate(
        &self,
        snapshot: Arc<KnowledgeGraphSnapshot>,
        probe: bool,
    ) -> Result<Arc<Partitions>, String> {
        if self.binds_recursion(&snapshot) {
            self.sharing.store(false, Ordering::Relaxed);
            return Err("a parameter binds a recursive relation".to_string());
        }
        let judged = self.evaluate_and_judge(snapshot, probe).await;
        // Counted once judged: whoever sees the count sees the verdict.
        self.metrics.record_shared_evaluation();
        judged
    }

    async fn evaluate_and_judge(
        &self,
        snapshot: Arc<KnowledgeGraphSnapshot>,
        probe: bool,
    ) -> Result<Arc<Partitions>, String> {
        let rules = Arc::clone(snapshot.persistent_rules());
        let dependencies = Dependencies::for_query(&self.shape.goal, &snapshot.rules);
        let revision = snapshot.revision;
        let ran = run_query(
            &self.handler,
            &self.knowledge_graph,
            &self.shape.query,
            snapshot,
            probe,
        )
        .await;
        let ran = match ran {
            Ok(ran) => ran,
            Err(e) => {
                self.stop_sharing(&rules);
                return Err(e);
            }
        };
        let partitioning = Instant::now();
        let parts = Partitions::build(&self.shape, ran.rows).inspect_err(|e| {
            debug!(query = %self.shape.query, error = %e, "subscription_family_stops_sharing");
            self.stop_sharing(&rules);
        })?;
        let own = self.own_cost_us.load(Ordering::Relaxed);
        let cost = ran.cost + partitioning.elapsed();
        let shared = if own > 0 {
            self.smooth(&self.shared_cost_us, cost, &rules)
        } else {
            None
        };
        if let Some(shared) = shared {
            let bindings = self.bindings.load(Ordering::Relaxed) as u64;
            let permits = self.handler.compute_permits() as u64;
            let keeps = self.handler.shares_regardless_of_cost()
                || keeps_sharing(shared, own, bindings, permits);
            if !keeps {
                debug!(
                    query = %self.shape.query,
                    shared_us = shared,
                    own_us = own,
                    bindings,
                    permits,
                    "subscription_family_stops_sharing"
                );
                self.stop_sharing(&rules);
            } else if self.start_sharing(&rules) {
                if self.stops.load(Ordering::Relaxed) != 0 {
                    self.stops.store(0, Ordering::Relaxed);
                }
                if (self.rounds.fetch_add(1, Ordering::Relaxed) + 1).is_multiple_of(SAMPLE_EVERY) {
                    self.sample_due.store(true, Ordering::Relaxed);
                }
            }
        }
        Ok(Arc::new(Partitions {
            revision,
            rules,
            dependencies,
            schema_columns: self
                .handler
                .source_schema_columns(&self.knowledge_graph, &self.shape.program),
            parts,
        }))
    }

    /// Whether the caller should evaluate its own query as a cost sample.
    fn takes_sample(&self) -> bool {
        self.sample_due.load(Ordering::Relaxed) && self.sample_due.swap(false, Ordering::Relaxed)
    }

    /// Whether the family's costs are those of `rules`: samples measured
    /// under other rules change nothing.
    fn judges(&self, rules: &Arc<PersistentRules>) -> bool {
        self.judged
            .load()
            .as_ref()
            .is_some_and(|judged| Arc::ptr_eq(&judged.rules, rules))
    }

    /// [`smooth`] `cost`, measured under `rules`, into `average`, unless
    /// the family no longer judges `rules`.
    fn smooth(
        &self,
        average: &AtomicU64,
        cost: Duration,
        rules: &Arc<PersistentRules>,
    ) -> Option<u64> {
        self.judges(rules).then(|| smooth(average, cost))
    }

    /// Start sharing on a verdict under `rules`, unless the family no longer
    /// judges them; whether it did.
    fn start_sharing(&self, rules: &Arc<PersistentRules>) -> bool {
        let judged = self.judges(rules);
        if judged {
            self.sharing.store(true, Ordering::Relaxed);
        }
        judged
    }

    /// Stop sharing on a verdict under `rules`, counting a failure, unless
    /// the family no longer judges them.
    fn stop_sharing(&self, rules: &Arc<PersistentRules>) {
        if !self.judges(rules) {
            return;
        }
        self.own_since_stop.store(0, Ordering::Relaxed);
        self.shared_cost_us.store(0, Ordering::Relaxed);
        self.stops.fetch_add(1, Ordering::Relaxed);
        self.sharing.store(false, Ordering::Relaxed);
    }

    /// Note a view's own evaluation on `snapshot`: its cost when it reused a
    /// compiled plan (`None` when it compiled one). Whether to probe: no
    /// parameter binds recursion under `snapshot`'s rules, the own cost is
    /// known, there are more bindings than compute permits, and the family
    /// never shared or enough own evaluations passed since it stopped.
    fn record_own(&self, cost: Option<Duration>, snapshot: &KnowledgeGraphSnapshot) -> bool {
        let rules = snapshot.persistent_rules();
        if self.binds_recursion(snapshot) || !self.judges(rules) {
            return false;
        }
        let average = match cost {
            Some(cost) => match self.smooth(&self.own_cost_us, cost, rules) {
                Some(average) => average,
                None => return false,
            },
            None => self.own_cost_us.load(Ordering::Relaxed),
        };
        if self.sharing.load(Ordering::Relaxed) {
            return false;
        }
        let since_stop = self.own_since_stop.fetch_add(1, Ordering::Relaxed) + 1;
        let bindings = self.bindings.load(Ordering::Relaxed) as u64;
        let stops = self.stops.load(Ordering::Relaxed);
        average > 0
            && self.outnumbers_permits()
            && (stops == 0 || since_stop >= probe_after(bindings, stops))
    }

    /// Evaluate the round at `snapshot`, which no view waits for, at most one
    /// probe at a time, under the server's probe permit: the guard judging
    /// it decides whether views share, and views that then share at its
    /// revision read it, while a round judged too slow is let go. With too
    /// few compute permits to spare the CPU, share without one.
    fn probe(family: &Arc<Family>, snapshot: Arc<KnowledgeGraphSnapshot>) {
        if family.handler.compute_permits() < MIN_PERMITS_FOR_PROBES {
            family.start_sharing(snapshot.persistent_rules());
            return;
        }
        if family.probing.swap(true, Ordering::Relaxed) {
            return;
        }
        let round = family.round_for(snapshot.revision);
        let family = Arc::clone(family);
        tokio::spawn(async move {
            #[cfg(test)]
            let gate = family.probe_gate.read().await;
            let _ = family.outcome(&round, true).await;
            #[cfg(test)]
            drop(gate);
            family.probing.store(false, Ordering::Relaxed);
        });
    }
}

/// A family's key: knowledge graph, lifted query and parameter kinds.
type FamilyKey = (String, String, Vec<ParamKind>);

/// Every family of a server, by knowledge graph, lifted query and parameter
/// kinds.
#[derive(Default)]
pub struct Families {
    families: Mutex<HashMap<FamilyKey, Weak<Family>>>,
}

impl Families {
    /// A view of `own`'s query in the family of its lifted shape.
    pub fn member(
        &self,
        own: ReevaluatingQuery,
        lifted: Lifted,
        metrics: &Arc<SubscriptionMetrics>,
    ) -> MemberQuery {
        let Lifted { shape, binding } = lifted;
        let key = (
            own.knowledge_graph().to_string(),
            shape.query.clone(),
            shape.kinds.clone(),
        );
        let family = {
            let mut families = self.families.lock();
            match families.get(&key).and_then(Weak::upgrade) {
                Some(family) => family,
                None => {
                    families.retain(|_, family| family.strong_count() > 0);
                    let family = Arc::new(Family {
                        handler: Arc::clone(own.handler()),
                        knowledge_graph: key.0.clone(),
                        shape,
                        metrics: Arc::clone(metrics),
                        members: Mutex::default(),
                        bindings: AtomicUsize::new(0),
                        latest: ArcSwapOption::empty(),
                        evaluating: tokio::sync::Mutex::new(()),
                        sharing: AtomicBool::new(false),
                        probing: AtomicBool::new(false),
                        #[cfg(test)]
                        probe_gate: tokio::sync::RwLock::default(),
                        #[cfg(test)]
                        round_gate: tokio::sync::RwLock::default(),
                        own_since_stop: AtomicU64::new(0),
                        stops: AtomicU64::new(0),
                        own_cost_us: AtomicU64::new(0),
                        shared_cost_us: AtomicU64::new(0),
                        rounds: AtomicU64::new(0),
                        sample_due: AtomicBool::new(false),
                        judged: ArcSwapOption::empty(),
                    });
                    families.insert(key, Arc::downgrade(&family));
                    family
                }
            }
        };
        family.join(&binding);
        let program = transform_query_shorthand(own.query())
            .map(|transform| transform.query)
            .unwrap_or_default();
        let width = family.shape.projection.len();
        MemberQuery {
            query_columns: extract_column_names_from_query(&program, width),
            own,
            family,
            binding,
            validated: None,
        }
    }
}

/// A view of a family: reads its rows from the family's rounds, or evaluates
/// its own query.
pub struct MemberQuery {
    own: ReevaluatingQuery,
    family: Arc<Family>,
    binding: Binding,
    /// Column names when the source relation's schema does not name them.
    query_columns: Vec<String>,
    /// The rules this view's own query last evaluated without error.
    validated: Option<Arc<PersistentRules>>,
}

impl MemberQuery {
    async fn reevaluate(&mut self) -> Result<Refresh, String> {
        let snapshot = self.own.current_snapshot()?;
        let validated = self
            .validated
            .as_ref()
            .is_some_and(|rules| Arc::ptr_eq(rules, snapshot.persistent_rules()));
        if validated && self.family.shares() && !self.family.takes_sample() {
            let round = self.family.round_for(snapshot.revision);
            if let Ok(partitions) = self.family.outcome(&round, false).await {
                if let Some(evaluated) = self.read(&partitions) {
                    return Ok(self.own.adopt(evaluated));
                }
            }
        }
        let snapshot = self.own.current_snapshot()?;
        let rules = Arc::clone(snapshot.persistent_rules());
        let evaluated = self.own.evaluate_on(Arc::clone(&snapshot)).await?;
        if self
            .family
            .record_own(evaluated.plan_cached.then_some(evaluated.cost), &snapshot)
        {
            Family::probe(&self.family, snapshot);
        }
        self.validated = Some(rules);
        Ok(self.own.adopt(evaluated))
    }

    /// This view's result in `partitions`, unless it must evaluate its own
    /// query: the rules changed, or its rows may exceed `max_result_rows`.
    fn read(&self, partitions: &Partitions) -> Option<Evaluated> {
        if !self
            .validated
            .as_ref()
            .is_some_and(|rules| Arc::ptr_eq(rules, &partitions.rules))
        {
            return None;
        }
        let cap = self
            .own
            .handler()
            .config()
            .storage
            .performance
            .max_result_rows;
        let part = partitions.parts.get(&self.binding);
        if cap > 0 && part.is_some_and(|part| part.rows > cap) {
            return None;
        }
        let result = part.map_or_else(Arc::default, |part| Arc::clone(&part.result));
        let columns = (!result.is_empty()).then(|| match &partitions.schema_columns {
            Some(names) if names.len() == self.query_columns.len() => names.clone(),
            _ => self.query_columns.clone(),
        });
        Some(Evaluated {
            columns,
            result,
            dependencies: partitions.dependencies.clone(),
            revision: partitions.revision,
            cost: Duration::ZERO,
            plan_cached: false,
        })
    }
}

impl StandingQuery for MemberQuery {
    fn refresh(&mut self) -> BoxFuture<'_, Result<Refresh, String>> {
        Box::pin(self.reevaluate())
    }
}

impl Drop for MemberQuery {
    fn drop(&mut self) {
        self.family.leave(&self.binding);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
