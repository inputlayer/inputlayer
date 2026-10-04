//! The machine-readable result of a GenBI benchmark run.
//!
//! Compatible with the perf gate's run file (`inputlayer-perf-gate/v1`):
//! the same [`Environment`] fingerprint, latency series as raw microsecond
//! samples keyed by name, [`Rate`] counters and point gauges, one record per
//! scenario run. Expected rows never appear here; checks report counts only.

use std::collections::BTreeMap;

use serde::Serialize;

use super::agent::Fault;
use super::score::Diff;
use crate::schema::{Environment, Rate};

pub const SCHEMA: &str = "inputlayer-genbi-bench/v1";

#[derive(Debug, Serialize)]
pub struct BenchRecord {
    pub schema: &'static str,
    pub created_unix: u64,
    pub environment: Environment,
    pub suite: SuiteInfo,
    pub config: Config,
    pub scenarios: Vec<ScenarioRun>,
}

#[derive(Debug, Serialize)]
pub struct SuiteInfo {
    pub dir: String,
    pub version: String,
    pub cases_total: usize,
    pub cases_selected: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Config {
    pub server_label: String,
    pub server_binary: String,
    pub server_sha256: String,
    pub server_overrides: BTreeMap<String, String>,
    pub repeat: u32,
    pub agents: usize,
    pub quiet_ms: u64,
    pub deadline_ms: u64,
    pub fault: Option<Fault>,
}

/// One scenario on one fresh server.
#[derive(Debug, Default, Serialize)]
pub struct ScenarioRun {
    pub case_id: String,
    pub category: String,
    pub repetition: u32,
    /// Set when the harness could not run the scenario at all.
    pub error: Option<String>,
    pub findings: Vec<Finding>,
    pub checks: Vec<CheckOutcome>,
    pub mutations: Vec<MutationRun>,
    /// Raw samples in microseconds: `load_us`, `initial_query_us`,
    /// `subscribe_us`, `ack_us`, `requery_us`, `delta_us` (writer send to
    /// the delta completing the agent's answer), `delta_insert_us`,
    /// `delta_retract_us` (split by whether the answer lost rows) and
    /// `ack_to_delta_us` (the same delta measured from the write ack; also
    /// split into `ack_to_delta_first_us` for the scenario's first write and
    /// `ack_to_delta_later_us` for the rest).
    pub series: BTreeMap<String, Vec<u64>>,
    /// `load_statements`; `converged_mutations` (send to every agent
    /// converged, serial writer; a mutation any live subscription diverged
    /// on is not counted, and retired questions do not count).
    pub rates: BTreeMap<String, Rate>,
    /// `rss_after_load_kb`, `rss_after_subscribe_kb`, `rss_end_kb`,
    /// `peak_rss_kb`, `seed_statements`, `seed_failed`, `subscriptions`.
    pub gauges: BTreeMap<String, u64>,
}

impl ScenarioRun {
    pub fn sample(&mut self, series: &str, micros: u64) {
        self.series
            .entry(series.to_string())
            .or_default()
            .push(micros);
    }

    pub fn finding(&mut self, kind: FindingKind, detail: impl Into<String>) {
        self.findings.push(Finding {
            kind,
            detail: detail.into(),
        });
    }
}

/// Something the run learned about the suite's seed or the engine.
#[derive(Debug, Serialize)]
pub struct Finding {
    pub kind: FindingKind,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingKind {
    /// A seed statement failed to load (the seed is unverified upstream).
    SeedStatementFailed,
    /// A check has no IQL translation for this seed.
    UnsupportedCheck,
    /// A mutation has no IQL translation for this seed.
    UnsupportedMutation,
    /// A translated mutation failed in the engine.
    MutationFailed,
    /// A standing query failed to evaluate or subscribe.
    QueryFailed,
    /// A delta retracted a row the agent did not hold, or re-inserted one.
    DeltaAnomaly,
}

#[derive(Debug, Serialize)]
pub struct CheckOutcome {
    pub phase: String,
    pub check: String,
    pub status: CheckStatus,
    pub reason: Option<Reason>,
    /// The answer lost rows in this phase: scored as retraction correctness.
    pub after_retraction: bool,
    /// Agent-maintained answer versus expected.
    pub agent: Option<Diff>,
    /// Fresh re-query versus expected.
    pub requery: Option<Diff>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    Pass,
    Fail,
    NotRun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// The agent's maintained answer differs from a fresh evaluation: the
    /// delta path is wrong.
    DeltaDivergence,
    /// The engine pushed a `subscription_error`.
    SubscriptionError,
    /// The standing query failed to evaluate or subscribe.
    QueryFailed,
    /// Agent and re-query agree, but not with the expected rows: the seed's
    /// rules or data disagree with the reference model.
    ResultMismatch,
    /// No IQL translation for the check.
    Unsupported,
    /// An earlier mutation could not be applied, so this phase is undefined.
    MutationNotApplied,
    /// The expected check has no public question.
    NoQuestion,
}

impl Reason {
    /// Failures of the reactive delta path itself, as opposed to seed or
    /// translation coverage.
    pub fn is_delta_path(self) -> bool {
        matches!(self, Reason::DeltaDivergence | Reason::SubscriptionError)
    }

    pub fn label(self) -> &'static str {
        match self {
            Reason::DeltaDivergence => "delta divergence",
            Reason::SubscriptionError => "subscription error",
            Reason::QueryFailed => "query failed",
            Reason::ResultMismatch => "result mismatch",
            Reason::Unsupported => "unsupported check",
            Reason::MutationNotApplied => "mutation not applied",
            Reason::NoQuestion => "no public question",
        }
    }
}

/// One ordered mutation as applied by the independent writer.
#[derive(Debug, Serialize)]
pub struct MutationRun {
    pub label: String,
    pub statements: usize,
    /// Writer send to acknowledgement.
    pub ack_us: Option<u64>,
    pub error: Option<String>,
    pub subscriptions: Vec<DeltaOutcome>,
}

/// What one subscription of one agent saw for one mutation.
#[derive(Debug, Serialize)]
pub struct DeltaOutcome {
    pub subscription: String,
    pub agent: usize,
    /// The answer changed (per a fresh re-query).
    pub changed: bool,
    /// The change removed rows.
    pub retracting: bool,
    /// Writer send to the last delta frame (none if no frame arrived).
    pub delta_us: Option<u64>,
    pub frames: usize,
    pub rows_inserted: usize,
    pub rows_retracted: usize,
    pub convergence: Convergence,
}

/// Whether a subscription's answer reached the fresh re-query by the deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Convergence {
    Converged,
    Diverged,
    /// The question failed to evaluate and was retired: it no longer counts
    /// toward convergence.
    Retired,
}
