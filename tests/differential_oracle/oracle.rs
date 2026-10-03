//! Runs one history through every adapter and compares them step by step.
//!
//! Statement outcomes are compared after every statement; query results at
//! every checkpoint (plus a final implicit `end` checkpoint). The baseline of
//! a comparison is the first adapter, in factory order, that gave a definite
//! answer. Unsupported answers are recorded as skips, never as agreement.
//! The run stops after the first step that diverges: later state would only
//! repeat the same defect.

use std::collections::BTreeSet;
use std::fmt;

use crate::adapter::{Adapter, AdapterFactory};
use crate::model::{show_row, AdapterError, History, Observation, Outcome, Revision, Row, Step};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    /// One adapter accepted a statement another rejected.
    Outcome,
    /// Result rows differ from the baseline's.
    Rows,
    /// A row appears other than exactly once.
    Multiplicity,
    /// The adapter itself failed.
    Failed,
}

/// What makes two divergences "the same bug" for minimization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    pub kind: Kind,
    pub adapter: String,
    pub query: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Divergence {
    pub signature: Signature,
    pub baseline: Option<String>,
    /// Checkpoint name, or the statement that diverged.
    pub at: String,
    pub revision: Revision,
    pub detail: String,
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = &self.signature;
        write!(f, "{:?} divergence in '{}'", s.kind, s.adapter)?;
        if let Some(baseline) = &self.baseline {
            write!(f, " vs '{baseline}'")?;
        }
        write!(f, " at {} (revision {})", self.at, self.revision.0)?;
        if let Some(query) = &s.query {
            write!(f, " for `{query}`")?;
        }
        write!(f, ": {}", self.detail)
    }
}

/// An answer an adapter could not give.
#[derive(Debug, Clone)]
pub struct Skip {
    pub adapter: String,
    pub at: String,
    pub reason: String,
}

#[derive(Debug, Default)]
pub struct Report {
    pub divergences: Vec<Divergence>,
    pub skips: Vec<Skip>,
    /// Observations compared against a baseline, per adapter name.
    pub compared: std::collections::BTreeMap<String, usize>,
}

impl Report {
    pub fn finds(&self, signature: &Signature) -> bool {
        self.divergences.iter().any(|d| &d.signature == signature)
    }

    /// Skips of one adapter.
    pub fn skips_of<'a>(&'a self, adapter: &'a str) -> impl Iterator<Item = &'a Skip> + 'a {
        self.skips.iter().filter(move |s| s.adapter == adapter)
    }

    pub fn summary(&self) -> String {
        let mut out = String::new();
        for d in &self.divergences {
            out.push_str(&format!("{d}\n"));
        }
        out
    }
}

pub fn run(history: &History, factory: &AdapterFactory<'_>) -> Report {
    let mut adapters = factory(&history.queries);
    let mut report = Report::default();
    let mut observed_last = false;
    for (index, step) in history.steps.iter().enumerate() {
        let revision = Revision(index + 1);
        observed_last = false;
        match step {
            Step::Execute(statement) => execute(&mut adapters, statement, revision, &mut report),
            Step::Restart => {
                for adapter in &mut adapters {
                    let result = adapter.restart(revision);
                    record_error(
                        &mut report,
                        adapter.as_ref(),
                        "restart",
                        revision,
                        None,
                        result.err(),
                    );
                }
            }
            Step::Checkpoint(name) => {
                observe(&mut adapters, &history.queries, name, revision, &mut report);
                observed_last = true;
            }
        }
        if !report.divergences.is_empty() {
            return report;
        }
    }
    if !observed_last {
        let end = Revision(history.steps.len());
        observe(&mut adapters, &history.queries, "end", end, &mut report);
    }
    report
}

fn record_error(
    report: &mut Report,
    adapter: &dyn Adapter,
    at: &str,
    revision: Revision,
    query: Option<&str>,
    error: Option<AdapterError>,
) {
    match error {
        None => {}
        Some(AdapterError::Unsupported(reason)) => report.skips.push(Skip {
            adapter: adapter.name().to_string(),
            at: at.to_string(),
            reason,
        }),
        Some(AdapterError::Failed(detail)) => report.divergences.push(Divergence {
            signature: Signature {
                kind: Kind::Failed,
                adapter: adapter.name().to_string(),
                query: query.map(str::to_string),
            },
            baseline: None,
            at: at.to_string(),
            revision,
            detail,
        }),
    }
}

fn execute(
    adapters: &mut [Box<dyn Adapter>],
    statement: &str,
    revision: Revision,
    report: &mut Report,
) {
    let outcomes: Vec<Result<Outcome, AdapterError>> = adapters
        .iter_mut()
        .map(|a| a.execute(statement, revision))
        .collect();
    let baseline = outcomes.iter().enumerate().find_map(|(i, o)| match o {
        Ok(o @ (Outcome::Applied | Outcome::Rejected(_))) => Some((i, o.clone())),
        _ => None,
    });
    let names: Vec<String> = adapters.iter().map(|a| a.name().to_string()).collect();
    let at = format!("`{statement}`");
    for (i, (adapter, outcome)) in adapters.iter_mut().zip(outcomes).enumerate() {
        match (outcome, &baseline) {
            (Err(e), _) => record_error(report, adapter.as_ref(), &at, revision, None, Some(e)),
            (Ok(_), None) => {}
            (Ok(_), Some((b, _))) if *b == i => {}
            (Ok(Outcome::Assumed), Some((b, Outcome::Rejected(why)))) => {
                let reason = format!(
                    "{} rejected {at}, which this adapter assumed valid: {why}",
                    names[*b]
                );
                adapter.desync(reason.clone());
                report.skips.push(Skip {
                    adapter: adapter.name().to_string(),
                    at: at.clone(),
                    reason,
                });
            }
            (Ok(Outcome::Assumed), Some(_)) => {}
            (Ok(mine), Some((b, theirs))) => {
                if mine.accepted() != theirs.accepted() {
                    report.divergences.push(Divergence {
                        signature: Signature {
                            kind: Kind::Outcome,
                            adapter: adapter.name().to_string(),
                            query: None,
                        },
                        baseline: Some(names[*b].clone()),
                        at: at.clone(),
                        revision,
                        detail: format!("{mine:?}, baseline {theirs:?}"),
                    });
                }
            }
        }
    }
}

fn observe(
    adapters: &mut [Box<dyn Adapter>],
    queries: &[String],
    checkpoint: &str,
    revision: Revision,
    report: &mut Report,
) {
    let at = format!("checkpoint '{checkpoint}'");
    for query in queries {
        let mut baseline: Option<(String, Observation)> = None;
        for adapter in adapters.iter_mut() {
            let name = adapter.name().to_string();
            let observation = match adapter.observe(query, revision) {
                Ok(observation) => observation,
                Err(e) => {
                    record_error(
                        report,
                        adapter.as_ref(),
                        &at,
                        revision,
                        Some(query),
                        Some(e),
                    );
                    continue;
                }
            };
            let signature = |kind| Signature {
                kind,
                adapter: name.clone(),
                query: Some(query.clone()),
            };
            let non_set = observation.non_set_rows();
            if !non_set.is_empty() {
                let rows: Vec<String> = non_set
                    .iter()
                    .map(|(r, w)| format!("{} x{w}", show_row(r)))
                    .collect();
                report.divergences.push(Divergence {
                    signature: signature(Kind::Multiplicity),
                    baseline: None,
                    at: at.clone(),
                    revision,
                    detail: format!("rows not present exactly once: {}", rows.join(", ")),
                });
            }
            match &baseline {
                None => baseline = Some((name.clone(), observation)),
                Some((base_name, base)) => {
                    *report.compared.entry(name.clone()).or_default() += 1;
                    if let Some(detail) = difference(base, &observation) {
                        report.divergences.push(Divergence {
                            signature: signature(Kind::Rows),
                            baseline: Some(base_name.clone()),
                            at: at.clone(),
                            revision,
                            detail,
                        });
                    }
                }
            }
        }
        if let Some((name, _)) = baseline {
            *report.compared.entry(name).or_default() += 1;
        }
    }
}

fn difference(expected: &Observation, actual: &Observation) -> Option<String> {
    let expected: BTreeSet<&Row> = expected.support().collect();
    let actual: BTreeSet<&Row> = actual.support().collect();
    if expected == actual {
        return None;
    }
    let list = |rows: Vec<&&Row>| {
        rows.iter()
            .map(|r| show_row(r))
            .collect::<Vec<_>>()
            .join(", ")
    };
    Some(format!(
        "missing [{}], unexpected [{}]",
        list(expected.difference(&actual).collect()),
        list(actual.difference(&expected).collect()),
    ))
}
