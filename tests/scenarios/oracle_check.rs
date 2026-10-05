//! Feed a scenario's history to the differential oracle.
//!
//! The scenario drives one real engine over `/ws`; the same statements run
//! here through the oracle's adapters (the naive reference, recompute,
//! standing queries rebuilt from deltas, and a subscription group), which
//! must agree on every query at every checkpoint. The reference must model
//! the whole history: a scenario whose meaning the reference cannot compute
//! proves nothing here.

use crate::adapter::Adapter;
use crate::group::GroupAdapter;
use crate::minimize;
use crate::model::{History, Step};
use crate::oracle::{self, Report};
use crate::recompute::RecomputeAdapter;
use crate::reference::ReferenceAdapter;
use crate::subscription::{Fault, SubscriptionAdapter};

fn adapters(queries: &[String]) -> Vec<Box<dyn Adapter>> {
    vec![
        Box::new(ReferenceAdapter::new()),
        Box::new(RecomputeAdapter::open().expect("open recompute engine")),
        Box::new(
            SubscriptionAdapter::open(queries, Fault::None).expect("open subscription engine"),
        ),
        Box::new(GroupAdapter::open(queries).expect("open subscription group engine")),
    ]
}

/// Run `steps` (statements; `"#name"` is a checkpoint, `"restart"` a
/// restart) observing `queries`; panics with the first divergence, minimized
/// to a reproducing script, or when the reference skipped anything.
pub fn oracle_check(label: &str, queries: &[&str], steps: &[String]) -> Report {
    let history = History {
        queries: queries.iter().map(|q| (*q).to_string()).collect(),
        steps: steps
            .iter()
            .map(|line| match line.as_str() {
                "restart" => Step::Restart,
                l if l.starts_with('#') => Step::Checkpoint(l[1..].to_string()),
                l => Step::Execute(l.to_string()),
            })
            .collect(),
    };
    let report = oracle::run(&history, &adapters);
    if let Some(first) = report.divergences.first() {
        let minimized = minimize::minimize(&history, &adapters, &first.signature);
        panic!(
            "{label}: {first}\nminimized to {} statements:\n{}",
            minimized.statements(),
            minimized.script()
        );
    }
    let skips: Vec<String> = report
        .skips
        .iter()
        .map(|s| format!("{} at {}: {}", s.adapter, s.at, s.reason))
        .collect();
    assert!(skips.is_empty(), "{label}: skipped:\n{}", skips.join("\n"));
    // The reference is the baseline; the others are compared against it.
    for adapter in ["recompute", "subscription", "subscription[group]"] {
        assert!(
            report.compared.get(adapter).copied().unwrap_or(0) > 0,
            "{label}: nothing was compared for {adapter}: {}",
            report.summary()
        );
    }
    report
}
