//! Differential correctness oracle for derived results.
//!
//! Runs the same history through independent adapters and compares them at
//! named revisions:
//!
//! - `reference` - naive finite evaluator ([`reference`]), the meaning;
//! - `recompute` - the engine's snapshot evaluator, queried afresh;
//! - `subscription` - standing queries assembled from pushed deltas, the
//!   path a subscribed agent sees;
//! - `spec` - recorded `.iql.out` results (corpus cases only).
//!
//! Histories come from hand-written scenarios, seeded random generation and
//! the `.iql` corpus. A divergence is minimized to a short reproducing script.
//!
//! Scale the random run with `INPUTLAYER_ORACLE_SEEDS=<n>` (default 12) and
//! reproduce one seed with `INPUTLAYER_ORACLE_SEED=<seed>`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod adapter;
mod corpus;
mod engine;
mod generate;
mod minimize;
mod model;
mod oracle;
mod recompute;
mod reference;
mod scenarios;
mod subscription;

use adapter::Adapter;
use model::History;
use oracle::Report;
use recompute::RecomputeAdapter;
use reference::ReferenceAdapter;
use subscription::{Fault, SubscriptionAdapter};

/// The standard adapter set, reference first so it is the baseline wherever
/// it can answer.
fn adapters(queries: &[String]) -> Vec<Box<dyn Adapter>> {
    with_fault(queries, Fault::None)
}

fn with_fault(queries: &[String], fault: Fault) -> Vec<Box<dyn Adapter>> {
    vec![
        Box::new(ReferenceAdapter::new()),
        Box::new(RecomputeAdapter::open().expect("open recompute engine")),
        Box::new(SubscriptionAdapter::open(queries, fault).expect("open subscription engine")),
    ]
}

/// Fail with the first divergence, minimized to a reproducing script.
fn assert_agrees(label: &str, history: &History) -> Report {
    let report = oracle::run(history, &adapters);
    if let Some(first) = report.divergences.first() {
        let minimized = minimize::minimize(history, &adapters, &first.signature);
        let again = oracle::run(&minimized, &adapters);
        panic!(
            "{label}: {}\nminimized to {} statements:\n{}\n{}",
            first,
            minimized.statements(),
            minimized.script(),
            again.summary()
        );
    }
    report
}

/// The reference must answer every observation of a history it fully models.
fn assert_reference_complete(label: &str, report: &Report) {
    let skips: Vec<String> = report
        .skips_of("reference")
        .map(|s| format!("{}: {}", s.at, s.reason))
        .collect();
    assert!(
        skips.is_empty(),
        "{label}: reference skipped:\n{}",
        skips.join("\n")
    );
    assert!(
        report.compared.get("subscription").copied().unwrap_or(0) > 0,
        "{label}: nothing was compared"
    );
}

#[test]
fn scenarios_agree_across_all_adapters() {
    for (label, history) in scenarios::all() {
        let report = assert_agrees(label, &history);
        assert_reference_complete(label, &report);
    }
}

fn seeds() -> Vec<u64> {
    if let Some(seed) = std::env::var("INPUTLAYER_ORACLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return vec![seed];
    }
    let count = std::env::var("INPUTLAYER_ORACLE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12);
    (0..count).collect()
}

#[test]
fn seeded_random_histories_agree() {
    for seed in seeds() {
        let history = generate::history(seed, 24);
        let report = assert_agrees(&format!("seed {seed}"), &history);
        assert_reference_complete(&format!("seed {seed}"), &report);
    }
}

/// The oracle must catch a maintenance bug, and shrink it to a short script.
#[test]
fn corrupted_adapter_is_caught_and_minimized() {
    let corrupted = |queries: &[String]| with_fault(queries, Fault::DropRetractions);
    let (seed, history, report) = (0..32)
        .map(|seed| {
            let history = generate::history(seed, 24);
            let report = oracle::run(&history, &corrupted);
            (seed, history, report)
        })
        .find(|(_, _, report)| !report.divergences.is_empty())
        .expect("dropping retractions must diverge within 32 seeds");
    let first = report.divergences[0].clone();
    assert_eq!(
        first.signature.adapter, "subscription[drop-retractions]",
        "{first}"
    );

    let minimized = minimize::minimize(&history, &corrupted, &first.signature);
    let again = oracle::run(&minimized, &corrupted);
    assert!(
        again.finds(&first.signature),
        "minimized history must still fail"
    );
    println!(
        "seed {seed}: {first}\nminimized from {} to {} statements:\n{}",
        history.statements(),
        minimized.statements(),
        minimized.script()
    );
    // An insert and a delete are the least that can expose a lost retraction.
    assert!(
        minimized.statements() <= 3,
        "minimization left {} statements:\n{}",
        minimized.statements(),
        minimized.script()
    );
    // The same history is clean without the fault: the defect, not the
    // history, is what the oracle found.
    assert!(oracle::run(&minimized, &adapters).divergences.is_empty());
}

#[test]
fn unsupported_constructs_are_reported_not_compared() {
    let history = History {
        queries: vec!["?scaled(X, Y)".into(), "?edge(X, Y)".into()],
        steps: vec![
            model::Step::Execute("+edge[(1, 2), (2, 3)]".into()),
            model::Step::Checkpoint("before".into()),
            model::Step::Execute("+scaled(X, Z) <- edge(X, Y), Z = Y * 10".into()),
            model::Step::Checkpoint("after".into()),
        ],
    };
    let report = assert_agrees("unsupported", &history);
    let skips: Vec<_> = report.skips_of("reference").collect();
    assert!(
        skips
            .iter()
            .any(|s| s.reason.contains("reference stopped at `+scaled")),
        "arithmetic must be reported as unsupported by the reference: {skips:?}"
    );
    // Engine adapters are still compared with each other after the reference stops.
    assert!(report.compared.get("subscription").copied().unwrap_or(0) >= 4);
}

#[test]
fn iql_corpus_agrees_with_spec_and_reference() {
    corpus::check_all();
}
