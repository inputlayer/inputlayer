//! Differential correctness oracle for derived results.
//!
//! Runs the same history through independent adapters and compares them at
//! named revisions:
//!
//! - `reference` - naive finite evaluator ([`reference`]), the meaning;
//! - `recompute` - the engine's snapshot evaluator, queried afresh;
//! - `subscription` - standing queries assembled from pushed deltas, the
//!   path a subscribed agent sees;
//! - `subscription[group]` - every query in one subscription group, each
//!   member assembled from pushed group deltas;
//! - `spec` - recorded `.iql.out` results (corpus cases only);
//! - `maintained` - `recompute` on an engine whose persistent rules are
//!   maintained views, when `INPUTLAYER_ORACLE_VIEWS=maintained`.
//!
//! Histories come from hand-written scenarios, seeded random generation, the
//! `.iql` corpus and the scenario suite's shop pack ([`shop`]). A divergence
//! is minimized to a short reproducing script.
//!
//! Scale the random run with `INPUTLAYER_ORACLE_SEEDS=<n>` (default 12) and
//! reproduce one seed with `INPUTLAYER_ORACLE_SEED=<seed>`. With
//! `INPUTLAYER_ORACLE_VIEWS=maintained` every history also runs through the
//! `maintained` adapter (and the soak's server runs in that mode); until V2
//! (#309) adds the mode, every test fails saying it is not available.
//!
//! [`soak`] holds a real server under sustained concurrent writers, rule
//! changes and many (fast, slow, stalled, grouped) subscribers to the same
//! reference, at every revision any of them observes.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod adapter;
mod corpus;
mod engine;
mod generate;
mod group;
mod minimize;
mod model;
mod oracle;
mod recompute;
mod reference;
mod scenarios;
mod shop;
mod soak;
mod subscription;

use adapter::Adapter;
use engine::views_mode;
use group::GroupAdapter;
use inputlayer_testkit::Mode;
use model::History;
use oracle::Report;
use recompute::RecomputeAdapter;
use reference::ReferenceAdapter;
use subscription::{Fault, SubscriptionAdapter};

/// The standard adapter set, reference first so it is the baseline wherever
/// it can answer, and `maintained` when [`views_mode`] selects it.
fn adapters(queries: &[String]) -> Vec<Box<dyn Adapter>> {
    let mut adapters = with_fault(queries, Fault::None);
    adapters.push(Box::new(
        GroupAdapter::open(queries).expect("open subscription group engine"),
    ));
    if views_mode() == Mode::Maintained {
        match RecomputeAdapter::open_maintained() {
            Ok(adapter) => adapters.push(Box::new(adapter)),
            Err(error) => panic!("open the maintained adapter: {error:?}"),
        }
    }
    adapters
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
    for adapter in ["subscription", "subscription[group]"] {
        assert!(
            report.compared.get(adapter).copied().unwrap_or(0) > 0,
            "{label}: nothing was compared for {adapter}"
        );
    }
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

/// The shop pack and the scenario histories run on it (S9, S9b) agree
/// across all adapters, and the reference models all of them.
#[test]
fn shop_pack_corpus_agrees_across_all_adapters() {
    corpus::check_shop_pack();
}

/// Concurrent writers, rule churn and many subscribers against one server,
/// every observation checked against the reference at its revision. A smoke
/// by default; `scripts/soak.sh` runs the sustained soak.
#[test]
fn concurrent_soak_agrees_with_reference() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let report = runtime.block_on(soak::run(
        env!("CARGO_BIN_EXE_inputlayer-server"),
        soak::Config::from_env(),
        views_mode(),
    ));
    report.write();
    let summary = report.summary();
    println!("{summary}");
    assert!(report.all_failures().is_empty(), "{summary}");
}
