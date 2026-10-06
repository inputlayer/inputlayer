use super::*;
use crate::policy::Tolerance;
use crate::schema::{Arm, Environment, Rate, Workload};

const ROUNDS: u32 = 10;

fn policy() -> Policy {
    Policy {
        status: "test".into(),
        note: String::new(),
        min_rounds: 6,
        min_samples_p50: 20,
        min_samples_p99: 200,
        confidence: 0.95,
        tolerance: Tolerance {
            p50: 0.05,
            p99: 0.10,
            rate: 0.05,
        },
        required: vec![
            "q.latency_us.p50".into(),
            "q.latency_us.p99".into(),
            "q.per_sec".into(),
        ],
        ceilings: std::collections::BTreeMap::new(),
    }
}

/// Deterministic per-round jitter in [-1, 1].
fn jitter(round: u32, arm: u32) -> f64 {
    let x = f64::from((round * 7 + arm * 3) % 11) / 5.0;
    x - 1.0
}

/// Round samples: 1000 values around `center` microseconds with a tail.
fn samples(center: f64) -> Vec<u64> {
    (0..1000u64)
        .map(|i| {
            let tail = if i % 100 == 0 { 2.0 } else { 1.0 };
            (center * tail * (0.9 + (i % 20) as f64 / 100.0)) as u64
        })
        .collect()
}

/// A run where the candidate's latency is `slowdown` times the baseline's
/// and its throughput `1 / slowdown`, with round noise of +/- `noise`.
fn record(slowdown: f64, noise: f64) -> RunRecord {
    let mut runs = Vec::new();
    for round in 0..ROUNDS {
        for (index, arm) in ["baseline", "candidate"].into_iter().enumerate() {
            let factor = if arm == "candidate" { slowdown } else { 1.0 };
            let wobble = 1.0 + noise * jitter(round, index as u32);
            let center = 1000.0 * factor * wobble;
            let mut run = FixtureRun {
                arm: arm.into(),
                round,
                fixture: "q".into(),
                ..FixtureRun::default()
            };
            run.series.insert("latency_us".into(), samples(center));
            run.rates.insert(
                "per_sec".into(),
                Rate {
                    ops: 1000,
                    elapsed_us: (1_000_000.0 * factor * wobble) as u64,
                },
            );
            runs.push(run);
        }
    }
    RunRecord {
        schema: SCHEMA.into(),
        created_unix: 0,
        environment: Environment::default(),
        workload: Workload {
            profile: "test".into(),
            rounds: ROUNDS,
            fixtures: vec!["q".into()],
            server_overrides: std::collections::BTreeMap::new(),
            parameters: serde_json::Value::Null,
        },
        arms: ["baseline", "candidate"]
            .into_iter()
            .map(|name| Arm {
                name: name.into(),
                label: name.into(),
                binary: String::new(),
                binary_sha256: String::new(),
            })
            .collect(),
        runs,
    }
}

fn status_of<'a>(verdict: &'a Verdict, metric: &str) -> &'a MetricVerdict {
    verdict
        .metrics
        .iter()
        .find(|m| m.metric == metric)
        .unwrap_or_else(|| panic!("no {metric}"))
}

#[test]
fn identical_quiet_arms_pass() {
    let verdict = judge(&record(1.0, 0.01), &policy());
    assert_eq!(verdict.status, Status::Pass, "{verdict:#?}");
}

#[test]
fn small_improvement_passes() {
    let verdict = judge(&record(0.8, 0.01), &policy());
    assert_eq!(verdict.status, Status::Pass, "{verdict:#?}");
}

#[test]
fn injected_slowdown_fails() {
    let verdict = judge(&record(1.25, 0.01), &policy());
    assert_eq!(verdict.status, Status::Fail, "{verdict:#?}");
    for metric in ["q.latency_us.p50", "q.latency_us.p99", "q.per_sec"] {
        assert_eq!(status_of(&verdict, metric).status, Status::Fail, "{metric}");
    }
}

#[test]
fn slowdown_within_p99_budget_but_beyond_p50_budget_fails() {
    let verdict = judge(&record(1.08, 0.005), &policy());
    assert_eq!(status_of(&verdict, "q.latency_us.p50").status, Status::Fail);
    assert_eq!(status_of(&verdict, "q.latency_us.p99").status, Status::Pass);
    assert_eq!(verdict.status, Status::Fail);
}

#[test]
fn noisy_rounds_are_inconclusive_not_pass() {
    let verdict = judge(&record(1.0, 0.25), &policy());
    assert_eq!(verdict.status, Status::Inconclusive, "{verdict:#?}");
}

#[test]
fn a_failed_fixture_run_invalidates() {
    let mut run = record(1.0, 0.01);
    run.runs[3].error = Some("wrong row count".into());
    let verdict = judge(&run, &policy());
    assert_eq!(verdict.status, Status::Invalid);
    assert!(verdict
        .problems
        .iter()
        .any(|p| p.contains("wrong row count")));
}

#[test]
fn a_missing_required_metric_invalidates() {
    let mut policy = policy();
    policy.required.push("absent.latency_us.p50".into());
    let verdict = judge(&record(1.0, 0.01), &policy);
    assert_eq!(verdict.status, Status::Invalid);
    assert_eq!(
        status_of(&verdict, "absent.latency_us.p50").status,
        Status::Invalid
    );
}

#[test]
fn too_few_rounds_invalidate() {
    let mut run = record(1.0, 0.01);
    run.runs.retain(|r| r.round < 3);
    assert_eq!(judge(&run, &policy()).status, Status::Invalid);
}

#[test]
fn too_few_samples_for_p99_invalidate() {
    let mut run = record(1.0, 0.01);
    for fixture_run in &mut run.runs {
        if let Some(samples) = fixture_run.series.get_mut("latency_us") {
            samples.truncate(50);
        }
    }
    let verdict = judge(&run, &policy());
    assert_eq!(verdict.status, Status::Invalid);
    assert_eq!(status_of(&verdict, "q.latency_us.p50").status, Status::Pass);
    assert_eq!(
        status_of(&verdict, "q.latency_us.p99").status,
        Status::Invalid
    );
}

#[test]
fn a_foreign_schema_invalidates() {
    let mut run = record(1.0, 0.01);
    run.schema = "something/v0".into();
    assert_eq!(judge(&run, &policy()).status, Status::Invalid);
}

#[test]
fn an_empty_policy_never_passes() {
    let mut policy = policy();
    policy.required.clear();
    assert_eq!(judge(&record(1.0, 0.01), &policy).status, Status::Invalid);
}

#[test]
fn unrequired_metrics_are_reported_but_do_not_gate() {
    let mut run = record(1.0, 0.01);
    for fixture_run in &mut run.runs {
        let factor = if fixture_run.arm == "candidate" { 3 } else { 1 };
        fixture_run
            .series
            .insert("diagnostic_us".into(), vec![100 * factor; 300]);
    }
    let verdict = judge(&run, &policy());
    assert_eq!(verdict.status, Status::Pass);
    let diagnostic = status_of(&verdict, "q.diagnostic_us.p50");
    assert!(!diagnostic.required);
    assert_eq!(diagnostic.status, Status::Fail);
}

/// A policy with one absolute ceiling, in microseconds.
fn with_ceiling(metric: &str, ceiling: f64) -> Policy {
    let mut policy = policy();
    policy.ceilings.insert(metric.into(), ceiling);
    policy
}

#[test]
fn a_ceiling_breach_fails_without_any_regression() {
    // Both arms around 1 ms: no regression, but over a 0.5 ms ceiling.
    let verdict = judge(&record(1.0, 0.0), &with_ceiling("q.latency_us.p50", 500.0));
    assert_eq!(verdict.ceilings.len(), 1);
    assert_eq!(verdict.ceilings[0].status, Status::Fail);
    assert_eq!(verdict.status, Status::Fail);
}

#[test]
fn a_held_ceiling_passes() {
    let verdict = judge(
        &record(1.0, 0.0),
        &with_ceiling("q.latency_us.p50", 5_000.0),
    );
    assert_eq!(verdict.ceilings[0].status, Status::Pass);
    assert_eq!(verdict.status, Status::Pass);
}

#[test]
fn a_ceiling_on_a_fixture_not_run_is_not_judged() {
    let verdict = judge(
        &record(1.0, 0.0),
        &with_ceiling("other.latency_us.p50", 1.0),
    );
    assert!(verdict.ceilings.is_empty());
    assert_eq!(verdict.status, Status::Pass);
}
