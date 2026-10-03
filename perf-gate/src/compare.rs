//! Judging a candidate against the baseline under a [`Policy`].
//!
//! The unit of replication is the round: each round yields one value per arm
//! per metric (a percentile of that round's raw samples, or its rate). Both
//! arms of a round run back to back, so each round gives one paired *cost*
//! ratio, candidate over baseline (above 1.0 is worse, for latency and
//! throughput alike), and host drift between rounds cancels out. The estimate
//! is the median of the per-round ratios, with its distribution-free
//! (binomial order-statistic) confidence interval:
//!
//! - interval entirely within budget (`hi <= 1 + tolerance`): pass;
//! - interval entirely beyond budget (`lo > 1 + tolerance`): fail;
//! - otherwise inconclusive: too noisy to tell, which never passes.
//!
//! Missing fixtures, failed or wrong-result runs, too few rounds or too few
//! samples make the verdict invalid. Only a pass exits zero.

use std::collections::BTreeSet;

use serde::Serialize;

use crate::policy::{MetricKey, Policy, Stat};
use crate::schema::{FixtureRun, RunRecord, SCHEMA};
use crate::stats::{median, median_interval, percentile, relative_spread};

/// Outcome of a metric or of the whole gate, worst last.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Status {
    Pass,
    Inconclusive,
    Fail,
    Invalid,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricVerdict {
    pub metric: String,
    pub required: bool,
    pub status: Status,
    pub reason: String,
    /// Per-round values, microseconds or operations per second.
    pub baseline: Vec<f64>,
    pub candidate: Vec<f64>,
    pub baseline_median: Option<f64>,
    pub candidate_median: Option<f64>,
    /// Paired per-round cost ratios, candidate over baseline.
    pub round_ratios: Vec<f64>,
    /// Median of `round_ratios`; above 1.0 is worse.
    pub cost_ratio: Option<f64>,
    pub interval: Option<(f64, f64)>,
    pub tolerance: f64,
    /// (max - min) / median of the baseline's rounds.
    pub baseline_spread: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Verdict {
    pub status: Status,
    pub policy_status: String,
    /// Run-level problems; any makes the verdict invalid.
    pub problems: Vec<String>,
    pub metrics: Vec<MetricVerdict>,
}

/// Judge `record` under `policy`.
pub fn judge(record: &RunRecord, policy: &Policy) -> Verdict {
    let mut problems = run_problems(record);
    let required: BTreeSet<MetricKey> = policy
        .required
        .iter()
        .filter_map(|key| MetricKey::parse(key).ok())
        .collect();
    let mut keys = observed_keys(record);
    keys.extend(required.iter().cloned());
    let metrics: Vec<MetricVerdict> = keys
        .iter()
        .map(|key| judge_metric(record, policy, key, required.contains(key)))
        .collect();
    for metric in metrics.iter().filter(|m| m.required) {
        if metric.status == Status::Invalid {
            problems.push(format!("{}: {}", metric.metric, metric.reason));
        }
    }
    let worst = metrics
        .iter()
        .filter(|m| m.required)
        .map(|m| m.status)
        .max()
        .unwrap_or(Status::Invalid);
    let status = if problems.is_empty() {
        worst
    } else {
        Status::Invalid
    };
    Verdict {
        status,
        policy_status: policy.status.clone(),
        problems,
        metrics,
    }
}

fn run_problems(record: &RunRecord) -> Vec<String> {
    let mut problems = Vec::new();
    if record.schema != SCHEMA {
        problems.push(format!(
            "schema '{}' is not '{SCHEMA}'; re-run with this gate",
            record.schema
        ));
    }
    let arms: Vec<&str> = record.arms.iter().map(|a| a.name.as_str()).collect();
    if arms != ["baseline", "candidate"] {
        problems.push(format!("arms {arms:?}, expected [baseline, candidate]"));
    }
    for run in &record.runs {
        if let Some(error) = &run.error {
            problems.push(format!(
                "round {} {} {}: {error}",
                run.round + 1,
                run.fixture,
                run.arm
            ));
        }
    }
    problems
}

/// Every metric the run produced: p50/p99 of each series and each rate.
fn observed_keys(record: &RunRecord) -> BTreeSet<MetricKey> {
    let mut keys = BTreeSet::new();
    for run in &record.runs {
        for name in run.series.keys() {
            for stat in [Stat::P50, Stat::P99] {
                keys.insert(MetricKey {
                    fixture: run.fixture.clone(),
                    name: name.clone(),
                    stat,
                });
            }
        }
        for name in run.rates.keys() {
            keys.insert(MetricKey {
                fixture: run.fixture.clone(),
                name: name.clone(),
                stat: Stat::Rate,
            });
        }
    }
    keys
}

fn judge_metric(
    record: &RunRecord,
    policy: &Policy,
    key: &MetricKey,
    required: bool,
) -> MetricVerdict {
    let tolerance = policy.tolerance_for(key);
    let (baseline, base_issue) = round_values(record, policy, key, "baseline");
    let (candidate, cand_issue) = round_values(record, policy, key, "candidate");
    let round_ratios = paired_cost_ratios(key.stat, &baseline, &candidate);
    let values = |rounds: &[(u32, f64)]| rounds.iter().map(|(_, v)| *v).collect::<Vec<_>>();
    let mut verdict = MetricVerdict {
        metric: key.to_string(),
        required,
        status: Status::Invalid,
        reason: String::new(),
        baseline_median: median(&values(&baseline)),
        candidate_median: median(&values(&candidate)),
        baseline_spread: relative_spread(&values(&baseline)),
        baseline: values(&baseline),
        candidate: values(&candidate),
        cost_ratio: median(&round_ratios),
        round_ratios,
        interval: None,
        tolerance,
    };
    if let Some(issue) = base_issue.or(cand_issue) {
        verdict.reason = issue;
        return verdict;
    }
    if verdict.round_ratios.len() < policy.min_rounds {
        verdict.reason = format!(
            "{} paired rounds with a non-zero baseline, need {}",
            verdict.round_ratios.len(),
            policy.min_rounds
        );
        return verdict;
    }
    let interval = median_interval(&verdict.round_ratios, policy.confidence);
    let Some((lo, hi)) = interval else {
        verdict.reason = format!(
            "{} rounds cannot give a {:.0}% interval; add rounds",
            verdict.round_ratios.len(),
            policy.confidence * 100.0
        );
        return verdict;
    };
    verdict.interval = Some((lo, hi));
    let budget = 1.0 + tolerance;
    (verdict.status, verdict.reason) = if hi <= budget {
        (Status::Pass, format!("interval within {budget:.2}"))
    } else if lo > budget {
        (
            Status::Fail,
            format!("regression: whole interval above {budget:.2}"),
        )
    } else {
        (
            Status::Inconclusive,
            format!("interval straddles {budget:.2}: rerun on a quieter host or add rounds"),
        )
    };
    verdict
}

/// Cost ratio of each round both arms completed: latency candidate over
/// baseline, throughput baseline over candidate. Rounds with a zero
/// denominator are dropped.
fn paired_cost_ratios(stat: Stat, baseline: &[(u32, f64)], candidate: &[(u32, f64)]) -> Vec<f64> {
    baseline
        .iter()
        .filter_map(|(round, base)| {
            let (_, cand) = candidate.iter().find(|(r, _)| r == round)?;
            let (num, den) = match stat {
                Stat::Rate => (*base, *cand),
                Stat::P50 | Stat::P99 => (*cand, *base),
            };
            (den > 0.0).then(|| num / den)
        })
        .collect()
}

/// Per-round values of `key` on `arm`, or why they cannot be used.
fn round_values(
    record: &RunRecord,
    policy: &Policy,
    key: &MetricKey,
    arm: &str,
) -> (Vec<(u32, f64)>, Option<String>) {
    let runs: Vec<&FixtureRun> = record
        .runs
        .iter()
        .filter(|r| r.arm == arm && r.fixture == key.fixture && r.error.is_none())
        .collect();
    let mut values = Vec::with_capacity(runs.len());
    for run in runs {
        let value = match key.stat.quantile() {
            Some(q) => {
                let Some(samples) = run.series.get(&key.name) else {
                    return (
                        values,
                        Some(format!("{arm} round {} lacks it", run.round + 1)),
                    );
                };
                let needed = if key.stat == Stat::P99 {
                    policy.min_samples_p99
                } else {
                    policy.min_samples_p50
                };
                if samples.len() < needed {
                    let issue = format!(
                        "{arm} round {}: {} samples, need {needed}",
                        run.round + 1,
                        samples.len()
                    );
                    return (values, Some(issue));
                }
                percentile(samples, q)
            }
            None => run.rates.get(&key.name).map(crate::schema::Rate::per_sec),
        };
        match value {
            Some(v) => values.push((run.round, v)),
            None => {
                return (
                    values,
                    Some(format!("{arm} round {} lacks it", run.round + 1)),
                )
            }
        }
    }
    if values.len() < policy.min_rounds {
        let issue = format!(
            "{arm}: {} valid rounds, need {}",
            values.len(),
            policy.min_rounds
        );
        return (values, Some(issue));
    }
    (values, None)
}

#[cfg(test)]
#[path = "compare_tests.rs"]
mod tests;
