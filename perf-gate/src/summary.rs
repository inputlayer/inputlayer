//! Absolute numbers from a run file: what one server binary measured, for a
//! published baseline table. `compare` judges ratios between two arms; this
//! reports each metric's level instead.
//!
//! Every value is the median over runs (one per round and arm) of that run's
//! statistic (a nearest-rank percentile of its samples, its rate, or its
//! gauge), with the range of the per-run values beside it.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::schema::RunRecord;
use crate::stats::{median, percentile};

/// Per-round values of one metric, in round order.
#[derive(Default)]
struct Values {
    p50: Vec<f64>,
    p99: Vec<f64>,
    samples: usize,
}

/// Markdown tables of `record`'s runs on the arms named in `arms` (all arms
/// when empty). Arms measured from one binary (an A/A run) pool their rounds.
pub fn markdown(record: &RunRecord, arms: &[String]) -> String {
    let selected = |arm: &str| arms.is_empty() || arms.iter().any(|a| a == arm);
    let mut series: BTreeMap<(String, String), Values> = BTreeMap::new();
    let mut rates: BTreeMap<(String, String), Vec<f64>> = BTreeMap::new();
    let mut gauges: BTreeMap<(String, String), Vec<f64>> = BTreeMap::new();
    let mut failed: Vec<String> = Vec::new();
    for run in record.runs.iter().filter(|run| selected(&run.arm)) {
        if let Some(error) = &run.error {
            failed.push(format!(
                "{} round {} ({}): {error}",
                run.fixture,
                run.round + 1,
                run.arm
            ));
            continue;
        }
        for (name, samples) in &run.series {
            let values = series
                .entry((run.fixture.clone(), name.clone()))
                .or_default();
            values.p50.extend(percentile(samples, 0.50));
            values.p99.extend(percentile(samples, 0.99));
            values.samples += samples.len();
        }
        for (name, rate) in &run.rates {
            rates
                .entry((run.fixture.clone(), name.clone()))
                .or_default()
                .push(rate.per_sec());
        }
        for (name, value) in &run.gauges {
            gauges
                .entry((run.fixture.clone(), name.clone()))
                .or_default()
                .push(*value as f64);
        }
    }

    let mut out = header(record, &selected);
    // Tables without rows are left out (a memory-only run has no latencies).
    if !series.is_empty() {
        let _ = writeln!(
            out,
            "| fixture | latency series | p50 | p99 | p50 range over runs | runs | samples |"
        );
        let _ = writeln!(out, "|---|---|---|---|---|---|---|");
        for ((fixture, name), values) in &series {
            let _ = writeln!(
                out,
                "| {fixture} | {name} | {} | {} | {} | {} | {} |",
                ms(median(&values.p50)),
                ms(median(&values.p99)),
                range(&values.p50, |v| ms(Some(v))),
                values.p50.len(),
                values.samples,
            );
        }
        let _ = writeln!(out);
    }
    for (kind, unit, values) in [("rate", "per second", &rates), ("gauge", "value", &gauges)] {
        if values.is_empty() {
            continue;
        }
        let _ = writeln!(
            out,
            "| fixture | {kind} | {unit} | range over runs | runs |"
        );
        let _ = writeln!(out, "|---|---|---|---|---|");
        for ((fixture, name), values) in values {
            let _ = writeln!(
                out,
                "| {fixture} | {name} | {} | {} | {} |",
                count(median(values)),
                range(values, |v| count(Some(v))),
                values.len()
            );
        }
        let _ = writeln!(out);
    }
    if !failed.is_empty() {
        let _ = writeln!(out, "Failed runs (excluded above):");
        for line in failed {
            let _ = writeln!(out, "- {line}");
        }
    }
    out
}

/// The provenance lines above the tables.
fn header(record: &RunRecord, selected: &dyn Fn(&str) -> bool) -> String {
    let env = &record.environment;
    let mut out = String::new();
    let labels: Vec<String> = record
        .arms
        .iter()
        .filter(|arm| selected(&arm.name))
        .map(|arm| {
            format!(
                "{} `{}` (binary sha256 {})",
                arm.name,
                arm.label,
                &arm.binary_sha256[..12.min(arm.binary_sha256.len())]
            )
        })
        .collect();
    let _ = writeln!(out, "- server: {}", labels.join("; "));
    let _ = writeln!(
        out,
        "- host `{}`: {} ({} CPUs, {} GB), kernel {}, governor {}, data on {}, server CPUs {}",
        env.hostname,
        env.cpu_model,
        env.logical_cpus,
        env.mem_total_kb / 1_048_576,
        env.kernel,
        env.cpu_governor,
        env.data_fs,
        env.server_cpus.as_deref().unwrap_or("unpinned"),
    );
    let _ = writeln!(
        out,
        "- profile `{}`, {} rounds per arm, load average start `{}`, end `{}`",
        record.workload.profile, record.workload.rounds, env.loadavg_start, env.loadavg_end
    );
    let _ = writeln!(out, "- build: {}", env.build);
    let _ = writeln!(out);
    out
}

fn ms(us: Option<f64>) -> String {
    match us {
        Some(us) if us >= 1_000_000.0 => format!("{:.2} s", us / 1_000_000.0),
        Some(us) => format!("{:.3} ms", us / 1000.0),
        None => "-".to_string(),
    }
}

fn count(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |v| format!("{v:.0}"))
}

fn range(values: &[f64], show: impl Fn(f64) -> String) -> String {
    let min = values.iter().copied().reduce(f64::min);
    let max = values.iter().copied().reduce(f64::max);
    match (min, max) {
        (Some(min), Some(max)) => format!("{}..{}", show(min), show(max)),
        _ => "-".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Arm, Environment, FixtureRun, Workload, SCHEMA};

    fn run(arm: &str, round: u32, samples: Vec<u64>, gauge: u64) -> FixtureRun {
        let mut run = FixtureRun {
            arm: arm.into(),
            round,
            fixture: "f".into(),
            ..FixtureRun::default()
        };
        run.series.insert("lat_us".into(), samples);
        run.gauges.insert("rss_kb".into(), gauge);
        run
    }

    fn record(runs: Vec<FixtureRun>) -> RunRecord {
        RunRecord {
            schema: SCHEMA.into(),
            created_unix: 0,
            environment: Environment::default(),
            workload: Workload {
                profile: "test".into(),
                rounds: 2,
                fixtures: vec!["f".into()],
                server_overrides: BTreeMap::new(),
                parameters: serde_json::Value::Null,
            },
            arms: ["baseline", "candidate"]
                .into_iter()
                .map(|name| Arm {
                    name: name.into(),
                    label: "abc".into(),
                    binary: String::new(),
                    binary_sha256: "0123456789abcdef".into(),
                })
                .collect(),
            runs,
        }
    }

    #[test]
    fn pools_arms_and_reports_medians_of_per_run_statistics() {
        let record = record(vec![
            run("baseline", 0, vec![1_000, 2_000, 3_000], 10),
            run("candidate", 0, vec![3_000, 4_000, 5_000], 20),
            run("baseline", 1, vec![5_000, 6_000, 7_000], 30),
        ]);
        let text = markdown(&record, &[]);
        // Per-run p50s 2, 4 and 6 ms; p99s 3, 5 and 7 ms.
        assert!(
            text.contains("| f | lat_us | 4.000 ms | 5.000 ms | 2.000 ms..6.000 ms | 3 | 9 |"),
            "{text}"
        );
        assert!(text.contains("| f | rss_kb | 20 | 10..30 | 3 |"), "{text}");
        // No rates were measured: no rate table.
        assert!(!text.contains("| rate |"), "{text}");
    }

    #[test]
    fn selects_arms_and_lists_failed_runs() {
        let mut failed = run("candidate", 1, vec![], 0);
        failed.error = Some("wrong result".into());
        let record = record(vec![
            run("baseline", 0, vec![1_000], 10),
            run("candidate", 0, vec![9_000], 90),
            failed,
        ]);
        let text = markdown(&record, &["baseline".to_string()]);
        assert!(text.contains("| f | lat_us | 1.000 ms |"), "{text}");
        assert!(!text.contains("wrong result"), "{text}");
        let all = markdown(&record, &[]);
        assert!(all.contains("f round 2 (candidate): wrong result"), "{all}");
    }
}
