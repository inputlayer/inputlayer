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
    let _ = writeln!(
        out,
        "| fixture | rate | per second | range over runs | runs |"
    );
    let _ = writeln!(out, "|---|---|---|---|---|");
    for ((fixture, name), values) in &rates {
        let _ = writeln!(
            out,
            "| {fixture} | {name} | {} | {} | {} |",
            count(median(values)),
            range(values, |v| count(Some(v))),
            values.len()
        );
    }
    let _ = writeln!(out);
    let _ = writeln!(out, "| fixture | gauge | value | range over runs | runs |");
    let _ = writeln!(out, "|---|---|---|---|---|");
    for ((fixture, name), values) in &gauges {
        let _ = writeln!(
            out,
            "| {fixture} | {name} | {} | {} | {} |",
            count(median(values)),
            range(values, |v| count(Some(v))),
            values.len()
        );
    }
    if !failed.is_empty() {
        let _ = writeln!(out);
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
