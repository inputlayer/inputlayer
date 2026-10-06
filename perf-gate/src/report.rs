//! Markdown rendering of a verdict, suitable for a PR record.

use std::fmt::Write;

use crate::compare::{MetricVerdict, Status, Verdict};
use crate::policy::Policy;
use crate::schema::{Environment, RunRecord};

pub fn markdown(record: &RunRecord, policy: &Policy, verdict: &Verdict) -> String {
    let mut out = String::new();
    let env = &record.environment;
    let _ = writeln!(
        out,
        "## Performance gate: {}\n",
        status_word(verdict.status)
    );
    let _ = writeln!(
        out,
        "Policy **{}**: cost budget p50 +{:.0}%, p99 +{:.0}%, throughput -{:.0}%; \
         {:.0}% interval of the median paired per-round ratio. {}\n",
        policy.status,
        policy.tolerance.p50 * 100.0,
        policy.tolerance.p99 * 100.0,
        policy.tolerance.rate * 100.0,
        policy.confidence * 100.0,
        policy.note
    );
    for arm in &record.arms {
        let sha = arm.binary_sha256.get(..12).unwrap_or(&arm.binary_sha256);
        let _ = writeln!(out, "- {}: `{}` (binary sha256 {sha})", arm.name, arm.label);
    }
    let _ = writeln!(
        out,
        "- profile `{}`, {} rounds, fixtures {}",
        record.workload.profile,
        record.workload.rounds,
        record.workload.fixtures.join(", ")
    );
    let _ = writeln!(
        out,
        "- host `{}`: {} ({} CPUs, {} GB), kernel {}, governor {}, data on {} ({}), server CPUs {}",
        env.hostname,
        env.cpu_model,
        env.logical_cpus,
        env.mem_total_kb / 1_048_576,
        env.kernel,
        env.cpu_governor,
        env.data_fs,
        env.data_root,
        env.server_cpus.as_deref().unwrap_or("unpinned")
    );
    let _ = writeln!(
        out,
        "- load average: start `{}`, end `{}`",
        env.loadavg_start, env.loadavg_end
    );
    if let Some(load) = busiest_load(env).filter(|l| *l > env.logical_cpus as f64 / 2.0) {
        let _ = writeln!(
            out,
            "- **busy host**: 1-minute load {load:.0} on {} CPUs; results carry other work's noise",
            env.logical_cpus
        );
    }
    if !env.build.is_empty() {
        let _ = writeln!(out, "- build: {}", env.build);
    }
    if !verdict.problems.is_empty() {
        let _ = writeln!(out, "\n### Problems\n");
        for problem in &verdict.problems {
            let _ = writeln!(out, "- {problem}");
        }
    }
    let _ = writeln!(out, "\n### Required metrics\n");
    table(&mut out, verdict.metrics.iter().filter(|m| m.required));
    if !verdict.ceilings.is_empty() {
        let _ = writeln!(out, "\n### Ceilings (absolute)\n");
        let _ = writeln!(out, "| metric | candidate | ceiling | status |");
        let _ = writeln!(out, "|---|---|---|---|");
        for c in &verdict.ceilings {
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} |",
                c.metric,
                value(c.candidate_median, false),
                value(Some(c.ceiling), false),
                if c.status == Status::Pass {
                    status_word(c.status).to_string()
                } else {
                    format!("{}: {}", status_word(c.status), c.reason)
                }
            );
        }
    }
    let _ = writeln!(out, "\n### Diagnostic metrics (not gated)\n");
    table(&mut out, verdict.metrics.iter().filter(|m| !m.required));
    out
}

/// Highest 1-minute load average recorded at the start or end of the run.
fn busiest_load(env: &Environment) -> Option<f64> {
    [&env.loadavg_start, &env.loadavg_end]
        .iter()
        .filter_map(|l| l.split_whitespace().next()?.parse::<f64>().ok())
        .reduce(f64::max)
}

fn table<'a>(out: &mut String, metrics: impl Iterator<Item = &'a MetricVerdict>) {
    let _ = writeln!(
        out,
        "| metric | baseline | candidate | cost ratio | interval | budget | baseline spread | status |"
    );
    let _ = writeln!(out, "|---|---|---|---|---|---|---|---|");
    for m in metrics {
        let rate = m.metric.ends_with("_per_sec");
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {:.2} | {} | {} |",
            m.metric,
            value(m.baseline_median, rate),
            value(m.candidate_median, rate),
            m.cost_ratio.map_or("-".into(), |r| format!("{r:.3}")),
            m.interval
                .map_or("-".into(), |(lo, hi)| format!("{lo:.3}..{hi:.3}")),
            1.0 + m.tolerance,
            m.baseline_spread
                .map_or("-".into(), |s| format!("{:.1}%", s * 100.0)),
            if m.status == Status::Invalid {
                format!("INVALID: {}", m.reason)
            } else {
                status_word(m.status).to_string()
            }
        );
    }
}

fn value(v: Option<f64>, rate: bool) -> String {
    match v {
        None => "-".into(),
        Some(v) if rate => format!("{v:.0}/s"),
        Some(v) => format!("{:.3} ms", v / 1000.0),
    }
}

pub fn status_word(status: Status) -> &'static str {
    match status {
        Status::Pass => "PASS",
        Status::Inconclusive => "INCONCLUSIVE",
        Status::Fail => "FAIL",
        Status::Invalid => "INVALID",
    }
}
