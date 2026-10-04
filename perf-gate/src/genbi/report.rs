//! The human summary of a GenBI benchmark run: one row per category, the
//! reactive-path priority categories first, then every failure with its
//! reason and every finding. Counts only; expected rows are never shown.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use super::record::{BenchRecord, CheckStatus, Convergence, FindingKind, Reason, ScenarioRun};
use super::suite::Suite;
use super::PRIORITY;
use crate::stats::percentile;

/// Overall outcome of a run.
pub struct Verdict {
    pub passed: bool,
    /// Checks failing on the delta path itself.
    pub delta_path_failures: usize,
    /// Subscriptions that had not converged by a mutation's deadline.
    pub unconverged: usize,
    pub failures: usize,
    pub scenario_errors: usize,
}

pub fn verdict(record: &BenchRecord, strict: bool) -> Verdict {
    let checks = || record.scenarios.iter().flat_map(|s| &s.checks);
    let delta_path_failures = checks()
        .filter(|c| c.reason.is_some_and(Reason::is_delta_path))
        .count();
    let failures = checks().filter(|c| c.status != CheckStatus::Pass).count();
    let scenario_errors = record
        .scenarios
        .iter()
        .filter(|s| s.error.is_some())
        .count();
    let unconverged = record
        .scenarios
        .iter()
        .flat_map(|s| &s.mutations)
        .flat_map(|m| &m.subscriptions)
        .filter(|d| d.convergence == Convergence::Diverged)
        .count();
    let passed = delta_path_failures == 0
        && unconverged == 0
        && scenario_errors == 0
        && (!strict || failures == 0);
    Verdict {
        passed,
        delta_path_failures,
        unconverged,
        failures,
        scenario_errors,
    }
}

/// One stderr line per scenario run.
pub fn progress_line(run: &ScenarioRun) -> String {
    let passed = run
        .checks
        .iter()
        .filter(|c| c.status == CheckStatus::Pass)
        .count();
    let delta = run.series.get("delta_us").map_or(&[][..], Vec::as_slice);
    let error = run
        .error
        .as_deref()
        .map(|e| format!("  ERROR {e}"))
        .unwrap_or_default();
    format!(
        "{} r{}: {passed}/{} checks pass, {} deltas (p50 {}){error}",
        run.case_id,
        run.repetition,
        run.checks.len(),
        delta.len(),
        ms(percentile(delta, 0.5)),
    )
}

#[derive(Default)]
struct Group<'a> {
    runs: Vec<&'a ScenarioRun>,
}

impl Group<'_> {
    fn series(&self, name: &str) -> Vec<u64> {
        self.runs
            .iter()
            .filter_map(|r| r.series.get(name))
            .flatten()
            .copied()
            .collect()
    }

    fn checks(&self, retraction_only: bool) -> (usize, usize, usize) {
        let checks = self
            .runs
            .iter()
            .flat_map(|r| &r.checks)
            .filter(|c| !retraction_only || c.after_retraction);
        let (mut pass, mut total, mut not_run) = (0, 0, 0);
        for check in checks {
            total += 1;
            match check.status {
                CheckStatus::Pass => pass += 1,
                CheckStatus::NotRun => not_run += 1,
                CheckStatus::Fail => {}
            }
        }
        (pass, total, not_run)
    }

    fn row(&self, label: &str, name: &str) -> String {
        let (pass, total, not_run) = self.checks(false);
        let (rpass, rtotal, _) = self.checks(true);
        let rate = if total == 0 {
            "-".to_string()
        } else {
            format!("{:.0}%", 100.0 * pass as f64 / total as f64)
        };
        let peak = self
            .runs
            .iter()
            .filter_map(|r| r.gauges.get("peak_rss_kb"))
            .max()
            .map_or("-".to_string(), |kb| format!("{:.0}", *kb as f64 / 1024.0));
        let cases: std::collections::BTreeSet<&str> =
            self.runs.iter().map(|r| r.case_id.as_str()).collect();
        format!(
            "| {label} | {name} | {} | {pass}/{total} ({not_run} not run) | {rate} | {rpass}/{rtotal} | {} | {} | {} | {} | {} | {} | {} | {peak} |\n",
            cases.len(),
            p50_p99(&self.series("delta_insert_us")),
            p50_p99(&self.series("delta_retract_us")),
            p50_p99(&self.series("ack_to_delta_us")),
            p50_p99(&self.series("ack_us")),
            p50_p99(&self.series("requery_us")),
            ms(percentile(&self.series("subscribe_us"), 0.5)),
            ms(percentile(&self.series("load_us"), 0.5)),
        )
    }
}

const HEADER: &str = "| Cat | Name | Cases | Checks pass/total | Pass | Retraction checks | Insert→delta p50/p99 ms | Retract→delta p50/p99 ms | Ack→delta p50/p99 ms | Write ack p50/p99 ms | Re-query p50/p99 ms | Subscribe p50 ms | Cold load p50 ms | Peak RSS MB |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n";

pub fn markdown(record: &BenchRecord, suite: &Suite, verdict: &Verdict) -> String {
    let mut out = String::new();
    let mut groups: BTreeMap<&str, Group> = BTreeMap::new();
    for run in &record.scenarios {
        groups
            .entry(run.category.as_str())
            .or_default()
            .runs
            .push(run);
    }
    let config = &record.config;
    let _ = writeln!(out, "# GenBI-trust reactive agent benchmark\n");
    let _ = writeln!(
        out,
        "Suite {} ({} of {} cases) x {} repetition(s); {} agent(s) per scenario; server `{}` ({}); quiet {} ms, deadline {} ms; fault: {}; load {} -> {}.\n",
        record.suite.version,
        record.suite.cases_selected.len(),
        record.suite.cases_total,
        config.repeat,
        config.agents,
        config.server_label,
        &config.server_sha256[..config.server_sha256.len().min(12)],
        config.quiet_ms,
        config.deadline_ms,
        config.fault.map_or("none".to_string(), |f| format!("{f:?}")),
        record.environment.loadavg_start,
        record.environment.loadavg_end,
    );
    let _ = writeln!(
        out,
        "**{}**: {} check(s) failing on the delta path (agent answer differs from a fresh evaluation, or a subscription error), {} subscription(s) not converged by a deadline, {} check(s) not passing, {} scenario error(s).\n",
        if verdict.passed { "PASS" } else { "FAIL" },
        verdict.delta_path_failures,
        verdict.unconverged,
        verdict.failures,
        verdict.scenario_errors,
    );
    let _ = writeln!(
        out,
        "Insert/retract→delta: writer send to the delta that completes the agent's answer, split by whether the answer lost rows (the retraction checks are the checkpoints right after such a change). Ack→delta: the same deltas measured from the durable write ack, i.e. propagation alone. Re-query: the full re-evaluation a polling agent would run after the ack. Pass rates count not-run checks as not passing.\n"
    );
    let _ = writeln!(out, "## Reactive-path priority categories\n");
    out.push_str(HEADER);
    for id in PRIORITY {
        if let Some(group) = groups.get(id) {
            out.push_str(&group.row(id, suite.category_name(id)));
        }
    }
    let _ = writeln!(out, "\n## Other categories\n");
    out.push_str(HEADER);
    for (id, group) in &groups {
        if !PRIORITY.contains(id) {
            out.push_str(&group.row(id, suite.category_name(id)));
        }
    }
    let all = Group {
        runs: record.scenarios.iter().collect(),
    };
    out.push_str(&all.row("**All**", ""));
    let _ = writeln!(
        out,
        "\nAck→delta (propagation), all categories: first write after subscribing {}; later writes {}.",
        p50_p99(&all.series("ack_to_delta_first_us")),
        p50_p99(&all.series("ack_to_delta_later_us")),
    );
    failures(&mut out, record);
    findings(&mut out, record);
    out
}

/// Priority rank, case, phase, check, reason.
type FailureKey<'a> = (usize, &'a str, &'a str, &'a str, &'static str);

fn failures(out: &mut String, record: &BenchRecord) {
    let mut rows: BTreeMap<FailureKey, (usize, String)> = BTreeMap::new();
    for run in &record.scenarios {
        let rank = PRIORITY
            .iter()
            .position(|p| *p == run.category)
            .unwrap_or(PRIORITY.len());
        if let Some(error) = &run.error {
            rows.entry((rank, &run.case_id, "-", "-", "scenario error"))
                .or_insert((0, error.clone()))
                .0 += 1;
        }
        for check in run.checks.iter().filter(|c| c.status != CheckStatus::Pass) {
            let reason = check.reason.map_or("unknown", Reason::label);
            let diff = |d: Option<super::score::Diff>| {
                d.map_or("-".to_string(), |d| format!("-{} +{}", d.missing, d.extra))
            };
            let detail = if check.agent.is_none() && check.requery.is_none() {
                "-".to_string()
            } else {
                format!(
                    "agent {}, re-query {}",
                    diff(check.agent),
                    diff(check.requery)
                )
            };
            rows.entry((rank, &run.case_id, &check.phase, &check.check, reason))
                .or_insert((0, detail))
                .0 += 1;
        }
    }
    let _ = writeln!(out, "\n## Checks not passing\n");
    if rows.is_empty() {
        let _ = writeln!(out, "None.");
        return;
    }
    let _ = writeln!(
        out,
        "Diffs are missing/extra row counts against the expected checkpoint.\n"
    );
    let _ = writeln!(
        out,
        "| Case | Phase | Check | Reason | Detail | Runs |\n|---|---|---|---|---|---:|"
    );
    for ((_, case, phase, check, reason), (count, detail)) in rows {
        let _ = writeln!(
            out,
            "| {case} | {phase} | {check} | {reason} | {detail} | {count} |"
        );
    }
}

fn findings(out: &mut String, record: &BenchRecord) {
    // First repetition only: findings repeat identically per run.
    let mut by_kind: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for run in record.scenarios.iter().filter(|r| r.repetition == 1) {
        for finding in &run.findings {
            by_kind
                .entry(kind_label(finding.kind).to_string())
                .or_default()
                .push(format!(
                    "{}: {}",
                    run.case_id,
                    finding.detail.replace('|', "\\|")
                ));
        }
    }
    let _ = writeln!(out, "\n## Findings\n");
    if by_kind.is_empty() {
        let _ = writeln!(out, "None.");
        return;
    }
    for (kind, items) in by_kind {
        let _ = writeln!(out, "### {kind} ({})\n", items.len());
        for item in items {
            let _ = writeln!(out, "- {item}");
        }
        out.push('\n');
    }
}

fn kind_label(kind: FindingKind) -> &'static str {
    match kind {
        FindingKind::SeedStatementFailed => "Seed statements that failed to load",
        FindingKind::UnsupportedCheck => "Checks with no IQL translation",
        FindingKind::UnsupportedMutation => "Mutations with no IQL translation",
        FindingKind::MutationFailed => "Mutations the engine rejected",
        FindingKind::QueryFailed => "Standing queries that failed",
        FindingKind::DeltaAnomaly => "Delta anomalies",
    }
}

fn p50_p99(samples: &[u64]) -> String {
    if samples.is_empty() {
        return "-".to_string();
    }
    format!(
        "{} / {} (n={})",
        ms(percentile(samples, 0.5)),
        ms(percentile(samples, 0.99)),
        samples.len()
    )
}

fn ms(micros: Option<f64>) -> String {
    micros.map_or("-".to_string(), |us| format!("{:.2}", us / 1000.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::genbi::record::{CheckOutcome, Config, SuiteInfo, SCHEMA};

    fn record(reasons: &[Option<Reason>]) -> BenchRecord {
        let checks = reasons
            .iter()
            .map(|reason| CheckOutcome {
                phase: "initial".into(),
                check: "c".into(),
                status: if reason.is_some() {
                    CheckStatus::Fail
                } else {
                    CheckStatus::Pass
                },
                reason: *reason,
                after_retraction: false,
                agent: None,
                requery: None,
            })
            .collect();
        BenchRecord {
            schema: SCHEMA,
            created_unix: 0,
            environment: crate::schema::Environment::default(),
            suite: SuiteInfo {
                dir: String::new(),
                version: String::new(),
                cases_total: 1,
                cases_selected: vec!["X01-01".into()],
            },
            config: Config {
                server_label: String::new(),
                server_binary: String::new(),
                server_sha256: String::new(),
                server_overrides: BTreeMap::new(),
                repeat: 1,
                agents: 1,
                quiet_ms: 0,
                deadline_ms: 0,
                fault: None,
            },
            scenarios: vec![ScenarioRun {
                case_id: "X01-01".into(),
                checks,
                ..ScenarioRun::default()
            }],
        }
    }

    #[test]
    fn delta_path_failures_always_fail_the_run() {
        let run = record(&[None, Some(Reason::DeltaDivergence)]);
        assert!(!verdict(&run, false).passed);
        let run = record(&[Some(Reason::SubscriptionError)]);
        assert!(!verdict(&run, false).passed);
    }

    #[test]
    fn coverage_findings_fail_only_when_strict() {
        let run = record(&[
            None,
            Some(Reason::ResultMismatch),
            Some(Reason::Unsupported),
        ]);
        assert!(verdict(&run, false).passed);
        assert!(!verdict(&run, true).passed);
        assert_eq!(verdict(&run, true).failures, 2);
    }
}
