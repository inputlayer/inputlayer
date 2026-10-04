//! One scenario on one fresh server: cold load, standing subscriptions, the
//! ordered mutations from an independent writer, and a score per checkpoint.
//!
//! Per mutation the writer sends the whole translated program and waits for
//! the acknowledgement; agents apply pushes until they go quiet; the writer
//! then re-queries every question (the full re-query baseline, and the truth
//! the delta path must match); agents keep applying pushes until their
//! answers match that truth or the deadline passes.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use super::agent::{row_set, Agent, Fault, RowSet};
use super::checkpoint::score;
use super::record::{Convergence, DeltaOutcome, FindingKind, MutationRun, Reason, ScenarioRun};
use super::suite::{Case, Expected, Mutation, Scenario, Seed, Suite, INITIAL_PHASE};
use super::translate::{Catalog, Change, Standing, Unsupported};
use crate::client::Client;
use crate::fixtures::{elapsed_us, micros};
use crate::schema::Rate;
use crate::server::{RunningServer, ServerSpec};

/// Knowledge graph each scenario is loaded into.
const KG: &str = "genbi";

/// Seed load failures listed individually per scenario (all are counted).
const MAX_LISTED_FAILURES: usize = 20;

#[derive(Debug, Clone, Copy)]
pub struct Options {
    /// Agents settle once no push arrives for this long after the ack.
    pub quiet: Duration,
    /// Longest wait, from the write, for agents to converge.
    pub deadline: Duration,
    pub agents: usize,
    pub fault: Option<Fault>,
}

/// Run `case` on a fresh server started from `spec` under `root`.
pub async fn run(
    spec: &ServerSpec,
    root: &std::path::Path,
    suite: &Suite,
    case: &Case,
    repetition: u32,
    options: Options,
) -> ScenarioRun {
    let mut run = ScenarioRun {
        case_id: case.id.clone(),
        category: case.category.clone(),
        repetition,
        ..ScenarioRun::default()
    };
    let name = format!("{}-r{repetition}", case.id);
    let result = async {
        let scenario = suite.scenario(case)?;
        let expected = suite.expected(case)?;
        let server = RunningServer::start(spec, root, &name).await?;
        let outcome = drive(&mut run, &server, &scenario, &expected, options).await;
        if let Some(peak) = server.peak_rss_kb() {
            run.gauges.insert("peak_rss_kb".into(), peak);
        }
        outcome
    }
    .await;
    if let Err(error) = result {
        run.error = Some(format!("{error:#}"));
    }
    run
}

/// The scenario's distinct questions and where each phase's checks point.
pub(super) struct Plan {
    pub(super) questions: Vec<(String, Standing)>,
    pub(super) by_check: BTreeMap<(String, String), Result<String, Unsupported>>,
}

/// Mutable state carried from phase to phase.
pub(super) struct Live {
    writer: Client,
    pub(super) agents: Vec<Agent>,
    /// The engine's current answer per question, from the last re-query.
    pub(super) truth: BTreeMap<String, RowSet>,
    /// Questions that failed to evaluate, with the error.
    pub(super) failed: BTreeMap<String, String>,
}

async fn drive(
    run: &mut ScenarioRun,
    server: &RunningServer,
    scenario: &Scenario,
    expected: &Expected,
    options: Options,
) -> Result<()> {
    let mut admin = server.client("default").await?;
    admin.execute(&format!(".kg create {KG}")).await?;
    let mut writer = server.client(KG).await?;
    load(run, &mut writer, &scenario.seed).await?;
    gauge_rss(run, server, "rss_after_load_kb");

    let plan = plan(run, &Catalog::new(&scenario.seed), scenario);
    let mut live = Live {
        writer,
        agents: Vec::new(),
        truth: BTreeMap::new(),
        failed: BTreeMap::new(),
    };
    live.truth = requery(run, &mut live, &plan, "initial_query_us").await?;
    let subscribed: Vec<(String, String)> = plan
        .questions
        .iter()
        .filter(|(id, _)| !live.failed.contains_key(id))
        .map(|(id, s)| (id.clone(), s.query.clone()))
        .collect();
    run.gauges
        .insert("subscriptions".into(), subscribed.len() as u64);
    for _ in 0..options.agents {
        let client = server.client(KG).await?;
        let (agent, latencies) = Agent::start(client, &subscribed, options.fault)
            .await
            .context("subscribe agent")?;
        for micros in latencies {
            run.sample("subscribe_us", micros);
        }
        live.agents.push(agent);
    }
    gauge_rss(run, server, "rss_after_subscribe_kb");

    let mut scored = BTreeSet::new();
    score(
        run,
        &mut scored,
        INITIAL_PHASE,
        expected,
        &plan,
        &live,
        &BTreeSet::new(),
        None,
    );
    let mut blocked = None;
    for mutation in &scenario.mutations {
        let mut retracting = BTreeSet::new();
        if blocked.is_none() {
            match apply(run, &mut live, &plan, mutation, &scenario.seed, options).await? {
                Ok(lost) => retracting = lost,
                Err(reason) => blocked = Some(reason),
            }
        }
        score(
            run,
            &mut scored,
            &mutation.label,
            expected,
            &plan,
            &live,
            &retracting,
            blocked,
        );
    }
    // Expected phases that matched no public mutation label.
    for phase in &expected.phases {
        if !scored.contains(&phase.phase) {
            score(
                run,
                &mut scored,
                &phase.phase,
                expected,
                &plan,
                &live,
                &BTreeSet::new(),
                Some(Reason::NoQuestion),
            );
        }
    }
    for agent in &live.agents {
        let anomalies: usize = agent.subscriptions.values().map(|s| s.anomalies).sum();
        if anomalies > 0 {
            run.finding(
                FindingKind::DeltaAnomaly,
                format!("{anomalies} retraction(s) of absent rows or duplicate inserts"),
            );
        }
    }
    gauge_rss(run, server, "rss_end_kb");
    Ok(())
}

/// Load the seed, one request per [`Seed::batches`] entry: the cold-load
/// measurement. Separate requests keep every failure attributable and no
/// single request near the server's execution timeout.
async fn load(run: &mut ScenarioRun, writer: &mut Client, seed: &Seed) -> Result<()> {
    let mut elapsed = 0;
    let mut failed = 0;
    for (index, statement) in seed.batches() {
        let started = Instant::now();
        let (_, answer) = writer.query(&statement).await.context("load seed")?;
        elapsed += micros(started.elapsed());
        let Some(error) = answer.errors.first() else {
            continue;
        };
        failed += 1;
        if failed <= MAX_LISTED_FAILURES {
            let preview: String = statement.chars().take(80).collect();
            run.finding(
                FindingKind::SeedStatementFailed,
                format!("statement {index} `{preview}`: {}", message(error)),
            );
        }
    }
    run.sample("load_us", elapsed);
    run.rates.insert(
        "load_statements".into(),
        Rate {
            ops: seed.statements.len() as u64,
            elapsed_us: elapsed,
        },
    );
    run.gauges
        .insert("seed_statements".into(), seed.statements.len() as u64);
    run.gauges.insert("seed_failed".into(), failed as u64);
    Ok(())
}

fn plan(run: &mut ScenarioRun, catalog: &Catalog, scenario: &Scenario) -> Plan {
    let mut plan = Plan {
        questions: Vec::new(),
        by_check: BTreeMap::new(),
    };
    let mut by_sql: BTreeMap<&str, Result<String, Unsupported>> = BTreeMap::new();
    let phases = std::iter::once((INITIAL_PHASE, &scenario.questions)).chain(
        scenario
            .mutations
            .iter()
            .map(|m| (m.label.as_str(), &m.queries)),
    );
    for (phase, questions) in phases {
        for question in questions {
            let target = by_sql
                .entry(&question.sql)
                .or_insert_with(|| match catalog.standing(&question.sql) {
                    Ok(standing) => {
                        let id = format!("q{}", plan.questions.len());
                        plan.questions.push((id.clone(), standing));
                        Ok(id)
                    }
                    Err(unsupported) => {
                        run.finding(
                            FindingKind::UnsupportedCheck,
                            format!("{}: {unsupported}", question.name),
                        );
                        Err(unsupported)
                    }
                })
                .clone();
            plan.by_check
                .insert((phase.to_string(), question.name.clone()), target);
        }
    }
    plan
}

/// Apply one mutation and measure it. `Ok(Err(reason))` means it could not
/// be applied and later phases are undefined; `Ok(Ok(lost))` names the
/// questions whose answer lost rows.
async fn apply(
    run: &mut ScenarioRun,
    live: &mut Live,
    plan: &Plan,
    mutation: &Mutation,
    seed: &Seed,
    options: Options,
) -> Result<Result<BTreeSet<String>, Reason>> {
    let program = match compile(run, live, mutation, seed).await? {
        Ok(program) => program,
        Err(reason) => return Ok(Err(reason)),
    };
    let mut record = MutationRun {
        label: mutation.label.clone(),
        statements: program.len(),
        ack_us: None,
        error: None,
        subscriptions: Vec::new(),
    };
    for agent in &mut live.agents {
        agent.clear_errors(&live.truth);
        agent.take_activity();
    }
    let start = if program.is_empty() {
        None
    } else {
        let (start, answer) = live.writer.query(&program.join("\n")).await?;
        let ack = elapsed_us(start, answer.at);
        record.ack_us = Some(ack);
        run.sample("ack_us", ack);
        if let Some(error) = answer.errors.first() {
            let error = message(error);
            run.finding(
                FindingKind::MutationFailed,
                format!("{}: {error}", mutation.label),
            );
            record.error = Some(error);
            run.mutations.push(record);
            return Ok(Err(Reason::MutationNotApplied));
        }
        Some(start)
    };

    let deadline = tokio::time::Instant::now() + options.deadline;
    for agent in &mut live.agents {
        agent.drain_quiet(options.quiet, deadline).await?;
    }
    let truth = requery(run, live, plan, "requery_us").await?;
    for agent in &mut live.agents {
        agent
            .drain_until(deadline, |a| a.diverged(&truth, &live.failed).is_empty())
            .await?;
    }
    let lost = record_deltas(
        run,
        &mut live.agents,
        &live.truth,
        &live.failed,
        &truth,
        start,
        &mut record,
    );
    live.truth = truth;
    run.mutations.push(record);
    Ok(Ok(lost))
}

/// The IQL program of a mutation. Reading the rows an `UPDATE` rewrites is
/// preparation, not part of the timed write.
async fn compile(
    run: &mut ScenarioRun,
    live: &mut Live,
    mutation: &Mutation,
    seed: &Seed,
) -> Result<Result<Vec<String>, Reason>> {
    let changes = match Catalog::new(seed).mutation(&mutation.sql) {
        Ok(changes) => changes,
        Err(unsupported) => {
            run.finding(
                FindingKind::UnsupportedMutation,
                format!("{}: {unsupported}", mutation.label),
            );
            return Ok(Err(Reason::MutationNotApplied));
        }
    };
    let mut program = Vec::new();
    for change in changes {
        match change {
            Change::Program(statements) => program.extend(statements),
            Change::Update(update) => {
                let (_, current) = live.writer.query(&update.read).await?;
                if let Some(error) = current.errors.first() {
                    run.finding(
                        FindingKind::MutationFailed,
                        format!(
                            "{}: reading rows to update: {}",
                            mutation.label,
                            message(error)
                        ),
                    );
                    return Ok(Err(Reason::MutationNotApplied));
                }
                program.extend(update.program(&current.rows));
            }
        }
    }
    Ok(Ok(program))
}

/// Evaluate every live question from the writer: the engine's current
/// answers. Latencies go to `series`; failures are recorded and retired.
async fn requery(
    run: &mut ScenarioRun,
    live: &mut Live,
    plan: &Plan,
    series: &str,
) -> Result<BTreeMap<String, RowSet>> {
    let mut truth = BTreeMap::new();
    for (id, standing) in &plan.questions {
        if live.failed.contains_key(id) {
            continue;
        }
        match evaluate(&mut live.writer, standing).await? {
            Ok((micros, rows)) => {
                run.sample(series, micros);
                truth.insert(id.clone(), rows);
            }
            Err(error) => {
                run.finding(
                    FindingKind::QueryFailed,
                    format!("{}: {error}", standing.query),
                );
                live.failed.insert(id.clone(), error);
            }
        }
    }
    Ok(truth)
}

/// Record what every subscription saw for the write sent at `start`, against
/// the new `truth` and the answers `before` it; `retired` questions no longer
/// count toward convergence. Returns the questions whose answer lost rows.
fn record_deltas(
    run: &mut ScenarioRun,
    agents: &mut [Agent],
    before: &BTreeMap<String, RowSet>,
    retired: &BTreeMap<String, String>,
    truth: &BTreeMap<String, RowSet>,
    start: Option<Instant>,
    record: &mut MutationRun,
) -> BTreeSet<String> {
    let lost: BTreeSet<String> = truth
        .iter()
        .filter(|(id, now)| {
            before
                .get(*id)
                .is_some_and(|before| before.keys().any(|k| !now.contains_key(k)))
        })
        .map(|(id, _)| id.clone())
        .collect();
    // Per subscription: converged, and when its last delta frame arrived.
    let mut arrivals = Vec::new();
    for (index, agent) in agents.iter_mut().enumerate() {
        let diverged = agent.diverged(truth, retired);
        for (id, activity) in agent.take_activity() {
            let answer_changed = before.get(&id) != truth.get(&id);
            let delta_us = match (start, activity.last) {
                (Some(sent), Some(arrived)) if activity.frames > 0 => {
                    Some(elapsed_us(sent, arrived))
                }
                _ => None,
            };
            let convergence = if retired.contains_key(&id) {
                Convergence::Retired
            } else if diverged.contains(&id) {
                Convergence::Diverged
            } else {
                Convergence::Converged
            };
            let converged = convergence == Convergence::Converged;
            if convergence != Convergence::Retired {
                arrivals.push((converged, activity.last.filter(|_| activity.frames > 0)));
            }
            if let (true, true, Some(micros)) = (answer_changed, converged, delta_us) {
                run.sample("delta_us", micros);
                // Propagation after the durable commit was acknowledged.
                let ack = record.ack_us.unwrap_or(0);
                let propagation = micros.saturating_sub(ack);
                run.sample("ack_to_delta_us", propagation);
                // The first refresh after subscribing is reported apart.
                let first_write = !run.mutations.iter().any(|m| m.ack_us.is_some());
                run.sample(
                    if first_write {
                        "ack_to_delta_first_us"
                    } else {
                        "ack_to_delta_later_us"
                    },
                    propagation,
                );
                let series = if lost.contains(&id) {
                    "delta_retract_us"
                } else {
                    "delta_insert_us"
                };
                run.sample(series, micros);
            }
            record.subscriptions.push(DeltaOutcome {
                subscription: id.clone(),
                agent: index,
                changed: answer_changed,
                retracting: lost.contains(&id),
                delta_us,
                frames: activity.frames,
                rows_inserted: activity.inserted,
                rows_retracted: activity.retracted,
                convergence,
            });
        }
    }
    if let Some(elapsed) = start.and_then(|sent| convergence_time(sent, &arrivals)) {
        let rate = run
            .rates
            .entry("converged_mutations".into())
            .or_insert(Rate {
                ops: 0,
                elapsed_us: 0,
            });
        rate.ops += 1;
        rate.elapsed_us += micros(elapsed);
    }
    lost
}

/// Time from sending a mutation until every subscription converged: the
/// latest delta arrival, or zero when no delta was due. `None` when any
/// subscription diverged, or when there was no live subscription to measure:
/// that mutation did not converge.
fn convergence_time(sent: Instant, arrivals: &[(bool, Option<Instant>)]) -> Option<Duration> {
    if arrivals.is_empty() || arrivals.iter().any(|(converged, _)| !converged) {
        return None;
    }
    let end = arrivals.iter().filter_map(|(_, last)| *last).max();
    Some(end.map_or(Duration::ZERO, |end| end.saturating_duration_since(sent)))
}

/// Evaluate a standing query once: its latency and rows, or the engine's
/// error. Only a broken connection is an `Err`.
async fn evaluate(
    writer: &mut Client,
    standing: &Standing,
) -> Result<Result<(u64, RowSet), String>> {
    let (start, answer) = writer.query(&standing.query).await?;
    Ok(if let Some(error) = answer.errors.first() {
        Err(message(error))
    } else if answer.truncated {
        Err("result truncated".into())
    } else {
        Ok((elapsed_us(start, answer.at), row_set(answer.rows)))
    })
}

/// The message of a statement error the server reported.
fn message(error: &serde_json::Value) -> String {
    error["message"]
        .as_str()
        .map_or_else(|| error.to_string(), ToString::to_string)
}

fn gauge_rss(run: &mut ScenarioRun, server: &RunningServer, name: &str) {
    if let Some(rss) = server.rss_kb() {
        run.gauges.insert(name.to_string(), rss);
    }
}

#[cfg(test)]
mod tests {
    use super::super::agent::Subscription;
    use super::*;
    use serde_json::json;

    fn answers(entries: &[(&str, serde_json::Value)]) -> BTreeMap<String, RowSet> {
        entries
            .iter()
            .map(|(id, rows)| {
                (
                    id.to_string(),
                    row_set(serde_json::from_value(rows.clone()).unwrap()),
                )
            })
            .collect()
    }

    fn mutation(label: &str) -> MutationRun {
        MutationRun {
            label: label.into(),
            statements: 1,
            ack_us: Some(1),
            error: None,
            subscriptions: Vec::new(),
        }
    }

    /// Deliver a delta inserting `row` into subscription `id`.
    fn deliver(agent: &mut Agent, id: &str, row: serde_json::Value) {
        let sub = agent.subscriptions.get_mut(id).unwrap();
        sub.apply(Instant::now(), vec![vec![row]], vec![], None);
    }

    #[tokio::test]
    async fn later_mutations_converge_after_a_question_is_retired() {
        let mut agents = vec![Agent::holding(vec![
            Subscription::new("q0", vec![vec![json!(1)]]),
            Subscription::new("q3", vec![vec![json!(7)]]),
        ])];
        let mut run = ScenarioRun::default();
        let mut retired = BTreeMap::new();
        let mut before = answers(&[("q0", json!([[1]])), ("q3", json!([[7]]))]);

        // m1: both questions evaluate; both answers converge.
        deliver(&mut agents[0], "q0", json!(2));
        deliver(&mut agents[0], "q3", json!(8));
        let truth = answers(&[("q0", json!([[1], [2]])), ("q3", json!([[7], [8]]))]);
        let mut m1 = mutation("m1");
        record_deltas(
            &mut run,
            &mut agents,
            &before,
            &retired,
            &truth,
            Some(Instant::now()),
            &mut m1,
        );
        assert!(m1
            .subscriptions
            .iter()
            .all(|d| d.convergence == Convergence::Converged));
        before = truth;

        // m2 and m3: q3's re-query fails, so it is retired; its subscription
        // errors and goes stale, and q0 still converges and counts.
        retired.insert("q3".to_string(), "result truncated".to_string());
        agents[0].subscriptions.get_mut("q3").unwrap().error = Some("result truncated".into());
        for (label, row) in [("m2", 3), ("m3", 4)] {
            deliver(&mut agents[0], "q0", json!(row));
            let mut truth = before.clone();
            truth.remove("q3");
            let q0 = truth.get_mut("q0").unwrap();
            q0.insert(json!([row]).to_string(), vec![json!(row)]);
            assert!(agents[0].diverged(&truth, &retired).is_empty());
            let mut outcome = mutation(label);
            record_deltas(
                &mut run,
                &mut agents,
                &before,
                &retired,
                &truth,
                Some(Instant::now()),
                &mut outcome,
            );
            let q3 = outcome
                .subscriptions
                .iter()
                .find(|d| d.subscription == "q3")
                .unwrap();
            assert_eq!(q3.convergence, Convergence::Retired);
            let q0 = outcome
                .subscriptions
                .iter()
                .find(|d| d.subscription == "q0")
                .unwrap();
            assert_eq!(q0.convergence, Convergence::Converged);
            run.mutations.push(outcome);
            before = truth;
        }
        assert_eq!(run.rates["converged_mutations"].ops, 3);
    }

    #[tokio::test]
    async fn a_mutation_with_every_question_retired_is_not_counted() {
        let mut agents = vec![Agent::holding(vec![Subscription::new(
            "q0",
            vec![vec![json!(1)]],
        )])];
        let mut run = ScenarioRun::default();
        let retired = BTreeMap::from([("q0".to_string(), "result truncated".to_string())]);
        let before = answers(&[("q0", json!([[1]]))]);
        deliver(&mut agents[0], "q0", json!(2));
        let mut outcome = mutation("m1");
        record_deltas(
            &mut run,
            &mut agents,
            &before,
            &retired,
            &BTreeMap::new(),
            Some(Instant::now()),
            &mut outcome,
        );
        assert_eq!(outcome.subscriptions[0].convergence, Convergence::Retired);
        assert!(!run.rates.contains_key("converged_mutations"));
        assert_eq!(convergence_time(Instant::now(), &[]), None);
    }

    #[test]
    fn a_divergent_subscription_means_the_mutation_did_not_converge() {
        let sent = Instant::now();
        let arrivals = [
            (true, Some(sent + Duration::from_millis(5))),
            (false, Some(sent + Duration::from_millis(9))),
        ];
        assert_eq!(convergence_time(sent, &arrivals), None);
    }

    #[test]
    fn convergence_time_is_the_latest_arrival_of_converged_subscriptions() {
        let sent = Instant::now();
        let arrivals = [
            (true, Some(sent + Duration::from_millis(5))),
            (true, None),
            (true, Some(sent + Duration::from_millis(9))),
        ];
        assert_eq!(
            convergence_time(sent, &arrivals),
            Some(Duration::from_millis(9))
        );
        assert_eq!(
            convergence_time(sent, &[(true, None)]),
            Some(Duration::ZERO)
        );
    }
}
