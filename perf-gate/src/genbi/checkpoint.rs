//! Scoring a phase's checkpoints: delta-path failures first (the agent's
//! answer must equal a fresh evaluation), then the evaluated answer against
//! the evaluator-private expected rows.

use std::collections::BTreeSet;

use serde_json::Value;

use super::agent::RowSet;
use super::record::{CheckOutcome, CheckStatus, Reason, ScenarioRun};
use super::scenario::{Live, Plan};
use super::score::{compare, Diff};
use super::suite::Expected;

/// Score `phase`'s expected checks; `retracting` names the questions whose
/// answer lost rows in this phase, `blocked` why the phase is undefined.
#[allow(clippy::too_many_arguments)]
pub(super) fn score(
    run: &mut ScenarioRun,
    scored: &mut BTreeSet<String>,
    phase: &str,
    expected: &Expected,
    plan: &Plan,
    live: &Live,
    retracting: &BTreeSet<String>,
    blocked: Option<Reason>,
) {
    scored.insert(phase.to_string());
    let Some(checks) = expected.phases.iter().find(|p| p.phase == phase) else {
        return;
    };
    for check in &checks.checks {
        let target = plan.by_check.get(&(phase.to_string(), check.name.clone()));
        let (status, reason, agent, requery, id) = match (blocked, target) {
            (Some(reason), _) => (CheckStatus::NotRun, Some(reason), None, None, None),
            (None, None) => (
                CheckStatus::NotRun,
                Some(Reason::NoQuestion),
                None,
                None,
                None,
            ),
            (None, Some(Err(_))) => (
                CheckStatus::NotRun,
                Some(Reason::Unsupported),
                None,
                None,
                None,
            ),
            (None, Some(Ok(id))) => {
                let (status, reason, agent, requery) = judge(id, plan, live, &check.expected_rows);
                (status, reason, agent, requery, Some(id))
            }
        };
        run.checks.push(CheckOutcome {
            phase: phase.to_string(),
            check: check.name.clone(),
            status,
            reason,
            after_retraction: id.is_some_and(|id| retracting.contains(id)),
            agent,
            requery,
        });
    }
}

/// Score one answered check: delta-path failures first, then the result.
fn judge(
    id: &str,
    plan: &Plan,
    live: &Live,
    expected: &[Vec<Value>],
) -> (CheckStatus, Option<Reason>, Option<Diff>, Option<Diff>) {
    let fail = |reason, agent, requery| (CheckStatus::Fail, Some(reason), agent, requery);
    let Some(standing) = plan.questions.iter().find(|(q, _)| q == id).map(|(_, s)| s) else {
        return fail(Reason::QueryFailed, None, None);
    };
    let (Some(truth), false) = (live.truth.get(id), live.failed.contains_key(id)) else {
        return fail(Reason::QueryFailed, None, None);
    };
    let project = |rows: &RowSet| -> Vec<Vec<Value>> {
        rows.values().map(|row| standing.project(row)).collect()
    };
    let requery = compare(&project(truth), expected);
    let mut agent_diff = None;
    let mut reason: Option<Reason> = None;
    for agent in &live.agents {
        let Some(subscription) = agent.subscriptions.get(id) else {
            continue;
        };
        let diff = compare(&project(&subscription.state), expected);
        agent_diff = Some(agent_diff.map_or(diff, |d: Diff| if d.is_equal() { diff } else { d }));
        let this = if subscription.error.is_some() {
            Some(Reason::SubscriptionError)
        } else if subscription.state.keys().ne(truth.keys()) {
            Some(Reason::DeltaDivergence)
        } else {
            None
        };
        reason = match (reason, this) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
    }
    if let Some(reason) = reason {
        return fail(reason, agent_diff, Some(requery));
    }
    if !requery.is_equal() {
        return fail(Reason::ResultMismatch, agent_diff, Some(requery));
    }
    (CheckStatus::Pass, None, agent_diff, Some(requery))
}
