//! The scenarios' shared assertion vocabulary (test strategy, Section 7.3).
//!
//! `rows_equal_fresh_query` is `View::assert_matches` against an auditor's
//! query, `delta_exact` is `Delta::assert_rows`, and `seq_contiguous` /
//! `revision_increasing` are checked by `Agent` on every delta it applies;
//! this module adds what the testkit does not have.

use std::collections::BTreeSet;
use std::time::Duration;

use inputlayer_testkit::{
    Agent, Checked, Counters, Delta, KnownDefect, QueryResult, Refusal, Reproduction, Violation,
    WsClient,
};
use serde_json::Value;

/// Longest a delta may take to reach an agent another connection's refusal
/// or disconnect must not hold up.
pub const DELTA_DEADLINE: Duration = Duration::from_secs(5);

/// How long a subscription must stay silent to count as quiet.
pub const QUIET: Duration = Duration::from_millis(300);

/// The committed result of a write, or the refusal as a violation.
pub fn committed(reply: Result<QueryResult, Refusal>) -> Checked<QueryResult> {
    let result = reply.map_err(|r| Violation::Rejected(format!("{r:?}")))?;
    if result.errors.is_empty() {
        Ok(result)
    } else {
        Err(Violation::Rejected(format!("{:?}", result.errors)))
    }
}

/// The revision a committed write's reply names.
pub fn write_revision(result: &QueryResult) -> Checked<u64> {
    result
        .revision
        .ok_or_else(|| Violation::Transport(format!("write reply names no revision: {result:?}")))
}

/// `write_revision_matches_delta`: the delta a write causes names the
/// revision the write's reply did.
pub fn write_revision_matches_delta(write: &QueryResult, delta: &Delta) -> Checked<()> {
    let revision = write_revision(write)?;
    if delta.revision == revision {
        return Ok(());
    }
    Err(Violation::WrongDelta {
        subscription: delta.subscription.clone(),
        detail: format!(
            "delta seq {} names revision {}, the write that caused it committed at {revision}",
            delta.seq, delta.revision
        ),
    })
}

/// `refused(reply, code, message_contains)`: the request failed with the
/// structured `code` and a message naming the reason.
///
/// A refusal with the right message but without the code fails with
/// [`Violation::Rejected`] whose text starts `missing code`, which a known
/// defect can match on.
pub fn refused(
    reply: Result<QueryResult, Refusal>,
    code: &str,
    message_contains: &str,
) -> Checked<Refusal> {
    let refusal = match reply {
        Err(refusal) => refusal,
        Ok(result) if !result.errors.is_empty() => {
            return Err(Violation::Rejected(format!(
                "refused per statement, not as a request: {:?}",
                result.errors
            )))
        }
        Ok(result) => {
            return Err(Violation::Transport(format!(
                "expected a {code} refusal naming {message_contains:?}, the request succeeded: \
                 {:?}",
                result.rows
            )))
        }
    };
    if !refusal.message.contains(message_contains) {
        return Err(Violation::Transport(format!(
            "refusal does not name {message_contains:?}: {refusal:?}"
        )));
    }
    match refusal.code.as_deref() {
        Some(c) if c == code => Ok(refusal),
        Some(other) => Err(Violation::Transport(format!(
            "refused with code {other}, expected {code}: {}",
            refusal.message
        ))),
        None => Err(Violation::Rejected(format!(
            "missing code: expected {code} on {:?}",
            refusal.message
        ))),
    }
}

/// `others_unaffected(agents)`: each agent receives the next delta of its
/// subscription within [`DELTA_DEADLINE`], whatever happened to another
/// connection meanwhile. Returns the deltas in order.
pub async fn others_unaffected(agents: &mut [(&mut Agent, &str)]) -> Checked<Vec<Delta>> {
    let mut deltas = Vec::with_capacity(agents.len());
    for (agent, subscription) in agents.iter_mut() {
        let delta = tokio::time::timeout(DELTA_DEADLINE, agent.next_delta(subscription))
            .await
            .map_err(|_| {
                Violation::Timeout(format!(
                    "'{subscription}' delta within {DELTA_DEADLINE:?} while another connection \
                     was refused or disconnected"
                ))
            })??;
        deltas.push(delta);
    }
    Ok(deltas)
}

/// Rows of a fresh query on another connection.
pub async fn fresh(auditor: &mut WsClient, query: &str) -> Checked<Vec<Value>> {
    Ok(auditor.query(query).await?.rows)
}

/// The engine does not export the view counters yet (V1 #308).
pub const NO_VIEW_COUNTERS: KnownDefect = KnownDefect {
    plan_item: "#308",
    summary: "rule_evaluations / view_reads / view_maintenance_us are not exported",
    signature: |v| matches!(v, Violation::NotMeasurable(_)),
    reproduction: Reproduction::Deterministic,
};

/// `no_rule_evaluations(counters.delta)`: across `delta` no deployed rule was
/// evaluated and at least `view_reads` reads were served from views (pass 0
/// for a step that only maintains views). [`Violation::NotMeasurable`] while
/// the engine lacks a counter, [`Violation::UnexpectedWork`] when a rule ran.
pub fn no_rule_evaluations(delta: &Counters, view_reads: u64) -> Checked<()> {
    let evaluations = Counters::require("rule_evaluations", delta.rule_evaluations)?;
    let served = Counters::require("view_reads", delta.view_reads)?;
    if evaluations > 0 {
        return Err(Violation::UnexpectedWork(format!(
            "{evaluations} deployed rule evaluation(s) where views should have been read: \
             {delta:?}"
        )));
    }
    if served < view_reads {
        return Err(Violation::UnexpectedWork(format!(
            "{served} view read(s), expected at least {view_reads}: {delta:?}"
        )));
    }
    Ok(())
}

/// Text of the violation a query reply without `revision` produces (V9 #315).
pub const NO_QUERY_REVISION: &str = "query reply names no revision";

/// `revision_aligned`: a read `at` `revision` answered at that revision with
/// exactly `expected` rows (compared on the columns `project` keeps).
pub fn revision_aligned(
    read: &QueryResult,
    revision: u64,
    project: impl Fn(&Value) -> Value,
    expected: &BTreeSet<String>,
) -> Checked<()> {
    let answered = read
        .revision
        .ok_or_else(|| Violation::Transport(format!("{NO_QUERY_REVISION} (read at {revision})")))?;
    if answered != revision {
        return Err(Violation::StaleRevision {
            subscription: format!("read at {revision}"),
            previous: revision,
            got: answered,
        });
    }
    let got: BTreeSet<String> = read.rows.iter().map(|r| project(r).to_string()).collect();
    if &got == expected {
        return Ok(());
    }
    Err(Violation::Diverged {
        subscription: format!("read at {revision}"),
        missing: expected.difference(&got).cloned().collect(),
        unexpected: got.difference(expected).cloned().collect(),
    })
}
