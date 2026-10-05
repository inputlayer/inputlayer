//! S4, a subscription and a query consistent at a stated revision.
//!
//! Captain's table rows 3 (subscriptions read the rows that changed between
//! revisions) and 7 (a subscription and a query agree at one revision). An
//! agent subscribes to `eligible("o-42", ..)` at revision s; a writer commits
//! three changes to it. The deltas are exact and name each write's revision
//! (required, the delta half). After each delta the agent joins facts with the
//! view at the delta's revision, `at: r_n`, and must see exactly the
//! snapshot plus deltas 1..n, answered at r_n; at the end a read `at: s` must
//! equal the snapshot, and a read at a compacted revision must fail with
//! `revision_compacted` and return no rows.
//!
//! The read half is an expected failure: query replies carry no `revision`
//! until V9 #315, and the engine answers every read at its latest revision,
//! ignoring `at`, until V13 #316. V13 adds the retention setting; when this
//! flips, give the engine a one-revision window so revision 1 is compacted.

use std::collections::BTreeSet;

use inputlayer_testkit::{
    Agent, Checked, Fixture, KnownDefect, Reproduction, Size, Violation, WsClient,
};
use serde_json::{json, Value};

use crate::engine;
use crate::lifecycle::{KG, MINE};
use crate::support::{
    committed, refused, revision_aligned, write_revision_matches_delta, NO_QUERY_REVISION,
};

/// Query replies name no revision (V9 #315).
const NO_REVISION: KnownDefect = KnownDefect {
    plan_item: "#315",
    summary: "query replies carry no revision",
    signature: |v| matches!(v, Violation::Transport(m) if m.starts_with(NO_QUERY_REVISION)),
    reproduction: Reproduction::Deterministic,
};

/// `at` is not honoured: reads answer at the latest revision (V13 #316).
const AT_IGNORED: KnownDefect = KnownDefect {
    plan_item: "#316",
    summary: "a read `at: r` answers at the latest revision instead of r",
    signature: |v| {
        matches!(
            v,
            Violation::StaleRevision { .. } | Violation::Diverged { .. }
        )
    },
    reproduction: Reproduction::Deterministic,
};

/// No retention window: a read at a compacted revision returns rows (V13 #316).
const NO_COMPACTION: KnownDefect = KnownDefect {
    plan_item: "#316",
    summary: "a read at a compacted revision is answered instead of refused revision_compacted",
    signature: |v| matches!(v, Violation::Transport(m) if m.contains("the request succeeded")),
    reproduction: Reproduction::Deterministic,
};

/// Facts joined with the agent's view, answered as columns `I, Q, W`.
const JOINED: &str = r#"?stock(I, Q), eligible("o-42", I, W)"#;

/// The `eligible("o-42", I, W)` part of a [`JOINED`] row.
fn view_part(row: &Value) -> Value {
    json!(["o-42", row[0], row[2]])
}

/// Each write and the exact delta it causes on `eligible("o-42", ..)`.
const WRITES: [(&str, &[&str], &[&str]); 3] = [
    (r#"+stock("i13", 4)"#, &["i13"], &[]),
    (r#"+blocked("i5")"#, &[], &["i5"]),
    (r#"-blocked("i5")"#, &["i5"], &[]),
];

fn eligible(items: &[&str]) -> Vec<Value> {
    items
        .iter()
        .map(|i| json!(["o-42", i, "in_stock"]))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn s4_subscription_and_query_agree_at_a_revision() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Small).install(&engine).await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;

    let snapshot = agent.subscribe("mine", MINE).await?.clone();
    let mut aligned = Vec::new();
    for (program, inserted, retracted) in WRITES {
        let write = committed(writer.try_execute(program).await?)?;
        let delta = agent.next_delta("mine").await?;
        delta.assert_rows(&eligible(inserted), &eligible(retracted))?;
        write_revision_matches_delta(&write, &delta)?;
        let read = agent
            .client_mut()
            .execute_at(JOINED, delta.revision)
            .await?;
        aligned.push(revision_aligned(
            &read,
            delta.revision,
            view_part,
            &agent.view("mine").rows,
        ));
    }
    let at_snapshot = agent
        .client_mut()
        .execute_at(JOINED, snapshot.revision)
        .await?;
    aligned.push(revision_aligned(
        &at_snapshot,
        snapshot.revision,
        view_part,
        &snapshot.rows,
    ));
    KnownDefect::judge_first(
        &[NO_REVISION, AT_IGNORED],
        aligned.into_iter().collect::<Checked<Vec<()>>>().map(drop),
    );

    let compacted = agent.client_mut().try_execute_at(JOINED, 1).await?;
    NO_COMPACTION.judge(refused(compacted, "revision_compacted", "compacted").map(drop));
    // The agent's view stayed exact through all of it.
    let fresh: BTreeSet<String> = writer
        .query(MINE)
        .await?
        .rows
        .iter()
        .map(Value::to_string)
        .collect();
    assert_eq!(agent.view("mine").rows, fresh);
    Ok(())
}
