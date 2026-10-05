//! S9, retraction through recursion and negation.
//!
//! Captain's table row 1 (a deployed rule is a live view) and row 3
//! (subscriptions read the rows that changed between revisions), on the
//! constructs incremental maintenance gets wrong. Gates V4 #311 and V5 #312
//! (B16): S9 is their maintained-mode acceptance, and V0 #307 must keep it
//! green.
//!
//! An agent watches `related("i1", X)` (recursion over the chain
//! i1 -> i2 -> i3 -> i4) and `offer("o-42", X)` (negation over recursion).
//! Cutting `link(i2, i3)` retracts exactly (i1, i3) and (i1, i4) from
//! `related`; of the offers behind them only (o-42, i3) goes, because i4 is
//! still offered through o-42's other item i5 (a row with two supports
//! survives losing one). Blocking i4 then retracts that offer through
//! `!blocked(Other)` and unblocking brings it back: the negation flips one row
//! each way while `related` stays quiet. The same history, fed to the
//! differential oracle (`s9_history_agrees_with_the_differential_oracle`),
//! agrees at every revision.

use inputlayer_testkit::{Agent, Checked, Fixture, Size, WsClient};
use serde_json::{json, Value};

use crate::engine;
use crate::lifecycle::{KG, OFFERS};
use crate::oracle_check::oracle_check;
use crate::support::{committed, fresh, write_revision_matches_delta, QUIET};

const RELATED: &str = r#"?related("i1", X)"#;
/// o-42's other item i5 also reaches i4, so (o-42, i4) has two supports.
const SECOND_SUPPORT: &str = r#"+link("i5", "i4")"#;

/// One write and the exact delta of each subscription (`None`: quiet).
struct Step {
    program: &'static str,
    related: Option<(&'static [&'static str], &'static [&'static str])>,
    offers: Option<(&'static [&'static str], &'static [&'static str])>,
}

const STEPS: &[Step] = &[
    Step {
        program: r#"-link("i2", "i3")"#,
        related: Some((&[], &["i3", "i4"])),
        offers: Some((&[], &["i3"])),
    },
    Step {
        program: r#"+blocked("i4")"#,
        related: None,
        offers: Some((&[], &["i4"])),
    },
    Step {
        program: r#"-blocked("i4")"#,
        related: None,
        offers: Some((&["i4"], &[])),
    },
];

fn rows(key: &str, items: &[&str]) -> Vec<Value> {
    items.iter().map(|item| json!([key, item])).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn s9_retraction_through_recursion_and_negation() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Small).install(&engine).await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;
    committed(writer.try_execute(SECOND_SUPPORT).await?)?;

    agent.subscribe("related", RELATED).await?;
    agent.subscribe("offers", OFFERS).await?;
    agent
        .view("related")
        .assert_matches(&rows("i1", &["i2", "i3", "i4"]))?;
    agent
        .view("offers")
        .assert_matches(&rows("o-42", &["i2", "i3", "i4", "i6", "i7", "i8", "i9"]))?;

    for step in STEPS {
        let write = committed(writer.try_execute(step.program).await?)?;
        for (id, key, expected) in [
            ("related", "i1", &step.related),
            ("offers", "o-42", &step.offers),
        ] {
            match expected {
                Some((inserted, retracted)) => {
                    let delta = agent.next_delta(id).await?;
                    delta.assert_rows(&rows(key, inserted), &rows(key, retracted))?;
                    write_revision_matches_delta(&write, &delta)?;
                }
                None => agent.expect_quiet(id, QUIET).await?,
            }
        }
    }
    agent
        .view("related")
        .assert_matches(&fresh(&mut auditor, RELATED).await?)?;
    agent
        .view("offers")
        .assert_matches(&fresh(&mut auditor, OFFERS).await?)?;
    Ok(())
}

/// S9's history through the differential oracle, observed after the pack and
/// after every step.
#[test]
fn s9_history_agrees_with_the_differential_oracle() {
    let mut history = Fixture::shop_pack(Size::Small).statements;
    history.push(SECOND_SUPPORT.to_string());
    history.push("#installed".to_string());
    for (n, step) in STEPS.iter().enumerate() {
        history.push(step.program.to_string());
        history.push(format!("#step-{}", n + 1));
    }
    let queries = [RELATED, OFFERS, "?eligible(O, I, W)", "?n_eligible(O, N)"];
    oracle_check("S9", &queries, &history);
}
