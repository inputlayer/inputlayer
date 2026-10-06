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
//! `related` and the offers behind them, (o-42, i3) and (o-42, i4). Blocking
//! i4 and unblocking it are then quiet on both subscriptions: after the cut
//! nothing reaches i4 from o-42's eligible items i1 and i5, so `!blocked(i4)`
//! guards no row.
//!
//! S9b gives (o-42, i4) a second support, `link(i5, i4)`, before subscribing:
//! the same cut retracts only (o-42, i3) from the offers (a row with two
//! supports survives losing one), and blocking i4 retracts that offer through
//! `!blocked(Other)` while unblocking brings it back, the negation flipping
//! one row each way while `related` stays quiet.
//!
//! The histories' writes are defined once, in the oracle's shop corpus
//! (`tests/differential_oracle/shop.rs`); this module adds the deltas each
//! step must deliver. Both histories, fed to the differential oracle
//! (`s9_history_agrees_with_the_differential_oracle`, and the oracle's own
//! corpus), agree at every revision.

use inputlayer_testkit::{Agent, Checked, Fixture, Size, WsClient};
use serde_json::{json, Value};

use crate::engine;
use crate::lifecycle::{KG, OFFERS};
use crate::oracle_check::oracle_check;
use crate::shop::{self, Retraction, S9, S9B};
use crate::support::{committed, fresh, write_revision_matches_delta, QUIET};

const RELATED: &str = shop::S9_QUERIES[0];

/// The exact delta of each subscription after one step (`None`: quiet).
struct Expected {
    related: Option<(&'static [&'static str], &'static [&'static str])>,
    offers: Option<(&'static [&'static str], &'static [&'static str])>,
}

/// After [`S9`]'s cut, blocking and unblocking i4 are quiet.
const S9_DELTAS: [Expected; 3] = [
    Expected {
        related: Some((&[], &["i3", "i4"])),
        offers: Some((&[], &["i3", "i4"])),
    },
    Expected {
        related: None,
        offers: None,
    },
    Expected {
        related: None,
        offers: None,
    },
];

/// [`S9B`]: (o-42, i4) survives the cut, then flips with `blocked(i4)`.
const S9B_DELTAS: [Expected; 3] = [
    Expected {
        related: Some((&[], &["i3", "i4"])),
        offers: Some((&[], &["i3"])),
    },
    Expected {
        related: None,
        offers: Some((&[], &["i4"])),
    },
    Expected {
        related: None,
        offers: Some((&["i4"], &[])),
    },
];

fn rows(key: &str, items: &[&str]) -> Vec<Value> {
    items.iter().map(|item| json!([key, item])).collect()
}

async fn run(history: &Retraction, deltas: &[Expected]) -> Checked<()> {
    assert_eq!(
        history.steps.len(),
        deltas.len(),
        "{}: a delta per step",
        history.name
    );
    let engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Small).install(&engine).await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;
    for program in history.setup {
        committed(writer.try_execute(program).await?)?;
    }

    agent.subscribe("related", RELATED).await?;
    agent.subscribe("offers", OFFERS).await?;
    agent
        .view("related")
        .assert_matches(&rows("i1", &["i2", "i3", "i4"]))?;
    agent
        .view("offers")
        .assert_matches(&rows("o-42", &["i2", "i3", "i4", "i6", "i7", "i8", "i9"]))?;

    for (program, step) in history.steps.iter().zip(deltas) {
        let write = committed(writer.try_execute(program).await?)?;
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

#[tokio::test(flavor = "multi_thread")]
async fn s9_retraction_through_recursion_and_negation() -> Checked<()> {
    run(&S9, &S9_DELTAS).await
}

#[tokio::test(flavor = "multi_thread")]
async fn s9b_a_second_support_keeps_the_offer_through_the_cut() -> Checked<()> {
    run(&S9B, &S9B_DELTAS).await
}

/// S9's and S9b's histories through the differential oracle, observed after
/// the pack and its setup and after every step. The oracle's own corpus runs
/// the same histories (`shop_pack_corpus_agrees_across_all_adapters`).
#[test]
fn s9_history_agrees_with_the_differential_oracle() {
    for history in [&S9, &S9B] {
        oracle_check(history.name, &shop::s9_history(history));
    }
}
