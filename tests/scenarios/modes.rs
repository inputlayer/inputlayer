//! S16, mode equivalence: the migration proof.
//!
//! Every captain's table row, across milestone 9: V2 #309 adds
//! `engine.views = "maintained"`, and from then on every slot through V20
//! #332 must leave the rows, deltas and revisions a client sees unchanged.
//! Only the work counters may differ.
//!
//! Two layers check it. The whole suite runs once per mode
//! (`make test-scenarios-modes`: `INPUTLAYER_SCENARIO_VIEWS=recompute`, then
//! `maintained`, which may fail until V9 #315 and is required after), and
//! the oracle runs S9's history through a `maintained` adapter
//! (`INPUTLAYER_ORACLE_VIEWS=maintained make oracle-test`). This test is the
//! direct comparison: S9b's live history (the shop pack, an agent on
//! `related("i1", X)` and `offer("o-42", X)`, the cut, blocking and unblocking
//! i4) runs on one engine per mode, and each run's snapshots, deltas,
//! revisions and write replies must be identical.
//!
//! Expected failure (V2 #309): this engine has no `maintained` mode.

use std::collections::BTreeSet;

use inputlayer_testkit::{
    Agent, Checked, EngineBuilder, Fixture, KnownDefect, Mode, Reproduction, Size, Violation,
    WsClient,
};

use crate::lifecycle::{KG, OFFERS};
use crate::shop::{self, S9B};
use crate::support::{committed, write_revision};

/// The engine has no `maintained` mode (V2 #309).
const NO_MAINTAINED_MODE: KnownDefect = KnownDefect {
    plan_item: "#309",
    summary: "engine.views = \"maintained\" is not available",
    signature: |v| matches!(v, Violation::Unavailable(_)),
    reproduction: Reproduction::Deterministic,
};

/// What a client saw of S9b in one mode.
#[derive(Debug, PartialEq)]
struct Seen {
    snapshots: Vec<BTreeSet<String>>,
    /// Per write: its reply's revision, then each subscription's delta
    /// (inserted, retracted, revision), or `None` when it stayed quiet.
    writes: Vec<(u64, Vec<Option<(BTreeSet<String>, BTreeSet<String>, u64)>>)>,
}

/// Run S9b in `mode`; [`Violation::Unavailable`] when the engine lacks it.
async fn run(mode: Mode) -> Checked<Seen> {
    let engine = EngineBuilder::new(env!("CARGO_BIN_EXE_inputlayer-server"))
        .try_views(mode)?
        .start()
        .await
        .expect("start engine");
    Fixture::shop_pack(Size::Small).install(&engine).await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    for program in S9B.setup {
        committed(writer.try_execute(program).await?)?;
    }
    let subscriptions = [("related", shop::S9_QUERIES[0]), ("offers", OFFERS)];
    let mut seen = Seen {
        snapshots: Vec::new(),
        writes: Vec::new(),
    };
    for (id, query) in subscriptions {
        seen.snapshots
            .push(agent.subscribe(id, query).await?.rows.clone());
    }
    for program in S9B.steps {
        let write = committed(writer.try_execute(program).await?)?;
        let mut deltas = Vec::new();
        for (id, _) in subscriptions {
            let delta = agent.poll_delta(id, crate::support::QUIET).await?;
            deltas.push(delta.map(|d| (d.inserted, d.retracted, d.revision)));
        }
        seen.writes.push((write_revision(&write)?, deltas));
    }
    Ok(seen)
}

#[tokio::test(flavor = "multi_thread")]
async fn s16_both_views_modes_give_identical_rows_deltas_and_revisions() -> Checked<()> {
    let recompute = run(Mode::Recompute).await?;
    assert!(
        recompute
            .writes
            .iter()
            .all(|(_, d)| d.iter().any(Option::is_some)),
        "every S9b write changes a subscription: {recompute:?}"
    );
    let maintained = run(Mode::Maintained).await;
    NO_MAINTAINED_MODE.judge(maintained.and_then(|maintained| {
        if maintained == recompute {
            Ok(())
        } else {
            Err(Violation::WrongDelta {
                subscription: "S9b in maintained mode".into(),
                detail: format!("recompute saw {recompute:?}, maintained saw {maintained:?}"),
            })
        }
    }));
    Ok(())
}
