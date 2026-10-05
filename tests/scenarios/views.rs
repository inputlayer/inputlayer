//! Required passes: how reads of deployed rules are answered, as the work
//! counters on `/metrics/prometheus` report it (#308). Today every read of a
//! deployed rule evaluates it: `rule_evaluations` grows by one per read and
//! no read is served from a view. Once queries read maintained views (#315)
//! these expectations flip: reads count as `view_reads` and evaluate nothing.

use inputlayer_testkit::fixture::reachability_chain;
use inputlayer_testkit::{Agent, Checked, Counters, Engine, WsClient};

use crate::engine;

const KG: &str = "views";

/// The view counters' change between two scrapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Work {
    rule_evaluations: u64,
    view_reads: u64,
    view_maintenance_us: u64,
}

impl Work {
    fn evaluations(rule_evaluations: u64) -> Self {
        Self {
            rule_evaluations,
            view_reads: 0,
            view_maintenance_us: 0,
        }
    }

    /// Work done since the scrape `before`.
    async fn since(engine: &Engine, before: &Counters) -> Checked<Self> {
        let delta = Counters::delta(before, &scrape(engine).await);
        Ok(Self {
            rule_evaluations: Counters::require("rule_evaluations", delta.rule_evaluations)?,
            view_reads: Counters::require("view_reads", delta.view_reads)?,
            view_maintenance_us: Counters::require(
                "view_maintenance_us",
                delta.view_maintenance_us,
            )?,
        })
    }
}

async fn scrape(engine: &Engine) -> Counters {
    engine.metrics().await.expect("scrape metrics")
}

/// Run `query` and return the counters' change over it.
async fn read(engine: &Engine, client: &mut WsClient, query: &str) -> Checked<Work> {
    let before = scrape(engine).await;
    client.query(query).await?;
    Work::since(engine, &before).await
}

#[tokio::test(flavor = "multi_thread")]
async fn every_read_of_a_deployed_rule_evaluates_it_once() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    reachability_chain(KG, 5).install(&engine).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    writer
        .commit("+hop2(X, Z) <- edge(X, Y), edge(Y, Z)")
        .await?;
    let mut reader = WsClient::connect(&engine, KG).await?;

    let cases = [
        ("base relation", "?edge(0, X)", 0),
        ("non-recursive rule, bound", "?hop2(0, Z)", 1),
        ("non-recursive rule, unbound", "?hop2(X, Z)", 1),
        ("recursive rule, bound", "?reach(0, X)", 1),
        ("recursive rule, unbound", "?reach(X, Y)", 1),
        (
            "two deployed rules in one read",
            "?hop2(0, Z), reach(Z, W)",
            1,
        ),
        ("session rule over base facts", "?edge(X, Y), X > 2", 0),
    ];
    for (case, query, evaluations) in cases {
        assert_eq!(
            read(&engine, &mut reader, query).await?,
            Work::evaluations(evaluations),
            "{case}: {query}"
        );
    }
    let repeated = {
        let before = scrape(&engine).await;
        for _ in 0..3 {
            reader.query("?reach(0, X)").await?;
        }
        Work::since(&engine, &before).await?
    };
    assert_eq!(
        repeated,
        Work::evaluations(3),
        "a repeated read evaluates again"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_on_a_session_with_session_facts_evaluates_once() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    reachability_chain(KG, 5).install(&engine).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    writer
        .commit("+hop2(X, Z) <- edge(X, Y), edge(Y, Z)")
        .await?;
    let mut reader = WsClient::connect(&engine, KG).await?;
    reader.commit("edge(9, 10)").await?;

    let before = scrape(&engine).await;
    let result = reader.query("?hop2(0, Z)").await?;
    assert!(!result.rows.is_empty(), "the read has results");
    assert_eq!(
        Work::since(&engine, &before).await?,
        Work::evaluations(1),
        "the provenance baseline is part of the read, not another evaluation"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_evaluates_its_deployed_rule_on_every_relevant_commit() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    reachability_chain(KG, 4).install(&engine).await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;

    let before = scrape(&engine).await;
    agent.subscribe("r", "?reach(0, X)").await?;
    assert_eq!(
        Work::since(&engine, &before).await?,
        Work::evaluations(1),
        "subscribing evaluates the rule for the snapshot"
    );

    for edge in ["+edge(3, 4)", "+edge(4, 5)"] {
        let before = scrape(&engine).await;
        writer.commit(edge).await?;
        agent.next_delta("r").await?;
        assert_eq!(
            Work::since(&engine, &before).await?,
            Work::evaluations(1),
            "{edge}: the refresh evaluates the rule again"
        );
    }
    Ok(())
}
