//! Required passes: how reads of deployed rules are answered, as the work
//! counters on `/metrics/prometheus` report it (#308). Today every read of a
//! deployed rule evaluates it: `rule_evaluations` grows by one per read and
//! no read is served from a view. Once queries read maintained views (#315)
//! these expectations flip: reads count as `view_reads` and evaluate nothing.

use std::time::Duration;

use inputlayer_testkit::fixture::reachability_chain;
use inputlayer_testkit::{Agent, Checked, Engine, WsClient};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::engine;

const KG: &str = "views";

/// The view counters at one scrape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Counters {
    rule_evaluations: u64,
    view_reads: u64,
    view_maintenance_us: u64,
}

impl Counters {
    /// Work done since `before`.
    fn since(self, before: Self) -> Self {
        Self {
            rule_evaluations: self.rule_evaluations - before.rule_evaluations,
            view_reads: self.view_reads - before.view_reads,
            view_maintenance_us: self.view_maintenance_us - before.view_maintenance_us,
        }
    }

    fn evaluations(rule_evaluations: u64) -> Self {
        Self {
            rule_evaluations,
            view_reads: 0,
            view_maintenance_us: 0,
        }
    }
}

/// Scrape `/metrics/prometheus` with the engine's admin key.
async fn scrape(engine: &Engine) -> Counters {
    let mut stream = TcpStream::connect(("127.0.0.1", engine.port()))
        .await
        .expect("connect for metrics");
    let request = format!(
        "GET /metrics/prometheus HTTP/1.0\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {}\r\n\r\n",
        engine.api_key()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send metrics request");
    let mut response = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        stream.read_to_string(&mut response),
    )
    .await
    .expect("metrics within 10 s")
    .expect("read metrics");
    assert!(
        response.starts_with("HTTP/1.0 200") || response.starts_with("HTTP/1.1 200"),
        "{response}"
    );
    let counter = |name: &str| -> u64 {
        response
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name} ")))
            .unwrap_or_else(|| panic!("{name} is not exported"))
            .trim()
            .parse()
            .expect("a whole-number counter")
    };
    Counters {
        rule_evaluations: counter("inputlayer_rule_evaluations_total"),
        view_reads: counter("inputlayer_view_reads_total"),
        view_maintenance_us: counter("inputlayer_view_maintenance_us_total"),
    }
}

/// Run `query` and return the counters' change over it.
async fn read(engine: &Engine, client: &mut WsClient, query: &str) -> Checked<Counters> {
    let before = scrape(engine).await;
    client.query(query).await?;
    Ok(scrape(engine).await.since(before))
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
            Counters::evaluations(evaluations),
            "{case}: {query}"
        );
    }
    let repeated = {
        let before = scrape(&engine).await;
        for _ in 0..3 {
            reader.query("?reach(0, X)").await?;
        }
        scrape(&engine).await.since(before)
    };
    assert_eq!(
        repeated,
        Counters::evaluations(3),
        "a repeated read evaluates again"
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
        scrape(&engine).await.since(before),
        Counters::evaluations(1),
        "subscribing evaluates the rule for the snapshot"
    );

    for edge in ["+edge(3, 4)", "+edge(4, 5)"] {
        let before = scrape(&engine).await;
        writer.commit(edge).await?;
        agent.next_delta("r").await?;
        assert_eq!(
            scrape(&engine).await.since(before),
            Counters::evaluations(1),
            "{edge}: the refresh evaluates the rule again"
        );
    }
    Ok(())
}
