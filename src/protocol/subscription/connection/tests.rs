use std::collections::BTreeSet;
use std::time::Duration;

use futures_util::future::BoxFuture;
use serde_json::json;
use tempfile::TempDir;

use super::*;
use crate::protocol::subscription::{Refresh, Row, Snapshot};
use crate::Config;

const KG: &str = "handoff";
const WAIT: Duration = Duration::from_secs(10);

impl ConnectionSubscriptions {
    /// Open, attach and register `query` as `id` in one step.
    async fn subscribe(
        &mut self,
        knowledge_graph: &str,
        id: &str,
        query: &str,
    ) -> Result<(Snapshot, u64), String> {
        let opening = self.begin_subscribe(knowledge_graph, id, query)?;
        self.finish_subscribe(opening.run().await, |_| true)
    }

    /// Attach `id` to the view of `key`, created from `view`, and register it.
    async fn register(
        &mut self,
        key: ViewKey,
        id: &str,
        view: Box<dyn StandingQuery>,
    ) -> Result<(Snapshot, u64), String> {
        let opening = self.opening(key, id, 0, view);
        self.finish_subscribe(opening.run().await, |_| true)
    }
}

fn handler() -> (Arc<Handler>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    config.http.rate_limit.ws_max_subscriptions = 0;
    let handler = Arc::new(Handler::from_config(config).unwrap());
    handler.get_storage().create_knowledge_graph(KG).unwrap();
    (handler, tmp)
}

async fn write(handler: &Handler, program: &str) {
    handler
        .execute_program(None, Some(KG.to_string()), program.to_string(), None)
        .await
        .unwrap();
}

/// The next push of `subscriptions`, failing after [`WAIT`].
async fn next_push(subscriptions: &mut ConnectionSubscriptions) -> SubscriptionPush {
    tokio::time::timeout(WAIT, async {
        loop {
            let subscriber = subscriptions.next_delivery().await;
            if let Some(push) = subscriptions.deliver(subscriber, |_| true) {
                return push;
            }
        }
    })
    .await
    .expect("a push")
}

fn inserted(push: SubscriptionPush) -> Vec<Row> {
    match push {
        SubscriptionPush::SubscriptionDelta { inserted, .. } => inserted,
        other => panic!("expected a delta, got {other:?}"),
    }
}

/// A view whose first refresh commits `program` after taking its snapshot:
/// the commit lands between the snapshot and the registration.
struct CommitsAfterSnapshot {
    inner: ReevaluatingQuery,
    handler: Arc<Handler>,
    program: Option<&'static str>,
}

impl StandingQuery for CommitsAfterSnapshot {
    fn refresh(&mut self) -> BoxFuture<'_, Result<Refresh, String>> {
        Box::pin(async move {
            let refresh = self.inner.refresh().await;
            if let Some(program) = self.program.take() {
                write(&self.handler, program).await;
            }
            refresh
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_commit_between_snapshot_and_registration_is_delivered() {
    let (handler, _tmp) = handler();
    write(&handler, "+p(1)").await;
    let mut subscriptions = ConnectionSubscriptions::new(Arc::clone(&handler), None);
    let view = CommitsAfterSnapshot {
        inner: ReevaluatingQuery::new(Arc::clone(&handler), KG, "?p(X)").unwrap(),
        handler: Arc::clone(&handler),
        program: Some("+p(2)"),
    };
    let key = ViewKey {
        knowledge_graph: KG.to_string(),
        query: "?p(X)".to_string(),
    };
    let (snapshot, _) = subscriptions
        .register(key, "s", Box::new(view))
        .await
        .unwrap();
    assert_eq!(snapshot.rows, [vec![json!(1)]]);

    let SubscriptionPush::SubscriptionDelta {
        seq,
        revision,
        inserted,
        retracted,
        ..
    } = next_push(&mut subscriptions).await
    else {
        panic!("expected a delta");
    };
    assert_eq!(seq, 1);
    assert!(
        revision > snapshot.revision,
        "{revision} after {}",
        snapshot.revision
    );
    assert_eq!(inserted, [vec![json!(2)]]);
    assert!(retracted.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_at_the_current_revision_needs_no_second_evaluation() {
    let (handler, _tmp) = handler();
    write(&handler, "+p(1)").await;
    let mut subscriptions = ConnectionSubscriptions::new(Arc::clone(&handler), None);
    let (snapshot, generation) = subscriptions.subscribe(KG, "s", "?p(X)").await.unwrap();
    assert_eq!(generation, 1);
    assert_eq!(
        snapshot.revision,
        handler.get_storage().get_snapshot_for(KG).unwrap().revision
    );
    assert_eq!(handler.subscription_metrics().evaluations(), 1);
    let idle =
        tokio::time::timeout(Duration::from_millis(200), subscriptions.next_delivery()).await;
    assert!(idle.is_err(), "nothing to re-evaluate");
}

/// What a fan-out run observed.
struct FanOut {
    evaluations: u64,
    views: u64,
    /// Each subscription's rows: its snapshot plus every delta it received.
    results: Vec<BTreeSet<String>>,
}

/// Subscribe `per_connection` subscriptions on each of `connections`
/// connections, the `i`-th overall to `query(i)`, then commit `writes` facts
/// one at a time, waiting until every subscription has its delta.
async fn fan_out(
    connections: usize,
    per_connection: usize,
    query: impl Fn(usize) -> String,
    writes: i64,
) -> FanOut {
    let (handler, _tmp) = handler();
    write(&handler, "+p(0)").await;
    let mut results: Vec<BTreeSet<String>> = Vec::new();
    let mut open = Vec::new();
    for _ in 0..connections {
        let mut subscriptions = ConnectionSubscriptions::new(Arc::clone(&handler), None);
        for _ in 0..per_connection {
            let i = results.len();
            let (snapshot, _) = subscriptions
                .subscribe(KG, &format!("s{i}"), &query(i))
                .await
                .unwrap();
            results.push(
                snapshot
                    .rows
                    .iter()
                    .map(|row| json!(row).to_string())
                    .collect(),
            );
        }
        open.push(subscriptions);
    }
    for k in 1..=writes {
        write(&handler, &format!("+p({k})")).await;
        for (c, subscriptions) in open.iter_mut().enumerate() {
            for _ in 0..per_connection {
                let SubscriptionPush::SubscriptionDelta {
                    subscription,
                    inserted,
                    ..
                } = next_push(subscriptions).await
                else {
                    panic!("expected a delta");
                };
                let i: usize = subscription[1..].parse().unwrap();
                assert_eq!(i / per_connection, c, "a push for this connection");
                results[i].extend(inserted.iter().map(|row| json!(row).to_string()));
            }
        }
    }
    let metrics = handler.subscription_metrics();
    FanOut {
        evaluations: metrics.evaluations(),
        views: metrics.views(),
        results,
    }
}

fn identical(_: usize) -> String {
    "?p(X)".to_string()
}

/// A different query per subscriber, each matching every `p` fact.
fn distinct(i: usize) -> String {
    format!("?p(X), X > -{}", i + 1)
}

#[tokio::test(flavor = "multi_thread")]
async fn identical_subscriptions_share_one_evaluation_per_commit() {
    const WRITES: i64 = 3;
    let expected: BTreeSet<String> = (0..=WRITES).map(|k| json!([k]).to_string()).collect();
    for (connections, per_connection) in [(1, 1), (1, 64), (2, 50)] {
        let run = fan_out(connections, per_connection, identical, WRITES).await;
        let subscribers = connections * per_connection;
        assert_eq!(run.views, 1, "{subscribers} identical subscribers");
        assert_eq!(
            run.evaluations,
            1 + WRITES as u64,
            "{subscribers} identical"
        );
        assert!(run.results.iter().all(|rows| *rows == expected));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn distinct_subscriptions_are_evaluated_separately() {
    const WRITES: i64 = 2;
    let expected: BTreeSet<String> = (0..=WRITES).map(|k| json!([k]).to_string()).collect();
    for (connections, per_connection) in [(1, 1), (1, 64), (2, 50)] {
        let subscribers = (connections * per_connection) as u64;
        let run = fan_out(connections, per_connection, distinct, WRITES).await;
        assert_eq!(run.views, subscribers);
        assert_eq!(run.evaluations, subscribers * (1 + WRITES as u64));
        assert!(run.results.iter().all(|rows| *rows == expected));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unsubscribing_the_last_subscriber_drops_the_view_and_a_reused_name_starts_fresh() {
    let (handler, _tmp) = handler();
    write(&handler, "+p(1)").await;
    let mut first = ConnectionSubscriptions::new(Arc::clone(&handler), None);
    let mut second = ConnectionSubscriptions::new(Arc::clone(&handler), None);
    first.subscribe(KG, "s", "?p(X)").await.unwrap();
    second.subscribe(KG, "s", "?p(X)").await.unwrap();
    assert_eq!(handler.subscription_metrics().evaluations(), 1);

    first.unsubscribe("s").unwrap();
    assert!(first.unsubscribe("s").is_err());
    write(&handler, "+p(2)").await;
    assert_eq!(inserted(next_push(&mut second).await), [vec![json!(2)]]);
    let idle = tokio::time::timeout(Duration::from_millis(200), first.next_delivery()).await;
    assert!(idle.is_err(), "nothing for the unsubscribed name");

    // The reused name gets a new generation on the still-shared view.
    let (snapshot, generation) = first.subscribe(KG, "s", "?p(X)").await.unwrap();
    assert_eq!(generation, 2);
    assert_eq!(snapshot.rows, [vec![json!(1)], vec![json!(2)]]);
    assert_eq!(handler.subscription_metrics().evaluations(), 2);

    drop(second);
    first.clear();
    no_views_left(&handler).await;
    assert_eq!(handler.subscription_metrics().active(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_includes_a_write_acknowledged_before_subscribing() {
    let (handler, _tmp) = handler();
    write(&handler, "+order(1)").await;
    let mut agent_a = ConnectionSubscriptions::new(Arc::clone(&handler), None);
    let mut agent_b = ConnectionSubscriptions::new(Arc::clone(&handler), None);
    agent_a.subscribe(KG, "s", "?order(X)").await.unwrap();

    write(&handler, "+order(42)").await;
    let (snapshot, _) = agent_b.subscribe(KG, "s", "?order(X)").await.unwrap();
    assert_eq!(snapshot.rows, [vec![json!(1)], vec![json!(42)]]);
    assert_eq!(inserted(next_push(&mut agent_a).await), [vec![json!(42)]]);
}

/// Wait until every view is gone, failing after [`WAIT`].
async fn no_views_left(handler: &Handler) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while handler.subscription_metrics().views() != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the view outlived its subscribers"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_is_withheld_when_access_is_lost_while_it_opens() {
    let (handler, _tmp) = handler();
    write(&handler, "+p(1)").await;
    let mut subscriptions = ConnectionSubscriptions::new(Arc::clone(&handler), None);
    let opened = subscriptions
        .begin_subscribe(KG, "s", "?p(X)")
        .unwrap()
        .run()
        .await;
    let mut asked = None;
    let message = subscriptions
        .finish_subscribe(opened, |kg| {
            asked = Some(kg.to_string());
            false
        })
        .unwrap_err();
    assert_eq!(asked.as_deref(), Some(KG));
    assert!(message.contains("Access denied"), "{message}");
    assert!(subscriptions.is_empty());
    assert_eq!(handler.subscription_metrics().active(), 0);
    no_views_left(&handler).await;

    let (snapshot, _) = subscriptions.subscribe(KG, "s", "?p(X)").await.unwrap();
    assert_eq!(snapshot.rows, [vec![json!(1)]], "the name is free again");
}

#[tokio::test(flavor = "multi_thread")]
async fn reset_spares_a_newer_registration() {
    let (handler, _tmp) = handler();
    let mut subscriptions = ConnectionSubscriptions::new(Arc::clone(&handler), None);
    assert!(!subscriptions.reset("s", 1), "no such subscription");
    let (_, generation) = subscriptions.subscribe(KG, "s", "?p(X)").await.unwrap();
    assert!(
        !subscriptions.reset("s", generation + 1),
        "no such generation"
    );
    assert!(!subscriptions.reset("other", generation));
    assert!(subscriptions.reset("s", generation));
    assert!(subscriptions.is_empty());
    assert_eq!(handler.subscription_metrics().active(), 0);

    let (_, newer) = subscriptions.subscribe(KG, "s", "?p(X)").await.unwrap();
    assert!(!subscriptions.reset("s", generation), "an old generation");
    assert!(subscriptions.reset("s", newer));
    assert!(subscriptions.is_empty());
}
