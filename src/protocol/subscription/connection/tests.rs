use std::time::Duration;

use futures_util::future::BoxFuture;
use serde_json::json;
use tempfile::TempDir;

use super::*;
use crate::Config;

const KG: &str = "handoff";

fn handler() -> (Arc<Handler>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
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
        inner: ReevaluatingQuery::new(Arc::clone(&handler), KG, "?p(X)", None).unwrap(),
        handler: Arc::clone(&handler),
        program: Some("+p(2)"),
    };
    let (snapshot, _) = subscriptions
        .register(KG, "s", Box::new(view))
        .await
        .unwrap();
    assert_eq!(snapshot.inserted, [vec![json!(1)]]);

    // No notification is fed: only the registration check can catch p(2).
    let completion = tokio::time::timeout(Duration::from_secs(10), subscriptions.next_completion())
        .await
        .expect("registration re-evaluates a subscription its KG moved past");
    let Some(SubscriptionPush::SubscriptionDelta {
        seq,
        revision,
        inserted,
        retracted,
        ..
    }) = subscriptions.on_completion(completion)
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
        tokio::time::timeout(Duration::from_millis(200), subscriptions.next_completion()).await;
    assert!(idle.is_err(), "nothing to re-evaluate");
}
