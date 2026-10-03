use std::time::{Duration, Instant};

use serde_json::json;
use tokio::sync::mpsc::UnboundedReceiver;

use super::*;
use crate::protocol::subscription::testing::{doorbell, rows, Scripted, Step};
use crate::protocol::subscription::{Attach, ChangeSet, ViewKey, ViewRegistry};

const KG: &str = "kg";

fn key() -> ViewKey {
    ViewKey {
        knowledge_graph: KG.to_string(),
        query: "?a(X)".to_string(),
    }
}

/// A view of `?a(X)` and subscribers on it.
struct Fixture {
    registry: ViewRegistry,
    mailboxes: Vec<UnboundedReceiver<SubscriberId>>,
}

impl Fixture {
    /// The view's first evaluation is `steps[0]`, then one per [`Fixture::commit`].
    async fn new(steps: Vec<Step>) -> (Self, Subscriber) {
        let mut fixture = Self {
            registry: ViewRegistry::new(Duration::ZERO),
            mailboxes: Vec::new(),
        };
        let (bell, mailbox) = doorbell(1);
        fixture.mailboxes.push(mailbox);
        let Attach::Waiting(Some(first)) =
            fixture
                .registry
                .attach(key(), Arc::clone(&bell), || Scripted::boxed(steps))
        else {
            panic!("a new view evaluates");
        };
        let mut completed = fixture
            .registry
            .on_complete(first.run().await, Instant::now());
        let attachment = completed.replies.remove(0).1.unwrap();
        let subscriber = Subscriber::new("s1", 1, KG, bell, &attachment);
        (fixture, subscriber)
    }

    fn join(&mut self, id: SubscriberId) -> Subscriber {
        let (bell, mailbox) = doorbell(id);
        self.mailboxes.push(mailbox);
        let Attach::Attached(attachment) = self
            .registry
            .attach(key(), Arc::clone(&bell), || Scripted::boxed([]))
        else {
            panic!("joins the live view");
        };
        Subscriber::new(&format!("s{id}"), 1, KG, bell, &attachment)
    }

    /// A commit to `a` and the view's refresh for it.
    async fn commit(&mut self) {
        let dispatch = self
            .registry
            .on_change(KG, &ChangeSet::relation("a"), Instant::now())
            .remove(0);
        let completed = self
            .registry
            .on_complete(dispatch.run().await, Instant::now());
        assert!(completed.follow_up.is_none());
    }
}

fn ok(values: &[i64]) -> Step {
    Ok((values.to_vec(), "a"))
}

fn delta(push: Option<SubscriptionPush>) -> (u64, u64, Vec<Row>, Vec<Row>) {
    match push {
        Some(SubscriptionPush::SubscriptionDelta {
            seq,
            revision,
            inserted,
            retracted,
            ..
        }) => (seq, revision, inserted, retracted),
        other => panic!("expected a delta, got {other:?}"),
    }
}

fn error(push: Option<SubscriptionPush>) -> String {
    match push {
        Some(SubscriptionPush::SubscriptionError { message, .. }) => message,
        other => panic!("expected an error, got {other:?}"),
    }
}

#[tokio::test]
async fn snapshot_lists_the_result_for_creator_and_joiner() {
    let (mut fixture, _) = Fixture::new(vec![ok(&[3, 1, 2])]).await;
    let (bell, _mailbox) = doorbell(9);
    let Attach::Attached(attachment) = fixture.registry.attach(key(), bell, || Scripted::boxed([]))
    else {
        panic!("joins");
    };
    let snapshot = Snapshot::of(&attachment);
    assert_eq!(snapshot.rows, rows(&[1, 2, 3]));
    assert_eq!(snapshot.columns, ["x"]);
    assert_eq!(snapshot.revision, 1);
}

#[tokio::test]
async fn every_subscriber_gets_the_shared_delta_with_its_own_numbering() {
    let (mut fixture, mut first) = Fixture::new(vec![ok(&[1]), ok(&[1, 2]), ok(&[2, 3])]).await;
    let mut second = fixture.join(2);
    fixture.commit().await;
    for subscriber in [&mut first, &mut second] {
        assert_eq!(
            delta(subscriber.deliver(|_| true)),
            (1, 2, rows(&[2]), vec![])
        );
        assert!(subscriber.deliver(|_| true).is_none(), "nothing new");
    }
    fixture.commit().await;
    assert_eq!(
        delta(first.deliver(|_| true)),
        (2, 3, rows(&[3]), rows(&[1]))
    );
    let SubscriptionPush::SubscriptionDelta {
        subscription,
        generation,
        knowledge_graph,
        ..
    } = second.deliver(|_| true).unwrap()
    else {
        panic!("delta");
    };
    assert_eq!((subscription.as_str(), generation), ("s2", 1));
    assert_eq!(knowledge_graph, KG);
}

#[tokio::test]
async fn a_subscriber_that_skipped_publications_gets_one_combined_delta() {
    let (mut fixture, mut slow) = Fixture::new(vec![ok(&[1]), ok(&[1, 2]), ok(&[2, 3])]).await;
    fixture.commit().await;
    fixture.commit().await;
    assert_eq!(fixture.mailboxes[0].try_recv().unwrap(), 1);
    assert!(
        fixture.mailboxes[0].try_recv().is_err(),
        "one queued wake-up"
    );
    assert_eq!(
        delta(slow.deliver(|_| true)),
        (1, 3, rows(&[2, 3]), rows(&[1]))
    );
}

#[tokio::test]
async fn a_denied_publication_is_withheld_without_a_gap() {
    let (mut fixture, mut subscriber) =
        Fixture::new(vec![ok(&[1]), ok(&[1, 2]), ok(&[1, 2, 3])]).await;
    let mut readable = fixture.join(2);
    fixture.commit().await;
    let message = error(subscriber.deliver(|_| false));
    assert!(
        message.contains("Access denied to knowledge graph 'kg'"),
        "{message}"
    );
    assert_eq!(
        delta(readable.deliver(|_| true)).2,
        rows(&[2]),
        "others unaffected"
    );

    // Restored: the next delta is relative to the last delivered result.
    fixture.commit().await;
    assert_eq!(
        delta(subscriber.deliver(|_| true)),
        (1, 3, rows(&[2, 3]), vec![])
    );
    assert_eq!(
        delta(readable.deliver(|_| true)),
        (2, 3, rows(&[3]), vec![])
    );
}

#[tokio::test]
async fn a_denied_subscriber_never_sees_a_refresh_error() {
    let (mut fixture, mut subscriber) =
        Fixture::new(vec![ok(&[1]), Err("secret detail".to_string())]).await;
    fixture.commit().await;
    let message = error(subscriber.deliver(|_| false));
    assert!(!message.contains("secret detail"), "{message}");
}

#[tokio::test]
async fn a_failed_refresh_is_pushed_and_the_next_delta_follows_the_last_result() {
    let (mut fixture, mut subscriber) =
        Fixture::new(vec![ok(&[1]), Err("over the cap".to_string()), ok(&[1, 2])]).await;
    fixture.commit().await;
    assert_eq!(error(subscriber.deliver(|_| true)), "over the cap");
    fixture.commit().await;
    assert_eq!(
        delta(subscriber.deliver(|_| true)),
        (1, 3, rows(&[2]), vec![])
    );
}

#[tokio::test]
async fn rows_keep_their_exact_values() {
    let (mut fixture, mut subscriber) = Fixture::new(vec![ok(&[1]), ok(&[1, 2])]).await;
    fixture.commit().await;
    let (_, _, inserted, _) = delta(subscriber.deliver(|_| true));
    assert_eq!(inserted, vec![vec![json!(2)]]);
}
