use std::hash::{BuildHasherDefault, Hasher};

use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::UnboundedReceiver;

use super::*;
use crate::protocol::subscription::testing::{doorbell, rows, Scripted, Step};

mod failures;

const KG: &str = "kg";

fn key(query: &str) -> ViewKey {
    ViewKey {
        knowledge_graph: KG.to_string(),
        query: query.to_string(),
    }
}

fn ok(values: &[i64], relation: &'static str) -> Step {
    Ok((values.to_vec(), relation))
}

fn now() -> Instant {
    Instant::now()
}

fn change(relation: &str) -> ChangeSet {
    ChangeSet::relation(relation)
}

/// Attach subscriber `id` to `query`, scripted by `steps` if the view is new.
fn attach<S: BuildHasher + Default>(
    registry: &mut ViewRegistry<S>,
    query: &str,
    id: SubscriberId,
    steps: Vec<Step>,
) -> (Attach, UnboundedReceiver<SubscriberId>) {
    let (doorbell, mailbox) = doorbell(id);
    (
        registry.attach(key(query), doorbell, || Scripted::boxed(steps)),
        mailbox,
    )
}

async fn complete<S: BuildHasher + Default>(
    registry: &mut ViewRegistry<S>,
    dispatch: Dispatch,
) -> Completed {
    registry.on_complete(dispatch.run().await, now())
}

/// Create the view of `query` for subscriber `id` and run its first evaluation.
async fn live<S: BuildHasher + Default>(
    registry: &mut ViewRegistry<S>,
    query: &str,
    id: SubscriberId,
    steps: Vec<Step>,
) -> (Attachment, UnboundedReceiver<SubscriberId>) {
    let (Attach::Waiting(Some(first)), mailbox) = attach(registry, query, id, steps) else {
        panic!("a new view evaluates");
    };
    let mut completed = complete(registry, first).await;
    let (subscriber, attachment) = completed.replies.remove(0);
    assert_eq!(subscriber, id);
    (attachment.unwrap(), mailbox)
}

fn latest(attachment: &Attachment) -> Arc<Publication> {
    attachment.cell.latest()
}

fn rang(mailbox: &mut UnboundedReceiver<SubscriberId>) -> bool {
    match mailbox.try_recv() {
        Ok(_) => true,
        Err(TryRecvError::Empty) => false,
        Err(TryRecvError::Disconnected) => panic!("mailbox closed"),
    }
}

#[tokio::test]
async fn identical_queries_share_one_view_and_one_evaluation_per_change() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let (first, mut mailbox_1) = live(
        &mut registry,
        "?a(X)",
        1,
        vec![ok(&[1], "a"), ok(&[1, 2], "a")],
    )
    .await;
    assert_eq!(first.initial_rows.as_deref(), Some(&rows(&[1])));

    let (Attach::Attached(second), mut mailbox_2) = attach(&mut registry, "?a(X)", 2, vec![])
    else {
        panic!("a live view attaches without evaluating");
    };
    assert!(Arc::ptr_eq(&first.cell, &second.cell));
    assert!(second.initial_rows.is_none());
    assert_eq!(registry.len(), 1);

    let mut dispatches = registry.on_change(KG, &change("a"), now());
    assert_eq!(dispatches.len(), 1, "one evaluation for both subscribers");
    let completed = complete(&mut registry, dispatches.remove(0)).await;
    assert!(completed.follow_up.is_none() && completed.replies.is_empty());
    assert!(rang(&mut mailbox_1) && rang(&mut mailbox_2));
    let publication = latest(&first);
    assert_eq!(publication.number, 2);
    assert!(
        matches!(&publication.outcome, Outcome::Delta { base: 1, inserted, retracted }
            if *inserted == rows(&[2]) && retracted.is_empty())
    );
}

#[tokio::test]
async fn distinct_queries_and_graphs_get_their_own_views() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    live(&mut registry, "?a(X)", 1, vec![ok(&[1], "a")]).await;
    live(&mut registry, "?a(Y)", 2, vec![ok(&[1], "a")]).await;
    let (doorbell_3, _mailbox) = doorbell(3);
    let other_graph = ViewKey {
        knowledge_graph: "other".to_string(),
        query: "?a(X)".to_string(),
    };
    assert!(matches!(
        registry.attach(other_graph, doorbell_3, || Scripted::boxed([ok(&[], "a")])),
        Attach::Waiting(Some(_))
    ));
    assert_eq!(registry.len(), 3);
}

/// Every key hashes alike.
#[derive(Default)]
struct Colliding;

impl Hasher for Colliding {
    fn finish(&self) -> u64 {
        7
    }
    fn write(&mut self, _: &[u8]) {}
}

#[tokio::test]
async fn colliding_keys_never_share_a_view() {
    let mut registry = ViewRegistry::<BuildHasherDefault<Colliding>>::with_hasher(Duration::ZERO);
    let (secret, _m1) = live(&mut registry, "?secret(X)", 1, vec![ok(&[42], "secret")]).await;
    let (public, _m2) = live(&mut registry, "?public(X)", 2, vec![ok(&[1], "public")]).await;
    assert!(!Arc::ptr_eq(&secret.cell, &public.cell));
    assert_eq!(public.initial_rows.as_deref(), Some(&rows(&[1])));
    let (Attach::Attached(again), _m3) = attach(&mut registry, "?public(X)", 3, vec![]) else {
        panic!("attaches to the existing view");
    };
    assert!(Arc::ptr_eq(&again.cell, &public.cell));
    assert_eq!(again.publication.result.sorted_rows(), rows(&[1]));
}

#[tokio::test]
async fn unrelated_change_or_other_graph_does_not_evaluate() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    live(&mut registry, "?a(X)", 1, vec![ok(&[1], "a")]).await;
    assert!(registry.on_change(KG, &change("b"), now()).is_empty());
    assert!(registry.on_change("other", &change("a"), now()).is_empty());
    assert_eq!(registry.on_unknown_changes(now()).len(), 1);
}

#[tokio::test]
async fn changes_while_in_flight_coalesce_into_one_follow_up() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let (attachment, _mailbox) = live(
        &mut registry,
        "?a(X)",
        1,
        vec![ok(&[], "a"), ok(&[1], "a"), ok(&[1, 2, 3], "a")],
    )
    .await;
    let first = registry.on_change(KG, &change("a"), now()).remove(0);
    for _ in 0..10 {
        assert!(registry.on_change(KG, &change("a"), now()).is_empty());
    }
    let completed = complete(&mut registry, first).await;
    let follow_up = completed.follow_up.expect("one follow-up for the burst");
    let completed = complete(&mut registry, follow_up).await;
    assert!(completed.follow_up.is_none());
    let publication = latest(&attachment);
    assert_eq!(publication.number, 3);
    assert_eq!(publication.result.sorted_rows(), rows(&[1, 2, 3]));
}

#[tokio::test]
async fn pending_change_is_checked_against_the_new_dependencies() {
    // The in-flight evaluation discovers a new dependency `b` (a rule changed
    // during the refresh); a write to `b` seen meanwhile needs a follow-up.
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let (attachment, _mailbox) = live(
        &mut registry,
        "?a(X)",
        1,
        vec![ok(&[], "a"), ok(&[], "b"), ok(&[7], "b")],
    )
    .await;
    let first = registry.on_change(KG, &change("a"), now()).remove(0);
    assert!(registry.on_change(KG, &change("b"), now()).is_empty());
    let completed = complete(&mut registry, first).await;
    assert_eq!(
        latest(&attachment).number,
        1,
        "unchanged result publishes nothing"
    );
    complete(&mut registry, completed.follow_up.expect("rerun for b")).await;
    assert_eq!(latest(&attachment).result.sorted_rows(), rows(&[7]));
}

#[tokio::test]
async fn a_change_before_the_first_result_is_not_lost() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let (Attach::Waiting(Some(first)), _mailbox) = attach(
        &mut registry,
        "?a(X)",
        1,
        vec![ok(&[1], "a"), ok(&[1, 2], "a")],
    ) else {
        panic!("a new view evaluates");
    };
    assert!(registry.on_change(KG, &change("a"), now()).is_empty());
    let completed = complete(&mut registry, first).await;
    let attachment = completed.replies[0].1.as_ref().unwrap().clone();
    complete(
        &mut registry,
        completed.follow_up.expect("the change is evaluated"),
    )
    .await;
    assert_eq!(latest(&attachment).result.sorted_rows(), rows(&[1, 2]));
}

#[tokio::test]
async fn subscribers_waiting_for_the_first_result_share_it() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let (Attach::Waiting(Some(first)), _m1) =
        attach(&mut registry, "?a(X)", 1, vec![ok(&[5], "a")])
    else {
        panic!("a new view evaluates");
    };
    let (Attach::Waiting(None), _m2) = attach(&mut registry, "?a(X)", 2, vec![]) else {
        panic!("joins the running first evaluation");
    };
    let completed = complete(&mut registry, first).await;
    let ids: Vec<SubscriberId> = completed.replies.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, [1, 2]);
    for (_, reply) in completed.replies {
        assert_eq!(reply.unwrap().initial_rows.as_deref(), Some(&rows(&[5])));
    }
}

#[tokio::test]
async fn a_subscriber_joining_mid_refresh_gets_a_result_that_saw_every_earlier_change() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let steps = vec![ok(&[1], "a"), ok(&[1, 2], "a"), ok(&[1, 2, 3], "a")];
    let (_, _m1) = live(&mut registry, "?a(X)", 1, steps).await;
    let in_flight = registry.on_change(KG, &change("a"), now()).remove(0);
    let (Attach::Waiting(None), _m2) = attach(&mut registry, "?a(X)", 2, vec![]) else {
        panic!("waits for the refresh in flight");
    };
    assert!(registry.on_change(KG, &change("a"), now()).is_empty());
    let (Attach::Waiting(None), _m3) = attach(&mut registry, "?a(X)", 3, vec![]) else {
        panic!("waits for the refresh after the one in flight");
    };

    let completed = complete(&mut registry, in_flight).await;
    let [(2, Ok(second))] = &completed.replies[..] else {
        panic!("only subscriber 2 is answered");
    };
    assert_eq!(second.publication.result.sorted_rows(), rows(&[1, 2]));
    let completed = complete(&mut registry, completed.follow_up.unwrap()).await;
    let [(3, Ok(third))] = &completed.replies[..] else {
        panic!("subscriber 3 is answered by the follow-up");
    };
    assert_eq!(third.publication.result.sorted_rows(), rows(&[1, 2, 3]));
}

#[tokio::test]
async fn a_subscriber_starts_a_due_refresh_at_once() {
    let mut registry = ViewRegistry::new(Duration::from_secs(60));
    let steps = vec![ok(&[1], "a"), ok(&[1, 2], "a")];
    let (_, _m1) = live(&mut registry, "?a(X)", 1, steps).await;
    assert!(registry.on_change(KG, &change("a"), now()).is_empty());
    let (Attach::Waiting(Some(refresh)), _m2) = attach(&mut registry, "?a(X)", 2, vec![]) else {
        panic!("a due refresh starts for the new subscriber");
    };
    assert!(registry.next_due().is_none());
    let completed = complete(&mut registry, refresh).await;
    let [(2, Ok(joined))] = &completed.replies[..] else {
        panic!("subscriber 2 is answered");
    };
    assert_eq!(joined.publication.result.sorted_rows(), rows(&[1, 2]));
}

#[tokio::test]
async fn a_rule_change_retires_the_views_it_affects() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let (_, _m1) = live(&mut registry, "?a(X)", 1, vec![ok(&[1], "a")]).await;
    assert!(registry.on_rule_change(KG, &change("z"), now()).is_empty());
    let (Attach::Attached(_), _m2) = attach(&mut registry, "?a(X)", 2, vec![]) else {
        panic!("an unrelated rule change keeps sharing");
    };

    let refresh = registry.on_rule_change(KG, &change("a"), now());
    assert_eq!(refresh.len(), 1, "current subscribers get the refresh");
    let (Attach::Waiting(Some(fresh)), _m3) =
        attach(&mut registry, "?a(X)", 3, vec![ok(&[1, 2], "a")])
    else {
        panic!("a new subscriber evaluates under the new rules");
    };
    assert_eq!(registry.len(), 2);
    let mut completed = complete(&mut registry, fresh).await;
    let attachment = completed.replies.remove(0).1.unwrap();
    assert_eq!(attachment.initial_rows.as_deref(), Some(&rows(&[1, 2])));

    // The retired view leaving does not take the new view's key.
    registry.detach(1);
    registry.detach(2);
    assert_eq!(registry.len(), 1);
    let (Attach::Attached(_), _m4) = attach(&mut registry, "?a(X)", 4, vec![]) else {
        panic!("joins the new view");
    };
}

#[tokio::test]
async fn the_last_detach_drops_the_view_and_a_reused_key_starts_fresh() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    live(
        &mut registry,
        "?a(X)",
        1,
        vec![ok(&[1], "a"), ok(&[2], "a")],
    )
    .await;
    let (Attach::Attached(_), _m2) = attach(&mut registry, "?a(X)", 2, vec![]) else {
        panic!("attaches");
    };
    let running = registry.on_change(KG, &change("a"), now()).remove(0);
    assert!(
        registry.detach(1).is_none(),
        "subscriber 2 still uses the view"
    );
    assert_eq!(
        registry.detach(2),
        Some(running.view),
        "gone with its last subscriber"
    );
    assert!(registry.is_empty());
    let stale = complete(&mut registry, running).await;
    assert!(stale.follow_up.is_none() && stale.replies.is_empty());

    let (Attach::Waiting(Some(fresh)), _m3) =
        attach(&mut registry, "?a(X)", 3, vec![ok(&[9], "a")])
    else {
        panic!("a reused key evaluates a new view");
    };
    assert_ne!(fresh.view, 1);
    let mut completed = complete(&mut registry, fresh).await;
    let attachment = completed.replies.remove(0).1.unwrap();
    assert_eq!(attachment.initial_rows.as_deref(), Some(&rows(&[9])));
}

#[tokio::test]
async fn a_subscriber_whose_connection_is_gone_is_detached_on_publish() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let (_, mailbox) = live(
        &mut registry,
        "?a(X)",
        1,
        vec![ok(&[1], "a"), ok(&[2], "a")],
    )
    .await;
    drop(mailbox);
    let d = registry.on_change(KG, &change("a"), now()).remove(0);
    complete(&mut registry, d).await;
    assert!(registry.is_empty());
}

#[tokio::test]
async fn a_window_coalesces_changes_and_never_restarts() {
    let window = Duration::from_millis(5);
    let mut registry = ViewRegistry::new(window);
    let (attachment, _mailbox) = live(
        &mut registry,
        "?a(X)",
        1,
        vec![ok(&[], "a"), ok(&[1, 2], "a"), ok(&[1, 2, 3], "a")],
    )
    .await;
    let t0 = now();
    assert!(registry.on_change(KG, &change("a"), t0).is_empty());
    assert_eq!(registry.next_due(), Some(t0 + window));
    // A later change inside the window does not push the refresh back.
    assert!(registry
        .on_change(KG, &change("a"), t0 + Duration::from_millis(4))
        .is_empty());
    assert_eq!(registry.next_due(), Some(t0 + window));
    assert!(registry.take_due(t0 + Duration::from_millis(4)).is_empty());
    let mut due = registry.take_due(t0 + window);
    assert_eq!(due.len(), 1);
    assert_eq!(registry.next_due(), None);

    // A change during the evaluation waits for the rest of its own window.
    let t1 = t0 + Duration::from_millis(6);
    assert!(registry.on_change(KG, &change("a"), t1).is_empty());
    let completed = registry.on_complete(due.remove(0).run().await, t1 + Duration::from_millis(1));
    assert!(completed.follow_up.is_none());
    assert_eq!(registry.next_due(), Some(t1 + window));
    let follow_up = registry.take_due(t1 + window).remove(0);
    registry.on_complete(follow_up.run().await, t1 + window);
    assert_eq!(latest(&attachment).result.sorted_rows(), rows(&[1, 2, 3]));
}

#[tokio::test]
async fn detaching_a_scheduled_view_unschedules_it() {
    let mut registry = ViewRegistry::new(Duration::from_millis(5));
    live(&mut registry, "?a(X)", 1, vec![ok(&[], "a")]).await;
    registry.on_change(KG, &change("a"), now());
    assert!(registry.next_due().is_some());
    registry.detach(1);
    assert_eq!(registry.next_due(), None);
}
