//! Views of subscription groups: several queries refreshed and published together.

use super::*;
use crate::protocol::subscription::testing::GroupStep;

fn group_key(queries: &[&str]) -> ViewKey {
    ViewKey {
        knowledge_graph: KG.to_string(),
        queries: queries.iter().map(ToString::to_string).collect(),
    }
}

fn step(results: &[(&[i64], &'static str)]) -> GroupStep {
    Ok(results.iter().map(|(v, r)| (v.to_vec(), *r)).collect())
}

/// Create the group view of `?a(X)` and `?b(X)` for subscriber 1.
async fn live_group(
    registry: &mut ViewRegistry,
    steps: Vec<GroupStep>,
) -> (Attachment, UnboundedReceiver<SubscriberId>) {
    let (doorbell, mailbox) = doorbell(1);
    let key = group_key(&["?a(X)", "?b(X)"]);
    let Attach::Waiting(Some(first)) = registry.attach(key, doorbell, || Scripted::group(steps))
    else {
        panic!("a new view evaluates");
    };
    let mut completed = complete(registry, first).await;
    (completed.replies.remove(0).1.unwrap(), mailbox)
}

async fn commit(registry: &mut ViewRegistry, relation: &str) {
    let dispatch = registry.on_change(KG, &change(relation), now()).remove(0);
    let completed = complete(registry, dispatch).await;
    assert!(completed.follow_up.is_none());
}

#[tokio::test]
async fn a_group_starts_from_every_result_at_one_revision() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let (attachment, _mailbox) =
        live_group(&mut registry, vec![step(&[(&[1], "a"), (&[5, 6], "b")])]).await;
    assert_eq!(
        attachment.initial_rows.as_deref(),
        Some(&vec![rows(&[1]), rows(&[5, 6])])
    );
    let publication = &attachment.publication;
    assert_eq!(publication.results.len(), 2);
    assert_eq!(publication.revision, 1);
    assert!(matches!(publication.outcome, Outcome::Snapshot));
}

#[tokio::test]
async fn a_change_to_one_member_publishes_the_whole_group_once() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let steps = vec![
        step(&[(&[1], "a"), (&[5], "b")]),
        step(&[(&[1, 2], "a"), (&[5], "b")]),
        step(&[(&[2], "a"), (&[6], "b")]),
    ];
    let (attachment, mut mailbox) = live_group(&mut registry, steps).await;
    let first = latest(&attachment);

    commit(&mut registry, "a").await;
    assert!(rang(&mut mailbox));
    let second = latest(&attachment);
    assert_eq!((second.number, second.revision), (2, 2));
    let Outcome::Delta { base, changes } = &second.outcome else {
        panic!("a delta");
    };
    assert_eq!(*base, 1);
    assert_eq!(
        changes,
        &[
            RowChange {
                inserted: rows(&[2]),
                retracted: vec![]
            },
            RowChange::default()
        ]
    );
    assert!(
        Arc::ptr_eq(&second.results[1].rows, &first.results[1].rows),
        "the unchanged member keeps its result"
    );

    // Either member's relation refreshes the group; both changes come as one.
    commit(&mut registry, "b").await;
    let third = latest(&attachment);
    let Outcome::Delta { changes, .. } = &third.outcome else {
        panic!("a delta");
    };
    assert_eq!(changes[0].retracted, rows(&[1]));
    assert_eq!(changes[1].inserted, rows(&[6]));
    assert_eq!(changes[1].retracted, rows(&[5]));
    assert!(registry.on_change(KG, &change("c"), now()).is_empty());
}

#[tokio::test]
async fn a_refresh_that_changes_no_member_publishes_nothing() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let steps = vec![
        step(&[(&[1], "a"), (&[5], "b")]),
        step(&[(&[1], "a"), (&[5], "b")]),
    ];
    let (attachment, mut mailbox) = live_group(&mut registry, steps).await;
    commit(&mut registry, "a").await;
    assert!(!rang(&mut mailbox));
    assert_eq!(latest(&attachment).number, 1);
}

#[tokio::test]
async fn a_failed_group_refresh_keeps_every_result_and_recovers_with_both_changes() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let steps = vec![
        step(&[(&[1], "a"), (&[5], "b")]),
        Err("?a(X): too many rows".to_string()),
        step(&[(&[1, 2], "a"), (&[6], "b")]),
    ];
    let (attachment, mut mailbox) = live_group(&mut registry, steps).await;
    let first = latest(&attachment);

    commit(&mut registry, "a").await;
    assert!(rang(&mut mailbox));
    let failed = latest(&attachment);
    assert!(matches!(&failed.outcome, Outcome::Failed(e) if e.starts_with("?a(X)")));
    assert_eq!(failed.result_number, 1);
    assert!(Arc::ptr_eq(&failed.results, &first.results));

    commit(&mut registry, "b").await;
    let recovered = latest(&attachment);
    let Outcome::Delta { base, changes } = &recovered.outcome else {
        panic!("a delta");
    };
    assert_eq!(*base, 1, "relative to the last complete results");
    assert_eq!(changes[0].inserted, rows(&[2]));
    assert_eq!(changes[1].inserted, rows(&[6]));
}

#[tokio::test]
async fn groups_share_a_view_only_with_the_same_queries_in_the_same_order() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let (attachment, _mailbox) =
        live_group(&mut registry, vec![step(&[(&[1], "a"), (&[5], "b")])]).await;
    let (same, _m2) = doorbell(2);
    let Attach::Attached(shared) = registry.attach(group_key(&["?a(X)", "?b(X)"]), same, || {
        unreachable!("the view exists")
    }) else {
        panic!("joins the live group");
    };
    assert!(Arc::ptr_eq(&shared.cell, &attachment.cell));
    for (id, queries) in [(3, vec!["?b(X)", "?a(X)"]), (4, vec!["?a(X)"])] {
        let (other, _m) = doorbell(id);
        let steps = vec![step(&[(&[], "a")])];
        assert!(matches!(
            registry.attach(group_key(&queries), other, || Scripted::group(steps)),
            Attach::Waiting(Some(_))
        ));
    }
    assert_eq!(registry.len(), 3);
}
