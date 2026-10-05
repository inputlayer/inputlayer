//! Failed evaluations and their retries.

use super::*;

#[tokio::test]
async fn a_failed_first_evaluation_rejects_every_waiter() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let (Attach::Waiting(Some(first)), _m1) = attach(
        &mut registry,
        "?a(X)",
        1,
        vec![Err("over the cap".to_string())],
    ) else {
        panic!("a new view evaluates");
    };
    attach(&mut registry, "?a(X)", 2, vec![]);
    let completed = complete(&mut registry, first).await;
    assert_eq!(completed.replies.len(), 2);
    assert!(completed
        .replies
        .iter()
        .all(|(_, reply)| matches!(reply, Err(m) if m == "over the cap")));
    assert!(registry.is_empty());
    assert!(registry.detach(1).is_none(), "nothing left to detach");
}

#[tokio::test]
async fn a_failed_refresh_keeps_the_result_and_a_new_subscriber_reevaluates_once() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let (attachment, mut mailbox) = live(
        &mut registry,
        "?a(X)",
        1,
        vec![ok(&[1], "a"), Err("boom".to_string()), ok(&[1, 2], "a")],
    )
    .await;
    let d = registry.on_change(KG, &change("a"), now()).remove(0);
    complete(&mut registry, d).await;
    assert!(rang(&mut mailbox));
    let failed = latest(&attachment);
    assert!(matches!(&failed.outcome, Outcome::Failed(m) if m == "boom"));
    assert_eq!((failed.result_number, failed.revision), (1, 1));
    assert!(Arc::ptr_eq(
        &failed.results[0].rows,
        &attachment.publication.results[0].rows
    ));

    let (Attach::Waiting(Some(retry)), _m2) = attach(&mut registry, "?a(X)", 2, vec![]) else {
        panic!("a failed view reevaluates for a new subscriber");
    };
    let (Attach::Waiting(None), _m3) = attach(&mut registry, "?a(X)", 3, vec![]) else {
        panic!("and shares the evaluation in flight");
    };
    let completed = complete(&mut registry, retry).await;
    assert!(completed.follow_up.is_none());
    assert_eq!(completed.replies.len(), 2);
    for (_, reply) in completed.replies {
        let joined = reply.unwrap();
        assert!(Arc::ptr_eq(&joined.cell, &attachment.cell));
        assert_eq!(
            joined.publication.results[0].rows.sorted_rows(),
            rows(&[1, 2])
        );
    }
    let recovered = latest(&attachment);
    assert!(matches!(&recovered.outcome, Outcome::Delta { base: 1, .. }));
    assert!(matches!(
        attach(&mut registry, "?a(X)", 4, vec![]).0,
        Attach::Attached(_)
    ));
}

#[tokio::test]
async fn a_retry_failing_the_same_way_answers_only_its_waiters() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let capped = || Err("capped".to_string());
    let (attachment, mut mailbox) = live(
        &mut registry,
        "?a(X)",
        1,
        vec![ok(&[1], "a"), capped(), capped()],
    )
    .await;
    let d = registry.on_change(KG, &change("a"), now()).remove(0);
    complete(&mut registry, d).await;
    assert!(rang(&mut mailbox));
    let failed = latest(&attachment);

    let (Attach::Waiting(Some(retry)), _m2) = attach(&mut registry, "?a(X)", 2, vec![]) else {
        panic!("reevaluates");
    };
    let (Attach::Waiting(None), _m3) = attach(&mut registry, "?a(X)", 3, vec![]) else {
        panic!("shares the retry in flight");
    };
    let completed = complete(&mut registry, retry).await;
    assert!(matches!(
        &completed.replies[..],
        [(2, Err(a)), (3, Err(b))] if a == "capped" && b == "capped"
    ));
    assert!(!rang(&mut mailbox), "no duplicate error for subscriber 1");
    assert!(Arc::ptr_eq(&latest(&attachment), &failed));
}

#[tokio::test]
async fn a_commit_failing_the_same_way_still_reaches_every_subscriber() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let capped = || Err("capped".to_string());
    let (attachment, mut mailbox) = live(
        &mut registry,
        "?a(X)",
        1,
        vec![ok(&[1], "a"), capped(), capped()],
    )
    .await;
    let d = registry.on_change(KG, &change("a"), now()).remove(0);
    complete(&mut registry, d).await;
    assert!(rang(&mut mailbox));
    let failed = latest(&attachment);

    let d = registry.on_change(KG, &change("a"), now()).remove(0);
    complete(&mut registry, d).await;
    let again = latest(&attachment);
    assert_eq!(
        again.number,
        failed.number + 1,
        "each relevant commit reports the error"
    );
    assert!(matches!(&again.outcome, Outcome::Failed(m) if m == "capped"));
}

#[tokio::test]
async fn a_failed_retry_answers_its_waiters_with_the_new_error() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let steps = vec![
        ok(&[1], "a"),
        Err("first".to_string()),
        Err("second".to_string()),
        ok(&[1], "a"),
    ];
    let (attachment, _m1) = live(&mut registry, "?a(X)", 1, steps).await;
    let d = registry.on_change(KG, &change("a"), now()).remove(0);
    complete(&mut registry, d).await;

    let (Attach::Waiting(Some(retry)), _m2) = attach(&mut registry, "?a(X)", 2, vec![]) else {
        panic!("reevaluates");
    };
    let completed = complete(&mut registry, retry).await;
    assert!(matches!(&completed.replies[..], [(2, Err(m))] if m == "second"));
    assert!(
        !registry.subscribers.contains_key(&2),
        "the rejected waiter is not attached"
    );

    // Recovering to an unchanged result is still news: later subscribers attach.
    let (Attach::Waiting(Some(retry)), _m3) = attach(&mut registry, "?a(X)", 3, vec![]) else {
        panic!("reevaluates");
    };
    let completed = complete(&mut registry, retry).await;
    assert!(matches!(&completed.replies[..], [(3, Ok(_))]));
    assert!(matches!(
        &latest(&attachment).outcome,
        Outcome::Delta { changes, .. } if changes.iter().all(RowChange::is_empty)
    ));
}

#[tokio::test]
async fn the_only_waiter_of_a_failed_retry_takes_the_view_with_it() {
    let mut registry = ViewRegistry::new(Duration::ZERO);
    let steps = vec![
        ok(&[1], "a"),
        Err("first".to_string()),
        Err("again".to_string()),
    ];
    let (_, _m1) = live(&mut registry, "?a(X)", 1, steps).await;
    let d = registry.on_change(KG, &change("a"), now()).remove(0);
    complete(&mut registry, d).await;
    let (Attach::Waiting(Some(retry)), _m2) = attach(&mut registry, "?a(X)", 2, vec![]) else {
        panic!("reevaluates");
    };
    assert!(registry.detach(1).is_none(), "subscriber 2 still waits");
    complete(&mut registry, retry).await;
    assert!(registry.is_empty());
}
