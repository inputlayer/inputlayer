use std::sync::Arc;

use super::*;

fn change(relation: &str) -> Notification {
    Notification::PersistentUpdate {
        knowledge_graph: "kg".to_string(),
        relation: relation.to_string(),
        operation: "insert".to_string(),
        count: 1,
        timestamp_ms: 0,
        session_id: None,
        seq: 0,
    }
}

fn seqs(notifications: &[Notification]) -> Vec<u64> {
    notifications.iter().map(Notification::seq).collect()
}

fn cursor(log: &NotificationLog, last_seq: u64) -> Cursor {
    Cursor {
        epoch: Some(log.epoch().to_string()),
        last_seq,
    }
}

/// Sequence numbers of everything `live` holds, past any lag.
fn drain(live: &mut broadcast::Receiver<Notification>) -> Vec<u64> {
    let mut seqs = Vec::new();
    loop {
        match live.try_recv() {
            Ok(notification) => seqs.push(notification.seq()),
            Err(broadcast::error::TryRecvError::Lagged(_)) => {}
            Err(_) => return seqs,
        }
    }
}

#[test]
fn publishes_numbered_from_one_and_retains_a_bounded_ring() {
    let log = NotificationLog::new(4);
    let mut live = log.subscribe();
    for relation in ["a", "b", "c", "d", "e"] {
        log.publish(change(relation));
    }
    assert_eq!(
        drain(&mut live),
        [2, 3, 4, 5],
        "the channel keeps the newest 4"
    );
    let resumed = log.resume(Some(&cursor(&log, 1)));
    assert_eq!(seqs(&resumed.replay.unwrap()), [2, 3, 4, 5]);
}

#[test]
fn epochs_differ_between_runs() {
    assert_ne!(
        NotificationLog::new(1).epoch(),
        NotificationLog::new(1).epoch()
    );
    assert_eq!(NotificationLog::new(1).epoch().len(), 16);
}

#[test]
fn concurrent_publishers_deliver_in_seq_order() {
    const THREADS: u64 = 8;
    const EACH: u64 = 500;
    let log = Arc::new(NotificationLog::new((THREADS * EACH) as usize));
    let mut live = log.subscribe();
    let threads: Vec<_> = (0..THREADS)
        .map(|t| {
            let log = Arc::clone(&log);
            std::thread::spawn(move || {
                for _ in 0..EACH {
                    log.publish(change(&format!("t{t}")));
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let delivered = drain(&mut live);
    assert_eq!(delivered, (1..=THREADS * EACH).collect::<Vec<_>>());
    let replayed = log.resume(Some(&cursor(&log, 0))).replay.unwrap();
    assert_eq!(seqs(&replayed), delivered, "ring order is delivery order");
}

#[test]
fn resume_replays_after_the_cursor_then_continues_live_without_overlap() {
    let log = NotificationLog::new(8);
    for relation in ["a", "b", "c"] {
        log.publish(change(relation));
    }
    let mut resumed = log.resume(Some(&cursor(&log, 1)));
    log.publish(change("d"));
    assert_eq!(seqs(resumed.replay.as_ref().unwrap()), [2, 3]);
    assert_eq!(drain(&mut resumed.live), [4]);

    let up_to_date = log.resume(Some(&cursor(&log, 4)));
    assert!(up_to_date.replay.unwrap().is_empty());
}

#[test]
fn resume_without_cursor_replays_nothing() {
    let log = NotificationLog::new(8);
    log.publish(change("a"));
    let mut resumed = log.resume(None);
    assert!(resumed.replay.unwrap().is_empty());
    log.publish(change("b"));
    assert_eq!(drain(&mut resumed.live), [2]);
}

#[test]
fn a_cursor_from_another_epoch_is_a_gap_even_when_its_seq_exists() {
    let log = NotificationLog::new(8);
    for relation in ["a", "b", "c"] {
        log.publish(change(relation));
    }
    // Restarted engine: same numbers, different history.
    let foreign = Cursor {
        epoch: Some("0000000000000000".to_string()),
        last_seq: 1,
    };
    assert_eq!(
        log.resume(Some(&foreign)).replay.unwrap_err(),
        ReplayGap::OtherEpoch
    );
    let unnamed = Cursor {
        epoch: None,
        last_seq: 1,
    };
    assert_eq!(
        log.resume(Some(&unnamed)).replay.unwrap_err(),
        ReplayGap::OtherEpoch
    );
}

#[test]
fn an_evicted_cursor_is_a_gap() {
    let log = NotificationLog::new(2);
    for relation in ["a", "b", "c", "d"] {
        log.publish(change(relation));
    }
    // Retained: 3 and 4. After seq 2 the history is complete; after 1 it is not.
    assert_eq!(
        seqs(&log.resume(Some(&cursor(&log, 2))).replay.unwrap()),
        [3, 4]
    );
    let gap = log.resume(Some(&cursor(&log, 1))).replay.unwrap_err();
    assert_eq!(gap, ReplayGap::Evicted { oldest_retained: 3 });
    assert!(
        gap.message(1).contains("oldest retained seq is 3"),
        "{gap:?}"
    );
}

#[test]
fn a_cursor_past_the_stream_is_a_gap() {
    let log = NotificationLog::new(4);
    log.publish(change("a"));
    assert_eq!(
        log.resume(Some(&cursor(&log, 5))).replay.unwrap_err(),
        ReplayGap::Ahead { last_published: 1 }
    );
}
