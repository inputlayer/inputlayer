//! Conversation-scoped evaluation events (D5).
//!
//! A subscriber opens `WS /v1/events?conversation=<id>` and receives the
//! evaluation events for that conversation: `translation` (the stored
//! tuples), `finding` (watch-view rows with engine proof trees), and
//! `report` (the per-ontology close of each request). A per-conversation
//! ring buffer replays recent events to late subscribers. The gateway
//! invents none of the content - findings and proofs come from the engine.

use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use tokio::sync::broadcast;

const RING_CAPACITY: usize = 256;
const CHANNEL_CAPACITY: usize = 64;
/// Conversation ids are caller-chosen, so the hub must not grow with them:
/// past this many tracked conversations the least recently touched one is
/// evicted (its live subscribers see the channel close and can resubscribe).
const MAX_CONVERSATIONS: usize = 1024;

struct Conversation {
    ring: VecDeque<Value>,
    /// Sequence number the next published event gets (starts at 1).
    next_seq: u64,
    tx: broadcast::Sender<Value>,
    /// Monotonic touch counter for LRU eviction.
    touched: u64,
    /// Created by a subscriber before the conversation existed: evict
    /// these first, so subscriber churn cannot displace live traffic.
    pending: bool,
}

#[derive(Default)]
pub struct EventHub {
    conversations: Mutex<HashMap<String, Conversation>>,
    clock: std::sync::atomic::AtomicU64,
}

impl EventHub {
    /// Publish an event to a conversation's subscribers and its replay
    /// ring, stamping it with the conversation's next `seq`. Returns it.
    pub fn publish(&self, conversation: &str, mut event: Value) -> u64 {
        let mut map = self
            .conversations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = self.entry(&mut map, conversation);
        let seq = entry.next_seq;
        entry.next_seq += 1;
        if let Some(object) = event.as_object_mut() {
            object.insert("seq".to_string(), json!(seq));
        }
        if entry.ring.len() == RING_CAPACITY {
            entry.ring.pop_front();
        }
        entry.pending = false;
        entry.ring.push_back(event.clone());
        let _ = entry.tx.send(event); // no subscribers is fine
        seq
    }

    /// Fetch or create a conversation, evicting the least recently touched
    /// one when the hub is full.
    fn entry<'a>(
        &self,
        map: &'a mut HashMap<String, Conversation>,
        conversation: &str,
    ) -> &'a mut Conversation {
        let now = self
            .clock
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if !map.contains_key(conversation) && map.len() >= MAX_CONVERSATIONS {
            if let Some(oldest) = map
                .iter()
                .min_by_key(|(_, c)| (!c.pending, c.touched))
                .map(|(k, _)| k.clone())
            {
                map.remove(&oldest);
            }
        }
        let entry = map
            .entry(conversation.to_string())
            .or_insert_with(|| Conversation {
                ring: VecDeque::with_capacity(RING_CAPACITY),
                next_seq: 1,
                tx: broadcast::channel(CHANNEL_CAPACITY).0,
                touched: now,
                pending: false,
            });
        entry.touched = now;
        entry
    }

    /// Replay plus a live receiver for a conversation.
    ///
    /// Subscribing does NOT create an entry: otherwise a client opening
    /// sockets on junk ids would evict live conversations from the cap.
    /// An unknown conversation gets an empty replay and a receiver that
    /// starts producing as soon as that conversation publishes.
    ///
    /// Snapshot and receiver are taken under the publish lock, so the live
    /// stream continues exactly where the replay ends.
    pub fn subscribe(
        &self,
        conversation: &str,
        after_seq: Option<u64>,
    ) -> (Vec<Value>, broadcast::Receiver<Value>) {
        let mut map = self
            .conversations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = map.get(conversation) {
            let replay = replay(entry, conversation, after_seq);
            return (replay, entry.tx.subscribe());
        }
        // Park on a channel that this conversation will adopt when it
        // first publishes. A resume point means the client saw events this
        // gateway no longer has: tell it.
        let entry = self.entry(&mut map, conversation);
        entry.pending = true;
        let replay = match after_seq {
            Some(n) if n > 0 => vec![resync(conversation, n, 0)],
            _ => Vec::new(),
        };
        (replay, entry.tx.subscribe())
    }
}

fn seq_of(event: &Value) -> u64 {
    event["seq"].as_u64().unwrap_or(0)
}

fn resync(conversation: &str, after_seq: u64, latest_seq: u64) -> Value {
    json!({
        "type": "resync",
        "conversation": conversation,
        "after_seq": after_seq,
        "latest_seq": latest_seq,
        "reason": "after_seq is ahead of this stream (gateway restarted or \
                   conversation evicted); replaying everything retained",
    })
}

fn replay(entry: &Conversation, conversation: &str, after_seq: Option<u64>) -> Vec<Value> {
    let Some(after) = after_seq else {
        return entry.ring.iter().cloned().collect();
    };
    let latest = entry.next_seq - 1;
    if after > latest {
        let mut out = vec![resync(conversation, after, latest)];
        out.extend(entry.ring.iter().cloned());
        return out;
    }
    let mut out = Vec::new();
    let oldest = entry.ring.front().map_or(entry.next_seq, seq_of);
    if oldest > after + 1 {
        out.push(lagged(conversation, after, oldest - after - 1));
    }
    out.extend(entry.ring.iter().filter(|e| seq_of(e) > after).cloned());
    out
}

/// Events between `after_seq` and the next delivered one are gone for
/// good; the client resumes from `after_seq + skipped`.
pub fn lagged(conversation: &str, after_seq: u64, skipped: u64) -> Value {
    json!({
        "type": "lagged",
        "conversation": conversation,
        "after_seq": after_seq,
        "skipped": skipped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_and_live_delivery() {
        let hub = EventHub::default();
        hub.publish("c1", json!({"n": 1}));
        hub.publish("c1", json!({"n": 2}));
        hub.publish("other", json!({"n": 99}));
        let (replay, mut rx) = hub.subscribe("c1", None);
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[1]["n"], 2);
        hub.publish("c1", json!({"n": 3}));
        assert_eq!(rx.try_recv().expect("live event")["n"], 3);
    }

    #[test]
    fn seq_is_monotonic_per_conversation() {
        let hub = EventHub::default();
        assert_eq!(hub.publish("a", json!({})), 1);
        assert_eq!(hub.publish("a", json!({})), 2);
        assert_eq!(hub.publish("b", json!({})), 1);
        let (replay, _) = hub.subscribe("a", None);
        let seqs: Vec<u64> = replay.iter().map(seq_of).collect();
        assert_eq!(seqs, vec![1, 2]);
    }

    #[test]
    fn resume_returns_only_newer_events() {
        let hub = EventHub::default();
        for n in 1..=5 {
            hub.publish("c", json!({ "n": n }));
        }
        let (replay, _) = hub.subscribe("c", Some(3));
        let seqs: Vec<u64> = replay.iter().map(seq_of).collect();
        assert_eq!(seqs, vec![4, 5]);
        let (replay, _) = hub.subscribe("c", Some(5));
        assert!(replay.is_empty());
        let (replay, _) = hub.subscribe("c", Some(0));
        assert_eq!(replay.len(), 5);
    }

    #[test]
    fn resume_past_the_ring_reports_the_gap() {
        let hub = EventHub::default();
        for n in 0..300 {
            hub.publish("c", json!({ "n": n }));
        }
        // Ring holds seq 45..=300; resuming after 10 lost 11..=44.
        let (replay, _) = hub.subscribe("c", Some(10));
        assert_eq!(replay[0]["type"], "lagged");
        assert_eq!(replay[0]["skipped"], 34);
        assert_eq!(seq_of(&replay[1]), 45);
        assert_eq!(replay.len(), 1 + RING_CAPACITY);
    }

    #[test]
    fn resume_ahead_of_the_stream_resyncs() {
        let hub = EventHub::default();
        hub.publish("c", json!({}));
        // A seq this stream never issued: the gateway restarted.
        let (replay, _) = hub.subscribe("c", Some(40));
        assert_eq!(replay[0]["type"], "resync");
        assert_eq!(replay[0]["latest_seq"], 1);
        assert_eq!(seq_of(&replay[1]), 1);
        // Unknown conversation with a resume point resyncs too.
        let (replay, _) = hub.subscribe("fresh", Some(3));
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0]["type"], "resync");
    }

    #[test]
    fn hub_evicts_least_recently_touched_conversation() {
        let hub = EventHub::default();
        for n in 0..(MAX_CONVERSATIONS + 10) {
            hub.publish(&format!("c{n}"), json!({ "n": n }));
        }
        let map = hub.conversations.lock().expect("lock");
        assert_eq!(map.len(), MAX_CONVERSATIONS, "hub is capped");
        assert!(!map.contains_key("c0"), "oldest evicted");
        assert!(map.contains_key(&format!("c{}", MAX_CONVERSATIONS + 9)));
    }

    #[test]
    fn ring_caps_history() {
        let hub = EventHub::default();
        for n in 0..300 {
            hub.publish("c", json!({ "n": n }));
        }
        let (replay, _) = hub.subscribe("c", None);
        assert_eq!(replay.len(), 256);
        assert_eq!(replay[0]["n"], 44);
    }
}
