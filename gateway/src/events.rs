//! Conversation-scoped evaluation events (D5).
//!
//! A subscriber opens `WS /v1/events?conversation=<id>` and receives the
//! evaluation events for that conversation: `translation` (the stored
//! tuples), `finding` (watch-view rows with engine proof trees), and
//! `report` (the per-ontology close of each request). A per-conversation
//! ring buffer replays recent events to late subscribers. The gateway
//! invents none of the content - findings and proofs come from the engine.

use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use tokio::sync::broadcast;

const RING_CAPACITY: usize = 256;
const CHANNEL_CAPACITY: usize = 64;

struct Conversation {
    ring: VecDeque<Value>,
    tx: broadcast::Sender<Value>,
}

#[derive(Default)]
pub struct EventHub {
    conversations: Mutex<HashMap<String, Conversation>>,
}

impl EventHub {
    /// Publish an event to a conversation's subscribers and its replay ring.
    pub fn publish(&self, conversation: &str, event: Value) {
        let mut map = self
            .conversations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = map
            .entry(conversation.to_string())
            .or_insert_with(|| Conversation {
                ring: VecDeque::with_capacity(RING_CAPACITY),
                tx: broadcast::channel(CHANNEL_CAPACITY).0,
            });
        if entry.ring.len() == RING_CAPACITY {
            entry.ring.pop_front();
        }
        entry.ring.push_back(event.clone());
        let _ = entry.tx.send(event); // no subscribers is fine
    }

    /// Recent history plus a live receiver for a conversation.
    pub fn subscribe(&self, conversation: &str) -> (Vec<Value>, broadcast::Receiver<Value>) {
        let mut map = self
            .conversations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = map
            .entry(conversation.to_string())
            .or_insert_with(|| Conversation {
                ring: VecDeque::with_capacity(RING_CAPACITY),
                tx: broadcast::channel(CHANNEL_CAPACITY).0,
            });
        (entry.ring.iter().cloned().collect(), entry.tx.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn replay_and_live_delivery() {
        let hub = EventHub::default();
        hub.publish("c1", json!({"n": 1}));
        hub.publish("c1", json!({"n": 2}));
        hub.publish("other", json!({"n": 99}));
        let (replay, mut rx) = hub.subscribe("c1");
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[1]["n"], 2);
        hub.publish("c1", json!({"n": 3}));
        assert_eq!(rx.try_recv().expect("live event")["n"], 3);
    }

    #[test]
    fn ring_caps_history() {
        let hub = EventHub::default();
        for n in 0..300 {
            hub.publish("c", json!({ "n": n }));
        }
        let (replay, _) = hub.subscribe("c");
        assert_eq!(replay.len(), 256);
        assert_eq!(replay[0]["n"], 44);
    }
}
