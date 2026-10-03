//! Reassembling a subscription delta streamed in chunks, strictly.
//!
//! A delta too large for one frame arrives as `subscription_delta_start`,
//! `subscription_delta_chunk`s and `subscription_delta_end`. Like any client,
//! the agent applies it only once the end frame confirms it whole: chunks in
//! order from 0, all for the started delta, adding up to the announced counts.
//! Anything else is a [`Violation::BrokenStream`], never a partial delta.

use std::time::Instant;

use serde_json::Value;

use crate::agent::{row_keys, Delta};
use crate::contract::{Checked, Violation};

/// A streamed delta between its start and end frames.
#[derive(Debug)]
pub struct StreamedDelta {
    subscription: String,
    seq: u64,
    revision: u64,
    inserted: Vec<Value>,
    retracted: Vec<Value>,
    chunks: u64,
}

impl StreamedDelta {
    /// Begin the delta a `subscription_delta_start` frame announces.
    pub fn start(frame: &Value) -> Checked<Self> {
        let subscription = frame["subscription"].as_str().unwrap_or_default();
        let field = |name: &str| {
            frame[name]
                .as_u64()
                .ok_or_else(|| broken(subscription, format!("start without {name}: {frame}")))
        };
        Ok(Self {
            seq: field("seq")?,
            revision: field("revision")?,
            subscription: subscription.to_string(),
            inserted: Vec::new(),
            retracted: Vec::new(),
            chunks: 0,
        })
    }

    /// Take the next frame of the stream; returns the delta once it ends.
    pub fn next(&mut self, frame: &Value, at: Instant) -> Checked<Option<Delta>> {
        let kind = frame["type"].as_str().unwrap_or_default();
        if frame["seq"].as_u64() != Some(self.seq) {
            return Err(self.broken(format!(
                "{kind} for another delta than seq {}: {frame}",
                self.seq
            )));
        }
        match kind {
            "subscription_delta_chunk" => {
                if frame["chunk_index"].as_u64() != Some(self.chunks) {
                    return Err(self.broken(format!(
                        "expected chunk {}, got {}",
                        self.chunks, frame["chunk_index"]
                    )));
                }
                for (field, rows) in [
                    ("inserted", &mut self.inserted),
                    ("retracted", &mut self.retracted),
                ] {
                    let chunk = frame[field].as_array().ok_or_else(|| {
                        broken(
                            &self.subscription,
                            format!("chunk without {field}: {frame}"),
                        )
                    })?;
                    rows.extend(chunk.iter().cloned());
                }
                self.chunks += 1;
                Ok(None)
            }
            "subscription_delta_end" => {
                let announced = (
                    frame["chunk_count"].as_u64(),
                    frame["inserted_count"].as_u64(),
                    frame["retracted_count"].as_u64(),
                );
                let received = (
                    Some(self.chunks),
                    Some(self.inserted.len() as u64),
                    Some(self.retracted.len() as u64),
                );
                if announced != received {
                    return Err(self.broken(format!(
                        "end announces (chunks, inserted, retracted) {announced:?}, received \
                         {received:?}"
                    )));
                }
                Ok(Some(Delta {
                    subscription: self.subscription.clone(),
                    seq: self.seq,
                    revision: self.revision,
                    inserted: row_keys(&self.inserted),
                    retracted: row_keys(&self.retracted),
                    at,
                }))
            }
            other => Err(self.broken(format!("{other} inside a streamed delta: {frame}"))),
        }
    }

    fn broken(&self, detail: String) -> Violation {
        broken(&self.subscription, detail)
    }
}

fn broken(subscription: &str, detail: String) -> Violation {
    Violation::BrokenStream {
        subscription: subscription.to_string(),
        detail,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use serde_json::json;

    use super::*;

    fn start() -> StreamedDelta {
        StreamedDelta::start(
            &json!({"type": "subscription_delta_start", "subscription": "s",
            "generation": 1, "knowledge_graph": "kg", "seq": 2, "revision": 9, "columns": ["x"]}),
        )
        .unwrap()
    }

    fn chunk(index: u64, inserted: &[i64], retracted: &[i64]) -> Value {
        json!({"type": "subscription_delta_chunk", "subscription": "s", "generation": 1,
            "seq": 2, "chunk_index": index,
            "inserted": inserted.iter().map(|v| json!([v])).collect::<Vec<_>>(),
            "retracted": retracted.iter().map(|v| json!([v])).collect::<Vec<_>>()})
    }

    fn end(chunks: u64, inserted: u64, retracted: u64) -> Value {
        json!({"type": "subscription_delta_end", "subscription": "s", "generation": 1, "seq": 2,
            "chunk_count": chunks, "inserted_count": inserted, "retracted_count": retracted})
    }

    fn feed(frames: &[Value]) -> Checked<Option<Delta>> {
        let mut stream = start();
        let mut last = None;
        for frame in frames {
            last = stream.next(frame, Instant::now())?;
        }
        Ok(last)
    }

    #[test]
    fn a_complete_stream_is_one_delta() {
        let delta = feed(&[chunk(0, &[1, 2], &[]), chunk(1, &[3], &[7]), end(2, 3, 1)])
            .unwrap()
            .expect("applies at its end");
        assert_eq!((delta.seq, delta.revision), (2, 9));
        assert_eq!(
            delta.inserted,
            row_keys(&[json!([1]), json!([2]), json!([3])])
        );
        assert_eq!(delta.retracted, row_keys(&[json!([7])]));
    }

    #[test]
    fn nothing_applies_before_the_end() {
        assert!(feed(&[chunk(0, &[1], &[])]).unwrap().is_none());
    }

    #[test]
    fn missing_duplicate_or_foreign_chunks_break_the_stream() {
        let broken = |frames: &[Value]| matches!(feed(frames), Err(Violation::BrokenStream { .. }));
        assert!(broken(&[chunk(1, &[1], &[])]), "missing chunk 0");
        assert!(
            broken(&[chunk(0, &[1], &[]), chunk(0, &[1], &[])]),
            "duplicate chunk"
        );
        assert!(
            broken(&[chunk(0, &[1], &[]), end(2, 1, 0)]),
            "missing last chunk"
        );
        assert!(broken(&[chunk(0, &[1], &[]), end(1, 2, 0)]), "rows lost");
        let mut other_seq = chunk(0, &[1], &[]);
        other_seq["seq"] = json!(3);
        assert!(broken(&[other_seq]), "chunk of another delta");
        let delta = json!({"type": "subscription_delta", "seq": 2});
        assert!(broken(&[delta]), "a whole delta inside a stream");
    }
}
