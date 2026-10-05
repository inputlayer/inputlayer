//! Replication stream lines.
//!
//! Every line is one tag byte, a payload and a newline:
//!
//! ```text
//! C<WAL record>        a committed transaction, in the WAL's own framing
//!                      (CRC, JSON, newline); during a resync, part of the
//!                      current graph's state
//! E<json>\n            another engine change: graph create or drop,
//!                      relation drop, vector-index create or drop
//! S<json>\n            resync framing: begin, next graph, end
//! ```
//!
//! A follower applies `C` and `E` lines in order; `S` lines bracket a
//! checkpoint of the primary's whole state.

use crate::index_manager::RegisteredIndex;
use crate::storage::persist::{self, Transaction};
use crate::storage::{StorageError, StorageResult};
use serde::{Deserialize, Serialize};

const COMMIT: u8 = b'C';
const ENGINE: u8 = b'E';
const RESYNC: u8 = b'S';

/// A durable engine change that is not a transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum EngineEvent {
    /// A knowledge graph was created.
    CreateGraph { name: String },
    /// A knowledge graph was dropped.
    DropGraph { name: String },
    /// A relation was dropped with its data, schema, rules and indexes.
    DropRelation { kg: String, relation: String },
    /// A vector index was created (and built from the relation's facts).
    CreateIndex { kg: String, index: RegisteredIndex },
    /// A vector index was dropped.
    DropIndex { kg: String, name: String },
}

/// One change to replicate.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// A committed transaction.
    Commit(Transaction),
    /// Any other durable change.
    Engine(EngineEvent),
}

/// Resync framing around a checkpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ResyncMark {
    /// A checkpoint of every graph at `revision` follows; it holds exactly
    /// the events up to LSN `head`, and the stream continues after it.
    Begin {
        revision: u64,
        head: u64,
        graphs: Vec<String>,
    },
    /// The state of graph `name` follows as `C` lines (its rules, schemas
    /// and facts), until the next mark.
    Graph {
        name: String,
        indexes: Vec<RegisteredIndex>,
    },
    /// The checkpoint is complete.
    End,
}

/// One decoded line.
#[derive(Debug, Clone, PartialEq)]
pub enum Line {
    /// A transaction: a commit, or part of a graph's state during a resync.
    Commit(Transaction),
    /// Another engine change.
    Engine(EngineEvent),
    /// Resync framing.
    Resync(ResyncMark),
}

/// Encode a committed transaction from its WAL record.
pub fn commit_line(record: &[u8]) -> Vec<u8> {
    let mut line = Vec::with_capacity(record.len() + 1);
    line.push(COMMIT);
    line.extend_from_slice(record);
    line
}

/// Encode a transaction as a `C` line.
///
/// # Errors
/// The transaction cannot be serialized.
pub fn transaction_line(txn: &Transaction) -> StorageResult<Vec<u8>> {
    Ok(commit_line(&persist::encode_record(txn)?))
}

/// Encode an engine event as an `E` line.
pub fn engine_line(event: &EngineEvent) -> Vec<u8> {
    json_line(ENGINE, event)
}

/// Encode a resync mark as an `S` line.
pub fn resync_line(mark: &ResyncMark) -> Vec<u8> {
    json_line(RESYNC, mark)
}

fn json_line<T: Serialize>(tag: u8, value: &T) -> Vec<u8> {
    let mut line = vec![tag];
    // These types hold only strings, numbers and index definitions, which
    // always serialize.
    serde_json::to_writer(&mut line, value).expect("replication event serializes");
    line.push(b'\n');
    line
}

/// Split a frame's payload into lines, each with its newline.
pub fn split_lines(payload: &[u8]) -> impl Iterator<Item = &[u8]> {
    payload
        .split_inclusive(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
}

/// Decode one line.
///
/// # Errors
/// An unknown tag, a damaged record or JSON this server cannot read.
pub fn decode_line(line: &[u8]) -> StorageResult<Line> {
    let unreadable =
        |reason: String| StorageError::Other(format!("unreadable replication line: {reason}"));
    let (&tag, payload) = line
        .split_first()
        .ok_or_else(|| unreadable("empty line".to_string()))?;
    if !payload.ends_with(b"\n") {
        return Err(unreadable("line has no newline".to_string()));
    }
    match tag {
        COMMIT => persist::decode_record(payload)
            .map(Line::Commit)
            .map_err(unreadable),
        ENGINE => serde_json::from_slice(payload)
            .map(Line::Engine)
            .map_err(|e| unreadable(e.to_string())),
        RESYNC => serde_json::from_slice(payload)
            .map(Line::Resync)
            .map_err(|e| unreadable(e.to_string())),
        other => Err(unreadable(format!("unknown tag {other:#04x}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Tuple;

    #[test]
    fn every_line_kind_round_trips() {
        let mut txn = Transaction::new(7);
        txn.insert("kg:edge", vec![Tuple::from_pair(1, 2)]);
        let commit = transaction_line(&txn).unwrap();
        let drop = engine_line(&EngineEvent::DropRelation {
            kg: "kg".into(),
            relation: "edge".into(),
        });
        let begin = resync_line(&ResyncMark::Begin {
            revision: 3,
            head: 9,
            graphs: vec!["kg".into()],
        });
        let mut payload = commit.clone();
        payload.extend_from_slice(&drop);
        payload.extend_from_slice(&begin);
        let lines: Vec<_> = split_lines(&payload)
            .map(|l| decode_line(l).unwrap())
            .collect();
        assert_eq!(
            lines,
            vec![
                Line::Commit(txn),
                Line::Engine(EngineEvent::DropRelation {
                    kg: "kg".into(),
                    relation: "edge".into()
                }),
                Line::Resync(ResyncMark::Begin {
                    revision: 3,
                    head: 9,
                    graphs: vec!["kg".into()]
                }),
            ]
        );
    }

    #[test]
    fn damaged_and_unknown_lines_are_refused() {
        let mut txn = Transaction::new(1);
        txn.insert("kg:edge", vec![Tuple::from_pair(1, 2)]);
        let mut commit = transaction_line(&txn).unwrap();
        let mid = commit.len() / 2;
        commit[mid] ^= 0x01;
        assert!(decode_line(&commit).is_err());
        assert!(decode_line(b"X{}\n").is_err());
        assert!(decode_line(b"E{\"create_graph\":{\"name\":\"a\"}}").is_err());
        assert!(decode_line(b"E{\"launch\":{}}\n").is_err());
        assert!(decode_line(b"").is_err());
    }
}
