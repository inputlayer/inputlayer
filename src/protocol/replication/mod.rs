//! Warm-standby replication: the network side.
//!
//! A follower opens a WebSocket to the primary's
//! `GET /v1/replication/stream`, presenting `replication.token` as a bearer
//! token, and says where it is (`hello`). The primary answers `start`: either
//! `tail` (the follower's position is in the retained log) or `resync` (a
//! checkpoint of the whole state comes first). Then:
//!
//! - binary frames carry stream lines (see `replication::event`): the LSN of
//!   the first line and the primary's head LSN when it was sent (8 bytes
//!   each, big-endian), then the lines; LSN 0 marks the lines of a
//!   checkpoint;
//! - `heartbeat` text frames carry the primary's head LSN while idle;
//! - the follower answers each applied frame with `ack`.
//!
//! A follower that hears nothing for `replication.timeout_ms` drops the
//! connection and reconnects, which is how it notices a dead primary or a
//! partition. See [`primary`] and [`follower`].

pub mod follower;
pub mod primary;
mod status;

pub use status::{FollowerState, ReplicationStatus, StatusReport};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The stream endpoint, under `/v1`.
pub const STREAM_PATH: &str = "/replication/stream";

/// Bytes of stream lines per frame (a single larger line is sent alone).
const FRAME_BYTES: usize = 1024 * 1024;

/// Largest frame a follower accepts (one commit can be large).
const MAX_FRAME_BYTES: usize = 1 << 30;

/// Messages a follower sends.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum FollowerMessage {
    /// The follower's position; `stream_id` 0 when it has none.
    Hello {
        name: String,
        stream_id: u64,
        lsn: u64,
    },
    /// Every event up to `lsn` is applied and durable on the follower.
    Ack { lsn: u64 },
}

/// How a stream starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StartMode {
    /// Events after the follower's position follow.
    Tail,
    /// A checkpoint follows, then the events after it.
    Resync,
}

/// Messages the primary sends as text.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum PrimaryMessage {
    Start {
        stream_id: u64,
        mode: StartMode,
        head: u64,
    },
    Heartbeat {
        head: u64,
    },
    /// The primary ends the stream; the follower reconnects.
    Error {
        message: String,
    },
}

/// Whether `presented` is the configured token, compared in time that does
/// not depend on where they differ.
fn token_matches(expected: &str, presented: &str) -> bool {
    let a = Sha256::digest(expected.as_bytes());
    let b = Sha256::digest(presented.as_bytes());
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

/// Bytes before a frame's lines.
const FRAME_HEADER: usize = 16;

/// A binary frame: the first line's LSN (0 for checkpoint lines), the
/// primary's head LSN, then the lines.
fn encode_frame(
    first_lsn: u64,
    head: u64,
    lines: impl IntoIterator<Item = impl AsRef<[u8]>>,
) -> Vec<u8> {
    let mut frame = Vec::with_capacity(FRAME_HEADER);
    frame.extend_from_slice(&first_lsn.to_be_bytes());
    frame.extend_from_slice(&head.to_be_bytes());
    for line in lines {
        frame.extend_from_slice(line.as_ref());
    }
    frame
}

/// Split a binary frame into its first LSN, the primary's head and its
/// lines' bytes.
fn decode_frame(frame: &[u8]) -> Result<(u64, u64, &[u8]), String> {
    let short = || format!("replication frame of {} bytes has no header", frame.len());
    let (lsn, rest) = frame.split_first_chunk::<8>().ok_or_else(short)?;
    let (head, lines) = rest.split_first_chunk::<8>().ok_or_else(short)?;
    Ok((u64::from_be_bytes(*lsn), u64::from_be_bytes(*head), lines))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_match_only_when_equal() {
        assert!(token_matches("0123456789abcdef", "0123456789abcdef"));
        assert!(!token_matches("0123456789abcdef", "0123456789abcdeg"));
        assert!(!token_matches("0123456789abcdef", ""));
    }

    #[test]
    fn frames_round_trip() {
        let frame = encode_frame(42, 50, [b"a\n".as_slice(), b"b\n".as_slice()]);
        assert_eq!(
            decode_frame(&frame).unwrap(),
            (42, 50, b"a\nb\n".as_slice())
        );
        assert!(decode_frame(b"short").is_err());
    }

    #[test]
    fn messages_have_a_stable_wire_shape() {
        let hello = FollowerMessage::Hello {
            name: "standby".into(),
            stream_id: 7,
            lsn: 3,
        };
        assert_eq!(
            serde_json::to_string(&hello).unwrap(),
            r#"{"type":"hello","name":"standby","stream_id":7,"lsn":3}"#
        );
        let start = PrimaryMessage::Start {
            stream_id: 7,
            mode: StartMode::Resync,
            head: 9,
        };
        assert_eq!(
            serde_json::to_string(&start).unwrap(),
            r#"{"type":"start","stream_id":7,"mode":"resync","head":9}"#
        );
    }
}
