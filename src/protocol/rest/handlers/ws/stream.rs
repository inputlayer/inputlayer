//! Encoding a result or a subscription delta whole.
//!
//! A payload that fits [`FRAME_BUDGET`] becomes one frame, serialized once.
//! A larger one is streamed: a header frame, chunk frames of at most
//! [`CHUNK_ROWS`] rows and about [`FRAME_BUDGET`] bytes, and an end frame
//! carrying the counts. Together they are one logical result or delta, which
//! the client applies only at its end.
//!
//! Every row is sized before any frame is built: a row no frame can carry
//! (over [`MAX_MESSAGE_SIZE`] on its own) makes the payload undeliverable,
//! reported instead of a stream that could not finish. Rows move into their
//! chunk frames, so streaming costs one pass to size and one to serialize,
//! and never builds a throwaway JSON copy of the whole payload.

use inputlayer_ws_protocol::{ResultFrame, ResultStartFrame, Row, ServerFrame, SubscriptionPush};
use tracing::{info, warn};

use super::framing::{
    encode_within, json_len, plan_chunks, OversizedItem, Unencodable, CHUNK_ROWS, FRAME_BUDGET,
};
use crate::protocol::MAX_MESSAGE_SIZE;

/// Bytes the optional `"row_provenance":[]` field adds to a result chunk.
const PROVENANCE_FIELD_BYTES: usize = r#","row_provenance":[]"#.len();

/// Why a payload cannot be delivered whole.
pub(super) type Undeliverable = String;

/// Frames of `result`: one `result` frame, or `result_start`,
/// `result_chunk`s and `result_end` when over [`FRAME_BUDGET`].
pub(super) fn result_frames(result: ResultFrame) -> Result<Vec<String>, Undeliverable> {
    let frame = ServerFrame::Result(result);
    match encode_within(&frame, FRAME_BUDGET) {
        Ok(json) => return Ok(vec![json]),
        Err(Unencodable::Failed(e)) => return Err(unserializable(&e)),
        Err(Unencodable::TooLarge) => {}
    }
    let ServerFrame::Result(result) = frame else {
        unreachable!("constructed as a result above");
    };
    info!(row_count = result.row_count, "ws_streaming_result");
    stream_result(result)
}

/// Frames of a standing-query push. A delta over [`FRAME_BUDGET`] becomes
/// `subscription_delta_start`, `subscription_delta_chunk`s and
/// `subscription_delta_end`.
pub(super) fn push_frames(push: SubscriptionPush) -> Result<Vec<String>, Undeliverable> {
    let frame = ServerFrame::Subscription(push);
    match encode_within(&frame, FRAME_BUDGET) {
        Ok(json) => return Ok(vec![json]),
        Err(Unencodable::Failed(e)) => return Err(unserializable(&e)),
        Err(Unencodable::TooLarge) => {}
    }
    match frame {
        ServerFrame::Subscription(push @ SubscriptionPush::SubscriptionDelta { .. }) => {
            stream_delta(push)
        }
        other => Ok(vec![encode_whole(&other)?]),
    }
}

fn stream_result(result: ResultFrame) -> Result<Vec<String>, Undeliverable> {
    let ResultFrame {
        id,
        columns,
        rows,
        row_count,
        total_count,
        truncated,
        execution_time_ms,
        row_provenance,
        metadata,
        switched_kg,
        proof_trees,
        timing_breakdown,
        errors,
        statements,
        subscribed,
    } = result;
    let empty_chunk = ServerFrame::ResultChunk {
        id: id.clone(),
        rows: Vec::new(),
        row_provenance: Vec::new(),
        chunk_index: usize::MAX,
    };
    let overhead = json_len(&empty_chunk).map_err(|e| unserializable(&e))?;
    let sizes = rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            let provenance = row_provenance
                .get(i)
                .map_or(Ok(0), |p| json_len(p).map(|len| len + 1))?;
            Ok(json_len(row)? + provenance)
        })
        .collect::<Result<Vec<_>, serde_json::Error>>()
        .map_err(|e| unserializable(&e))?;
    let plan = plan_chunks(
        sizes,
        overhead + PROVENANCE_FIELD_BYTES,
        FRAME_BUDGET,
        MAX_MESSAGE_SIZE,
        CHUNK_ROWS,
    )
    .map_err(|row| oversized_row("The result", row))?;

    let mut frames = Vec::with_capacity(plan.len() + 2);
    frames.push(encode_whole(&ServerFrame::ResultStart(ResultStartFrame {
        id: id.clone(),
        columns,
        total_count,
        truncated,
        execution_time_ms,
        metadata,
        switched_kg,
        proof_trees,
        timing_breakdown,
        errors,
        statements,
        subscribed,
    }))?);
    let mut rows = rows.into_iter();
    let mut provenance = row_provenance.into_iter();
    for (chunk_index, &len) in plan.iter().enumerate() {
        frames.push(encode_whole(&ServerFrame::ResultChunk {
            id: id.clone(),
            rows: rows.by_ref().take(len).collect(),
            row_provenance: provenance.by_ref().take(len).collect(),
            chunk_index,
        })?);
    }
    frames.push(encode_whole(&ServerFrame::ResultEnd {
        id,
        row_count,
        chunk_count: plan.len(),
    })?);
    Ok(frames)
}

fn stream_delta(delta: SubscriptionPush) -> Result<Vec<String>, Undeliverable> {
    let SubscriptionPush::SubscriptionDelta {
        subscription,
        generation,
        knowledge_graph,
        seq,
        revision,
        columns,
        inserted,
        retracted,
    } = delta
    else {
        unreachable!("called with a delta");
    };
    let empty_chunk = ServerFrame::Subscription(SubscriptionPush::SubscriptionDeltaChunk {
        subscription: subscription.clone(),
        generation,
        seq,
        chunk_index: usize::MAX,
        inserted: Vec::new(),
        retracted: Vec::new(),
    });
    let overhead = json_len(&empty_chunk).map_err(|e| unserializable(&e))?;
    let sizes = inserted
        .iter()
        .chain(&retracted)
        .map(json_len)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| unserializable(&e))?;
    let plan = plan_chunks(sizes, overhead, FRAME_BUDGET, MAX_MESSAGE_SIZE, CHUNK_ROWS)
        .map_err(|row| oversized_row(&format!("Delta {seq}"), row))?;
    info!(
        subscription,
        seq,
        inserted = inserted.len(),
        retracted = retracted.len(),
        chunks = plan.len(),
        "ws_streaming_delta"
    );

    let push = |push| encode_whole(&ServerFrame::Subscription(push));
    let mut frames = Vec::with_capacity(plan.len() + 2);
    frames.push(push(SubscriptionPush::SubscriptionDeltaStart {
        subscription: subscription.clone(),
        generation,
        knowledge_graph,
        seq,
        revision,
        columns,
    })?);
    let (inserted_count, retracted_count) = (inserted.len(), retracted.len());
    let mut inserted = inserted.into_iter();
    let mut retracted = retracted.into_iter();
    for (chunk_index, &len) in plan.iter().enumerate() {
        let ins: Vec<Row> = inserted.by_ref().take(len).collect();
        let ret: Vec<Row> = retracted.by_ref().take(len - ins.len()).collect();
        frames.push(push(SubscriptionPush::SubscriptionDeltaChunk {
            subscription: subscription.clone(),
            generation,
            seq,
            chunk_index,
            inserted: ins,
            retracted: ret,
        })?);
    }
    frames.push(push(SubscriptionPush::SubscriptionDeltaEnd {
        subscription,
        generation,
        seq,
        chunk_count: plan.len(),
        inserted_count,
        retracted_count,
    })?);
    Ok(frames)
}

/// JSON of `frame` unchanged, or why it cannot be sent: never a substitute.
fn encode_whole(frame: &ServerFrame) -> Result<String, Undeliverable> {
    match encode_within(frame, MAX_MESSAGE_SIZE) {
        Ok(json) => Ok(json),
        Err(Unencodable::TooLarge) => {
            warn!(max = MAX_MESSAGE_SIZE, "ws_frame_too_large");
            Err(format!(
                "A frame exceeds the {MAX_MESSAGE_SIZE}-byte message limit"
            ))
        }
        Err(Unencodable::Failed(e)) => Err(unserializable(&e)),
    }
}

fn oversized_row(payload: &str, row: OversizedItem) -> Undeliverable {
    warn!(
        row = row.index,
        bytes = row.bytes,
        max = MAX_MESSAGE_SIZE,
        "ws_row_too_large"
    );
    format!(
        "{payload} has a row of {} bytes, over the {MAX_MESSAGE_SIZE}-byte message limit",
        row.bytes
    )
}

fn unserializable(error: &serde_json::Error) -> Undeliverable {
    tracing::error!(%error, "ws_frame_serialize_failed");
    "Internal server error".to_string()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
