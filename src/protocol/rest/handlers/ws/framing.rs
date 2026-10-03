//! Byte-bounded framing: whether a payload fits one frame, and how to split
//! one that does not into chunks that each do.
//!
//! Sizes come from serializing into a byte counter or a bounded buffer, never
//! from a throwaway copy of the payload: a frame that fits is serialized once,
//! and one that does not is abandoned at the budget and split instead.

use std::io;

use serde::Serialize;

/// Payloads whose single frame would exceed this many bytes are streamed as
/// chunk frames of about this size.
pub(super) const FRAME_BUDGET: usize = 1024 * 1024; // 1 MiB

/// Most rows in one chunk frame.
pub(super) const CHUNK_ROWS: usize = 500;

/// Why a value has no JSON within a byte limit.
#[derive(Debug)]
pub(super) enum Unencodable {
    /// It serializes to more bytes than the limit.
    TooLarge,
    /// It cannot be serialized at all.
    Failed(serde_json::Error),
}

/// JSON of `value` if it is at most `limit` bytes. Serialization stops as
/// soon as the limit is passed.
pub(super) fn encode_within(value: &impl Serialize, limit: usize) -> Result<String, Unencodable> {
    let mut out = Bounded {
        bytes: Vec::new(),
        limit,
    };
    match serde_json::to_writer(&mut out, value) {
        // serde_json writes only valid UTF-8.
        Ok(()) => String::from_utf8(out.bytes)
            .map_err(|e| Unencodable::Failed(serde::ser::Error::custom(e))),
        Err(e) if e.is_io() => Err(Unencodable::TooLarge),
        Err(e) => Err(Unencodable::Failed(e)),
    }
}

/// Number of bytes `value` serializes to.
pub(super) fn json_len(value: &impl Serialize) -> Result<usize, serde_json::Error> {
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

/// An item that cannot fit any chunk frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct OversizedItem {
    /// Position of the item in the payload.
    pub index: usize,
    /// Its serialized size.
    pub bytes: usize,
}

/// Split items, whose serialized sizes are `item_sizes`, into consecutive
/// chunks; returns how many items each chunk takes.
///
/// A chunk frame costs `overhead` bytes plus each item and its separator. A
/// chunk holds at most `max_items` items and stays within `budget` bytes,
/// except that one item always fits a chunk of its own as long as that frame
/// stays within `max_frame`. An item too large even for that makes the whole
/// payload unframable, which is known before any frame is sent.
pub(super) fn plan_chunks(
    item_sizes: impl IntoIterator<Item = usize>,
    overhead: usize,
    budget: usize,
    max_frame: usize,
    max_items: usize,
) -> Result<Vec<usize>, OversizedItem> {
    let mut chunks = Vec::new();
    let (mut items, mut bytes) = (0, overhead);
    for (index, size) in item_sizes.into_iter().enumerate() {
        let cost = size.saturating_add(1);
        if overhead.saturating_add(cost) > max_frame {
            return Err(OversizedItem { index, bytes: size });
        }
        if items > 0 && (items == max_items || bytes + cost > budget) {
            chunks.push(items);
            (items, bytes) = (0, overhead);
        }
        items += 1;
        bytes += cost;
    }
    if items > 0 {
        chunks.push(items);
    }
    Ok(chunks)
}

/// A buffer that refuses to grow past `limit` bytes.
struct Bounded {
    bytes: Vec<u8>,
    limit: usize,
}

impl io::Write for Bounded {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.bytes.len() + buf.len() > self.limit {
            return Err(io::Error::other("frame byte limit reached"));
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A writer that only counts bytes.
struct Counter(usize);

impl io::Write for Counter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn encode_within_returns_json_up_to_the_limit_only() {
        let value = json!({"rows": [[1, "a"], [2, "b"]]});
        let exact = serde_json::to_string(&value).unwrap();
        assert_eq!(encode_within(&value, exact.len()).unwrap(), exact);
        assert!(matches!(
            encode_within(&value, exact.len() - 1),
            Err(Unencodable::TooLarge)
        ));
    }

    #[test]
    fn json_len_is_the_serialized_length() {
        let value = json!(["héllo \"quoted\"", 1.5, null, {"k": [true]}]);
        assert_eq!(
            json_len(&value).unwrap(),
            serde_json::to_string(&value).unwrap().len()
        );
    }

    #[test]
    fn chunks_respect_item_count_and_byte_budget() {
        // overhead 10, budget 40: items of 9 bytes cost 10 each, 3 per chunk.
        assert_eq!(plan_chunks([9; 7], 10, 40, 100, 500), Ok(vec![3, 3, 1]));
        // The item cap binds first.
        assert_eq!(plan_chunks([1; 5], 10, 1_000, 2_000, 2), Ok(vec![2, 2, 1]));
        assert_eq!(plan_chunks([], 10, 40, 100, 500), Ok(vec![]));
    }

    #[test]
    fn an_item_over_the_budget_gets_a_chunk_of_its_own() {
        assert_eq!(plan_chunks([5, 60, 5], 10, 40, 100, 500), Ok(vec![1, 1, 1]));
    }

    #[test]
    fn an_item_over_the_frame_limit_makes_the_payload_unframable() {
        assert_eq!(
            plan_chunks([5, 90, 5], 10, 40, 100, 500),
            Err(OversizedItem {
                index: 1,
                bytes: 90
            })
        );
        assert_eq!(plan_chunks([89], 10, 40, 100, 500), Ok(vec![1]));
    }

    #[test]
    fn planned_chunks_never_exceed_the_budget_unless_single() {
        let sizes: Vec<usize> = (0..1_000).map(|i| (i * 37) % 300 + 1).collect();
        let (overhead, budget) = (50, 2_000);
        let chunks = plan_chunks(sizes.iter().copied(), overhead, budget, 10_000, 500).unwrap();
        assert_eq!(chunks.iter().sum::<usize>(), sizes.len());
        let mut start = 0;
        for len in chunks {
            let bytes: usize = overhead
                + sizes[start..start + len]
                    .iter()
                    .map(|s| s + 1)
                    .sum::<usize>();
            assert!(len == 1 || bytes <= budget, "{len} items, {bytes} bytes");
            start += len;
        }
    }
}
