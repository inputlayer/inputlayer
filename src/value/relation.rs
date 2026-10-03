//! Structurally shared tuple storage for one relation.
//!
//! A [`Relation`] is a sequence of fixed-size chunks, each behind an `Arc`.
//! Cloning copies chunk pointers only, so snapshots and query inputs share
//! tuples with the writer. Appending copies at most the last chunk (when a
//! reader still holds it), so a small write costs O(`CHUNK_SIZE`), not
//! O(relation size).

use super::Tuple;
use std::collections::HashMap;
use std::sync::Arc;

/// Tuples per chunk. Every chunk but the last is full.
pub const CHUNK_SIZE: usize = 1024;

/// Relation name -> tuples, the input format of the query engine.
pub type RelationMap = HashMap<String, Relation>;

/// Insertion-ordered, structurally shared tuple sequence.
#[derive(Clone, Default)]
pub struct Relation {
    chunks: Vec<Arc<Vec<Tuple>>>,
    len: usize,
}

impl Relation {
    /// Empty relation.
    pub const fn new() -> Self {
        Self {
            chunks: Vec::new(),
            len: 0,
        }
    }

    /// Number of tuples.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the relation holds no tuples.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Tuple at insertion position `index`.
    pub fn get(&self, index: usize) -> Option<&Tuple> {
        if index >= self.len {
            return None;
        }
        self.chunks[index / CHUNK_SIZE].get(index % CHUNK_SIZE)
    }

    /// Tuples in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = &Tuple> + '_ {
        self.chunks.iter().flat_map(|chunk| chunk.iter())
    }

    /// Whether both share every chunk, and so hold the same tuples. A write
    /// to a relation that a snapshot shares copies the chunks it changes, so
    /// this is `false` after any write since `other` was cloned from `self`.
    pub fn shares_tuples_with(&self, other: &Relation) -> bool {
        self.len == other.len
            && self.chunks.len() == other.chunks.len()
            && self
                .chunks
                .iter()
                .zip(&other.chunks)
                .all(|(a, b)| Arc::ptr_eq(a, b))
    }

    /// Linear membership test. Writers that need fast dedup keep a hash index.
    pub fn contains(&self, tuple: &Tuple) -> bool {
        self.iter().any(|t| t == tuple)
    }

    /// Append a tuple, copying the last chunk only if it is shared.
    pub fn push(&mut self, tuple: Tuple) {
        match self.chunks.last_mut() {
            Some(last) if last.len() < CHUNK_SIZE => Arc::make_mut(last).push(tuple),
            _ => {
                let mut chunk = Vec::with_capacity(CHUNK_SIZE);
                chunk.push(tuple);
                self.chunks.push(Arc::new(chunk));
            }
        }
        self.len += 1;
    }

    /// Keep only tuples matching `keep` (a pure predicate; it may run twice per
    /// tuple). Chunks before the first removal stay shared; the rest are rebuilt.
    pub fn retain(&mut self, mut keep: impl FnMut(&Tuple) -> bool) {
        let Some(first_changed) = self
            .chunks
            .iter()
            .position(|chunk| !chunk.iter().all(&mut keep))
        else {
            return;
        };
        let tail: Vec<Tuple> = self.chunks[first_changed..]
            .iter()
            .flat_map(|chunk| chunk.iter())
            .filter(|t| keep(t))
            .cloned()
            .collect();
        self.chunks.truncate(first_changed);
        self.len = first_changed * CHUNK_SIZE;
        self.extend(tail);
    }

    /// Keep the first `len` tuples. Whole chunks before the cut stay shared;
    /// only the chunk the cut falls in is copied, and only if it is shared.
    pub fn truncate(&mut self, len: usize) {
        if len >= self.len {
            return;
        }
        let mut remaining = len;
        let mut kept = 0;
        for chunk in &mut self.chunks {
            if remaining == 0 {
                break;
            }
            if chunk.len() > remaining {
                Arc::make_mut(chunk).truncate(remaining);
            }
            remaining -= chunk.len();
            kept += 1;
        }
        self.chunks.truncate(kept);
        self.len = len;
    }

    /// Copy the tuples into a plain vector.
    pub fn to_vec(&self) -> Vec<Tuple> {
        self.iter().cloned().collect()
    }
}

impl Extend<Tuple> for Relation {
    fn extend<I: IntoIterator<Item = Tuple>>(&mut self, iter: I) {
        for tuple in iter {
            self.push(tuple);
        }
    }
}

impl From<Vec<Tuple>> for Relation {
    fn from(tuples: Vec<Tuple>) -> Self {
        if tuples.len() <= CHUNK_SIZE {
            let len = tuples.len();
            let chunks = if len == 0 {
                Vec::new()
            } else {
                vec![Arc::new(tuples)]
            };
            return Self { chunks, len };
        }
        tuples.into_iter().collect()
    }
}

impl FromIterator<Tuple> for Relation {
    fn from_iter<I: IntoIterator<Item = Tuple>>(iter: I) -> Self {
        let mut relation = Self::new();
        relation.extend(iter);
        relation
    }
}

impl<'a> IntoIterator for &'a Relation {
    type Item = &'a Tuple;
    type IntoIter = Box<dyn Iterator<Item = &'a Tuple> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// Owning iterator: moves tuples out of unshared chunks, clones the rest.
pub struct IntoIter {
    chunks: std::vec::IntoIter<Arc<Vec<Tuple>>>,
    current: Option<ChunkIter>,
}

enum ChunkIter {
    Owned(std::vec::IntoIter<Tuple>),
    Shared(Arc<Vec<Tuple>>, usize),
}

impl Iterator for IntoIter {
    type Item = Tuple;

    fn next(&mut self) -> Option<Tuple> {
        loop {
            match &mut self.current {
                Some(ChunkIter::Owned(it)) => {
                    if let Some(t) = it.next() {
                        return Some(t);
                    }
                }
                Some(ChunkIter::Shared(chunk, pos)) => {
                    if let Some(t) = chunk.get(*pos) {
                        *pos += 1;
                        return Some(t.clone());
                    }
                }
                None => {}
            }
            let chunk = self.chunks.next()?;
            self.current = Some(match Arc::try_unwrap(chunk) {
                Ok(owned) => ChunkIter::Owned(owned.into_iter()),
                Err(shared) => ChunkIter::Shared(shared, 0),
            });
        }
    }
}

impl IntoIterator for Relation {
    type Item = Tuple;
    type IntoIter = IntoIter;

    fn into_iter(self) -> IntoIter {
        IntoIter {
            chunks: self.chunks.into_iter(),
            current: None,
        }
    }
}

impl PartialEq for Relation {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.iter().eq(other.iter())
    }
}

impl Eq for Relation {}

impl std::fmt::Debug for Relation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::value::Value;

    fn t(i: i64) -> Tuple {
        Tuple::new(vec![Value::Int64(i)])
    }

    fn ints(r: &Relation) -> Vec<i64> {
        r.iter()
            .map(|t| match t.values()[0] {
                Value::Int64(v) => v,
                _ => unreachable!(),
            })
            .collect()
    }

    #[test]
    fn test_relation_push_spans_chunks_in_order() {
        let n = CHUNK_SIZE as i64 * 2 + 5;
        let r: Relation = (0..n).map(t).collect();
        assert_eq!(r.len(), n as usize);
        assert_eq!(ints(&r), (0..n).collect::<Vec<_>>());
        assert_eq!(r.get(CHUNK_SIZE + 1), Some(&t(CHUNK_SIZE as i64 + 1)));
        assert_eq!(r.get(n as usize), None);
    }

    #[test]
    fn test_relation_clone_shares_and_push_copies_only_last_chunk() {
        let mut writer: Relation = (0..(CHUNK_SIZE as i64 + 3)).map(t).collect();
        let reader = writer.clone();
        writer.push(t(-1));
        assert!(Arc::ptr_eq(&writer.chunks[0], &reader.chunks[0]));
        assert!(!Arc::ptr_eq(&writer.chunks[1], &reader.chunks[1]));
        assert_eq!(reader.len(), CHUNK_SIZE + 3);
        assert_eq!(writer.len(), CHUNK_SIZE + 4);
        assert!(!reader.contains(&t(-1)));
    }

    #[test]
    fn test_relation_shares_tuples_until_either_side_writes() {
        let writer: Relation = (0..(CHUNK_SIZE as i64 + 3)).map(t).collect();
        let reader = writer.clone();
        assert!(writer.shares_tuples_with(&reader));

        let mut pushed = writer.clone();
        pushed.push(t(-1));
        assert!(!pushed.shares_tuples_with(&reader));

        let mut retained = writer.clone();
        retained.retain(|_| true);
        assert!(
            retained.shares_tuples_with(&reader),
            "a no-op retain keeps sharing"
        );
        retained.retain(|v| *v != t(CHUNK_SIZE as i64));
        assert!(!retained.shares_tuples_with(&reader));

        // Equal contents built separately are not shared.
        let rebuilt: Relation = (0..(CHUNK_SIZE as i64 + 3)).map(t).collect();
        assert!(!rebuilt.shares_tuples_with(&reader));
    }

    #[test]
    fn test_relation_truncate_keeps_prefix_shared() {
        let n = CHUNK_SIZE as i64 * 3;
        let mut writer: Relation = (0..n).map(t).collect();
        let reader = writer.clone();
        let cut = CHUNK_SIZE + 5;
        writer.truncate(cut);
        assert_eq!(writer.len(), cut);
        assert_eq!(writer.chunks.len(), 2);
        assert!(Arc::ptr_eq(&writer.chunks[0], &reader.chunks[0]));
        assert_eq!(ints(&writer), (0..cut as i64).collect::<Vec<_>>());
        assert_eq!(reader.len(), n as usize);
        writer.truncate(CHUNK_SIZE);
        assert_eq!(writer.chunks.len(), 1);
        writer.truncate(usize::MAX);
        assert_eq!(writer.len(), CHUNK_SIZE);
        writer.truncate(0);
        assert!(writer.is_empty() && writer.chunks.is_empty());
    }

    #[test]
    fn test_relation_retain_keeps_unchanged_prefix_shared() {
        let mut writer: Relation = (0..(CHUNK_SIZE as i64 * 3)).map(t).collect();
        let reader = writer.clone();
        let removed = CHUNK_SIZE as i64 + 7;
        writer.retain(|x| *x != t(removed));
        assert!(Arc::ptr_eq(&writer.chunks[0], &reader.chunks[0]));
        assert_eq!(writer.len(), CHUNK_SIZE * 3 - 1);
        assert!(!writer.contains(&t(removed)));
        assert!(reader.contains(&t(removed)));
        let expected: Vec<i64> = (0..(CHUNK_SIZE as i64 * 3))
            .filter(|&i| i != removed)
            .collect();
        assert_eq!(ints(&writer), expected);
        // Invariant: all chunks but the last are full.
        assert_eq!(
            writer.get(CHUNK_SIZE * 2),
            Some(&t(CHUNK_SIZE as i64 * 2 + 1))
        );
    }

    #[test]
    fn test_relation_into_iter_owned_and_shared() {
        let r: Relation = (0..10).map(t).collect();
        let shared = r.clone();
        assert_eq!(r.into_iter().count(), 10);
        let owned: Vec<Tuple> = shared.into_iter().collect();
        assert_eq!(owned.len(), 10);
    }

    #[test]
    fn test_relation_from_vec_and_eq() {
        let small = Relation::from(vec![t(1), t(2)]);
        assert_eq!(small, [t(1), t(2)].into_iter().collect::<Relation>());
        assert!(Relation::from(Vec::new()).is_empty());
        let big: Vec<Tuple> = (0..=CHUNK_SIZE as i64).map(t).collect();
        assert_eq!(Relation::from(big.clone()).to_vec(), big);
    }
}
