//! Writer-side relation storage: shared tuple data plus a dedup index.
//!
//! Data lives in a [`RelationMap`] whose relations share chunks with every
//! published snapshot. Each relation also has a hash index (writer only, never
//! copied into snapshots) so set-semantics dedup on insert is O(1) per tuple.
//! The store keeps a running estimate of the bytes its tuples occupy, which
//! the knowledge graph's memory budget is checked against.

use crate::value::{Relation, RelationMap, Tuple};
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;

/// Estimated index bytes per stored tuple: one hash slot and its position.
const INDEX_BYTES_PER_TUPLE: usize = 32;

/// Estimated bytes one stored tuple occupies, its index entry included.
pub fn stored_bytes(tuple: &Tuple) -> usize {
    tuple.estimated_bytes() + INDEX_BYTES_PER_TUPLE
}

/// Positions of tuples sharing one hash.
#[derive(Debug, Clone)]
enum Slot {
    One(usize),
    Many(Vec<usize>),
}

impl Slot {
    fn positions(&self) -> &[usize] {
        match self {
            Slot::One(p) => std::slice::from_ref(p),
            Slot::Many(ps) => ps,
        }
    }

    fn add(&mut self, position: usize) {
        match self {
            Slot::One(p) => *self = Slot::Many(vec![*p, position]),
            Slot::Many(ps) => ps.push(position),
        }
    }
}

/// Tuple hash -> insertion positions in the relation.
#[derive(Debug, Default)]
struct TupleIndex {
    hasher: RandomState,
    slots: HashMap<u64, Slot>,
    /// Estimated bytes of the indexed relation; see [`stored_bytes`].
    bytes: usize,
}

impl TupleIndex {
    fn build(relation: &Relation) -> Self {
        let mut index = Self::default();
        for (position, tuple) in relation.iter().enumerate() {
            index.add(index.hasher.hash_one(tuple), position);
            index.bytes += stored_bytes(tuple);
        }
        index
    }

    fn add(&mut self, hash: u64, position: usize) {
        match self.slots.get_mut(&hash) {
            Some(slot) => slot.add(position),
            None => {
                self.slots.insert(hash, Slot::One(position));
            }
        }
    }

    fn find(&self, hash: u64, tuple: &Tuple, relation: &Relation) -> bool {
        self.slots.get(&hash).is_some_and(|slot| {
            slot.positions()
                .iter()
                .any(|&p| relation.get(p) == Some(tuple))
        })
    }
}

/// Relations of one knowledge graph with O(1) membership on insert.
#[derive(Debug, Default)]
pub struct RelationStore {
    relations: RelationMap,
    indexes: HashMap<String, TupleIndex>,
    /// Sum of every index's `bytes`.
    bytes: usize,
}

impl RelationStore {
    /// Empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// A store of `relations`, each of distinct tuples, with their indexes
    /// built in parallel (used when loading).
    pub fn from_relations(relations: Vec<(String, Vec<Tuple>)>) -> Self {
        use rayon::prelude::*;
        let built: Vec<(String, Relation, TupleIndex)> = relations
            .into_par_iter()
            .map(|(name, tuples)| {
                let tuples = Relation::from(tuples);
                let index = TupleIndex::build(&tuples);
                (name, tuples, index)
            })
            .collect();
        let mut store = Self::new();
        for (name, tuples, index) in built {
            store.replace_index(&name, index);
            store.relations.insert(name, tuples);
        }
        store
    }

    /// All relations. Cloning the map shares tuples.
    pub fn relations(&self) -> &RelationMap {
        &self.relations
    }

    /// Estimated bytes all stored tuples occupy; see `stored_bytes`.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Tuples of `relation`, if present.
    pub fn get(&self, relation: &str) -> Option<&Relation> {
        self.relations.get(relation)
    }

    /// Whether `relation` exists (possibly empty).
    pub fn contains_relation(&self, relation: &str) -> bool {
        self.relations.contains_key(relation)
    }

    /// Replace a relation's contents wholesale (used when loading).
    pub fn set(&mut self, relation: &str, tuples: Vec<Tuple>) {
        let tuples = Relation::from(tuples);
        self.replace_index(relation, TupleIndex::build(&tuples));
        self.relations.insert(relation.to_string(), tuples);
    }

    /// Append tuples not already present (set semantics, also within the batch).
    /// Creates the relation if missing. Returns the newly added tuples and the
    /// duplicate count.
    pub fn insert(&mut self, relation: &str, tuples: Vec<Tuple>) -> (Vec<Tuple>, usize) {
        let data = self.relations.entry(relation.to_string()).or_default();
        let index = self.indexes.entry(relation.to_string()).or_default();
        let mut added = Vec::new();
        let mut duplicates = 0;
        for tuple in tuples {
            let hash = index.hasher.hash_one(&tuple);
            if index.find(hash, &tuple, data) {
                duplicates += 1;
                continue;
            }
            index.add(hash, data.len());
            let bytes = stored_bytes(&tuple);
            index.bytes += bytes;
            self.bytes += bytes;
            added.push(tuple.clone());
            data.push(tuple);
        }
        (added, duplicates)
    }

    /// Whether `relation` holds `tuple`, in O(1).
    pub fn contains(&self, relation: &str, tuple: &Tuple) -> bool {
        self.relations
            .get(relation)
            .zip(self.indexes.get(relation))
            .is_some_and(|(data, index)| index.find(index.hasher.hash_one(tuple), tuple, data))
    }

    /// Remove every tuple in `tuples`. Returns the distinct tuples actually removed.
    pub fn delete(&mut self, relation: &str, tuples: &[Tuple]) -> Vec<Tuple> {
        let (Some(data), Some(index)) = (
            self.relations.get_mut(relation),
            self.indexes.get_mut(relation),
        ) else {
            return Vec::new();
        };
        let remove_set: HashSet<&Tuple> = tuples
            .iter()
            .filter(|tuple| index.find(index.hasher.hash_one(*tuple), tuple, data))
            .collect();
        if remove_set.is_empty() {
            return Vec::new();
        }
        data.retain(|t| !remove_set.contains(t));
        let rebuilt = TupleIndex::build(data);
        self.bytes = self.bytes - index.bytes + rebuilt.bytes;
        *index = rebuilt;
        remove_set.into_iter().cloned().collect()
    }

    /// Remove all tuples of `relation`, keeping it as an empty relation.
    pub fn clear(&mut self, relation: &str) -> usize {
        let Some(data) = self.relations.get_mut(relation) else {
            return 0;
        };
        let count = data.len();
        *data = Relation::new();
        self.replace_index(relation, TupleIndex::default());
        count
    }

    /// Drop a relation entirely. Returns whether it existed.
    pub fn remove(&mut self, relation: &str) -> bool {
        if let Some(index) = self.indexes.remove(relation) {
            self.bytes -= index.bytes;
        }
        self.relations.remove(relation).is_some()
    }

    /// Install `index` as `relation`'s, keeping the byte total in step.
    fn replace_index(&mut self, relation: &str, index: TupleIndex) {
        self.bytes += index.bytes;
        if let Some(old) = self.indexes.insert(relation.to_string(), index) {
            self.bytes -= old.bytes;
        }
    }

    /// Relation names.
    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.relations.keys()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::value::Value;

    fn t(a: i64, b: i64) -> Tuple {
        Tuple::new(vec![Value::Int64(a), Value::Int64(b)])
    }

    #[test]
    fn test_relation_store_insert_dedups_existing_and_within_batch() {
        let mut store = RelationStore::new();
        let (added, dups) = store.insert("e", vec![t(1, 2), t(2, 3), t(1, 2)]);
        assert_eq!((added.len(), dups), (2, 1));
        let (added, dups) = store.insert("e", vec![t(2, 3), t(3, 4)]);
        assert_eq!(added, vec![t(3, 4)]);
        assert_eq!(dups, 1);
        assert_eq!(
            store.get("e").unwrap().to_vec(),
            vec![t(1, 2), t(2, 3), t(3, 4)]
        );
    }

    #[test]
    fn test_relation_store_contains_follows_inserts_and_deletes() {
        let mut store = RelationStore::new();
        assert!(!store.contains("e", &t(1, 1)));
        store.insert("e", vec![t(1, 1), t(2, 2)]);
        assert!(store.contains("e", &t(1, 1)));
        assert!(!store.contains("e", &t(3, 3)));
        assert!(!store.contains("other", &t(1, 1)));
        store.delete("e", &[t(1, 1)]);
        assert!(!store.contains("e", &t(1, 1)));
        assert!(store.contains("e", &t(2, 2)));
    }

    #[test]
    fn test_relation_store_dedup_at_scale_across_chunks() {
        let mut store = RelationStore::new();
        let n = 5_000;
        let (added, _) = store.insert("e", (0..n).map(|i| t(i, i)).collect());
        assert_eq!(added.len(), n as usize);
        let (added, dups) = store.insert("e", (0..n).map(|i| t(i, i)).collect());
        assert_eq!((added.len(), dups), (0, n as usize));
        assert_eq!(store.get("e").unwrap().len(), n as usize);
    }

    #[test]
    fn test_relation_store_delete_then_reinsert() {
        let mut store = RelationStore::new();
        store.insert("e", (0..3000).map(|i| t(i, 0)).collect());
        let removed = store.delete("e", &[t(5, 0), t(5, 0), t(9999, 0)]);
        assert_eq!(removed, vec![t(5, 0)]);
        assert_eq!(store.get("e").unwrap().len(), 2999);
        // Index stays consistent after the rebuild.
        let (added, dups) = store.insert("e", vec![t(5, 0), t(6, 0)]);
        assert_eq!((added, dups), (vec![t(5, 0)], 1));
    }

    #[test]
    fn test_relation_store_insert_does_not_touch_shared_readers() {
        let mut store = RelationStore::new();
        store.insert("e", vec![t(1, 1)]);
        let reader = store.relations().clone();
        store.insert("e", vec![t(2, 2)]);
        store.delete("e", &[t(1, 1)]);
        assert_eq!(reader["e"].to_vec(), vec![t(1, 1)]);
        assert_eq!(store.get("e").unwrap().to_vec(), vec![t(2, 2)]);
    }

    #[test]
    fn test_relation_store_loaded_in_parallel_counts_bytes_and_dedups() {
        let one = stored_bytes(&t(1, 1));
        let mut store = RelationStore::from_relations(vec![
            ("e".to_string(), vec![t(1, 1), t(2, 2)]),
            ("f".to_string(), vec![t(3, 3)]),
        ]);
        assert_eq!(store.bytes(), 3 * one);
        assert_eq!(store.insert("e", vec![t(1, 1), t(4, 4)]).1, 1);
        assert_eq!(store.delete("f", &[t(3, 3)]), vec![t(3, 3)]);
        assert_eq!(store.bytes(), 3 * one);
    }

    #[test]
    fn test_relation_store_bytes_follow_every_change() {
        let mut store = RelationStore::new();
        let one = stored_bytes(&t(1, 1));
        assert!(one > 0);
        store.insert("e", vec![t(1, 1), t(2, 2), t(1, 1)]);
        assert_eq!(store.bytes(), 2 * one);
        store.set("f", vec![t(1, 1), t(2, 2), t(3, 3)]);
        assert_eq!(store.bytes(), 5 * one);
        store.set("f", vec![t(1, 1)]);
        assert_eq!(store.bytes(), 3 * one);
        store.delete("e", &[t(1, 1), t(9, 9)]);
        assert_eq!(store.bytes(), 2 * one);
        store.clear("f");
        assert_eq!(store.bytes(), one);
        assert!(store.remove("e"));
        assert_eq!(store.bytes(), 0);

        let text = Tuple::new(vec![Value::String("x".repeat(1000).into())]);
        assert!(stored_bytes(&text) > 1000);
    }

    #[test]
    fn test_relation_store_set_builds_index_and_clear_resets() {
        let mut store = RelationStore::new();
        store.set("e", vec![t(1, 1), t(2, 2)]);
        assert_eq!(store.insert("e", vec![t(1, 1)]).1, 1);
        assert_eq!(store.clear("e"), 2);
        assert!(store.contains_relation("e"));
        assert_eq!(store.insert("e", vec![t(1, 1)]).0.len(), 1);
        assert!(store.remove("e"));
        assert!(!store.contains_relation("e"));
    }
}
