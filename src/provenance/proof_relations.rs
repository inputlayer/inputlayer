//! Relations a proof reads, looked up by their bound columns.
//!
//! [`ProofRelations`] borrows the relations of a knowledge graph snapshot, or
//! of one evaluation, without copying any tuple. Proof search looks tuples up
//! by the columns a subgoal has bound. A (relation, bound columns) pattern is
//! answered by a linear scan until it has been scanned [`SCANS_BEFORE_INDEX`]
//! times in this proof call; it is then indexed once and every later lookup
//! visits only the tuples that can match. A small proof over a large knowledge
//! graph scans exactly as often as before and builds nothing; a call that
//! proves many findings builds each index once and reuses it.
//!
//! Lookups return candidates: every tuple that agrees with the key on each
//! bound column it has (a superset of the matches), in insertion order, so a
//! proof visits matches in the same order whether or not an index exists.
//! Callers verify each candidate with their own equality; the index never
//! decides a match. Its hash only has to agree with that equality: numbers
//! equal across `Int32`/`Int64` and floats equal under IEEE comparison hash
//! alike.

use crate::value::{Relation, RelationMap, Tuple, Value};
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::rc::Rc;

/// Scans of one (relation, bound columns) pattern before it is indexed.
///
/// Building an index costs about 3 scans of a relation up to 100K tuples and
/// up to about 10 at 1M (cache misses), so indexing after this many keeps a
/// proof call within a small constant factor of the cheaper strategy in
/// hindsight, whatever the number of lookups.
const SCANS_BEFORE_INDEX: usize = 4;

/// End of a candidate chain.
const NONE: u32 = u32::MAX;

/// Relations read by one proof call, with the bound-column indexes it built.
pub struct ProofRelations<'a> {
    relations: &'a RelationMap,
    scans_before_index: usize,
    patterns: RefCell<HashMap<&'a str, HashMap<Box<[usize]>, Pattern>>>,
}

/// How one (relation, bound columns) pattern is answered.
enum Pattern {
    /// Answered by linear scans so far, this many times.
    Scanned(usize),
    Indexed(Rc<ColumnIndex>),
}

impl<'a> ProofRelations<'a> {
    /// Look up tuples of `relations`; indexes are built as lookups repeat.
    pub fn new(relations: &'a RelationMap) -> Self {
        Self {
            relations,
            scans_before_index: SCANS_BEFORE_INDEX,
            patterns: RefCell::default(),
        }
    }

    /// Index a pattern after `scans` linear scans of it instead of the default.
    #[cfg(test)]
    pub(crate) fn with_scans_before_index(mut self, scans: usize) -> Self {
        self.scans_before_index = scans;
        self
    }

    /// The tuples of `relation`, if it has any.
    pub fn get(&self, relation: &str) -> Option<&'a Relation> {
        self.relations.get(relation)
    }

    /// Whether `relation` holds a tuple equal to `tuple`.
    pub fn contains(&self, relation: &str, tuple: &Tuple) -> bool {
        let columns: Vec<usize> = (0..tuple.arity()).collect();
        let key: Vec<&Value> = tuple.values().iter().collect();
        self.candidates(relation, &columns, &key)
            .any(|candidate| candidate == tuple)
    }

    /// Tuples of `relation` whose value in each of `columns` (ascending) may
    /// equal the value at the same position of `key`, in insertion order.
    ///
    /// A tuple that lacks one of the columns is a candidate too.
    pub fn candidates(&self, relation: &str, columns: &[usize], key: &[&Value]) -> Candidates<'a> {
        debug_assert_eq!(columns.len(), key.len());
        debug_assert!(columns.windows(2).all(|pair| pair[0] < pair[1]));
        let Some((name, tuples)) = self.relations.get_key_value(relation) else {
            return Candidates::scan(&EMPTY);
        };
        if columns.is_empty() || tuples.len() >= NONE as usize {
            return Candidates::scan(tuples);
        }
        match self.index(name, tuples, columns) {
            Some(index) => {
                let head = index.head(hash_key(key.iter().copied()));
                Candidates {
                    tuples,
                    positions: Positions::Chain {
                        index,
                        next: head,
                        short: 0,
                    },
                }
            }
            None => Candidates::scan(tuples),
        }
    }

    /// The index of a pattern, built once the pattern has been scanned often
    /// enough; `None` while it is still answered by scanning.
    fn index(
        &self,
        name: &'a str,
        tuples: &Relation,
        columns: &[usize],
    ) -> Option<Rc<ColumnIndex>> {
        let mut patterns = self.patterns.borrow_mut();
        let by_columns = patterns.entry(name).or_default();
        let pattern = match by_columns.get_mut(columns) {
            Some(pattern) => pattern,
            None => by_columns
                .entry(columns.into())
                .or_insert(Pattern::Scanned(0)),
        };
        match pattern {
            Pattern::Indexed(index) => Some(Rc::clone(index)),
            Pattern::Scanned(scans) if *scans >= self.scans_before_index => {
                let index = Rc::new(ColumnIndex::build(tuples, columns));
                *pattern = Pattern::Indexed(Rc::clone(&index));
                Some(index)
            }
            Pattern::Scanned(scans) => {
                *scans += 1;
                None
            }
        }
    }
}

static EMPTY: Relation = Relation::new();

/// Positions of a relation grouped by the hash of their values in a set of
/// columns: one chain per hash, ascending, linked through `next`.
struct ColumnIndex {
    heads: HashMap<u64, u32, BuildHasherDefault<PreHashed>>,
    next: Vec<u32>,
    /// Positions of tuples lacking one of the columns, ascending.
    short: Vec<u32>,
}

impl ColumnIndex {
    /// One pass from the last tuple to the first, so each chain ends up
    /// ascending: a position links to the previous head of its hash. The map
    /// is sized for distinct keys up front (growing it costs more than the
    /// pass) and shrunk to the keys actually seen afterwards.
    fn build(tuples: &Relation, columns: &[usize]) -> Self {
        let mut heads =
            HashMap::with_capacity_and_hasher(tuples.len(), BuildHasherDefault::default());
        let mut next = vec![NONE; tuples.len()];
        let mut short = Vec::new();
        for position in (0..tuples.len()).rev() {
            let Some(values) = tuples.get(position).map(Tuple::values) else {
                continue;
            };
            if columns.iter().any(|&column| column >= values.len()) {
                short.push(position as u32);
                continue;
            }
            let hash = hash_key(columns.iter().map(|&column| &values[column]));
            next[position] = heads.insert(hash, position as u32).unwrap_or(NONE);
        }
        heads.shrink_to_fit();
        short.reverse();
        Self { heads, next, short }
    }

    fn head(&self, hash: u64) -> u32 {
        self.heads.get(&hash).copied().unwrap_or(NONE)
    }
}

/// Candidate tuples of one lookup, in insertion order.
pub struct Candidates<'a> {
    tuples: &'a Relation,
    positions: Positions,
}

enum Positions {
    All(std::ops::Range<usize>),
    /// A hash chain merged with the positions of short tuples.
    Chain {
        index: Rc<ColumnIndex>,
        next: u32,
        short: usize,
    },
}

impl<'a> Candidates<'a> {
    fn scan(tuples: &'a Relation) -> Self {
        Self {
            tuples,
            positions: Positions::All(0..tuples.len()),
        }
    }
}

impl<'a> Iterator for Candidates<'a> {
    type Item = &'a Tuple;

    fn next(&mut self) -> Option<&'a Tuple> {
        let position = match &mut self.positions {
            Positions::All(range) => range.next()?,
            Positions::Chain { index, next, short } => {
                let short_position = index.short.get(*short).copied().unwrap_or(NONE);
                let position = (*next).min(short_position);
                if position == NONE {
                    return None;
                }
                if position == *next {
                    *next = index.next[position as usize];
                } else {
                    *short += 1;
                }
                position as usize
            }
        };
        self.tuples.get(position)
    }
}

/// Hash of a lookup key, equal for keys whose values are equal under proof
/// unification.
fn hash_key<'v>(values: impl Iterator<Item = &'v Value>) -> u64 {
    let mut hasher = KeyHasher::default();
    for value in values {
        hash_value(&mut hasher, value);
    }
    hasher.finish()
}

#[allow(clippy::float_cmp)]
fn hash_value(hasher: &mut KeyHasher, value: &Value) {
    match value {
        // Unification compares integers by value across widths.
        Value::Int32(n) => {
            hasher.write_u8(0);
            hasher.write_i64(i64::from(*n));
        }
        Value::Int64(n) => {
            hasher.write_u8(0);
            hasher.write_i64(*n);
        }
        // IEEE equality: -0.0 equals 0.0.
        Value::Float64(f) => {
            hasher.write_u8(1);
            hasher.write_u64(if *f == 0.0 { 0 } else { f.to_bits() });
        }
        other => other.hash(hasher),
    }
}

/// Multiply-rotate word hasher (FxHash) with a final avalanche, so the low
/// bits the hash map buckets on depend on every input bit.
#[derive(Default)]
struct KeyHasher(u64);

impl KeyHasher {
    fn add(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
}

impl Hasher for KeyHasher {
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut word = [0u8; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            self.add(u64::from_le_bytes(word));
        }
    }

    fn write_u8(&mut self, n: u8) {
        self.add(u64::from(n));
    }

    fn write_u64(&mut self, n: u64) {
        self.add(n);
    }

    fn write_i64(&mut self, n: i64) {
        self.add(n as u64);
    }

    fn write_usize(&mut self, n: usize) {
        self.add(n as u64);
    }

    fn finish(&self) -> u64 {
        // murmur3 fmix64
        let mut h = self.0;
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        h ^ (h >> 33)
    }
}

/// Hasher for keys that already are well-mixed `u64` hashes.
#[derive(Default)]
struct PreHashed(u64);

impl Hasher for PreHashed {
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(byte);
        }
    }

    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
#[path = "proof_relations_tests.rs"]
mod tests;
