//! Transactions: the unit of atomic, durable change.
//!
//! A [`Transaction`] groups every change one commit makes, across any number of
//! relations, under a single revision. The WAL stores it as one record, so after a
//! crash it is replayed either completely or not at all. A single-relation write is
//! a transaction with one operation.
//!
//! Every change in a transaction happens at its revision: fact changes carry only
//! their tuple and multiplicity change, and replay stamps them with the revision.
//! A transaction cannot mix logical times.
//!
//! Each kind of change has its own [`TxnOp`] variant with a typed payload. New kinds
//! of change (rules, schemas) are added as variants, never as strings to re-parse on
//! replay. A record holding a variant this server does not know fails startup
//! instead of being skipped; see the `wal_record` module.

use super::batch::Update;
use crate::value::Tuple;

/// One kind of change inside a [`Transaction`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnOp {
    /// Fact changes to one shard (`{kg}:{relation}`): each tuple with its
    /// multiplicity change (+1 insert, -1 delete).
    Facts {
        /// Shard the facts belong to.
        shard: String,
        /// Tuples and their multiplicity changes.
        changes: Vec<(Tuple, i64)>,
    },
}

/// All changes of one commit, applied atomically at one revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transaction {
    /// Logical time of the commit; every change in it happens at this time.
    revision: u64,
    /// Changes in commit order.
    ops: Vec<TxnOp>,
}

impl Transaction {
    /// An empty transaction committing at `revision`.
    pub fn new(revision: u64) -> Self {
        Transaction {
            revision,
            ops: Vec::new(),
        }
    }

    /// Add fact changes to `shard`. Empty change lists add nothing.
    pub fn facts(&mut self, shard: impl Into<String>, changes: Vec<(Tuple, i64)>) -> &mut Self {
        if !changes.is_empty() {
            self.ops.push(TxnOp::Facts {
                shard: shard.into(),
                changes,
            });
        }
        self
    }

    /// Add inserts of `tuples` into `shard`.
    pub fn insert(
        &mut self,
        shard: impl Into<String>,
        tuples: impl IntoIterator<Item = Tuple>,
    ) -> &mut Self {
        self.facts(shard, tuples.into_iter().map(|t| (t, 1)).collect())
    }

    /// Add deletes of `tuples` from `shard`.
    pub fn delete(
        &mut self,
        shard: impl Into<String>,
        tuples: impl IntoIterator<Item = Tuple>,
    ) -> &mut Self {
        self.facts(shard, tuples.into_iter().map(|t| (t, -1)).collect())
    }

    /// Logical time of the commit.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// The changes, in commit order.
    pub fn ops(&self) -> &[TxnOp] {
        &self.ops
    }

    /// Whether the transaction changes nothing.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Shards the transaction writes, in commit order (may repeat).
    pub fn shards(&self) -> impl Iterator<Item = &str> {
        self.ops.iter().map(|op| match op {
            TxnOp::Facts { shard, .. } => shard.as_str(),
        })
    }

    /// Drop every change to `shard`. Returns whether anything was dropped.
    pub fn remove_shard(&mut self, shard: &str) -> bool {
        let before = self.ops.len();
        self.ops
            .retain(|op| !matches!(op, TxnOp::Facts { shard: s, .. } if s == shard));
        self.ops.len() != before
    }

    /// Split into per-shard [`Update`]s stamped with the revision, in commit order.
    pub fn into_updates(self) -> impl Iterator<Item = (String, Vec<Update>)> {
        let time = self.revision;
        self.ops.into_iter().map(move |op| match op {
            TxnOp::Facts { shard, changes } => {
                let updates = changes
                    .into_iter()
                    .map(|(data, diff)| Update { data, time, diff })
                    .collect();
                (shard, updates)
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn empty_change_lists_add_no_op() {
        let mut txn = Transaction::new(3);
        txn.insert("kg:a", Vec::new()).facts("kg:b", Vec::new());
        assert!(txn.is_empty());
    }

    #[test]
    fn updates_carry_the_revision() {
        let mut txn = Transaction::new(7);
        txn.insert("kg:a", [Tuple::from_pair(1, 2)])
            .delete("kg:b", [Tuple::from_pair(3, 4)]);
        let updates: Vec<_> = txn.into_updates().collect();
        assert_eq!(
            updates,
            vec![
                (
                    "kg:a".into(),
                    vec![Update::insert(Tuple::from_pair(1, 2), 7)]
                ),
                (
                    "kg:b".into(),
                    vec![Update::delete(Tuple::from_pair(3, 4), 7)]
                ),
            ]
        );
    }

    #[test]
    fn remove_shard_keeps_other_shards() {
        let mut txn = Transaction::new(1);
        txn.insert("kg:a", [Tuple::from_pair(1, 1)])
            .insert("kg:b", [Tuple::from_pair(2, 2)])
            .delete("kg:a", [Tuple::from_pair(3, 3)]);
        assert!(txn.remove_shard("kg:a"));
        assert!(!txn.remove_shard("kg:a"));
        assert_eq!(txn.shards().collect::<Vec<_>>(), ["kg:b"]);
    }
}
