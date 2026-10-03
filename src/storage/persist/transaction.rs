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
//! Each kind of change has its own [`TxnOp`] variant with a typed payload, never
//! strings to re-parse on replay. A record holding a variant this server does not
//! know fails startup instead of being skipped; see the `wal_record` module.
//!
//! Rule and schema changes ([`TxnOp::Catalog`]) carry the state the commit leaves
//! behind, not the edit that produced it, so replaying one over a catalog that
//! already reflects it changes nothing.

use super::batch::Update;
use crate::rule_catalog::RuleDefinition;
use crate::schema::RelationSchema;
use crate::value::Tuple;

/// One kind of change inside a [`Transaction`].
#[derive(Debug, Clone, PartialEq)]
pub enum TxnOp {
    /// Fact changes to one shard (`{kg}:{relation}`): each tuple with its
    /// multiplicity change (+1 insert, -1 delete).
    Facts {
        /// Shard the facts belong to.
        shard: String,
        /// Tuples and their multiplicity changes.
        changes: Vec<(Tuple, i64)>,
    },
    /// A rule or schema of knowledge graph `kg` as the commit leaves it.
    Catalog {
        /// Knowledge graph whose catalog changes.
        kg: String,
        /// The new state of one catalog entry.
        entry: CatalogEntry,
    },
}

/// The state of one rule or persistent schema after a commit.
#[derive(Debug, Clone, PartialEq)]
pub enum CatalogEntry {
    /// Every clause of rule `name`, or `None` when the commit removes the rule.
    Rule {
        name: String,
        definition: Option<RuleDefinition>,
    },
    /// The persistent schema of `relation`, or `None` when the commit removes it.
    Schema {
        relation: String,
        schema: Option<RelationSchema>,
    },
}

/// A catalog change read back from the WAL, with the revision it committed at.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogRecord {
    pub revision: u64,
    pub kg: String,
    pub entry: CatalogEntry,
}

/// All changes of one commit, applied atomically at one revision.
#[derive(Debug, Clone, PartialEq)]
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

    /// Add the new state of one catalog entry of `kg`.
    pub fn catalog(&mut self, kg: impl Into<String>, entry: CatalogEntry) -> &mut Self {
        self.ops.push(TxnOp::Catalog {
            kg: kg.into(),
            entry,
        });
        self
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

    /// Shards the transaction writes facts to, in commit order (may repeat).
    pub fn shards(&self) -> impl Iterator<Item = &str> {
        self.ops.iter().filter_map(|op| match op {
            TxnOp::Facts { shard, .. } => Some(shard.as_str()),
            TxnOp::Catalog { .. } => None,
        })
    }

    /// Knowledge graphs whose catalog the transaction changes (may repeat).
    pub fn catalog_kgs(&self) -> impl Iterator<Item = &str> {
        self.ops.iter().filter_map(|op| match op {
            TxnOp::Catalog { kg, .. } => Some(kg.as_str()),
            TxnOp::Facts { .. } => None,
        })
    }

    /// Keep only the changes `keep` accepts. Returns whether anything was dropped.
    pub fn retain(&mut self, keep: impl FnMut(&TxnOp) -> bool) -> bool {
        let before = self.ops.len();
        self.ops.retain(keep);
        self.ops.len() != before
    }

    /// Drop every change to `shard`. Returns whether anything was dropped.
    pub fn remove_shard(&mut self, shard: &str) -> bool {
        self.retain(|op| !matches!(op, TxnOp::Facts { shard: s, .. } if s == shard))
    }

    /// Split into per-shard [`Update`]s stamped with the revision, and the
    /// catalog changes, each in commit order.
    pub fn split(self) -> (Vec<(String, Vec<Update>)>, Vec<CatalogRecord>) {
        let revision = self.revision;
        let mut updates = Vec::new();
        let mut catalog = Vec::new();
        for op in self.ops {
            match op {
                TxnOp::Facts { shard, changes } => {
                    let shard_updates = changes
                        .into_iter()
                        .map(|(data, diff)| Update {
                            data,
                            time: revision,
                            diff,
                        })
                        .collect();
                    updates.push((shard, shard_updates));
                }
                TxnOp::Catalog { kg, entry } => catalog.push(CatalogRecord {
                    revision,
                    kg,
                    entry,
                }),
            }
        }
        (updates, catalog)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn dropped_rule(name: &str) -> CatalogEntry {
        CatalogEntry::Rule {
            name: name.to_string(),
            definition: None,
        }
    }

    #[test]
    fn empty_change_lists_add_no_op() {
        let mut txn = Transaction::new(3);
        txn.insert("kg:a", Vec::new()).facts("kg:b", Vec::new());
        assert!(txn.is_empty());
    }

    #[test]
    fn split_stamps_updates_with_the_revision_and_keeps_catalog_order() {
        let mut txn = Transaction::new(7);
        txn.insert("kg:a", [Tuple::from_pair(1, 2)])
            .catalog("kg", dropped_rule("r"))
            .delete("kg:b", [Tuple::from_pair(3, 4)])
            .catalog("kg", dropped_rule("s"));
        let (updates, catalog) = txn.split();
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
        let names: Vec<_> = catalog
            .iter()
            .map(|record| (record.revision, record.kg.as_str(), &record.entry))
            .collect();
        assert_eq!(
            names,
            vec![(7, "kg", &dropped_rule("r")), (7, "kg", &dropped_rule("s"))]
        );
    }

    #[test]
    fn remove_shard_keeps_other_shards_and_catalog_changes() {
        let mut txn = Transaction::new(1);
        txn.insert("kg:a", [Tuple::from_pair(1, 1)])
            .insert("kg:b", [Tuple::from_pair(2, 2)])
            .catalog("kg", dropped_rule("r"))
            .delete("kg:a", [Tuple::from_pair(3, 3)]);
        assert!(txn.remove_shard("kg:a"));
        assert!(!txn.remove_shard("kg:a"));
        assert_eq!(txn.shards().collect::<Vec<_>>(), ["kg:b"]);
        assert_eq!(txn.catalog_kgs().collect::<Vec<_>>(), ["kg"]);
    }
}
