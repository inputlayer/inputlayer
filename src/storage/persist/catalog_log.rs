//! Which rule and schema changes the WAL must keep.
//!
//! A knowledge graph keeps its rules and schemas in catalog files, but a commit
//! that changes them is made durable by its WAL record: the files are saved after
//! the record is written, and a crash in between leaves them behind it. The WAL
//! therefore keeps each catalog change until the engine reports that the KG's
//! saved catalog files reflect it ([`CatalogLog::saved`]). Changes up to that
//! revision are dropped at the next rewrite of the WAL. Startup replays the ones
//! still in the WAL over the files.

use super::transaction::{Transaction, TxnOp};
use std::collections::HashMap;

/// Per knowledge graph: the catalog changes in the WAL, and the revision its
/// saved catalog files reflect.
#[derive(Debug, Default)]
pub(super) struct CatalogLog {
    /// KGs with catalog changes in the WAL: the oldest revision that may
    /// still be there, and the newest.
    logged: HashMap<String, Logged>,
    /// Revision each KG's saved catalog files reflect.
    saved: HashMap<String, u64>,
}

#[derive(Debug, Clone, Copy)]
struct Logged {
    oldest: u64,
    newest: u64,
}

impl CatalogLog {
    /// Note the catalog changes of `txn`, just written to the WAL.
    pub fn logged(&mut self, txn: &Transaction) {
        let revision = txn.revision();
        for kg in txn.catalog_kgs() {
            let logged = self.logged.entry(kg.to_string()).or_insert(Logged {
                oldest: revision,
                newest: revision,
            });
            logged.oldest = logged.oldest.min(revision);
            logged.newest = logged.newest.max(revision);
        }
    }

    /// `kg`'s catalog files now reflect every change up to `revision`.
    pub fn saved(&mut self, kg: &str, revision: u64) {
        let saved = self.saved.entry(kg.to_string()).or_insert(0);
        *saved = (*saved).max(revision);
    }

    /// Forget `kg`, whose catalog changes were removed from the WAL.
    pub fn forget(&mut self, kg: &str) {
        self.logged.remove(kg);
        self.saved.remove(kg);
    }

    /// Whether the WAL must keep `op`, committed at `revision`. Fact changes
    /// are not tracked here and always kept.
    pub fn keeps(&self, revision: u64, op: &TxnOp) -> bool {
        match op {
            TxnOp::Facts { .. } => true,
            TxnOp::Catalog { kg, .. } => self.needed(kg, revision),
        }
    }

    /// Whether the WAL may hold catalog changes that a rewrite would drop.
    pub fn has_redundant(&self) -> bool {
        self.logged
            .iter()
            .any(|(kg, logged)| !self.needed(kg, logged.oldest))
    }

    /// After a rewrite kept only what [`Self::keeps`] accepted: forget KGs
    /// none of whose changes remain, and move the others' oldest revision
    /// past what was dropped.
    pub fn pruned(&mut self) {
        let saved = &self.saved;
        self.logged.retain(|kg, logged| match saved.get(kg) {
            Some(&saved) if logged.newest <= saved => false,
            Some(&saved) => {
                logged.oldest = logged.oldest.max(saved + 1);
                true
            }
            None => true,
        });
    }

    fn needed(&self, kg: &str, revision: u64) -> bool {
        self.saved.get(kg).is_none_or(|&saved| revision > saved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::persist::transaction::CatalogEntry;

    fn catalog_txn(revision: u64, kg: &str) -> Transaction {
        let mut txn = Transaction::new(revision);
        txn.catalog(
            kg,
            CatalogEntry::Rule {
                name: "r".into(),
                definition: None,
            },
        );
        txn
    }

    fn op(kg: &str) -> TxnOp {
        catalog_txn(0, kg).ops()[0].clone()
    }

    #[test]
    fn a_change_is_kept_until_its_kg_saves_past_it() {
        let mut log = CatalogLog::default();
        log.logged(&catalog_txn(5, "a"));
        log.logged(&catalog_txn(7, "b"));
        assert!(log.keeps(5, &op("a")));
        assert!(!log.has_redundant());

        log.saved("a", 5);
        assert!(!log.keeps(5, &op("a")));
        assert!(log.keeps(6, &op("a")));
        assert!(log.keeps(7, &op("b")));
        assert!(log.has_redundant());
        log.pruned();
        assert!(!log.has_redundant());
        assert_eq!(log.logged.keys().collect::<Vec<_>>(), ["b"]);

        // A KG saved only part of the way: the rest stays, and once the
        // saved part is pruned nothing is redundant until it saves again.
        log.logged(&catalog_txn(8, "b"));
        log.saved("b", 7);
        assert!(log.has_redundant());
        log.pruned();
        assert!(!log.has_redundant());
        assert!(log.keeps(8, &op("b")));

        log.saved("b", 9);
        assert!(!log.keeps(7, &op("b")));
        log.pruned();
        assert!(log.logged.is_empty());
    }

    #[test]
    fn saved_revision_never_moves_back() {
        let mut log = CatalogLog::default();
        log.logged(&catalog_txn(5, "a"));
        log.saved("a", 5);
        log.saved("a", 3);
        assert!(!log.keeps(5, &op("a")));
    }

    #[test]
    fn fact_changes_are_always_kept() {
        let mut txn = Transaction::new(1);
        txn.insert("a:r", [crate::value::Tuple::from_pair(1, 1)]);
        let log = CatalogLog::default();
        assert!(log.keeps(1, &txn.ops()[0]));
    }
}
