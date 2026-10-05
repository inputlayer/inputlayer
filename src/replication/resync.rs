//! A checkpoint as replication stream lines, for a follower that cannot
//! catch up from the retained log.
//!
//! ```text
//! S{"begin":{"revision":R,"head":H,"graphs":[..]}}
//! S{"graph":{"name":"kg","indexes":[..]}}
//! C<record: every rule and persistent schema of kg>
//! C<record: up to `chunk` facts of one relation>   (repeated)
//! S{"graph":..}                                    (next graph)
//! S{"end":null}
//! ```
//!
//! The records carry revision `R` and only inserts. The follower collects
//! each graph into a `GraphState` and reconciles its own copy to it.

use super::event::{resync_line, transaction_line, ResyncMark};
use crate::storage::backup::{Checkpoint, KgCheckpoint};
use crate::storage::persist::{CatalogEntry, Transaction};
use crate::storage::StorageResult;

/// Facts per checkpoint record.
pub const CHUNK_TUPLES: usize = 4096;

/// Emit `checkpoint` (holding the events up to LSN `head`) as stream lines,
/// in order, to `sink`. Stops at the first error `sink` returns.
///
/// # Errors
/// A record cannot be encoded, or `sink` failed.
pub fn checkpoint_lines<E: From<crate::storage::StorageError>>(
    checkpoint: &Checkpoint,
    head: u64,
    chunk: usize,
    mut sink: impl FnMut(Vec<u8>) -> Result<(), E>,
) -> Result<(), E> {
    let revision = checkpoint.revision;
    sink(resync_line(&ResyncMark::Begin {
        revision,
        head,
        graphs: checkpoint
            .knowledge_graphs
            .iter()
            .map(|kg| kg.name.clone())
            .collect(),
    }))?;
    for kg in &checkpoint.knowledge_graphs {
        sink(resync_line(&ResyncMark::Graph {
            name: kg.name.clone(),
            indexes: kg.indexes.clone(),
        }))?;
        sink(transaction_line(&catalog_record(kg, revision))?)?;
        for (relation, tuples) in &kg.relations {
            let shard = format!("{}:{relation}", kg.name);
            let mut batch = Vec::with_capacity(chunk.min(tuples.len()));
            for tuple in tuples.iter() {
                batch.push(tuple.clone());
                if batch.len() >= chunk.max(1) {
                    sink(facts_line(&shard, revision, std::mem::take(&mut batch))?)?;
                }
            }
            if !batch.is_empty() {
                sink(facts_line(&shard, revision, batch)?)?;
            }
        }
    }
    sink(resync_line(&ResyncMark::End))
}

/// Every rule and persistent schema of `kg` as one record.
fn catalog_record(kg: &KgCheckpoint, revision: u64) -> Transaction {
    let mut txn = Transaction::new(revision);
    for name in kg.rules.list() {
        let definition = kg.rules.get(&name).cloned();
        txn.catalog(&kg.name, CatalogEntry::Rule { name, definition });
    }
    for schema in kg.schemas.persistent_schemas() {
        txn.catalog(
            &kg.name,
            CatalogEntry::Schema {
                relation: schema.name.clone(),
                schema: Some(schema.clone()),
            },
        );
    }
    txn
}

fn facts_line(
    shard: &str,
    revision: u64,
    tuples: Vec<crate::value::Tuple>,
) -> StorageResult<Vec<u8>> {
    let mut txn = Transaction::new(revision);
    txn.insert(shard, tuples);
    transaction_line(&txn)
}
