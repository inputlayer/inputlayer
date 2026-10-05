//! A checkpoint: every knowledge graph's committed state, each at its own revision,
//! captured from a running engine by `StorageEngine::capture_checkpoint`.
//!
//! A checkpoint shares data with the engine instead of copying it: each
//! relation shares its tuple chunks with the engine's published snapshots, so
//! taking one costs O(relations + chunks) reference-count increments. The
//! captured chunks stay alive until the checkpoint is dropped; writes that
//! continue meanwhile build new chunks and never change it.

use crate::index_manager::RegisteredIndex;
use crate::rule_catalog::RuleCatalog;
use crate::schema::SchemaCatalog;
use crate::value::Relation;
use std::time::Duration;

/// Every knowledge graph's committed state, each at its own revision.
#[derive(Debug)]
pub struct Checkpoint {
    /// The newest revision it holds: each knowledge graph holds every commit
    /// to it up to its own revision, at most this one, and none after.
    pub revision: u64,
    /// How long commits were held off while it was captured.
    pub capture_time: Duration,
    /// The knowledge graphs, by name.
    pub knowledge_graphs: Vec<KgCheckpoint>,
}

/// One knowledge graph's committed state.
#[derive(Debug)]
pub struct KgCheckpoint {
    /// Knowledge graph name.
    pub name: String,
    /// When the knowledge graph was created (RFC 3339).
    pub created_at: String,
    /// Base relations that hold facts, by name. Derived relations are not
    /// stored: the rules recompute them.
    pub relations: Vec<(String, Relation)>,
    /// Persistent rules, as a copy that never saves to the engine's files.
    pub rules: RuleCatalog,
    /// Persistent schemas; session schemas are never saved.
    pub schemas: SchemaCatalog,
    /// Vector index definitions; the indexes are rebuilt from the facts.
    pub indexes: Vec<RegisteredIndex>,
}

impl Checkpoint {
    /// Total number of facts in every knowledge graph.
    pub fn tuple_count(&self) -> usize {
        self.knowledge_graphs
            .iter()
            .map(KgCheckpoint::tuple_count)
            .sum()
    }
}

impl KgCheckpoint {
    /// Number of facts in the knowledge graph's base relations.
    pub fn tuple_count(&self) -> usize {
        self.relations.iter().map(|(_, r)| r.len()).sum()
    }
}
