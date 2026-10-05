//! Online checkpoints: a consistent backup of a running engine.
//!
//! Knowledge graphs are independent, so a checkpoint holds each one
//! consistent at its own revision: every commit to it up to that revision,
//! and none after.
//!
//! [`StorageEngine::capture_checkpoint`] takes every loaded knowledge graph's
//! read lock at once. Every commit assigns its revision and applies its changes
//! under its knowledge graph's write lock, so with all read locks held no
//! commit is in flight: every revision assigned so far is fully applied (or
//! failed and applied nothing), and the newest one is these graphs' revision.
//! Knowledge graph creation waits for this capture too, so a graph created
//! meanwhile cannot hold a commit the checkpoint misses. This capture only
//! clones shared references (see [`Checkpoint`]), so it holds off commits for
//! O(knowledge graphs + relations) work. Queries read published snapshots and
//! do not take these locks.
//!
//! Each knowledge graph that was not loaded is then captured on its own: its
//! slot is held, so it takes no commit, only while its revision, shard list
//! and catalogs are read. Its facts are read from disk after, up to that
//! revision, so a backup never keeps a request waiting for a knowledge graph
//! to load.
//!
//! [`StorageEngine::start_checkpoint_export`] then writes the checkpoint on
//! one background thread with no engine lock held. One export runs at a time;
//! dropping the engine cancels a running export and waits for it to remove
//! what it wrote.

use super::catalog_change::apply_catalog_records;
use super::residency::KgSlot;
use super::{KnowledgeGraph, StorageEngine};
use crate::index_manager::{IndexManager, INDEX_DEFINITIONS_FILE};
use crate::storage::backup::{
    self, BackupError, BackupResult, Checkpoint, Export, KgCheckpoint, Report,
};
use crate::storage::persist::{consolidate_to_current, into_tuples, PersistBackend};
use crate::storage::{KnowledgeGraphMetadata, StorageResult};
use crate::value::{Relation, Tuple};
use parking_lot::Mutex;
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// What the engine's online checkpoint export is doing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportStatus {
    /// No export has run since the engine started.
    Idle,
    /// An export is being written.
    Running {
        /// Directory being written.
        dir: PathBuf,
        /// Revision being exported.
        revision: u64,
    },
    /// The last export finished.
    Finished {
        /// Directory written (removed again on failure).
        dir: PathBuf,
        /// Revision exported.
        revision: u64,
        /// How long commits were held off for the capture.
        capture_time: Duration,
        /// What was written, or why the export failed.
        outcome: Result<Report, String>,
    },
}

/// A started export.
#[derive(Debug)]
pub struct CheckpointExport {
    /// Revision being exported.
    pub revision: u64,
    /// Directory being written.
    pub dir: PathBuf,
    /// How long commits were held off for the capture.
    pub capture_time: Duration,
    done: mpsc::Receiver<BackupResult<Report>>,
}

impl CheckpointExport {
    /// Wait for the export to finish.
    ///
    /// # Errors
    /// Why the export failed; it removed what it wrote.
    pub fn wait(self) -> BackupResult<Report> {
        self.done.recv().unwrap_or(Err(BackupError::Cancelled))
    }
}

/// The engine's single export slot.
#[derive(Debug)]
pub(super) struct CheckpointExports {
    status: Arc<Mutex<ExportStatus>>,
    cancel: Arc<AtomicBool>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl Default for CheckpointExports {
    fn default() -> Self {
        Self {
            status: Arc::new(Mutex::new(ExportStatus::Idle)),
            cancel: Arc::new(AtomicBool::new(false)),
            worker: Mutex::new(None),
        }
    }
}

impl Drop for CheckpointExports {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.get_mut().take() {
            let _ = worker.join();
        }
    }
}

impl StorageEngine {
    /// Capture every knowledge graph's committed state, each at its own
    /// revision; see the module documentation for why each is consistent.
    ///
    /// # Errors
    /// Reading a knowledge graph that is not loaded failed.
    pub fn capture_checkpoint(&self) -> BackupResult<Checkpoint> {
        Ok(self.capture_checkpoint_at_lsn()?.0)
    }

    /// [`Self::capture_checkpoint`], with the replication log's head LSN
    /// read under the loaded graphs' locks (0 when this engine is not a
    /// primary). Every replication event is appended under one of those
    /// locks, so the loaded graphs hold exactly the events up to that LSN; a
    /// graph captured on its own after may hold later ones too, which
    /// replaying from that LSN applies again without changing it.
    ///
    /// # Errors
    /// Reading a knowledge graph that is not loaded failed.
    pub fn capture_checkpoint_at_lsn(&self) -> BackupResult<(Checkpoint, u64)> {
        let start = Instant::now();
        let no_new_kgs = self.kg_set.write();
        let slots = self.slots();
        let handles: Vec<_> = slots.iter().filter_map(|(_, slot)| slot.graph()).collect();
        // Writers each take one KG lock, so read locks in any order cannot deadlock.
        let guards: Vec<_> = handles.iter().map(|kg| kg.read()).collect();
        let mut revision = self.logical_time.load(Ordering::SeqCst).saturating_sub(1);
        let head = self.replication_log().map_or(0, |log| log.head());
        let mut knowledge_graphs: Vec<KgCheckpoint> = guards
            .iter()
            .filter(|kg| kg.retired.is_none())
            .map(|kg| capture_kg(kg))
            .collect();
        drop(guards);
        drop(no_new_kgs);
        let capture_time = start.elapsed();

        // Unloaded (or dropped) while not held: each of the rest on its own.
        let captured: std::collections::HashSet<String> =
            knowledge_graphs.iter().map(|kg| kg.name.clone()).collect();
        for (name, slot) in &slots {
            if captured.contains(name) {
                continue;
            }
            if let Some((kg, at)) = self.capture_alone(name, slot)? {
                revision = revision.max(at);
                knowledge_graphs.push(kg);
            }
        }

        knowledge_graphs.sort_by(|a, b| a.name.cmp(&b.name));
        for kg in &mut knowledge_graphs {
            kg.relations.sort_by(|a, b| a.0.cmp(&b.0));
        }
        let checkpoint = Checkpoint {
            revision,
            capture_time,
            knowledge_graphs,
        };
        info!(
            revision,
            knowledge_graphs = checkpoint.knowledge_graphs.len(),
            tuples = checkpoint.tuple_count(),
            capture_us = capture_time.as_micros() as u64,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "checkpoint_captured"
        );
        Ok((checkpoint, head))
    }

    /// Capture `name` at its own revision, or `None` once it is dropped. A
    /// loaded graph is captured under its read lock. A dormant one is held
    /// only while its revision, shards and catalogs are read, then its facts
    /// are read up to that revision; a shard dropped meanwhile starts it over.
    fn capture_alone(
        &self,
        name: &str,
        slot: &KgSlot,
    ) -> BackupResult<Option<(KgCheckpoint, u64)>> {
        loop {
            let held = slot.hold();
            if let Some(graph) = held.graph() {
                let kg = graph.read();
                let revision = self.logical_time.load(Ordering::SeqCst).saturating_sub(1);
                return Ok(kg.retired.is_none().then(|| (capture_kg(&kg), revision)));
            }
            let Some(dormant) = held.dormant() else {
                return Ok(None);
            };
            // Held: no commit to it is in flight, and any later one gets a
            // later revision.
            let revision = self.logical_time.load(Ordering::SeqCst).saturating_sub(1);
            let mut shards = Vec::new();
            for (shard, relation) in self.live_shards(name, dormant) {
                if self.persist.has_shard(shard) {
                    let since = self.persist.shard_info(shard)?.since;
                    shards.push((shard.clone(), relation.clone(), since));
                }
            }
            let data_dir = self.config.storage.data_dir.join(name);
            let (mut rules, mut schemas) = KnowledgeGraph::load_catalogs(name, &data_dir)?;
            apply_catalog_records(&mut rules, &mut schemas, dormant.catalog.iter());
            let indexes = IndexManager::load_definitions(&data_dir.join(INDEX_DEFINITIONS_FILE))
                .unwrap_or_else(|e| {
                    warn!(kg = %name, error = %e, "index_definitions_load_failed");
                    Vec::new()
                });
            let created_at = held
                .created_at()
                .unwrap_or_else(|| KnowledgeGraphMetadata::new(name.to_string()).created_at);
            drop(held);

            #[cfg(test)]
            BEFORE_DORMANT_READ.with(|hook| hook.borrow_mut().as_mut().map(|hook| hook(name)));
            let relations: Option<Vec<(String, Vec<Tuple>)>> = shards
                .par_iter()
                .map(|(shard, relation, since)| {
                    let tuples = self.read_shard_at(shard, *since, revision)?;
                    Ok(tuples.map(|tuples| (relation.clone(), tuples)))
                })
                .collect::<StorageResult<_>>()?;
            let Some(relations) = relations else {
                continue;
            };
            let kg = KgCheckpoint {
                name: name.to_string(),
                created_at,
                relations: relations
                    .into_iter()
                    .filter(|(_, tuples)| !tuples.is_empty())
                    .map(|(relation, tuples)| (relation, Relation::from(tuples)))
                    .collect(),
                rules: rules.detached(),
                schemas,
                indexes,
            };
            return Ok(Some((kg, revision)));
        }
    }

    /// The facts `shard` holds at `revision`, or `None` once it is deleted.
    fn read_shard_at(
        &self,
        shard: &str,
        since: u64,
        revision: u64,
    ) -> StorageResult<Option<Vec<Tuple>>> {
        let mut updates = match self.persist.read(shard, since) {
            Ok(updates) => updates,
            Err(_) if !self.persist.has_shard(shard) => return Ok(None),
            Err(e) => return Err(e),
        };
        updates.retain(|update| update.time <= revision);
        consolidate_to_current(&mut updates);
        Ok(Some(into_tuples(updates)))
    }

    /// Where an export named `name` (or a time-derived name) goes: inside
    /// the configured `storage.backup_dir`.
    ///
    /// # Errors
    /// No backup directory is configured, or `name` is not a plain name.
    pub fn checkpoint_destination(&self, name: Option<&str>) -> BackupResult<PathBuf> {
        let root = self
            .config
            .storage
            .backup_dir
            .as_ref()
            .ok_or(BackupError::NoBackupDir)?;
        Ok(root.join(backup::checkpoint_name(name)?))
    }

    /// Claim `dest` (a new or empty directory outside the data directory),
    /// capture a checkpoint, and write it there on a background thread.
    ///
    /// # Errors
    /// [`BackupError::ExportRunning`] while another export is being written;
    /// or `dest` is refused. Nothing is captured in either case.
    pub fn start_checkpoint_export(&self, dest: &Path) -> BackupResult<CheckpointExport> {
        let exports = &self.checkpoint_exports;
        let mut status = exports.status.lock();
        if let ExportStatus::Running { dir, .. } = &*status {
            return Err(BackupError::ExportRunning(dir.clone()));
        }
        let export = Export::claim(dest, &self.config.storage.data_dir)?;
        let dir = export.dir().to_path_buf();
        let checkpoint = match self.capture_checkpoint() {
            Ok(checkpoint) => checkpoint,
            Err(e) => {
                export.abandon()?;
                return Err(e);
            }
        };
        let (revision, capture_time) = (checkpoint.revision, checkpoint.capture_time);
        let (done_tx, done) = mpsc::channel();

        let shared_status = Arc::clone(&exports.status);
        let cancel = Arc::clone(&exports.cancel);
        let target = dir.clone();
        let mut worker = exports.worker.lock();
        if let Some(previous) = worker.take() {
            let _ = previous.join();
        }
        let (export_tx, export_rx) = mpsc::channel::<Export>();
        let spawned = std::thread::Builder::new()
            .name("checkpoint-export".to_string())
            .spawn(move || {
                let Ok(export) = export_rx.recv() else {
                    return;
                };
                let result = export.write(&checkpoint, &cancel);
                drop(checkpoint);
                log_outcome(&target, revision, &result);
                *shared_status.lock() = ExportStatus::Finished {
                    dir: target,
                    revision,
                    capture_time,
                    outcome: result
                        .as_ref()
                        .map(Clone::clone)
                        .map_err(ToString::to_string),
                };
                let _ = done_tx.send(result);
            });
        match spawned {
            Ok(handle) => *worker = Some(handle),
            Err(source) => {
                export.abandon()?;
                return Err(BackupError::Io { path: dir, source });
            }
        }
        // The worker writes nothing until it receives the export. Still under
        // the status lock, so its `Finished` lands after this `Running`.
        let _ = export_tx.send(export);
        *status = ExportStatus::Running {
            dir: dir.clone(),
            revision,
        };
        Ok(CheckpointExport {
            revision,
            dir,
            capture_time,
            done,
        })
    }

    /// What the online checkpoint export is doing, or last did.
    pub fn checkpoint_export_status(&self) -> ExportStatus {
        self.checkpoint_exports.status.lock().clone()
    }
}

/// Clone `kg`'s committed state. Runs with commits held off, so it only
/// clones shared references and the (small) catalogs.
fn capture_kg(kg: &KnowledgeGraph) -> KgCheckpoint {
    let mut schemas = kg.schema_catalog.clone();
    schemas.clear_session();
    KgCheckpoint {
        name: kg.name.clone(),
        created_at: kg.metadata.created_at.clone(),
        relations: kg
            .store
            .relations()
            .iter()
            .filter(|(_, tuples)| !tuples.is_empty())
            .map(|(name, tuples)| (name.clone(), tuples.clone()))
            .collect(),
        rules: kg.rule_catalog.detached(),
        schemas,
        indexes: kg.indexes.definitions(),
    }
}

#[cfg(test)]
thread_local! {
    /// Run by a capture on this thread after it releases a dormant knowledge
    /// graph's slot and before it reads its facts.
    pub(super) static BEFORE_DORMANT_READ: std::cell::RefCell<Option<Box<dyn FnMut(&str)>>> =
        const { std::cell::RefCell::new(None) };
}

fn log_outcome(dir: &Path, revision: u64, result: &BackupResult<Report>) {
    match result {
        Ok(report) => info!(
            dir = %report.dir.display(),
            revision,
            files = report.files,
            bytes = report.bytes,
            write_ms = report.elapsed.as_millis() as u64,
            "checkpoint_exported"
        ),
        Err(e) => warn!(dir = %dir.display(), revision, error = %e, "checkpoint_export_failed"),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::Config;

    #[test]
    fn a_second_export_is_refused_while_one_runs() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.data_dir = tmp.path().join("data");
        let engine = StorageEngine::new(config).unwrap();
        let running = tmp.path().join("first");
        *engine.checkpoint_exports.status.lock() = ExportStatus::Running {
            dir: running.clone(),
            revision: 1,
        };

        let second = tmp.path().join("second");
        let refused = engine.start_checkpoint_export(&second);

        assert!(
            matches!(&refused, Err(BackupError::ExportRunning(dir)) if *dir == running),
            "{refused:?}"
        );
        assert!(!second.exists(), "a refused export claims nothing");
    }
}
