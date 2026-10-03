//! Online checkpoints: a consistent backup of a running engine.
//!
//! [`StorageEngine::capture_checkpoint`] takes every knowledge graph's read
//! lock at once. Every commit assigns its revision and applies its changes
//! under its knowledge graph's write lock, so with all read locks held no
//! commit is in flight: every revision assigned so far is fully applied (or
//! failed and applied nothing), and the newest one names the checkpoint.
//! Knowledge graph creation waits for the capture too, so a graph created
//! meanwhile cannot hold a commit the checkpoint misses.
//!
//! The capture only clones shared references (see [`Checkpoint`]), so it holds
//! off commits for O(knowledge graphs + relations) work. Queries read
//! published snapshots and do not take these locks.
//!
//! [`StorageEngine::start_checkpoint_export`] then writes the checkpoint on
//! one background thread with no engine lock held. One export runs at a time;
//! dropping the engine cancels a running export and waits for it to remove
//! what it wrote.

use super::{KnowledgeGraph, StorageEngine};
use crate::storage::backup::{
    self, BackupError, BackupResult, Checkpoint, Export, KgCheckpoint, Report,
};
use parking_lot::Mutex;
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
    /// Capture every knowledge graph's committed state at one revision; see
    /// the module documentation for why it is consistent.
    pub fn capture_checkpoint(&self) -> Checkpoint {
        let start = Instant::now();
        let _no_new_kgs = self.kg_set.write();
        let handles: Vec<_> = self
            .knowledge_graphs
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        // Writers each take one KG lock, so read locks in any order cannot deadlock.
        let guards: Vec<_> = handles.iter().map(|kg| kg.read()).collect();
        let revision = self.logical_time.load(Ordering::SeqCst).saturating_sub(1);
        let mut knowledge_graphs: Vec<KgCheckpoint> = guards
            .iter()
            .filter(|kg| !kg.dropped)
            .map(|kg| capture_kg(kg))
            .collect();
        drop(guards);
        let capture_time = start.elapsed();

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
            "checkpoint_captured"
        );
        checkpoint
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
        let checkpoint = self.capture_checkpoint();
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
