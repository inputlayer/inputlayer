//! `.backup [name]` and `.backup status`: online checkpoint exports.
//!
//! `.backup` claims `<storage.backup_dir>/<name>`, captures a checkpoint
//! (holding off commits only for the capture), and returns once the
//! background export has started. `.backup status` reports the running
//! export, or how the last one ended. Both are admin-only (see `auth`).

use super::super::wire::ErrorCode;
use crate::storage::backup::BackupError;
use crate::storage_engine::{ExportStatus, StorageEngine};

/// Start an export into the configured backup directory, named `name` or
/// after the current time. Returns the message rows.
pub(super) fn start(
    storage: &StorageEngine,
    name: Option<&str>,
) -> Result<Vec<String>, (ErrorCode, String)> {
    let started = storage
        .checkpoint_destination(name)
        .and_then(|dest| storage.start_checkpoint_export(&dest))
        .map_err(|e| (error_code(&e), format!("Backup failed: {e}")))?;
    Ok(vec![
        format!(
            "Exporting the checkpoint at revision {} to {}.",
            started.revision,
            started.dir.display()
        ),
        format!(
            "Commits were held for {} us; check progress with .backup status.",
            started.capture_time.as_micros()
        ),
    ])
}

/// The message rows describing the running or last export.
pub(super) fn status(storage: &StorageEngine) -> Vec<String> {
    match storage.checkpoint_export_status() {
        ExportStatus::Idle => vec!["No checkpoint export has run since the server started.".into()],
        ExportStatus::Running { dir, revision } => vec![format!(
            "Exporting the checkpoint at revision {revision} to {}.",
            dir.display()
        )],
        ExportStatus::Finished {
            revision,
            capture_time,
            outcome: Ok(report),
            ..
        } => vec![
            format!(
                "Last export complete: revision {revision} in {}.",
                report.dir.display()
            ),
            format!(
                "{} files, {} bytes; commits held {} us, written in {} ms.",
                report.files,
                report.bytes,
                capture_time.as_micros(),
                report.elapsed.as_millis()
            ),
        ],
        ExportStatus::Finished {
            dir,
            revision,
            outcome: Err(error),
            ..
        } => vec![format!(
            "Last export of revision {revision} to {} failed: {error}",
            dir.display()
        )],
    }
}

fn error_code(error: &BackupError) -> ErrorCode {
    match error {
        BackupError::InvalidName(_)
        | BackupError::NoBackupDir
        | BackupError::DestinationExists(_)
        | BackupError::DestinationInsideSource { .. }
        | BackupError::InvalidDestination(_) => ErrorCode::Validation,
        BackupError::ExportRunning(_) => ErrorCode::Conflict,
        _ => ErrorCode::Internal,
    }
}
