//! `inputlayer-backup` - offline backup and restore of a data directory.
//!
//! The server must be stopped: `create` takes the data directory lock and
//! refuses a directory a server is running on. Every command writes only
//! into a new or empty directory, never over existing data.
//!
//! ```bash
//! # Back up the configured data directory (server stopped)
//! inputlayer-backup create /backups/il-2026-10-03
//!
//! # Check a backup is complete and intact (read-only)
//! inputlayer-backup verify /backups/il-2026-10-03
//!
//! # Restore into a fresh directory, then load it with the engine
//! inputlayer-backup restore /backups/il-2026-10-03 /var/lib/inputlayer/data-restored
//! ```
//!
//! See `docs/guides/backup.md` for the full runbook.

use clap::{Parser, Subcommand};
use inputlayer::storage::backup::{self, Report};
use inputlayer::{Config, StorageEngine};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// Offline backup and restore for an InputLayer data directory
#[derive(Parser, Debug)]
#[command(name = "inputlayer-backup", version, about)]
struct Cli {
    /// Configuration file (TOML); default: the server's config lookup
    /// (config.toml, config.local.toml, INPUTLAYER_* env)
    #[arg(long, short, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Copy a stopped server's data directory into a new backup directory
    Create {
        /// Directory to write the backup into (must be new or empty)
        backup_dir: PathBuf,
        /// Data directory to back up (default: storage.data_dir from config)
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Check a backup is complete and every file matches its manifest
    Verify {
        /// Backup directory
        backup_dir: PathBuf,
    },
    /// Restore a backup into a new data directory and load it to validate
    Restore {
        /// Backup directory
        backup_dir: PathBuf,
        /// Data directory to restore into (must be new or empty)
        target_dir: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Create {
            backup_dir,
            data_dir,
        } => {
            let data_dir = match data_dir {
                Some(dir) => dir,
                None => load_config(cli.config.as_deref())?.storage.data_dir,
            };
            let report = backup::create(&data_dir, &backup_dir).map_err(|e| e.to_string())?;
            print_report("backup created", &report);
            println!("source:  {}", data_dir.display());
        }
        Command::Verify { backup_dir } => {
            let report = backup::verify(&backup_dir).map_err(|e| e.to_string())?;
            print_report("backup verified", &report);
        }
        Command::Restore {
            backup_dir,
            target_dir,
        } => {
            let config = load_config(cli.config.as_deref())?;
            let report = backup::restore(&backup_dir, &target_dir).map_err(|e| e.to_string())?;
            print_report("backup restored", &report);
            validate_restored(config, &report.dir)?;
        }
    }
    Ok(())
}

fn load_config(path: Option<&Path>) -> Result<Config, String> {
    match path {
        Some(path) => Config::from_file(&path.to_string_lossy()),
        None => Config::load(),
    }
    .map_err(|e| format!("invalid configuration: {e}"))
}

/// Open the restored directory exactly as the server would at startup
/// (WAL replay, shard and catalog load) and print what it holds.
///
/// The files already match the manifest byte for byte, so a failure here
/// means this engine version cannot load the backup.
fn validate_restored(mut config: Config, target: &Path) -> Result<(), String> {
    config.storage.data_dir = target.to_path_buf();
    let engine = StorageEngine::new(config).map_err(|e| {
        format!(
            "restored files match the backup but the engine cannot load {}: {e}; \
             do not start a server on it",
            target.display()
        )
    })?;
    println!("validated: the engine loads {}", target.display());
    for kg in engine.list_knowledge_graphs() {
        let relations = engine
            .list_relations_with_metadata(&kg)
            .map_err(|e| e.to_string())?;
        let rules = engine.list_rules_in(&kg).map_err(|e| e.to_string())?;
        let facts: usize = relations.iter().map(|(_, _, count)| count).sum();
        println!(
            "  {kg}: {} relations, {facts} facts, {} rules",
            relations.len(),
            rules.len()
        );
    }
    Ok(())
}

fn print_report(what: &str, report: &Report) {
    println!("{what}: {}", report.dir.display());
    println!(
        "         {} files, {} directories, {} bytes in {} ms",
        report.files,
        report.directories,
        report.bytes,
        report.elapsed.as_millis()
    );
}
