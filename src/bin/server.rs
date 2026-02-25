//! `InputLayer` Server Binary
//!
//! Starts an `InputLayer` server with WebSocket API and optional GUI.
//!
//! ## Usage
//!
//! ```bash
//! # Start server with default settings
//! inputlayer-server
//!
//! # Start with custom host and port
//! inputlayer-server --host 0.0.0.0 --port 9090
//!
//! # Start with custom data directory
//! inputlayer-server --data-dir /var/lib/inputlayer/data
//!
//! # Start with a specific config file
//! inputlayer-server --config /etc/inputlayer/config.toml
//! ```
//!
//! ## HTTP Server
//!
//! The HTTP server provides:
//! - WebSocket API at `/ws`
//! - AsyncAPI docs at `/api/ws-docs`
//! - GUI dashboard at `/` (if GUI is enabled)

use clap::Parser;
use inputlayer::config::LoggingConfig;
use inputlayer::protocol::rest;
use inputlayer::protocol::Handler;
use inputlayer::Config;

use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;

static TRACE_GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();

/// InputLayer - a reasoning engine for AI agents
#[derive(Parser, Debug)]
#[command(name = "inputlayer-server", version, about)]
struct Cli {
    /// Host address to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port to listen on
    #[arg(long, default_value_t = 8080)]
    port: u16,

    /// Path to configuration file (TOML)
    #[arg(long, short)]
    config: Option<PathBuf>,

    /// Override data directory
    #[arg(long)]
    data_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();

    println!("InputLayer Server");
    println!("=================");
    println!();

    // Load configuration
    let mut config = if let Some(ref config_path) = cli.config {
        // Explicit config path: fail hard if missing or invalid
        if !config_path.exists() {
            eprintln!("ERROR: Config file not found: {}", config_path.display());
            std::process::exit(1);
        }
        Config::from_file(config_path.to_str().unwrap_or("config.toml")).unwrap_or_else(|e| {
            eprintln!(
                "ERROR: Failed to parse config from {}: {e}",
                config_path.display()
            );
            std::process::exit(1);
        })
    } else {
        Config::load().unwrap_or_else(|_| {
            println!("Using default configuration");
            Config::default()
        })
    };

    // Initialize tracing using config as fallback when env vars are not set
    init_tracing(&config.logging);

    // Install a panic hook that logs panics via tracing and prints to stderr.
    // Without this, panics on worker threads may lose diagnostic data because
    // the non-blocking tracing writer buffers output.
    std::panic::set_hook(Box::new(|info| {
        let location = info.location().map_or_else(
            || "unknown".to_string(),
            |l| format!("{}:{}:{}", l.file(), l.line(), l.column()),
        );
        let payload = if let Some(s) = info.payload().downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "Box<dyn Any>".to_string()
        };
        tracing::error!(location, payload, "PANIC - thread panicked");
        eprintln!("PANIC at {location}: {payload}");
    }));

    // Override from CLI flags
    config.http.host = cli.host;
    config.http.port = cli.port;
    config.http.enabled = true;

    if let Some(data_dir) = cli.data_dir {
        config.storage.data_dir = data_dir;
    }

    let http_config = config.http.clone();

    // Warn about durability settings before config is moved
    if !config.storage.persist.enabled {
        eprintln!(
            "WARNING: DD-native persist layer is DISABLED. \
             Data will NOT survive restarts. Set [storage.persist] enabled = true for production."
        );
    }
    if config.storage.persist.durability_mode != inputlayer::config::DurabilityMode::Immediate {
        eprintln!(
            "WARNING: Durability mode is {:?} (not Immediate). \
             Recent writes may be lost on crash.",
            config.storage.persist.durability_mode
        );
    }
    // Auth is now always required via WebSocket (login/authenticate).
    // REST auth is validated via Bearer token against _internal KG.

    // Create handler
    let handler = Arc::new(Handler::from_config(config).map_err(|e| {
        eprintln!("ERROR: Failed to initialize InputLayer: {e}");
        Box::<dyn std::error::Error + Send + Sync>::from(e.clone())
    })?);

    // Bootstrap auth: create _internal KG and admin user if needed
    handler.bootstrap_auth();

    println!("Storage engine initialized");
    println!();
    println!("HTTP Server");
    println!("-----------");
    println!("Address: {}:{}", http_config.host, http_config.port);
    if http_config.gui.enabled {
        println!("GUI:     enabled");
    }
    println!();

    // Start HTTP server
    rest::start_http_server(handler, &http_config).await?;

    Ok(())
}

fn init_tracing(logging_config: &LoggingConfig) {
    // IL_TRACE_FILE controls where logs go:
    //   - Not set: logs to stderr (production default)
    //   - Set to a path: logs to that file
    // IL_TRACE=0 disables tracing entirely (not recommended for production)
    let disabled = env::var("IL_TRACE").ok().is_some_and(|v| v == "0");
    if disabled {
        return;
    }

    // Use IL_TRACE_JSON env var if set, otherwise fall back to config.logging.format
    let json = env::var("IL_TRACE_JSON")
        .ok()
        .map_or_else(|| logging_config.format == "json", |v| v != "0");

    // Use IL_TRACE_LEVEL env var if set, otherwise fall back to config.logging.level
    let level = env::var("IL_TRACE_LEVEL")
        .ok()
        .unwrap_or_else(|| logging_config.level.clone());

    let filter = tracing_subscriber::EnvFilter::try_new(&level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let use_file = if let Ok(log_path) = env::var("IL_TRACE_FILE") {
        // Try to open the log file
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
        {
            Ok(file) => {
                let (non_blocking, guard) = tracing_appender::non_blocking(file);
                let _ = TRACE_GUARD.set(guard);

                let base = || {
                    tracing_subscriber::fmt()
                        .with_env_filter(filter.clone())
                        .with_ansi(false)
                        .with_thread_names(true)
                        .with_thread_ids(true)
                        .with_writer(non_blocking.clone())
                        .with_timer(tracing_subscriber::fmt::time::SystemTime)
                };

                let subscriber: Box<dyn tracing::Subscriber + Send + Sync> = if json {
                    Box::new(base().json().finish())
                } else {
                    Box::new(base().compact().finish())
                };

                let _ = tracing::subscriber::set_global_default(subscriber);
                true
            }
            Err(e) => {
                eprintln!(
                    "WARNING: Unable to open IL_TRACE_FILE '{log_path}': {e}. \
                     Falling back to stderr logging."
                );
                false
            }
        }
    } else {
        false
    };

    if !use_file {
        // Log to stderr (production default)
        let base = || {
            tracing_subscriber::fmt()
                .with_env_filter(filter.clone())
                .with_ansi(true)
                .with_thread_names(false)
                .with_writer(std::io::stderr)
                .with_timer(tracing_subscriber::fmt::time::SystemTime)
        };

        let subscriber: Box<dyn tracing::Subscriber + Send + Sync> = if json {
            Box::new(base().json().finish())
        } else {
            Box::new(base().compact().finish())
        };

        let _ = tracing::subscriber::set_global_default(subscriber);
    }
}
