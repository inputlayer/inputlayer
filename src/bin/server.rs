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

/// InputLayer — streaming deductive knowledge graph database
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
        Config::from_file(config_path.to_str().unwrap_or("config.toml")).unwrap_or_else(|e| {
            eprintln!(
                "WARNING: Failed to load config from {}: {e}",
                config_path.display()
            );
            println!("Using default configuration");
            Config::default()
        })
    } else {
        Config::load().unwrap_or_else(|_| {
            println!("Using default configuration");
            Config::default()
        })
    };

    // Initialize tracing using config as fallback when env vars are not set
    init_tracing(&config.logging);

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

    // Create handler
    let handler = Arc::new(Handler::from_config(config).map_err(|e| {
        eprintln!("ERROR: Failed to initialize InputLayer: {e}");
        Box::<dyn std::error::Error + Send + Sync>::from(e.clone())
    })?);

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

    if let Ok(log_path) = env::var("IL_TRACE_FILE") {
        // Log to file
        let file = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
        {
            Ok(f) => f,
            Err(e) => {
                eprintln!("ERROR: Unable to open IL_TRACE_FILE '{log_path}': {e}");
                return;
            }
        };

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
    } else {
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
