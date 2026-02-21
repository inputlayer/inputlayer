//! `InputLayer` Server Binary
//!
//! Starts an `InputLayer` server with WebSocket API and optional GUI.
//!
//! ## Usage
//!
//! ```bash
//! # Start server with default settings
//! cargo run --bin inputlayer-server
//!
//! # Start with custom HTTP address
//! cargo run --bin inputlayer-server -- --addr 0.0.0.0:8080
//! ```
//!
//! ## HTTP Server
//!
//! The HTTP server provides:
//! - WebSocket API at `/ws`
//! - AsyncAPI docs at `/api/ws-docs`
//! - GUI dashboard at `/` (if GUI is enabled)

use inputlayer::config::LoggingConfig;
use inputlayer::protocol::rest;
use inputlayer::protocol::Handler;
use inputlayer::Config;

use std::env;
use std::sync::Arc;
use std::sync::OnceLock;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 8080;

static TRACE_GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let host = get_arg(&args, "--host").unwrap_or_else(|| DEFAULT_HOST.to_string());
    let port: u16 = get_arg(&args, "--port")
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    println!("InputLayer Server");
    println!("=================");
    println!();

    // Load configuration
    let mut config = Config::load().unwrap_or_else(|_| {
        println!("Using default configuration");
        Config::default()
    });

    // Initialize tracing using config as fallback when env vars are not set
    init_tracing(&config.logging);

    // Override HTTP config from command line
    config.http.host = host;
    config.http.port = port;
    config.http.enabled = true;

    let http_config = config.http.clone();

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
    // Environment variables take precedence over config file values
    let enabled = env::var("IL_TRACE").ok().is_some_and(|v| v != "0");
    if !enabled {
        return;
    }

    let log_path = env::var("IL_TRACE_FILE").unwrap_or_else(|_| "il_trace.log".to_string());

    // Use IL_TRACE_JSON env var if set, otherwise fall back to config.logging.format
    let json = env::var("IL_TRACE_JSON")
        .ok()
        .map_or_else(|| logging_config.format == "json", |v| v != "0");

    // Use IL_TRACE_LEVEL env var if set, otherwise fall back to config.logging.level
    let level = env::var("IL_TRACE_LEVEL")
        .ok()
        .unwrap_or_else(|| logging_config.level.clone());

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

    let filter = tracing_subscriber::EnvFilter::try_new(level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("trace"));

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
}

fn get_arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1).cloned())
}
