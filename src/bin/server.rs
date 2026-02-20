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

use inputlayer::protocol::rest;
use inputlayer::protocol::Handler;
use inputlayer::Config;

use std::env;
use std::sync::Arc;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 8080;

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

    // Override HTTP config from command line
    config.http.host = host;
    config.http.port = port;
    config.http.enabled = true;

    let http_config = config.http.clone();

    // Create handler
    let handler = Arc::new(Handler::from_config(config).expect("Failed to create handler"));

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

fn get_arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1).cloned())
}
