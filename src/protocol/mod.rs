//! `InputLayer` Protocol
//!
//! Client-server communication: HTTP REST API, wire format, error handling.
//!
//! # Architecture
//!
//! ```text
//! +-------------------------------------------------------------+
//! |                   InputLayer Protocol                       |
//! +-------------------------------------------------------------+
//! |  HTTP Endpoints:                                            |
//! |    - /health: health check                                  |
//! |    - /metrics: server statistics                             |
//! |    - /api/v1/ws: WebSocket (all data operations)            |
//! +-------------------------------------------------------------+
//! |  Wire Format: JSON (WebSocket) / bincode (internal)         |
//! |  Transport: WebSocket                                        |
//! +-------------------------------------------------------------+
//! ```
//!
//! # Module Structure
//!
//! - `wire` - Wire format types (`WireValue`, `WireTuple`, `QueryResult`, etc.)
//! - `error` - Protocol error types
//! - `handler` - Handler implementing business logic
//! - `rest` - REST API handlers and routing

pub mod error;
pub mod handler;
pub mod rest;
pub mod wire;

// Re-export error types
pub use error::{InputLayerError, InputLayerResult};

// Re-export wire types
pub use wire::{ColumnDef, QueryResult, WireDataType, WireTuple, WireValue};

// Re-export handler
pub use handler::Handler;

// Protocol Constants
/// Default HTTP server port
pub const DEFAULT_PORT: u16 = 8080;

/// Default query timeout in milliseconds
pub const DEFAULT_QUERY_TIMEOUT_MS: u64 = 30_000;

/// Maximum message size (16 MB)
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Protocol version
pub const PROTOCOL_VERSION: u32 = 1;
