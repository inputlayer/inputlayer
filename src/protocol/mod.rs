//! InputLayer RPC Protocol
//!
//! This module provides the network protocol for InputLayer client-server communication.
//! It uses QUIC+TLS 1.3 via rpcnet for secure, high-performance RPC.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   InputLayer Protocol                       │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Services:                                                  │
//! │    - DatabaseService: create, drop, list, info databases   │
//! │    - QueryService: execute Datalog queries, explain plans  │
//! │    - DataService: insert, delete, bulk operations          │
//! │    - AdminService: health, stats, backup, shutdown         │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Wire Format: bincode serialization                         │
//! │  Transport: QUIC + TLS 1.3 (via rpcnet)                     │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Module Structure
//!
//! - `stubs/` - Service definition files (`*.rpc.rs`) for rpcnet-gen
//! - `generated/` - Generated client/server code from rpcnet-gen
//! - `wire` - Wire format types (WireValue, WireTuple, etc.)
//! - `error` - Protocol error types
//! - `unified_handler` - Handler implementing all service traits
//!
//! # Example Usage
//!
//! ## Server
//!
//! ```rust,ignore
//! use datalog_engine::protocol::generated::databaseservice::server::*;
//! use datalog_engine::protocol::UnifiedHandler;
//! use datalog_engine::Config;
//! use rpcnet::RpcConfig;
//!
//! let config = Config::load()?;
//! let handler = UnifiedHandler::from_config(config)?;
//!
//! let rpc_config = RpcConfig::new("certs/server.pem", "0.0.0.0:5433")
//!     .with_key_path("certs/server.key");
//!
//! let server = DatabaseServiceServer::new(handler, rpc_config);
//! server.serve().await?;
//! ```
//!
//! ## Client
//!
//! ```rust,ignore
//! use datalog_engine::protocol::generated::databaseservice::client::*;
//! use datalog_engine::protocol::generated::databaseservice::types::*;
//! use rpcnet::RpcConfig;
//!
//! let rpc_config = RpcConfig::new("certs/ca.pem", "0.0.0.0:0");
//! let client = DatabaseServiceClient::connect("127.0.0.1:5433".parse()?, rpc_config).await?;
//!
//! let response = client.list_databases(ListDatabasesRequest {}).await?;
//! for db in response.databases {
//!     println!("Database: {}", db.name);
//! }
//! ```

pub mod error;
pub mod generated;
pub mod stubs;
pub mod unified_handler;
pub mod wire;

// Re-export error types
pub use error::{InputLayerError, InputLayerResult};

// Re-export wire types
pub use wire::{ColumnDef, WireDataType, WireTuple, WireValue};

// Re-export unified handler
pub use unified_handler::UnifiedHandler;

// Re-export generated clients
pub use generated::{
    AdminServiceClient, DatabaseServiceClient, DataServiceClient, QueryServiceClient,
};

// Re-export generated servers and handlers
pub use generated::{
    AdminServiceHandler, AdminServiceServer, DatabaseServiceHandler, DatabaseServiceServer,
    DataServiceHandler, DataServiceServer, QueryServiceHandler, QueryServiceServer,
};

// ============================================================================
// Protocol Constants
// ============================================================================

/// Default server port for InputLayer RPC
pub const DEFAULT_PORT: u16 = 5433;

/// Default query timeout in milliseconds
pub const DEFAULT_QUERY_TIMEOUT_MS: u64 = 30_000;

/// Maximum message size (16 MB)
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Protocol version
pub const PROTOCOL_VERSION: u32 = 1;
