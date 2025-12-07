//! InputLayer RPC Service Stubs
//!
//! This directory contains `.rpc.rs` service definition files for rpcnet-gen.
//! These files define the RPC services and are used to generate client/server code.
//!
//! ## Service Definition Files
//!
//! - `database.rpc.rs` - Database management service
//! - `query.rpc.rs` - Query execution service
//! - `data.rpc.rs` - Data manipulation service
//! - `admin.rpc.rs` - Server administration service
//!
//! ## Code Generation
//!
//! To generate client/server stubs from these definitions:
//!
//! ```bash
//! # Generate all services
//! rpcnet-gen --input src/protocol/stubs/database.rpc.rs --output src/protocol/generated
//! rpcnet-gen --input src/protocol/stubs/query.rpc.rs --output src/protocol/generated
//! rpcnet-gen --input src/protocol/stubs/data.rpc.rs --output src/protocol/generated
//! rpcnet-gen --input src/protocol/stubs/admin.rpc.rs --output src/protocol/generated
//! ```
//!
//! Or use the build script (when configured in build.rs).
//!
//! ## File Format
//!
//! Each `.rpc.rs` file contains:
//! 1. Request/Response structs with `#[derive(Serialize, Deserialize)]`
//! 2. Error enum for the service
//! 3. Service trait with `#[rpcnet::service]` attribute
//!
//! ## Note
//!
//! The `.rpc.rs` files are NOT compiled as regular Rust modules.
//! They serve as input for the rpcnet-gen code generator.
//! The actual service traits used at runtime are in `services.rs`.

// This module is intentionally empty - the .rpc.rs files are not compiled directly.
// They serve as input for rpcnet-gen code generation.
