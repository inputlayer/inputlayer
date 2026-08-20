//! Shared client-side machinery for consuming the ontology registry and
//! talking to the engine over its public WebSocket API.
//!
//! Used by the `il` CLI and the gateway - one implementation of registry
//! resolution (index fetch, digest-verified cache) and the WS protocol
//! (authenticate, execute, soft-error scanning), so the two products can
//! never drift.

pub mod registry;
pub mod ws;
