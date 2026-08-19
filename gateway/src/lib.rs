//! Verified Completions gateway — placeholder workspace member.
//!
//! The gateway (issue #83: `POST /v1/verify`, then #84: the
//! `/v1/chat/completions` proxy) lives here as its own crate so HTTP and LLM
//! client dependencies stay out of the engine crate. It talks to the engine
//! over the public WebSocket API like any other client.
