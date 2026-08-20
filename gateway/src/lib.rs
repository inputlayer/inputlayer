//! InputLayer Gateway - placeholder workspace member.
//!
//! The gateway is the model gateway of the stack, the second deployable next to the engine: an
//! OpenAI-compatible endpoint (issue #83: `POST /v1/verify`, then #84: the
//! `/v1/chat/completions` proxy) that forwards completions to the model
//! provider and verifies every conversation against the loaded ontology,
//! attaching findings with quoted spans and proof trees.
//!
//! It lives as its own crate so HTTP and LLM client dependencies stay out of
//! the engine, and it talks to the engine over the public WebSocket API like
//! any other client. Production deployments run both: the IL engine and the
//! IL model gateway.
