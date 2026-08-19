//! InputLayer Gateway - the model gateway of the stack.
//!
//! An OpenAI-compatible endpoint (issue #83: `POST /v1/verify`, then #84:
//! the `/v1/chat/completions` proxy) that forwards completions to the model
//! provider and verifies every conversation against the loaded ontology,
//! attaching findings with quoted spans and proof trees.
//!
//! Packaging: a separate deployable. Production runs two containers, the IL
//! engine and the IL gateway, each with its own image and configuration.
//! The split keeps the stateful engine isolated from the component that
//! talks to the internet: a gateway crash or a hung model-provider call
//! never touches the engine, and only the gateway process holds the model
//! provider key. The gateway talks to the engine over the public WebSocket
//! API like any other client, so HTTP and LLM client dependencies never
//! enter the engine crate.
