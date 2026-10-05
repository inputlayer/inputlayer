//! Warm-standby replication: the engine side.
//!
//! A primary appends every durable change to a [`ReplicationLog`] as one
//! [`event`] line, numbered by LSN in an order that is valid to apply. A
//! follower applies the lines with `StorageEngine::apply_replicated`, or
//! brings its whole state to a checkpoint with
//! `StorageEngine::reconcile_graph`, and saves its [`Position`]. With
//! synchronous shipping, [`writes`] tells a client request which events it
//! appended, so its reply can wait for a follower to apply them.
//!
//! The network side (the primary's stream endpoint and the follower's
//! client) lives in `protocol::replication`.

pub mod event;
pub mod log;
pub mod position;
pub mod resync;
pub mod writes;

pub use event::{EngineEvent, Event, Line, ResyncMark};
pub use log::{Read, ReplicationLog};
pub use position::Position;
