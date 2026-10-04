//! Test-only harness for the reactive agent path.
//!
//! Drives a real `inputlayer-server` process the way a subscribing agent does:
//! - [`engine`]: one engine process per test with a private data directory.
//! - [`client`]: a thin `/ws` client that timestamps frames on arrival.
//! - [`agent`]: subscriptions maintained purely from pushed deltas, with
//!   [`streamed`] deltas reassembled before they apply.
//! - [`fixture`]: named knowledge-graph fixtures shared with the benches.
//! - [`metrics`]: raw writer→agent delta latency samples.
//! - [`contract`] and [`known_defect`]: typed contract violations and
//!   expected failures for defects the plan tracks.

pub mod agent;
pub mod client;
pub mod contract;
pub mod engine;
pub mod fixture;
pub mod known_defect;
pub mod metrics;
pub mod streamed;

pub use agent::{Agent, Delta, View};
pub use client::{Commit, QueryResult, WsClient};
pub use contract::{Checked, Violation};
pub use engine::{Engine, EngineBuilder, Replication};
pub use fixture::Fixture;
pub use known_defect::{KnownDefect, Reproduction};
pub use metrics::SampleLog;
