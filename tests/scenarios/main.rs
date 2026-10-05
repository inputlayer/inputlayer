//! Scenarios against a real `inputlayer-server` process.
//!
//! Each scenario starts an engine with its own data directory, connects
//! agents and writers over `/ws` the way a deployment does, and checks the
//! related changes together: rows, deltas, revisions, structured errors and
//! the engine's work counters. Plain `cargo test` runs the suite in debug
//! (all but `saturation`, whose timing bounds need a release engine, and the
//! quarantined scenarios listed in TESTING.md); `make e2e-reactive` runs all
//! of it in release and writes writer→agent latency samples when
//! `INPUTLAYER_REACTIVE_SAMPLES_DIR` is set.
//!
//! `INPUTLAYER_SCENARIO_VIEWS` (`recompute`, the default, or `maintained`)
//! selects how the engine answers reads of persistent rules, so CI can run the
//! suite once per mode. Until V2 (#309) defines `engine.views`, `maintained`
//! fails every scenario before an engine starts.
//!
//! Modules: `reactive` (the agent subscription path), `stream` (the
//! notification stream and snapshot handoff), `delivery` (results and deltas
//! too large for one frame), `saturation` (sessions' deltas while writers
//! overload the engine), `wire` (request correlation) and `harness`
//! (the testkit pieces the scenarios build on). Expected failures for tracked
//! defects use `inputlayer_testkit::KnownDefect`; an XPASS fails the run.
//!
//! Agents use the testkit's thin `/ws` client until an SDK subscribe API
//! exists; switch them to the SDK then.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod delivery;
mod harness;
mod reactive;
mod saturation;
mod stream;
mod wire;

use inputlayer_testkit::{EngineBuilder, Mode};

/// Environment variable selecting the [`Mode`] every scenario runs in.
const VIEWS_ENV: &str = "INPUTLAYER_SCENARIO_VIEWS";

/// Builder for an engine running this crate's server binary in the selected
/// views mode.
fn engine() -> EngineBuilder {
    EngineBuilder::new(env!("CARGO_BIN_EXE_inputlayer-server")).views(Mode::from_env(VIEWS_ENV))
}
