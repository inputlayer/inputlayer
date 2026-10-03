//! End-to-end reactive agent path against a real `inputlayer-server` process.
//!
//! Agents subscribe to standing queries over the engine WebSocket; separate
//! writer connections insert and retract facts and change rules; agents must
//! receive the exact added/retracted rows, in order, without re-querying, and
//! end equal to a fresh full query. Writer→agent latency samples are written
//! when `INPUTLAYER_REACTIVE_SAMPLES_DIR` is set (`make e2e-reactive`).
//! The target is `test = false`: plain `cargo test` skips it; select it with
//! `--test e2e_reactive` (as `make e2e-reactive`, `test-all` and CI do).
//!
//! `known_defects` holds expected failures for defects tracked by the reactive
//! plan; each flips to a required pass when its plan item lands.
//!
//! Agents use the testkit's thin `/ws` client until an SDK subscribe API
//! exists; switch them to the SDK then.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod known_defects;
mod reactive;
mod wire;

use inputlayer_testkit::EngineBuilder;

/// Builder for an engine running this crate's server binary.
fn engine() -> EngineBuilder {
    EngineBuilder::new(env!("CARGO_BIN_EXE_inputlayer-server"))
}
