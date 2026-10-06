//! Entry points for the fuzz harness (`fuzz/`): the paths client text takes
//! through the server, exposed with the `test-support` feature. Each runs
//! the code the server runs, not a copy of it.

use std::sync::mpsc;
use std::sync::OnceLock;
use std::thread;

pub use crate::protocol::rest::handlers::ws::{decode_frame, DecodedFrame, DecodedJob};

use crate::params::Params;
use crate::statement::{SerializableRule, Statement};
use crate::Rule;

/// The statements of an `execute` program, split, parsed and with `params`
/// bound the way the handler does before it authorizes and runs them.
/// `Ok(None)` when a statement does not parse.
pub fn parse_bound_program(
    program: &str,
    params: &Params,
) -> Result<Option<Vec<Statement>>, String> {
    crate::protocol::handler::parse_bound_program(program, params).map_err(|e| e.message)
}

/// Whether the handler would run `program` as a read-only program that
/// may overlap others (see `Access::Shared`).
pub fn is_query_program(program: &str) -> bool {
    crate::protocol::handler::is_query_program(program)
}

/// Panic unless `rule`, stored the way the rule catalog and the write-ahead
/// log store it, reloads as the same rule: one that does not would change
/// meaning across a restart, or stop the engine from starting.
pub fn assert_rule_reloads(rule: &Rule) {
    let json = serde_json::to_vec(&SerializableRule::from_rule(rule))
        .unwrap_or_else(|e| panic!("persisted rule does not encode: {e}\n{rule:?}"));
    let stored: SerializableRule = crate::storage::nested_json::from_slice(&json)
        .unwrap_or_else(|e| panic!("persisted rule does not reload: {e}\n{rule:?}"));
    assert_eq!(&stored.to_rule(), rule, "persisted rule reloads changed");
}

/// Run `f` on a long-lived thread with the stack the server parses and
/// plans client programs on ([`crate::ENGINE_THREAD_STACK_BYTES`]), so
/// recursion is held to the server's budget rather than the fuzzer's main
/// thread. A panic or stack overflow there takes the process down, which is
/// what the fuzzer reports.
pub fn on_engine_stack(f: impl FnOnce() + Send + 'static) {
    type Job = Box<dyn FnOnce() + Send>;
    static WORKER: OnceLock<mpsc::SyncSender<(Job, mpsc::SyncSender<()>)>> = OnceLock::new();
    let worker = WORKER.get_or_init(|| {
        let (send, receive) = mpsc::sync_channel::<(Job, mpsc::SyncSender<()>)>(0);
        thread::Builder::new()
            .name("fuzz-engine".to_string())
            .stack_size(crate::ENGINE_THREAD_STACK_BYTES)
            .spawn(move || {
                for (job, done) in receive {
                    job();
                    let _ = done.send(());
                }
            })
            .expect("spawn the fuzz engine thread");
        send
    });
    let (done, finished) = mpsc::sync_channel(1);
    worker
        .send((Box::new(f), done))
        .expect("the fuzz engine thread is running");
    finished
        .recv()
        .expect("the fuzz engine thread finished the input");
}
