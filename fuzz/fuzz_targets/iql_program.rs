//! A whole `execute` program as the handler takes it: comments stripped,
//! continuation lines joined, each statement parsed, `$params` bound and
//! authorized for every role. The input is the program, optionally followed
//! by a NUL byte and the frame's `params` JSON.
//!
//! Invariant: a program the connection runs as read-only, overlapping other
//! requests, holds only queries.
#![no_main]

use inputlayer::auth::{authorize_statement, Role};
use inputlayer::fuzzing::{
    assert_rule_reloads, is_query_program, on_engine_stack, parse_bound_program,
};
use inputlayer::params::Params;
use inputlayer::parser::{set_max_nesting_depth, MAX_NESTING_DEPTH_CEILING};
use inputlayer::Statement;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let (program, params) = text.split_once('\0').unwrap_or((text, ""));
    let params = if params.is_empty() {
        Params::default()
    } else {
        match serde_json::from_str::<Params>(params) {
            Ok(params) => params,
            Err(_) => return,
        }
    };
    let program = program.to_owned();
    on_engine_stack(move || check(&program, &params));
});

fn check(program: &str, params: &Params) {
    set_max_nesting_depth(MAX_NESTING_DEPTH_CEILING);
    let read_only = is_query_program(program);
    if let Ok(Some(statements)) = parse_bound_program(program, params) {
        if read_only {
            for statement in &statements {
                assert!(
                    matches!(statement, Statement::Query(_)),
                    "a program run as read-only holds {statement:?}"
                );
            }
        }
        for statement in &statements {
            for role in [Role::Admin, Role::Editor, Role::Viewer] {
                let _ = authorize_statement(&role, statement);
            }
            if let Statement::PersistentRule(rule) = statement {
                assert_rule_reloads(rule);
            }
        }
    }
    // The rule-program parser that snapshot reads and rule registration use.
    let _ = inputlayer::parse_program(program);
}
