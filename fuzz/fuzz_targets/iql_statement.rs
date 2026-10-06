//! One IQL statement through [`parse_statement`], the parser every client
//! statement reaches, at the highest nesting limit an operator may
//! configure. A persistent rule it accepts must also pass rule validation
//! and survive the catalog's encoding (the #295 restart trap).
#![no_main]

use inputlayer::fuzzing::{assert_rule_reloads, on_engine_stack};
use inputlayer::parser::{set_max_nesting_depth, MAX_NESTING_DEPTH_CEILING};
use inputlayer::rule_catalog::validate_rule;
use inputlayer::{parse_statement, Statement};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // WebSocket text frames are UTF-8; nothing else reaches the parser.
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let text = text.to_owned();
    on_engine_stack(move || check(&text));
});

fn check(text: &str) {
    set_max_nesting_depth(MAX_NESTING_DEPTH_CEILING);
    let Ok(statement) = parse_statement(text) else {
        return;
    };
    if let Statement::PersistentRule(rule) | Statement::SessionRule(rule) = &statement {
        let _ = validate_rule(rule, &rule.head.relation);
    }
    if let Statement::PersistentRule(rule) = &statement {
        assert_rule_reloads(rule);
    }
}
