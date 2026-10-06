//! A `/ws` text frame through both decoders the server applies: the one
//! before authentication (`ClientFrame` and the `id` probe) and the one an
//! authenticated connection uses to classify a request
//! ([`decode_frame`]).
//!
//! Invariants: a decoded frame re-encodes to itself; every reply echoes
//! the frame's `id`; an immediate reply encodes; a request run as
//! read-only, overlapping others, holds only queries.
#![no_main]

use inputlayer::fuzzing::{decode_frame, on_engine_stack, parse_bound_program, DecodedJob};
use inputlayer::parser::{set_max_nesting_depth, MAX_NESTING_DEPTH_CEILING};
use inputlayer::Statement;
use inputlayer_ws_protocol::{probe_request_id, ClientFrame};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let text = text.to_owned();
    on_engine_stack(move || check(&text));
});

fn check(text: &str) {
    set_max_nesting_depth(MAX_NESTING_DEPTH_CEILING);
    let frame = serde_json::from_str::<ClientFrame>(text).ok();
    let probed = probe_request_id(text);
    if let Some(frame) = &frame {
        let json = serde_json::to_string(frame).expect("a decoded frame encodes");
        let again = serde_json::from_str::<ClientFrame>(&json)
            .unwrap_or_else(|e| panic!("re-encoded frame does not decode: {e}\n{json}"));
        assert_eq!(&again, frame, "frame changed through {json}");
        assert_eq!(
            probed.as_ref(),
            frame.id(),
            "probed id differs from the frame's"
        );
    }

    let decoded = decode_frame(text);
    if let Some(frame) = &frame {
        assert_eq!(
            decoded.id.as_ref(),
            frame.id(),
            "reply id differs from the frame's"
        );
    }
    match &decoded.job {
        DecodedJob::Immediate(reply) => {
            serde_json::to_string(reply).expect("an immediate reply encodes");
            assert_eq!(reply.request_id(), decoded.id.as_ref());
        }
        DecodedJob::Execute {
            program, params, ..
        } if decoded.shared => {
            if let Ok(Some(statements)) = parse_bound_program(program, params) {
                for statement in &statements {
                    assert!(
                        matches!(statement, Statement::Query(_)),
                        "a request run as read-only holds {statement:?}"
                    );
                }
            }
        }
        _ => {}
    }
}
