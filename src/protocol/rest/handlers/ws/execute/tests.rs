use super::*;

fn id(s: &str) -> Option<RequestId> {
    RequestId::new(s).ok()
}

#[test]
fn test_log_preview_redacts_credentials() {
    assert_eq!(
        log_preview(".user create bob s3cret admin"),
        ".user create <redacted>"
    );
    assert_eq!(
        log_preview("  .user password bob n3w"),
        ".user password <redacted>"
    );
    assert_eq!(log_preview("..user bob s3cret"), ".user <redacted>");
    assert_eq!(
        log_preview("?edge(X, Y)\n.apikey create ci"),
        ".apikey create <redacted>"
    );
    assert_eq!(
        log_preview(".USER create bob s3cret admin"),
        ".user create <redacted>"
    );
    assert_eq!(
        log_preview(".User Password bob n3w"),
        ".user password <redacted>"
    );
    assert_eq!(
        log_preview(".ApiKey CREATE ci"),
        ".apikey create <redacted>"
    );
}

#[test]
fn test_log_preview_first_line() {
    assert_eq!(log_preview("  ?edge(X, Y)  \n+edge(1, 2)"), "?edge(X, Y)");
    assert_eq!(log_preview(".users"), ".users");
    assert_eq!(log_preview(""), "");
}

#[test]
fn test_log_preview_truncates_on_char_boundary() {
    let program = format!("{}é{}", "a".repeat(79), "b".repeat(19));
    assert_eq!(program.len(), 100);
    assert!(!program.is_char_boundary(80));
    let preview = log_preview(&program);
    assert_eq!(preview.chars().count(), LOG_PREVIEW_CHARS);
    assert!(preview.ends_with('é'));
}

#[test]
fn test_program_error_frame_keeps_code_and_id() {
    let frame = program_error_frame(
        id("r1"),
        ProgramError {
            message: "Rule 'x' not found.".to_string(),
            code: Some(ErrorCode::NotFound),
        },
    );
    let json = serde_json::to_value(&frame).unwrap();
    assert_eq!(json["code"], "not_found", "{json}");
    assert_eq!(json["id"], "r1", "{json}");

    let frame = program_error_frame(None, ProgramError::from("Access denied".to_string()));
    let json = serde_json::to_value(&frame).unwrap();
    assert!(json.get("code").is_none(), "{json}");
    assert!(json.get("id").is_none(), "{json}");
}

#[test]
fn test_program_error_frame_parse_errors_are_validation() {
    let errors = vec![ValidationError {
        line: 1,
        statement_index: 0,
        error: "bad".to_string(),
    }];
    let message = format!(
        "{VALIDATION_ERROR_PREFIX}{}",
        serde_json::to_string(&errors).unwrap()
    );
    let frame = program_error_frame(id("r2"), ProgramError::from(message));
    let ServerFrame::Error {
        id: echoed,
        code,
        validation_errors,
        ..
    } = frame
    else {
        panic!("{frame:?}");
    };
    assert_eq!(echoed, id("r2"));
    assert_eq!(code, Some(ErrorCode::Validation));
    assert_eq!(validation_errors, Some(errors));
}

#[test]
fn test_result_frame_carries_id_rows_and_provenance() {
    let mut response = QueryResult::new(
        vec![crate::protocol::WireTuple::new(vec![
            crate::protocol::WireValue::Int32(1),
        ])],
        vec![crate::protocol::ColumnDef::int32("x")],
        3,
    );
    response.switched_kg = Some("kg".to_string());
    let ServerFrame::Result(frame) = result_frame(id("q"), response) else {
        panic!("not a result");
    };
    assert_eq!(frame.id, id("q"));
    assert_eq!(frame.columns, ["x"]);
    assert_eq!(frame.rows, [[serde_json::json!(1)]]);
    assert_eq!(frame.row_provenance, ["unknown"]);
    assert_eq!(frame.row_count, 1);
    assert_eq!(frame.switched_kg.as_deref(), Some("kg"));
}
