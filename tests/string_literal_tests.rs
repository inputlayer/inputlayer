//! String literals with commas, operators, parens, quotes and escapes, end to end through `Handler`.

use inputlayer::protocol::wire::WireValue;
use inputlayer::protocol::Handler;
use inputlayer::Config;
use tempfile::TempDir;

fn create_test_handler() -> (Handler, TempDir) {
    let temp = TempDir::new().expect("temp dir");
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    let storage = inputlayer::StorageEngine::new(config).expect("storage engine");
    (Handler::new(storage), temp)
}

async fn exec(handler: &Handler, program: &str) {
    handler
        .query_program(None, program.to_string())
        .await
        .unwrap_or_else(|e| panic!("Failed to execute '{program}': {e}"));
}

async fn rows(handler: &Handler, query: &str) -> Vec<Vec<WireValue>> {
    let result = handler
        .query_program(None, query.to_string())
        .await
        .unwrap_or_else(|e| panic!("Failed to execute '{query}': {e}"));
    result.rows.into_iter().map(|r| r.values).collect()
}

fn s(v: &str) -> WireValue {
    WireValue::String(v.to_string())
}

/// Same escaping as the JS and Python SDKs' `compileValue`.
fn sdk_literal(v: &str) -> String {
    let mut out = String::from('"');
    for ch in v.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

const TEXTS: &[&str] = &[
    "hi, there",
    "a<b",
    "x<-y",
    "say \"hi\"",
    "Option 1) Paris, option 2) Rome",
    "smile :), ok",
    "a \"x, y\" b",
    r"C:\temp",
    "Smith, John",
    "1 != 2 = 3 >= 4",
    "// not a comment",
    "line one\nline two",
    "tab\there\r\n",
];

#[tokio::test]
async fn text_values_insert_query_delete_roundtrip() {
    let (handler, _t) = create_test_handler();
    exec(&handler, "+said(u: int, m: string)").await;

    let tuples: Vec<String> = TEXTS
        .iter()
        .enumerate()
        .map(|(i, t)| format!("({i}, {})", sdk_literal(t)))
        .collect();
    exec(&handler, &format!("+said[{}]", tuples.join(", "))).await;

    for (i, t) in TEXTS.iter().enumerate() {
        let lit = sdk_literal(t);
        assert_eq!(
            rows(&handler, &format!("?said(U, {lit})")).await,
            vec![vec![WireValue::Int64(i as i64), s(t)]],
            "atom match for {t:?}"
        );
        assert_eq!(
            rows(&handler, &format!("?said(U, M), M = {lit}")).await,
            vec![vec![WireValue::Int64(i as i64), s(t)]],
            "comparison match for {t:?}"
        );
        assert_eq!(
            rows(&handler, &format!("?said({i}, M)")).await,
            vec![vec![WireValue::Int64(i as i64), s(t)]],
            "stored value for {t:?}"
        );
    }

    exec(&handler, &format!("-said(10, {})", sdk_literal(TEXTS[10]))).await;
    exec(&handler, &format!("-said(7, {})", sdk_literal(r"C:\temp"))).await;
    assert!(rows(&handler, "?said(7, M)").await.is_empty());
    assert!(rows(&handler, "?said(10, M)").await.is_empty());
    assert_eq!(
        rows(&handler, "?said(0, M)").await,
        vec![vec![WireValue::Int64(0), s("hi, there")]]
    );
}

#[tokio::test]
async fn single_insert_with_escaped_quote_before_comma() {
    let (handler, _t) = create_test_handler();
    exec(&handler, r#"+note(7, "say \"hi\" now")"#).await;
    exec(&handler, r#"+note[(3, "a \"x, y\" b")]"#).await;
    assert_eq!(
        rows(&handler, r#"?note(I, "say \"hi\" now")"#).await,
        vec![vec![WireValue::Int64(7), s("say \"hi\" now")]]
    );
    assert_eq!(
        rows(&handler, "?note(3, M)").await,
        vec![vec![WireValue::Int64(3), s("a \"x, y\" b")]]
    );
}

#[tokio::test]
async fn newline_escape_is_stored_as_newline() {
    let (handler, _t) = create_test_handler();
    exec(&handler, r#"+note(1, "line one\nline two")"#).await;
    assert_eq!(
        rows(&handler, "?note(1, M)").await,
        vec![vec![WireValue::Int64(1), s("line one\nline two")]]
    );
}

#[tokio::test]
async fn injection_payload_is_one_tuple() {
    let (handler, _t) = create_test_handler();
    let payload = r#"c1:e"), (", 7, true, ""#;
    exec(
        &handler,
        &format!("+claim[(\"c1:k\", {}, \"budget\")]", sdk_literal(payload)),
    )
    .await;
    assert_eq!(
        rows(&handler, "?claim(K, V, W)").await,
        vec![vec![s("c1:k"), s(payload), s("budget")]]
    );
}

#[tokio::test]
async fn persisted_rule_keeps_float_and_string_constants() {
    let (handler, _t) = create_test_handler();
    exec(&handler, "+n[(1), (2)]").await;
    exec(&handler, r#"+r(X, 2.0, "a, \"b\"") <- n(X)"#).await;
    exec(&handler, "+w(X, Y) <- n(X), Y = X * 2.0").await;
    assert_eq!(
        rows(&handler, "?r(1, F, S)").await,
        vec![vec![
            WireValue::Int64(1),
            WireValue::Float64(2.0),
            s("a, \"b\"")
        ]]
    );
    assert_eq!(
        rows(&handler, "?w(2, Y)").await,
        vec![vec![WireValue::Int64(2), WireValue::Float64(4.0)]]
    );
}

#[tokio::test]
async fn reserved_query_name_gives_name_error() {
    let (handler, _t) = create_test_handler();
    let err = handler
        .query_program(None, "?__x(N)".to_string())
        .await
        .unwrap_err();
    assert!(err.contains("reserved"), "got: {err}");
}

#[tokio::test]
async fn query_shorthand_accepts_underscore_and_space() {
    let (handler, _t) = create_test_handler();
    exec(&handler, "+n[(1), (2)]").await;
    let sid = handler.create_session("default").expect("session");
    let run = |q: &str| handler.execute_program(Some(&sid), None, q.to_string(), None);
    run("_tmp(X) <- n(X)").await.expect("session rule");
    for q in ["?_tmp(X)", "? n(X)"] {
        let res = run(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        let mut got: Vec<_> = res.rows.into_iter().map(|r| r.values).collect();
        got.sort_by_key(|r| format!("{r:?}"));
        assert_eq!(
            got,
            vec![vec![WireValue::Int64(1)], vec![WireValue::Int64(2)]],
            "{q}"
        );
    }
}
