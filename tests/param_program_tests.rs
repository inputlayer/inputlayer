//! Parameterised programs (EN-5): values sent beside a program's text, bound
//! to its `$name` references on the parsed statements. A value never passes
//! through the parser, so no value can change what a program does.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use inputlayer::params::Params;
use inputlayer::protocol::{ErrorCode, Handler, ProgramError, QueryResult, WireValue};
use inputlayer::{Config, StorageEngine};
use serde_json::json;
use tempfile::TempDir;

const KG: &str = "default";

fn config(dir: &TempDir) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_path_buf();
    config.storage.persist.durability_mode = inputlayer::config::DurabilityMode::Immediate;
    config
}

fn handler(dir: &TempDir) -> Handler {
    Handler::new(StorageEngine::new(config(dir)).expect("storage"))
}

/// `params` as the wire carries them.
fn params(value: serde_json::Value) -> Params {
    serde_json::from_value(value).expect("params")
}

async fn run_with(
    handler: &Handler,
    program: &str,
    params: &Params,
) -> Result<QueryResult, ProgramError> {
    handler
        .execute_program_with_params(
            None,
            Some(KG.to_string()),
            program.to_string(),
            params,
            None,
            &handler.request_control(None),
        )
        .await
}

async fn run(handler: &Handler, program: &str) -> Result<QueryResult, ProgramError> {
    run_with(handler, program, &Params::new()).await
}

/// Rows of `query`, sorted by their debug rendering.
async fn rows_with(handler: &Handler, query: &str, p: &Params) -> Vec<Vec<WireValue>> {
    let result = run_with(handler, query, p).await.expect(query);
    assert!(result.errors.is_empty(), "{query}: {:?}", result.errors);
    let mut rows: Vec<Vec<WireValue>> = result.rows.into_iter().map(|r| r.values).collect();
    rows.sort_by_key(|row| format!("{row:?}"));
    rows
}

async fn rows(handler: &Handler, query: &str) -> Vec<Vec<WireValue>> {
    rows_with(handler, query, &Params::new()).await
}

fn s(value: &str) -> WireValue {
    WireValue::String(value.to_string())
}

/// Strings that would change a statement if they were spliced into its text.
const HOSTILE: [&str; 12] = [
    r#"x"), +evil(1"#,
    "a\nb",
    "a\n+evil(2)",
    "\"",
    r"\",
    r#"\""#,
    "$other",
    "_",
    "X",
    "1, 2",
    "%comment",
    "// not a comment <- :=",
];

#[tokio::test]
async fn every_value_type_round_trips_exactly() {
    let dir = TempDir::new().unwrap();
    let h = handler(&dir);
    let p = params(json!({
        "min": i64::MIN, "max": i64::MAX,
        "tenth": 0.1, "tiny": 5e-324, "neg_zero": -0.0, "big": {"float": 1e300},
        "uni": "é \u{1F600} \u{0} \t", "yes": true, "no": false,
        "v": [0.5, -2, 1e-3],
    }));
    run_with(
        &h,
        "+ints($min, $max)\n+floats($tenth, $tiny, $neg_zero, $big)\n+other($uni, $yes, $no)\n+vec(1, $v)",
        &p,
    )
    .await
    .unwrap();

    assert_eq!(
        rows(&h, "?ints(A, B)").await,
        vec![vec![WireValue::Int64(i64::MIN), WireValue::Int64(i64::MAX)]]
    );
    let floats = rows(&h, "?floats(A, B, C, D)").await;
    let bits: Vec<u64> = floats[0]
        .iter()
        .map(|v| match v {
            WireValue::Float64(f) => f.to_bits(),
            other => panic!("not a float: {other:?}"),
        })
        .collect();
    assert_eq!(
        bits,
        [0.1f64, 5e-324, -0.0, 1e300].map(f64::to_bits).to_vec()
    );
    assert_eq!(
        rows(&h, "?other(A, B, C)").await,
        vec![vec![
            s("é \u{1F600} \u{0} \t"),
            WireValue::Bool(true),
            WireValue::Bool(false)
        ]]
    );
    assert_eq!(
        rows(&h, "?vec(1, V)").await,
        vec![vec![
            WireValue::Int64(1),
            WireValue::Vector(vec![0.5, -2.0, 1e-3])
        ]]
    );
}

#[tokio::test]
async fn a_value_never_changes_the_program() {
    let dir = TempDir::new().unwrap();
    let h = handler(&dir);
    for (i, value) in HOSTILE.iter().enumerate() {
        let p = params(json!({ "id": i, "value": value }));
        let result = run_with(&h, "+note($id, $value)", &p).await.unwrap();
        assert!(result.errors.is_empty(), "{value:?}: {:?}", result.errors);
    }
    // Stored byte for byte, and nothing else was written.
    let stored: Vec<String> = rows(&h, "?note(I, V)")
        .await
        .into_iter()
        .map(|row| match &row[1] {
            WireValue::String(v) => v.clone(),
            other => panic!("not a string: {other:?}"),
        })
        .collect();
    let mut expected: Vec<String> = HOSTILE.iter().map(ToString::to_string).collect();
    expected.sort_by_key(|v| format!("{:?}", s(v)));
    let mut stored_sorted = stored.clone();
    stored_sorted.sort_by_key(|v| format!("{:?}", s(v)));
    assert_eq!(stored_sorted, expected);
    let relations = run(&h, ".rel").await.unwrap();
    let listing = format!("{:?}", relations.rows);
    assert!(!listing.contains("evil"), "{listing}");

    // Each is found by exactly itself, as a query and as a delete.
    for value in HOSTILE {
        let p = params(json!({ "value": value }));
        assert_eq!(
            rows_with(&h, "?note(I, $value)", &p).await.len(),
            1,
            "{value:?}"
        );
        run_with(&h, "-note(I, $value) <- note(I, $value)", &p)
            .await
            .unwrap();
        assert!(
            rows_with(&h, "?note(I, $value)", &p).await.is_empty(),
            "{value:?}"
        );
    }
    assert!(rows(&h, "?note(I, V)").await.is_empty());
}

#[tokio::test]
async fn one_text_binds_each_request_its_own_values() {
    let dir = TempDir::new().unwrap();
    let h = handler(&dir);
    let program = "+eta($shipment, $date)";
    for (shipment, date) in [("S-1", "2026-10-10"), ("S-2", "2026-11-01")] {
        run_with(
            &h,
            program,
            &params(json!({"shipment": shipment, "date": date})),
        )
        .await
        .unwrap();
    }
    assert_eq!(
        rows(&h, "?eta(S, D)").await,
        vec![
            vec![s("S-1"), s("2026-10-10")],
            vec![s("S-2"), s("2026-11-01")]
        ]
    );
    let query = "?eta($shipment, D)";
    assert_eq!(
        rows_with(&h, query, &params(json!({"shipment": "S-2"}))).await,
        vec![vec![s("S-2"), s("2026-11-01")]]
    );
    assert!(rows_with(&h, query, &params(json!({"shipment": "S-3"})))
        .await
        .is_empty());
}

#[tokio::test]
async fn deletes_and_updates_match_only_the_bound_value() {
    let dir = TempDir::new().unwrap();
    let h = handler(&dir);
    run(&h, "+stock[(\"a\", 1), (\"b\", 2), (\"c\", 3)]")
        .await
        .unwrap();

    // A literal delete removes one fact, never a pattern.
    run_with(
        &h,
        "-stock($item, $n)",
        &params(json!({"item": "a", "n": 1})),
    )
    .await
    .unwrap();
    // A wrong type matches nothing: 2 the int is not 2.0 the float.
    run_with(
        &h,
        "-stock($item, $n)",
        &params(json!({"item": "b", "n": 2.0})),
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&h, "?stock(I, N)").await,
        vec![
            vec![s("b"), WireValue::Int64(2)],
            vec![s("c"), WireValue::Int64(3)]
        ]
    );

    // Conditional delete and update bind in heads and bodies.
    run_with(
        &h,
        "-stock(I, N) <- stock(I, N), N > $limit",
        &params(json!({"limit": 2})),
    )
    .await
    .unwrap();
    run_with(
        &h,
        "-stock($item, N), +stock($item, M) <- stock($item, N), M = N + $delta",
        &params(json!({"item": "b", "delta": 40})),
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&h, "?stock(I, N)").await,
        vec![vec![s("b"), WireValue::Int64(42)]]
    );
}

#[tokio::test]
async fn bulk_tuples_bind_each_value() {
    let dir = TempDir::new().unwrap();
    let h = handler(&dir);
    let p = params(json!({"a": "x\"), (\"y", "b": "z", "n": 7}));
    run_with(&h, "+pair[($a, $n), ($b, 1), ($b, $n)]", &p)
        .await
        .unwrap();
    assert_eq!(
        rows(&h, "?pair(K, V)").await,
        vec![
            vec![s("x\"), (\"y"), WireValue::Int64(7)],
            vec![s("z"), WireValue::Int64(1)],
            vec![s("z"), WireValue::Int64(7)]
        ]
    );
    run_with(
        &h,
        "-pair[($a, $n), ($b, 1)]",
        &params(json!({"a": "x\"), (\"y", "b": "z", "n": 7})),
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&h, "?pair(K, V)").await,
        vec![vec![s("z"), WireValue::Int64(7)]]
    );
}

#[tokio::test]
async fn queries_bind_in_goals_comparisons_arithmetic_and_negation() {
    let dir = TempDir::new().unwrap();
    let h = handler(&dir);
    run(
        &h,
        "+score[(\"ann\", 10), (\"bob\", 20), (\"cy\", 30)]\n+banned[(\"cy\")]",
    )
    .await
    .unwrap();
    assert_eq!(
        rows_with(
            &h,
            "?score(N, S), S > $min, !banned(N)",
            &params(json!({"min": 15}))
        )
        .await,
        vec![vec![s("bob"), WireValue::Int64(20)]]
    );
    assert_eq!(
        rows_with(&h, "?score($name, S)", &params(json!({"name": "cy"}))).await,
        vec![vec![s("cy"), WireValue::Int64(30)]]
    );
    let p = params(json!({"name": "cy", "bonus": 0.5}));
    assert_eq!(
        rows_with(&h, "?score(N, S), N = $name, T = S + $bonus", &p).await,
        vec![vec![
            s("cy"),
            WireValue::Int64(30),
            WireValue::Float64(30.5)
        ]]
    );
}

#[tokio::test]
async fn persistent_rules_store_the_bound_value_across_restart() {
    let dir = TempDir::new().unwrap();
    {
        let h = handler(&dir);
        run(&h, "+customer[(\"ann\", 5), (\"bob\", 50)]")
            .await
            .unwrap();
        run_with(
            &h,
            "+vip(C) <- customer(C, T), T >= $threshold",
            &params(json!({"threshold": 10})),
        )
        .await
        .unwrap();
        assert_eq!(rows(&h, "?vip(C)").await, vec![vec![s("bob")]]);
    }
    let h = handler(&dir);
    assert_eq!(rows(&h, "?vip(C)").await, vec![vec![s("bob")]]);
    let definition = run(&h, ".rule def vip").await.unwrap();
    let text = format!("{:?}", definition.rows);
    assert!(text.contains("10") && !text.contains('$'), "{text}");
}

#[tokio::test]
async fn program_local_facts_and_rules_bind_too() {
    let dir = TempDir::new().unwrap();
    let h = handler(&dir);
    run(&h, "+edge[(1, 2), (2, 3)]").await.unwrap();
    let p = params(json!({"from": 1, "label": "start"}));
    let result = run_with(
        &h,
        "mark($from, $label)\nreach(Y) <- edge($from, Y)\n?reach(Y), mark(M, L)",
        &p,
    )
    .await
    .unwrap();
    let rows: Vec<_> = result.rows.into_iter().map(|r| r.values).collect();
    assert_eq!(
        rows,
        vec![vec![WireValue::Int64(2), WireValue::Int64(1), s("start")]]
    );
}

#[tokio::test]
async fn session_facts_and_rules_bind_in_the_session() {
    let dir = TempDir::new().unwrap();
    let h = handler(&dir);
    run(&h, "+edge[(1, 2), (2, 3)]").await.unwrap();
    let sid = h.create_session(KG).unwrap();
    let in_session = |program: &'static str, p: Params| {
        let h = &h;
        let sid = &sid;
        async move {
            h.execute_program_with_params(
                Some(sid),
                None,
                program.to_string(),
                &p,
                None,
                &h.request_control(None),
            )
            .await
            .unwrap()
        }
    };
    in_session("start($node)", params(json!({"node": 2}))).await;
    in_session("next(Y) <- start(X), edge(X, Y)", Params::new()).await;
    // A dirty session evaluates the query with its rules and facts.
    let result = in_session("?next(Y), Y != $not", params(json!({"not": 9}))).await;
    let rows: Vec<_> = result.rows.into_iter().map(|r| r.values).collect();
    assert_eq!(rows, vec![vec![WireValue::Int64(3)]]);
    let result = in_session("?next($y)", params(json!({"y": 3}))).await;
    assert_eq!(result.rows.len(), 1);
    let result = in_session("?next($y)", params(json!({"y": 4}))).await;
    assert!(result.rows.is_empty());
}

#[tokio::test]
async fn a_missing_or_unused_parameter_fails_the_whole_program() {
    let dir = TempDir::new().unwrap();
    let h = handler(&dir);

    let error = run_with(&h, "+a(1)\n+b($x, $y)", &params(json!({"x": 1})))
        .await
        .unwrap_err();
    assert_eq!(error.code, Some(ErrorCode::Validation));
    assert!(
        error.message.contains("$y has no value"),
        "{}",
        error.message
    );

    let error = run_with(&h, "+a($x)", &params(json!({"x": 1, "typo": 2})))
        .await
        .unwrap_err();
    assert_eq!(error.code, Some(ErrorCode::Validation));
    assert!(error.message.contains("$typo"), "{}", error.message);

    let error = run(&h, "+a($x)").await.unwrap_err();
    assert!(
        error.message.contains("$x has no value"),
        "{}",
        error.message
    );

    let error = run_with(
        &h,
        "+a(1)\nq(X) <- a(X), Y = X + $s",
        &params(json!({"s": "1"})),
    )
    .await
    .unwrap_err();
    assert!(
        error.message.contains("$s is a string"),
        "{}",
        error.message
    );

    // Nothing ran.
    assert!(rows(&h, "?a(X)").await.is_empty());
    assert!(rows(&h, "?b(X, Y)").await.is_empty());
}

#[tokio::test]
async fn a_failing_statement_still_rolls_back_a_parameterised_program() {
    let dir = TempDir::new().unwrap();
    let h = handler(&dir);
    run(&h, "+person(name: string, age: int)").await.unwrap();
    // The second insert breaks the schema: neither commits.
    let result = run_with(
        &h,
        "+person($name, $age)\n+person($name, $bad)",
        &params(json!({"name": "ann", "age": 30, "bad": "thirty"})),
    )
    .await;
    match result {
        Ok(result) => assert!(!result.errors.is_empty()),
        Err(error) => assert_eq!(error.code, Some(ErrorCode::Validation)),
    }
    assert!(rows(&h, "?person(N, A)").await.is_empty());
}

#[tokio::test]
async fn meta_commands_take_no_parameters() {
    let dir = TempDir::new().unwrap();
    let h = handler(&dir);
    run(&h, "+a(1)").await.unwrap();
    for program in [".why ?a($x)", ".debug ?a($x)"] {
        let error = run_with(&h, program, &params(json!({"x": 1})))
            .await
            .unwrap_err();
        assert!(
            error.message.contains("take no parameters"),
            "{program}: {}",
            error.message
        );
    }
    // Inside a string literal, `$x` is text.
    run(&h, "+t(\"$x\")").await.unwrap();
    assert_eq!(rows(&h, "?t(V)").await, vec![vec![s("$x")]]);
}

#[tokio::test]
async fn parameters_count_toward_the_program_size_limit() {
    let dir = TempDir::new().unwrap();
    let mut config = config(&dir);
    config.storage.performance.max_query_size_bytes = 1_000;
    let h = Handler::new(StorageEngine::new(config).unwrap());
    let error = run_with(&h, "+a($x)", &params(json!({"x": "y".repeat(2_000)})))
        .await
        .unwrap_err();
    assert!(error.message.contains("too large"), "{}", error.message);
    assert!(rows(&h, "?a(X)").await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_never_see_each_others_values() {
    let dir = TempDir::new().unwrap();
    let h = std::sync::Arc::new(handler(&dir));
    let writers: Vec<_> = (0..16)
        .map(|i| {
            let h = std::sync::Arc::clone(&h);
            tokio::spawn(async move {
                let key = format!("key-{i}");
                let p = params(json!({"k": key, "v": i}));
                run_with(&h, "+kv($k, $v)", &p).await.unwrap();
                let rows = rows_with(&h, "?kv($k, V)", &params(json!({"k": key}))).await;
                assert_eq!(
                    rows,
                    vec![vec![s(&format!("key-{i}")), WireValue::Int64(i)]]
                );
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }
    assert_eq!(rows(&h, "?kv(K, V)").await.len(), 16);
}

/// A program with parameters gives exactly the results of the same program
/// with each value written as its literal.
#[tokio::test]
async fn binding_equals_the_literal_program() {
    let dir = TempDir::new().unwrap();
    let h = handler(&dir);
    run(
        &h,
        "+r[(1, \"a\", 1.5, true), (2, \"b\", 2.5, false), (3, \"c\", 3.5, true)]",
    )
    .await
    .unwrap();
    for (with_params, p, literal) in [
        (
            "?r(I, S, F, B), I >= $i",
            json!({"i": 2}),
            "?r(I, S, F, B), I >= 2",
        ),
        ("?r(I, $s, F, B)", json!({"s": "b"}), "?r(I, \"b\", F, B)"),
        (
            "?r(I, S, F, $b), F < $f",
            json!({"b": true, "f": 3.0}),
            "?r(I, S, F, true), F < 3.0",
        ),
        (
            "?r(I, S, F, B), G = F * $m",
            json!({"m": 2}),
            "?r(I, S, F, B), G = F * 2",
        ),
    ] {
        assert_eq!(
            rows_with(&h, with_params, &params(p)).await,
            rows(&h, literal).await,
            "{with_params}"
        );
    }
}

mod ws {
    //! The `params` field of a real `/ws` `execute` frame.

    use std::sync::Arc;
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use inputlayer::protocol::rest::create_router;
    use inputlayer::protocol::Handler;
    use inputlayer::Config;
    use serde_json::{json, Value};
    use tempfile::TempDir;
    use tokio_tungstenite::tungstenite::Message;

    const PASSWORD: &str = "ws-params-test-pw";

    async fn connect() -> (
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tokio::task::JoinHandle<()>,
        TempDir,
    ) {
        let tmp = TempDir::new().unwrap();
        let mut config = Config::default();
        config.storage.data_dir = tmp.path().join("data");
        config.http.auth.bootstrap_admin_password = Some(PASSWORD.to_string());
        config.http.auth.credentials_file = Some(tmp.path().join("credentials.toml"));
        config.http.rate_limit.ws_max_messages_per_sec = 0;
        config.http.gui.enabled = false;
        let handler = Arc::new(Handler::from_config(config).unwrap());
        handler.bootstrap_auth().unwrap();
        let app = create_router(Arc::clone(&handler), &handler.config().http);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
            .await
            .unwrap();
        ws.send(Message::Text(
            json!({"type": "login", "id": "login", "username": "admin", "password": PASSWORD})
                .to_string(),
        ))
        .await
        .unwrap();
        let mut client = (ws, task, tmp);
        let reply = recv(&mut client.0).await;
        assert_eq!(reply["type"], "authenticated", "{reply}");
        client
    }

    async fn recv(
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Value {
        loop {
            let msg = tokio::time::timeout(Duration::from_secs(30), ws.next())
                .await
                .expect("timed out")
                .expect("closed")
                .unwrap();
            if let Message::Text(text) = msg {
                let value: Value = serde_json::from_str(&text).unwrap();
                // Skip pushes: only replies carry the request id.
                if value.get("id").is_some() {
                    return value;
                }
            }
        }
    }

    /// Send `frame` (raw JSON text) and return its reply.
    async fn request(
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        frame: &str,
    ) -> Value {
        ws.send(Message::Text(frame.to_string())).await.unwrap();
        recv(ws).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_frames_carry_params() {
        let (mut ws, task, _tmp) = connect().await;
        let reply = request(
            &mut ws,
            r#"{"type":"execute","id":"w","program":"+eta($s, $d)","params":{"s":"S-77\"), +x(1","d":{"int":"20261010"}}}"#,
        )
        .await;
        assert_eq!(reply["type"], "result", "{reply}");
        assert_eq!(reply["id"], "w");

        let reply = request(
            &mut ws,
            r#"{"type":"execute","id":"q","program":"?eta($s, D)","params":{"s":"S-77\"), +x(1"}}"#,
        )
        .await;
        assert_eq!(reply["type"], "result", "{reply}");
        assert_eq!(reply["rows"], json!([["S-77\"), +x(1", 20_261_010]]));

        // A value that cannot be bound exactly fails the frame, nothing runs.
        for (id, params) in [
            ("n", r#"{"s": null}"#),
            ("o", r#"{"s": 9223372036854775808}"#),
            ("d", r#"{"s": 1, "s": 2}"#),
        ] {
            let frame = format!(
                r#"{{"type":"execute","id":"{id}","program":"+bad($s)","params":{params}}}"#
            );
            let reply = request(&mut ws, &frame).await;
            assert_eq!(reply["type"], "error", "{reply}");
            assert_eq!(reply["code"], "invalid_request", "{reply}");
            assert_eq!(reply["id"], id, "{reply}");
        }

        // A missing parameter is a validation error naming its statement.
        let reply = request(
            &mut ws,
            r#"{"type":"execute","id":"m","program":"+bad($s)"}"#,
        )
        .await;
        assert_eq!(reply["type"], "error", "{reply}");
        assert_eq!(reply["code"], "validation", "{reply}");
        let detail = reply["validation_errors"][0]["error"].as_str().unwrap();
        assert!(detail.contains("$s has no value"), "{reply}");

        // Standing queries take none.
        let reply = request(
            &mut ws,
            r#"{"type":"execute","id":"s","program":".subscribe s ?eta($s, D)","params":{"s":"S-77"}}"#,
        )
        .await;
        assert_eq!(reply["type"], "error", "{reply}");
        assert_eq!(reply["code"], "validation", "{reply}");

        let reply = request(
            &mut ws,
            r#"{"type":"execute","id":"r","program":"?bad(X)"}"#,
        )
        .await;
        assert_eq!(reply["rows"], json!([]), "{reply}");
        task.abort();
    }
}
