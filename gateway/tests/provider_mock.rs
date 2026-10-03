//! Provider selection against a mock upstream on loopback: the configured
//! provider is built exactly as the binary builds it, then driven through
//! the `Extractor`/`Completer` traits the pipeline uses. No model key or
//! network is needed.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use inputlayer_gateway::model::{ChatParams, UpstreamStatus};
use inputlayer_gateway::provider::{ModelProvider, ProviderConfig};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// One recorded upstream request: (authorization header, JSON body).
type Seen = (Option<String>, Value);

#[derive(Default)]
struct Upstream {
    replies: Mutex<VecDeque<(u16, String)>>,
    seen: Mutex<Vec<Seen>>,
}

async fn reply(
    State(upstream): State<Arc<Upstream>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, String) {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let body = serde_json::from_slice(&body).unwrap_or(Value::Null);
    upstream.seen.lock().expect("lock").push((auth, body));
    let (status, text) = upstream
        .replies
        .lock()
        .expect("lock")
        .pop_front()
        .unwrap_or((500, "{}".to_string()));
    (StatusCode::from_u16(status).expect("status"), text)
}

/// Serve scripted replies on both provider endpoints; returns the root URL.
async fn mock(replies: Vec<(u16, String)>) -> (String, Arc<Upstream>) {
    let upstream = Arc::new(Upstream {
        replies: Mutex::new(replies.into()),
        seen: Mutex::default(),
    });
    let app = Router::new()
        .route("/v1/chat/completions", post(reply))
        .route("/v1/messages", post(reply))
        .with_state(Arc::clone(&upstream));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, app).await });
    (format!("http://{addr}"), upstream)
}

fn provider(vars: &[(&str, &str)]) -> ModelProvider {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    ProviderConfig::from_lookup(|name| vars.get(name).cloned())
        .expect("valid config")
        .expect("provider selected")
        .build()
}

fn local(root: &str, key: Option<&str>) -> ModelProvider {
    let base = format!("{root}/v1");
    let mut vars = vec![
        ("GATEWAY_MODEL_PROVIDER", "openai-compatible"),
        ("OPENAI_BASE_URL", base.as_str()),
        ("GATEWAY_CHAT_MODEL", "local-chat"),
        ("GATEWAY_EXTRACTION_MODEL", "local-extract"),
    ];
    if let Some(key) = key {
        vars.push(("OPENAI_API_KEY", key));
    }
    provider(&vars)
}

fn chat(content: &str, finish_reason: &str) -> (u16, String) {
    let body = json!({
        "choices": [{ "message": { "role": "assistant", "content": content },
                      "finish_reason": finish_reason }],
        "usage": { "prompt_tokens": 11, "completion_tokens": 3 },
    });
    (200, body.to_string())
}

fn params(model: String) -> ChatParams {
    ChatParams {
        model,
        system: Some("Be brief.".to_string()),
        messages: vec![("user".to_string(), "hi".to_string())],
        max_tokens: 32,
        temperature: None,
        top_p: None,
        stop: Vec::new(),
    }
}

fn upstream_status(err: &anyhow::Error) -> Option<u16> {
    err.downcast_ref::<UpstreamStatus>().map(|s| s.0)
}

#[tokio::test]
async fn local_provider_extracts_with_the_configured_model_and_schema() {
    let claims = json!({ "claims": [{ "id": "c1" }] });
    let (root, upstream) = mock(vec![chat(&claims.to_string(), "stop")]).await;
    let provider = local(&root, None);
    let schema = json!({ "type": "object", "additionalProperties": false });
    let model = provider.routing.extraction_model("claude-haiku-4-5");
    let extraction = provider
        .extractor
        .extract(model, "STATIC", "[0] user: hi\n", &schema)
        .await
        .expect("extraction");
    assert_eq!(extraction.output, claims);
    assert_eq!(extraction.usage["prompt_tokens"], 11);

    let seen = upstream.seen.lock().expect("lock");
    let (auth, body) = &seen[0];
    assert_eq!(*auth, None, "no key configured, no bearer sent");
    assert_eq!(
        body["model"], "local-extract",
        "the pack's Claude id never reaches a local server"
    );
    assert_eq!(body["messages"][0]["content"], "STATIC");
    assert_eq!(body["response_format"]["json_schema"]["schema"], schema);
}

#[tokio::test]
async fn local_provider_completes_with_bearer_and_maps_finish_reason() {
    let (root, upstream) = mock(vec![chat("Bonjour", "length")]).await;
    let provider = local(&root, Some("sk-local"));
    let model = provider.routing.chat_model(Some("llama3.3"));
    let completion = provider
        .completer
        .complete(&params(model))
        .await
        .expect("completion");
    assert_eq!(completion.text, "Bonjour");
    assert_eq!(completion.finish_reason, "length");
    assert_eq!(
        (completion.prompt_tokens, completion.completion_tokens),
        (11, 3)
    );

    let seen = upstream.seen.lock().expect("lock");
    let (auth, body) = &seen[0];
    assert_eq!(auth.as_deref(), Some("Bearer sk-local"));
    assert_eq!(body["model"], "llama3.3");
    assert_eq!(body["messages"][0]["role"], "system");
}

#[tokio::test]
async fn local_provider_errors_keep_status_and_refuse_partial_facts() {
    let schema = json!({ "type": "object" });
    let (root, _upstream) = mock(vec![
        (
            401,
            json!({ "error": { "message": "bad key" } }).to_string(),
        ),
        (502, "<html>bad gateway</html>".to_string()),
        (404, json!({ "error": "model 'x' not found" }).to_string()),
        chat("{\"claims\": [", "length"),
        chat("not json", "stop"),
    ])
    .await;
    let provider = local(&root, None);
    let extract = || provider.extractor.extract("m", "S", "U", &schema);

    let err = extract().await.expect_err("401");
    assert_eq!(upstream_status(&err), Some(401));
    assert!(err.to_string().contains("bad key"), "{err:#}");

    let err = extract().await.expect_err("html 502");
    assert_eq!(
        upstream_status(&err),
        Some(502),
        "status survives a non-JSON body"
    );

    let err = provider
        .completer
        .complete(&params("x".to_string()))
        .await
        .expect_err("404");
    assert_eq!(upstream_status(&err), Some(404));
    assert!(err.to_string().contains("model 'x' not found"), "{err:#}");

    let err = extract().await.expect_err("truncated");
    assert!(
        err.to_string().contains("refusing partial facts"),
        "{err:#}"
    );

    let err = extract().await.expect_err("not json");
    assert!(err.to_string().contains("not valid JSON"), "{err:#}");
}

#[tokio::test]
async fn unreachable_provider_is_an_error_not_a_hang() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let root = format!("http://{}", listener.local_addr().expect("addr"));
    drop(listener);
    let provider = local(&root, None);
    let err = provider
        .completer
        .complete(&params("x".to_string()))
        .await
        .expect_err("refused");
    assert!(
        err.to_string().contains("completion request failed"),
        "{err:#}"
    );
    assert_eq!(upstream_status(&err), None);
}

#[tokio::test]
async fn anthropic_base_url_comes_from_the_validated_config() {
    let (root, upstream) = mock(vec![(
        429,
        json!({ "error": { "message": "slow down" } }).to_string(),
    )])
    .await;
    let provider = provider(&[
        ("ANTHROPIC_API_KEY", "sk-ant-test"),
        ("ANTHROPIC_BASE_URL", &root),
    ]);
    assert_eq!(provider.name(), "anthropic");
    let model = provider.routing.chat_model(Some("gpt-4o"));
    let err = provider
        .completer
        .complete(&params(model))
        .await
        .expect_err("429");
    assert_eq!(upstream_status(&err), Some(429));
    let seen = upstream.seen.lock().expect("lock");
    assert_eq!(seen[0].1["model"], "claude-sonnet-5");
}

/// Adapter overhead: per-call wall time of each provider client against
/// an instant loopback upstream, i.e. request mapping + HTTP + response
/// mapping with zero model time. Measurement, not a correctness check:
/// skipped unless `INPUTLAYER_MEASURE` is set. Run explicitly:
/// `INPUTLAYER_MEASURE=1 cargo test -p inputlayer-gateway --test provider_mock -- --nocapture`
#[tokio::test]
async fn adapter_overhead() {
    if std::env::var_os("INPUTLAYER_MEASURE").is_none() {
        eprintln!("skipping measurement: set INPUTLAYER_MEASURE=1 to run");
        return;
    }
    const CALLS: usize = 2000;
    let anthropic_reply = json!({
        "content": [{ "type": "text", "text": "{\"claims\": []}" }],
        "stop_reason": "end_turn",
        "usage": { "input_tokens": 1, "output_tokens": 1 },
    })
    .to_string();
    let openai_reply = chat("{\"claims\": []}", "stop");
    let schema = json!({ "type": "object" });
    for name in ["anthropic", "openai-compatible"] {
        let reply = if name == "anthropic" {
            (200, anthropic_reply.clone())
        } else {
            openai_reply.clone()
        };
        let (root, _upstream) = mock(vec![reply; CALLS + 100]).await;
        let provider = if name == "anthropic" {
            provider(&[("ANTHROPIC_API_KEY", "k"), ("ANTHROPIC_BASE_URL", &root)])
        } else {
            local(&root, None)
        };
        for _ in 0..100 {
            provider
                .extractor
                .extract("m", "S", "U", &schema)
                .await
                .expect("warm");
        }
        let mut micros: Vec<u128> = Vec::with_capacity(CALLS);
        for _ in 0..CALLS {
            let started = std::time::Instant::now();
            provider
                .extractor
                .extract("m", "S", "U", &schema)
                .await
                .expect("call");
            micros.push(started.elapsed().as_micros());
        }
        micros.sort_unstable();
        println!(
            "{name}: extract p50 {}us p99 {}us over {CALLS} loopback calls",
            micros[CALLS / 2],
            micros[CALLS * 99 / 100]
        );
    }
}
