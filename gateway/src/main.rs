//! InputLayer Gateway service - the model gateway of the stack.
//!
//! `POST /v1/chat/completions` is the ONLY ingestion path: the standard
//! OpenAI request shape, completions from the model provider. Translation
//! is opt-in per request: the `x-il-ontology` header lists KG/ontology
//! PAIRS (`<kg>/<ontology>[@version]`, comma-separated; or the
//! `il_ontology` body field); omitted, the gateway is a pure OpenAI proxy.
//! Selected, each pair's pack translates the conversation into tuples
//! (one extraction prompt per pack), the tuples are stored
//! conversation-prefixed into the pair's KG (whose ontology must have been
//! installed by the engine's `.ontology install` - the gateway NEVER
//! deploys rules), the pack's rules evaluate incrementally, and
//! per-ontology reports ride the response under `inputlayer.reports`.
//!
//! Observers subscribe per conversation: `WS /v1/events?conversation=<id>`
//! streams translation, finding (with engine-produced proof trees), and
//! report events.
//!
//! Configuration (env):
//!   GATEWAY_HOST / GATEWAY_PORT   bind address (defaults 127.0.0.1:8081)
//!   GATEWAY_API_KEY               when set, /v1/* require
//!                                 "Authorization: Bearer <key>"; unset
//!                                 leaves them open (local dev) with a
//!                                 startup warning
//!   INPUTLAYER_REGISTRY           HTTPS index URL, or a local filesystem
//!                                 path to a cloned registry (no outbound
//!                                 network); INPUTLAYER_REGISTRY_TOKEN for
//!                                 private remotes
//!   INPUTLAYER_URL / INPUTLAYER_API_KEY  engine access
//!   ANTHROPIC_API_KEY             model provider key (never seen by the
//!                                 engine); without it /v1/* return 503

use anyhow::{Context, Result};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use inputlayer_gateway::events::EventHub;
use inputlayer_gateway::model::{
    render_conversation, AnthropicClient, ChatParams, Completer, Extractor,
};
use inputlayer_gateway::ontology::LoadedOntology;
use inputlayer_gateway::pipeline::{evaluate, EngineConfig};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Fallback when the request's model is not a claude-* id (issue #84:
/// forward claude models as-is, route everything else here).
const DEFAULT_CHAT_MODEL: &str = "claude-sonnet-5";

struct AppState {
    http: reqwest::Client,
    engine_url: String,
    engine: EngineConfig,
    ontologies: HashMap<String, Arc<LoadedOntology>>,
    extractor: Option<Arc<dyn Extractor>>,
    completer: Option<Arc<dyn Completer>>,
    /// Bearer token required on /v1/* when configured.
    api_key: Option<String>,
    events: EventHub,
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// Load every ontology published in the registry index, pinning each at its
/// latest version and digest for the process lifetime. The gateway loads
/// pack CONTENT (prompts, schemas, mapping specs) - rules are deployed to
/// KGs by the engine's `.ontology install`, never from here.
async fn load_ontologies() -> Result<HashMap<String, Arc<LoadedOntology>>> {
    use inputlayer_ontology_client::registry::Registry;
    // Compose passes these through even when unset on the host; an empty
    // token must mean anonymous, not "Authorization: Bearer <nothing>".
    let token = std::env::var("INPUTLAYER_REGISTRY_TOKEN")
        .ok()
        .filter(|t| !t.trim().is_empty())
        .or_else(|| std::env::var("GITHUB_TOKEN").ok())
        .filter(|t| !t.trim().is_empty());
    let index_url = std::env::var("INPUTLAYER_REGISTRY")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| inputlayer_ontology_client::registry::DEFAULT_INDEX_URL.to_string());
    let registry = Registry::new(index_url, token);
    let index = registry.index().await.context("fetching registry index")?;
    let mut loaded = HashMap::new();
    for spec in index.entries.keys() {
        let (name, entry) = registry
            .resolve(spec)
            .await
            .with_context(|| format!("resolving ontology {spec}"))?;
        let entry_dir = registry
            .fetch(&name, &entry)
            .await
            .with_context(|| format!("fetching {spec}"))?;
        let ontology = LoadedOntology::load(&entry_dir, &entry.digest)
            .with_context(|| format!("loading {spec}"))?;
        println!(
            "ontology loaded: {}@{} ({})",
            ontology.name, ontology.version, ontology.digest
        );
        loaded.insert(ontology.name.clone(), Arc::new(ontology));
    }
    Ok(loaded)
}

#[tokio::main]
async fn main() -> Result<()> {
    let host = env_or("GATEWAY_HOST", "127.0.0.1");
    let port: u16 = env_or("GATEWAY_PORT", "8081")
        .parse()
        .context("GATEWAY_PORT must be a port number")?;
    let engine_url = env_or("INPUTLAYER_URL", "http://127.0.0.1:8080")
        .trim_end_matches('/')
        .to_string();

    // An unreachable registry degrades gracefully: the gateway still boots
    // (health/ready keep working) and evaluation answers 503 until restart.
    let ontologies = load_ontologies().await.unwrap_or_else(|err| {
        println!("warning: could not load registry: {err:#}");
        HashMap::new()
    });
    if ontologies.is_empty() {
        println!("no ontologies loaded - evaluation requests will 503");
    }
    let model_client: Option<Arc<AnthropicClient>> = match std::env::var("ANTHROPIC_API_KEY") {
        Ok(key) if !key.trim().is_empty() => Some(Arc::new(AnthropicClient::new(key))),
        _ => {
            println!("ANTHROPIC_API_KEY not set - /v1/chat/completions will 503");
            None
        }
    };
    let extractor: Option<Arc<dyn Extractor>> =
        model_client.clone().map(|c| c as Arc<dyn Extractor>);
    let completer: Option<Arc<dyn Completer>> = model_client.map(|c| c as Arc<dyn Completer>);

    let api_key = std::env::var("GATEWAY_API_KEY")
        .ok()
        .map(|k| k.trim().to_string())
        .filter(|k| !k.is_empty());
    if api_key.is_none() {
        println!("warning: GATEWAY_API_KEY not set - /v1/* endpoints are UNAUTHENTICATED");
    }

    // Readiness probes trigger an outbound engine call; without a timeout a
    // wedged engine turns every probe into a hung socket.
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;

    let state = Arc::new(AppState {
        http,
        engine_url: engine_url.clone(),
        engine: EngineConfig {
            url: engine_url,
            api_key: env_or("INPUTLAYER_API_KEY", ""),
        },
        ontologies,
        extractor,
        completer,
        api_key,
        events: EventHub::default(),
    });

    // Browser clients (the Studio) may run on a different origin than the
    // gateway; the API is bearer-authenticated, so permissive CORS is the
    // standard posture (same as any public model API).
    let cors = tower_http::cors::CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([axum::http::Method::GET, axum::http::Method::POST])
        .allow_headers(tower_http::cors::Any);

    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/ontologies", get(list_ontologies))
        .route("/v1/events", get(events_ws))
        .layer(cors)
        .with_state(state);

    let addr = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("InputLayer Gateway listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<Arc<AppState>>) -> Json<Value> {
    let published: Vec<String> = state
        .ontologies
        .values()
        .map(|o| format!("{}@{}", o.name, o.version))
        .collect();
    Json(json!({
        "status": "ok",
        "service": "inputlayer-gateway",
        "version": env!("CARGO_PKG_VERSION"),
        "model_key_configured": state.extractor.is_some(),
        "ontologies": published,
    }))
}

async fn ready(State(state): State<Arc<AppState>>) -> (StatusCode, Json<Value>) {
    let engine_health = format!("{}/health", state.engine_url);
    let engine_ok = state
        .http
        .get(&engine_health)
        .send()
        .await
        .is_ok_and(|r| r.status().is_success());
    if engine_ok {
        (StatusCode::OK, Json(json!({ "status": "ready" })))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "engine unreachable" })),
        )
    }
}

fn bad_request(message: String) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": { "type": "invalid_request", "message": message } })),
    )
}

/// Bearer auth on the /v1/* endpoints when GATEWAY_API_KEY is configured.
/// The scheme is case-insensitive per RFC 7235; the comparison goes through
/// SHA-256 digests so equality time does not depend on where the presented
/// key diverges. `extra_token` supports browser WebSocket clients, which
/// cannot set headers (?token=).
fn authorize(
    state: &AppState,
    headers: &HeaderMap,
    extra_token: Option<&str>,
) -> Result<(), (StatusCode, Json<Value>)> {
    use sha2::Digest;
    let Some(expected) = &state.api_key else {
        return Ok(());
    };
    let expected_digest = sha2::Sha256::digest(expected.as_bytes());
    let header_token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let (scheme, token) = v.split_once(' ')?;
            scheme
                .eq_ignore_ascii_case("bearer")
                .then(|| token.trim_start())
        });
    let matches = header_token
        .into_iter()
        .chain(extra_token)
        .any(|token| sha2::Sha256::digest(token.as_bytes()) == expected_digest);
    if matches {
        Ok(())
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": { "type": "unauthorized",
                "message": "missing or invalid Authorization: Bearer token" } })),
        ))
    }
}

/// The published ontology set: what a selection pair can reference.
async fn list_ontologies(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    if let Err(response) = authorize(&state, &headers, None) {
        return response;
    }
    let mut ontologies: Vec<Value> = state
        .ontologies
        .values()
        .map(|o| {
            json!({
                "name": o.name,
                "version": o.version,
                "digest": o.digest,
                "title": o.manifest.ontology.title,
            })
        })
        .collect();
    ontologies.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    (StatusCode::OK, Json(json!({ "ontologies": ontologies })))
}

/// One selected (kg, ontology) pair.
struct Selection {
    kg: String,
    ontology: Arc<LoadedOntology>,
}

/// Parse `<kg>/<ontology>[@version]` pairs from the header (comma list) or
/// body field (string or array). The named KG must exist engine-side with
/// the ontology installed - that is checked at evaluation time; here the
/// pair must be well-formed and reference a published pack, with any
/// version assertion matching the gateway's boot-time pin.
fn parse_selection(
    state: &AppState,
    headers: &HeaderMap,
    body_field: Option<&Value>,
) -> Result<Vec<Selection>, (StatusCode, Json<Value>)> {
    let mut specs: Vec<String> = Vec::new();
    if let Some(value) = headers.get("x-il-ontology") {
        match value.to_str() {
            Ok(v) => specs.extend(
                v.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string),
            ),
            Err(_) => {
                return Err(bad_request(
                    "x-il-ontology header is not valid ASCII".to_string(),
                ));
            }
        }
    }
    if specs.is_empty() {
        match body_field {
            None | Some(Value::Null) => {}
            Some(Value::String(s)) => specs.extend(
                s.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string),
            ),
            Some(Value::Array(items)) => {
                for item in items {
                    match item.as_str() {
                        Some(s) if !s.trim().is_empty() => specs.push(s.trim().to_string()),
                        _ => {
                            return Err(bad_request(
                                "il_ontology array entries must be non-empty strings".to_string(),
                            ));
                        }
                    }
                }
            }
            Some(_) => {
                return Err(bad_request(
                    "il_ontology must be a string or an array of strings".to_string(),
                ));
            }
        }
    }

    let mut selections = Vec::new();
    for spec in specs {
        let Some((kg, rest)) = spec.split_once('/') else {
            return Err(bad_request(format!(
                "selection {spec:?} must be <kg>/<ontology>[@version]"
            )));
        };
        if let Err(err) =
            inputlayer_ontology_client::registry::validate_component("knowledge graph", kg)
        {
            return Err(bad_request(err.to_string()));
        }
        let (name, version) = match rest.split_once('@') {
            Some((n, v)) => (n, Some(v)),
            None => (rest, None),
        };
        let Some(ontology) = state.ontologies.get(name) else {
            let mut names: Vec<&str> = state.ontologies.keys().map(String::as_str).collect();
            names.sort_unstable();
            return Err(bad_request(format!(
                "ontology {name:?} is not published; available: {}",
                names.join(", ")
            )));
        };
        if let Some(asserted) = version {
            if asserted != ontology.version {
                return Err(bad_request(format!(
                    "ontology {name} is pinned at {}, request asserted {asserted}",
                    ontology.version
                )));
            }
        }
        selections.push(Selection {
            kg: kg.to_string(),
            ontology: Arc::clone(ontology),
        });
    }
    Ok(selections)
}

/// Tracing is per-request opt-in. Like x-il-ontology, a malformed value is
/// explicit intent that failed: reject it rather than silently not tracing.
fn parse_trace_header(headers: &HeaderMap) -> Result<bool, (StatusCode, Json<Value>)> {
    match headers.get("x-il-trace") {
        None => Ok(false),
        Some(value) => match value.to_str().map(|v| v.trim().to_ascii_lowercase()) {
            Ok(v) if v == "1" || v == "true" => Ok(true),
            Ok(v) if v.is_empty() || v == "0" || v == "false" => Ok(false),
            _ => Err(bad_request(
                "x-il-trace must be 1/true or 0/false".to_string(),
            )),
        },
    }
}

/// OpenAI chat completion request shape; unknown fields are ignored.
#[derive(Deserialize)]
struct ChatRequest {
    #[serde(default)]
    model: Option<String>,
    messages: Vec<ChatMessage>,
    #[serde(default)]
    max_tokens: Option<u32>,
    #[serde(default)]
    temperature: Option<f64>,
    #[serde(default)]
    top_p: Option<f64>,
    #[serde(default)]
    stop: Option<Value>,
    #[serde(default)]
    stream: Option<bool>,
    #[serde(default)]
    il_ontology: Option<Value>,
    #[serde(default)]
    il_mode: Option<String>,
    #[serde(default)]
    il_conversation: Option<String>,
}

#[derive(Deserialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[allow(clippy::too_many_lines)]
async fn chat_completions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<ChatRequest>,
) -> (StatusCode, Json<Value>) {
    if let Err(response) = authorize(&state, &headers, None) {
        return response;
    }
    let Some(completer) = state.completer.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": { "type": "not_configured",
                "message": "model key not configured (ANTHROPIC_API_KEY)" } })),
        );
    };
    if request.stream == Some(true) {
        return bad_request(
            "streaming is not supported yet (M2, \
             https://github.com/inputlayer/inputlayer/issues/85); send stream: false"
                .to_string(),
        );
    }
    // Configuration failures come FIRST: with no ontologies loaded (an
    // unreachable registry at startup) every selection would otherwise be
    // reported as "ontology not published", blaming the caller for the
    // operator's outage.
    let wants_evaluation = headers.contains_key("x-il-ontology")
        || request.il_ontology.as_ref().is_some_and(|v| !v.is_null());
    if wants_evaluation && state.ontologies.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": { "type": "not_configured",
                "message": "no ontologies loaded (registry unreachable at startup)" } })),
        );
    }
    let selections = match parse_selection(&state, &headers, request.il_ontology.as_ref()) {
        Ok(selections) => selections,
        Err(response) => return response,
    };
    let want_trace = match parse_trace_header(&headers) {
        Ok(flag) => flag,
        Err(response) => return response,
    };
    // Mode: header wins over body field, default annotate. Enforce fails
    // CLOSED (D4): refuse on blocking findings, refuse when any selected
    // pair could not be evaluated.
    let mode = headers
        .get("x-il-mode")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| request.il_mode.clone())
        .unwrap_or_else(|| "annotate".to_string());
    if mode != "annotate" && mode != "enforce" {
        return bad_request(format!(
            "il_mode must be \"annotate\" or \"enforce\", got {mode:?}"
        ));
    }
    if request.messages.is_empty() {
        return bad_request("messages must not be empty".to_string());
    }
    // Conversation identity: header wins over body field. Optional - a
    // request without one is one-shot (synthetic prefix, retracted after).
    let conversation = headers
        .get("x-il-conversation")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| {
            request
                .il_conversation
                .as_deref()
                .map(str::trim)
                .map(str::to_string)
                .filter(|v| !v.is_empty())
        });
    if let Some(id) = &conversation {
        if id.len() > 64
            || inputlayer_ontology_client::registry::validate_component("conversation id", id)
                .is_err()
        {
            return bad_request(
                "il_conversation must be 1-64 characters of [A-Za-z0-9._-]".to_string(),
            );
        }
    }

    if selections.is_empty() {
        // Pure OpenAI proxy: no translation, no engine, no extra fields.
        if mode == "enforce" {
            return bad_request(
                "enforce mode requires an ontology selection (x-il-ontology)".to_string(),
            );
        }
        if want_trace {
            return bad_request(
                "x-il-trace requires an ontology selection (there is nothing to trace)".to_string(),
            );
        }
        let model = route_model(request.model.as_deref());
        let params = match chat_params(&request, model) {
            Ok(params) => params,
            Err(response) => return response,
        };
        let response_model = params.model.clone();
        return match completer.complete(&params).await {
            Ok(completion) => (
                StatusCode::OK,
                Json(openai_response(&response_model, &completion, None)),
            ),
            Err(err) => upstream_error(&err),
        };
    }

    // Evaluation path. The extractor exists whenever the completer does.
    if state.ontologies.is_empty() || state.extractor.is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": { "type": "not_configured",
                "message": "no ontologies loaded (registry unreachable at startup)" } })),
        );
    }
    let request_id = fresh_id("req");
    let prefix = conversation.clone().unwrap_or_else(|| request_id.clone());
    let retract_after = conversation.is_none();

    let messages: Vec<(String, String)> = request
        .messages
        .iter()
        .map(|m| (m.role.clone(), m.content.clone()))
        .collect();
    let model = route_model(request.model.as_deref());
    let params = match chat_params(&request, model) {
        Ok(params) => params,
        Err(response) => return response,
    };
    let response_model = params.model.clone();

    let run_reports = evaluate_all(
        &state,
        &selections,
        &messages,
        &prefix,
        want_trace,
        retract_after,
    );

    if mode == "enforce" {
        // Verify FIRST: a refused conversation never spends completion
        // tokens; an unevaluatable one refuses too (fail closed).
        let reports = run_reports.await;
        publish_events(&state, conversation.as_deref(), &request_id, &reports);
        if let Some(refusal) = enforcement_refusal(&reports) {
            return refusal;
        }
        match completer.complete(&params).await {
            Ok(completion) => (
                StatusCode::OK,
                Json(openai_response(
                    &response_model,
                    &completion,
                    Some(reports_json(&reports)),
                )),
            ),
            Err(err) => upstream_error(&err),
        }
    } else {
        // annotate: completion and evaluation run concurrently.
        let (completion, reports) = tokio::join!(completer.complete(&params), run_reports);
        publish_events(&state, conversation.as_deref(), &request_id, &reports);
        match completion {
            Ok(completion) => (
                StatusCode::OK,
                Json(openai_response(
                    &response_model,
                    &completion,
                    Some(reports_json(&reports)),
                )),
            ),
            Err(err) => upstream_error(&err),
        }
    }
}

/// One evaluated pair's outcome, ready for both response and events.
struct Report {
    kg: String,
    ontology: String,
    digest: String,
    status: &'static str,
    reason: Option<String>,
    findings: Vec<Value>,
    dropped: Vec<String>,
    notes: Vec<String>,
    tuples: Vec<Value>,
    trace: Option<Value>,
}

/// Extract + evaluate every selected pair concurrently. Failures become
/// "incomplete" reports (with reason), never panics or dropped pairs.
async fn evaluate_all(
    state: &AppState,
    selections: &[Selection],
    messages: &[(String, String)],
    prefix: &str,
    want_trace: bool,
    retract_after: bool,
) -> Vec<Report> {
    let futures = selections.iter().map(|selection| {
        let ontology = Arc::clone(&selection.ontology);
        let kg = selection.kg.clone();
        async move {
            let outcome = evaluate_one(
                state,
                &ontology,
                &kg,
                messages,
                prefix,
                want_trace,
                retract_after,
            )
            .await;
            match outcome {
                Ok((findings, dropped, notes, tuples, trace)) => Report {
                    kg,
                    ontology: format!("{}@{}", ontology.name, ontology.version),
                    digest: ontology.digest.clone(),
                    status: "complete",
                    reason: None,
                    findings,
                    dropped,
                    notes,
                    tuples,
                    trace,
                },
                Err(err) => Report {
                    kg,
                    ontology: format!("{}@{}", ontology.name, ontology.version),
                    digest: ontology.digest.clone(),
                    status: "incomplete",
                    reason: Some(err.to_string()),
                    findings: Vec::new(),
                    dropped: Vec::new(),
                    notes: Vec::new(),
                    tuples: Vec::new(),
                    trace: None,
                },
            }
        }
    });
    futures_util::future::join_all(futures).await
}

type EvalParts = (
    Vec<Value>,
    Vec<String>,
    Vec<String>,
    Vec<Value>,
    Option<Value>,
);

async fn evaluate_one(
    state: &AppState,
    ontology: &LoadedOntology,
    kg: &str,
    messages: &[(String, String)],
    prefix: &str,
    want_trace: bool,
    retract_after: bool,
) -> Result<EvalParts> {
    let extractor = state
        .extractor
        .as_ref()
        .context("model key not configured")?;
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let system_prompt = ontology.render_prompt(&today);
    let user_content = render_conversation(messages);
    let extract_started = std::time::Instant::now();
    let extraction = extractor
        .extract(
            &ontology.extraction_model,
            &system_prompt,
            &user_content,
            &ontology.schema,
        )
        .await?;
    let extract_ms = extract_started.elapsed().as_millis();
    let outcome = evaluate(
        &state.engine,
        ontology,
        kg,
        prefix,
        extraction,
        messages,
        want_trace,
        retract_after,
    )
    .await?;
    let trace = outcome.trace.map(|mut trace| {
        if let Some(t) = trace.as_object_mut() {
            t.insert("model".to_string(), json!(ontology.extraction_model));
            t.insert("extract_ms".to_string(), json!(extract_ms));
        }
        trace
    });
    Ok((
        outcome.findings,
        outcome.dropped,
        outcome.notes,
        outcome.tuples,
        trace,
    ))
}

/// Enforce fails CLOSED: 422 on any blocking finding from any pair, 503
/// when any pair could not be evaluated at all.
fn enforcement_refusal(reports: &[Report]) -> Option<(StatusCode, Json<Value>)> {
    let blocking_finding = reports.iter().any(|r| {
        r.findings
            .iter()
            .any(|f| f["blocking"].as_bool() == Some(true))
    });
    if blocking_finding {
        return Some((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": { "type": "consistency_violation",
                    "message": "conversation triggers blocking findings; completion refused" },
                "inputlayer": reports_json(reports),
            })),
        ));
    }
    // Fail closed on ANY evaluation gap: an incomplete pair, or dropped
    // extraction rows (a misquoted claim is exactly what an engineered
    // bypass looks like - under enforce, partial evaluation refuses).
    if reports
        .iter()
        .any(|r| r.status != "complete" || !r.dropped.is_empty())
    {
        return Some((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": { "type": "verification_unavailable",
                    "message": "conversation could not be fully evaluated; completion refused (enforce fails closed)" },
                "inputlayer": reports_json(reports),
            })),
        ));
    }
    None
}

fn reports_json(reports: &[Report]) -> Value {
    let list: Vec<Value> = reports
        .iter()
        .map(|r| {
            let mut report = json!({
                "kg": r.kg,
                "ontology": r.ontology,
                "digest": r.digest,
                "status": r.status,
                "findings": r.findings,
                "dropped": r.dropped,
            });
            if !r.notes.is_empty() {
                report["notes"] = json!(r.notes);
            }
            if let Some(reason) = &r.reason {
                report["reason"] = json!(reason);
            }
            if let Some(trace) = &r.trace {
                report["trace"] = trace.clone();
            }
            report
        })
        .collect();
    json!({ "reports": list })
}

/// Emit translation / finding / report events for a conversation. One-shot
/// requests (no conversation id) emit nothing - there is no stream to
/// subscribe to.
fn publish_events(
    state: &AppState,
    conversation: Option<&str>,
    request_id: &str,
    reports: &[Report],
) {
    let Some(conversation) = conversation else {
        return;
    };
    for report in reports {
        state.events.publish(
            conversation,
            json!({
                "type": "translation",
                "conversation": conversation,
                "request": request_id,
                "kg": report.kg,
                "ontology": report.ontology,
                "tuples": report.tuples,
                "dropped": report.dropped,
            }),
        );
        for finding in &report.findings {
            let mut event = json!({
                "type": "finding",
                "conversation": conversation,
                "request": request_id,
                "kg": report.kg,
                "ontology": report.ontology,
            });
            if let (Some(event_map), Some(finding_map)) =
                (event.as_object_mut(), finding.as_object())
            {
                for (key, value) in finding_map {
                    event_map.insert(key.clone(), value.clone());
                }
            }
            state.events.publish(conversation, event);
        }
        let mut close = json!({
            "type": "report",
            "conversation": conversation,
            "request": request_id,
            "kg": report.kg,
            "ontology": report.ontology,
            "status": report.status,
            "findings": report.findings.len(),
        });
        if let Some(reason) = &report.reason {
            close["reason"] = json!(reason);
        }
        state.events.publish(conversation, close);
    }
}

#[derive(Deserialize)]
struct EventsQuery {
    conversation: String,
    #[serde(default)]
    token: Option<String>,
}

/// Conversation-scoped event subscription: replay the ring, then stream
/// live events until the client goes away.
async fn events_ws(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<EventsQuery>,
    upgrade: WebSocketUpgrade,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if let Err((code, body)) = authorize(&state, &headers, query.token.as_deref()) {
        return (code, body).into_response();
    }
    if query.conversation.is_empty()
        || query.conversation.len() > 64
        || inputlayer_ontology_client::registry::validate_component(
            "conversation id",
            &query.conversation,
        )
        .is_err()
    {
        return bad_request("conversation must be 1-64 characters of [A-Za-z0-9._-]".to_string())
            .into_response();
    }
    let conversation = query.conversation.clone();
    upgrade.on_upgrade(move |mut socket| async move {
        let (replay, mut rx) = state.events.subscribe(&conversation);
        for event in replay {
            if socket
                .send(axum::extract::ws::Message::Text(event.to_string()))
                .await
                .is_err()
            {
                return;
            }
        }
        loop {
            tokio::select! {
                event = rx.recv() => {
                    match event {
                        Ok(event) => {
                            if socket
                                .send(axum::extract::ws::Message::Text(event.to_string()))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        // Lagged: skip ahead; Closed: hub dropped.
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    }
                }
                incoming = socket.recv() => {
                    match incoming {
                        Some(Ok(_)) => {} // client pings/messages: ignore
                        _ => return,      // closed or errored
                    }
                }
            }
        }
    })
}

/// Model routing (#84): claude-* forwarded as-is, everything else falls
/// back to the default.
fn route_model(requested: Option<&str>) -> String {
    match requested {
        Some(model) if model.starts_with("claude-") => model.to_string(),
        _ => DEFAULT_CHAT_MODEL.to_string(),
    }
}

/// Map the OpenAI request to Anthropic chat params: system turns join into
/// the system prompt, consecutive same-role turns merge (the Messages API
/// requires alternation), stop accepts string or array.
fn chat_params(
    request: &ChatRequest,
    model: String,
) -> Result<ChatParams, (StatusCode, Json<Value>)> {
    let mut system_parts: Vec<String> = Vec::new();
    let mut turns: Vec<(String, String)> = Vec::new();
    for message in &request.messages {
        // "developer" is OpenAI's system-equivalent role.
        if message.role == "system" || message.role == "developer" {
            system_parts.push(message.content.clone());
            continue;
        }
        let role = if message.role == "assistant" {
            "assistant"
        } else {
            "user"
        };
        match turns.last_mut() {
            Some((last_role, content)) if last_role == role => {
                content.push_str("\n\n");
                content.push_str(&message.content);
            }
            _ => turns.push((role.to_string(), message.content.clone())),
        }
    }
    if turns.is_empty() {
        return Err(bad_request(
            "messages must contain at least one user or assistant turn".to_string(),
        ));
    }
    let stop = match &request.stop {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        Some(_) => {
            return Err(bad_request(
                "stop must be a string or an array of strings".to_string(),
            ));
        }
    };
    Ok(ChatParams {
        model,
        system: (!system_parts.is_empty()).then(|| system_parts.join("\n\n")),
        messages: turns,
        max_tokens: request.max_tokens.unwrap_or(4096),
        temperature: request.temperature,
        top_p: request.top_p,
        stop,
    })
}

/// Opaque unique id: hash of time, pid, and a per-process counter.
fn fresh_id(prefix: &str) -> String {
    use sha2::Digest;
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let mut hasher = sha2::Sha256::new();
    hasher.update(now.to_le_bytes());
    hasher.update(std::process::id().to_le_bytes());
    hasher.update(
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .to_le_bytes(),
    );
    let digest = hasher.finalize();
    digest[..8]
        .iter()
        .fold(format!("{prefix}-"), |mut out, byte| {
            use std::fmt::Write;
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Assemble the OpenAI-shaped response; the reports ride on an extra key
/// and are simply absent for pure-proxy requests.
fn openai_response(
    model: &str,
    completion: &inputlayer_gateway::model::ChatCompletion,
    inputlayer: Option<Value>,
) -> Value {
    let created = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let id = fresh_id("chatcmpl");
    let mut response = json!({
        "id": id,
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": completion.text },
            "finish_reason": completion.finish_reason,
        }],
        "usage": {
            "prompt_tokens": completion.prompt_tokens,
            "completion_tokens": completion.completion_tokens,
            "total_tokens": completion.prompt_tokens + completion.completion_tokens,
        },
    });
    if let Some(inputlayer) = inputlayer {
        response["inputlayer"] = inputlayer;
    }
    response
}

/// Map a provider error to a status that tells the truth about WHOSE
/// problem it is: a rejected request shape (400/404/413/422) is the
/// caller's, but the provider refusing our credentials (401/403) or rate
/// limiting us (429) is the operator's - reporting those as 400 would send
/// a developer hunting through their own request while the deployment's
/// model key is what is wrong.
fn upstream_error(err: &anyhow::Error) -> (StatusCode, Json<Value>) {
    let upstream = err
        .downcast_ref::<inputlayer_gateway::model::UpstreamStatus>()
        .map(|s| s.0);
    let (status, error_type) = match upstream {
        Some(401 | 403) => (StatusCode::BAD_GATEWAY, "model_credentials"),
        Some(429) => (StatusCode::SERVICE_UNAVAILABLE, "model_rate_limited"),
        Some(code) if (400..500).contains(&code) => (StatusCode::BAD_REQUEST, "invalid_request"),
        _ => (StatusCode::BAD_GATEWAY, "upstream_error"),
    };
    (
        status,
        Json(json!({ "error": { "type": error_type, "message": err.to_string() } })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(role: &str, content: &str) -> ChatMessage {
        ChatMessage {
            role: role.to_string(),
            content: content.to_string(),
        }
    }

    fn request(messages: Vec<ChatMessage>) -> ChatRequest {
        ChatRequest {
            model: None,
            messages,
            max_tokens: None,
            temperature: None,
            top_p: None,
            stop: None,
            stream: None,
            il_ontology: None,
            il_mode: None,
            il_conversation: None,
        }
    }

    #[test]
    fn model_routing() {
        assert_eq!(route_model(Some("claude-haiku-4-5")), "claude-haiku-4-5");
        assert_eq!(route_model(Some("gpt-4o")), DEFAULT_CHAT_MODEL);
        assert_eq!(route_model(None), DEFAULT_CHAT_MODEL);
    }

    #[test]
    fn system_turns_join_and_same_roles_merge() {
        let req = request(vec![
            message("system", "Be brief."),
            message("system", "Answer in French."),
            message("user", "Hello"),
            message("user", "are you there?"),
            message("assistant", "Oui."),
            message("tool", "unknown role becomes user"),
        ]);
        let params = chat_params(&req, "claude-sonnet-5".to_string()).expect("params");
        assert_eq!(
            params.system.as_deref(),
            Some("Be brief.\n\nAnswer in French.")
        );
        assert_eq!(
            params.messages,
            vec![
                ("user".to_string(), "Hello\n\nare you there?".to_string()),
                ("assistant".to_string(), "Oui.".to_string()),
                ("user".to_string(), "unknown role becomes user".to_string()),
            ]
        );
        assert_eq!(params.max_tokens, 4096);
    }

    #[test]
    fn stop_accepts_string_or_array_and_rejects_other() {
        let mut req = request(vec![message("user", "hi")]);
        req.stop = Some(serde_json::json!("END"));
        assert_eq!(
            chat_params(&req, "m".to_string()).expect("params").stop,
            vec!["END"]
        );
        req.stop = Some(serde_json::json!(["a", "b"]));
        assert_eq!(
            chat_params(&req, "m".to_string()).expect("params").stop,
            vec!["a", "b"]
        );
        req.stop = Some(serde_json::json!(42));
        assert!(chat_params(&req, "m".to_string()).is_err());
    }

    #[test]
    fn system_only_conversation_is_rejected() {
        let req = request(vec![message("system", "only instructions")]);
        assert!(chat_params(&req, "m".to_string()).is_err());
    }

    #[test]
    fn developer_role_is_system() {
        let req = request(vec![
            message("developer", "Follow the spec."),
            message("user", "hi"),
        ]);
        let params = chat_params(&req, "m".to_string()).expect("params");
        assert_eq!(params.system.as_deref(), Some("Follow the spec."));
        assert_eq!(params.messages.len(), 1);
    }

    fn report(status: &'static str, blocking: bool) -> Report {
        Report {
            kg: "kg".to_string(),
            ontology: "o@1".to_string(),
            digest: "sha256:x".to_string(),
            status,
            reason: (status != "complete").then(|| "why".to_string()),
            findings: if blocking {
                vec![serde_json::json!({"blocking": true})]
            } else {
                vec![serde_json::json!({"blocking": false})]
            },
            dropped: Vec::new(),
            notes: Vec::new(),
            tuples: Vec::new(),
            trace: None,
        }
    }

    #[test]
    fn enforcement_decision_table() {
        // Blocking finding refuses with 422 even when another pair is clean.
        let refusal = enforcement_refusal(&[report("complete", false), report("complete", true)]);
        assert_eq!(
            refusal.map(|(code, _)| code),
            Some(StatusCode::UNPROCESSABLE_ENTITY)
        );
        // Any incomplete pair refuses with 503 (fail closed).
        let refusal =
            enforcement_refusal(&[report("complete", false), report("incomplete", false)]);
        assert_eq!(
            refusal.map(|(code, _)| code),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
        // All complete, only non-blocking findings: proceed.
        assert!(enforcement_refusal(&[report("complete", false)]).is_none());
        // Dropped extraction rows refuse under enforce (partial evaluation).
        let mut with_drops = report("complete", false);
        with_drops.dropped = vec!["claims: quote not verbatim".to_string()];
        assert_eq!(
            enforcement_refusal(&[with_drops]).map(|(code, _)| code),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
    }

    #[test]
    fn ids_are_unique() {
        assert_ne!(fresh_id("chatcmpl"), fresh_id("chatcmpl"));
    }
}
