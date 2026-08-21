//! InputLayer Gateway service.
//!
//! The model gateway of the stack. `POST /v1/verify` (M0, #83) extracts a
//! conversation into typed claims (bound to a registry ontology - never
//! open-domain), inserts them into an ephemeral knowledge graph, and
//! returns the ontology's findings. `POST /v1/chat/completions` (M1, #84)
//! is the OpenAI-compatible proxy: completions come from the model
//! provider and every response carries the same consistency block -
//! `annotate` mode attaches findings, `enforce` mode refuses (422) to
//! complete over a contradictory conversation.
//!
//! Configuration (env):
//!   GATEWAY_HOST / GATEWAY_PORT   bind address (defaults 127.0.0.1:8081)
//!   GATEWAY_API_KEY               when set, /v1/* require
//!                                 "Authorization: Bearer <key>"; unset
//!                                 leaves them open (local dev) with a
//!                                 startup warning
//!   INPUTLAYER_REGISTRY / INPUTLAYER_REGISTRY_TOKEN  registry index URL
//!                                 and access token (private registries)
//!   INPUTLAYER_URL / INPUTLAYER_API_KEY  engine access
//!   ANTHROPIC_API_KEY             model provider key (never seen by the
//!                                 engine); without it /v1/* return 503
//!
//! The gateway serves every ontology published in the registry index: all
//! entries are resolved at startup and pinned by version and digest for the
//! process lifetime, so a registry update never changes a running gateway.
//! Every request MUST select an ontology (x-il-ontology header or
//! il_ontology body field) - extraction is always bound to one, so without
//! a selection there is nothing to translate against and the request is
//! rejected before any model call. An x-il-trace: 1 header additionally
//! returns the validated extraction, the mapped IQL statements, and
//! timings inside the consistency block.

use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use inputlayer_gateway::model::{
    render_conversation, AnthropicClient, ChatParams, Completer, Extractor,
};
use inputlayer_gateway::ontology::LoadedOntology;
use inputlayer_gateway::pipeline::{run_verify, EngineConfig};
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
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// Load every ontology published in the registry index, pinning each at its
/// latest version and digest for the process lifetime.
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
    // (health/ready keep working) and /v1/verify answers 503 until restart.
    let ontologies = load_ontologies().await.unwrap_or_else(|err| {
        println!("warning: could not load registry: {err:#}");
        HashMap::new()
    });
    if ontologies.is_empty() {
        println!("no ontologies loaded - /v1/verify will 503");
    }
    let model_client: Option<Arc<AnthropicClient>> = match std::env::var("ANTHROPIC_API_KEY") {
        Ok(key) if !key.trim().is_empty() => Some(Arc::new(AnthropicClient::new(key))),
        _ => {
            println!("ANTHROPIC_API_KEY not set - /v1/verify and /v1/chat/completions will 503");
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
        .route("/v1/verify", post(verify))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/ontologies", get(list_ontologies))
        .route("/v1/ontologies/install", post(install_ontology))
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

#[derive(Deserialize)]
struct VerifyRequest {
    messages: Vec<VerifyMessage>,
    #[serde(default)]
    il_ontology: Option<String>,
}

#[derive(Deserialize)]
struct VerifyMessage {
    role: String,
    content: String,
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
/// key diverges.
fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), (StatusCode, Json<Value>)> {
    let Some(expected) = &state.api_key else {
        return Ok(());
    };
    let presented = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let (scheme, token) = v.split_once(' ')?;
            scheme
                .eq_ignore_ascii_case("bearer")
                .then(|| token.trim_start())
        });
    let matches = presented.is_some_and(|token| {
        use sha2::Digest;
        sha2::Sha256::digest(token.as_bytes()) == sha2::Sha256::digest(expected.as_bytes())
    });
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

/// Both /v1/* endpoints need the model key and at least one ontology.
fn require_configured(state: &AppState) -> Result<(), (StatusCode, Json<Value>)> {
    if state.extractor.is_none() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": { "type": "not_configured",
                "message": "model key not configured (ANTHROPIC_API_KEY)" } })),
        ));
    }
    if state.ontologies.is_empty() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": { "type": "not_configured",
                "message": "no ontologies loaded (registry unreachable at startup)" } })),
        ));
    }
    Ok(())
}

/// Tracing is per-request opt-in: the response additionally carries the
/// validated extraction, the mapped IQL statements, and timings - only data
/// derived from the caller's own request. Like x-il-ontology, a malformed
/// value is explicit intent that failed: reject it rather than silently
/// not tracing.
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

/// Run extraction + verification for a conversation and render the
/// consistency block. Fail-open: any verifier trouble yields an
/// "unverified" block with a reason, never an error to the caller.
async fn consistency_block(
    state: &AppState,
    ontology: &LoadedOntology,
    messages: &[(String, String)],
    want_trace: bool,
) -> Value {
    let Some(extractor) = &state.extractor else {
        // Unreachable behind require_configured; keep the honest fallback.
        return unverified_block(ontology, "model key not configured");
    };
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let system_prompt = ontology.render_prompt(&today);
    let user_content = render_conversation(messages);

    let extract_started = std::time::Instant::now();
    let extraction = match extractor
        .extract(
            &ontology.extraction_model,
            &system_prompt,
            &user_content,
            &ontology.schema,
        )
        .await
    {
        Ok(value) => value,
        Err(err) => return unverified_block(ontology, &err.to_string()),
    };
    let extract_ms = extract_started.elapsed().as_millis();

    match run_verify(&state.engine, ontology, extraction, messages, want_trace).await {
        Ok(outcome) => {
            let mut consistency = json!({
                "status": outcome.status,
                "ontology": format!("{}@{}", ontology.name, ontology.version),
                "digest": ontology.digest,
                "findings": outcome.findings,
                "dropped": outcome.dropped,
            });
            if let Some(mut trace) = outcome.trace {
                if let Some(t) = trace.as_object_mut() {
                    t.insert("model".to_string(), json!(ontology.extraction_model));
                    t.insert("extract_ms".to_string(), json!(extract_ms));
                }
                consistency["trace"] = trace;
            }
            consistency
        }
        Err(err) => unverified_block(ontology, &err.to_string()),
    }
}

async fn verify(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<VerifyRequest>,
) -> (StatusCode, Json<Value>) {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    if let Err(response) = require_configured(&state) {
        return response;
    }
    let ontology = match select_ontology(&state, &headers, request.il_ontology.as_deref()) {
        Ok(ontology) => Arc::clone(ontology),
        Err(response) => return response,
    };
    let want_trace = match parse_trace_header(&headers) {
        Ok(flag) => flag,
        Err(response) => return response,
    };
    if request.messages.is_empty() {
        return bad_request("messages must not be empty".to_string());
    }
    let messages: Vec<(String, String)> = request
        .messages
        .iter()
        .map(|m| (m.role.clone(), m.content.clone()))
        .collect();

    let consistency = consistency_block(&state, &ontology, &messages, want_trace).await;
    (
        StatusCode::OK,
        Json(json!({ "inputlayer": { "consistency": consistency } })),
    )
}

/// Resolve the request's REQUIRED ontology selection: extraction is always
/// bound to an ontology, so without one there is nothing to translate
/// against. The x-il-ontology header wins over the body field; a
/// name@version assertion must match the pinned version. A malformed header
/// is an explicit client intent that failed - reject it rather than
/// silently falling through.
fn select_ontology<'a>(
    state: &'a AppState,
    headers: &HeaderMap,
    body_field: Option<&str>,
) -> Result<&'a Arc<LoadedOntology>, (StatusCode, Json<Value>)> {
    let header_selection = match headers.get("x-il-ontology") {
        Some(value) => match value.to_str() {
            Ok(v) => Some(v.trim().to_string()),
            Err(_) => {
                return Err(bad_request(
                    "x-il-ontology header is not valid ASCII".to_string(),
                ));
            }
        },
        None => None,
    };
    let selected = header_selection.filter(|v| !v.is_empty()).or_else(|| {
        body_field
            .map(str::trim)
            .map(str::to_string)
            .filter(|v| !v.is_empty())
    });
    let Some(spec) = selected else {
        return Err(bad_request(format!(
            "an ontology must be selected (x-il-ontology header or il_ontology field); \
             available: {}",
            available_list(state)
        )));
    };
    let (name, version) = match spec.split_once('@') {
        Some((n, v)) => (n, Some(v)),
        None => (spec.as_str(), None),
    };
    let Some(ontology) = state.ontologies.get(name) else {
        return Err(bad_request(format!(
            "ontology {name:?} is not published; available: {}",
            available_list(state)
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
    Ok(ontology)
}

fn available_list(state: &AppState) -> String {
    let mut names: Vec<&str> = state.ontologies.keys().map(String::as_str).collect();
    names.sort_unstable();
    names.join(", ")
}

fn unverified_block(ontology: &LoadedOntology, reason: &str) -> Value {
    json!({
        "status": "unverified",
        "ontology": format!("{}@{}", ontology.name, ontology.version),
        "reason": reason,
    })
}

/// OpenAI chat completion request shape; unknown fields are ignored.
#[derive(Deserialize)]
struct ChatRequest {
    #[serde(default)]
    model: Option<String>,
    messages: Vec<VerifyMessage>,
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
    il_ontology: Option<String>,
    #[serde(default)]
    il_mode: Option<String>,
}

async fn chat_completions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<ChatRequest>,
) -> (StatusCode, Json<Value>) {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    if let Err(response) = require_configured(&state) {
        return response;
    }
    let Some(completer) = state.completer.clone() else {
        // Same configuration as the extractor; unreachable behind
        // require_configured.
        return bad_request("model key not configured".to_string());
    };
    if request.stream == Some(true) {
        return bad_request(
            "streaming is not supported yet (M2, \
             https://github.com/inputlayer/inputlayer/issues/85); send stream: false"
                .to_string(),
        );
    }
    let ontology = match select_ontology(&state, &headers, request.il_ontology.as_deref()) {
        Ok(ontology) => Arc::clone(ontology),
        Err(response) => return response,
    };
    let want_trace = match parse_trace_header(&headers) {
        Ok(flag) => flag,
        Err(response) => return response,
    };
    // Mode: header wins over body field, default annotate.
    let mode = headers
        .get("x-il-mode")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| request.il_mode.clone())
        .unwrap_or_else(|| "annotate".to_string());
    if mode != "annotate" && mode != "enforce" && mode != "enforce-strict" {
        return bad_request(format!(
            "il_mode must be \"annotate\", \"enforce\", or \"enforce-strict\", got {mode:?}"
        ));
    }
    if request.messages.is_empty() {
        return bad_request("messages must not be empty".to_string());
    }

    // Verification sees the conversation as-is, including system turns.
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
    if mode == "annotate" {
        // annotate: completion and verification run concurrently -
        // verification checks the incoming conversation, not the reply.
        let (completion, consistency) = tokio::join!(
            completer.complete(&params),
            consistency_block(&state, &ontology, &messages, want_trace)
        );
        return match completion {
            Ok(completion) => (
                StatusCode::OK,
                Json(openai_response(&response_model, &completion, consistency)),
            ),
            Err(err) => upstream_error(&err),
        };
    }

    // enforce / enforce-strict: verify FIRST, so a refused conversation
    // never spends completion tokens.
    let consistency = consistency_block(&state, &ontology, &messages, want_trace).await;
    if let Some(refusal) = enforcement_refusal(&mode, consistency["status"].as_str()) {
        let (status_code, error_type, message) = refusal;
        return (
            status_code,
            Json(json!({
                "error": { "type": error_type, "message": message },
                "inputlayer": { "consistency": consistency },
            })),
        );
    }
    match completer.complete(&params).await {
        Ok(completion) => (
            StatusCode::OK,
            Json(openai_response(&response_model, &completion, consistency)),
        ),
        Err(err) => upstream_error(&err),
    }
}

/// The enforcement decision, pure for testability. `enforce` fails OPEN on
/// an unverifiable conversation (verifier trouble must not take down caller
/// traffic - and note this is bypassable by content engineered to break
/// extraction, which is why the strict variant exists). `enforce-strict`
/// fails CLOSED: no completion unless verification actually ran clean.
fn enforcement_refusal(
    mode: &str,
    status: Option<&str>,
) -> Option<(StatusCode, &'static str, &'static str)> {
    match (mode, status) {
        (_, Some("conflicts_found")) => Some((
            StatusCode::UNPROCESSABLE_ENTITY,
            "consistency_violation",
            "conversation contains contradictions; completion refused",
        )),
        ("enforce-strict", status) if status != Some("verified") => Some((
            StatusCode::SERVICE_UNAVAILABLE,
            "verification_unavailable",
            "conversation could not be verified; completion refused (enforce-strict)",
        )),
        _ => None,
    }
}

/// The published ontology set: what a Studio (or any client) can select
/// from, one entry per loaded pack with its pinned identity.
async fn list_ontologies(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    if let Err(response) = authorize(&state, &headers) {
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

#[derive(Deserialize)]
struct InstallRequest {
    ontology: String,
    kg: String,
    #[serde(default)]
    create: bool,
}

/// Install a loaded ontology pack into a persistent knowledge graph -
/// the same deploy-and-pin routine `il install` uses, exposed for the
/// Studio. The pack comes from the gateway's own pinned set, never from an
/// arbitrary caller-supplied source.
async fn install_ontology(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<InstallRequest>,
) -> (StatusCode, Json<Value>) {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    if state.ontologies.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": { "type": "not_configured",
                "message": "no ontologies loaded (registry unreachable at startup)" } })),
        );
    }
    let (name, version) = match request.ontology.split_once('@') {
        Some((n, v)) => (n, Some(v)),
        None => (request.ontology.as_str(), None),
    };
    let Some(ontology) = state.ontologies.get(name) else {
        return bad_request(format!(
            "ontology {name:?} is not published; available: {}",
            available_list(&state)
        ));
    };
    if let Some(asserted) = version {
        if asserted != ontology.version {
            return bad_request(format!(
                "ontology {name} is pinned at {}, request asserted {asserted}",
                ontology.version
            ));
        }
    }
    if let Err(err) =
        inputlayer_ontology_client::registry::validate_component("knowledge graph", &request.kg)
    {
        return bad_request(err.to_string());
    }

    let Ok(mut engine) =
        inputlayer_ontology_client::ws::Engine::connect(&state.engine.url, &state.engine.api_key)
            .await
    else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": { "type": "engine_unavailable",
                    "message": "engine unreachable" } })),
        );
    };
    match inputlayer_ontology_client::deploy::deploy_pack(
        &mut engine,
        &request.kg,
        request.create,
        &ontology.name,
        &ontology.version,
        &ontology.digest,
        &ontology.rules_program,
    )
    .await
    {
        Ok(statements) => (
            StatusCode::OK,
            Json(json!({
                "installed": {
                    "ontology": format!("{}@{}", ontology.name, ontology.version),
                    "digest": ontology.digest,
                    "kg": request.kg,
                    "statements": statements,
                }
            })),
        ),
        Err(err) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({ "error": { "type": "install_failed", "message": err.to_string() } })),
        ),
    }
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

/// Assemble the OpenAI-shaped response with the consistency block attached.
fn openai_response(
    model: &str,
    completion: &inputlayer_gateway::model::ChatCompletion,
    consistency: Value,
) -> Value {
    use sha2::Digest;
    // Per-process counter so two identical completions in the same second
    // still get distinct ids.
    static ID_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let created = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    // Opaque id; uniqueness matters, readability of inputs does not.
    let mut hasher = sha2::Sha256::new();
    hasher.update(created.to_le_bytes());
    hasher.update(completion.text.as_bytes());
    hasher.update(std::process::id().to_le_bytes());
    hasher.update(
        ID_SEQ
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .to_le_bytes(),
    );
    let digest = hasher.finalize();
    let id: String = digest[..12]
        .iter()
        .fold(String::from("chatcmpl-"), |mut out, byte| {
            use std::fmt::Write;
            let _ = write!(out, "{byte:02x}");
            out
        });
    json!({
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
        "inputlayer": { "consistency": consistency },
    })
}

/// A provider 4xx (except 429) is the caller's own mistake (bad params the
/// gateway does not pre-validate) and surfaces as 400; everything else is
/// provider trouble and surfaces as 502.
fn upstream_error(err: &anyhow::Error) -> (StatusCode, Json<Value>) {
    let upstream = err
        .downcast_ref::<inputlayer_gateway::model::UpstreamStatus>()
        .map(|s| s.0);
    let (status, error_type) = match upstream {
        Some(code) if (400..500).contains(&code) && code != 429 => {
            (StatusCode::BAD_REQUEST, "invalid_request")
        }
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

    fn message(role: &str, content: &str) -> VerifyMessage {
        VerifyMessage {
            role: role.to_string(),
            content: content.to_string(),
        }
    }

    fn request(messages: Vec<VerifyMessage>) -> ChatRequest {
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

    #[test]
    fn enforcement_decision_table() {
        // Conflicts refuse in both enforcement modes.
        assert!(enforcement_refusal("enforce", Some("conflicts_found")).is_some());
        assert!(enforcement_refusal("enforce-strict", Some("conflicts_found")).is_some());
        // enforce fails OPEN on unverified; strict fails CLOSED.
        assert!(enforcement_refusal("enforce", Some("unverified")).is_none());
        let strict = enforcement_refusal("enforce-strict", Some("unverified"));
        assert_eq!(
            strict.map(|(code, kind, _)| (code, kind)),
            Some((StatusCode::SERVICE_UNAVAILABLE, "verification_unavailable"))
        );
        // Verified proceeds everywhere.
        assert!(enforcement_refusal("enforce", Some("verified")).is_none());
        assert!(enforcement_refusal("enforce-strict", Some("verified")).is_none());
    }
}
