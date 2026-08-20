//! InputLayer Gateway service.
//!
//! The model gateway of the stack. This binary serves M0 of Verified
//! Completions (#83): `POST /v1/verify` extracts a conversation into typed
//! claims (bound to a registry ontology - never open-domain), inserts them
//! into an ephemeral knowledge graph, and returns the ontology's findings.
//! The `/v1/chat/completions` proxy is M1 (#84) and still answers 501.
//!
//! Configuration (env):
//!   GATEWAY_HOST / GATEWAY_PORT   bind address (defaults 127.0.0.1:8081)
//!   INPUTLAYER_REGISTRY / INPUTLAYER_REGISTRY_TOKEN  registry index URL
//!                                 and access token (private registries)
//!   INPUTLAYER_URL / INPUTLAYER_API_KEY  engine access
//!   ANTHROPIC_API_KEY             extraction model key (never seen by the
//!                                 engine); without it /v1/verify returns 503
//!
//! The gateway serves every ontology published in the registry index: all
//! entries are resolved at startup and pinned by version and digest for the
//! process lifetime, so a registry update never changes a running gateway.
//! Every request MUST select an ontology (x-il-ontology header or
//! il_ontology body field) - extraction is always bound to one, so without
//! a selection there is nothing to translate against and the request is
//! rejected before any model call.

use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use inputlayer_gateway::extract::{render_conversation, AnthropicExtractor, Extractor};
use inputlayer_gateway::ontology::LoadedOntology;
use inputlayer_gateway::pipeline::{run_verify, EngineConfig};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

struct AppState {
    http: reqwest::Client,
    engine_url: String,
    engine: EngineConfig,
    ontologies: HashMap<String, Arc<LoadedOntology>>,
    extractor: Option<Arc<dyn Extractor>>,
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
    let extractor: Option<Arc<dyn Extractor>> = match std::env::var("ANTHROPIC_API_KEY") {
        Ok(key) if !key.is_empty() => Some(Arc::new(AnthropicExtractor::new(key))),
        _ => {
            println!("ANTHROPIC_API_KEY not set - /v1/verify will 503");
            None
        }
    };

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
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/v1/verify", post(verify))
        .route("/v1/chat/completions", post(not_implemented))
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

async fn verify(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<VerifyRequest>,
) -> (StatusCode, Json<Value>) {
    let Some(extractor) = &state.extractor else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": { "type": "not_configured",
                "message": "extraction model key not configured (ANTHROPIC_API_KEY)" } })),
        );
    };
    if state.ontologies.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": { "type": "not_configured",
                "message": "no ontologies loaded (registry unreachable at startup)" } })),
        );
    }

    // Ontology selection is REQUIRED: extraction is always bound to an
    // ontology, so without one there is nothing to translate against. The
    // header wins over the body field; a version assertion must match the
    // pinned version. A malformed header is an explicit client intent that
    // failed - reject it rather than silently falling through.
    let header_selection = match headers.get("x-il-ontology") {
        Some(value) => match value.to_str() {
            Ok(v) => Some(v.trim().to_string()),
            Err(_) => {
                return bad_request("x-il-ontology header is not valid ASCII".to_string());
            }
        },
        None => None,
    };
    let selected = header_selection.filter(|v| !v.is_empty()).or_else(|| {
        request
            .il_ontology
            .as_deref()
            .map(str::trim)
            .map(str::to_string)
            .filter(|v| !v.is_empty())
    });
    let Some(spec) = selected else {
        return bad_request(format!(
            "an ontology must be selected (x-il-ontology header or il_ontology field); \
             available: {}",
            available_list(&state)
        ));
    };
    let (name, version) = match spec.split_once('@') {
        Some((n, v)) => (n, Some(v)),
        None => (spec.as_str(), None),
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
    if request.messages.is_empty() {
        return bad_request("messages must not be empty".to_string());
    }

    let messages: Vec<(String, String)> = request
        .messages
        .iter()
        .map(|m| (m.role.clone(), m.content.clone()))
        .collect();
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let system_prompt = ontology.render_prompt(&today);
    let user_content = render_conversation(&messages);

    // Fail open from here: verifier trouble must not fail caller traffic.
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
        Err(err) => return (StatusCode::OK, Json(unverified(ontology, &err.to_string()))),
    };

    match run_verify(&state.engine, ontology, extraction, &messages).await {
        Ok(outcome) => (
            StatusCode::OK,
            Json(json!({ "inputlayer": { "consistency": {
                "status": outcome.status,
                "ontology": format!("{}@{}", ontology.name, ontology.version),
                "digest": ontology.digest,
                "findings": outcome.findings,
                "dropped": outcome.dropped,
            }}})),
        ),
        Err(err) => (StatusCode::OK, Json(unverified(ontology, &err.to_string()))),
    }
}

fn available_list(state: &AppState) -> String {
    let mut names: Vec<&str> = state.ontologies.keys().map(String::as_str).collect();
    names.sort_unstable();
    names.join(", ")
}

fn unverified(ontology: &LoadedOntology, reason: &str) -> Value {
    json!({ "inputlayer": { "consistency": {
        "status": "unverified",
        "ontology": format!("{}@{}", ontology.name, ontology.version),
        "reason": reason,
    }}})
}

async fn not_implemented() -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": {
                "type": "not_implemented",
                "message": "POST /v1/chat/completions (annotate/enforce/repair proxy) is M1, tracked in https://github.com/inputlayer/inputlayer/issues/84. POST /v1/verify is live."
            }
        })),
    )
}
