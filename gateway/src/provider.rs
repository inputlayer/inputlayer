//! Model provider selection: one validated configuration, read once at
//! startup, that picks which client serves the [`Extractor`] and
//! [`Completer`] traits and how request model ids route to it.
//!
//! Environment:
//!   GATEWAY_MODEL_PROVIDER    `anthropic` | `openai-compatible`; unset means
//!                             `anthropic` when ANTHROPIC_API_KEY is set,
//!                             otherwise no provider (model routes 503)
//!   ANTHROPIC_API_KEY         anthropic: required
//!   ANTHROPIC_BASE_URL        anthropic: API root without `/v1`
//!                             (default https://api.anthropic.com)
//!   OPENAI_BASE_URL           openai-compatible: required, API root with
//!                             its version segment, e.g.
//!                             http://127.0.0.1:11434/v1 for Ollama
//!   OPENAI_API_KEY            openai-compatible: optional bearer token
//!   GATEWAY_CHAT_MODEL        completion model for requests that name no
//!                             routable model; anthropic default
//!                             `claude-sonnet-5`, openai-compatible required
//!   GATEWAY_EXTRACTION_MODEL  replaces every pack's `[extraction].model`;
//!                             anthropic optional, openai-compatible
//!                             required (pack models are Claude ids)
//!
//! An explicitly selected provider that is incomplete or malformed is a
//! startup error, never a gateway that boots and then fails every request.
//! Selection is resolved here once; the request path only does a string
//! compare to route the model and a trait-object call to reach the client.

use crate::model::{AnthropicClient, Completer, Extractor};
use crate::openai_compatible::OpenAiCompatibleClient;
use anyhow::{anyhow, bail, Result};
use std::sync::Arc;

/// The completion model an anthropic gateway falls back to when the
/// request names no `claude-*` model (#84).
pub const DEFAULT_ANTHROPIC_CHAT_MODEL: &str = "claude-sonnet-5";
const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com";
const MAX_MODEL_ID_LEN: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKind {
    Anthropic,
    OpenAiCompatible,
}

impl ProviderKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
            Self::OpenAiCompatible => "openai-compatible",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "anthropic" => Ok(Self::Anthropic),
            "openai-compatible" => Ok(Self::OpenAiCompatible),
            other => bail!(
                "GATEWAY_MODEL_PROVIDER must be \"anthropic\" or \"openai-compatible\", got {other:?}"
            ),
        }
    }
}

/// How request and pack model ids map to the model actually called.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRouting {
    kind: ProviderKind,
    chat_model: String,
    extraction_model: Option<String>,
}

impl ModelRouting {
    /// The completion model for a request. Anthropic forwards `claude-*`
    /// ids as-is and routes anything else (an OpenAI client's `gpt-4o`) to
    /// the default; an OpenAI-compatible server is asked for exactly the
    /// model the client named.
    pub fn chat_model(&self, requested: Option<&str>) -> String {
        let requested = requested.map(str::trim).filter(|m| !m.is_empty());
        match (self.kind, requested) {
            (ProviderKind::Anthropic, Some(model)) if model.starts_with("claude-") => {
                model.to_string()
            }
            (ProviderKind::OpenAiCompatible, Some(model)) => model.to_string(),
            _ => self.chat_model.clone(),
        }
    }

    /// The extraction model for a pack: the configured override, else the
    /// model the pack declares.
    pub fn extraction_model<'a>(&'a self, pack_model: &'a str) -> &'a str {
        self.extraction_model.as_deref().unwrap_or(pack_model)
    }
}

/// The validated provider selection. `Debug` redacts the credential.
#[derive(Clone, PartialEq, Eq)]
pub struct ProviderConfig {
    routing: ModelRouting,
    api_key: Option<String>,
    base_url: String,
}

impl std::fmt::Debug for ProviderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderConfig")
            .field("routing", &self.routing)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl ProviderConfig {
    /// Read the selection from the process environment.
    pub fn from_env() -> Result<Option<Self>> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// Resolve and validate the selection. `Ok(None)` is the one
    /// unconfigured state: no provider named and no Anthropic key.
    /// Blank values count as unset (compose passes unset host vars as "").
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Option<Self>> {
        let get = |name: &str| {
            lookup(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let kind = match get("GATEWAY_MODEL_PROVIDER") {
            Some(name) => ProviderKind::parse(&name)?,
            None if get("ANTHROPIC_API_KEY").is_some() => ProviderKind::Anthropic,
            None => return Ok(None),
        };
        let extraction_model = get("GATEWAY_EXTRACTION_MODEL")
            .map(|m| model_id("GATEWAY_EXTRACTION_MODEL", m))
            .transpose()?;
        let chat_model = get("GATEWAY_CHAT_MODEL")
            .map(|m| model_id("GATEWAY_CHAT_MODEL", m))
            .transpose()?;
        let config = match kind {
            ProviderKind::Anthropic => Self {
                api_key: Some(get("ANTHROPIC_API_KEY").ok_or_else(|| {
                    anyhow!("GATEWAY_MODEL_PROVIDER=anthropic requires ANTHROPIC_API_KEY")
                })?),
                base_url: base_url(
                    "ANTHROPIC_BASE_URL",
                    &get("ANTHROPIC_BASE_URL")
                        .unwrap_or_else(|| DEFAULT_ANTHROPIC_BASE_URL.to_string()),
                )?,
                routing: ModelRouting {
                    kind,
                    chat_model: chat_model
                        .unwrap_or_else(|| DEFAULT_ANTHROPIC_CHAT_MODEL.to_string()),
                    extraction_model,
                },
            },
            ProviderKind::OpenAiCompatible => {
                let required = |name: &str| {
                    anyhow!("GATEWAY_MODEL_PROVIDER=openai-compatible requires {name}")
                };
                Self {
                    api_key: get("OPENAI_API_KEY"),
                    base_url: base_url(
                        "OPENAI_BASE_URL",
                        &get("OPENAI_BASE_URL").ok_or_else(|| required("OPENAI_BASE_URL"))?,
                    )?,
                    routing: ModelRouting {
                        kind,
                        chat_model: chat_model.ok_or_else(|| required("GATEWAY_CHAT_MODEL"))?,
                        extraction_model: Some(
                            extraction_model.ok_or_else(|| required("GATEWAY_EXTRACTION_MODEL"))?,
                        ),
                    },
                }
            }
        };
        Ok(Some(config))
    }

    pub fn kind(&self) -> ProviderKind {
        self.routing.kind
    }

    /// Construct the selected client once; both capabilities share it.
    pub fn build(self) -> ModelProvider {
        let (extractor, completer): (Arc<dyn Extractor>, Arc<dyn Completer>) =
            match self.routing.kind {
                ProviderKind::Anthropic => {
                    let client = Arc::new(AnthropicClient::new(
                        self.api_key.unwrap_or_default(),
                        self.base_url.clone(),
                    ));
                    (client.clone(), client)
                }
                ProviderKind::OpenAiCompatible => {
                    let client = Arc::new(OpenAiCompatibleClient::new(
                        self.api_key,
                        self.base_url.clone(),
                    ));
                    (client.clone(), client)
                }
            };
        ModelProvider {
            extractor,
            completer,
            routing: self.routing,
            base_url: self.base_url,
        }
    }
}

/// The selected provider as the request path uses it.
pub struct ModelProvider {
    pub extractor: Arc<dyn Extractor>,
    pub completer: Arc<dyn Completer>,
    pub routing: ModelRouting,
    base_url: String,
}

impl ModelProvider {
    pub fn name(&self) -> &'static str {
        self.routing.kind.name()
    }

    /// One startup line naming what was selected (never the credential).
    pub fn describe(&self) -> String {
        let extraction = self
            .routing
            .extraction_model
            .as_deref()
            .unwrap_or("per ontology pack");
        format!(
            "model provider: {} at {} (chat default {}, extraction {extraction})",
            self.name(),
            self.base_url,
            self.routing.chat_model
        )
    }
}

/// A model id is passed verbatim to the provider: reject what no provider
/// could mean rather than discovering it on the first request.
fn model_id(name: &str, value: String) -> Result<String> {
    if value.len() > MAX_MODEL_ID_LEN || value.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        bail!("{name} must be a model id without whitespace (at most {MAX_MODEL_ID_LEN} bytes), got {value:?}");
    }
    Ok(value)
}

/// An http(s) URL with a host and nothing after the path; returned without
/// a trailing slash so endpoint paths join cleanly.
fn base_url(name: &str, value: &str) -> Result<String> {
    let url = reqwest::Url::parse(value)
        .map_err(|err| anyhow!("{name} is not a valid URL ({err}): {value:?}"))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        bail!("{name} must be an http(s) URL with a host, got {value:?}");
    }
    if url.query().is_some() || url.fragment().is_some() || !url.username().is_empty() {
        bail!("{name} must not carry credentials, a query or a fragment, got {value:?}");
    }
    Ok(value.trim_end_matches('/').to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn config(vars: &[(&str, &str)]) -> Result<Option<ProviderConfig>> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        ProviderConfig::from_lookup(|name| vars.get(name).cloned())
    }

    fn error(vars: &[(&str, &str)]) -> String {
        format!("{:#}", config(vars).expect_err("config must be rejected"))
    }

    const LOCAL: [(&str, &str); 4] = [
        ("GATEWAY_MODEL_PROVIDER", "openai-compatible"),
        ("OPENAI_BASE_URL", "http://127.0.0.1:11434/v1/"),
        ("GATEWAY_CHAT_MODEL", "qwen3:8b"),
        ("GATEWAY_EXTRACTION_MODEL", "qwen3:8b"),
    ];

    #[test]
    fn nothing_configured_is_the_only_unconfigured_state() {
        assert_eq!(config(&[]).expect("valid"), None);
        // Compose passes unset host vars through as empty strings.
        let blank = [("GATEWAY_MODEL_PROVIDER", " "), ("ANTHROPIC_API_KEY", "")];
        assert_eq!(config(&blank).expect("valid"), None);
    }

    #[test]
    fn anthropic_key_alone_keeps_the_existing_defaults() {
        let selected = config(&[("ANTHROPIC_API_KEY", "sk-ant-x")])
            .expect("valid")
            .expect("selected");
        assert_eq!(selected.kind(), ProviderKind::Anthropic);
        assert_eq!(selected.base_url, DEFAULT_ANTHROPIC_BASE_URL);
        assert_eq!(selected.routing.chat_model, DEFAULT_ANTHROPIC_CHAT_MODEL);
        assert_eq!(selected.routing.extraction_model, None);
        assert!(!format!("{selected:?}").contains("sk-ant-x"));
    }

    #[test]
    fn explicit_provider_must_be_complete() {
        assert!(error(&[("GATEWAY_MODEL_PROVIDER", "anthropic")]).contains("ANTHROPIC_API_KEY"));
        assert!(error(&[("GATEWAY_MODEL_PROVIDER", "openai")]).contains("openai-compatible"));
        for missing in [
            "OPENAI_BASE_URL",
            "GATEWAY_CHAT_MODEL",
            "GATEWAY_EXTRACTION_MODEL",
        ] {
            let vars: Vec<_> = LOCAL.into_iter().filter(|(k, _)| *k != missing).collect();
            assert!(error(&vars).contains(missing), "{missing}");
        }
    }

    #[test]
    fn malformed_values_fail_at_startup() {
        for url in [
            "127.0.0.1:11434",
            "ftp://host/v1",
            "http://u:p@host/v1",
            "http://h/v1?x=1",
        ] {
            let mut vars = LOCAL.to_vec();
            vars[1] = ("OPENAI_BASE_URL", url);
            assert!(error(&vars).contains("OPENAI_BASE_URL"), "{url}");
        }
        let mut vars = LOCAL.to_vec();
        vars[3] = ("GATEWAY_EXTRACTION_MODEL", "two words");
        assert!(error(&vars).contains("GATEWAY_EXTRACTION_MODEL"));
        let bad_anthropic = [("ANTHROPIC_API_KEY", "k"), ("ANTHROPIC_BASE_URL", "nope")];
        assert!(error(&bad_anthropic).contains("ANTHROPIC_BASE_URL"));
    }

    #[test]
    fn openai_compatible_key_is_optional_and_url_normalized() {
        let selected = config(&LOCAL).expect("valid").expect("selected");
        assert_eq!(selected.kind(), ProviderKind::OpenAiCompatible);
        assert_eq!(selected.base_url, "http://127.0.0.1:11434/v1");
        assert_eq!(selected.api_key, None);
        let mut vars = LOCAL.to_vec();
        vars.push(("OPENAI_API_KEY", "sk-local"));
        let keyed = config(&vars).expect("valid").expect("selected");
        assert_eq!(keyed.api_key.as_deref(), Some("sk-local"));
    }

    #[test]
    fn anthropic_routing_forwards_claude_ids_only() {
        let routing = config(&[("ANTHROPIC_API_KEY", "k")])
            .expect("valid")
            .expect("selected")
            .routing;
        assert_eq!(
            routing.chat_model(Some("claude-haiku-4-5")),
            "claude-haiku-4-5"
        );
        assert_eq!(
            routing.chat_model(Some("gpt-4o")),
            DEFAULT_ANTHROPIC_CHAT_MODEL
        );
        assert_eq!(routing.chat_model(None), DEFAULT_ANTHROPIC_CHAT_MODEL);
        assert_eq!(
            routing.extraction_model("claude-haiku-4-5"),
            "claude-haiku-4-5"
        );
    }

    #[test]
    fn openai_compatible_routing_forwards_as_named() {
        let routing = config(&LOCAL).expect("valid").expect("selected").routing;
        assert_eq!(routing.chat_model(Some("llama3.3")), "llama3.3");
        assert_eq!(routing.chat_model(Some(" ")), "qwen3:8b");
        assert_eq!(routing.chat_model(None), "qwen3:8b");
        // The pack's Claude id never reaches a local server.
        assert_eq!(routing.extraction_model("claude-haiku-4-5"), "qwen3:8b");
    }

    #[test]
    fn overrides_apply_to_anthropic_too() {
        let routing = config(&[
            ("ANTHROPIC_API_KEY", "k"),
            ("GATEWAY_CHAT_MODEL", "claude-opus-5-5"),
            ("GATEWAY_EXTRACTION_MODEL", "claude-haiku-4-5"),
        ])
        .expect("valid")
        .expect("selected")
        .routing;
        assert_eq!(routing.chat_model(Some("gpt-4o")), "claude-opus-5-5");
        assert_eq!(
            routing.extraction_model("claude-sonnet-5"),
            "claude-haiku-4-5"
        );
    }
}
