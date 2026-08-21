//! Extraction: text to typed claims via the model, always bound to the
//! selected ontology's prompt and schema - never open-domain.
//!
//! The model call is behind a trait so the pipeline is testable without a
//! model key; the production implementation talks to the Anthropic Messages
//! API over REST with structured outputs (`output_config.format`).

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

#[async_trait::async_trait]
pub trait Extractor: Send + Sync {
    async fn extract(
        &self,
        model: &str,
        system_prompt: &str,
        user_content: &str,
        schema: &Value,
    ) -> Result<Value>;
}

/// Parameters for a chat completion, mapped from the OpenAI request shape.
pub struct ChatParams {
    pub model: String,
    pub system: Option<String>,
    /// (role, content) with role "user" or "assistant" only.
    pub messages: Vec<(String, String)>,
    pub max_tokens: u32,
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub stop: Vec<String>,
}

pub struct ChatCompletion {
    pub text: String,
    /// OpenAI-style finish reason ("stop" or "length").
    pub finish_reason: String,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
}

/// Plain chat completions (no structured outputs) - the M1 proxy's model
/// call, behind a trait for the same testability reason as `Extractor`.
#[async_trait::async_trait]
pub trait Completer: Send + Sync {
    async fn complete(&self, params: &ChatParams) -> Result<ChatCompletion>;
}

pub struct AnthropicExtractor {
    http: reqwest::Client,
    api_key: String,
    base_url: String,
}

impl AnthropicExtractor {
    pub fn new(api_key: String) -> Self {
        let base_url = std::env::var("ANTHROPIC_BASE_URL")
            .unwrap_or_else(|_| "https://api.anthropic.com".to_string());
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            http,
            api_key,
            base_url,
        }
    }
}

#[async_trait::async_trait]
impl Extractor for AnthropicExtractor {
    async fn extract(
        &self,
        model: &str,
        system_prompt: &str,
        user_content: &str,
        schema: &Value,
    ) -> Result<Value> {
        let body = json!({
            "model": model,
            "max_tokens": 8192,
            "temperature": 0,
            "system": system_prompt,
            "messages": [{ "role": "user", "content": user_content }],
            "output_config": { "format": { "type": "json_schema", "schema": schema } },
        });
        let response = self
            .http
            .post(format!("{}/v1/messages", self.base_url))
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .context("extraction request failed")?;
        let status = response.status();
        let payload: Value = response
            .json()
            .await
            .context("extraction response is not JSON")?;
        if !status.is_success() {
            return Err(anyhow!(
                "extraction API error ({status}): {}",
                payload["error"]["message"].as_str().unwrap_or("unknown")
            ));
        }
        if payload["stop_reason"].as_str() == Some("max_tokens") {
            return Err(anyhow!(
                "extraction truncated (max_tokens) - refusing partial facts"
            ));
        }
        let text = payload["content"][0]["text"]
            .as_str()
            .ok_or_else(|| anyhow!("extraction response has no text content"))?;
        serde_json::from_str(text).context("extraction output is not valid JSON")
    }
}

#[async_trait::async_trait]
impl Completer for AnthropicExtractor {
    async fn complete(&self, params: &ChatParams) -> Result<ChatCompletion> {
        let messages: Vec<Value> = params
            .messages
            .iter()
            .map(|(role, content)| json!({ "role": role, "content": content }))
            .collect();
        let mut body = json!({
            "model": params.model,
            "max_tokens": params.max_tokens,
            "messages": messages,
        });
        if let Some(system) = &params.system {
            body["system"] = json!(system);
        }
        if let Some(temperature) = params.temperature {
            body["temperature"] = json!(temperature);
        }
        if let Some(top_p) = params.top_p {
            body["top_p"] = json!(top_p);
        }
        if !params.stop.is_empty() {
            body["stop_sequences"] = json!(params.stop);
        }
        let response = self
            .http
            .post(format!("{}/v1/messages", self.base_url))
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .context("completion request failed")?;
        let status = response.status();
        let payload: Value = response
            .json()
            .await
            .context("completion response is not JSON")?;
        if !status.is_success() {
            return Err(anyhow!(
                "completion API error ({status}): {}",
                payload["error"]["message"].as_str().unwrap_or("unknown")
            ));
        }
        // Concatenate all text blocks; adaptive-thinking models may emit
        // non-text blocks first.
        let text: String = payload["content"]
            .as_array()
            .map(|blocks| {
                blocks
                    .iter()
                    .filter_map(|b| b["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("")
            })
            .unwrap_or_default();
        if text.is_empty() {
            return Err(anyhow!("completion response has no text content"));
        }
        let finish_reason = match payload["stop_reason"].as_str() {
            Some("max_tokens") => "length",
            _ => "stop",
        }
        .to_string();
        Ok(ChatCompletion {
            text,
            finish_reason,
            prompt_tokens: payload["usage"]["input_tokens"].as_u64().unwrap_or(0),
            completion_tokens: payload["usage"]["output_tokens"].as_u64().unwrap_or(0),
        })
    }
}

/// Render the conversation for the extraction user turn: numbered messages,
/// matching the message-index convention the packs' prompts teach.
pub fn render_conversation(messages: &[(String, String)]) -> String {
    let mut out = String::from("Messages (index - role - content):\n");
    for (index, (role, content)) in messages.iter().enumerate() {
        out.push_str(&format!("[{index}] {role}: {content}\n"));
    }
    out.push_str("\nExtract from ALL messages above.");
    out
}
