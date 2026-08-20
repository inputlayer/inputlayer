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
