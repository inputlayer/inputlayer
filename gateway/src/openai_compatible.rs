//! Client for the OpenAI chat completions shape (`POST {base}/chat/completions`).
//!
//! This is the surface local model servers expose (Ollama, vLLM, llama.cpp,
//! LM Studio) as well as OpenAI itself, so one client covers "run the
//! extractor on a local model" without touching the pipeline: it implements
//! the same [`Extractor`] and [`Completer`] traits as `AnthropicClient`.
//! Extraction uses `response_format: json_schema` (strict), so the server
//! must support schema-constrained output; a server that ignores it fails
//! the downstream JSON/quote checks instead of producing silent facts.

use crate::model::{
    provider_http_client, ChatCompletion, ChatParams, Completer, Extraction, Extractor,
    UpstreamStatus,
};
use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

pub struct OpenAiCompatibleClient {
    http: reqwest::Client,
    /// Local servers usually need none; sent as a bearer token when set.
    api_key: Option<String>,
    /// API root including any version segment, e.g.
    /// `http://127.0.0.1:11434/v1` (the OpenAI SDK convention).
    base_url: String,
}

impl OpenAiCompatibleClient {
    pub fn new(api_key: Option<String>, base_url: String) -> Self {
        Self {
            http: provider_http_client(),
            api_key,
            base_url,
        }
    }

    /// POST one chat completions request and return the decoded body, with
    /// any non-2xx status carried in the error chain as [`UpstreamStatus`].
    async fn post(&self, body: &Value, what: &str) -> Result<Value> {
        let mut request = self
            .http
            .post(format!("{}/chat/completions", self.base_url))
            .json(body);
        if let Some(key) = &self.api_key {
            request = request.bearer_auth(key);
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("{what} request failed"))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .with_context(|| format!("{what} response body unreadable"))?;
        let payload = serde_json::from_slice::<Value>(&bytes);
        if !status.is_success() {
            // A proxy in front of a local server may answer with HTML: the
            // status still has to reach the caller's error mapping.
            let message = payload.as_ref().map_or("unknown", error_message);
            return Err(anyhow::Error::new(UpstreamStatus(status.as_u16()))
                .context(format!("{what} API error ({status}): {message}")));
        }
        payload.with_context(|| format!("{what} response is not JSON"))
    }
}

/// OpenAI nests the message under `error.message`; some local servers
/// return `error` as a bare string.
fn error_message(payload: &Value) -> &str {
    payload["error"]["message"]
        .as_str()
        .or_else(|| payload["error"].as_str())
        .unwrap_or("unknown")
}

/// The one choice's message, refusing anything that is not a complete
/// answer: a truncated or filtered response must never become facts or a
/// silently clipped completion labelled `stop`.
fn first_message<'a>(payload: &'a Value, what: &str) -> Result<(&'a str, &'a str)> {
    let choice = &payload["choices"][0];
    if let Some(refusal) = choice["message"]["refusal"].as_str() {
        return Err(anyhow!("{what} refused by the model: {refusal}"));
    }
    let text = choice["message"]["content"]
        .as_str()
        .filter(|t| !t.is_empty())
        .ok_or_else(|| anyhow!("{what} response has no text content"))?;
    let finish_reason = choice["finish_reason"].as_str().unwrap_or("stop");
    Ok((text, finish_reason))
}

/// The extraction request: the static pack prompt is the system message
/// (byte-stable, so servers with prefix caching reuse it) and the schema
/// forces the output shape, mirroring the Anthropic structured-outputs call.
fn extraction_body(model: &str, system_prompt: &str, user_content: &str, schema: &Value) -> Value {
    json!({
        "model": model,
        "max_tokens": 8192,
        "temperature": 0,
        "messages": [
            { "role": "system", "content": system_prompt },
            { "role": "user", "content": user_content },
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": { "name": "extraction", "strict": true, "schema": schema },
        },
    })
}

/// The completion request. `ChatParams` were mapped FROM the OpenAI shape,
/// so this is close to the identity: no temperature clamp is needed (0-2 is
/// the native range here).
fn completion_body(params: &ChatParams) -> Value {
    let mut messages: Vec<Value> = Vec::with_capacity(params.messages.len() + 1);
    if let Some(system) = &params.system {
        messages.push(json!({ "role": "system", "content": system }));
    }
    messages.extend(
        params
            .messages
            .iter()
            .map(|(role, content)| json!({ "role": role, "content": content })),
    );
    let mut body = json!({
        "model": params.model,
        "max_tokens": params.max_tokens,
        "messages": messages,
    });
    if let Some(temperature) = params.temperature {
        body["temperature"] = json!(temperature);
    }
    if let Some(top_p) = params.top_p {
        body["top_p"] = json!(top_p);
    }
    if !params.stop.is_empty() {
        body["stop"] = json!(params.stop);
    }
    body
}

#[async_trait::async_trait]
impl Extractor for OpenAiCompatibleClient {
    async fn extract(
        &self,
        model: &str,
        system_prompt: &str,
        user_content: &str,
        schema: &Value,
    ) -> Result<Extraction> {
        let body = extraction_body(model, system_prompt, user_content, schema);
        let payload = self.post(&body, "extraction").await?;
        let (text, finish_reason) = first_message(&payload, "extraction")?;
        if finish_reason != "stop" {
            return Err(anyhow!(
                "extraction ended with finish_reason {finish_reason:?} - refusing partial facts"
            ));
        }
        Ok(Extraction {
            output: serde_json::from_str(text).context("extraction output is not valid JSON")?,
            usage: payload["usage"].clone(),
        })
    }
}

#[async_trait::async_trait]
impl Completer for OpenAiCompatibleClient {
    async fn complete(&self, params: &ChatParams) -> Result<ChatCompletion> {
        let payload = self.post(&completion_body(params), "completion").await?;
        let (text, finish_reason) = first_message(&payload, "completion")?;
        let finish_reason = match finish_reason {
            "length" | "content_filter" => finish_reason,
            _ => "stop",
        }
        .to_string();
        Ok(ChatCompletion {
            text: text.to_string(),
            finish_reason,
            prompt_tokens: payload["usage"]["prompt_tokens"].as_u64().unwrap_or(0),
            completion_tokens: payload["usage"]["completion_tokens"].as_u64().unwrap_or(0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(system: Option<&str>) -> ChatParams {
        ChatParams {
            model: "local-chat".to_string(),
            system: system.map(str::to_string),
            messages: vec![("user".to_string(), "hi".to_string())],
            max_tokens: 64,
            temperature: Some(1.5),
            top_p: None,
            stop: vec!["END".to_string()],
        }
    }

    #[test]
    fn extraction_forces_the_pack_schema() {
        let schema = json!({"type": "object", "additionalProperties": false});
        let body = extraction_body("local-x", "STATIC", "per call", &schema);
        assert_eq!(body["model"], "local-x");
        assert_eq!(body["temperature"], 0);
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(body["messages"][0]["content"], "STATIC");
        assert_eq!(body["messages"][1]["content"], "per call");
        assert_eq!(body["response_format"]["type"], "json_schema");
        assert_eq!(body["response_format"]["json_schema"]["strict"], true);
        assert_eq!(body["response_format"]["json_schema"]["schema"], schema);
    }

    #[test]
    fn completion_keeps_the_openai_shape() {
        let body = completion_body(&params(Some("Be brief.")));
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(body["messages"][1]["content"], "hi");
        // Native 0-2 range: no Anthropic-style clamp.
        assert_eq!(body["temperature"], 1.5);
        assert_eq!(body["stop"], json!(["END"]));
        assert!(body.get("top_p").is_none());
        let body = completion_body(&params(None));
        assert_eq!(body["messages"][0]["role"], "user");
    }

    #[test]
    fn incomplete_answers_are_refused() {
        let refused = json!({"choices": [{"message": {"content": null, "refusal": "no"}}]});
        assert!(first_message(&refused, "x").is_err());
        let empty = json!({"choices": [{"message": {"content": ""}, "finish_reason": "stop"}]});
        assert!(first_message(&empty, "x").is_err());
        assert!(first_message(&json!({}), "x").is_err());
        let cut = json!({"choices": [{"message": {"content": "a"}, "finish_reason": "length"}]});
        assert_eq!(first_message(&cut, "x").expect("text"), ("a", "length"));
    }

    #[test]
    fn error_messages_accept_both_server_shapes() {
        assert_eq!(error_message(&json!({"error": {"message": "bad"}})), "bad");
        assert_eq!(
            error_message(&json!({"error": "model not found"})),
            "model not found"
        );
        assert_eq!(error_message(&json!({})), "unknown");
    }
}
