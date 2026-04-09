//! Claude API client for the teaching agent.
//!
//! Calls the Anthropic Messages API server-side.
//! The API key is never exposed to the browser.

use super::Message;

/// Call the Claude Messages API and return the response text.
pub async fn call_claude(
    api_key: &str,
    model: &str,
    system_prompt: &str,
    messages: &[Message],
    max_tokens: usize,
) -> Result<String, String> {
    let client = reqwest::Client::new();

    let api_messages: Vec<serde_json::Value> = messages
        .iter()
        .map(|m| {
            serde_json::json!({
                "role": m.role,
                "content": m.content,
            })
        })
        .collect();

    let body = serde_json::json!({
        "model": model,
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": api_messages,
    });

    let response = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Claude API request failed: {e}"))?;

    let status = response.status();
    let response_text = response
        .text()
        .await
        .map_err(|e| format!("Failed to read Claude API response: {e}"))?;

    if !status.is_success() {
        return Err(format!(
            "Claude API error ({}): {}",
            status,
            truncate(&response_text, 200)
        ));
    }

    let json: serde_json::Value = serde_json::from_str(&response_text)
        .map_err(|e| format!("Failed to parse Claude API response: {e}"))?;

    // Extract text from the response content blocks
    let content = json["content"]
        .as_array()
        .ok_or("Claude API response missing content array")?;

    let text: String = content
        .iter()
        .filter_map(|block| {
            if block["type"] == "text" {
                block["text"].as_str().map(String::from)
            } else {
                None
            }
        })
        .collect::<String>();

    if text.is_empty() {
        return Err("Claude API returned empty response".to_string());
    }

    Ok(text)
}

fn truncate(s: &str, max_len: usize) -> &str {
    if s.len() <= max_len {
        s
    } else {
        &s[..max_len]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("hello world", 5), "hello");
    }
}
