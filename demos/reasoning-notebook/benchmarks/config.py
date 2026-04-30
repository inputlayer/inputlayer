"""Benchmark configuration: models and test inputs."""

from __future__ import annotations

import os

# ── Models to benchmark ────────────────────────────────────────────
# Each entry needs: name, provider, base_url, api_key, model
# api_key can be an env var name (resolved at runtime)

MODELS = [
    # Local (LM Studio)
    {
        "name": "ministral-3-3b (local)",
        "provider": "openai",
        "base_url": "http://localhost:1234/v1",
        "api_key": "lm-studio",
        "model": "mistralai/ministral-3-3b",
        "multimodal": True,
    },
    {
        "name": "deepseek-r1-8b (local)",
        "provider": "openai",
        "base_url": "http://localhost:1234/v1",
        "api_key": "lm-studio",
        "model": "deepseek/deepseek-r1-0528-qwen3-8b",
        "multimodal": False,
    },
    {
        "name": "gemma-4-e4b (local)",
        "provider": "openai",
        "base_url": "http://localhost:1234/v1",
        "api_key": "lm-studio",
        "model": "google/gemma-4-e4b",
        "multimodal": True,
    },
    # Cloud — OpenAI
    {
        "name": "gpt-4o-mini",
        "provider": "openai",
        "base_url": "https://api.openai.com/v1",
        "api_key_env": "OPENAI_API_KEY",
        "model": "gpt-4o-mini",
        "multimodal": True,
    },
    {
        "name": "gpt-4o",
        "provider": "openai",
        "base_url": "https://api.openai.com/v1",
        "api_key_env": "OPENAI_API_KEY",
        "model": "gpt-4o",
        "multimodal": True,
    },
    # Cloud — Anthropic
    # {
    #     "name": "claude-sonnet-4",
    #     "provider": "anthropic",
    #     "api_key_env": "ANTHROPIC_API_KEY",
    #     "model": "claude-sonnet-4-20250514",
    #     "multimodal": True,
    # },
]


def get_enabled_models() -> list[dict]:
    """Return models that have their API keys available."""
    enabled = []
    for m in MODELS:
        key_env = m.get("api_key_env")
        if key_env:
            key = os.environ.get(key_env)
            if not key:
                continue
            m = {**m, "api_key": key}
        enabled.append(m)
    return enabled
