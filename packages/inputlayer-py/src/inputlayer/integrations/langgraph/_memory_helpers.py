"""Extracted helpers for InputLayerMemory.

Contains topic extraction and LangGraph node factory functions.
Separated from memory.py to keep individual files under 500 lines.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Coroutine
from typing import Any

logger = logging.getLogger(__name__)

# ── Topic keywords for simple extraction ──────────────────────────────
# NOTE: Demo-quality extractor only. In production, use an LLM by passing
# explicit `topics=` to astore().

_TOPIC_KEYWORDS: dict[str, list[str]] = {
    "python": ["python", "pip", "django", "flask", "pandas", "numpy"],
    "rust": ["rust", "cargo", "borrow checker", "lifetime"],
    "javascript": ["javascript", "typescript", "node", "react", "vue"],
    "ml": ["machine learning", "ml", "model", "training", "neural"],
    "deep_learning": [
        "deep learning", "cnn", "rnn", "transformer", "bert", "gpt",
    ],
    "data": ["data", "database", "sql", "etl", "pipeline", "spark"],
    "devops": ["docker", "kubernetes", "k8s", "ci/cd", "deploy"],
    "api": ["api", "rest", "graphql", "endpoint", "http"],
    "security": ["security", "auth", "encryption", "vulnerability"],
    "performance": [
        "performance", "latency", "optimization", "cache", "fast", "slow",
    ],
}


def extract_topics(text: str) -> list[str]:
    """Extract topics from text using keyword matching.

    Production note: keyword-based and will miss most real messages.
    Pass explicit ``topics=`` to ``astore()`` and use an LLM extractor.
    """
    text_lower = text.lower()
    return [
        topic
        for topic, keywords in _TOPIC_KEYWORDS.items()
        if any(kw in text_lower for kw in keywords)
    ]


# ── LangGraph node factories ─────────────────────────────────────────


def make_store_node(
    memory: Any,
    *,
    state_key: str,
    thread_key: str,
    strict: bool,
) -> Callable[[dict[str, Any]], Coroutine[Any, Any, dict[str, Any]]]:
    """Build the async node function for InputLayerMemory.store_node()."""

    async def _node(state: dict[str, Any]) -> dict[str, Any]:
        thread_id = state.get(thread_key)
        if thread_id is None or thread_id == "":
            if strict:
                raise ValueError(
                    f"InputLayerMemory.store_node: '{thread_key}' not found "
                    f"in state. Add state['{thread_key}'] = '<conversation-id>' "
                    "to your graph state, or use strict=False to fall back to "
                    "a shared default thread."
                )
            logger.warning(
                "InputLayerMemory.store_node: '%s' not found in state. "
                "Falling back to thread_id='default'. All agents without "
                "an explicit thread_id will share the same memory pool. "
                "Set state['%s'] to a unique ID per conversation, or use "
                "strict=True to raise an error instead.",
                thread_key,
                thread_key,
            )
            thread_id = "default"

        msg = state.get(state_key)
        if msg is None:
            return {}
        if not isinstance(msg, dict):
            if strict:
                raise TypeError(
                    f"InputLayerMemory.store_node: expected state['{state_key}']"
                    f" to be a dict with 'role' and 'content' keys, got "
                    f"{type(msg).__name__}. Convert LangChain message objects "
                    "with msg.dict() or pass "
                    "{{'role': msg.type, 'content': msg.content}}."
                )
            logger.warning(
                "InputLayerMemory.store_node: expected state['%s'] to be a "
                "dict with 'role' and 'content' keys, got %s. "
                "If you are using LangChain message objects (HumanMessage, "
                "AIMessage), convert them with msg.dict() or pass "
                "{'role': msg.type, 'content': msg.content} instead. "
                "This message was NOT stored.",
                state_key,
                type(msg).__name__,
            )
            return {}

        topics = msg.get("topics")
        await memory.astore(
            thread_id,
            msg.get("role", "user"),
            msg.get("content", ""),
            topics=topics,
        )
        return {}

    _node.__name__ = "memory_store"
    _node.__qualname__ = "memory_store"
    return _node


def make_recall_node(
    memory: Any,
    *,
    state_key: str,
    thread_key: str,
    strict: bool,
) -> Callable[[dict[str, Any]], Coroutine[Any, Any, dict[str, Any]]]:
    """Build the async node function for InputLayerMemory.recall_node()."""

    async def _node(state: dict[str, Any]) -> dict[str, Any]:
        thread_id = state.get(thread_key)
        if thread_id is None or thread_id == "":
            if strict:
                raise ValueError(
                    f"InputLayerMemory.recall_node: '{thread_key}' not found "
                    f"in state. Add state['{thread_key}'] = '<conversation-id>'"
                    " to your graph state, or use strict=False to fall back "
                    "to a shared default thread."
                )
            logger.warning(
                "InputLayerMemory.recall_node: '%s' not found in state. "
                "Falling back to thread_id='default'. Use strict=True to "
                "raise an error instead.",
                thread_key,
            )
            thread_id = "default"

        context = await memory.arecall(thread_id)
        return {state_key: context}

    _node.__name__ = "memory_recall"
    _node.__qualname__ = "memory_recall"
    return _node
