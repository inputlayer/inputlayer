"""Shared helpers for LangGraph examples."""

from __future__ import annotations

import os
from typing import Any

from inputlayer import InputLayer

# ── ANSI colors ──────────────────────────────────────────────────────

BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
MAGENTA = "\033[35m"
BLUE = "\033[34m"
WHITE = "\033[97m"
RED = "\033[31m"

__all__ = [
    "BLUE",
    "BOLD",
    "CYAN",
    "DIM",
    "GREEN",
    "MAGENTA",
    "RED",
    "RESET",
    "WHITE",
    "YELLOW",
    "InputLayer",
    "check_llm",
    "cleanup",
    "connect",
    "get_llm",
    "header",
    "os",
    "step",
    "subheader",
    "success",
]


def header(title: str, num: int) -> None:
    print()
    print(f"{CYAN}{BOLD}{'━' * 64}{RESET}")
    print(f"{CYAN}{BOLD}  Example {num}: {title}{RESET}")
    print(f"{CYAN}{BOLD}{'━' * 64}{RESET}")


def subheader(text: str) -> None:
    print(f"\n{WHITE}{BOLD}  {text}{RESET}")


def step(num: int, text: str) -> None:
    print(f"\n{WHITE}{BOLD}  Step {num}: {text}{RESET}")


def success(text: str) -> None:
    print(f"\n{GREEN}{BOLD}  {text}{RESET}\n")


_llm_available: bool | None = None


def check_llm() -> bool:
    """Check if an LLM server is reachable. Result is cached after first call.

    Set LLM_BASE_URL and LLM_API_KEY for authenticated providers (e.g. OpenAI).
    Defaults to a local LM Studio server at http://localhost:1234/v1.
    """
    global _llm_available
    if _llm_available is not None:
        return _llm_available
    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    api_key = os.environ.get("LLM_API_KEY", "")
    try:
        import httpx

        headers: dict[str, str] = {}
        if api_key and api_key != "lm-studio":
            headers["Authorization"] = f"Bearer {api_key}"
        resp = httpx.get(f"{base_url}/models", timeout=5, headers=headers)
        resp.raise_for_status()
        _llm_available = True
    except ImportError:
        print(f"  {DIM}httpx not installed (needed to detect LLM server){RESET}")
        _llm_available = False
    except Exception as exc:
        print(f"  {DIM}LLM server not reachable at {base_url}: {exc}{RESET}")
        _llm_available = False
    return _llm_available


def get_llm() -> Any:
    """Create a ChatOpenAI instance for the local LLM server.

    Environment variables:
        LLM_BASE_URL: API base URL (default: http://localhost:1234/v1)
        LLM_MODEL: Model name (default: deepseek/deepseek-r1-0528-qwen3-8b)
        LLM_API_KEY: API key (default: lm-studio)
    """
    from langchain_openai import ChatOpenAI

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")
    api_key = os.environ.get("LLM_API_KEY", "lm-studio")
    return ChatOpenAI(
        base_url=base_url,
        api_key=api_key,
        model=model,
        temperature=0,
    )


async def connect() -> tuple[InputLayer, Any]:
    """Connect to InputLayer. Returns (client, None).

    Each example creates its own KG, so no shared KG handle is returned.
    Set INPUTLAYER_URL, INPUTLAYER_USER, INPUTLAYER_PASSWORD to override defaults.
    """
    il = InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    )
    await il.connect()
    return il, None


async def cleanup(il: InputLayer, kg_name: str = "") -> None:
    import contextlib

    if kg_name:
        with contextlib.suppress(Exception):
            await il.drop_knowledge_graph(kg_name)
    await il.close()
