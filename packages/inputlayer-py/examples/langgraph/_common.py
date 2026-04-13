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


def check_llm() -> bool:
    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
        return True
    except Exception:
        return False


def get_llm() -> Any:
    from langchain_openai import ChatOpenAI

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")
    return ChatOpenAI(
        base_url=base_url,
        api_key="lm-studio",
        model=model,
        temperature=0,
    )


async def connect() -> tuple[InputLayer, Any]:
    il = InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    )
    await il.connect()
    return il, None  # no shared KG — each example sets up its own


async def cleanup(il: InputLayer, kg_name: str = "") -> None:
    import contextlib

    if kg_name:
        with contextlib.suppress(Exception):
            await il.drop_knowledge_graph(kg_name)
    await il.close()
