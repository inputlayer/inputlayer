"""Shared helpers, schema, and setup for LangChain examples."""

from __future__ import annotations

import os
from typing import Any

from inputlayer import (
    Derived,
    From,
    HnswIndex,
    InputLayer,
    Relation,
    Vector,
)
from inputlayer.integrations.langchain import InputLayerRetriever, InputLayerTool

# ── Re-exports for examples ─────────────────────────────────────────

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
    "Article",
    "Derived",
    "From",
    "HnswIndex",
    "InputLayer",
    "InputLayerRetriever",
    "InputLayerTool",
    "Relation",
    "RelevantArticle",
    "UserInterest",
    "Vector",
    "check_llm",
    "cleanup",
    "connect",
    "doc_row",
    "get_llm",
    "header",
    "os",
    "setup",
    "subheader",
    "success",
    "tool_table",
]

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

# ── Display helpers ──────────────────────────────────────────────────


def header(title: str, num: int) -> None:
    print()
    print(f"{CYAN}{BOLD}{'━' * 64}{RESET}")
    print(f"{CYAN}{BOLD}  Example {num}: {title}{RESET}")
    print(f"{CYAN}{BOLD}{'━' * 64}{RESET}")


def subheader(text: str) -> None:
    print(f"\n{WHITE}{BOLD}  {text}{RESET}")


def doc_row(
    title: str,
    content: str,
    tag: str = "",
    score: str | float = "",
) -> None:
    tag_str = f"{YELLOW}[{tag}]{RESET} " if tag else ""
    score_str = f" {DIM}(dist={score:.6f}){RESET}" if isinstance(score, float) else ""
    print(f"  {tag_str}{GREEN}{title}{RESET}{score_str}")
    print(f"  {DIM}{content[:90]}{RESET}")


def tool_table(text: str) -> None:
    """Pretty-print a tab-separated tool result as a table."""
    lines = text.strip().split("\n")
    if not lines:
        return

    rows = [line.split("\t") for line in lines]
    widths = [0] * len(rows[0])
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(widths):
                widths[i] = min(max(widths[i], len(cell)), 40)

    def fmt_row(row: list[str], color: str = "") -> str:
        cells = []
        for i, cell in enumerate(row):
            w = widths[i] if i < len(widths) else 10
            cells.append(cell[:w].ljust(w))
        return f"  {color}{'  '.join(cells)}{RESET}"

    print(fmt_row(rows[0], f"{BOLD}{WHITE}"))
    print(f"  {DIM}{'  '.join('─' * w for w in widths)}{RESET}")
    for row in rows[1:]:
        if row[0].startswith("..."):
            print(f"  {DIM}{row[0]}{RESET}")
        else:
            print(fmt_row(row))


def success(text: str) -> None:
    print(f"\n{GREEN}{BOLD}  {text}{RESET}\n")


# ── LLM helpers ──────────────────────────────────────────────────────


def check_llm() -> bool:
    """Check if an LLM server is available."""
    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
        return True
    except Exception:
        return False


def get_llm() -> Any:
    """Get a ChatOpenAI instance pointing at LM Studio or compatible server."""
    from langchain_openai import ChatOpenAI

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")
    return ChatOpenAI(base_url=base_url, api_key="lm-studio", model=model, temperature=0)


# ── Schema definitions ───────────────────────────────────────────────


class Article(Relation):
    id: int
    title: str
    content: str
    category: str
    embedding: Vector


class UserInterest(Relation):
    user: str
    category: str


class RelevantArticle(Derived):
    """Articles relevant to a user — derived via Datalog rules."""

    title: str
    content: str
    category: str

    rules = [  # noqa: RUF012
        From(Article, UserInterest)
        .where(lambda a, u: a.category == u.category)
        .select(
            title=Article.title,
            content=Article.content,
            category=Article.category,
        ),
    ]


# ── Setup ────────────────────────────────────────────────────────────


async def setup(il: InputLayer) -> Any:
    """Create and populate the langchain_demo KG. Returns the KG handle."""
    kg = il.knowledge_graph("langchain_demo")

    print(f"\n{DIM}  Setting up knowledge graph...{RESET}")

    await kg.define(Article, UserInterest)

    await kg.create_index(
        HnswIndex(
            name="article_emb_idx",
            relation=Article,
            column="embedding",
            metric="cosine",
        )
    )

    await kg.insert(
        [
            Article(
                id=1,
                title="Introduction to Machine Learning",
                content="ML is a subset of AI that enables systems to learn from data.",
                category="ml",
                embedding=[0.1, 0.9, 0.0],
            ),
            Article(
                id=2,
                title="Deep Learning with Neural Networks",
                content="Deep learning uses multi-layer neural networks for complex tasks.",
                category="ml",
                embedding=[0.2, 0.8, 0.1],
            ),
            Article(
                id=3,
                title="Building REST APIs",
                content="REST APIs use HTTP methods to create, read, update, and delete resources.",
                category="web",
                embedding=[0.9, 0.1, 0.0],
            ),
            Article(
                id=4,
                title="Graph Databases Explained",
                content="Graph databases store data as nodes and edges, ideal for connected data.",
                category="db",
                embedding=[0.5, 0.5, 0.5],
            ),
            Article(
                id=5,
                title="Reinforcement Learning Basics",
                content="RL trains agents to make decisions by rewarding desired behaviors.",
                category="ml",
                embedding=[0.15, 0.85, 0.05],
            ),
        ]
    )

    await kg.insert(
        [
            UserInterest(user="alice", category="ml"),
            UserInterest(user="alice", category="db"),
            UserInterest(user="bob", category="web"),
        ]
    )

    await kg.define_rules(RelevantArticle)

    print(f"{DIM}  Inserted 5 articles, 3 user interests, 1 derived rule{RESET}")
    return kg


async def connect() -> tuple[InputLayer, Any]:
    """Connect and setup. Returns (client, kg). Caller must close client."""
    il = InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    )
    await il.connect()
    kg = await setup(il)
    return il, kg


async def cleanup(il: InputLayer) -> None:
    """Drop the demo KG and close the connection."""
    import contextlib

    with contextlib.suppress(Exception):
        await il.drop_knowledge_graph("langchain_demo")
    await il.close()
