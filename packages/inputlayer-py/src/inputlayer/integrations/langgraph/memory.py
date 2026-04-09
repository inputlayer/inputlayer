"""InputLayerMemory — semantic long-term memory for LangGraph agents.

Stores conversation turns as facts in a KG. Datalog rules automatically
derive active topics, relevant context, and conversation threads.
Unlike raw chat history, the recalled context is DERIVED — rules decide
what's relevant, not just vector similarity or recency.

Usage::

    from inputlayer.integrations.langgraph import InputLayerMemory

    memory = InputLayerMemory(kg=kg)
    await memory.setup()

    # Store a turn
    await memory.astore("thread-1", "user", "I need help with ML in Python")

    # Rules derive topics, relevant context, etc.

    # Recall context for the next turn
    context = await memory.arecall("thread-1")
    # Returns: {"topics": [...], "recent": [...], "related": [...]}

    # Use as LangGraph nodes
    graph.add_node("recall", memory.recall_node(state_key="context"))
    graph.add_node("store", memory.store_node(state_key="new_message"))
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from inputlayer._sync import run_sync

# ── Topic keywords for simple extraction ─────────────────────────────
# In production, you'd use an LLM for this. These keywords demonstrate
# the pattern without requiring an LLM dependency.

_TOPIC_KEYWORDS: dict[str, list[str]] = {
    "python": ["python", "pip", "django", "flask", "pandas", "numpy"],
    "rust": ["rust", "cargo", "borrow checker", "lifetime"],
    "javascript": ["javascript", "typescript", "node", "react", "vue"],
    "ml": ["machine learning", "ml", "model", "training", "neural"],
    "deep_learning": ["deep learning", "cnn", "rnn", "transformer", "bert", "gpt"],
    "data": ["data", "database", "sql", "etl", "pipeline", "spark"],
    "devops": ["docker", "kubernetes", "k8s", "ci/cd", "deploy"],
    "api": ["api", "rest", "graphql", "endpoint", "http"],
    "security": ["security", "auth", "encryption", "vulnerability"],
    "performance": ["performance", "latency", "optimization", "cache", "fast", "slow"],
}


def _extract_topics(text: str) -> list[str]:
    """Extract topics from text using keyword matching."""
    text_lower = text.lower()
    topics = []
    for topic, keywords in _TOPIC_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            topics.append(topic)
    return topics


class InputLayerMemory:
    """Semantic memory backed by an InputLayer KnowledgeGraph.

    Stores conversation turns and derived context. Datalog rules
    automatically compute:

    - **active_topic(ThreadId, Topic)** — topics mentioned in this thread
    - **relevant_turn(ThreadId, TurnId, Role, Content, Topic)** —
      turns that mention an active topic (cross-referenced)
    - **topic_thread(ThreadId, TopicA, TopicB)** — pairs of topics
      discussed in the same thread (conversation themes)

    Args:
        kg: An InputLayer KnowledgeGraph handle.
        max_recent: Number of recent turns to include in recall.
    """

    def __init__(self, kg: Any, *, max_recent: int = 10) -> None:
        self.kg = kg
        self.max_recent = max_recent
        self._setup_done = False
        self._turn_counter = 0

    # ── Setup ────────────────────────────────────────────────────────

    async def setup(self) -> None:
        """Create memory relations and rules (idempotent)."""
        if self._setup_done:
            return

        # Schema
        await self.kg.execute(
            "+memory_turn(thread_id: string, turn_id: int, role: string, content: string, ts: int)"
        )
        await self.kg.execute("+memory_topic(thread_id: string, turn_id: int, topic: string)")

        # Rule: active topics per thread
        await self.kg.execute(
            "+active_topic(ThreadId, Topic) <- memory_topic(ThreadId, TurnId, Topic)"
        )

        # Rule: relevant turns — turns that share an active topic
        await self.kg.execute(
            "+relevant_turn(ThreadId, TurnId, Role, Content, Topic) <- "
            "memory_turn(ThreadId, TurnId, Role, Content, Ts), "
            "memory_topic(ThreadId, TurnId, Topic)"
        )

        # Rule: topic threads — pairs of topics discussed together
        await self.kg.execute(
            "+topic_thread(ThreadId, TopicA, TopicB) <- "
            "memory_topic(ThreadId, TurnIdA, TopicA), "
            "memory_topic(ThreadId, TurnIdB, TopicB), "
            "TopicA != TopicB"
        )

        self._setup_done = True

    def setup_sync(self) -> None:
        run_sync(self.setup())

    # ── Store ────────────────────────────────────────────────────────

    async def astore(
        self,
        thread_id: str,
        role: str,
        content: str,
        *,
        topics: list[str] | None = None,
    ) -> int:
        """Store a conversation turn and its topics.

        Args:
            thread_id: Conversation thread identifier.
            role: "user", "assistant", or "system".
            content: The message content.
            topics: Explicit topics. If None, auto-extracted from content.

        Returns:
            The turn_id assigned to this turn.
        """
        await self.setup()

        self._turn_counter += 1
        turn_id = self._turn_counter
        ts = time.time_ns()

        escaped_content = content.replace("\\", "\\\\").replace('"', '\\"')
        await self.kg.execute(
            f'+memory_turn("{thread_id}", {turn_id}, "{role}", "{escaped_content}", {ts})'
        )

        # Extract and store topics
        if topics is None:
            topics = _extract_topics(content)

        for topic in topics:
            await self.kg.execute(f'+memory_topic("{thread_id}", {turn_id}, "{topic}")')

        return turn_id

    def store(
        self,
        thread_id: str,
        role: str,
        content: str,
        *,
        topics: list[str] | None = None,
    ) -> int:
        return run_sync(self.astore(thread_id, role, content, topics=topics))

    # ── Recall ───────────────────────────────────────────────────────

    async def arecall(self, thread_id: str) -> dict[str, Any]:
        """Recall derived context for a thread.

        Returns a dict with:
        - topics: list of active topics
        - recent: list of recent turns [{role, content, turn_id}]
        - relevant: list of turns grouped by topic
        - related_topics: pairs of topics discussed together
        """
        await self.setup()

        result: dict[str, Any] = {
            "topics": [],
            "recent": [],
            "relevant": {},
            "related_topics": [],
        }

        # Active topics
        r = await self.kg.execute(f'?active_topic("{thread_id}", Topic)')
        result["topics"] = sorted({str(row[-1]) for row in r.rows})

        # Recent turns (all turns, sorted by turn_id desc)
        r = await self.kg.execute(f'?memory_turn("{thread_id}", TurnId, Role, Content, Ts)')
        turns = sorted(r.rows, key=lambda row: row[-1], reverse=True)
        for row in turns[: self.max_recent]:
            result["recent"].append(
                {
                    "turn_id": row[-4],
                    "role": str(row[-3]),
                    "content": str(row[-2]),
                }
            )

        # Relevant turns grouped by topic
        r = await self.kg.execute(f'?relevant_turn("{thread_id}", TurnId, Role, Content, Topic)')
        by_topic: dict[str, list[dict[str, Any]]] = {}
        for row in r.rows:
            topic = str(row[-1])
            turn = {
                "turn_id": row[-4],
                "role": str(row[-3]),
                "content": str(row[-2]),
            }
            by_topic.setdefault(topic, []).append(turn)
        result["relevant"] = by_topic

        # Related topic pairs
        r = await self.kg.execute(f'?topic_thread("{thread_id}", TopicA, TopicB)')
        seen: set[tuple[str, str]] = set()
        for row in r.rows:
            a, b = sorted([str(row[-2]), str(row[-1])])
            pair = (a, b)
            if pair not in seen:
                seen.add(pair)
                result["related_topics"].append(pair)

        return result

    def recall(self, thread_id: str) -> dict[str, Any]:
        return run_sync(self.arecall(thread_id))

    # ── LangGraph node factories ─────────────────────────────────────

    def store_node(
        self,
        *,
        state_key: str = "new_message",
        thread_key: str = "thread_id",
    ) -> Callable[[dict[str, Any]], Any]:
        """Create a LangGraph node that stores a message from state.

        Expects state[state_key] to be a dict with 'role' and 'content',
        and state[thread_key] to be the thread id.

        Usage::

            graph.add_node("store", memory.store_node())
        """
        memory = self

        async def _node(state: dict[str, Any]) -> dict[str, Any]:
            msg = state.get(state_key)
            thread_id = state.get(thread_key, "default")
            if msg and isinstance(msg, dict):
                await memory.astore(
                    thread_id,
                    msg.get("role", "user"),
                    msg.get("content", ""),
                )
            return {}

        _node.__name__ = "memory_store"
        _node.__qualname__ = "memory_store"
        return _node

    def recall_node(
        self,
        *,
        state_key: str = "context",
        thread_key: str = "thread_id",
    ) -> Callable[[dict[str, Any]], Any]:
        """Create a LangGraph node that recalls context into state.

        Writes the recall result to state[state_key].

        Usage::

            graph.add_node("recall", memory.recall_node())
        """
        memory = self

        async def _node(state: dict[str, Any]) -> dict[str, Any]:
            thread_id = state.get(thread_key, "default")
            context = await memory.arecall(thread_id)
            return {state_key: context}

        _node.__name__ = "memory_recall"
        _node.__qualname__ = "memory_recall"
        return _node
