"""InputLayerMemory: semantic long-term memory for LangGraph agents.

Stores conversation turns as facts in a KG. Rules automatically
derive active topics, relevant context, and conversation threads.
The recalled context is DERIVED: rules compute what's relevant
based on shared topics and conversation structure.

Usage::

    from inputlayer.integrations.langgraph import InputLayerMemory

    memory = InputLayerMemory(kg=kg)
    await memory.setup()

    # Store a turn (topics auto-extracted from content)
    await memory.astore("thread-1", "user", "I need help with ML in Python")

    # Or provide topics explicitly (recommended for production - use an LLM)
    await memory.astore("thread-1", "user", "...", topics=["ml", "python"])

    # Recall context derived by rules
    context = await memory.arecall("thread-1")
    # Returns: {"topics": [...], "recent": [...], "relevant": {...}, "related_topics": [...]}

    # Use as LangGraph nodes
    graph.add_node("recall", memory.recall_node(state_key="context"))
    graph.add_node("store", memory.store_node(state_key="new_message"))
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Coroutine
from typing import Any

from inputlayer._sync import run_sync
from inputlayer.integrations.langgraph._utils import escape_iql

logger = logging.getLogger(__name__)

# ── Topic keywords for simple extraction ─────────────────────────────
# NOTE: This keyword list is a demo-quality extractor only. In production,
# replace this with an LLM call by passing explicit `topics=` to astore().
# The keyword list is intentionally simple and will miss many real messages.

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
    """Extract topics from text using keyword matching.

    Production note: this is keyword-based and will miss most real messages.
    Pass explicit ``topics=`` to ``astore()`` and use an LLM extractor instead.
    """
    text_lower = text.lower()
    topics = []
    for topic, keywords in _TOPIC_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            topics.append(topic)
    return topics


class InputLayerMemory:
    """Semantic memory backed by an InputLayer KnowledgeGraph.

    Stores conversation turns and derived context. Rules automatically compute:

    - **active_topic(ThreadId, Topic)**: topics mentioned in this thread
    - **relevant_turn(ThreadId, TurnId, Role, Content, Topic)**:
      turns that mention an active topic (cross-referenced by topic)
    - **topic_thread(ThreadId, TopicA, TopicB)**: pairs of topics
      discussed together in the same thread

    Thread safety: a single instance can be shared across coroutines.
    ``setup()`` is guarded by a lock; ``astore()`` uses a per-thread lock
    to ensure turn IDs are sequential within each thread.

    Process restart safety: the turn counter is initialized from the KG
    on the first store for each thread, so IDs continue correctly after
    a restart rather than resetting to 1.

    Args:
        kg: An InputLayer KnowledgeGraph handle.
        max_recent: Number of recent turns to include in recall (default 10).
    """

    def __init__(self, kg: Any, *, max_recent: int = 10) -> None:
        self.kg = kg
        self.max_recent = max_recent
        self._setup_done = False
        self._setup_lock = asyncio.Lock()
        self._turn_counters: dict[str, int] = {}
        self._counter_lock = asyncio.Lock()

    # ── Setup ────────────────────────────────────────────────────────

    async def setup(self) -> None:
        """Create memory relations and rules (idempotent, concurrency-safe).

        Safe to call from multiple coroutines simultaneously. The first
        caller runs the DDL; subsequent callers return immediately.
        Ignores "already exists" errors so re-running setup is harmless.
        """
        if self._setup_done:
            return

        async with self._setup_lock:
            if self._setup_done:  # re-check after acquiring lock
                return

            # Run each DDL/rule and track whether anything raised a real error.
            # Server-side "already exists" responses come back as ResultSet rows,
            # not exceptions. Exceptions here mean connection/auth problems.
            # If any step raises, don't mark setup as done so the next call retries.
            for ddl in [
                "+memory_turn("
                "thread_id: string, turn_id: int, role: string, content: string, ts: int)",
                "+memory_topic(thread_id: string, turn_id: int, topic: string)",
            ]:
                await self.kg.execute(ddl)

            for rule in [
                "+active_topic(ThreadId, Topic) <- memory_topic(ThreadId, TurnId, Topic)",
                (
                    "+relevant_turn(ThreadId, TurnId, Role, Content, Topic) <- "
                    "memory_turn(ThreadId, TurnId, Role, Content, Ts), "
                    "memory_topic(ThreadId, TurnId, Topic)"
                ),
                (
                    "+topic_thread(ThreadId, TopicA, TopicB) <- "
                    "memory_topic(ThreadId, TurnIdA, TopicA), "
                    "memory_topic(ThreadId, TurnIdB, TopicB), "
                    "TopicA != TopicB"
                ),
            ]:
                await self.kg.execute(rule)

            # Only mark done after ALL steps complete. If the server was down,
            # the exception propagated above and this line is never reached,
            # so the next call will retry the full setup.
            self._setup_done = True

    def setup_sync(self) -> None:
        run_sync(self.setup())

    def __repr__(self) -> str:
        kg_name = getattr(self.kg, "name", repr(self.kg))
        return (
            f"InputLayerMemory(kg={kg_name!r}, "
            f"max_recent={self.max_recent}, "
            f"setup_done={self._setup_done}, "
            f"threads={list(self._turn_counters.keys())})"
        )

    # ── Internal: turn counter ───────────────────────────────────────

    async def _next_turn_id(self, thread_id: str) -> int:
        """Return the next turn_id for a thread, initializing from the KG if needed.

        Initializes the counter from the stored max turn_id on the first call
        for each thread, so the counter resumes correctly after a process restart.
        Guarded by a lock so concurrent callers get distinct IDs.
        """
        async with self._counter_lock:
            if thread_id not in self._turn_counters:
                # Query the KG for the current max turn_id for this thread
                r = await self.kg.execute(
                    f'?memory_turn("{escape_iql(thread_id)}", TurnId, Role, Content, Ts)'
                )
                if r.rows:
                    self._turn_counters[thread_id] = max(int(row[-4]) for row in r.rows)
                else:
                    self._turn_counters[thread_id] = 0

            self._turn_counters[thread_id] += 1
            return self._turn_counters[thread_id]

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
            topics: Explicit topic list. If None, auto-extracted via keyword
                matching. **For production use, pass topics extracted by an LLM.**

        Returns:
            The turn_id assigned to this turn (1-based, sequential per thread,
            continues correctly across process restarts).
        """
        await self.setup()

        turn_id = await self._next_turn_id(thread_id)
        ts = time.time_ns()

        await self.kg.execute(
            f'+memory_turn("{escape_iql(thread_id)}", {turn_id}, '
            f'"{escape_iql(role)}", "{escape_iql(content)}", {ts})'
        )

        if topics is None:
            topics = _extract_topics(content)

        for topic in topics:
            await self.kg.execute(
                f'+memory_topic("{escape_iql(thread_id)}", {turn_id}, "{escape_iql(topic)}")'
            )

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

        - **topics** (list[str]): active topics, sorted alphabetically
        - **recent** (list[dict]): recent turns newest-first,
          each ``{"turn_id", "role", "content"}``
        - **relevant** (dict[str, list[dict]]): turns grouped by topic,
          each turn ``{"turn_id", "role", "content"}``
        - **related_topics** (list[tuple[str, str]]): deduplicated pairs of
          topics that co-occur in this thread
        """
        await self.setup()

        result: dict[str, Any] = {
            "topics": [],
            "recent": [],
            "relevant": {},
            "related_topics": [],
        }

        # Active topics
        r = await self.kg.execute(f'?active_topic("{escape_iql(thread_id)}", Topic)')
        result["topics"] = sorted({str(row[-1]) for row in r.rows})

        # Recent turns sorted by turn_id descending (canonical ordering)
        r = await self.kg.execute(
            f'?memory_turn("{escape_iql(thread_id)}", TurnId, Role, Content, Ts)'
        )
        turns = sorted(r.rows, key=lambda row: int(row[-4]), reverse=True)
        for row in turns[: self.max_recent]:
            result["recent"].append(
                {
                    "turn_id": int(row[-4]),
                    "role": str(row[-3]),
                    "content": str(row[-2]),
                }
            )

        # Relevant turns grouped by topic
        r = await self.kg.execute(
            f'?relevant_turn("{escape_iql(thread_id)}", TurnId, Role, Content, Topic)'
        )
        by_topic: dict[str, list[dict[str, Any]]] = {}
        for row in r.rows:
            topic = str(row[-1])
            turn = {
                "turn_id": int(row[-4]),
                "role": str(row[-3]),
                "content": str(row[-2]),
            }
            by_topic.setdefault(topic, []).append(turn)
        result["relevant"] = by_topic

        # Related topic pairs (deduplicated, order-independent)
        r = await self.kg.execute(
            f'?topic_thread("{escape_iql(thread_id)}", TopicA, TopicB)'
        )
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
        strict: bool = False,
    ) -> Callable[[dict[str, Any]], Coroutine[Any, Any, dict[str, Any]]]:
        """Create a LangGraph node that stores a message from state.

        Reads ``state[thread_key]`` for the thread ID. Reads ``state[state_key]``
        for the message, which must be a ``dict`` with ``"role"`` and ``"content"``
        keys. Logs a warning if the message is not a dict (e.g., a LangChain
        ``AIMessage`` object was passed instead).

        Args:
            state_key: State key for the message dict to store.
            thread_key: State key for the thread ID string.
            strict: If ``True``, raises ``ValueError`` when ``thread_key`` is
                missing from state. If ``False`` (default), logs a warning and
                falls back to ``thread_id="default"``. **Set ``strict=True`` in
                production** to prevent all sessions without a thread ID from
                silently sharing the same memory pool.

        Usage::

            graph.add_node("store", memory.store_node(strict=True))
        """
        memory = self

        async def _node(state: dict[str, Any]) -> dict[str, Any]:
            thread_id = state.get(thread_key)
            if not thread_id:
                if strict:
                    raise ValueError(
                        f"InputLayerMemory.store_node: '{thread_key}' not found in state. "
                        f"Add state['{thread_key}'] = '<conversation-id>' to your graph state, "
                        "or use strict=False to fall back to a shared default thread."
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
        strict: bool = False,
    ) -> Callable[[dict[str, Any]], Coroutine[Any, Any, dict[str, Any]]]:
        """Create a LangGraph node that recalls context into state.

        Reads ``state[thread_key]`` for the thread ID. Writes the recall result
        dict to ``state[state_key]``.

        Args:
            state_key: State key to write the recalled context dict into.
            thread_key: State key for the thread ID string.
            strict: If ``True``, raises ``ValueError`` when ``thread_key`` is
                missing from state. If ``False`` (default), logs a warning and
                falls back to ``thread_id="default"``. **Set ``strict=True`` in
                production** to prevent all sessions without a thread ID from
                silently sharing the same memory pool.

        Usage::

            graph.add_node("recall", memory.recall_node(strict=True))
        """
        memory = self

        async def _node(state: dict[str, Any]) -> dict[str, Any]:
            thread_id = state.get(thread_key)
            if not thread_id:
                if strict:
                    raise ValueError(
                        f"InputLayerMemory.recall_node: '{thread_key}' not found in state. "
                        f"Add state['{thread_key}'] = '<conversation-id>' to your graph state, "
                        "or use strict=False to fall back to a shared default thread."
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
