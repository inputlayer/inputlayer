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
import base64
import logging
import threading
import time
from collections.abc import Callable, Coroutine
from typing import Any

from inputlayer._sync import run_sync
from inputlayer.integrations.langgraph._utils import escape_iql

logger = logging.getLogger(__name__)

# Default timeout for KG operations (seconds).
_DEFAULT_KG_TIMEOUT = 30.0


def _b64e(s: str) -> str:
    """Encode a string as base64 for safe IQL string storage.

    The IQL parser does not unescape string literals (``\\"`` stays as
    two characters, not a single quote). Base64 avoids this entirely:
    the encoded value contains only alphanumeric chars, ``+``, ``/``,
    and ``=``, which need no escaping.
    """
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


def _b64d(s: str) -> str:
    """Decode a base64-encoded string back to the original."""
    return base64.b64decode(s.encode("ascii")).decode("utf-8")


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


# ── Column indices for memory query results ─────────────────────────
# memory_turn(thread_id, turn_id, role, content, ts)
# When thread_id is bound, remaining: TurnId, Role, Content, Ts
_TURN_ID = -4      # TurnId
_TURN_ROLE = -3    # Role
_TURN_CONTENT = -2 # Content
_TURN_TS = -1      # Ts

# relevant_turn(thread_id, turn_id, role, content, topic)
_REL_TURN_ID = -4
_REL_ROLE = -3
_REL_CONTENT = -2
_REL_TOPIC = -1

# active_topic(thread_id, topic)
_TOPIC_VAL = -1

# topic_thread(thread_id, topic_a, topic_b)
_TOPIC_A = -2
_TOPIC_B = -1


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
    to ensure turn IDs are sequential within each thread without blocking
    unrelated threads.

    Process restart safety: the turn counter is initialized from the KG
    on the first store for each thread, so IDs continue correctly after
    a restart rather than resetting to 1.

    Args:
        kg: An InputLayer KnowledgeGraph handle.
        max_recent: Number of recent turns to include in recall (default 10).
        max_tracked_threads: Maximum number of thread IDs to keep in the
            in-memory turn counter and lock caches. When exceeded, the
            oldest idle entries are evicted (the KG is the source of truth,
            so evicted threads simply re-initialize on next access).
            Default 10_000.
        kg_timeout: Timeout in seconds for individual KG operations
            (default 30.0). Prevents indefinite hangs.
    """

    def __init__(
        self,
        kg: Any,
        *,
        max_recent: int = 10,
        max_tracked_threads: int = 10_000,
        kg_timeout: float = _DEFAULT_KG_TIMEOUT,
    ) -> None:
        self.kg = kg
        self.max_recent = max_recent
        self._max_tracked_threads = max_tracked_threads
        self._kg_timeout = kg_timeout
        self._setup_done = False
        self._setup_lock_guard = threading.Lock()
        self._setup_lock: asyncio.Lock | None = None
        self._turn_counters: dict[str, int] = {}
        self._thread_locks: dict[str, asyncio.Lock] = {}
        self._thread_locks_guard_sync = threading.Lock()
        self._thread_locks_guard: asyncio.Lock | None = None
        # Track which thread locks are currently held, so eviction skips them
        self._active_threads: set[str] = set()

    # ── Internal: KG execution with timeout ─────────────────────────

    async def _exec(self, iql: str) -> Any:
        """Execute IQL against the KG with a timeout."""
        return await asyncio.wait_for(
            self.kg.execute(iql),
            timeout=self._kg_timeout,
        )

    # ── Setup ────────────────────────────────────────────────────────

    def _get_setup_lock(self) -> asyncio.Lock:
        """Get or create the setup lock, guarded by a threading.Lock for safety."""
        with self._setup_lock_guard:
            if self._setup_lock is None:
                self._setup_lock = asyncio.Lock()
            return self._setup_lock

    def _get_thread_locks_guard(self) -> asyncio.Lock:
        """Get or create the thread-locks guard, guarded by a threading.Lock for safety."""
        with self._thread_locks_guard_sync:
            if self._thread_locks_guard is None:
                self._thread_locks_guard = asyncio.Lock()
            return self._thread_locks_guard

    async def setup(self) -> None:
        """Create memory relations and rules (idempotent, concurrency-safe).

        Safe to call from multiple coroutines simultaneously. The first
        caller runs the DDL; subsequent callers return immediately.
        Ignores "already exists" errors so re-running setup is harmless.
        """
        if self._setup_done:
            return

        async with self._get_setup_lock():
            if self._setup_done:  # re-check after acquiring lock
                return

            logger.debug("InputLayerMemory: creating memory relations and rules")

            # Run each DDL/rule and track whether anything raised a real error.
            # Server-side "already exists" responses come back as ResultSet rows,
            # not exceptions. Exceptions here mean connection/auth problems.
            # If any step raises, don't mark setup as done so the next call retries.
            for ddl in [
                "+memory_turn("
                "thread_id: string, turn_id: int, role: string, content: string, ts: int)",
                "+memory_topic(thread_id: string, turn_id: int, topic: string)",
            ]:
                await self._exec(ddl)

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
                await self._exec(rule)

            # Only mark done after ALL steps complete. If the server was down,
            # the exception propagated above and this line is never reached,
            # so the next call will retry the full setup.
            self._setup_done = True
            logger.debug("InputLayerMemory: setup complete")

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

    async def _get_thread_lock(self, thread_id: str) -> asyncio.Lock:
        """Get or create a per-thread lock (creating is guarded by a guard lock).

        When the number of tracked threads exceeds ``max_tracked_threads``,
        the oldest idle entries are evicted. Evicted threads re-initialize
        their turn counter from the KG on next access.

        Active threads (currently holding their lock) are never evicted,
        preventing orphaned waiters.
        """
        async with self._get_thread_locks_guard():
            if thread_id not in self._thread_locks:
                if len(self._thread_locks) >= self._max_tracked_threads:
                    self._evict_oldest_threads()
                self._thread_locks[thread_id] = asyncio.Lock()
            return self._thread_locks[thread_id]

    def _evict_oldest_threads(self) -> None:
        """Evict the oldest half of idle tracked threads from caches.

        Uses dict insertion order (Python 3.7+) as a proxy for age.
        Threads that are currently active (holding their lock via astore)
        are skipped to prevent orphaned lock waiters.

        The KG remains the source of truth, so evicted threads simply
        re-query on next access.
        """
        keep = self._max_tracked_threads // 2
        evict_target = len(self._thread_locks) - keep
        evicted = 0
        keys = list(self._thread_locks.keys())
        for key in keys:
            if evicted >= evict_target:
                break
            # Never evict a thread that is currently active
            if key in self._active_threads:
                continue
            self._thread_locks.pop(key, None)
            self._turn_counters.pop(key, None)
            evicted += 1

        if evicted > 0:
            logger.debug(
                "InputLayerMemory: evicted %d idle thread locks (%d remain)",
                evicted, len(self._thread_locks),
            )

    async def _next_turn_id(self, thread_id: str) -> int:
        """Return the next turn_id for a thread, initializing from the KG if needed.

        Initializes the counter from the stored max turn_id on the first call
        for each thread, so the counter resumes correctly after a process restart.

        The per-thread lock ensures that concurrent calls for the same thread
        are serialized, preventing duplicate turn IDs even during counter
        initialization from the KG.
        """
        lock = await self._get_thread_lock(thread_id)
        async with lock:
            # Mark this thread as active so eviction skips it
            self._active_threads.add(thread_id)
            try:
                if thread_id not in self._turn_counters:
                    # Query the KG for the current max turn_id for this thread
                    r = await self._exec(
                        f'?memory_turn("{escape_iql(thread_id)}", TurnId, Role, Content, Ts)'
                    )
                    if r.rows:
                        self._turn_counters[thread_id] = max(int(row[_TURN_ID]) for row in r.rows)
                    else:
                        self._turn_counters[thread_id] = 0

                self._turn_counters[thread_id] += 1
                return self._turn_counters[thread_id]
            finally:
                self._active_threads.discard(thread_id)

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

        # Content and role are base64-encoded because the IQL parser does not
        # unescape string literals (e.g. \" stays as two characters). Base64
        # ensures lossless round-trip for arbitrary content.
        await self._exec(
            f'+memory_turn("{escape_iql(thread_id)}", {turn_id}, '
            f'"{_b64e(role)}", "{_b64e(content)}", {ts})'
        )

        if topics is None:
            topics = _extract_topics(content)

        if topics:
            # Submit all topic inserts concurrently. Use return_exceptions=True
            # so a single failure doesn't cancel the rest.
            results = await asyncio.gather(
                *(
                    self._exec(
                        f'+memory_topic("{escape_iql(thread_id)}", {turn_id}, "{escape_iql(topic)}")'
                    )
                    for topic in topics
                ),
                return_exceptions=True,
            )

            errors = [r for r in results if isinstance(r, BaseException)]
            if errors:
                logger.error(
                    "InputLayerMemory.astore: %d/%d topic inserts failed for "
                    "thread=%r turn=%d",
                    len(errors), len(topics), thread_id, turn_id,
                )
                raise RuntimeError(
                    f"astore: {len(errors)}/{len(topics)} topic inserts failed. "
                    f"First error: {errors[0]}"
                ) from errors[0]

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

        escaped = escape_iql(thread_id)

        # Run all four independent queries concurrently. These are read-only
        # so partial failure is acceptable (the exception propagates).
        r_topics, r_turns, r_relevant, r_related = await asyncio.gather(
            self._exec(f'?active_topic("{escaped}", Topic)'),
            self._exec(f'?memory_turn("{escaped}", TurnId, Role, Content, Ts)'),
            self._exec(f'?relevant_turn("{escaped}", TurnId, Role, Content, Topic)'),
            self._exec(f'?topic_thread("{escaped}", TopicA, TopicB)'),
        )

        result: dict[str, Any] = {}

        # Active topics
        result["topics"] = sorted({str(row[_TOPIC_VAL]) for row in r_topics.rows})

        # Recent turns sorted by turn_id descending (canonical ordering)
        turns = sorted(r_turns.rows, key=lambda row: int(row[_TURN_ID]), reverse=True)
        result["recent"] = [
            {
                "turn_id": int(row[_TURN_ID]),
                "role": _b64d(str(row[_TURN_ROLE])),
                "content": _b64d(str(row[_TURN_CONTENT])),
            }
            for row in turns[: self.max_recent]
        ]

        # Relevant turns grouped by topic
        by_topic: dict[str, list[dict[str, Any]]] = {}
        for row in r_relevant.rows:
            topic = str(row[_REL_TOPIC])
            turn = {
                "turn_id": int(row[_REL_TURN_ID]),
                "role": _b64d(str(row[_REL_ROLE])),
                "content": _b64d(str(row[_REL_CONTENT])),
            }
            by_topic.setdefault(topic, []).append(turn)
        result["relevant"] = by_topic

        # Related topic pairs (deduplicated, order-independent)
        seen: set[tuple[str, str]] = set()
        related: list[tuple[str, str]] = []
        for row in r_related.rows:
            a, b = sorted([str(row[_TOPIC_A]), str(row[_TOPIC_B])])
            pair = (a, b)
            if pair not in seen:
                seen.add(pair)
                related.append(pair)
        result["related_topics"] = related

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
