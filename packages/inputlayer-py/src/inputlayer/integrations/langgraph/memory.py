"""InputLayerMemory: semantic long-term memory for LangGraph agents.

Stores conversation turns as facts in a KG. Rules automatically
derive active topics, relevant context, and conversation threads.

Usage::

    from inputlayer.integrations.langgraph import InputLayerMemory

    memory = InputLayerMemory(kg=kg)
    await memory.setup()

    await memory.astore("thread-1", "user", "I need help with ML in Python")
    await memory.astore("thread-1", "user", "...", topics=["ml", "python"])

    context = await memory.arecall("thread-1")

    graph.add_node("recall", memory.recall_node(state_key="context"))
    graph.add_node("store", memory.store_node(state_key="new_message"))
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import logging
import threading
import time
from collections.abc import Callable, Coroutine
from typing import Any

from inputlayer._sync import run_sync
from inputlayer.integrations.langgraph._memory_helpers import (
    extract_topics as _extract_topics,
)
from inputlayer.integrations.langgraph._memory_helpers import (
    make_recall_node as _make_recall_node,
)
from inputlayer.integrations.langgraph._memory_helpers import (
    make_store_node as _make_store_node,
)
from inputlayer.integrations.langgraph._utils import (
    DEFAULT_KG_TIMEOUT,
    escape_iql,
    validate_row_length,
)

logger = logging.getLogger(__name__)


def _b64e(s: str) -> str:
    """Encode a string as base64 for safe IQL string storage."""
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


def _b64d(s: str) -> str:
    """Decode a base64-encoded string back to the original."""
    try:
        return base64.b64decode(s.encode("ascii")).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError(
            f"Failed to decode base64 memory data: {s[:40]!r}"
        ) from exc


# ── Column indices for memory query results ─────────────────────────
# memory_turn(thread_id, turn_id, role, content, ts)
_TURN_ID = -4
_TURN_ROLE = -3
_TURN_CONTENT = -2
_TURN_TS = -1
_MIN_TURN_ROW_LEN = 4

# relevant_turn(thread_id, turn_id, role, content, topic)
_REL_TURN_ID = -4
_REL_ROLE = -3
_REL_CONTENT = -2
_REL_TOPIC = -1
_MIN_REL_ROW_LEN = 4

# active_topic(thread_id, topic)
_TOPIC_VAL = -1
_MIN_TOPIC_ROW_LEN = 1

# topic_thread(thread_id, topic_a, topic_b)
_TOPIC_A = -2
_TOPIC_B = -1
_MIN_TOPIC_THREAD_ROW_LEN = 2


class InputLayerMemory:
    """Semantic memory backed by an InputLayer KnowledgeGraph.

    Stores conversation turns and derived context. Rules compute:

    - **active_topic(ThreadId, Topic)**: topics mentioned in this thread
    - **relevant_turn(ThreadId, TurnId, Role, Content, Topic)**:
      turns that mention an active topic
    - **topic_thread(ThreadId, TopicA, TopicB)**: co-occurring topic pairs

    Thread safety: a single instance can be shared across coroutines.
    ``setup()`` is guarded by a lock; ``astore()`` uses per-thread locks.

    ``max_tracked_threads`` is a **soft limit**. If all tracked threads
    are currently active (holding their per-thread lock), a new thread
    is still admitted to avoid deadlock, and a warning is logged. The
    excess entries are evicted once threads become idle.
    """

    def __init__(
        self,
        kg: Any,
        *,
        max_recent: int = 10,
        max_tracked_threads: int = 10_000,
        kg_timeout: float = DEFAULT_KG_TIMEOUT,
    ) -> None:
        if max_recent < 1:
            raise ValueError(
                f"max_recent must be >= 1, got {max_recent}. "
                "Set max_recent to the number of recent turns to include in recall."
            )
        if max_tracked_threads < 1:
            raise ValueError(
                f"max_tracked_threads must be >= 1, got {max_tracked_threads}."
            )
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
        # Track which thread locks are currently held so eviction skips them
        self._active_threads: set[str] = set()

    # ── Internal: KG execution with timeout ─────────────────────────

    async def _exec(self, iql: str) -> Any:
        """Execute IQL against the KG with a timeout."""
        try:
            return await asyncio.wait_for(
                self.kg.execute(iql),
                timeout=self._kg_timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"KG operation timed out after {self._kg_timeout}s. "
                f"Query: {iql[:100]}{'...' if len(iql) > 100 else ''}"
            ) from None

    # ── Setup ────────────────────────────────────────────────────────

    def _get_setup_lock(self) -> asyncio.Lock:
        with self._setup_lock_guard:
            if self._setup_lock is None:
                self._setup_lock = asyncio.Lock()
            return self._setup_lock

    def _get_thread_locks_guard(self) -> asyncio.Lock:
        with self._thread_locks_guard_sync:
            if self._thread_locks_guard is None:
                self._thread_locks_guard = asyncio.Lock()
            return self._thread_locks_guard

    async def setup(self) -> None:
        """Create memory relations and rules (idempotent, concurrency-safe)."""
        if self._setup_done:
            return

        async with self._get_setup_lock():
            if self._setup_done:
                return

            logger.debug("InputLayerMemory: creating memory relations and rules")

            for ddl in [
                "+memory_turn(thread_id: string, turn_id: int, "
                "role: string, content: string, ts: int)",
                "+memory_topic(thread_id: string, turn_id: int, "
                "topic: string)",
            ]:
                await self._exec(ddl)

            for rule in [
                "+active_topic(ThreadId, Topic) <- "
                "memory_topic(ThreadId, TurnId, Topic)",
                "+relevant_turn(ThreadId, TurnId, Role, Content, Topic) <- "
                "memory_turn(ThreadId, TurnId, Role, Content, Ts), "
                "memory_topic(ThreadId, TurnId, Topic)",
                "+topic_thread(ThreadId, TopicA, TopicB) <- "
                "memory_topic(ThreadId, TurnIdA, TopicA), "
                "memory_topic(ThreadId, TurnIdB, TopicB), "
                "TopicA != TopicB",
            ]:
                await self._exec(rule)

            self._setup_done = True
            logger.debug("InputLayerMemory: setup complete")

    def setup_sync(self) -> None:
        """Create memory relations and rules (blocking). See ``setup`` for details."""
        run_sync(self.setup())

    def __repr__(self) -> str:
        kg_name = getattr(self.kg, "name", repr(self.kg))
        return (
            f"InputLayerMemory(kg={kg_name!r}, "
            f"max_recent={self.max_recent}, "
            f"kg_timeout={self._kg_timeout}, "
            f"setup_done={self._setup_done}, "
            f"tracked_threads={len(self._turn_counters)})"
        )

    # ── Internal: turn counter ───────────────────────────────────────

    async def _get_thread_lock(self, thread_id: str) -> asyncio.Lock:
        """Get or create a per-thread lock, with eviction of idle entries.

        Marks the thread as active while the guard lock is held so that
        eviction cannot remove this thread between returning and the
        caller acquiring the per-thread lock.
        """
        async with self._get_thread_locks_guard():
            if thread_id not in self._thread_locks:
                if len(self._thread_locks) >= self._max_tracked_threads:
                    self._evict_oldest_threads()
                self._thread_locks[thread_id] = asyncio.Lock()
            # Mark active while guard is held to prevent eviction race
            self._active_threads.add(thread_id)
            return self._thread_locks[thread_id]

    def _evict_oldest_threads(self) -> None:
        """Evict the oldest half of idle tracked threads from caches.

        Skips threads currently in ``_active_threads``. If all threads
        are active, no eviction occurs (the new thread is still added).
        """
        keep = self._max_tracked_threads // 2
        evict_target = len(self._thread_locks) - keep
        if evict_target <= 0:
            return
        evicted = 0
        # Iterate oldest-first (dict preserves insertion order)
        keys = list(self._thread_locks.keys())
        for key in keys:
            if evicted >= evict_target:
                break
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
        elif evict_target > 0:
            logger.warning(
                "InputLayerMemory: eviction requested but all %d threads "
                "are active (soft limit %d exceeded). Excess entries will "
                "be evicted once threads become idle. Consider increasing "
                "max_tracked_threads.",
                len(self._thread_locks),
                self._max_tracked_threads,
            )

    async def _next_turn_id(self, thread_id: str) -> int:
        """Return the next turn_id, initializing from KG if needed."""
        lock = await self._get_thread_lock(thread_id)
        try:
            async with lock:
                if thread_id not in self._turn_counters:
                    r = await self._exec(
                        f'?memory_turn("{escape_iql(thread_id)}", '
                        f"TurnId, Role, Content, Ts)"
                    )
                    if r.rows:
                        for row in r.rows:
                            validate_row_length(
                                row, _MIN_TURN_ROW_LEN, "memory_turn", "_next_turn_id",
                            )
                        self._turn_counters[thread_id] = max(
                            int(row[_TURN_ID]) for row in r.rows
                        )
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

        Returns the turn_id assigned (1-based, sequential per thread).
        """
        await self.setup()

        turn_id = await self._next_turn_id(thread_id)
        ts = time.time_ns()

        await self._exec(
            f'+memory_turn("{escape_iql(thread_id)}", {turn_id}, '
            f'"{_b64e(role)}", "{_b64e(content)}", {ts})'
        )

        if topics is None:
            topics = _extract_topics(content)

        if topics:
            escaped_tid = escape_iql(thread_id)
            results = await asyncio.gather(
                *(
                    self._exec(
                        f'+memory_topic("{escaped_tid}", '
                        f'{turn_id}, "{_b64e(topic)}")'
                    )
                    for topic in set(topics)
                ),
                return_exceptions=True,
            )

            errors = [r for r in results if isinstance(r, BaseException)]
            if errors:
                logger.error(
                    "InputLayerMemory.astore: %d/%d topic inserts failed "
                    "for thread=%r turn=%d",
                    len(errors), len(topics), thread_id, turn_id,
                )
                raise RuntimeError(
                    f"astore: {len(errors)}/{len(topics)} topic inserts "
                    f"failed. First error: {errors[0]}"
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
        """Store a conversation turn (blocking). See ``astore`` for details."""
        return run_sync(self.astore(thread_id, role, content, topics=topics))

    # ── Recall ───────────────────────────────────────────────────────

    async def arecall(self, thread_id: str) -> dict[str, Any]:
        """Recall derived context for a thread.

        Returns a dict with: topics, recent, relevant, related_topics.
        All four queries run concurrently; if any fails, a RuntimeError
        is raised after all complete.
        """
        await self.setup()

        escaped = escape_iql(thread_id)

        results = await asyncio.gather(
            self._exec(f'?active_topic("{escaped}", Topic)'),
            self._exec(
                f'?memory_turn("{escaped}", TurnId, Role, Content, Ts)',
            ),
            self._exec(
                f'?relevant_turn("{escaped}", TurnId, Role, Content, Topic)',
            ),
            self._exec(f'?topic_thread("{escaped}", TopicA, TopicB)'),
            return_exceptions=True,
        )

        errors = [r for r in results if isinstance(r, BaseException)]
        if errors:
            raise RuntimeError(
                f"arecall: {len(errors)}/4 queries failed. "
                f"First error: {errors[0]}"
            ) from errors[0]

        r_topics, r_turns, r_relevant, r_related = results

        for row in r_topics.rows:
            validate_row_length(row, _MIN_TOPIC_ROW_LEN, "active_topic", "arecall")
        for row in r_turns.rows:
            validate_row_length(row, _MIN_TURN_ROW_LEN, "memory_turn", "arecall")
        for row in r_relevant.rows:
            validate_row_length(row, _MIN_REL_ROW_LEN, "relevant_turn", "arecall")
        for row in r_related.rows:
            validate_row_length(row, _MIN_TOPIC_THREAD_ROW_LEN, "topic_thread", "arecall")

        result: dict[str, Any] = {}

        result["topics"] = sorted(
            {_b64d(str(row[_TOPIC_VAL])) for row in r_topics.rows},
        )

        turns = sorted(
            r_turns.rows, key=lambda row: int(row[_TURN_ID]), reverse=True,
        )
        result["recent"] = [
            {
                "turn_id": int(row[_TURN_ID]),
                "role": _b64d(str(row[_TURN_ROLE])),
                "content": _b64d(str(row[_TURN_CONTENT])),
            }
            for row in turns[: self.max_recent]
        ]

        by_topic: dict[str, list[dict[str, Any]]] = {}
        for row in r_relevant.rows:
            topic = _b64d(str(row[_REL_TOPIC]))
            turn = {
                "turn_id": int(row[_REL_TURN_ID]),
                "role": _b64d(str(row[_REL_ROLE])),
                "content": _b64d(str(row[_REL_CONTENT])),
            }
            by_topic.setdefault(topic, []).append(turn)
        result["relevant"] = by_topic

        seen: set[tuple[str, str]] = set()
        related: list[tuple[str, str]] = []
        for row in r_related.rows:
            a, b = sorted([_b64d(str(row[_TOPIC_A])), _b64d(str(row[_TOPIC_B]))])
            pair = (a, b)
            if pair not in seen:
                seen.add(pair)
                related.append(pair)
        result["related_topics"] = related

        return result

    def recall(self, thread_id: str) -> dict[str, Any]:
        """Recall derived context for a thread (blocking). See ``arecall`` for details."""
        return run_sync(self.arecall(thread_id))

    # ── LangGraph node factories ─────────────────────────────────────

    def store_node(
        self,
        *,
        state_key: str = "new_message",
        thread_key: str = "thread_id",
        strict: bool = True,
    ) -> Callable[[dict[str, Any]], Coroutine[Any, Any, dict[str, Any]]]:
        """Create a LangGraph node that stores a message from state.

        Args:
            state_key: State key for the message dict to store.
            thread_key: State key for the thread ID string.
            strict: If True (default), raises on missing thread_id or
                non-dict messages. Set to False to fall back to a shared
                'default' thread (not recommended for production).
        """
        return _make_store_node(
            self,
            state_key=state_key,
            thread_key=thread_key,
            strict=strict,
        )

    def recall_node(
        self,
        *,
        state_key: str = "context",
        thread_key: str = "thread_id",
        strict: bool = True,
    ) -> Callable[[dict[str, Any]], Coroutine[Any, Any, dict[str, Any]]]:
        """Create a LangGraph node that recalls context into state.

        Args:
            state_key: State key to write the recalled context dict into.
            thread_key: State key for the thread ID string.
            strict: If True (default), raises on missing thread_id.
                Set to False to fall back to a shared 'default' thread
                (not recommended for production).
        """
        return _make_recall_node(
            self,
            state_key=state_key,
            thread_key=thread_key,
            strict=strict,
        )
