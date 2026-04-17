"""InputLayerMemory: semantic long-term memory for LangGraph agents.

Stores conversation turns as facts in a KG. Rules automatically derive
active topics, relevant context, and conversation threads. Usage::

    memory = InputLayerMemory(kg=kg)
    await memory.setup()
    await memory.astore("thread-1", "user", "I need help with ML in Python")
    context = await memory.arecall("thread-1")
    graph.add_node("recall", memory.recall_node(state_key="context"))
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Callable, Coroutine
from typing import Any

from inputlayer.integrations.langgraph._memory_helpers import (
    extract_topics as _extract_topics,
)
from inputlayer.integrations.langgraph._memory_helpers import (
    make_recall_node as _make_recall_node,
)
from inputlayer.integrations.langgraph._memory_helpers import (
    make_store_node as _make_store_node,
)
from inputlayer.integrations.langgraph._memory_mixin import _MemorySyncAndMaintenanceMixin
from inputlayer.integrations.langgraph._utils import (
    DEFAULT_KG_TIMEOUT,
    check_error_response,
    validate_row_length,
    validate_thread_id,
)
from inputlayer.integrations.langgraph._utils import (
    b64d as _b64d,
)
from inputlayer.integrations.langgraph._utils import (
    b64e as _b64e,
)

logger = logging.getLogger(__name__)


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


class InputLayerMemory(_MemorySyncAndMaintenanceMixin):
    """Semantic memory backed by an InputLayer KnowledgeGraph.

    Stores conversation turns and derived context. Rules compute:

    - **active_topic(ThreadId, Topic)**: topics mentioned in this thread
    - **relevant_turn(ThreadId, TurnId, Role, Content, Topic)**:
      turns that mention an active topic
    - **topic_thread(ThreadId, TopicA, TopicB)**: co-occurring topic pairs

    Thread safety: a single instance can be shared across coroutines.
    ``setup()`` is guarded by a lock; ``astore()`` uses per-thread locks.
    ``max_tracked_threads`` is a soft limit; excess threads are admitted
    to avoid deadlock (with a warning) and evicted once idle.
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
        if max_tracked_threads < 2:
            raise ValueError(
                f"max_tracked_threads must be >= 2, got {max_tracked_threads}. "
                "Set this to the soft upper bound on how many distinct "
                "thread IDs to track in memory (per-thread locks and turn "
                "counters). Values below 2 would evict on every new "
                "thread, defeating the purpose of the cache. Excess "
                "threads are evicted when idle."
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
        # Refcount of in-flight uses per thread. Eviction skips threads
        # with nonzero refcount. A set is not enough because a discard()
        # from one finally block would mark the thread evictable while a
        # second concurrent caller is still using the lock.
        self._active_refcount: dict[str, int] = {}

    # ── Internal: KG execution with timeout ─────────────────────────

    async def _exec(self, iql: str) -> Any:
        """Execute IQL against the KG with a timeout."""
        try:
            result = await asyncio.wait_for(
                self.kg.execute(iql),
                timeout=self._kg_timeout,
            )
        except (asyncio.TimeoutError, TimeoutError):
            raise TimeoutError(
                f"KG operation timed out after {self._kg_timeout}s. "
                f"Query: {iql[:100]}{'...' if len(iql) > 100 else ''}"
            ) from None
        check_error_response(result, "InputLayerMemory", iql)
        return result

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
                "+memory_topic(thread_id: string, turn_id: int, topic: string)",
            ]:
                await self._exec(ddl)

            for rule in [
                "+active_topic(ThreadId, Topic) <- memory_topic(ThreadId, TurnId, Topic)",
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

    # ── Internal: turn counter ───────────────────────────────────────

    async def _acquire_thread_lock(self, thread_id: str) -> asyncio.Lock:
        """Refcount-and-return the per-thread lock.

        Increments the thread's refcount under the guard so eviction
        skips it while any caller holds a reference. The caller MUST
        release via ``_release_thread_lock(thread_id)`` in a finally
        block, even if the lock was never acquired.
        """
        async with self._get_thread_locks_guard():
            if thread_id not in self._thread_locks:
                if len(self._thread_locks) >= self._max_tracked_threads:
                    self._evict_oldest_threads()
                self._thread_locks[thread_id] = asyncio.Lock()
            self._active_refcount[thread_id] = self._active_refcount.get(thread_id, 0) + 1
            return self._thread_locks[thread_id]

    async def _release_thread_lock(self, thread_id: str) -> None:
        """Decrement the thread's refcount. Removes the entry when zero."""
        async with self._get_thread_locks_guard():
            remaining = self._active_refcount.get(thread_id, 0) - 1
            if remaining <= 0:
                self._active_refcount.pop(thread_id, None)
            else:
                self._active_refcount[thread_id] = remaining

    def _evict_oldest_threads(self) -> None:
        """Evict the oldest half of idle tracked threads from caches.

        Skips threads with a nonzero ``_active_refcount``. If all threads
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
            if self._active_refcount.get(key, 0) > 0:
                continue
            self._thread_locks.pop(key, None)
            self._turn_counters.pop(key, None)
            evicted += 1

        if evicted > 0:
            logger.debug(
                "InputLayerMemory: evicted %d idle thread locks (%d remain)",
                evicted,
                len(self._thread_locks),
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
        lock = await self._acquire_thread_lock(thread_id)
        try:
            async with lock:
                if thread_id not in self._turn_counters:
                    r = await self._exec(
                        f'?memory_turn("{_b64e(thread_id)}", TurnId, Role, Content, Ts)'
                    )
                    if r.rows:
                        for row in r.rows:
                            validate_row_length(
                                row,
                                _MIN_TURN_ROW_LEN,
                                "memory_turn",
                                "_next_turn_id",
                            )
                        self._turn_counters[thread_id] = max(int(row[_TURN_ID]) for row in r.rows)
                    else:
                        self._turn_counters[thread_id] = 0

                self._turn_counters[thread_id] += 1
                return self._turn_counters[thread_id]
        finally:
            await self._release_thread_lock(thread_id)

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

        If ``topics`` is None, a simple keyword extractor runs on
        ``content``. For production use, pass explicit topics from
        your own LLM-based extractor.

        Returns the turn_id assigned (1-based, sequential per thread).
        """
        validate_thread_id(thread_id, "InputLayerMemory.astore")
        await self.setup()

        turn_id = await self._next_turn_id(thread_id)
        ts = time.time_ns()

        tid_b64 = _b64e(thread_id)
        await self._exec(
            f'+memory_turn("{tid_b64}", {turn_id}, '
            f'"{_b64e(role)}", "{_b64e(content)}", {ts})'
        )

        if topics is None:
            topics = _extract_topics(content)

        if topics:
            results = await asyncio.gather(
                *(
                    self._exec(f'+memory_topic("{tid_b64}", {turn_id}, "{_b64e(topic)}")')
                    for topic in set(topics)
                ),
                return_exceptions=True,
            )

            errors = [r for r in results if isinstance(r, BaseException)]
            if errors:
                logger.error(
                    "InputLayerMemory.astore: %d/%d topic inserts failed for thread=%r turn=%d",
                    len(errors),
                    len(topics),
                    thread_id,
                    turn_id,
                )
                raise RuntimeError(
                    f"InputLayerMemory.astore: {len(errors)}/{len(topics)} "
                    f"topic inserts failed for thread={thread_id!r} turn={turn_id}. "
                    f"First error: {errors[0]}"
                ) from errors[0]

        return turn_id

    # ── Recall ───────────────────────────────────────────────────────

    async def arecall(self, thread_id: str) -> dict[str, Any]:
        """Recall derived context for a thread.

        Returns a dict with four keys:

        - **topics**: ``list[str]`` sorted active topics in this thread.
        - **recent**: ``list[dict]`` with ``turn_id``, ``role``,
          ``content`` keys, newest first, capped at ``max_recent``.
        - **relevant**: ``dict[str, list[dict]]`` turns grouped by topic.
        - **related_topics**: ``list[tuple[str, str]]`` deduplicated
          co-occurring topic pairs.

        All four queries run concurrently; if any fails, a RuntimeError
        is raised after all complete.
        """
        validate_thread_id(thread_id, "InputLayerMemory.arecall")
        await self.setup()

        tid_b64 = _b64e(thread_id)

        results = await asyncio.gather(
            self._exec(f'?active_topic("{tid_b64}", Topic)'),
            self._exec(
                f'?memory_turn("{tid_b64}", TurnId, Role, Content, Ts)',
            ),
            self._exec(
                f'?relevant_turn("{tid_b64}", TurnId, Role, Content, Topic)',
            ),
            self._exec(f'?topic_thread("{tid_b64}", TopicA, TopicB)'),
            return_exceptions=True,
        )

        errors = [r for r in results if isinstance(r, BaseException)]
        if errors:
            query_names = ["active_topic", "memory_turn", "relevant_turn", "topic_thread"]
            failed_names = [
                query_names[i] for i, r in enumerate(results) if isinstance(r, BaseException)
            ]
            raise RuntimeError(
                f"InputLayerMemory.arecall: {len(errors)}/4 queries failed "
                f"for thread={thread_id!r} (failed: {', '.join(failed_names)}). "
                f"First error: {errors[0]}"
            ) from errors[0]

        # Narrow from Any|BaseException to the actual result type
        r_topics: Any = results[0]
        r_turns: Any = results[1]
        r_relevant: Any = results[2]
        r_related: Any = results[3]

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
            r_turns.rows,
            key=lambda row: int(row[_TURN_ID]),
            reverse=True,
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
