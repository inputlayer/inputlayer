"""Sync wrappers, deletion, and __repr__ for InputLayerMemory.

Separated from memory.py to keep individual files under 500 lines.
Methods are mixed into InputLayerMemory via inheritance.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from inputlayer._sync import run_sync
from inputlayer.integrations.langgraph._utils import b64e, validate_thread_id

logger = logging.getLogger(__name__)


class _MemorySyncAndMaintenanceMixin:
    """Sync wrappers and maintenance ops for InputLayerMemory.

    Attributes and async methods below are declared for mypy; concrete
    values are set by ``InputLayerMemory.__init__`` and its
    method definitions.
    """

    kg: Any
    _turn_counters: dict[str, int]
    _thread_locks: dict[str, asyncio.Lock]
    _active_refcount: dict[str, int]

    async def setup(self) -> None: ...
    async def astore(  # type: ignore[empty-body]
        self,
        thread_id: str,
        role: str,
        content: str,
        *,
        topics: list[str] | None = None,
    ) -> int: ...
    async def arecall(  # type: ignore[empty-body]
        self, thread_id: str
    ) -> dict[str, Any]: ...
    async def _exec(self, iql: str) -> Any: ...

    # ── Sync wrappers ───────────────────────────────────────────────

    def setup_sync(self) -> None:
        """Create memory relations and rules (blocking). See ``setup`` for details."""
        run_sync(self.setup())

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

    def recall(self, thread_id: str) -> dict[str, Any]:
        """Recall derived context for a thread (blocking). See ``arecall`` for details."""
        return run_sync(self.arecall(thread_id))

    # ── Thread deletion ─────────────────────────────────────────────

    async def adelete_thread(self, thread_id: str) -> None:
        """Delete all turns and topics for a thread.

        Removes base facts from ``memory_turn`` and ``memory_topic``.
        Derived relations (``active_topic``, ``relevant_turn``,
        ``topic_thread``) are automatically retracted by the engine.

        Also clears the in-memory turn counter and thread lock for this
        thread so subsequent stores start fresh.
        """
        validate_thread_id(thread_id, "InputLayerMemory.adelete_thread")
        await self.setup()

        tid_b64 = b64e(thread_id)
        await asyncio.gather(
            self._exec(
                f"-memory_turn(ThreadId, TurnId, Role, Content, Ts) <- "
                f'ThreadId = "{tid_b64}"'
            ),
            self._exec(
                f"-memory_topic(ThreadId, TurnId, Topic) <- "
                f'ThreadId = "{tid_b64}"'
            ),
        )

        # Clear in-memory caches for this thread. The refcount may be
        # nonzero if another coroutine is still holding the lock; we do
        # not clear it here, but the guard behavior in _evict and
        # _release handles the stale-entry case safely.
        self._turn_counters.pop(thread_id, None)
        self._thread_locks.pop(thread_id, None)

    def delete_thread(self, thread_id: str) -> None:
        """Delete all turns and topics for a thread (blocking).

        See ``adelete_thread`` for details.
        """
        run_sync(self.adelete_thread(thread_id))

    # ── Repr ────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        kg_name = getattr(self.kg, "name", repr(self.kg))
        max_recent = getattr(self, "max_recent", "?")
        kg_timeout = getattr(self, "_kg_timeout", "?")
        setup_done = getattr(self, "_setup_done", False)
        return (
            f"InputLayerMemory(kg={kg_name!r}, "
            f"max_recent={max_recent}, "
            f"kg_timeout={kg_timeout}, "
            f"setup_done={setup_done}, "
            f"tracked_threads={len(self._turn_counters)})"
        )
