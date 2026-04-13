"""Sync-from-async bridge using a dedicated background event loop thread.

Safe to call from any context: plain scripts, Jupyter notebooks,
inside running event loops (FastAPI, LangGraph), etc.

This is the same pattern used by httpx.Client and playwright.
"""

from __future__ import annotations

import asyncio
import atexit
import logging
import threading
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")

logger = logging.getLogger(__name__)

# Default timeout (seconds) for waiting on coroutine results.
# None means wait indefinitely (matching httpx behavior).
DEFAULT_TIMEOUT: float | None = None


class _LoopThread:
    """Background thread owning its own event loop."""

    def __init__(self, timeout: float | None = DEFAULT_TIMEOUT) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._timeout = timeout

    def _ensure_running(self) -> asyncio.AbstractEventLoop:
        # Fast path: already running (no lock needed for a quick read -
        # the lock below handles the actual creation race).
        loop = self._loop
        if loop is not None and loop.is_running():
            return loop
        with self._lock:
            # Re-check under lock to avoid creating a duplicate.
            if self._loop is not None and self._loop.is_running():
                return self._loop
            new_loop = asyncio.new_event_loop()
            thread = threading.Thread(
                target=new_loop.run_forever,
                daemon=True,
                name="inputlayer-sync",
            )
            thread.start()
            self._loop = new_loop
            self._thread = thread
            return new_loop

    def run(self, coro: Coroutine[Any, Any, T]) -> T:
        """Submit a coroutine to the background loop and block until done."""
        if threading.current_thread() is self._thread:
            raise RuntimeError(
                "Cannot call run_sync from the background event loop thread "
                "- use 'await' instead. This would deadlock."
            )
        loop = self._ensure_running()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result(timeout=self._timeout)

    def shutdown(self) -> None:
        """Stop the background loop and join the thread."""
        with self._lock:
            loop = self._loop
            thread = self._thread
            self._loop = None
            self._thread = None

        if loop is not None and loop.is_running():
            try:
                loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:
                # Loop already closed or stopping - not an error.
                logger.debug("Loop already stopped during shutdown")
        if thread is not None:
            thread.join(timeout=5)
            if thread.is_alive():
                logger.warning(
                    "inputlayer-sync thread did not stop within 5 s; "
                    "it will be cleaned up at process exit (daemon thread)."
                )
        if loop is not None and not loop.is_closed():
            loop.close()


_default_thread = _LoopThread()
atexit.register(_default_thread.shutdown)


def run_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Run an async coroutine from synchronous code. Always safe.

    Uses a module-level background thread with its own event loop,
    so it works even when the caller already has a running loop
    (Jupyter, FastAPI, LangGraph, etc.).
    """
    return _default_thread.run(coro)
