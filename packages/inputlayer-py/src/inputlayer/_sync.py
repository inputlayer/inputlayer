"""Sync-from-async bridge using a dedicated background event loop thread.

Safe to call from any context: plain scripts, Jupyter notebooks,
inside running event loops (FastAPI, LangGraph), etc.

This is the same pattern used by httpx.Client and playwright.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


class _LoopThread:
    """Background thread owning its own event loop."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def _ensure_running(self) -> asyncio.AbstractEventLoop:
        if self._loop is not None and self._loop.is_running():
            return self._loop
        with self._lock:
            if self._loop is not None and self._loop.is_running():
                return self._loop
            loop = asyncio.new_event_loop()
            thread = threading.Thread(
                target=loop.run_forever,
                daemon=True,
                name="inputlayer-sync",
            )
            thread.start()
            self._loop = loop
            self._thread = thread
            return loop

    def run(self, coro: Coroutine[Any, Any, T]) -> T:
        """Submit a coroutine to the background loop and block until done."""
        loop = self._ensure_running()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result()

    def shutdown(self) -> None:
        """Stop the background loop and join the thread."""
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._loop is not None:
            self._loop.close()
        self._loop = None
        self._thread = None


_default_thread = _LoopThread()


def run_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Run an async coroutine from synchronous code. Always safe.

    Uses a module-level background thread with its own event loop,
    so it works even when the caller already has a running loop
    (Jupyter, FastAPI, LangGraph, etc.).
    """
    return _default_thread.run(coro)
