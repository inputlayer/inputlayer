"""Notification dispatcher for push events from the server."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable


@dataclass(frozen=True)
class NotificationEvent:
    """A single notification event from the server."""

    type: str  # persistent_update, rule_change, kg_change, schema_change
    seq: int
    timestamp_ms: int
    session_id: str | None = None
    knowledge_graph: str | None = None
    relation: str | None = None
    operation: str | None = None
    count: int | None = None
    rule_name: str | None = None
    entity: str | None = None


Callback = Callable[[NotificationEvent], Any]


class NotificationDispatcher:
    """Routes notification events to registered callbacks."""

    def __init__(self) -> None:
        self._callbacks: list[tuple[str | None, str | None, str | None, Callback]] = []
        self._queue: asyncio.Queue[NotificationEvent] = asyncio.Queue()
        self._last_seq: int = 0

    @property
    def last_seq(self) -> int:
        return self._last_seq

    def on(
        self,
        event_type: str | None = None,
        *,
        relation: str | None = None,
        knowledge_graph: str | None = None,
        callback: Callback | None = None,
    ) -> Callable | None:
        """Register a callback for notifications. Can be used as a decorator."""
        def decorator(fn: Callback) -> Callback:
            self._callbacks.append((event_type, relation, knowledge_graph, fn))
            return fn

        if callback is not None:
            self._callbacks.append((event_type, relation, knowledge_graph, callback))
            return None
        return decorator

    def dispatch(self, event: NotificationEvent) -> None:
        """Dispatch a notification to matching callbacks and the async queue."""
        self._last_seq = max(self._last_seq, event.seq)
        # Push to async iterator queue
        self._queue.put_nowait(event)
        # Call matching callbacks
        for evt_type, rel, kg, cb in self._callbacks:
            if evt_type is not None and event.type != evt_type:
                continue
            if rel is not None and event.relation != rel:
                continue
            if kg is not None and event.knowledge_graph != kg:
                continue
            try:
                result = cb(event)
                if asyncio.iscoroutine(result):
                    asyncio.ensure_future(result)
            except Exception:
                pass  # Callbacks should not break the dispatcher

    async def __aiter__(self) -> AsyncIterator[NotificationEvent]:
        """Async iterator yielding notification events."""
        while True:
            event = await self._queue.get()
            yield event
