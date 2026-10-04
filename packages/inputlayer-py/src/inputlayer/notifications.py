"""Dispatchers for what the server pushes and for connection events."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeVar

logger = logging.getLogger(__name__)


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


ConnectionEventType = Literal[
    "disconnected",
    "reconnected",
    "session_reset",
    "notification_gap",
    "closed",
]


@dataclass(frozen=True)
class ConnectionEvent:
    """Something that happened to a knowledge graph's connection.

    - ``disconnected``: the connection dropped; ``code`` is the server's
      closing notice when it sent one. Pending calls failed with
      ``ConnectionLost``.
    - ``reconnected``: a new connection to the same knowledge graph is
      authenticated.
    - ``session_reset``: follows every ``reconnected``. Session rules and
      session facts lived on the old connection and are gone: recreate them.
    - ``notification_gap``: notifications were lost (``code`` is
      ``replay_gap`` when the reconnect cursor could not be honoured, or
      ``notifications_missed`` when the client read too slowly; ``None`` after
      a reconnect that had no cursor to resume from because no notification
      had arrived yet). Re-read the state you track.
    - ``closed``: reconnecting gave up (or was not allowed); the connection
      stays closed.
    """

    type: ConnectionEventType
    knowledge_graph: str | None = None
    code: str | None = None
    message: str | None = None


E = TypeVar("E")
Callback = Callable[[Any], Any]


class _End:
    """Queue sentinel: the iterator raises ``error``, or stops when ``None``."""

    def __init__(self, error: BaseException | None) -> None:
        self.error = error


class _Broadcast(Generic[E]):
    """Fan an event out to filtered callbacks and to every active iterator.

    Iterators get their own queue from the moment they start, so events are
    only buffered for consumers that exist.
    """

    def __init__(self) -> None:
        self._callbacks: list[tuple[Callable[[E], bool], Callback]] = []
        self._queues: set[asyncio.Queue[E | _End]] = set()
        self._background_tasks: set[asyncio.Task[Any]] = set()

    def _add_callback(self, accepts: Callable[[E], bool], callback: Callback) -> None:
        self._callbacks.append((accepts, callback))

    def _publish(self, event: E) -> None:
        for queue in self._queues:
            queue.put_nowait(event)
        for accepts, cb in self._callbacks:
            if not accepts(event):
                continue
            try:
                result = cb(event)
                if asyncio.iscoroutine(result):
                    task = asyncio.ensure_future(result)
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
            except Exception:
                logger.exception("Callback %r raised an exception for event %r", cb, event)

    def fail(self, error: BaseException) -> None:
        """End every active iterator by raising *error* in it."""
        for queue in self._queues:
            queue.put_nowait(_End(error))

    def end(self) -> None:
        """Stop every active iterator after the events already queued."""
        for queue in self._queues:
            queue.put_nowait(_End(None))

    async def __aiter__(self) -> AsyncIterator[E]:
        queue: asyncio.Queue[E | _End] = asyncio.Queue()
        self._queues.add(queue)
        try:
            while True:
                item = await queue.get()
                if isinstance(item, _End):
                    if item.error is not None:
                        raise item.error
                    return
                yield item
        finally:
            self._queues.discard(queue)


# How many recent notifications are remembered to drop the copy that a second
# connection of the same client receives (``kg_change`` reaches every admin
# connection).
_SEEN_WINDOW = 4096


class NotificationDispatcher(_Broadcast[NotificationEvent]):
    """Routes notification events to registered callbacks and iterators.

    One dispatcher serves every connection of a client; a notification that
    arrives on two of them (same stream epoch and ``seq``) is delivered once.
    """

    def __init__(self) -> None:
        super().__init__()
        self._last_seq: int = 0
        self._epoch: str | None = None
        self._seen: set[tuple[str | None, int]] = set()
        self._seen_order: deque[tuple[str | None, int]] = deque()

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
    ) -> Callable[[Callback], Callback] | None:
        """Register a callback for notifications. Can be used as a decorator."""

        def accepts(event: NotificationEvent) -> bool:
            return (
                (event_type is None or event.type == event_type)
                and (relation is None or event.relation == relation)
                and (knowledge_graph is None or event.knowledge_graph == knowledge_graph)
            )

        def decorator(fn: Callback) -> Callback:
            self._add_callback(accepts, fn)
            return fn

        if callback is not None:
            self._add_callback(accepts, callback)
            return None
        return decorator

    def dispatch(self, event: NotificationEvent, *, epoch: str | None = None) -> None:
        """Dispatch a notification to matching callbacks and the iterators."""
        key = (epoch, event.seq)
        if epoch is not None and epoch != self._epoch:
            # The engine restarted: seqs count again from its new run.
            if self._epoch is not None:
                self._last_seq = 0
            self._epoch = epoch
        if epoch is not None:
            if key in self._seen:
                return
            self._seen.add(key)
            self._seen_order.append(key)
            if len(self._seen_order) > _SEEN_WINDOW:
                self._seen.discard(self._seen_order.popleft())
        self._last_seq = max(self._last_seq, event.seq)
        self._publish(event)


class EventDispatcher(_Broadcast[ConnectionEvent]):
    """Connection events (``il.events``): callbacks and async iteration."""

    def on(
        self,
        event_type: ConnectionEventType | None = None,
        *,
        knowledge_graph: str | None = None,
        callback: Callback | None = None,
    ) -> Callable[[Callback], Callback] | None:
        """Register a callback for connection events. Can be used as a decorator."""

        def accepts(event: ConnectionEvent) -> bool:
            return (event_type is None or event.type == event_type) and (
                knowledge_graph is None or event.knowledge_graph == knowledge_graph
            )

        def decorator(fn: Callback) -> Callback:
            self._add_callback(accepts, fn)
            return fn

        if callback is not None:
            self._add_callback(accepts, callback)
            return None
        return decorator

    def emit(self, event: ConnectionEvent) -> None:
        self._publish(event)
