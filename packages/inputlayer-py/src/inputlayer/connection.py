"""The connection core: one WebSocket to one knowledge graph, read by one task.

A single reader task reads every frame the server sends and routes it:

- replies (they echo the request's ``id``) complete the call waiting on that
  id; a streamed result (``result_start``, ``result_chunk``s, ``result_end``)
  is assembled there and checked against the counts its end announces;
- standing-query pushes go to the route registered for their
  ``subscription``, and only when their ``generation`` is the route's; any
  other push is dropped and counted;
- notifications go to the notification dispatcher, and notices are logged and
  remembered so a closing one names why pending calls failed.

Because the reader always runs, notifications and deltas arrive while no call
is in flight. Requests are pipelined: every one carries an id (``r<n>``), at
most ``max_in_flight`` are unanswered at once (one under the server's bound,
so a ``cancel`` can always be read), and each runs under a deadline the server
enforces (``timeout_ms``) with a local backstop that sends ``cancel``. A
dropped connection fails every pending call with ``ConnectionLost`` and, when
allowed, reconnects with backoff to the same knowledge graph, resuming the
notification stream from the last ``seq`` seen.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import quote

import websockets
from websockets.asyncio.client import ClientConnection

from inputlayer._protocol import (
    AuthenticatedResponse,
    AuthenticateMessage,
    AuthErrorResponse,
    CancelAckResponse,
    CancelMessage,
    ErrorResponse,
    ExecuteMessage,
    LoginMessage,
    NoticeResponse,
    NotificationResponse,
    PingMessage,
    PongResponse,
    ResultChunkResponse,
    ResultEndResponse,
    ResultResponse,
    ResultStartResponse,
    ServerMessage,
    SubscriptionDeltaChunkResponse,
    SubscriptionDeltaEndResponse,
    SubscriptionDeltaResponse,
    SubscriptionDeltaStartResponse,
    SubscriptionErrorResponse,
    SubscriptionPush,
    SubscriptionResetResponse,
    deserialize_message,
)
from inputlayer.exceptions import (
    AuthenticationError,
    Cancelled,
    ConnectionError,
    ConnectionLost,
    DeadlineExceeded,
    InternalError,
    OutcomeUnknownError,
    ProtocolError,
    QueryError,
    RateLimited,
    StatementFailedError,
    StoreReadOnlyError,
)
from inputlayer.notifications import (
    ConnectionEvent,
    EventDispatcher,
    NotificationDispatcher,
    NotificationEvent,
)

logger = logging.getLogger("inputlayer")

_SUBSCRIPTION_PUSHES = (
    SubscriptionDeltaResponse,
    SubscriptionDeltaStartResponse,
    SubscriptionDeltaChunkResponse,
    SubscriptionDeltaEndResponse,
    SubscriptionErrorResponse,
    SubscriptionResetResponse,
)

# The server's default ``http.rate_limit.ws_max_in_flight_requests`` is 16;
# keeping one slot free lets a ``cancel`` through when every other is taken.
DEFAULT_MAX_IN_FLIGHT = 15

# How long past a request's deadline the SDK waits for the server's own
# ``deadline_exceeded`` before sending ``cancel``, and then for any reply.
DEFAULT_DEADLINE_GRACE = 1.0

# The rate limit is per second; a refused request is retried once after it.
_RATE_LIMIT_WINDOW = 1.0

# Notices after which reconnecting cannot succeed with the same credential.
_FINAL_NOTICES = frozenset({"credential_revoked", "credential_expired"})

_State = Literal["idle", "connecting", "open", "reconnecting", "closed"]
_Kind = Literal["execute", "ping", "cancel"]

PushSink = Callable[[SubscriptionPush], None]
ReconnectHook = Callable[["Connection"], Awaitable[None]]

LinkChange = Literal["lost", "given_up", "closed"]
"""What happened to a connection's link: it dropped (a reconnect may follow),
reconnecting gave up or was not allowed, or the client closed it."""
LinkListener = Callable[[LinkChange, "str | None"], None]
"""Called with what happened and the server's closing notice code, if any."""


@dataclass
class SubscriptionRoute:
    """Where pushes of one subscription go.

    Register it before sending ``.subscribe``: the reader sets ``generation``
    from the ``.subscribe`` reply before it reads the next frame. Pushes that
    arrive before the reply are held and then filtered by it, so no push of
    the new generation is dropped. Pushes of any other generation are stale;
    they are dropped and counted in ``stale``.
    """

    subscription: str
    sink: PushSink
    generation: int | None = None
    stale: int = 0
    early: list[SubscriptionPush] = field(default_factory=list)


@dataclass
class _Stream:
    """A streamed result being assembled."""

    start: ResultStartResponse
    rows: list[list[Any]] = field(default_factory=list)
    provenance: list[str] = field(default_factory=list)
    chunks: int = 0


@dataclass
class _Pending:
    """A request awaiting its reply."""

    id: str
    kind: _Kind
    future: asyncio.Future[Any]
    program: str | None = None
    holds_slot: bool = False
    stream: _Stream | None = None
    # The caller stopped waiting (local deadline or cancellation): its reply,
    # when it comes, is read and discarded.
    abandoned: bool = False
    # Answered by a cancel_ack: the target of a cancel this request carries.
    target: str | None = None


class _Slots:
    """A FIFO bound on unanswered requests that never leaks a slot when a
    waiter times out or is cancelled at the moment it is granted one."""

    def __init__(self, size: int) -> None:
        self._free = size
        self._waiters: list[asyncio.Future[None]] = []

    async def acquire(self, timeout: float | None) -> None:
        if self._free > 0 and not self._waiters:
            self._free -= 1
            return
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        try:
            await asyncio.wait_for(asyncio.shield(waiter), timeout)
        except BaseException:
            if waiter.done() and not waiter.cancelled():
                self.release()
            else:
                waiter.cancel()
                with contextlib.suppress(ValueError):
                    self._waiters.remove(waiter)
            raise

    def release(self) -> None:
        while self._waiters:
            waiter = self._waiters.pop(0)
            if not waiter.done():
                waiter.set_result(None)
                return
        self._free += 1


def _may_write(program: str) -> bool:
    """Whether *program* could commit anything: all but pure ``?`` queries."""
    statements = [line.strip() for line in program.splitlines() if line.strip()]
    return not statements or not all(s.startswith("?") for s in statements)


class Connection:
    """One authenticated WebSocket bound to one knowledge graph.

    ``initial_kg`` is the knowledge graph the connection binds with
    ``?kg=<name>`` (the server's default when ``None``); a reconnect binds the
    knowledge graph the connection was on when it dropped. ``lazy=True`` opens
    the connection on the first request instead of requiring ``connect()``.
    ``create_kg`` is called when the bound knowledge graph is missing at first
    connect; when it returns ``True`` the connect is retried once.
    ``ends_notifications=False`` keeps this connection's giving up from ending
    the iterators of a ``dispatcher`` that other connections also feed.
    """

    def __init__(
        self,
        url: str,
        *,
        username: str | None = None,
        password: str | None = None,
        api_key: str | None = None,
        auto_reconnect: bool = True,
        reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 30.0,
        max_reconnect_attempts: int = 10,
        initial_kg: str | None = None,
        last_seq: int | None = None,
        epoch: str | None = None,
        default_timeout: float | None = 30.0,
        keepalive: float | None = 20.0,
        max_in_flight: int = DEFAULT_MAX_IN_FLIGHT,
        deadline_grace: float = DEFAULT_DEADLINE_GRACE,
        lazy: bool = False,
        dispatcher: NotificationDispatcher | None = None,
        events: EventDispatcher | None = None,
        create_kg: Callable[[str], Awaitable[bool]] | None = None,
        ends_notifications: bool = True,
    ) -> None:
        if max_in_flight < 1:
            raise ValueError("max_in_flight must be at least 1")
        self._url = url
        self._username = username
        self._password = password
        self._api_key = api_key
        self._auto_reconnect = auto_reconnect
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay
        self._max_reconnect_attempts = max_reconnect_attempts
        self._initial_kg = initial_kg
        self._default_timeout = default_timeout
        self._keepalive = keepalive
        self._max_in_flight = max_in_flight
        self._deadline_grace = deadline_grace
        self._lazy = lazy
        self._create_kg = create_kg
        self._ends_notifications = ends_notifications

        # The notification cursor: the last seq seen on this connection and
        # the stream epoch it belongs to.
        self._last_seq = last_seq
        self._epoch = epoch

        self._ws: ClientConnection | None = None
        self._session_id: str | None = None
        self._server_version: str | None = None
        self._role: str | None = None
        self._current_kg: str | None = None
        self._state: _State = "idle"

        self._dispatcher = dispatcher if dispatcher is not None else NotificationDispatcher()
        self._events = events if events is not None else EventDispatcher()
        self._routes: dict[str, SubscriptionRoute] = {}
        self._reconnect_hooks: list[ReconnectHook] = []
        self._link_listeners: list[LinkListener] = []
        self._stale_pushes = 0

        # Created on first use, inside the event loop that uses them, so a
        # Connection can be constructed outside any loop.
        self._slots: _Slots | None = None
        self._ready: asyncio.Event | None = None
        self._connect_lock: asyncio.Lock | None = None

        self._pending: dict[str, _Pending] = {}
        self._next_id = 0
        self._reader: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._close_notice: NoticeResponse | None = None
        self._loop_last_send = 0.0
        self._closing = False
        self._reopenable = False

    # ── Properties ────────────────────────────────────────────────────

    @property
    def connected(self) -> bool:
        return self._state == "open"

    @property
    def state(self) -> str:
        """``idle``, ``connecting``, ``open``, ``reconnecting`` or ``closed``."""
        return self._state

    @property
    def session_id(self) -> str | None:
        return self._session_id

    @property
    def server_version(self) -> str | None:
        return self._server_version

    @property
    def role(self) -> str | None:
        return self._role

    @property
    def current_kg(self) -> str | None:
        return self._current_kg

    @property
    def dispatcher(self) -> NotificationDispatcher:
        return self._dispatcher

    @property
    def events(self) -> EventDispatcher:
        return self._events

    @property
    def last_seq(self) -> int:
        """The last notification ``seq`` this connection received (0 if none)."""
        return self._last_seq or 0

    @property
    def stream_epoch(self) -> str | None:
        """The engine run the notification cursor belongs to."""
        return self._epoch

    @property
    def in_flight(self) -> int:
        """Requests sent and not yet answered (abandoned ones included)."""
        return len(self._pending)

    @property
    def stale_pushes(self) -> int:
        """Standing-query pushes dropped: unknown subscription or stale generation."""
        return self._stale_pushes

    # ── Lifecycle ─────────────────────────────────────────────────────

    def _primitives(self) -> tuple[_Slots, asyncio.Event, asyncio.Lock]:
        if self._slots is None:
            self._slots = _Slots(self._max_in_flight)
        if self._ready is None:
            self._ready = asyncio.Event()
        if self._connect_lock is None:
            self._connect_lock = asyncio.Lock()
        return self._slots, self._ready, self._connect_lock

    async def connect(self) -> None:
        """Connect and authenticate. A no-op on an open connection."""
        _, ready, lock = self._primitives()
        async with lock:
            if self._state == "open":
                return
            self._closing = False
            self._reopenable = False
            self._state = "connecting"
            try:
                await self._open_bound(first=True)
            except BaseException:
                self._state = "idle"
                raise
            self._state = "open"
            ready.set()

    async def close(self, *, final: bool = False) -> None:
        """Close the connection. Pending calls fail with ``ConnectionLost``.

        A lazy connection reopens on its next call once closed, unless the
        close is ``final``.
        """
        was_closed = self._state == "closed"
        self._closing = True
        self._reopenable = False
        self._state = "closed"
        if self._reconnect_task is not None and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._reconnect_task
        self._reconnect_task = None
        ws = self._ws
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()
        reader = self._reader
        if reader is not None and not reader.done():
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await asyncio.wait_for(reader, 5)
        self._teardown(ws, ConnectionLost("Connection closed"))
        if self._ready is not None:
            self._ready.set()
        self._reopenable = not final
        if not was_closed:
            self._link_changed("closed")

    def _url_for(self, kg: str | None) -> str:
        params = []
        if kg:
            params.append(f"kg={quote(kg, safe='')}")
        if self._last_seq is not None:
            params.append(f"last_seq={self._last_seq}")
            if self._epoch is not None:
                params.append(f"epoch={quote(self._epoch, safe='')}")
        if not params:
            return self._url
        separator = "&" if "?" in self._url else "?"
        return f"{self._url}{separator}{'&'.join(params)}"

    async def _open_bound(self, *, first: bool) -> None:
        """Open to the bound knowledge graph, creating it once if it is missing."""
        kg = self._current_kg or self._initial_kg
        try:
            await self._open(kg)
        except AuthenticationError as err:
            if not (first and kg and self._create_kg is not None):
                raise
            try:
                created = await self._create_kg(kg)
            except QueryError:
                raise err from None
            if not created:
                raise
            await self._open(kg)

    async def _open(self, kg: str | None) -> None:
        """Open a socket, authenticate, and start the reader."""
        url = self._url_for(kg)
        try:
            ws = await websockets.connect(url)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {self._url}: {e}") from e
        try:
            await self._authenticate(ws)
        except BaseException:
            with contextlib.suppress(Exception):
                await ws.close()
            raise
        self._ws = ws
        self._close_notice = None
        self._loop_last_send = asyncio.get_running_loop().time()
        self._reader = asyncio.create_task(self._read_loop(ws), name="inputlayer-reader")
        if self._keepalive:
            self._keepalive_task = asyncio.create_task(
                self._keepalive_loop(ws), name="inputlayer-keepalive"
            )

    async def _authenticate(self, ws: ClientConnection) -> None:
        """Authenticate on *ws* before the reader starts."""
        auth_id = self._new_id()
        if self._api_key:
            msg: AuthenticateMessage | LoginMessage = AuthenticateMessage(
                api_key=self._api_key, id=auth_id
            )
        elif self._username and self._password:
            msg = LoginMessage(username=self._username, password=self._password, id=auth_id)
        else:
            raise AuthenticationError("No credentials provided (need username/password or api_key)")

        await ws.send(msg.to_json())
        while True:
            try:
                raw = await ws.recv()
            except websockets.ConnectionClosed as e:
                raise ConnectionError(f"Connection closed during authentication: {e}") from e
            response = deserialize_message(raw)
            if isinstance(response, NoticeResponse):
                if response.closes_connection:
                    raise AuthenticationError(response.message)
                continue
            if isinstance(response, AuthErrorResponse):
                raise AuthenticationError(response.message)
            if isinstance(response, AuthenticatedResponse):
                self._session_id = response.session_id
                self._server_version = response.version
                self._role = response.role
                self._current_kg = response.knowledge_graph
                if self._epoch != response.stream_epoch:
                    # A cursor from another engine run means nothing here; the
                    # server answers it with a replay_gap notice, and the
                    # cursor counts again from the start of this run.
                    if self._epoch is not None and self._last_seq is not None:
                        self._last_seq = 0
                    self._epoch = response.stream_epoch
                return
            raise AuthenticationError(f"Unexpected auth response: {response!r}")

    # ── Requests ──────────────────────────────────────────────────────

    def _new_id(self) -> str:
        self._next_id += 1
        return f"r{self._next_id}"

    async def execute(self, program: str, *, timeout: float | None = None) -> ResultResponse:
        """Send a program and await its result.

        ``timeout`` (seconds, default the connection's ``default_timeout``)
        becomes the request's deadline, covering any wait for a connection
        or a request slot; ``DeadlineExceeded`` means nothing was applied. A
        write that gets no reply at all by then raises ``OutcomeUnknownError``.
        A ``rate_limited`` refusal is retried once after the rate window, when
        the deadline leaves time for it.
        """
        if timeout is None:
            timeout = self._default_timeout
        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
        try:
            return await self._execute_once(program, deadline)
        except RateLimited:
            remaining = self._remaining(deadline)
            if remaining is not None and remaining <= _RATE_LIMIT_WINDOW:
                raise
            await asyncio.sleep(_RATE_LIMIT_WINDOW)
            return await self._execute_once(program, deadline)

    async def _execute_once(self, program: str, deadline: float | None) -> ResultResponse:
        await self._ensure_open(deadline, program)
        slots, _, _ = self._primitives()
        try:
            await slots.acquire(self._remaining(deadline))
        except asyncio.TimeoutError:
            raise DeadlineExceeded(
                "No request slot freed before the deadline; nothing was sent",
                query=program,
            ) from None
        pending = self._register("execute", program=program, holds_slot=True)
        remaining = self._remaining(deadline)
        timeout_ms = None if remaining is None else max(1, int(remaining * 1000))
        await self._send(ExecuteMessage(program=program, id=pending.id, timeout_ms=timeout_ms))
        result: ResultResponse = await self._await_reply(pending, deadline)
        return result

    async def ping(self) -> None:
        """Send an application-level ping and await its ``pong``."""
        await self._ensure_open(None, None)
        pending = self._register("ping")
        await self._send(PingMessage(id=pending.id))
        await self._await_reply(pending, None)

    async def cancel(self, request_id: str) -> str:
        """Cancel the unanswered request *request_id*; returns the outcome."""
        ack: CancelAckResponse = await self._send_cancel(request_id)
        return ack.outcome

    def _remaining(self, deadline: float | None) -> float | None:
        if deadline is None:
            return None
        return max(0.0, deadline - asyncio.get_running_loop().time())

    async def _ensure_open(self, deadline: float | None, program: str | None) -> None:
        if self._state == "open":
            return
        if self._state == "idle" and not self._lazy:
            raise ConnectionError("Not connected")
        if self._state in ("idle", "connecting"):
            # Opens it (lazy), or waits for the connect in progress.
            await self.connect()
            return
        if self._state == "closed" and self._reopenable and self._lazy:
            # Closed by close(), not given up: the next call reopens it.
            await self.connect()
            return
        if self._state == "closed":
            raise ConnectionLost(
                "Not connected: the connection is closed", code=self._notice_code()
            )
        # connecting or reconnecting: wait for it, within the deadline.
        _, ready, _ = self._primitives()
        try:
            await asyncio.wait_for(ready.wait(), self._remaining(deadline))
        except asyncio.TimeoutError:
            raise DeadlineExceeded(
                "The connection did not come back before the deadline; nothing was sent",
                query=program,
            ) from None
        if not self.connected:  # the reconnect gave up meanwhile
            raise ConnectionLost(
                "Not connected: reconnecting gave up", code=self._notice_code()
            )

    def _register(
        self,
        kind: _Kind,
        *,
        program: str | None = None,
        holds_slot: bool = False,
        target: str | None = None,
    ) -> _Pending:
        pending = _Pending(
            id=self._new_id(),
            kind=kind,
            future=asyncio.get_running_loop().create_future(),
            program=program,
            holds_slot=holds_slot,
            target=target,
        )
        self._pending[pending.id] = pending
        return pending

    async def _send(self, msg: ExecuteMessage | PingMessage | CancelMessage) -> None:
        ws = self._ws
        if ws is None:
            self._fail_unsent(msg)
            return
        try:
            await ws.send(msg.to_json())
        except Exception:
            # The reader sees the same closed socket and fails every pending
            # call; this request was never sent.
            self._fail_unsent(msg)
            return
        self._loop_last_send = asyncio.get_running_loop().time()

    def _fail_unsent(self, msg: ExecuteMessage | PingMessage | CancelMessage) -> None:
        pending = self._pending.get(msg.id or "")
        if pending is not None:
            self._finish(
                pending,
                error=ConnectionLost(
                    "Connection lost before the request was sent",
                    code=self._notice_code(),
                    may_have_committed=False,
                ),
            )

    async def _await_reply(self, pending: _Pending, deadline: float | None) -> Any:
        """Await *pending*'s reply; past the deadline, cancel it on the server."""
        try:
            if deadline is None:
                return await asyncio.shield(pending.future)
            backstop = self._remaining(deadline) + self._deadline_grace  # type: ignore[operator]
            with contextlib.suppress(asyncio.TimeoutError):
                return await asyncio.wait_for(asyncio.shield(pending.future), backstop)
            # The server's own deadline_exceeded should have arrived by now.
            return await self._cancel_overdue(pending)
        except asyncio.CancelledError:
            if not pending.future.done():
                pending.abandoned = True
                if self._state == "open":
                    self._fire_cancel(pending.id)
            raise

    async def _cancel_overdue(self, pending: _Pending) -> Any:
        ack_future: asyncio.Future[Any] | None = None
        if self._state == "open":
            cancel = self._register("cancel", target=pending.id)
            await self._send(CancelMessage(target=pending.id, id=cancel.id))
            ack_future = cancel.future
        waiting: set[asyncio.Future[Any]] = {pending.future}
        if ack_future is not None:
            waiting.add(ack_future)
        loop = asyncio.get_running_loop()
        give_up = loop.time() + self._deadline_grace
        stopped = False
        while not pending.future.done():
            remaining = give_up - loop.time()
            if remaining <= 0:
                break
            await asyncio.wait(waiting, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
            if ack_future is not None and ack_future.done():
                waiting.discard(ack_future)
                outcome = (
                    ack_future.result().outcome
                    if not ack_future.cancelled() and ack_future.exception() is None
                    else None
                )
                if outcome == "too_late":
                    # The write began committing: its reply reports it.
                    return await asyncio.shield(pending.future)
                stopped = outcome == "cancelled"
                ack_future = None
        if pending.future.done():
            return pending.future.result()
        pending.abandoned = True
        if not stopped and pending.program is not None and _may_write(pending.program):
            raise OutcomeUnknownError(
                "No reply before the deadline and no confirmation that the cancel "
                "stopped it; the write may have committed: read back before retrying"
            )
        raise DeadlineExceeded(
            "No reply before the deadline; the request was cancelled",
            query=pending.program,
        )

    def _fire_cancel(self, target: str) -> None:
        """Send a cancel for *target* without waiting for its ack."""
        cancel = self._register("cancel", target=target)
        cancel.abandoned = True
        task = asyncio.ensure_future(self._send(CancelMessage(target=target, id=cancel.id)))
        task.add_done_callback(lambda t: t.cancelled() or t.exception())

    async def _send_cancel(self, target: str) -> Any:
        await self._ensure_open(None, None)
        cancel = self._register("cancel", target=target)
        await self._send(CancelMessage(target=target, id=cancel.id))
        return await self._await_reply(cancel, None)

    # ── Subscription routes and reconnect hooks ───────────────────────

    def add_route(self, subscription: str, sink: PushSink) -> SubscriptionRoute:
        """Route pushes of *subscription* to *sink* (replacing any route)."""
        route = SubscriptionRoute(subscription=subscription, sink=sink)
        self._routes[subscription] = route
        return route

    def remove_route(self, subscription: str, route: SubscriptionRoute | None = None) -> None:
        """Stop routing pushes of *subscription* (only if its route is still
        *route*, when given)."""
        if route is None or self._routes.get(subscription) is route:
            self._routes.pop(subscription, None)

    def add_reconnect_hook(self, hook: ReconnectHook) -> None:
        """Run *hook* after every reconnect, before ``reconnected`` is emitted."""
        self._reconnect_hooks.append(hook)

    def remove_reconnect_hook(self, hook: ReconnectHook) -> None:
        with contextlib.suppress(ValueError):
            self._reconnect_hooks.remove(hook)

    def add_link_listener(self, listener: LinkListener) -> None:
        """Call *listener* when this connection drops, gives up reconnecting,
        or is closed by the client. It runs in the reader, before a reconnect
        starts, so it must not block."""
        self._link_listeners.append(listener)

    def remove_link_listener(self, listener: LinkListener) -> None:
        with contextlib.suppress(ValueError):
            self._link_listeners.remove(listener)

    def _link_changed(self, change: LinkChange) -> None:
        code = self._notice_code()
        for listener in list(self._link_listeners):
            try:
                listener(change, code)
            except Exception:
                logger.exception("Link listener %r failed", listener)

    # ── The reader ────────────────────────────────────────────────────

    async def _read_loop(self, ws: ClientConnection) -> None:
        error: BaseException | None = None
        try:
            async for raw in ws:
                try:
                    frame = deserialize_message(raw)
                except (ValueError, KeyError, TypeError) as e:
                    logger.warning("Dropping an unreadable frame from the server: %s", e)
                    continue
                self._route(frame)
        except asyncio.CancelledError:
            # The event loop is shutting down; nothing to reconnect for.
            raise
        except websockets.ConnectionClosed as e:
            error = e
        except Exception as e:
            logger.exception("Connection reader failed")
            error = e
        if ws is self._ws:
            self._on_lost(ws, error)

    def _route(self, frame: ServerMessage) -> None:
        if isinstance(frame, NotificationResponse):
            self._deliver_notification(frame)
        elif isinstance(frame, NoticeResponse):
            self._on_notice(frame)
        elif isinstance(frame, _SUBSCRIPTION_PUSHES):
            self._deliver_push(frame)
        else:
            self._deliver_reply(frame)

    def _deliver_notification(self, frame: NotificationResponse) -> None:
        self._last_seq = max(self._last_seq or 0, frame.seq)
        event = NotificationEvent(
            type=frame.type,
            seq=frame.seq,
            timestamp_ms=frame.timestamp_ms,
            session_id=frame.session_id,
            knowledge_graph=frame.knowledge_graph,
            relation=frame.relation,
            operation=frame.operation,
            count=frame.count,
            rule_name=frame.rule_name,
            entity=frame.entity,
        )
        self._dispatcher.dispatch(event, epoch=self._epoch)

    def _on_notice(self, notice: NoticeResponse) -> None:
        if notice.closes_connection:
            logger.warning("server notice (%s): %s", notice.code, notice.message)
            self._close_notice = notice
            return
        logger.info("server notice (%s): %s", notice.code, notice.message)
        self._events.emit(
            ConnectionEvent(
                type="notification_gap",
                knowledge_graph=self._current_kg,
                code=notice.code,
                message=notice.message,
            )
        )

    def _deliver_push(self, frame: SubscriptionPush) -> None:
        route = self._routes.get(frame.subscription)
        if route is not None and route.generation is None:
            # Its .subscribe reply has not arrived: it names the generation.
            route.early.append(frame)
            return
        self._push_to(route, frame)

    def _push_to(self, route: SubscriptionRoute | None, frame: SubscriptionPush) -> None:
        if route is None or route.generation != frame.generation:
            self._stale_pushes += 1
            if route is not None:
                route.stale += 1
            logger.debug(
                "Dropped a push for subscription %r generation %d",
                frame.subscription,
                frame.generation,
            )
            return
        try:
            route.sink(frame)
        except Exception:
            logger.exception("Subscription sink for %r failed", frame.subscription)

    def _pending_for(self, request_id: str | None) -> _Pending | None:
        if request_id is not None:
            return self._pending.get(request_id)
        # Replies come back in request order; one without an id answers the
        # oldest unanswered request.
        return next(iter(self._pending.values()), None)

    def _deliver_reply(self, frame: ServerMessage) -> None:
        pending = self._pending_for(getattr(frame, "id", None))
        if pending is None:
            logger.debug("Dropped a reply no request is waiting for: %r", frame)
            return
        try:
            self._apply_reply(pending, frame)
        except Exception as e:
            self._finish(pending, error=e)

    def _apply_reply(self, pending: _Pending, frame: ServerMessage) -> None:
        if isinstance(frame, ResultResponse):
            self._finish(pending, result=self._accept(frame))
        elif isinstance(frame, ResultStartResponse):
            if pending.stream is not None:
                raise InternalError("A second result_start arrived inside a streamed result")
            pending.stream = _Stream(start=frame)
        elif isinstance(frame, ResultChunkResponse):
            stream = pending.stream
            if stream is None:
                raise InternalError("result_chunk arrived without a result_start")
            if frame.chunk_index != stream.chunks:
                raise InternalError(
                    f"Streamed result chunk {frame.chunk_index} arrived, expected {stream.chunks}"
                )
            stream.chunks += 1
            stream.rows.extend(frame.rows)
            if frame.row_provenance:
                stream.provenance.extend(frame.row_provenance)
        elif isinstance(frame, ResultEndResponse):
            self._finish(pending, result=self._accept(self._assemble(pending, frame)))
        elif isinstance(frame, ErrorResponse):
            self._finish(pending, error=_query_error(frame))
        elif isinstance(frame, (PongResponse, CancelAckResponse)):
            self._finish(pending, result=frame)
        else:
            raise InternalError(f"Unexpected reply: {frame!r}")

    def _assemble(self, pending: _Pending, end: ResultEndResponse) -> ResultResponse:
        stream = pending.stream
        if stream is None:
            raise InternalError("result_end arrived without a result_start")
        if (end.chunk_count, end.row_count) != (stream.chunks, len(stream.rows)):
            raise InternalError(
                f"Incomplete streamed result: {stream.chunks} chunk(s) and {len(stream.rows)} "
                f"row(s) arrived, end announces {end.chunk_count} and {end.row_count}"
            )
        start = stream.start
        return ResultResponse(
            columns=start.columns,
            rows=stream.rows,
            row_count=end.row_count,
            total_count=start.total_count,
            truncated=start.truncated,
            execution_time_ms=start.execution_time_ms,
            row_provenance=stream.provenance or None,
            metadata=start.metadata,
            switched_kg=start.switched_kg,
            proof_trees=start.proof_trees,
            timing_breakdown=start.timing_breakdown,
            errors=start.errors,
            subscribed=start.subscribed,
            id=start.id,
        )

    def _accept(self, result: ResultResponse) -> ResultResponse:
        """Track a KG switch and a new subscription generation, then raise if
        any statement failed. Runs in the reader, in reply order."""
        if result.switched_kg:
            self._current_kg = result.switched_kg
        if result.subscribed is not None:
            route = self._routes.get(result.subscribed.subscription)
            if route is not None:
                route.generation = result.subscribed.generation
                early, route.early = route.early, []
                for frame in early:
                    self._push_to(route, frame)
        for code, error_type in (
            ("outcome_unknown", OutcomeUnknownError),
            ("store_read_only", StoreReadOnlyError),
        ):
            for error in result.errors or []:
                if error.code == code:
                    raise error_type(error.message, result)
        if result.errors:
            raise StatementFailedError(result.errors, result)
        return result

    def _finish(
        self,
        pending: _Pending,
        *,
        result: Any = None,
        error: BaseException | None = None,
    ) -> None:
        if self._pending.get(pending.id) is not pending:
            return
        del self._pending[pending.id]
        if pending.holds_slot and self._slots is not None:
            self._slots.release()
        if pending.abandoned or pending.future.done():
            return
        if error is not None:
            pending.future.set_exception(error)
        else:
            pending.future.set_result(result)

    # ── Keepalive ─────────────────────────────────────────────────────

    async def _keepalive_loop(self, ws: ClientConnection) -> None:
        """Ping when nothing was sent for ``keepalive`` seconds, so a client
        that only holds subscriptions never reaches the server's idle timeout."""
        assert self._keepalive
        loop = asyncio.get_running_loop()
        while ws is self._ws and self._state == "open":
            wait = self._loop_last_send + self._keepalive - loop.time()
            if wait > 0:
                await asyncio.sleep(wait)
                continue
            if self._pending:
                # The server is not idle while it owes replies, and a ping
                # would take the slot kept free for a cancel.
                self._loop_last_send = loop.time()
                continue
            pending = self._register("ping")
            pending.abandoned = True
            await self._send(PingMessage(id=pending.id))
            self._loop_last_send = loop.time()

    # ── Losing the connection and getting it back ─────────────────────

    def _notice_code(self) -> str | None:
        return self._close_notice.code if self._close_notice is not None else None

    def _teardown(self, ws: ClientConnection | None, error: ConnectionLost) -> None:
        """Fail every pending call and stop the background tasks of *ws*."""
        if ws is not None and ws is self._ws:
            self._ws = None
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
            self._keepalive_task = None
        for pending in list(self._pending.values()):
            committed = (
                pending.kind == "execute"
                and pending.program is not None
                and _may_write(pending.program)
            )
            self._finish(
                pending,
                error=ConnectionLost(
                    str(error), code=error.code, may_have_committed=committed
                ),
            )

    def _on_lost(self, ws: ClientConnection, cause: BaseException | None) -> None:
        code = self._notice_code()
        if self._closing:
            return
        reason = f"Connection lost ({code})" if code else "Connection lost"
        if cause is not None and not code:
            reason = f"{reason}: {cause}"
        self._teardown(ws, ConnectionLost(reason, code=code))
        kg = self._current_kg
        self._link_changed("lost")
        self._events.emit(ConnectionEvent(type="disconnected", knowledge_graph=kg, code=code))
        if self._auto_reconnect and code not in _FINAL_NOTICES and self._max_reconnect_attempts:
            self._state = "reconnecting"
            if self._ready is not None:
                self._ready.clear()
            self._reconnect_task = asyncio.ensure_future(self._reconnect())
        else:
            self._give_up(code)

    def _give_up(self, code: str | None) -> None:
        self._state = "closed"
        if self._ready is not None:
            self._ready.set()
        self._link_changed("given_up")
        if self._ends_notifications:
            self._dispatcher.fail(
                ConnectionLost(
                    f"Connection to {self._current_kg!r} closed and not reconnected", code=code
                )
            )
        self._events.emit(
            ConnectionEvent(type="closed", knowledge_graph=self._current_kg, code=code)
        )

    async def _reconnect(self) -> None:
        """Reconnect with exponential backoff and jitter, then resume."""
        delay = self._reconnect_delay
        code = self._notice_code()
        for attempt in range(self._max_reconnect_attempts):
            await asyncio.sleep(delay * random.uniform(0.5, 1.0))
            if self._closing:
                return
            logger.info(
                "Reconnecting (attempt %d/%d)...", attempt + 1, self._max_reconnect_attempts
            )
            resumed = self._last_seq is not None
            try:
                await self._open_bound(first=False)
            except AuthenticationError as e:
                logger.warning("Reconnect refused: %s", e)
                break
            except Exception as e:
                logger.info("Reconnect attempt failed: %s", e)
                delay = min(delay * 2, self._max_reconnect_delay)
                continue
            logger.info("Reconnected to %r", self._current_kg)
            self._state = "open"
            if self._ready is not None:
                self._ready.set()
            kg = self._current_kg
            self._events.emit(ConnectionEvent(type="session_reset", knowledge_graph=kg))
            for hook in list(self._reconnect_hooks):
                try:
                    await hook(self)
                except Exception:
                    logger.exception("Reconnect hook %r failed", hook)
            self._events.emit(ConnectionEvent(type="reconnected", knowledge_graph=kg))
            if not resumed:
                self._events.emit(
                    ConnectionEvent(
                        type="notification_gap",
                        knowledge_graph=kg,
                        message="Reconnected without a notification cursor; "
                        "notifications sent while disconnected are lost",
                    )
                )
            return
        self._give_up(code)


def _query_error(response: ErrorResponse) -> QueryError:
    if response.code == "outcome_unknown":
        return OutcomeUnknownError(response.message)
    if response.code == "store_read_only":
        return StoreReadOnlyError(response.message)
    if response.code == "deadline_exceeded":
        return DeadlineExceeded(response.message)
    if response.code == "cancelled":
        return Cancelled(response.message)
    if response.code == "rate_limited":
        return RateLimited(response.message)
    if response.code == "invalid_request":
        return ProtocolError(response.message)
    return QueryError(
        response.message,
        code=response.code,
        validation_errors=response.validation_errors,
    )
