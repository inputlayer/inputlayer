"""Subscriptions: a standing query's result, kept exact on the client.

``kg.subscribe(...)`` is an async iterator of ``Change`` events, all of one
shape, so a loop that applies ``inserted`` and ``retracted`` is always
correct:

- ``snapshot``: the first event, the exact result at ``revision``, every row
  inserted;
- ``delta``: the result changed; ``inserted`` and ``retracted`` are the set
  differences between consecutive results, at ``revision`` (``seq`` is the
  engine's delta number). An unchanged result delivers nothing;
- ``unverified``: the result is no longer known to be current (``reason``:
  the connection was lost, a ``seq`` gap, a broken streamed delta, a
  ``subscription_reset`` or ``subscription_error`` from the server, or the
  consumer fell ``queue`` events behind). Both lists are empty and
  ``verified`` is false; the SDK subscribes again with backoff;
- ``resync``: the fresh snapshot after an ``unverified``: the exact
  difference between the rows the consumer holds (every event before it
  applied) and the fresh result, so an outage never replays rows that did not
  change.

The engine diffs full tuples, so a projected subscription (some columns of a
relation) keeps a count of the engine rows behind each projected row and
reports a row only when its count moves between 0 and 1: a row still
supported by another tuple is never retracted.

Coalesced commits produce one delta at the latest revision, so a row that
appears and disappears between two evaluations is never seen; durable needs
belong in facts. Two subscriptions do not share revisions.

The JavaScript SDK's ``kg.subscribe()`` yields the same events, with the same
kinds, fields and reasons.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import itertools
import logging
import random
import re
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from pydantic import BaseModel

from inputlayer import _meta
from inputlayer._protocol import (
    SubscriptionDeltaChunkResponse,
    SubscriptionDeltaEndResponse,
    SubscriptionDeltaResponse,
    SubscriptionDeltaStartResponse,
    SubscriptionErrorResponse,
    SubscriptionPush,
    SubscriptionResetResponse,
)
from inputlayer.exceptions import (
    Cancelled,
    ConnectionLost,
    DeadlineExceeded,
    InternalError,
    OutcomeUnknownError,
    QueryError,
    RateLimited,
    SubscriptionRejected,
    SubscriptionRejectedReason,
)

if TYPE_CHECKING:
    from inputlayer.compiler import QueryPlan
    from inputlayer.connection import Connection, LinkChange, SubscriptionRoute

logger = logging.getLogger("inputlayer")

T = TypeVar("T")

ChangeKind = Literal["snapshot", "delta", "unverified", "resync"]

UnverifiedReason = Literal[
    "connection_lost",
    "seq_gap",
    "broken_stream",
    "subscription_reset",
    "subscription_error",
    "slow_consumer",
]
"""Why a subscription's result stopped being verified."""

DEFAULT_QUEUE = 1024

# First and longest wait between attempts to subscribe again, in seconds.
RESUBSCRIBE_DELAY = 1.0
MAX_RESUBSCRIBE_DELAY = 30.0

_ids = itertools.count(1)


def _subscription_id() -> str:
    """A subscription id unique in this process, so unique on every connection."""
    return f"il_sub_{next(_ids)}"


# ── Events ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Change(Generic[T]):
    """One event of a subscription; see the module documentation."""

    kind: ChangeKind
    #: Rows that entered the result.
    inserted: list[T]
    #: Rows that left the result.
    retracted: list[T]
    #: The knowledge graph revision the result is at after this event; an
    #: ``unverified`` event carries the last verified one.
    revision: int
    #: The engine's delta number for a ``delta`` (from 1 per server
    #: subscription); 0 for ``snapshot`` and ``resync``; the last delta's for
    #: ``unverified``.
    seq: int
    #: False only on ``unverified``.
    verified: bool
    #: Why the result is unverified (``unverified`` only).
    reason: UnverifiedReason | None = None
    #: The server's message, for ``subscription_reset`` and ``subscription_error``.
    message: str | None = None


@dataclass(frozen=True)
class Live(Generic[T]):
    """The whole current result of a watched query; see ``KnowledgeGraph.watch``."""

    rows: list[T]
    revision: int
    #: False from an ``unverified`` event until the fresh result arrives.
    verified: bool
    reason: UnverifiedReason | None = None


@dataclass
class SubscriptionStats:
    #: Pushes of an earlier generation, dropped.
    stale_dropped: int = 0
    #: Times the subscription was opened again after an ``unverified``.
    resubscribes: int = 0


class Row(Mapping[str, Any]):
    """A row of a subscribed result: its values by column name.

    Read a value as ``row.order`` or ``row["order"]``; ``dict(row)`` gives
    a plain dict. Rows are immutable and hashable, so they can be kept in a
    set: two rows are equal when they have the same columns and values.
    """

    __slots__ = ("_key", "_labels", "_values")
    _labels: tuple[str, ...]
    _values: tuple[Any, ...]
    _key: tuple[tuple[str, ...], Any]

    def __init__(self, labels: list[str], values: list[Any]) -> None:
        object.__setattr__(self, "_labels", tuple(labels))
        object.__setattr__(self, "_values", tuple(values))
        object.__setattr__(self, "_key", (self._labels, _row_key(values)))

    def __getitem__(self, label: str) -> Any:
        try:
            return self._values[self._labels.index(label)]
        except ValueError:
            raise KeyError(label) from None

    def __getattr__(self, label: str) -> Any:
        try:
            return self[label]
        except KeyError:
            raise AttributeError(
                f"Row has no column {label!r} (columns: {', '.join(self._labels)})"
            ) from None

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("Row is immutable")

    def __iter__(self) -> Iterator[str]:
        return iter(self._labels)

    def __len__(self) -> int:
        return len(self._labels)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Row):
            return self._key == other._key
        if isinstance(other, Mapping):
            return dict(self) == dict(other)
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._key)

    def __repr__(self) -> str:
        pairs = zip(self._labels, self._values, strict=True)
        return f"Row({', '.join(f'{k}={v!r}' for k, v in pairs)})"

    def values_tuple(self) -> tuple[Any, ...]:
        """The values in column order."""
        return self._values


def _row_key(values: list[Any] | tuple[Any, ...]) -> Any:
    """A hashable identity of a row's values (a vector value is a list)."""
    try:
        key: Any = tuple(values)
        hash(key)
    except TypeError:
        key = repr(list(values))
    return key


def row_identity(row: Any) -> Any:
    """The identity of a row a subscription yields, for keeping rows in a map."""
    if isinstance(row, Row):
        return row._key
    if isinstance(row, BaseModel):
        return _row_key([getattr(row, name) for name in type(row).model_fields])
    return _row_key(list(row))


# ── Query shape ─────────────────────────────────────────────────────────

# An atom's relation name: the identifier right before an opening parenthesis.
_ATOM = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(")


@dataclass(frozen=True)
class Shape:
    """The standing query of a subscription and how to read its rows."""

    #: The ``?...`` query sent with ``.subscribe``.
    query: str
    #: The compiled query, or ``None`` for raw IQL (rows keep the engine's columns).
    plan: QueryPlan | None = None
    #: The relation class of typed rows, when one whole relation is selected.
    relation_cls: type | None = None

    @property
    def relations(self) -> list[str]:
        """Relation names the query reads, checked against the session's rules."""
        return list(dict.fromkeys(_ATOM.findall(self.query)))

    def labels(self, columns: list[str]) -> list[str]:
        return self.plan.labels if self.plan is not None else list(columns)

    def project(self, rows: list[list[Any]]) -> list[list[Any]]:
        """The projected values of each engine row, in label order, repeats kept."""
        return self.plan.pick(rows) if self.plan is not None else rows

    def row(self, labels: list[str], values: list[Any]) -> Any:
        if self.plan is not None:
            values = self.plan.from_engine(values)
        if self.relation_cls is not None:
            with contextlib.suppress(Exception):
                return self.relation_cls(**dict(zip(labels, values, strict=True)))
        return Row(labels, values)


def iql_shape(iql: str) -> Shape:
    """The shape of a raw ``?...`` query: rows keyed by the engine's column names."""
    query = iql.strip()
    if not query.startswith("?") or "\n" in query:
        raise SubscriptionRejected(
            "A subscription stands on one ?-query; to subscribe to several rules' "
            "union, define a persistent rule with one clause per branch and "
            "subscribe to it.",
            "rejected",
        )
    return Shape(query=query)


def plan_shape(plan: QueryPlan, relation_cls: type | None, *, aggregate: bool) -> Shape:
    """The shape of a compiled query, refusing what a standing query cannot track."""
    if aggregate:
        raise SubscriptionRejected(
            "An aggregate query is evaluated through a rule that lives only for one "
            "request, and subscriptions see persistent rules only. Define the "
            "aggregate as a persistent rule (kg.define_rules) and subscribe to that "
            "relation.",
            "session_view",
        )
    if plan.setup:
        raise SubscriptionRejected(
            "A negation of a constant binds the constant through a session fact, "
            "and subscriptions see persistent data only. Define the condition in a "
            "persistent rule (kg.define_rules) and subscribe to that relation.",
            "session_view",
        )
    statements = plan.program.split("\n")
    if len(statements) > 1:
        raise SubscriptionRejected(
            "An OR condition splits the query into branches that a standing query "
            "cannot hold under one revision. Define a persistent rule with one clause "
            "per branch (kg.define_rules) and subscribe to it.",
            "or_branches",
        )
    return Shape(query=statements[0], plan=plan, relation_cls=relation_cls)


# ── Held result ─────────────────────────────────────────────────────────


@dataclass
class _Held:
    row: Any
    #: Engine rows behind this projected row.
    count: int


@dataclass
class _DeltaStream:
    start: SubscriptionDeltaStartResponse
    inserted: list[list[Any]] = field(default_factory=list)
    retracted: list[list[Any]] = field(default_factory=list)
    chunks: int = 0


_State = Literal["idle", "opening", "live", "unverified", "closed"]


class Subscription(Generic[T]):
    """One standing query, as an async iterator of ``Change`` events.

    It opens on the first ``__anext__``; ``close()`` ends it here and on the
    server, and so does leaving an ``async for`` loop over it (or its
    ``async with`` block). A refusal ends the iterator with
    ``SubscriptionRejected``; losing the connection for good ends it with
    ``ConnectionLost``.
    """

    def __init__(
        self,
        conn: Connection,
        shape: Shape,
        *,
        queue: int = DEFAULT_QUEUE,
        timeout: float | None = None,
        session_rules: Callable[[], Awaitable[list[str]]] | None = None,
    ) -> None:
        #: The id the subscription has on the server.
        self.id = _subscription_id()
        self._conn = conn
        self._shape = shape
        self._capacity = max(1, queue)
        self._timeout = timeout
        self._session_rules = session_rules

        self._state: _State = "idle"
        self._route: SubscriptionRoute | None = None
        self._stale_before = 0
        # The server holds the id: .unsubscribe before opening it again.
        self._registered = False
        self._held: dict[Any, _Held] = {}
        self._revision = 0
        self._seq = 0
        self._stream: _DeltaStream | None = None
        # Pushes that arrive while a .subscribe reply is being taken.
        self._early: list[SubscriptionPush] | None = None
        self._queue: deque[Change[T]] = deque()
        self._changed: asyncio.Event | None = None
        # Why the iterator ended, until __anext__ has raised it.
        self._error: BaseException | None = None
        # Open again once the consumer has read everything queued (slow_consumer).
        self._reopen_when_drained = False
        self._reopen_task: asyncio.Task[None] | None = None
        self._unsubscribing: asyncio.Task[None] | None = None
        self._wake: asyncio.Event | None = None
        self._stats = SubscriptionStats()

    @property
    def query(self) -> str:
        """The ``?...`` query the subscription stands on."""
        return self._shape.query

    @property
    def stats(self) -> SubscriptionStats:
        stale = self._stale_before + (self._route.stale if self._route is not None else 0)
        return SubscriptionStats(stale_dropped=stale, resubscribes=self._stats.resubscribes)

    # ── Iteration ─────────────────────────────────────────────────────

    def __aiter__(self) -> AsyncIterator[Change[T]]:
        return self._events()

    async def _events(self) -> AsyncIterator[Change[T]]:
        # A generator, so that leaving the loop closes the subscription.
        try:
            while True:
                try:
                    change = await self.__anext__()
                except StopAsyncIteration:
                    return
                yield change
        finally:
            await self.close()

    async def __anext__(self) -> Change[T]:
        if self._state == "idle":
            await self._start()
        while True:
            if self._queue:
                change = self._queue.popleft()
                if not self._queue and self._reopen_when_drained:
                    self._reopen_when_drained = False
                    self._reopen()
                return change
            if self._state == "closed":
                error, self._error = self._error, None
                if error is not None:
                    raise error
                raise StopAsyncIteration
            changed = self._signal()
            changed.clear()
            await changed.wait()

    async def __aenter__(self) -> Subscription[T]:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """End the subscription here and on the server. Queued events are dropped."""
        if self._state != "closed":
            self._end()
            self._queue.clear()
            self._error = None
        if self._unsubscribing is not None:
            with contextlib.suppress(Exception):
                await self._unsubscribing

    def _signal(self) -> asyncio.Event:
        if self._changed is None:
            self._changed = asyncio.Event()
        return self._changed

    # ── Opening ───────────────────────────────────────────────────────

    async def _start(self) -> None:
        self._state = "opening"
        self._conn.add_link_listener(self._on_link)
        try:
            await self._check_persistent()
            await self._open("snapshot")
        except Exception as e:
            self._fail(e)
        except BaseException:
            self._end()
            raise

    async def _check_persistent(self) -> None:
        """Session rules are invisible to subscriptions: the engine would never push."""
        relations = self._shape.relations
        if not relations or self._session_rules is None:
            return
        heads = {rule.lstrip("+").split("(", 1)[0].strip() for rule in await self._session_rules()}
        session = next((r for r in relations if r in heads), None)
        if session is not None:
            raise SubscriptionRejected(
                f"'{session}' is a session rule, and subscriptions see persistent data "
                "only. Define it as a persistent rule (kg.define_rules) to subscribe to it.",
                "session_view",
                query=self._shape.query,
            )

    async def _open(self, kind: Literal["snapshot", "resync"]) -> None:
        """Send ``.subscribe`` and take its snapshot as the first event or as a resync."""
        program = _meta.subscribe(self.id, self._shape.query)
        self._early = []
        self._set_route(self._conn.add_route(self.id, self._on_push))
        try:
            reply = await self._conn.execute(program, timeout=self._timeout)
        except BaseException as e:
            self._set_route(None)
            self._early = None
            if isinstance(e, OutcomeUnknownError):
                # It may have registered although no reply came.
                self._registered = True
            refused = _refusal(e, program) if isinstance(e, Exception) else e
            if refused is not e:
                raise refused from e
            raise
        self._registered = True
        if self._state == "closed":
            # Closed while opening: the server registered it anyway.
            self._set_route(None)
            self._early = None
            self._unsubscribe()
            return
        try:
            subscribed = reply.subscribed
            if subscribed is None:
                raise InternalError(f"The .subscribe reply names no subscription: {reply!r}")
            fresh = self._result_of(reply.columns, reply.rows)
        except BaseException:
            self._set_route(None)
            self._early = None
            self._unsubscribe()
            raise
        if kind == "snapshot":
            inserted = [held.row for held in fresh.values()]
            self._push(Change(kind, inserted, [], subscribed.revision, 0, True))
        else:
            inserted, retracted = _difference(self._held, fresh)
            self._push(Change(kind, inserted, retracted, subscribed.revision, 0, True))
        self._held = fresh
        self._revision = subscribed.revision
        self._seq = 0
        self._stream = None
        self._state = "live"
        # Pushes that arrived before the snapshot was taken, in order.
        early, self._early = self._early or [], None
        for push in early:
            self._on_push(push)

    def _reopen(self) -> None:
        """Open again with backoff until it works, is refused, or the subscription ends."""
        if self._reopen_task is not None or self._state != "unverified":
            return
        self._reopen_task = asyncio.ensure_future(self._reopen_loop())

    async def _reopen_loop(self) -> None:
        delay = RESUBSCRIBE_DELAY
        try:
            while self._state == "unverified":
                try:
                    if self._unsubscribing is not None:
                        with contextlib.suppress(Exception):
                            await self._unsubscribing
                    if self._registered:
                        try:
                            await self._conn.execute(
                                _meta.unsubscribe(self.id), timeout=self._timeout
                            )
                        except Exception as e:
                            if _transient(e):
                                raise
                            # Gone already on the server (reset, or a new connection).
                        self._registered = False
                    await self._open("resync")
                    state: _State = self._state  # _open moved it on
                    if state == "live":
                        self._stats.resubscribes += 1
                    return
                except Exception as e:
                    if self._state != "unverified":
                        return
                    if not _transient(e):
                        self._fail(e)
                        return
                # Jitter in [delay/2, delay], ended early by close().
                self._wake = asyncio.Event()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), delay * random.uniform(0.5, 1.0))
                self._wake = None
                delay = min(delay * 2, MAX_RESUBSCRIBE_DELAY)
        finally:
            self._reopen_task = None

    # ── Pushes ────────────────────────────────────────────────────────

    def _on_push(self, push: SubscriptionPush) -> None:
        if self._early is not None:
            self._early.append(push)
            return
        if self._state != "live":
            return
        try:
            self._take(push)
        except Exception as e:
            self._fail(e)

    def _take(self, push: SubscriptionPush) -> None:
        if isinstance(push, SubscriptionDeltaResponse):
            if self._stream is not None:
                return self._unverified("broken_stream")
            if push.seq != self._seq + 1:
                return self._unverified("seq_gap")
            return self._apply(push.seq, push.revision, push.columns, push.inserted, push.retracted)
        if isinstance(push, SubscriptionDeltaStartResponse):
            if self._stream is not None:
                return self._unverified("broken_stream")
            if push.seq != self._seq + 1:
                return self._unverified("seq_gap")
            self._stream = _DeltaStream(start=push)
            return None
        if isinstance(push, SubscriptionDeltaChunkResponse):
            stream = self._stream
            if stream is None or push.seq != stream.start.seq or push.chunk_index != stream.chunks:
                return self._unverified("broken_stream")
            stream.chunks += 1
            stream.inserted.extend(push.inserted)
            stream.retracted.extend(push.retracted)
            return None
        if isinstance(push, SubscriptionDeltaEndResponse):
            stream, self._stream = self._stream, None
            if (
                stream is None
                or push.seq != stream.start.seq
                or push.chunk_count != stream.chunks
                or push.inserted_count != len(stream.inserted)
                or push.retracted_count != len(stream.retracted)
            ):
                return self._unverified("broken_stream")
            start = stream.start
            return self._apply(
                start.seq, start.revision, start.columns, stream.inserted, stream.retracted
            )
        if isinstance(push, SubscriptionResetResponse):
            # The server removed the subscription; the id is free.
            return self._unverified("subscription_reset", push.message, registered=False)
        if isinstance(push, SubscriptionErrorResponse):
            return self._unverified("subscription_error", push.message)
        return None

    def _result_of(self, columns: list[str], rows: list[list[Any]]) -> dict[Any, _Held]:
        result: dict[Any, _Held] = {}
        labels = self._shape.labels(columns)
        for values in self._shape.project(rows):
            key = _row_key(values)
            held = result.get(key)
            if held is not None:
                held.count += 1
            else:
                result[key] = _Held(self._shape.row(labels, values), 1)
        return result

    def _apply(
        self,
        seq: int,
        revision: int,
        columns: list[str],
        inserted: list[list[Any]],
        retracted: list[list[Any]],
    ) -> None:
        """Apply one whole delta of engine rows to the held result."""
        if len(self._queue) >= self._capacity:
            return self._unverified("slow_consumer")
        labels = self._shape.labels(columns)
        # Count before and after, so a row whose support only moved is no change.
        touched: dict[Any, tuple[int, _Held]] = {}
        for values in self._shape.project(retracted):
            key = _row_key(values)
            held = self._held.get(key)
            if held is None or held.count == 0:
                return self._unverified("broken_stream")
            touched.setdefault(key, (held.count, held))
            held.count -= 1
        for values in self._shape.project(inserted):
            key = _row_key(values)
            held = self._held.get(key)
            if held is None:
                held = self._held[key] = _Held(self._shape.row(labels, values), 0)
            touched.setdefault(key, (held.count, held))
            held.count += 1
        change: Change[T] = Change("delta", [], [], revision, seq, True)
        for key, (before, held) in touched.items():
            if held.count == 0:
                del self._held[key]
            if before == 0 and held.count > 0:
                change.inserted.append(held.row)
            elif before > 0 and held.count == 0:
                change.retracted.append(held.row)
        self._seq = seq
        self._revision = revision
        if change.inserted or change.retracted:
            self._push(change)
        return None

    def _unverified(
        self,
        reason: UnverifiedReason,
        message: str | None = None,
        *,
        registered: bool | None = None,
    ) -> None:
        """The held result is no longer known to be current: say so at once,
        then open again (after the consumer catches up, for ``slow_consumer``)."""
        if self._state != "live":
            # Already unverified, or still opening (a failed open is retried or reported).
            if reason == "connection_lost" and self._state == "unverified":
                self._registered = False
            return
        self._state = "unverified"
        self._set_route(None)
        self._stream = None
        if registered is not None:
            self._registered = registered
        self._push(Change("unverified", [], [], self._revision, self._seq, False, reason, message))
        if reason == "slow_consumer":
            # Stop the server pushing what would be dropped.
            if self._registered:
                self._unsubscribe()
            self._reopen_when_drained = True
            return
        self._reopen()

    def _on_link(self, change: LinkChange, code: str | None) -> None:
        if change == "lost":
            # The server dropped the subscription with the connection.
            self._unverified("connection_lost", registered=False)
            if self._state == "opening":
                self._registered = False
        elif change == "given_up":
            self._fail(
                ConnectionLost(
                    "The connection was lost and not reconnected; the subscription ended",
                    code=code,
                )
            )
        else:
            # The client closed the connection: what is queued can still be read.
            self._registered = False
            self._end()

    # ── Delivery and ending ───────────────────────────────────────────

    def _push(self, change: Change[T]) -> None:
        self._queue.append(change)
        self._signal().set()

    def _fail(self, error: BaseException) -> None:
        """End with *error*, raised by the next ``__anext__`` once the queue is read."""
        if self._state == "closed":
            return
        self._end()
        self._error = error

    def _end(self) -> None:
        registered = self._registered
        self._state = "closed"
        self._set_route(None)
        self._early = None
        self._reopen_when_drained = False
        if self._wake is not None:
            self._wake.set()
        self._conn.remove_link_listener(self._on_link)
        if registered:
            self._unsubscribe()
        self._signal().set()

    def _set_route(self, route: SubscriptionRoute | None) -> None:
        old = self._route
        if old is not None:
            self._stale_before += old.stale
            self._conn.remove_route(self.id, old)
        self._route = route

    def _unsubscribe(self) -> None:
        """Best effort: the server also drops the subscription with the
        connection. Opening again waits for it, so the id is free when
        ``.subscribe`` reuses it."""
        self._registered = False
        if not self._conn.connected:
            return

        async def unsubscribe() -> None:
            with contextlib.suppress(Exception):  # gone already, or the connection is ending
                await self._conn.execute(_meta.unsubscribe(self.id), timeout=self._timeout)

        self._unsubscribing = asyncio.ensure_future(unsubscribe())


def _difference(before: dict[Any, _Held], after: dict[Any, _Held]) -> tuple[list[Any], list[Any]]:
    """Rows to insert and retract to turn *before* into *after*."""
    inserted = [held.row for key, held in after.items() if key not in before]
    retracted = [held.row for key, held in before.items() if key not in after]
    return inserted, retracted


_TRANSIENT = (
    ConnectionLost,
    DeadlineExceeded,
    Cancelled,
    RateLimited,
    InternalError,
    OutcomeUnknownError,
)


def _transient(error: BaseException) -> bool:
    """Errors after which opening again may work."""
    return isinstance(error, _TRANSIENT)


_REFUSALS: tuple[tuple[re.Pattern[str], SubscriptionRejectedReason], ...] = (
    (re.compile(r"limit/offset"), "limit_offset"),
    (re.compile(r"max_result_rows"), "result_cap"),
    (re.compile(r"access denied|permission", re.IGNORECASE), "access_denied"),
    (re.compile(r"already exists on this connection"), "id_taken"),
    (re.compile(r"Subscription limit reached"), "subscription_limit"),
)


def _refusal(error: Exception, program: str) -> Exception:
    """The engine's refusal of a ``.subscribe`` as a typed rejection; other errors as they are."""
    if not isinstance(error, QueryError) or _transient(error):
        return error
    message = error.message
    reason: SubscriptionRejectedReason = next(
        (reason for pattern, reason in _REFUSALS if pattern.search(message)), "rejected"
    )
    return SubscriptionRejected(message, reason, query=program)


# ── Levels and callbacks ────────────────────────────────────────────────


async def watch_changes(sub: Subscription[T]) -> AsyncIterator[Live[T]]:
    """The whole current result each time it changes; see ``KnowledgeGraph.watch``."""
    rows: dict[Any, T] = {}
    try:
        async for change in sub:
            if change.kind == "unverified":
                yield Live(list(rows.values()), change.revision, False, change.reason)
                continue
            for row in change.retracted:
                rows.pop(row_identity(row), None)
            for row in change.inserted:
                rows[row_identity(row)] = row
            yield Live(list(rows.values()), change.revision, True)
    finally:
        await sub.close()


class SubscriptionHandle:
    """A callback subscription; see ``KnowledgeGraph.on``."""

    def __init__(self, sub: Subscription[Any], task: asyncio.Task[None]) -> None:
        self._sub = sub
        self._task = task

    @property
    def stats(self) -> SubscriptionStats:
        return self._sub.stats

    @property
    def subscription(self) -> Subscription[Any]:
        return self._sub

    async def close(self) -> None:
        """End the subscription and wait for the callback in progress."""
        await self._sub.close()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await self._task


def run_callback(
    sub: Subscription[T],
    callback: Callable[[Change[T]], Any],
    on_error: Callable[[BaseException], Any] | None = None,
) -> SubscriptionHandle:
    """Feed every change of *sub* to *callback*, one at a time."""

    def report(error: BaseException) -> None:
        if on_error is not None:
            on_error(error)
        else:
            logger.error("Subscription callback failed", exc_info=error)

    async def run() -> None:
        try:
            async for change in sub:
                try:
                    result = callback(change)
                    if inspect.isawaitable(result):
                        await result
                except Exception as e:
                    report(e)
        except Exception as e:
            report(e)

    return SubscriptionHandle(sub, asyncio.ensure_future(run()))
