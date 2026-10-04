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

``kg.subscribe_group({...})`` holds several queries as one subscription
group: its ``GroupChange`` events have the same kinds and reasons, with each
member's ``inserted``, ``retracted`` and ``unchanged`` under its name, and
after every verified event all members are exact at the event's one
``revision``. ``kg.read({...})`` answers several queries once, all at one
revision.

The JavaScript SDK's ``kg.subscribe()``, ``kg.subscribeGroup()`` and
``kg.read()`` yield the same events, with the same kinds, fields and reasons.
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
    NamedQuery,
    SnapshotResponse,
    SubscriptionDeltaChunkResponse,
    SubscriptionDeltaEndResponse,
    SubscriptionDeltaResponse,
    SubscriptionDeltaStartResponse,
    SubscriptionErrorResponse,
    SubscriptionGroupDeltaChunkResponse,
    SubscriptionGroupDeltaEndResponse,
    SubscriptionGroupDeltaResponse,
    SubscriptionGroupDeltaStartResponse,
    SubscriptionPush,
    SubscriptionResetResponse,
)
from inputlayer.connection import describe_queries
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
    from inputlayer._protocol import GroupMemberDeltaHeader, ResultResponse
    from inputlayer.compiler import QueryPlan
    from inputlayer.connection import Connection, LinkChange, SubscriptionRoute

logger = logging.getLogger("inputlayer")

T = TypeVar("T")
E = TypeVar("E")

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
class MemberChange(Generic[T]):
    """What one member of a subscription group did in a ``GroupChange``."""

    #: Rows that entered the member's result.
    inserted: list[T]
    #: Rows that left the member's result.
    retracted: list[T]
    #: True when both lists are empty: the member's result is the same at the
    #: event's revision as before it.
    unchanged: bool


@dataclass(frozen=True)
class GroupChange:
    """One event of a subscription group; see ``KnowledgeGraph.subscribe_group``.

    ``kind``, ``revision``, ``seq``, ``verified``, ``reason`` and ``message``
    mean what they mean on ``Change``, for the whole group. ``members`` holds
    every member, by name, in the group's order. After every verified event,
    each member's result (every event before it applied) is its query's
    exact answer at ``revision``.
    """

    kind: ChangeKind
    revision: int
    seq: int
    verified: bool
    reason: UnverifiedReason | None = None
    message: str | None = None
    members: dict[str, MemberChange[Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class ReadResult:
    """The results of ``KnowledgeGraph.read``, all exact at one revision."""

    #: The knowledge graph revision every result is the exact answer at.
    revision: int
    #: Each query's rows by its name, in the engine's order (a projection's
    #: repeated rows once).
    results: dict[str, list[Any]]
    #: Names of the results whose rows were cut, by their ``limit`` or by the
    #: engine's result cap (``max_result_rows``), in request order; empty when
    #: nothing was cut.
    truncated: list[str] = field(default_factory=list)


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

    def rows(self, columns: list[str], rows: list[list[Any]]) -> list[Any]:
        """The rows of a one-off result, in the engine's order; a projected
        row that several engine rows support comes once."""
        labels = self.labels(columns)
        seen: set[Any] = set()
        out: list[Any] = []
        for values in self.project(rows):
            key = _row_key(values)
            if key not in seen:
                seen.add(key)
                out.append(self.row(labels, values))
        return out


def iql_shape(iql: str, *, read: bool = False) -> Shape:
    """The shape of a raw ``?...`` query: rows keyed by the engine's column names."""
    query = iql.strip()
    if not query.startswith("?") or "\n" in query:
        if read:
            raise SubscriptionRejected(
                "A read query is one ?-query; to read several rules' union, define "
                "a persistent rule with one clause per branch and read it.",
                "rejected",
            )
        raise SubscriptionRejected(
            "A subscription stands on one ?-query; to subscribe to several rules' "
            "union, define a persistent rule with one clause per branch and "
            "subscribe to it.",
            "rejected",
        )
    return Shape(query=query)


def plan_shape(
    plan: QueryPlan, relation_cls: type | None, *, aggregate: bool, read: bool = False
) -> Shape:
    """The shape of a compiled query, refusing what a standing query (or a
    read, which sees what a standing query sees) cannot track."""
    who, verb = ("reads", "read") if read else ("subscriptions", "subscribe to")
    if aggregate:
        raise SubscriptionRejected(
            "An aggregate query is evaluated through a rule that lives only for one "
            f"request, and {who} see persistent rules only. Define the "
            f"aggregate as a persistent rule (kg.define_rules) and {verb} that "
            "relation.",
            "session_view",
        )
    if plan.setup:
        raise SubscriptionRejected(
            "A negation of a constant binds the constant through a session fact, "
            f"and {who} see persistent data only. Define the condition in a "
            f"persistent rule (kg.define_rules) and {verb} that relation.",
            "session_view",
        )
    statements = plan.program.split("\n")
    if len(statements) > 1:
        if plan.dedupe and (plan.limit is not None or plan.offset is not None):
            raise SubscriptionRejected(
                "A page (limit or offset) of a projection is collected through a rule "
                f"that lives only for one request, and {who} see persistent rules "
                "only. Select every column of the relations, or drop limit and offset.",
                "limit_offset",
            )
        raise SubscriptionRejected(
            "An OR condition splits the query into branches that a standing query "
            "cannot hold under one revision. Define a persistent rule with one clause "
            f"per branch (kg.define_rules) and {verb} it.",
            "or_branches",
        )
    return Shape(query=statements[0], plan=plan, relation_cls=relation_cls)


def check_names(names: list[str]) -> None:
    """A read or a group names at least one query, each by a non-empty string."""
    if not names:
        raise SubscriptionRejected("Name at least one query.", "rejected")
    for name in names:
        if not isinstance(name, str) or not name:
            raise SubscriptionRejected(
                f"Query names are non-empty strings, not {name!r}.", "rejected"
            )


def _check_results(reply: SnapshotResponse, names: list[str]) -> None:
    """A snapshot holds one result per query, in request order."""
    got = [result.name for result in reply.results]
    if got != names:
        raise InternalError(
            f"The snapshot holds results {got}, for queries {names}: {reply.id or ''}"
        )


async def check_persistent(
    shapes: Mapping[str, Shape],
    session_rules: Callable[[], Awaitable[list[str]]] | None,
    *,
    read: bool = False,
) -> None:
    """Session rules are invisible to subscriptions (the engine would never
    push) and to reads (the engine would answer nothing): refuse a query
    that reads one."""
    if session_rules is None or not any(shape.relations for shape in shapes.values()):
        return
    heads = {rule.lstrip("+").split("(", 1)[0].strip() for rule in await session_rules()}
    for name, shape in shapes.items():
        session = next((r for r in shape.relations if r in heads), None)
        if session is None:
            continue
        if read:
            raise SubscriptionRejected(
                f"Query '{name}': '{session}' is a session rule, and reads see persistent "
                "data only. Define it as a persistent rule (kg.define_rules) to read it.",
                "session_view",
                query=shape.query,
            )
        raise SubscriptionRejected(
            f"'{session}' is a session rule, and subscriptions see persistent data "
            "only. Define it as a persistent rule (kg.define_rules) to subscribe to it.",
            "session_view",
            query=shape.query,
        )


async def read(
    conn: Connection,
    shapes: dict[str, Shape],
    *,
    timeout: float | None = None,
    session_rules: Callable[[], Awaitable[list[str]]] | None = None,
) -> ReadResult:
    """Run every query of *shapes* on one snapshot; see ``KnowledgeGraph.read``.

    The check for session rules runs alongside the read, so it adds no round
    trip; a refusal discards the read's answer."""
    names = list(shapes)
    check_names(names)
    queries = [NamedQuery(name, shape.query) for name, shape in shapes.items()]
    checked, answered = await asyncio.gather(
        check_persistent(shapes, session_rules, read=True),
        conn.read(queries, timeout=timeout),
        return_exceptions=True,
    )
    if isinstance(checked, BaseException):
        raise checked
    if isinstance(answered, BaseException):
        if isinstance(answered, QueryError):
            answered.query = describe_queries(queries)
        raise answered
    reply = answered
    _check_results(reply, names)
    if reply.subscribed is not None:
        raise InternalError(f"The reply to a read names a subscription: {reply.subscribed}")
    return ReadResult(
        revision=reply.revision,
        results={
            result.name: shapes[result.name].rows(result.columns, result.rows)
            for result in reply.results
        },
        truncated=[result.name for result in reply.results if result.truncated],
    )


# ── Held result ─────────────────────────────────────────────────────────


@dataclass
class _Held:
    row: Any
    #: Engine rows behind this projected row.
    count: int


class _Result:
    """The held result of one standing query: each projected row, with the
    count of engine rows behind it."""

    def __init__(self, shape: Shape) -> None:
        self.shape = shape
        self.held: dict[Any, _Held] = {}

    @classmethod
    def of(cls, shape: Shape, columns: list[str], rows: list[list[Any]]) -> _Result:
        result = cls(shape)
        labels = shape.labels(columns)
        for values in shape.project(rows):
            key = _row_key(values)
            held = result.held.get(key)
            if held is not None:
                held.count += 1
            else:
                result.held[key] = _Held(shape.row(labels, values), 1)
        return result

    def rows(self) -> list[Any]:
        return [held.row for held in self.held.values()]

    def difference(self, after: _Result) -> tuple[list[Any], list[Any]]:
        """Rows to insert and retract to turn this result into *after*."""
        inserted = [held.row for key, held in after.held.items() if key not in self.held]
        retracted = [held.row for key, held in self.held.items() if key not in after.held]
        return inserted, retracted

    def holds(self, retracted: list[list[Any]]) -> bool:
        """Whether every projected row of *retracted* is held at least as many
        times as it is retracted: anything else is a delta of another result."""
        needed: dict[Any, int] = {}
        for values in retracted:
            key = _row_key(values)
            needed[key] = needed.get(key, 0) + 1
        return all(
            (held := self.held.get(key)) is not None and held.count >= n
            for key, n in needed.items()
        )

    def apply(
        self, columns: list[str], inserted: list[list[Any]], retracted: list[list[Any]]
    ) -> tuple[list[Any], list[Any]]:
        """Apply projected rows (*retracted* checked by ``holds``); returns the
        rows that entered and left: those whose count moved from or to 0."""
        labels = self.shape.labels(columns)
        # Count before and after, so a row whose support only moved is no change.
        touched: dict[Any, tuple[int, _Held]] = {}
        for values in retracted:
            key = _row_key(values)
            held = self.held[key]
            touched.setdefault(key, (held.count, held))
            held.count -= 1
        for values in inserted:
            key = _row_key(values)
            held_or_none = self.held.get(key)
            if held_or_none is None:
                held_or_none = self.held[key] = _Held(self.shape.row(labels, values), 0)
            touched.setdefault(key, (held_or_none.count, held_or_none))
            held_or_none.count += 1
        entered: list[Any] = []
        left: list[Any] = []
        for key, (before, held) in touched.items():
            if held.count == 0:
                del self.held[key]
            if before == 0 and held.count > 0:
                entered.append(held.row)
            elif before > 0 and held.count == 0:
                left.append(held.row)
        return entered, left


_State = Literal["idle", "opening", "live", "unverified", "closed"]


class _Standing(Generic[E]):
    """What a subscription and a subscription group share: opening (and
    opening again with backoff), routing pushes by generation, the event
    queue and its bound, losing and closing the connection, unsubscribing.

    A subclass sends the subscribe (``_request``), takes its snapshot as the
    first event or a resync (``_take_snapshot``), takes pushes (``_take``),
    and makes the ``unverified`` event.
    """

    def __init__(
        self,
        conn: Connection,
        *,
        queue: int = DEFAULT_QUEUE,
        timeout: float | None = None,
        session_rules: Callable[[], Awaitable[list[str]]] | None = None,
    ) -> None:
        #: The id the subscription has on the server.
        self.id = _subscription_id()
        self._conn = conn
        self._capacity = max(1, queue)
        self._timeout = timeout
        self._session_rules = session_rules

        self._state: _State = "idle"
        self._route: SubscriptionRoute | None = None
        self._stale_before = 0
        # The server holds the id: .unsubscribe before opening it again.
        self._registered = False
        self._revision = 0
        self._seq = 0
        # A streamed delta being assembled.
        self._stream: Any = None
        # Pushes that arrive while a subscribe reply is being taken.
        self._early: list[SubscriptionPush] | None = None
        self._queue: deque[E] = deque()
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
    def stats(self) -> SubscriptionStats:
        stale = self._stale_before + (self._route.stale if self._route is not None else 0)
        return SubscriptionStats(stale_dropped=stale, resubscribes=self._stats.resubscribes)

    # ── What a subclass provides ──────────────────────────────────────

    def _shapes(self) -> Mapping[str, Shape]:
        """The queries the subscription stands on, by name."""
        raise NotImplementedError

    def _request_text(self) -> str:
        """The request a refusal names."""
        raise NotImplementedError

    async def _request(self) -> Any:
        """Send the subscribe and return its reply."""
        raise NotImplementedError

    def _take_snapshot(self, reply: Any, kind: Literal["snapshot", "resync"]) -> int:
        """Check the reply, queue its event and hold its result; returns its revision.
        Nothing is queued or held when it raises."""
        raise NotImplementedError

    def _take(self, push: SubscriptionPush) -> None:
        raise NotImplementedError

    def _unverified_event(self, reason: UnverifiedReason, message: str | None) -> E:
        raise NotImplementedError

    # ── Iteration ─────────────────────────────────────────────────────

    def __aiter__(self) -> AsyncIterator[E]:
        return self._events()

    async def _events(self) -> AsyncIterator[E]:
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

    async def __anext__(self) -> E:
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
            await self._open_with_backoff("snapshot")
        except BaseException:
            self._end()
            raise

    async def _check_persistent(self) -> None:
        """Session rules are invisible to subscriptions: the engine would never push."""
        await check_persistent(self._shapes(), self._session_rules)

    async def _open(self, kind: Literal["snapshot", "resync"]) -> None:
        """Send the subscribe and take its snapshot as the first event or as a resync."""
        self._early = []
        self._set_route(self._conn.add_route(self.id, self._on_push))
        try:
            reply = await self._request()
        except BaseException as e:
            self._set_route(None)
            self._early = None
            if isinstance(e, (OutcomeUnknownError, InternalError)):
                # It may have registered although no whole reply came.
                self._registered = True
            refused = _refusal(e, self._request_text()) if isinstance(e, Exception) else e
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
            revision = self._take_snapshot(reply, kind)
        except BaseException:
            self._set_route(None)
            self._early = None
            self._unsubscribe()
            raise
        self._revision = revision
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
        try:
            await self._open_with_backoff("resync")
            state: _State = self._state  # _open moved it on
            if state == "live":
                self._stats.resubscribes += 1
        finally:
            self._reopen_task = None

    async def _open_with_backoff(self, kind: Literal["snapshot", "resync"]) -> None:
        """Open until it works, is refused, or the subscription ends."""
        waiting: _State = "opening" if kind == "snapshot" else "unverified"
        delay = RESUBSCRIBE_DELAY
        while self._state == waiting:
            try:
                if self._unsubscribing is not None:
                    with contextlib.suppress(Exception):
                        await self._unsubscribing
                if self._registered:
                    try:
                        await self._conn.execute(_meta.unsubscribe(self.id), timeout=self._timeout)
                    except Exception as e:
                        if _transient(e):
                            raise
                        # Gone already on the server (reset, or a new connection).
                    self._registered = False
                if kind == "snapshot":
                    await self._check_persistent()
                await self._open(kind)
                return
            except Exception as e:
                if self._state != waiting:
                    return
                if not _transient(e) or self._conn.closed_for_good:
                    self._fail(e)
                    return
            # Jitter in [delay/2, delay], ended early by close().
            self._wake = asyncio.Event()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._wake.wait(), delay * random.uniform(0.5, 1.0))
            self._wake = None
            delay = min(delay * 2, MAX_RESUBSCRIBE_DELAY)

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

    def _take_status(self, push: SubscriptionPush) -> bool:
        """Take a ``subscription_reset`` or ``subscription_error``; False for any other push."""
        if isinstance(push, SubscriptionResetResponse):
            # The server removed the subscription; the id is free.
            self._unverified("subscription_reset", push.message, registered=False)
            return True
        if isinstance(push, SubscriptionErrorResponse):
            self._unverified("subscription_error", push.message)
            return True
        return False

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
        self._push(self._unverified_event(reason, message))
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

    def _push(self, change: E) -> None:
        self._queue.append(change)
        self._signal().set()

    def _full(self) -> bool:
        """The consumer is ``queue`` events behind: one more is ``slow_consumer``."""
        return len(self._queue) >= self._capacity

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
        connection. Opening again waits for it, so the id is free when the
        subscribe reuses it."""
        self._registered = False
        if not self._conn.connected:
            return

        async def unsubscribe() -> None:
            with contextlib.suppress(Exception):  # gone already, or the connection is ending
                await self._conn.execute(_meta.unsubscribe(self.id), timeout=self._timeout)

        self._unsubscribing = asyncio.ensure_future(unsubscribe())


# ── One standing query ──────────────────────────────────────────────────


@dataclass
class _DeltaStream:
    start: SubscriptionDeltaStartResponse
    inserted: list[list[Any]] = field(default_factory=list)
    retracted: list[list[Any]] = field(default_factory=list)
    chunks: int = 0


class Subscription(_Standing[Change[T]]):
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
        super().__init__(conn, queue=queue, timeout=timeout, session_rules=session_rules)
        self._shape = shape
        self._result = _Result(shape)

    @property
    def query(self) -> str:
        """The ``?...`` query the subscription stands on."""
        return self._shape.query

    async def __aenter__(self) -> Subscription[T]:
        return self

    def _shapes(self) -> Mapping[str, Shape]:
        return {"query": self._shape}

    def _request_text(self) -> str:
        return _meta.subscribe(self.id, self._shape.query)

    async def _request(self) -> ResultResponse:
        return await self._conn.execute(self._request_text(), timeout=self._timeout)

    def _take_snapshot(self, reply: ResultResponse, kind: Literal["snapshot", "resync"]) -> int:
        subscribed = reply.subscribed
        if subscribed is None:
            raise InternalError(f"The .subscribe reply names no subscription: {reply!r}")
        fresh = _Result.of(self._shape, reply.columns, reply.rows)
        if kind == "snapshot":
            self._push(Change(kind, fresh.rows(), [], subscribed.revision, 0, True))
        else:
            inserted, retracted = self._result.difference(fresh)
            self._push(Change(kind, inserted, retracted, subscribed.revision, 0, True))
        self._result = fresh
        return subscribed.revision

    def _unverified_event(self, reason: UnverifiedReason, message: str | None) -> Change[T]:
        return Change("unverified", [], [], self._revision, self._seq, False, reason, message)

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
        if self._take_status(push):
            return None
        # A group's delta, under this subscription's name and generation.
        return self._unverified("broken_stream")

    def _apply(
        self,
        seq: int,
        revision: int,
        columns: list[str],
        inserted: list[list[Any]],
        retracted: list[list[Any]],
    ) -> None:
        """Apply one whole delta of engine rows to the held result."""
        if self._full():
            return self._unverified("slow_consumer")
        picked_in = self._shape.project(inserted)
        picked_out = self._shape.project(retracted)
        if not self._result.holds(picked_out):
            return self._unverified("broken_stream")
        entered, left = self._result.apply(columns, picked_in, picked_out)
        self._seq = seq
        self._revision = revision
        if entered or left:
            self._push(Change("delta", entered, left, revision, seq, True))
        return None


# ── A subscription group ────────────────────────────────────────────────


@dataclass
class _GroupStream:
    start: SubscriptionGroupDeltaStartResponse
    #: Each member's rows so far, in group order.
    inserted: list[list[list[Any]]]
    retracted: list[list[list[Any]]]
    chunks: int = 0
    # The member the last chunk held: members stream in order.
    member: int = 0

    def complete(self, member: int) -> bool:
        header = self.start.members[member]
        return (len(self.inserted[member]), len(self.retracted[member])) == (
            header.inserted_count,
            header.retracted_count,
        )


def _marked(header: GroupMemberDeltaHeader) -> bool:
    """A member is ``unchanged`` exactly when it has no rows."""
    return header.unchanged == (header.inserted_count == 0 and header.retracted_count == 0)


class GroupSubscription(_Standing[GroupChange]):
    """Several standing queries kept current together, as an async iterator
    of ``GroupChange`` events; see ``KnowledgeGraph.subscribe_group``.

    It opens, closes and ends like ``Subscription``. Its members are
    subscribed under one name, in the order of the mapping it was made from.
    """

    def __init__(
        self,
        conn: Connection,
        members: dict[str, Shape],
        *,
        queue: int = DEFAULT_QUEUE,
        timeout: float | None = None,
        session_rules: Callable[[], Awaitable[list[str]]] | None = None,
    ) -> None:
        check_names(list(members))
        super().__init__(conn, queue=queue, timeout=timeout, session_rules=session_rules)
        self._members = dict(members)
        self._names = list(members)
        self._results = {name: _Result(shape) for name, shape in members.items()}

    @property
    def queries(self) -> dict[str, str]:
        """Each member's ``?...`` query, by name, in group order."""
        return {name: shape.query for name, shape in self._members.items()}

    async def __aenter__(self) -> GroupSubscription:
        return self

    def _named_queries(self) -> list[NamedQuery]:
        return [NamedQuery(name, shape.query) for name, shape in self._members.items()]

    def _shapes(self) -> Mapping[str, Shape]:
        return self._members

    def _request_text(self) -> str:
        return describe_queries(self._named_queries())

    async def _request(self) -> SnapshotResponse:
        return await self._conn.subscribe(self.id, self._named_queries(), timeout=self._timeout)

    def _take_snapshot(self, reply: SnapshotResponse, kind: Literal["snapshot", "resync"]) -> int:
        subscribed = reply.subscribed
        if subscribed is None:
            raise InternalError(f"The subscribe reply names no subscription: {reply!r}")
        _check_results(reply, self._names)
        fresh = {
            result.name: _Result.of(self._members[result.name], result.columns, result.rows)
            for result in reply.results
        }
        members: dict[str, MemberChange[Any]] = {}
        for name, result in fresh.items():
            inserted: list[Any]
            retracted: list[Any]
            if kind == "snapshot":
                inserted, retracted = result.rows(), []
            else:
                inserted, retracted = self._results[name].difference(result)
            members[name] = MemberChange(inserted, retracted, not inserted and not retracted)
        self._push(GroupChange(kind, subscribed.revision, 0, True, members=members))
        self._results = fresh
        return subscribed.revision

    def _unverified_event(self, reason: UnverifiedReason, message: str | None) -> GroupChange:
        return GroupChange(
            "unverified",
            self._revision,
            self._seq,
            False,
            reason,
            message,
            {name: MemberChange([], [], True) for name in self._names},
        )

    def _take(self, push: SubscriptionPush) -> None:
        if isinstance(push, SubscriptionGroupDeltaResponse):
            if self._stream is not None:
                return self._unverified("broken_stream")
            if push.seq != self._seq + 1:
                return self._unverified("seq_gap")
            if any(m.unchanged != (not m.inserted and not m.retracted) for m in push.members):
                return self._unverified("broken_stream")
            return self._apply(
                push.seq,
                push.revision,
                [(m.name, m.columns, m.inserted, m.retracted) for m in push.members],
            )
        if isinstance(push, SubscriptionGroupDeltaStartResponse):
            if self._stream is not None:
                return self._unverified("broken_stream")
            if push.seq != self._seq + 1:
                return self._unverified("seq_gap")
            names = [m.name for m in push.members]
            if names != self._names or not all(_marked(m) for m in push.members):
                return self._unverified("broken_stream")
            self._stream = _GroupStream(
                start=push,
                inserted=[[] for _ in names],
                retracted=[[] for _ in names],
            )
            return None
        if isinstance(push, SubscriptionGroupDeltaChunkResponse):
            stream = self._stream
            if (
                not isinstance(stream, _GroupStream)
                or push.seq != stream.start.seq
                or push.chunk_index != stream.chunks
                or not stream.member <= push.member < len(self._names)
                or not all(stream.complete(m) for m in range(stream.member, push.member))
                or not (push.inserted or push.retracted)
            ):
                return self._unverified("broken_stream")
            header = stream.start.members[push.member]
            inserted = stream.inserted[push.member]
            retracted = stream.retracted[push.member]
            inserted.extend(push.inserted)
            retracted.extend(push.retracted)
            if len(inserted) > header.inserted_count or len(retracted) > header.retracted_count:
                return self._unverified("broken_stream")
            stream.chunks += 1
            stream.member = push.member
            return None
        if isinstance(push, SubscriptionGroupDeltaEndResponse):
            stream, self._stream = self._stream, None
            if (
                not isinstance(stream, _GroupStream)
                or push.seq != stream.start.seq
                or push.chunk_count != stream.chunks
                or not all(stream.complete(m) for m in range(len(self._names)))
            ):
                return self._unverified("broken_stream")
            start = stream.start
            return self._apply(
                start.seq,
                start.revision,
                [
                    (m.name, m.columns, stream.inserted[i], stream.retracted[i])
                    for i, m in enumerate(start.members)
                ],
            )
        if self._take_status(push):
            return None
        # A plain subscription's delta, under this group's name and generation.
        return self._unverified("broken_stream")

    def _apply(
        self,
        seq: int,
        revision: int,
        members: list[tuple[str, list[str], list[list[Any]], list[list[Any]]]],
    ) -> None:
        """Apply one whole group delta of engine rows to every member, or none."""
        if self._full():
            return self._unverified("slow_consumer")
        if [name for name, _, _, _ in members] != self._names:
            return self._unverified("broken_stream")
        picked: list[tuple[_Result, list[str], list[list[Any]], list[list[Any]]]] = []
        for name, columns, inserted, retracted in members:
            result = self._results[name]
            picked_out = result.shape.project(retracted)
            if not result.holds(picked_out):
                return self._unverified("broken_stream")
            picked.append((result, columns, result.shape.project(inserted), picked_out))
        changes: dict[str, MemberChange[Any]] = {}
        for name, (result, columns, picked_in, picked_out) in zip(
            self._names, picked, strict=True
        ):
            entered, left = result.apply(columns, picked_in, picked_out)
            changes[name] = MemberChange(entered, left, not entered and not left)
        self._seq = seq
        self._revision = revision
        if not all(change.unchanged for change in changes.values()):
            self._push(GroupChange("delta", revision, seq, True, members=changes))
        return None


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
    """The engine's refusal of a subscribe as a typed rejection; other errors as they are."""
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
