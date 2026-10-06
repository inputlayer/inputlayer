"""WebSocket wire protocol: message serialization and deserialization.

Matches the AsyncAPI spec at ``docs/spec/asyncapi.yaml`` (protocol version 5,
defined by the ``inputlayer-ws-protocol`` crate).

Any request may carry an ``id``; every reply to it (``authenticated``,
``auth_error``, ``result``, ``result_start``/``result_chunk``/``result_end``,
``snapshot``, ``snapshot_start``/``snapshot_chunk``/``snapshot_end``,
``error``, ``pong``, ``cancel_ack``) echoes it. Pushes (notifications, subscription deltas) and
``notice`` frames never carry one and are never replies.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

PROTOCOL_VERSION = 5
"""The ``/ws`` protocol version this SDK speaks (``authenticated.protocol_version``)."""

PARAMS_PROTOCOL_VERSION = 4
"""The first protocol version whose ``execute`` takes ``params``."""

GROUPS_PROTOCOL_VERSION = 5
"""The first protocol version with ``read`` and ``subscribe`` (subscription groups)."""


def _with_id(frame: dict[str, Any], request_id: str | None) -> str:
    if request_id is not None:
        frame["id"] = request_id
    # A parameter is never NaN or infinite; refuse to write one, never `NaN`.
    return json.dumps(frame, allow_nan=False)

# ── Client → Server messages ──────────────────────────────────────────

@dataclass(frozen=True)
class LoginMessage:
    username: str
    password: str
    id: str | None = None

    def to_json(self) -> str:
        return _with_id({
            "type": "login",
            "username": self.username,
            "password": self.password,
        }, self.id)


@dataclass(frozen=True)
class AuthenticateMessage:
    api_key: str
    id: str | None = None

    def to_json(self) -> str:
        return _with_id({
            "type": "authenticate",
            "api_key": self.api_key,
        }, self.id)


@dataclass(frozen=True)
class ExecuteMessage:
    program: str
    id: str | None = None
    timeout_ms: int | None = None
    """The request's deadline, counted from when the server reads it (capped
    by the engine's own query timeout)."""
    params: dict[str, Any] | None = None
    """Values of the program's ``$name`` references, bound by the engine
    without being parsed (protocol version 4)."""

    def to_json(self) -> str:
        frame: dict[str, Any] = {"type": "execute", "program": self.program}
        if self.params:
            frame["params"] = self.params
        if self.timeout_ms is not None:
            frame["timeout_ms"] = self.timeout_ms
        return _with_id(frame, self.id)


@dataclass(frozen=True)
class NamedQuery:
    """One query of a ``read`` or ``subscribe``, and the name its result goes by."""

    name: str
    query: str


def _queries(queries: tuple[NamedQuery, ...]) -> list[dict[str, str]]:
    return [{"name": q.name, "query": q.query} for q in queries]


@dataclass(frozen=True)
class ReadMessage:
    """Run several ``?`` queries on one snapshot; answered by ``snapshot``.

    Persistent data only (no session facts or rules). Deadline and ``cancel``
    stop the whole read; it fails as a whole."""

    queries: tuple[NamedQuery, ...]
    id: str | None = None
    timeout_ms: int | None = None

    def to_json(self) -> str:
        frame: dict[str, Any] = {"type": "read", "queries": _queries(self.queries)}
        if self.timeout_ms is not None:
            frame["timeout_ms"] = self.timeout_ms
        return _with_id(frame, self.id)


@dataclass(frozen=True)
class SubscribeMessage:
    """Open the subscription group ``subscription``; answered by a ``snapshot``
    naming it. Ended by ``.unsubscribe <subscription>``."""

    subscription: str
    queries: tuple[NamedQuery, ...]
    id: str | None = None

    def to_json(self) -> str:
        return _with_id({
            "type": "subscribe",
            "subscription": self.subscription,
            "queries": _queries(self.queries),
        }, self.id)


@dataclass(frozen=True)
class CancelMessage:
    """Stop the unanswered request ``target``; answered by ``cancel_ack``."""

    target: str
    id: str | None = None

    def to_json(self) -> str:
        return _with_id({"type": "cancel", "target": self.target}, self.id)


@dataclass(frozen=True)
class PingMessage:
    id: str | None = None

    def to_json(self) -> str:
        return _with_id({"type": "ping"}, self.id)


# ── Server → Client messages ─────────────────────────────────────────

@dataclass(frozen=True)
class AuthenticatedResponse:
    session_id: str
    knowledge_graph: str
    version: str
    role: str
    protocol_version: int
    stream_epoch: str
    """This engine run's id; notification ``seq`` numbers belong to it. Pass it
    back with ``last_seq`` when reconnecting."""
    id: str | None = None


@dataclass(frozen=True)
class AuthErrorResponse:
    message: str
    id: str | None = None
    #: ``access_denied`` when the credential may not use the knowledge graph.
    code: ErrorCode | None = None


ErrorCode = Literal[
    "store_read_only",
    "validation",
    "not_found",
    "conflict",
    "unsupported",
    "internal",
    "invalid_request",
    "rate_limited",
    "deadline_exceeded",
    "cancelled",
    "outcome_unknown",
    "resource_exhausted",
    "replica_unconfirmed",
    "access_denied",
    "overloaded",
]
"""Why the engine rejected a statement or request (``code`` on ``error`` and ``errors[]``).

``invalid_request`` and ``rate_limited`` reject a whole request before it runs.
``deadline_exceeded`` and ``cancelled`` stop it before it began committing, so
nothing was applied; ``outcome_unknown`` means its commit failed in a way that
leaves the changes possibly applied: read the state back before retrying.
``resource_exhausted`` refuses a query over the engine's per-query memory limit
or its server-wide query memory budget, or a write past its knowledge graph's
memory budget; nothing was applied. ``replica_unconfirmed``: the write committed
on a primary shipping synchronously, but no replica confirmed it in time; it is
applied there, so do not retry it as a failed write. ``access_denied`` refuses
what the caller may not do (its role, write grants or API key scope do not allow
a statement, or its credential was revoked or has expired); nothing ran.
``overloaded`` refuses a request the engine could not admit to compute in time
(its lane's queue was full, or no compute permit came within the engine's
longest admission wait); nothing ran, so retry later with backoff."""


@dataclass(frozen=True)
class StatementError:
    """A failed statement of a multi-statement program (0-based ``index``)."""

    index: int
    code: ErrorCode
    message: str


@dataclass(frozen=True)
class Subscribed:
    """The subscription a ``.subscribe`` registered; pushes for it carry this generation.

    ``revision`` is the knowledge graph revision the snapshot is the exact answer
    at; every later delta names a higher one."""

    subscription: str
    generation: int
    revision: int


@dataclass(frozen=True)
class ResultResponse:
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    total_count: int
    truncated: bool
    execution_time_ms: int
    row_provenance: list[str] | None = None
    metadata: dict[str, Any] | None = None
    switched_kg: str | None = None
    proof_trees: list[dict[str, Any]] | None = None
    timing_breakdown: dict[str, Any] | None = None
    errors: list[StatementError] | None = None
    subscribed: Subscribed | None = None
    id: str | None = None


@dataclass(frozen=True)
class ErrorResponse:
    message: str
    validation_errors: list[dict[str, Any]] | None = None
    code: ErrorCode | None = None
    id: str | None = None


@dataclass(frozen=True)
class ResultStartResponse:
    columns: list[str]
    total_count: int
    truncated: bool
    execution_time_ms: int
    metadata: dict[str, Any] | None = None
    switched_kg: str | None = None
    proof_trees: list[dict[str, Any]] | None = None
    timing_breakdown: dict[str, Any] | None = None
    errors: list[StatementError] | None = None
    subscribed: Subscribed | None = None
    """Set on a streamed reply to ``.subscribe``: the chunks hold the snapshot."""
    id: str | None = None


@dataclass(frozen=True)
class ResultChunkResponse:
    rows: list[list[Any]]
    chunk_index: int
    row_provenance: list[str] | None = None
    id: str | None = None


@dataclass(frozen=True)
class ResultEndResponse:
    row_count: int
    chunk_count: int
    id: str | None = None


@dataclass(frozen=True)
class NamedResult:
    """One query's result in a ``snapshot``."""

    name: str
    columns: list[str]
    rows: list[list[Any]]
    total_count: int
    truncated: bool
    """Whether a limit or the result cap cut the rows; never in a group snapshot."""


@dataclass(frozen=True)
class SnapshotResponse:
    """Results of several queries, all exact at ``revision``: the reply to
    ``read``, and to ``subscribe`` (then naming the group in ``subscribed``)."""

    knowledge_graph: str
    revision: int
    results: list[NamedResult]
    """One per query of the request, in its order."""
    execution_time_ms: int
    subscribed: Subscribed | None = None
    id: str | None = None


@dataclass(frozen=True)
class NamedResultHeader:
    """One result of a ``snapshot_start``: its chunks carry ``row_count`` rows."""

    name: str
    columns: list[str]
    row_count: int
    total_count: int
    truncated: bool


@dataclass(frozen=True)
class SnapshotStartResponse:
    """Header of a snapshot streamed as ``snapshot_chunk`` frames; complete only
    at its ``snapshot_end``."""

    knowledge_graph: str
    revision: int
    results: list[NamedResultHeader]
    execution_time_ms: int
    subscribed: Subscribed | None = None
    id: str | None = None


@dataclass(frozen=True)
class SnapshotChunkResponse:
    """Rows of result ``result`` (its index) of a streamed snapshot; ``chunk_index``
    counts from 0 across all results, which stream in order."""

    result: int
    chunk_index: int
    rows: list[list[Any]]
    id: str | None = None


@dataclass(frozen=True)
class SnapshotEndResponse:
    """End of a streamed snapshot: how many chunks it had."""

    chunk_count: int
    id: str | None = None


@dataclass(frozen=True)
class PongResponse:
    id: str | None = None


CancelOutcome = Literal["cancelled", "too_late", "not_found"]
"""What a ``cancel`` did to its target: stopped before it began committing
(its reply is an ``error`` with code ``cancelled``), too late (its reply
reports what it committed), or no unanswered request has that id."""


@dataclass(frozen=True)
class CancelAckResponse:
    """Answer to ``cancel``, released after the target's own reply."""

    target: str
    outcome: CancelOutcome
    id: str | None = None


NoticeCode = Literal[
    "notifications_missed",
    "replay_gap",
    "slow_consumer",
    "idle_timeout",
    "lifetime_exceeded",
    "auth_timeout",
    "credential_revoked",
    "credential_expired",
    "server_shutdown",
]
"""A connection event. The server closes the connection after every one but
``notifications_missed`` and ``replay_gap``."""


@dataclass(frozen=True)
class NoticeResponse:
    """A connection event announced by the server; never a reply."""

    code: NoticeCode
    message: str

    @property
    def closes_connection(self) -> bool:
        return self.code not in ("notifications_missed", "replay_gap")


@dataclass(frozen=True)
class SubscriptionDeltaResponse:
    """Rows that entered and left a standing query's result."""

    subscription: str
    generation: int
    knowledge_graph: str
    seq: int
    """Delta number within the generation, from 1, without gaps."""
    revision: int
    """The knowledge graph revision the result reaches with this delta."""
    columns: list[str]
    inserted: list[list[Any]]
    retracted: list[list[Any]]


@dataclass(frozen=True)
class SubscriptionDeltaStartResponse:
    """Header of a delta streamed in chunks: a ``subscription_delta`` without
    its rows. The delta applies only at its ``subscription_delta_end``."""

    subscription: str
    generation: int
    knowledge_graph: str
    seq: int
    revision: int
    columns: list[str]


@dataclass(frozen=True)
class SubscriptionDeltaChunkResponse:
    """Rows of a streamed delta, in order from ``chunk_index`` 0."""

    subscription: str
    generation: int
    seq: int
    chunk_index: int
    inserted: list[list[Any]]
    retracted: list[list[Any]]


@dataclass(frozen=True)
class SubscriptionDeltaEndResponse:
    """End of a streamed delta: the counts its chunks must add up to."""

    subscription: str
    generation: int
    seq: int
    chunk_count: int
    inserted_count: int
    retracted_count: int


@dataclass(frozen=True)
class GroupMemberDelta:
    """One member of a ``subscription_group_delta``, in group order."""

    name: str
    unchanged: bool
    """Its ``inserted`` and ``retracted`` are empty: already exact at the revision."""
    columns: list[str]
    inserted: list[list[Any]]
    retracted: list[list[Any]]


@dataclass(frozen=True)
class SubscriptionGroupDeltaResponse:
    """The results of a subscription group changed: every member, in group
    order; after it every member is exact at ``revision``."""

    subscription: str
    generation: int
    knowledge_graph: str
    seq: int
    """Delta number within the generation, from 1, shared by the group, without gaps."""
    revision: int
    members: list[GroupMemberDelta]


@dataclass(frozen=True)
class GroupMemberDeltaHeader:
    """One member of a ``subscription_group_delta_start``, with its row counts."""

    name: str
    unchanged: bool
    columns: list[str]
    inserted_count: int
    retracted_count: int


@dataclass(frozen=True)
class SubscriptionGroupDeltaStartResponse:
    """Header of a group delta streamed in chunks; it applies only at its
    ``subscription_group_delta_end``."""

    subscription: str
    generation: int
    knowledge_graph: str
    seq: int
    revision: int
    members: list[GroupMemberDeltaHeader]


@dataclass(frozen=True)
class SubscriptionGroupDeltaChunkResponse:
    """Rows of member ``member`` of a streamed group delta; ``chunk_index``
    counts from 0 across all members, which stream in order."""

    subscription: str
    generation: int
    seq: int
    chunk_index: int
    member: int
    inserted: list[list[Any]]
    retracted: list[list[Any]]


@dataclass(frozen=True)
class SubscriptionGroupDeltaEndResponse:
    """End of a streamed group delta: how many chunks it had."""

    subscription: str
    generation: int
    seq: int
    chunk_count: int


@dataclass(frozen=True)
class SubscriptionErrorResponse:
    """A standing query failed to re-evaluate; it stays registered. A group's
    refresh failed as a whole: every member keeps its last result."""

    subscription: str
    generation: int
    message: str
    #: ``access_denied`` when the subscriber may no longer read the knowledge graph.
    code: ErrorCode | None = None


@dataclass(frozen=True)
class SubscriptionResetResponse:
    """The server ended a subscription whose next change it could not deliver
    whole: discard its rows and subscribe again."""

    subscription: str
    generation: int
    message: str


@dataclass(frozen=True)
class NotificationResponse:
    type: str  # persistent_update, rule_change, kg_change, schema_change
    seq: int
    timestamp_ms: int
    session_id: str | None = None
    knowledge_graph: str | None = None
    # persistent_update fields
    relation: str | None = None
    operation: str | None = None
    count: int | None = None
    # rule_change fields
    rule_name: str | None = None
    # schema_change fields
    entity: str | None = None


# ── Type alias ────────────────────────────────────────────────────────

ServerMessage = (
    AuthenticatedResponse
    | AuthErrorResponse
    | ResultResponse
    | ErrorResponse
    | ResultStartResponse
    | ResultChunkResponse
    | ResultEndResponse
    | SnapshotResponse
    | SnapshotStartResponse
    | SnapshotChunkResponse
    | SnapshotEndResponse
    | PongResponse
    | CancelAckResponse
    | NoticeResponse
    | NotificationResponse
    | SubscriptionDeltaResponse
    | SubscriptionDeltaStartResponse
    | SubscriptionDeltaChunkResponse
    | SubscriptionDeltaEndResponse
    | SubscriptionGroupDeltaResponse
    | SubscriptionGroupDeltaStartResponse
    | SubscriptionGroupDeltaChunkResponse
    | SubscriptionGroupDeltaEndResponse
    | SubscriptionErrorResponse
    | SubscriptionResetResponse
)

ReplyMessage = (
    AuthenticatedResponse
    | AuthErrorResponse
    | ResultResponse
    | ErrorResponse
    | ResultStartResponse
    | ResultChunkResponse
    | ResultEndResponse
    | SnapshotResponse
    | SnapshotStartResponse
    | SnapshotChunkResponse
    | SnapshotEndResponse
    | PongResponse
    | CancelAckResponse
)
"""Frames that answer a request and echo its ``id``."""

SubscriptionPush = (
    SubscriptionDeltaResponse
    | SubscriptionDeltaStartResponse
    | SubscriptionDeltaChunkResponse
    | SubscriptionDeltaEndResponse
    | SubscriptionGroupDeltaResponse
    | SubscriptionGroupDeltaStartResponse
    | SubscriptionGroupDeltaChunkResponse
    | SubscriptionGroupDeltaEndResponse
    | SubscriptionErrorResponse
    | SubscriptionResetResponse
)
"""Standing-query frames; each names its ``subscription`` and ``generation``."""

PushMessage = (
    NoticeResponse
    | NotificationResponse
    | SubscriptionDeltaResponse
    | SubscriptionDeltaStartResponse
    | SubscriptionDeltaChunkResponse
    | SubscriptionDeltaEndResponse
    | SubscriptionGroupDeltaResponse
    | SubscriptionGroupDeltaStartResponse
    | SubscriptionGroupDeltaChunkResponse
    | SubscriptionGroupDeltaEndResponse
    | SubscriptionErrorResponse
    | SubscriptionResetResponse
)
"""Frames the server sends unprompted: never the reply to a request."""


# ── Serialization / Deserialization ───────────────────────────────────

def serialize_message(
    msg: LoginMessage
    | AuthenticateMessage
    | ExecuteMessage
    | ReadMessage
    | SubscribeMessage
    | CancelMessage
    | PingMessage,
) -> str:
    """Serialize a client message to JSON."""
    return msg.to_json()


def _statement_errors(raw: list[dict[str, Any]] | None) -> list[StatementError] | None:
    if raw is None:
        return None
    return [
        StatementError(index=e["index"], code=e["code"], message=e["message"])
        for e in raw
    ]


def _subscribed(raw: dict[str, Any] | None) -> Subscribed | None:
    if raw is None:
        return None
    return Subscribed(
        subscription=raw["subscription"],
        generation=raw["generation"],
        revision=raw["revision"],
    )


def _named_results(raw: list[dict[str, Any]]) -> list[NamedResult]:
    return [
        NamedResult(
            name=r["name"],
            columns=r["columns"],
            rows=r["rows"],
            total_count=r["total_count"],
            truncated=r["truncated"],
        )
        for r in raw
    ]


def _named_result_headers(raw: list[dict[str, Any]]) -> list[NamedResultHeader]:
    return [
        NamedResultHeader(
            name=r["name"],
            columns=r["columns"],
            row_count=r["row_count"],
            total_count=r["total_count"],
            truncated=r["truncated"],
        )
        for r in raw
    ]


def _member_deltas(raw: list[dict[str, Any]]) -> list[GroupMemberDelta]:
    return [
        GroupMemberDelta(
            name=m["name"],
            unchanged=m["unchanged"],
            columns=m["columns"],
            inserted=m["inserted"],
            retracted=m["retracted"],
        )
        for m in raw
    ]


def _member_headers(raw: list[dict[str, Any]]) -> list[GroupMemberDeltaHeader]:
    return [
        GroupMemberDeltaHeader(
            name=m["name"],
            unchanged=m["unchanged"],
            columns=m["columns"],
            inserted_count=m["inserted_count"],
            retracted_count=m["retracted_count"],
        )
        for m in raw
    ]


def deserialize_message(data: str | bytes) -> ServerMessage:
    """Deserialize a server JSON message into a typed response object."""
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    obj = json.loads(data)
    msg_type = obj.get("type")

    if msg_type == "authenticated":
        return AuthenticatedResponse(
            session_id=obj["session_id"],
            knowledge_graph=obj["knowledge_graph"],
            version=obj["version"],
            role=obj["role"],
            protocol_version=obj["protocol_version"],
            stream_epoch=obj["stream_epoch"],
            id=obj.get("id"),
        )
    if msg_type == "auth_error":
        return AuthErrorResponse(
            message=obj["message"], id=obj.get("id"), code=obj.get("code")
        )
    if msg_type == "result":
        return ResultResponse(
            columns=obj["columns"],
            rows=obj["rows"],
            row_count=obj["row_count"],
            total_count=obj["total_count"],
            truncated=obj["truncated"],
            execution_time_ms=obj["execution_time_ms"],
            row_provenance=obj.get("row_provenance"),
            metadata=obj.get("metadata"),
            switched_kg=obj.get("switched_kg"),
            proof_trees=obj.get("proof_trees"),
            timing_breakdown=obj.get("timing_breakdown"),
            errors=_statement_errors(obj.get("errors")),
            subscribed=_subscribed(obj.get("subscribed")),
            id=obj.get("id"),
        )
    if msg_type == "error":
        return ErrorResponse(
            message=obj["message"],
            validation_errors=obj.get("validation_errors"),
            code=obj.get("code"),
            id=obj.get("id"),
        )
    if msg_type == "result_start":
        return ResultStartResponse(
            columns=obj["columns"],
            total_count=obj["total_count"],
            truncated=obj["truncated"],
            execution_time_ms=obj["execution_time_ms"],
            metadata=obj.get("metadata"),
            switched_kg=obj.get("switched_kg"),
            proof_trees=obj.get("proof_trees"),
            timing_breakdown=obj.get("timing_breakdown"),
            errors=_statement_errors(obj.get("errors")),
            subscribed=_subscribed(obj.get("subscribed")),
            id=obj.get("id"),
        )
    if msg_type == "result_chunk":
        return ResultChunkResponse(
            rows=obj["rows"],
            chunk_index=obj["chunk_index"],
            row_provenance=obj.get("row_provenance"),
            id=obj.get("id"),
        )
    if msg_type == "result_end":
        return ResultEndResponse(
            row_count=obj["row_count"],
            chunk_count=obj["chunk_count"],
            id=obj.get("id"),
        )
    if msg_type == "snapshot":
        return SnapshotResponse(
            knowledge_graph=obj["knowledge_graph"],
            revision=obj["revision"],
            results=_named_results(obj["results"]),
            execution_time_ms=obj["execution_time_ms"],
            subscribed=_subscribed(obj.get("subscribed")),
            id=obj.get("id"),
        )
    if msg_type == "snapshot_start":
        return SnapshotStartResponse(
            knowledge_graph=obj["knowledge_graph"],
            revision=obj["revision"],
            results=_named_result_headers(obj["results"]),
            execution_time_ms=obj["execution_time_ms"],
            subscribed=_subscribed(obj.get("subscribed")),
            id=obj.get("id"),
        )
    if msg_type == "snapshot_chunk":
        return SnapshotChunkResponse(
            result=obj["result"],
            chunk_index=obj["chunk_index"],
            rows=obj["rows"],
            id=obj.get("id"),
        )
    if msg_type == "snapshot_end":
        return SnapshotEndResponse(chunk_count=obj["chunk_count"], id=obj.get("id"))
    if msg_type == "pong":
        return PongResponse(id=obj.get("id"))
    if msg_type == "cancel_ack":
        return CancelAckResponse(target=obj["target"], outcome=obj["outcome"], id=obj.get("id"))
    if msg_type == "notice":
        return NoticeResponse(code=obj["code"], message=obj["message"])
    if msg_type == "subscription_delta":
        return SubscriptionDeltaResponse(
            subscription=obj["subscription"],
            generation=obj["generation"],
            knowledge_graph=obj["knowledge_graph"],
            seq=obj["seq"],
            revision=obj["revision"],
            columns=obj["columns"],
            inserted=obj["inserted"],
            retracted=obj["retracted"],
        )
    if msg_type == "subscription_delta_start":
        return SubscriptionDeltaStartResponse(
            subscription=obj["subscription"],
            generation=obj["generation"],
            knowledge_graph=obj["knowledge_graph"],
            seq=obj["seq"],
            revision=obj["revision"],
            columns=obj["columns"],
        )
    if msg_type == "subscription_delta_chunk":
        return SubscriptionDeltaChunkResponse(
            subscription=obj["subscription"],
            generation=obj["generation"],
            seq=obj["seq"],
            chunk_index=obj["chunk_index"],
            inserted=obj["inserted"],
            retracted=obj["retracted"],
        )
    if msg_type == "subscription_delta_end":
        return SubscriptionDeltaEndResponse(
            subscription=obj["subscription"],
            generation=obj["generation"],
            seq=obj["seq"],
            chunk_count=obj["chunk_count"],
            inserted_count=obj["inserted_count"],
            retracted_count=obj["retracted_count"],
        )
    if msg_type == "subscription_group_delta":
        return SubscriptionGroupDeltaResponse(
            subscription=obj["subscription"],
            generation=obj["generation"],
            knowledge_graph=obj["knowledge_graph"],
            seq=obj["seq"],
            revision=obj["revision"],
            members=_member_deltas(obj["members"]),
        )
    if msg_type == "subscription_group_delta_start":
        return SubscriptionGroupDeltaStartResponse(
            subscription=obj["subscription"],
            generation=obj["generation"],
            knowledge_graph=obj["knowledge_graph"],
            seq=obj["seq"],
            revision=obj["revision"],
            members=_member_headers(obj["members"]),
        )
    if msg_type == "subscription_group_delta_chunk":
        return SubscriptionGroupDeltaChunkResponse(
            subscription=obj["subscription"],
            generation=obj["generation"],
            seq=obj["seq"],
            chunk_index=obj["chunk_index"],
            member=obj["member"],
            inserted=obj["inserted"],
            retracted=obj["retracted"],
        )
    if msg_type == "subscription_group_delta_end":
        return SubscriptionGroupDeltaEndResponse(
            subscription=obj["subscription"],
            generation=obj["generation"],
            seq=obj["seq"],
            chunk_count=obj["chunk_count"],
        )
    if msg_type == "subscription_error":
        return SubscriptionErrorResponse(
            subscription=obj["subscription"],
            generation=obj["generation"],
            message=obj["message"],
            code=obj.get("code"),
        )
    if msg_type == "subscription_reset":
        return SubscriptionResetResponse(
            subscription=obj["subscription"],
            generation=obj["generation"],
            message=obj["message"],
        )
    if msg_type in ("persistent_update", "rule_change", "kg_change", "schema_change"):
        return NotificationResponse(
            type=msg_type,
            seq=obj["seq"],
            timestamp_ms=obj["timestamp_ms"],
            session_id=obj.get("session_id"),
            knowledge_graph=obj.get("knowledge_graph"),
            relation=obj.get("relation"),
            operation=obj.get("operation"),
            count=obj.get("count"),
            rule_name=obj.get("rule_name"),
            entity=obj.get("entity"),
        )
    raise ValueError(f"Unknown message type: {msg_type!r}")
