"""WebSocket wire protocol: message serialization and deserialization.

Matches the AsyncAPI spec at ``docs/spec/asyncapi.yaml`` (protocol version 3,
defined by the ``inputlayer-ws-protocol`` crate).

Any request may carry an ``id``; every reply to it (``authenticated``,
``auth_error``, ``result``, ``result_start``/``result_chunk``/``result_end``,
``error``, ``pong``, ``cancel_ack``) echoes it. Pushes (notifications, subscription deltas) and
``notice`` frames never carry one and are never replies.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

PROTOCOL_VERSION = 3
"""The ``/ws`` protocol version this SDK speaks (``authenticated.protocol_version``)."""


def _with_id(frame: dict[str, Any], request_id: str | None) -> str:
    if request_id is not None:
        frame["id"] = request_id
    return json.dumps(frame)

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

    def to_json(self) -> str:
        frame: dict[str, Any] = {"type": "execute", "program": self.program}
        if self.timeout_ms is not None:
            frame["timeout_ms"] = self.timeout_ms
        return _with_id(frame, self.id)


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
]
"""Why the engine rejected a statement or request (``code`` on ``error`` and ``errors[]``).

``invalid_request`` and ``rate_limited`` reject a whole request before it runs.
``deadline_exceeded`` and ``cancelled`` stop it before it began committing, so
nothing was applied; ``outcome_unknown`` means its commit failed in a way that
leaves the changes possibly applied: read the state back before retrying.
``resource_exhausted`` refuses a query over the engine's per-query memory limit
or its server-wide query memory budget, or a write past its knowledge graph's memory budget; nothing was applied."""


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
class SubscriptionErrorResponse:
    """A standing query failed to re-evaluate; it stays registered."""

    subscription: str
    generation: int
    message: str


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
    | PongResponse
    | CancelAckResponse
    | NoticeResponse
    | NotificationResponse
    | SubscriptionDeltaResponse
    | SubscriptionDeltaStartResponse
    | SubscriptionDeltaChunkResponse
    | SubscriptionDeltaEndResponse
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
    | PongResponse
    | CancelAckResponse
)
"""Frames that answer a request and echo its ``id``."""

SubscriptionPush = (
    SubscriptionDeltaResponse
    | SubscriptionDeltaStartResponse
    | SubscriptionDeltaChunkResponse
    | SubscriptionDeltaEndResponse
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
    | SubscriptionErrorResponse
    | SubscriptionResetResponse
)
"""Frames the server sends unprompted: never the reply to a request."""


# ── Serialization / Deserialization ───────────────────────────────────

def serialize_message(
    msg: LoginMessage | AuthenticateMessage | ExecuteMessage | CancelMessage | PingMessage,
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
        return AuthErrorResponse(message=obj["message"], id=obj.get("id"))
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
    if msg_type == "subscription_error":
        return SubscriptionErrorResponse(
            subscription=obj["subscription"],
            generation=obj["generation"],
            message=obj["message"],
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
