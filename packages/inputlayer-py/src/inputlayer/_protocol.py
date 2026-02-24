"""WebSocket wire protocol: message serialization and deserialization.

Matches the AsyncAPI spec at ``docs/spec/asyncapi.yaml``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


# ── Client → Server messages ──────────────────────────────────────────

@dataclass(frozen=True)
class LoginMessage:
    username: str
    password: str

    def to_json(self) -> str:
        return json.dumps({
            "type": "login",
            "username": self.username,
            "password": self.password,
        })


@dataclass(frozen=True)
class AuthenticateMessage:
    api_key: str

    def to_json(self) -> str:
        return json.dumps({
            "type": "authenticate",
            "api_key": self.api_key,
        })


@dataclass(frozen=True)
class ExecuteMessage:
    program: str

    def to_json(self) -> str:
        return json.dumps({
            "type": "execute",
            "program": self.program,
        })


@dataclass(frozen=True)
class PingMessage:
    def to_json(self) -> str:
        return json.dumps({"type": "ping"})


# ── Server → Client messages ─────────────────────────────────────────

@dataclass(frozen=True)
class AuthenticatedResponse:
    session_id: str
    knowledge_graph: str
    version: str
    role: str


@dataclass(frozen=True)
class AuthErrorResponse:
    message: str


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


@dataclass(frozen=True)
class ErrorResponse:
    message: str
    validation_errors: list[dict[str, Any]] | None = None


@dataclass(frozen=True)
class ResultStartResponse:
    columns: list[str]
    total_count: int
    truncated: bool
    execution_time_ms: int
    metadata: dict[str, Any] | None = None
    switched_kg: str | None = None


@dataclass(frozen=True)
class ResultChunkResponse:
    rows: list[list[Any]]
    chunk_index: int
    row_provenance: list[str] | None = None


@dataclass(frozen=True)
class ResultEndResponse:
    row_count: int
    chunk_count: int


@dataclass(frozen=True)
class PongResponse:
    pass


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
    | NotificationResponse
)


# ── Serialization / Deserialization ───────────────────────────────────

def serialize_message(msg: LoginMessage | AuthenticateMessage | ExecuteMessage | PingMessage) -> str:
    """Serialize a client message to JSON."""
    return msg.to_json()


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
        )
    if msg_type == "auth_error":
        return AuthErrorResponse(message=obj["message"])
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
        )
    if msg_type == "error":
        return ErrorResponse(
            message=obj["message"],
            validation_errors=obj.get("validation_errors"),
        )
    if msg_type == "result_start":
        return ResultStartResponse(
            columns=obj["columns"],
            total_count=obj["total_count"],
            truncated=obj["truncated"],
            execution_time_ms=obj["execution_time_ms"],
            metadata=obj.get("metadata"),
            switched_kg=obj.get("switched_kg"),
        )
    if msg_type == "result_chunk":
        return ResultChunkResponse(
            rows=obj["rows"],
            chunk_index=obj["chunk_index"],
            row_provenance=obj.get("row_provenance"),
        )
    if msg_type == "result_end":
        return ResultEndResponse(
            row_count=obj["row_count"],
            chunk_count=obj["chunk_count"],
        )
    if msg_type == "pong":
        return PongResponse()
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
