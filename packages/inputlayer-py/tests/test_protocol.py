"""Tests for inputlayer._protocol - wire message serialization/deserialization."""

import json

import pytest

from inputlayer._protocol import (
    AuthenticateMessage,
    AuthenticatedResponse,
    AuthErrorResponse,
    ErrorResponse,
    ExecuteMessage,
    LoginMessage,
    NotificationResponse,
    PingMessage,
    PongResponse,
    ResultChunkResponse,
    ResultEndResponse,
    ResultResponse,
    ResultStartResponse,
    deserialize_message,
    serialize_message,
)


# ── Client → Server serialization ────────────────────────────────────

class TestLoginMessage:
    def test_serialize(self):
        msg = LoginMessage(username="admin", password="secret")
        data = json.loads(msg.to_json())
        assert data["type"] == "login"
        assert data["username"] == "admin"
        assert data["password"] == "secret"


class TestAuthenticateMessage:
    def test_serialize(self):
        msg = AuthenticateMessage(api_key="ilk_abc123")
        data = json.loads(msg.to_json())
        assert data["type"] == "authenticate"
        assert data["api_key"] == "ilk_abc123"


class TestExecuteMessage:
    def test_serialize(self):
        msg = ExecuteMessage(program="?edge(X, Y)")
        data = json.loads(msg.to_json())
        assert data["type"] == "execute"
        assert data["program"] == "?edge(X, Y)"

    def test_with_special_chars(self):
        msg = ExecuteMessage(program='+employee(1, "Alice")')
        data = json.loads(msg.to_json())
        assert data["program"] == '+employee(1, "Alice")'


class TestPingMessage:
    def test_serialize(self):
        msg = PingMessage()
        data = json.loads(msg.to_json())
        assert data["type"] == "ping"


class TestSerializeMessage:
    def test_login(self):
        msg = LoginMessage(username="u", password="p")
        text = serialize_message(msg)
        assert json.loads(text)["type"] == "login"

    def test_execute(self):
        msg = ExecuteMessage(program=".kg list")
        text = serialize_message(msg)
        assert json.loads(text)["type"] == "execute"


# ── Server → Client deserialization ───────────────────────────────────

class TestDeserializeAuthenticated:
    def test_basic(self):
        data = json.dumps({
            "type": "authenticated",
            "session_id": "42",
            "knowledge_graph": "default",
            "version": "0.1.0",
            "role": "admin",
        })
        msg = deserialize_message(data)
        assert isinstance(msg, AuthenticatedResponse)
        assert msg.session_id == "42"
        assert msg.knowledge_graph == "default"
        assert msg.version == "0.1.0"
        assert msg.role == "admin"


class TestDeserializeAuthError:
    def test_basic(self):
        data = json.dumps({"type": "auth_error", "message": "Bad creds"})
        msg = deserialize_message(data)
        assert isinstance(msg, AuthErrorResponse)
        assert msg.message == "Bad creds"


class TestDeserializeResult:
    def test_basic(self):
        data = json.dumps({
            "type": "result",
            "columns": ["col0", "col1"],
            "rows": [[1, 2], [3, 4]],
            "row_count": 2,
            "total_count": 2,
            "truncated": False,
            "execution_time_ms": 5,
        })
        msg = deserialize_message(data)
        assert isinstance(msg, ResultResponse)
        assert msg.columns == ["col0", "col1"]
        assert msg.rows == [[1, 2], [3, 4]]
        assert msg.row_count == 2
        assert msg.truncated is False

    def test_with_provenance(self):
        data = json.dumps({
            "type": "result",
            "columns": ["x"],
            "rows": [[1]],
            "row_count": 1,
            "total_count": 1,
            "truncated": False,
            "execution_time_ms": 1,
            "row_provenance": ["persistent"],
        })
        msg = deserialize_message(data)
        assert msg.row_provenance == ["persistent"]

    def test_with_switched_kg(self):
        data = json.dumps({
            "type": "result",
            "columns": ["message"],
            "rows": [["Switched"]],
            "row_count": 1,
            "total_count": 1,
            "truncated": False,
            "execution_time_ms": 1,
            "switched_kg": "test",
        })
        msg = deserialize_message(data)
        assert msg.switched_kg == "test"

    def test_with_metadata(self):
        data = json.dumps({
            "type": "result",
            "columns": ["x"],
            "rows": [[1]],
            "row_count": 1,
            "total_count": 1,
            "truncated": False,
            "execution_time_ms": 1,
            "metadata": {"has_ephemeral": True, "ephemeral_sources": ["tmp"], "warnings": []},
        })
        msg = deserialize_message(data)
        assert msg.metadata["has_ephemeral"] is True


class TestDeserializeError:
    def test_basic(self):
        data = json.dumps({"type": "error", "message": "Parse error"})
        msg = deserialize_message(data)
        assert isinstance(msg, ErrorResponse)
        assert msg.message == "Parse error"

    def test_with_validation_errors(self):
        data = json.dumps({
            "type": "error",
            "message": "Validation failed",
            "validation_errors": [{"line": 1, "statement_index": 0, "error": "bad syntax"}],
        })
        msg = deserialize_message(data)
        assert len(msg.validation_errors) == 1


class TestDeserializeStreaming:
    def test_result_start(self):
        data = json.dumps({
            "type": "result_start",
            "columns": ["id", "name"],
            "total_count": 50000,
            "truncated": False,
            "execution_time_ms": 120,
        })
        msg = deserialize_message(data)
        assert isinstance(msg, ResultStartResponse)
        assert msg.total_count == 50000

    def test_result_chunk(self):
        data = json.dumps({
            "type": "result_chunk",
            "rows": [[1, "alice"], [2, "bob"]],
            "chunk_index": 0,
        })
        msg = deserialize_message(data)
        assert isinstance(msg, ResultChunkResponse)
        assert len(msg.rows) == 2
        assert msg.chunk_index == 0

    def test_result_end(self):
        data = json.dumps({
            "type": "result_end",
            "row_count": 50000,
            "chunk_count": 100,
        })
        msg = deserialize_message(data)
        assert isinstance(msg, ResultEndResponse)
        assert msg.row_count == 50000
        assert msg.chunk_count == 100


class TestDeserializePong:
    def test_basic(self):
        data = json.dumps({"type": "pong"})
        msg = deserialize_message(data)
        assert isinstance(msg, PongResponse)


class TestDeserializeNotification:
    def test_persistent_update(self):
        data = json.dumps({
            "type": "persistent_update",
            "knowledge_graph": "default",
            "relation": "edge",
            "operation": "insert",
            "count": 5,
            "seq": 42,
            "timestamp_ms": 1708732800000,
        })
        msg = deserialize_message(data)
        assert isinstance(msg, NotificationResponse)
        assert msg.type == "persistent_update"
        assert msg.relation == "edge"
        assert msg.operation == "insert"
        assert msg.count == 5
        assert msg.seq == 42

    def test_rule_change(self):
        data = json.dumps({
            "type": "rule_change",
            "knowledge_graph": "default",
            "rule_name": "reachable",
            "operation": "registered",
            "seq": 43,
            "timestamp_ms": 1708732801000,
        })
        msg = deserialize_message(data)
        assert msg.type == "rule_change"
        assert msg.rule_name == "reachable"

    def test_kg_change(self):
        data = json.dumps({
            "type": "kg_change",
            "knowledge_graph": "analytics",
            "operation": "created",
            "seq": 44,
            "timestamp_ms": 1708732802000,
        })
        msg = deserialize_message(data)
        assert msg.type == "kg_change"
        assert msg.knowledge_graph == "analytics"

    def test_schema_change(self):
        data = json.dumps({
            "type": "schema_change",
            "knowledge_graph": "default",
            "entity": "users",
            "operation": "created",
            "seq": 45,
            "timestamp_ms": 1708732803000,
        })
        msg = deserialize_message(data)
        assert msg.type == "schema_change"
        assert msg.entity == "users"


class TestDeserializeBytes:
    def test_bytes_input(self):
        data = json.dumps({"type": "pong"}).encode("utf-8")
        msg = deserialize_message(data)
        assert isinstance(msg, PongResponse)


class TestDeserializeUnknown:
    def test_unknown_type(self):
        data = json.dumps({"type": "unknown_type"})
        with pytest.raises(ValueError, match="Unknown message type"):
            deserialize_message(data)
