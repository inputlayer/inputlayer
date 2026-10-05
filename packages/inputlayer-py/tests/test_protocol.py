"""Tests for inputlayer._protocol - wire message serialization/deserialization."""

import json

import pytest

from inputlayer._protocol import (
    AuthenticatedResponse,
    AuthenticateMessage,
    AuthErrorResponse,
    ErrorResponse,
    ExecuteMessage,
    LoginMessage,
    NamedQuery,
    NoticeResponse,
    NotificationResponse,
    PingMessage,
    PongResponse,
    ReadMessage,
    ResultChunkResponse,
    ResultEndResponse,
    ResultResponse,
    ResultStartResponse,
    SnapshotChunkResponse,
    SnapshotEndResponse,
    SnapshotResponse,
    SnapshotStartResponse,
    SubscribeMessage,
    SubscriptionDeltaChunkResponse,
    SubscriptionDeltaEndResponse,
    SubscriptionDeltaResponse,
    SubscriptionDeltaStartResponse,
    SubscriptionErrorResponse,
    SubscriptionGroupDeltaChunkResponse,
    SubscriptionGroupDeltaEndResponse,
    SubscriptionGroupDeltaResponse,
    SubscriptionGroupDeltaStartResponse,
    SubscriptionResetResponse,
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
            "protocol_version": 2,
            "stream_epoch": "00112233aabbccdd",
        })
        msg = deserialize_message(data)
        assert isinstance(msg, AuthenticatedResponse)
        assert msg.stream_epoch == "00112233aabbccdd"
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


class TestRequestIds:
    def test_requests_carry_an_optional_id(self):
        assert json.loads(ExecuteMessage(program="?a(X)").to_json()) == {
            "type": "execute", "program": "?a(X)",
        }
        assert json.loads(ExecuteMessage(program="?a(X)", id="7").to_json())["id"] == "7"
        assert json.loads(PingMessage(id="p").to_json()) == {"type": "ping", "id": "p"}

    def test_replies_expose_the_echoed_id(self):
        result = deserialize_message(json.dumps({
            "type": "result", "id": "7", "columns": ["x"], "rows": [[1]],
            "row_count": 1, "total_count": 1, "truncated": False,
            "execution_time_ms": 0, "errors": [],
        }))
        assert result.id == "7"
        error = deserialize_message(json.dumps({
            "type": "error", "message": "bad", "code": "invalid_request",
        }))
        assert error.id is None
        assert error.code == "invalid_request"
        assert deserialize_message('{"type": "pong", "id": "p"}').id == "p"

    def test_subscribe_reply_names_its_generation(self):
        result = deserialize_message(json.dumps({
            "type": "result", "id": "s", "columns": ["x"], "rows": [],
            "row_count": 0, "total_count": 0, "truncated": False,
            "execution_time_ms": 0, "errors": [],
            "subscribed": {"subscription": "live", "generation": 3, "revision": 11},
        }))
        assert result.subscribed.subscription == "live"
        assert result.subscribed.generation == 3
        assert result.subscribed.revision == 11


class TestDeserializePushes:
    def test_notice(self):
        notice = deserialize_message(json.dumps({
            "type": "notice", "code": "notifications_missed", "message": "Missed 2",
        }))
        assert isinstance(notice, NoticeResponse)
        assert not notice.closes_connection
        idle = deserialize_message('{"type":"notice","code":"idle_timeout","message":"Idle"}')
        assert idle.closes_connection
        gap = deserialize_message('{"type":"notice","code":"replay_gap","message":"re-read"}')
        assert gap.code == "replay_gap"
        assert not gap.closes_connection

    def test_subscription_frames(self):
        delta = deserialize_message(json.dumps({
            "type": "subscription_delta", "subscription": "live", "generation": 3,
            "knowledge_graph": "default", "seq": 1, "revision": 12, "columns": ["x"],
            "inserted": [[1]], "retracted": [],
        }))
        assert isinstance(delta, SubscriptionDeltaResponse)
        assert (delta.subscription, delta.generation, delta.inserted) == ("live", 3, [[1]])
        assert (delta.seq, delta.revision) == (1, 12)
        error = deserialize_message(json.dumps({
            "type": "subscription_error", "subscription": "live", "generation": 3,
            "message": "boom",
        }))
        assert isinstance(error, SubscriptionErrorResponse)

    def test_streamed_delta_and_reset_frames(self):
        start = deserialize_message(json.dumps({
            "type": "subscription_delta_start", "subscription": "live", "generation": 3,
            "knowledge_graph": "default", "seq": 2, "revision": 13, "columns": ["x"],
        }))
        assert isinstance(start, SubscriptionDeltaStartResponse)
        assert (start.seq, start.revision) == (2, 13)
        chunk = deserialize_message(json.dumps({
            "type": "subscription_delta_chunk", "subscription": "live", "generation": 3,
            "seq": 2, "chunk_index": 0, "inserted": [[2]], "retracted": [[1]],
        }))
        assert isinstance(chunk, SubscriptionDeltaChunkResponse)
        assert (chunk.chunk_index, chunk.inserted, chunk.retracted) == (0, [[2]], [[1]])
        end = deserialize_message(json.dumps({
            "type": "subscription_delta_end", "subscription": "live", "generation": 3,
            "seq": 2, "chunk_count": 1, "inserted_count": 1, "retracted_count": 1,
        }))
        assert isinstance(end, SubscriptionDeltaEndResponse)
        assert (end.chunk_count, end.inserted_count, end.retracted_count) == (1, 1, 1)
        reset = deserialize_message(json.dumps({
            "type": "subscription_reset", "subscription": "live", "generation": 3,
            "message": "gone",
        }))
        assert isinstance(reset, SubscriptionResetResponse)
        assert reset.message == "gone"

    def test_streamed_subscribe_reply_names_the_subscription(self):
        start = deserialize_message(json.dumps({
            "type": "result_start", "id": "1", "columns": ["x"], "total_count": 2,
            "truncated": False, "execution_time_ms": 1,
            "subscribed": {"subscription": "live", "generation": 4, "revision": 9},
        }))
        assert isinstance(start, ResultStartResponse)
        assert start.subscribed is not None
        assert (start.subscribed.subscription, start.subscribed.generation) == ("live", 4)



# ── Snapshot reads and subscription groups (protocol 4) ──────────────


class TestSnapshotFrames:
    QUERIES = (NamedQuery("orders", "?order(S, O)"), NamedQuery("eta", "?eta(O, T)"))

    def test_read_and_subscribe_serialize_like_the_rust_frames(self):
        read = json.loads(ReadMessage(self.QUERIES, id="r1", timeout_ms=500).to_json())
        assert read == {
            "type": "read",
            "queries": [
                {"name": "orders", "query": "?order(S, O)"},
                {"name": "eta", "query": "?eta(O, T)"},
            ],
            "timeout_ms": 500,
            "id": "r1",
        }
        assert "timeout_ms" not in json.loads(ReadMessage(self.QUERIES).to_json())
        subscribe = json.loads(serialize_message(SubscribeMessage("win", self.QUERIES, id="s1")))
        assert subscribe == {
            "type": "subscribe",
            "subscription": "win",
            "queries": read["queries"],
            "id": "s1",
        }

    def test_snapshot(self):
        frame = deserialize_message(json.dumps({
            "type": "snapshot", "id": "s1", "knowledge_graph": "default", "revision": 5,
            "results": [
                {"name": "orders", "columns": ["S", "O"], "rows": [[1, 2]],
                 "total_count": 1, "truncated": False},
                {"name": "eta", "columns": ["O", "T"], "rows": [], "total_count": 0,
                 "truncated": True},
            ],
            "execution_time_ms": 2,
            "subscribed": {"subscription": "win", "generation": 1, "revision": 5},
        }))
        assert isinstance(frame, SnapshotResponse)
        assert (frame.id, frame.revision, frame.knowledge_graph) == ("s1", 5, "default")
        assert [(r.name, r.columns, r.rows, r.truncated) for r in frame.results] == [
            ("orders", ["S", "O"], [[1, 2]], False),
            ("eta", ["O", "T"], [], True),
        ]
        assert frame.subscribed is not None and frame.subscribed.generation == 1

    def test_streamed_snapshot(self):
        start = deserialize_message(json.dumps({
            "type": "snapshot_start", "id": "r", "knowledge_graph": "kg", "revision": 9,
            "results": [{"name": "a", "columns": ["X"], "row_count": 3, "total_count": 3,
                         "truncated": False}],
            "execution_time_ms": 1,
        }))
        assert isinstance(start, SnapshotStartResponse) and start.subscribed is None
        assert start.results[0].row_count == 3
        chunk = deserialize_message(json.dumps({
            "type": "snapshot_chunk", "id": "r", "result": 0, "chunk_index": 0, "rows": [[1]],
        }))
        assert chunk == SnapshotChunkResponse(result=0, chunk_index=0, rows=[[1]], id="r")
        end = deserialize_message(json.dumps({"type": "snapshot_end", "id": "r", "chunk_count": 1}))
        assert end == SnapshotEndResponse(chunk_count=1, id="r")

    def test_group_delta_frames(self):
        base = {"subscription": "win", "generation": 1, "seq": 1}
        delta = deserialize_message(json.dumps({
            **base, "type": "subscription_group_delta", "knowledge_graph": "default",
            "revision": 6,
            "members": [
                {"name": "orders", "unchanged": False, "columns": ["S", "O"],
                 "inserted": [[1, 9]], "retracted": []},
                {"name": "eta", "unchanged": True, "columns": ["O", "T"],
                 "inserted": [], "retracted": []},
            ],
        }))
        assert isinstance(delta, SubscriptionGroupDeltaResponse)
        assert [(m.name, m.unchanged, m.inserted) for m in delta.members] == [
            ("orders", False, [[1, 9]]),
            ("eta", True, []),
        ]
        start = deserialize_message(json.dumps({
            **base, "type": "subscription_group_delta_start", "knowledge_graph": "default",
            "revision": 6,
            "members": [{"name": "orders", "unchanged": False, "columns": ["S", "O"],
                         "inserted_count": 2, "retracted_count": 1}],
        }))
        assert isinstance(start, SubscriptionGroupDeltaStartResponse)
        assert start.members[0].inserted_count == 2
        chunk = deserialize_message(json.dumps({
            **base, "type": "subscription_group_delta_chunk", "chunk_index": 0, "member": 0,
            "inserted": [[1, 1]], "retracted": [[2, 2]],
        }))
        assert isinstance(chunk, SubscriptionGroupDeltaChunkResponse) and chunk.member == 0
        end = deserialize_message(json.dumps({
            **base, "type": "subscription_group_delta_end", "chunk_count": 1,
        }))
        assert end == SubscriptionGroupDeltaEndResponse(
            subscription="win", generation=1, seq=1, chunk_count=1
        )
