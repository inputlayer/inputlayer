"""Tests for inputlayer.connection - mocked WebSocket connection tests."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from inputlayer._protocol import ResultResponse
from inputlayer.connection import Connection
from inputlayer.exceptions import AuthenticationError, ConnectionError


def _auth_response() -> str:
    return json.dumps({
        "type": "authenticated",
        "session_id": "1",
        "knowledge_graph": "default",
        "version": "0.1.0",
        "role": "admin",
    })


def _result_response(columns: list[str], rows: list[list]) -> str:
    return json.dumps({
        "type": "result",
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "total_count": len(rows),
        "truncated": False,
        "execution_time_ms": 1,
    })


def _error_response(message: str) -> str:
    return json.dumps({"type": "error", "message": message})


class TestConnectionProperties:
    def test_initial_state(self):
        conn = Connection("ws://localhost:8080/ws", username="admin", password="admin")
        assert conn.connected is False
        assert conn.session_id is None
        assert conn.server_version is None
        assert conn.role is None


class TestConnectionAuth:
    @pytest.mark.asyncio
    async def test_no_credentials_raises(self):
        conn = Connection("ws://localhost:8080/ws")
        mock_ws = AsyncMock()
        conn._ws = mock_ws
        with pytest.raises(AuthenticationError, match="No credentials"):
            await conn._authenticate()

    @pytest.mark.asyncio
    async def test_login_success(self):
        conn = Connection("ws://localhost:8080/ws", username="admin", password="secret")
        mock_ws = AsyncMock()
        mock_ws.recv = AsyncMock(return_value=_auth_response())
        conn._ws = mock_ws

        await conn._authenticate()
        assert conn.session_id == "1"
        assert conn.server_version == "0.1.0"
        assert conn.role == "admin"

    @pytest.mark.asyncio
    async def test_api_key_success(self):
        conn = Connection("ws://localhost:8080/ws", api_key="ilk_test")
        mock_ws = AsyncMock()
        mock_ws.recv = AsyncMock(return_value=_auth_response())
        conn._ws = mock_ws

        await conn._authenticate()
        assert conn.session_id == "1"

    @pytest.mark.asyncio
    async def test_auth_error(self):
        conn = Connection("ws://localhost:8080/ws", username="bad", password="bad")
        mock_ws = AsyncMock()
        mock_ws.recv = AsyncMock(return_value=json.dumps({
            "type": "auth_error",
            "message": "Invalid credentials",
        }))
        conn._ws = mock_ws

        with pytest.raises(AuthenticationError, match="Invalid credentials"):
            await conn._authenticate()


class TestConnectionExecute:
    @pytest.mark.asyncio
    async def test_execute_result(self):
        conn = Connection("ws://localhost:8080/ws", username="admin", password="admin")
        mock_ws = AsyncMock()
        mock_ws.recv = AsyncMock(return_value=_result_response(["x", "y"], [[1, 2]]))
        conn._ws = mock_ws
        conn._connected = True

        result = await conn.execute("?edge(X, Y)")
        assert isinstance(result, ResultResponse)
        assert result.columns == ["x", "y"]
        assert result.rows == [[1, 2]]

    @pytest.mark.asyncio
    async def test_execute_error_as_result(self):
        conn = Connection("ws://localhost:8080/ws", username="admin", password="admin")
        mock_ws = AsyncMock()
        mock_ws.recv = AsyncMock(return_value=_error_response("Parse error"))
        conn._ws = mock_ws
        conn._connected = True

        result = await conn.execute("bad query")
        # Errors are returned as ResultResponse with error column
        assert result.columns == ["error"]
        assert result.rows[0][0] == "Parse error"

    @pytest.mark.asyncio
    async def test_execute_not_connected(self):
        conn = Connection("ws://localhost:8080/ws", username="admin", password="admin")
        with pytest.raises(ConnectionError, match="Not connected"):
            await conn.execute("?test()")


class TestConnectionStreaming:
    @pytest.mark.asyncio
    async def test_stream_assembly(self):
        conn = Connection("ws://localhost:8080/ws", username="admin", password="admin")

        responses = [
            json.dumps({
                "type": "result_start",
                "columns": ["id", "name"],
                "total_count": 3,
                "truncated": False,
                "execution_time_ms": 10,
            }),
            json.dumps({
                "type": "result_chunk",
                "rows": [[1, "alice"], [2, "bob"]],
                "chunk_index": 0,
            }),
            json.dumps({
                "type": "result_chunk",
                "rows": [[3, "charlie"]],
                "chunk_index": 1,
            }),
            json.dumps({
                "type": "result_end",
                "row_count": 3,
                "chunk_count": 2,
            }),
        ]
        mock_ws = AsyncMock()
        mock_ws.recv = AsyncMock(side_effect=responses)
        conn._ws = mock_ws
        conn._connected = True

        result = await conn.execute("?big_table(Id, Name)")
        assert result.columns == ["id", "name"]
        assert len(result.rows) == 3
        assert result.rows[0] == [1, "alice"]
        assert result.rows[2] == [3, "charlie"]
        assert result.row_count == 3


class TestConnectionNotifications:
    @pytest.mark.asyncio
    async def test_notification_during_result(self):
        """Notifications received while waiting for a result should be dispatched."""
        conn = Connection("ws://localhost:8080/ws", username="admin", password="admin")

        received_events = []
        conn.dispatcher.on("persistent_update", callback=lambda e: received_events.append(e))

        responses = [
            json.dumps({
                "type": "persistent_update",
                "knowledge_graph": "default",
                "relation": "edge",
                "operation": "insert",
                "count": 5,
                "seq": 1,
                "timestamp_ms": 1000,
            }),
            _result_response(["x"], [[42]]),
        ]
        mock_ws = AsyncMock()
        mock_ws.recv = AsyncMock(side_effect=responses)
        conn._ws = mock_ws
        conn._connected = True

        result = await conn.execute("?edge(X)")
        assert result.rows == [[42]]
        assert len(received_events) == 1
        assert received_events[0].relation == "edge"


class TestConnectionConcurrency:
    """Verify that concurrent execute() calls are serialized.

    The WebSocket protocol is single-flight: only one request can be in
    flight at a time. The Connection._exec_lock ensures that concurrent
    callers (e.g., LangGraph parallel branches, the checkpointer) are
    safely interleaved instead of trampling on each other's responses.
    """

    @pytest.mark.asyncio
    async def test_concurrent_executes_are_serialized(self):
        """Two concurrent execute() calls should produce two distinct results
        without their request/response pairs interleaving."""
        conn = Connection("ws://localhost:8080/ws", username="admin", password="admin")

        # Track the order of send/recv to verify serialization
        sequence: list[str] = []

        send_event_a = asyncio.Event()
        send_event_b = asyncio.Event()
        responses_iter = iter(
            [
                _result_response(["x"], [["A"]]),
                _result_response(["x"], [["B"]]),
            ]
        )

        async def fake_send(payload: str) -> None:
            sequence.append(f"send:{payload}")
            # If A was sent, allow it to receive before B sends.
            if "query_a" in payload:
                send_event_a.set()
                await asyncio.sleep(0)  # let recv run
            elif "query_b" in payload:
                send_event_b.set()

        async def fake_recv() -> str:
            sequence.append("recv")
            return next(responses_iter)

        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock(side_effect=fake_send)
        mock_ws.recv = AsyncMock(side_effect=fake_recv)
        conn._ws = mock_ws
        conn._connected = True

        # Fire both concurrently. Without the lock, the second send could
        # happen before the first recv completes — causing the responses
        # to be misattributed.
        result_a, result_b = await asyncio.gather(
            conn.execute("query_a"),
            conn.execute("query_b"),
        )

        # Each request must have its full send/recv pair before the next
        # one starts. We expect: send query_a -> recv -> send query_b -> recv
        # or:                    send query_b -> recv -> send query_a -> recv
        # but never an interleaving like send/send/recv/recv.
        assert len(sequence) == 4
        assert sequence[0].startswith("send:")
        assert sequence[1] == "recv"
        assert sequence[2].startswith("send:")
        assert sequence[3] == "recv"

        # Both results returned distinct rows and didn't get crossed.
        all_values = {result_a.rows[0][0], result_b.rows[0][0]}
        assert all_values == {"A", "B"}

    @pytest.mark.asyncio
    async def test_high_concurrency_executes(self):
        """Stress test: 20 concurrent execute() calls return distinct results."""
        conn = Connection("ws://localhost:8080/ws", username="admin", password="admin")

        n = 20
        responses_iter = iter(
            [_result_response(["i"], [[i]]) for i in range(n)]
        )

        async def fake_recv() -> str:
            return next(responses_iter)

        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock(return_value=None)
        mock_ws.recv = AsyncMock(side_effect=fake_recv)
        conn._ws = mock_ws
        conn._connected = True

        results = await asyncio.gather(
            *[conn.execute(f"?test({i})") for i in range(n)]
        )

        # All 20 results returned, all distinct
        assert len(results) == n
        seen_values = {r.rows[0][0] for r in results}
        assert seen_values == set(range(n))

    @pytest.mark.asyncio
    async def test_concurrent_execute_propagates_error(self):
        """One execute() failing should not corrupt other in-flight calls."""
        conn = Connection("ws://localhost:8080/ws", username="admin", password="admin")

        responses_iter = iter(
            [
                _result_response(["x"], [["ok"]]),
                _error_response("Parse error"),
            ]
        )

        async def fake_recv() -> str:
            return next(responses_iter)

        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock(return_value=None)
        mock_ws.recv = AsyncMock(side_effect=fake_recv)
        conn._ws = mock_ws
        conn._connected = True

        result_ok, result_err = await asyncio.gather(
            conn.execute("good"),
            conn.execute("bad"),
        )

        # Error is returned as a ResultResponse, not raised — verify both
        # returned with their respective payloads
        all_outputs = {str(result_ok.rows[0][0]), str(result_err.rows[0][0])}
        assert "ok" in all_outputs
        assert "Parse error" in all_outputs

    def test_connection_has_exec_lock(self):
        """Sanity check: the lock attribute exists on a fresh connection."""
        conn = Connection("ws://localhost:8080/ws", username="admin", password="admin")
        assert hasattr(conn, "_exec_lock")
        assert isinstance(conn._exec_lock, asyncio.Lock)
