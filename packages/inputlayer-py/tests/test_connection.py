"""Tests for inputlayer.connection - mocked WebSocket connection tests."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inputlayer._protocol import (
    AuthenticatedResponse,
    ResultResponse,
)
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
