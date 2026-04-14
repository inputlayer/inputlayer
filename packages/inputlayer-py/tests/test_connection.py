"""Tests for inputlayer.connection - mocked WebSocket connection tests."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from inputlayer._protocol import (
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


    def test_failing_callback_does_not_crash_dispatcher(self) -> None:
        """A callback that raises must be logged but must not stop other callbacks."""
        from inputlayer.notifications import NotificationDispatcher, NotificationEvent

        dispatcher = NotificationDispatcher()
        received = []

        dispatcher.on(callback=lambda e: (_ for _ in ()).throw(RuntimeError("boom")))
        dispatcher.on(callback=lambda e: received.append(e))

        event = NotificationEvent(
            type="persistent_update", seq=1, timestamp_ms=0, relation="test"
        )
        dispatcher.dispatch(event)  # Must not raise

        assert received == [event]  # Second callback still ran

    def test_failing_callback_is_logged(self, caplog) -> None:
        """A callback exception must be logged via logger.exception."""
        import logging

        from inputlayer.notifications import NotificationDispatcher, NotificationEvent

        dispatcher = NotificationDispatcher()
        dispatcher.on(callback=lambda e: 1 / 0)

        event = NotificationEvent(type="persistent_update", seq=1, timestamp_ms=0)
        with caplog.at_level(logging.ERROR, logger="inputlayer.notifications"):
            dispatcher.dispatch(event)

        assert any("ZeroDivisionError" in r.message or "raised" in r.message.lower()
                   for r in caplog.records)


class TestConnectionLockSerialization:
    """Verify the execute lock prevents interleaved send/recv."""

    @pytest.mark.asyncio
    async def test_concurrent_executes_are_serialized(self) -> None:
        """Two concurrent execute() calls must not interleave.

        We verify by tracking the order of send/recv calls. Without the
        lock, sends could interleave with recvs. With the lock, each
        send-recv pair completes atomically.
        """
        conn = Connection("ws://localhost:8080/ws")
        call_log: list[str] = []

        async def mock_send(msg: str) -> None:
            parsed = json.loads(msg)
            program = parsed.get("program", "?")
            call_log.append(f"send:{program}")
            await asyncio.sleep(0.01)  # yield to event loop

        recv_responses = [
            _result_response(["x"], [[1]]),
            _result_response(["x"], [[2]]),
        ]
        recv_index = 0

        async def mock_recv() -> str:
            nonlocal recv_index
            idx = recv_index
            recv_index += 1
            call_log.append(f"recv:{idx}")
            await asyncio.sleep(0.01)  # yield to event loop
            return recv_responses[idx]

        mock_ws = AsyncMock()
        mock_ws.send = mock_send
        mock_ws.recv = mock_recv
        conn._ws = mock_ws
        conn._connected = True

        # Launch two concurrent executes
        await asyncio.gather(
            conn.execute("query_A"),
            conn.execute("query_B"),
        )

        # With the lock, the pattern must be send-recv-send-recv (atomic pairs).
        # Without the lock, we'd see send-send-recv-recv (interleaved).
        assert call_log[0].startswith("send:")
        assert call_log[1].startswith("recv:")
        assert call_log[2].startswith("send:")
        assert call_log[3].startswith("recv:")

    @pytest.mark.asyncio
    async def test_execute_lock_exists(self) -> None:
        conn = Connection("ws://localhost:8080/ws")
        assert hasattr(conn, "_execute_lock")
        assert isinstance(conn._execute_lock, asyncio.Lock)


class TestExecuteWithPreamble:
    """Tests for atomic preamble + execute."""

    @pytest.mark.asyncio
    async def test_preamble_and_program_under_single_lock(self) -> None:
        """Preamble (KG switch) and program must be atomic."""
        conn = Connection("ws://localhost:8080/ws")
        call_log: list[str] = []

        async def mock_send(msg: str) -> None:
            parsed = json.loads(msg)
            program = parsed.get("program", "?")
            call_log.append(f"send:{program}")
            await asyncio.sleep(0.01)

        responses = [
            # Preamble result (KG switch)
            json.dumps({
                "type": "result",
                "columns": [],
                "rows": [],
                "row_count": 0,
                "total_count": 0,
                "truncated": False,
                "execution_time_ms": 0,
                "switched_kg": "my_kg",
            }),
            # Actual command result
            _result_response(["x"], [[42]]),
        ]
        recv_idx = 0

        async def mock_recv() -> str:
            nonlocal recv_idx
            idx = recv_idx
            recv_idx += 1
            call_log.append(f"recv:{idx}")
            await asyncio.sleep(0.01)
            return responses[idx]

        mock_ws = AsyncMock()
        mock_ws.send = mock_send
        mock_ws.recv = mock_recv
        conn._ws = mock_ws
        conn._connected = True

        result = await conn.execute_with_preamble(
            ".kg use my_kg", "?query(X)"
        )
        assert result.rows == [[42]]
        assert conn._current_kg == "my_kg"
        # All four operations (send preamble, recv, send query, recv) happened
        assert len(call_log) == 4
        assert call_log[0] == "send:.kg use my_kg"
        assert call_log[2] == "send:?query(X)"

    @pytest.mark.asyncio
    async def test_preamble_none_skips_switch(self) -> None:
        conn = Connection("ws://localhost:8080/ws")
        call_log: list[str] = []

        async def mock_send(msg: str) -> None:
            parsed = json.loads(msg)
            call_log.append(parsed.get("program", "?"))

        mock_ws = AsyncMock()
        mock_ws.send = mock_send
        mock_ws.recv = AsyncMock(return_value=_result_response(["x"], [[1]]))
        conn._ws = mock_ws
        conn._connected = True

        await conn.execute_with_preamble(None, "?query(X)")
        # Only one send (no preamble)
        assert call_log == ["?query(X)"]


class TestExecuteSequence:
    """Tests for atomic multi-command execution."""

    @pytest.mark.asyncio
    async def test_sequence_holds_lock_across_all_commands(self) -> None:
        conn = Connection("ws://localhost:8080/ws")
        call_log: list[str] = []

        async def mock_send(msg: str) -> None:
            parsed = json.loads(msg)
            call_log.append(f"send:{parsed.get('program', '?')}")
            await asyncio.sleep(0.01)

        responses = [
            _result_response(["ok"], [["done"]]),
            _result_response(["x"], [[1], [2]]),
            _result_response(["ok"], [["dropped"]]),
        ]
        recv_idx = 0

        async def mock_recv() -> str:
            nonlocal recv_idx
            idx = recv_idx
            recv_idx += 1
            call_log.append(f"recv:{idx}")
            await asyncio.sleep(0.01)
            return responses[idx]

        mock_ws = AsyncMock()
        mock_ws.send = mock_send
        mock_ws.recv = mock_recv
        conn._ws = mock_ws
        conn._connected = True

        results = await conn.execute_sequence([
            "+setup_rule(...)",
            "?query(X)",
            ".rule drop temp",
        ])
        assert len(results) == 3
        assert results[1].rows == [[1], [2]]
        # All 6 operations in strict send-recv pairs
        assert call_log == [
            "send:+setup_rule(...)", "recv:0",
            "send:?query(X)", "recv:1",
            "send:.rule drop temp", "recv:2",
        ]


class TestConcurrentMultiKGAtomicity:
    """Verify that two KnowledgeGraph handles sharing one Connection
    cannot corrupt each other's KG context.

    This is the critical production scenario: two coroutines on the same
    event loop, each targeting a different KG, running concurrently via
    the sync bridge's background thread.
    """

    @pytest.mark.asyncio
    async def test_concurrent_kg_operations_are_isolated(self) -> None:
        """Two KG handles issuing concurrent queries must each operate
        against the correct KG. Without atomic preamble+execute, the
        sequence could be:
            KG-A sends ".kg use A"  (lock released)
            KG-B sends ".kg use B"  (lock released)
            KG-A sends query        -> runs against B (WRONG!)

        With the fix, each preamble+query is under a single lock hold.
        """
        from inputlayer.knowledge_graph import KnowledgeGraph

        conn = Connection("ws://localhost:8080/ws")
        sent_programs: list[str] = []

        async def mock_send(msg: str) -> None:
            parsed = json.loads(msg)
            sent_programs.append(parsed.get("program", "?"))
            # Yield to let the other coroutine try to interleave.
            await asyncio.sleep(0.01)

        # Build responses: each execute_with_preamble needs 2 responses
        # (preamble result + query result). We need 4 total for 2 KGs.
        responses = [
            # KG-A preamble (.kg use kg_a)
            json.dumps({
                "type": "result", "columns": [], "rows": [],
                "row_count": 0, "total_count": 0, "truncated": False,
                "execution_time_ms": 0, "switched_kg": "kg_a",
            }),
            # KG-A query result
            _result_response(["from_kg"], [["kg_a_data"]]),
            # KG-B preamble (.kg use kg_b)
            json.dumps({
                "type": "result", "columns": [], "rows": [],
                "row_count": 0, "total_count": 0, "truncated": False,
                "execution_time_ms": 0, "switched_kg": "kg_b",
            }),
            # KG-B query result
            _result_response(["from_kg"], [["kg_b_data"]]),
        ]
        recv_idx = 0

        async def mock_recv() -> str:
            nonlocal recv_idx
            idx = recv_idx
            recv_idx += 1
            await asyncio.sleep(0.01)
            return responses[idx]

        mock_ws = AsyncMock()
        mock_ws.send = mock_send
        mock_ws.recv = mock_recv
        conn._ws = mock_ws
        conn._connected = True
        conn._current_kg = "default"  # Start on neither KG.

        kg_a = KnowledgeGraph("kg_a", conn)
        kg_b = KnowledgeGraph("kg_b", conn)

        # Launch both concurrently.
        result_a, result_b = await asyncio.gather(
            kg_a._execute("?query_a(X)"),
            kg_b._execute("?query_b(X)"),
        )

        # The lock ensures preamble+query pairs are atomic:
        #   [".kg use kg_a", "?query_a(X)", ".kg use kg_b", "?query_b(X)"]
        # NOT interleaved like:
        #   [".kg use kg_a", ".kg use kg_b", "?query_a(X)", "?query_b(X)"]
        assert sent_programs[0] == ".kg use kg_a"
        assert sent_programs[1] == "?query_a(X)"
        assert sent_programs[2] == ".kg use kg_b"
        assert sent_programs[3] == "?query_b(X)"

        # Each KG got its own result.
        assert result_a.rows == [["kg_a_data"]]
        assert result_b.rows == [["kg_b_data"]]

    @pytest.mark.asyncio
    async def test_kg_auto_create_under_lock(self) -> None:
        """When _execute hits 'not found', the create+use+execute sequence
        must all happen under one lock hold."""
        from inputlayer.knowledge_graph import KnowledgeGraph

        conn = Connection("ws://localhost:8080/ws")
        sent_programs: list[str] = []

        async def mock_send(msg: str) -> None:
            parsed = json.loads(msg)
            sent_programs.append(parsed.get("program", "?"))

        responses = [
            # 1. ".kg use new_kg" -> error: not found
            json.dumps({
                "type": "result",
                "columns": ["error"],
                "rows": [["Knowledge graph 'new_kg' not found"]],
                "row_count": 1, "total_count": 1,
                "truncated": False, "execution_time_ms": 0,
            }),
            # 2. ".kg create new_kg" -> ok
            _result_response(["ok"], [["created"]]),
            # 3. ".kg use new_kg" retry -> switched
            json.dumps({
                "type": "result", "columns": [], "rows": [],
                "row_count": 0, "total_count": 0, "truncated": False,
                "execution_time_ms": 0, "switched_kg": "new_kg",
            }),
            # 4. actual query
            _result_response(["x"], [[1]]),
        ]
        recv_idx = 0

        async def mock_recv() -> str:
            nonlocal recv_idx
            idx = recv_idx
            recv_idx += 1
            return responses[idx]

        mock_ws = AsyncMock()
        mock_ws.send = mock_send
        mock_ws.recv = mock_recv
        conn._ws = mock_ws
        conn._connected = True
        conn._current_kg = "default"

        kg = KnowledgeGraph("new_kg", conn)
        result = await kg._execute("?test(X)")

        assert sent_programs == [
            ".kg use new_kg",       # attempt switch
            ".kg create new_kg",    # auto-create
            ".kg use new_kg",       # retry switch
            "?test(X)",             # actual query
        ]
        assert result.rows == [[1]]
        assert conn._current_kg == "new_kg"

    @pytest.mark.asyncio
    async def test_kg_already_current_skips_preamble(self) -> None:
        """When connection is already on the right KG, no preamble is sent."""
        from inputlayer.knowledge_graph import KnowledgeGraph

        conn = Connection("ws://localhost:8080/ws")
        sent_programs: list[str] = []

        async def mock_send(msg: str) -> None:
            parsed = json.loads(msg)
            sent_programs.append(parsed.get("program", "?"))

        mock_ws = AsyncMock()
        mock_ws.send = mock_send
        mock_ws.recv = AsyncMock(return_value=_result_response(["x"], [[42]]))
        conn._ws = mock_ws
        conn._connected = True
        conn._current_kg = "my_kg"  # Already on this KG.

        kg = KnowledgeGraph("my_kg", conn)
        result = await kg._execute("?test(X)")

        # Only the query, no preamble.
        assert sent_programs == ["?test(X)"]
        assert result.rows == [[42]]

    @pytest.mark.asyncio
    async def test_concurrent_sequence_no_interleave(self) -> None:
        """An execute_sequence from one coroutine cannot be interleaved
        by execute from another coroutine."""
        conn = Connection("ws://localhost:8080/ws")
        call_log: list[str] = []

        async def mock_send(msg: str) -> None:
            parsed = json.loads(msg)
            call_log.append(f"send:{parsed.get('program', '?')}")
            await asyncio.sleep(0.01)

        responses = [
            # sequence: 3 commands
            _result_response([], []),
            _result_response(["x"], [[1]]),
            _result_response([], []),
            # single execute
            _result_response(["y"], [[2]]),
        ]
        recv_idx = 0

        async def mock_recv() -> str:
            nonlocal recv_idx
            idx = recv_idx
            recv_idx += 1
            call_log.append(f"recv:{idx}")
            await asyncio.sleep(0.01)
            return responses[idx]

        mock_ws = AsyncMock()
        mock_ws.send = mock_send
        mock_ws.recv = mock_recv
        conn._ws = mock_ws
        conn._connected = True

        await asyncio.gather(
            conn.execute_sequence(["cmd_1", "cmd_2", "cmd_3"]),
            conn.execute("cmd_solo"),
        )

        # The sequence must be fully contiguous - cmd_solo cannot
        # appear between cmd_1, cmd_2, cmd_3.
        send_order = [e.split(":")[1] for e in call_log if e.startswith("send:")]
        # Either [cmd_1, cmd_2, cmd_3, cmd_solo] or [cmd_solo, cmd_1, cmd_2, cmd_3]
        # but NEVER [cmd_1, cmd_solo, cmd_2, cmd_3]
        if send_order[0] == "cmd_1":
            assert send_order == ["cmd_1", "cmd_2", "cmd_3", "cmd_solo"]
        else:
            assert send_order == ["cmd_solo", "cmd_1", "cmd_2", "cmd_3"]
