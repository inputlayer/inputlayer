"""Tests for inputlayer._sync - the sync-from-async bridge."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inputlayer._sync import _LoopThread, run_sync

# ── Basic functionality ──────────────────────────────────────────────


def test_run_sync_returns_value() -> None:
    async def coro() -> int:
        return 42

    assert run_sync(coro()) == 42


def test_run_sync_returns_string() -> None:
    async def coro() -> str:
        return "hello"

    assert run_sync(coro()) == "hello"


def test_run_sync_with_await() -> None:
    async def coro() -> int:
        await asyncio.sleep(0.01)
        return 99

    assert run_sync(coro()) == 99


def test_run_sync_returns_none() -> None:
    async def coro() -> None:
        await asyncio.sleep(0.01)

    assert run_sync(coro()) is None


def test_run_sync_returns_complex_types() -> None:
    async def coro() -> dict[str, list[int]]:
        return {"a": [1, 2, 3], "b": [4, 5]}

    result = run_sync(coro())
    assert result == {"a": [1, 2, 3], "b": [4, 5]}


def test_run_sync_returns_large_data() -> None:
    async def coro() -> list[int]:
        return list(range(100_000))

    result = run_sync(coro())
    assert len(result) == 100_000
    assert result[0] == 0
    assert result[-1] == 99_999


# ── Error propagation ───────────────────────────────────────────────


def test_run_sync_propagates_exception() -> None:
    async def coro() -> None:
        raise ValueError("test error")

    with pytest.raises(ValueError, match="test error"):
        run_sync(coro())


def test_run_sync_propagates_custom_exception() -> None:
    class CustomError(Exception):
        pass

    async def coro() -> None:
        raise CustomError("custom")

    with pytest.raises(CustomError, match="custom"):
        run_sync(coro())


def test_run_sync_propagates_runtime_error() -> None:
    async def coro() -> None:
        raise RuntimeError("runtime boom")

    with pytest.raises(RuntimeError, match="runtime boom"):
        run_sync(coro())


def test_run_sync_propagates_os_error() -> None:
    async def coro() -> None:
        raise OSError("os boom")

    with pytest.raises(OSError, match="os boom"):
        run_sync(coro())


def test_run_sync_preserves_exception_chain() -> None:
    async def coro() -> None:
        try:
            raise ValueError("original")
        except ValueError:
            raise RuntimeError("wrapped") from None

    with pytest.raises(RuntimeError, match="wrapped"):
        run_sync(coro())


def test_run_sync_exception_after_await() -> None:
    """Exception raised after successful async work still propagates."""

    async def coro() -> None:
        await asyncio.sleep(0.01)
        raise ValueError("late error")

    with pytest.raises(ValueError, match="late error"):
        run_sync(coro())


# ── Works inside a running event loop ────────────────────────────────


def test_run_sync_from_inside_running_loop() -> None:
    """The key scenario: calling run_sync when an event loop is already running."""
    result = None

    async def outer() -> None:
        nonlocal result
        result = run_sync(inner())

    async def inner() -> int:
        await asyncio.sleep(0.01)
        return 123

    asyncio.run(outer())
    assert result == 123


def test_run_sync_from_thread_inside_async_context() -> None:
    """Simulates LangChain's pattern: async framework spawns thread, thread calls sync API."""
    results: list[int] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, sync_worker)
        results.append(result)

    def sync_worker() -> int:
        async def inner() -> int:
            await asyncio.sleep(0.01)
            return 456

        return run_sync(inner())

    asyncio.run(main())
    assert results == [456]


def test_run_sync_multiple_calls_from_running_loop() -> None:
    """Multiple sequential run_sync calls from within a running loop."""
    results: list[int] = []

    async def outer() -> None:
        for i in range(5):
            results.append(run_sync(inner(i)))

    async def inner(n: int) -> int:
        return n * 10

    asyncio.run(outer())
    assert results == [0, 10, 20, 30, 40]


# ── Concurrent calls ────────────────────────────────────────────────


def test_run_sync_concurrent_from_threads() -> None:
    async def coro(n: int) -> int:
        await asyncio.sleep(0.01)
        return n * 2

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(run_sync, coro(i)) for i in range(8)]
        results = sorted(f.result() for f in futures)

    assert results == [0, 2, 4, 6, 8, 10, 12, 14]


def test_run_sync_sequential_calls() -> None:
    async def coro(n: int) -> int:
        return n + 1

    results = [run_sync(coro(i)) for i in range(5)]
    assert results == [1, 2, 3, 4, 5]


def test_run_sync_high_concurrency() -> None:
    """Stress test with many concurrent threads."""

    async def coro(n: int) -> int:
        await asyncio.sleep(0.001)
        return n

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(run_sync, coro(i)) for i in range(50)]
        results = sorted(f.result() for f in futures)

    assert results == list(range(50))


def test_run_sync_concurrent_errors_isolated() -> None:
    """Errors in one concurrent call don't affect others."""

    async def good(n: int) -> int:
        await asyncio.sleep(0.01)
        return n

    async def bad() -> None:
        await asyncio.sleep(0.01)
        raise ValueError("boom")

    with ThreadPoolExecutor(max_workers=4) as pool:
        good_futures = [pool.submit(run_sync, good(i)) for i in range(4)]
        bad_future = pool.submit(run_sync, bad())

        for f in good_futures:
            assert isinstance(f.result(), int)

        with pytest.raises(ValueError, match="boom"):
            bad_future.result()


# ── Async patterns ──────────────────────────────────────────────────


def test_run_sync_with_async_gather() -> None:
    """Coroutine that internally uses asyncio.gather."""

    async def coro() -> list[int]:
        async def worker(n: int) -> int:
            await asyncio.sleep(0.01)
            return n * 2

        return list(await asyncio.gather(worker(1), worker(2), worker(3)))

    assert run_sync(coro()) == [2, 4, 6]


def test_run_sync_with_async_queue() -> None:
    """Coroutine that uses asyncio.Queue internally."""

    async def coro() -> list[int]:
        q: asyncio.Queue[int] = asyncio.Queue()
        for i in range(3):
            await q.put(i)
        return [await q.get() for _ in range(3)]

    assert run_sync(coro()) == [0, 1, 2]


def test_run_sync_with_async_event() -> None:
    """Coroutine that uses asyncio.Event for coordination."""

    async def coro() -> str:
        event = asyncio.Event()

        async def setter() -> None:
            await asyncio.sleep(0.01)
            event.set()

        asyncio.get_event_loop().create_task(setter())
        await event.wait()
        return "done"

    assert run_sync(coro()) == "done"


def test_run_sync_with_timeout() -> None:
    """Coroutine that uses asyncio.wait_for internally."""

    async def coro() -> str:
        async def fast() -> str:
            return "fast"

        return await asyncio.wait_for(fast(), timeout=1.0)

    assert run_sync(coro()) == "fast"


def test_run_sync_timeout_raises() -> None:
    """asyncio.TimeoutError from a coroutine propagates correctly."""

    async def coro() -> None:
        async def slow() -> None:
            await asyncio.sleep(10)

        await asyncio.wait_for(slow(), timeout=0.01)

    with pytest.raises(asyncio.TimeoutError):
        run_sync(coro())


# ── _LoopThread lifecycle ───────────────────────────────────────────


def test_loop_thread_lazy_start() -> None:
    lt = _LoopThread()
    assert lt._loop is None
    assert lt._thread is None

    async def coro() -> int:
        return 1

    lt.run(coro())
    assert lt._loop is not None
    assert lt._loop.is_running()
    assert lt._thread is not None
    assert lt._thread.is_alive()
    lt.shutdown()


def test_loop_thread_shutdown() -> None:
    lt = _LoopThread()

    async def coro() -> int:
        return 1

    lt.run(coro())
    thread = lt._thread
    assert thread is not None
    lt.shutdown()
    assert lt._loop is None
    assert lt._thread is None
    assert not thread.is_alive()


def test_loop_thread_reusable_after_shutdown() -> None:
    lt = _LoopThread()

    async def coro() -> int:
        return 1

    assert lt.run(coro()) == 1
    lt.shutdown()
    assert lt.run(coro()) == 1
    lt.shutdown()


def test_loop_thread_daemon_thread() -> None:
    lt = _LoopThread()

    async def coro() -> int:
        return 1

    lt.run(coro())
    assert lt._thread is not None
    assert lt._thread.daemon
    assert lt._thread.name == "inputlayer-sync"
    lt.shutdown()


def test_loop_thread_multiple_shutdown_safe() -> None:
    """Calling shutdown multiple times should not raise."""
    lt = _LoopThread()

    async def coro() -> int:
        return 1

    lt.run(coro())
    lt.shutdown()
    lt.shutdown()
    lt.shutdown()


def test_loop_thread_shutdown_without_start() -> None:
    """Shutdown on a never-started thread should not raise."""
    lt = _LoopThread()
    lt.shutdown()


def test_loop_thread_multiple_restart_cycles() -> None:
    """Repeatedly start -> use -> shutdown."""
    lt = _LoopThread()

    async def coro(n: int) -> int:
        return n

    for i in range(5):
        assert lt.run(coro(i)) == i
        lt.shutdown()


def test_separate_loop_threads_are_independent() -> None:
    """Two _LoopThread instances don't interfere."""
    lt1 = _LoopThread()
    lt2 = _LoopThread()

    async def coro(n: int) -> int:
        await asyncio.sleep(0.01)
        return n

    assert lt1.run(coro(1)) == 1
    assert lt2.run(coro(2)) == 2

    assert lt1._thread is not lt2._thread
    assert lt1._loop is not lt2._loop

    lt1.shutdown()
    assert lt2.run(coro(3)) == 3
    lt2.shutdown()


# ── client_sync.py wiring ───────────────────────────────────────────


def test_knowledge_graph_sync_delegates_to_run_sync() -> None:
    """KnowledgeGraphSync methods correctly delegate through run_sync."""
    from inputlayer.client_sync import KnowledgeGraphSync

    mock_kg = MagicMock()
    mock_kg.name = "test_kg"
    mock_kg.relations = AsyncMock(return_value=[])
    mock_kg.list_rules = AsyncMock(return_value=[])
    mock_kg.list_indexes = AsyncMock(return_value=[])

    sync_kg = KnowledgeGraphSync(mock_kg)

    assert sync_kg.name == "test_kg"
    assert sync_kg.relations() == []
    assert sync_kg.list_rules() == []
    assert sync_kg.list_indexes() == []

    mock_kg.relations.assert_awaited_once()
    mock_kg.list_rules.assert_awaited_once()
    mock_kg.list_indexes.assert_awaited_once()


def test_knowledge_graph_sync_define_delegates() -> None:
    from inputlayer.client_sync import KnowledgeGraphSync
    from inputlayer.relation import Relation

    class TestRel(Relation):
        id: int
        name: str

    mock_kg = MagicMock()
    mock_kg.define = AsyncMock()

    sync_kg = KnowledgeGraphSync(mock_kg)
    sync_kg.define(TestRel)

    mock_kg.define.assert_awaited_once_with(TestRel)


def test_knowledge_graph_sync_execute_delegates() -> None:
    from inputlayer.client_sync import KnowledgeGraphSync
    from inputlayer.result import ResultSet

    mock_result = ResultSet(columns=["x"], rows=[[1], [2]], row_count=2)
    mock_kg = MagicMock()
    mock_kg.execute = AsyncMock(return_value=mock_result)

    sync_kg = KnowledgeGraphSync(mock_kg)
    result = sync_kg.execute("?edge(X, Y)")

    assert result.row_count == 2
    assert result.rows == [[1], [2]]
    mock_kg.execute.assert_awaited_once_with("?edge(X, Y)")


def test_knowledge_graph_sync_insert_delegates() -> None:
    from inputlayer.client_sync import KnowledgeGraphSync
    from inputlayer.knowledge_graph import InsertResult
    from inputlayer.relation import Relation

    class Employee(Relation):
        id: int
        name: str

    mock_kg = MagicMock()
    mock_kg.insert = AsyncMock(return_value=InsertResult(count=1))

    sync_kg = KnowledgeGraphSync(mock_kg)
    emp = Employee(id=1, name="Alice")
    result = sync_kg.insert(emp)

    assert result.count == 1
    mock_kg.insert.assert_awaited_once_with(emp, data=None)


def test_knowledge_graph_sync_delete_with_where() -> None:
    from inputlayer.client_sync import KnowledgeGraphSync
    from inputlayer.knowledge_graph import DeleteResult
    from inputlayer.relation import Relation

    class Employee(Relation):
        id: int
        department: str

    mock_kg = MagicMock()
    mock_kg.delete = AsyncMock(return_value=DeleteResult(count=3))

    sync_kg = KnowledgeGraphSync(mock_kg)
    where_fn = lambda e: e.department == "sales"  # noqa: E731
    result = sync_kg.delete(Employee, where=where_fn)

    assert result.count == 3
    mock_kg.delete.assert_awaited_once_with(Employee, where=where_fn)


def test_knowledge_graph_sync_edit_rule_clause_delegates() -> None:
    from inputlayer.client_sync import KnowledgeGraphSync

    mock_kg = MagicMock()
    mock_kg.edit_rule_clause = AsyncMock()

    sync_kg = KnowledgeGraphSync(mock_kg)
    sync_kg.edit_rule_clause("my_rule", 1, "clause_obj")

    mock_kg.edit_rule_clause.assert_awaited_once_with("my_rule", 1, "clause_obj")


def test_knowledge_graph_sync_query_stream_delegates() -> None:
    from inputlayer.client_sync import KnowledgeGraphSync

    mock_kg = MagicMock()

    async def fake_stream(*args, **kwargs):
        yield [[1, 2], [3, 4]]
        yield [[5, 6]]

    mock_kg.query_stream = fake_stream

    sync_kg = KnowledgeGraphSync(mock_kg)
    batches = sync_kg.query_stream()
    assert batches == [[[1, 2], [3, 4]], [[5, 6]]]


def test_knowledge_graph_sync_vector_search_delegates() -> None:
    from inputlayer.client_sync import KnowledgeGraphSync
    from inputlayer.result import ResultSet

    mock_result = ResultSet(
        columns=["Id", "Dist"], rows=[["1", 0.1]], row_count=1
    )
    mock_kg = MagicMock()
    mock_kg.vector_search = AsyncMock(return_value=mock_result)

    sync_kg = KnowledgeGraphSync(mock_kg)
    result = sync_kg.vector_search(
        "FakeRelation", [0.1, 0.2], k=5, extra_iql_clauses=["Source = \"a\""]
    )

    assert result.row_count == 1
    mock_kg.vector_search.assert_awaited_once()
    call_kwargs = mock_kg.vector_search.await_args[1]
    assert call_kwargs["k"] == 5
    assert call_kwargs["extra_iql_clauses"] == ["Source = \"a\""]


def test_input_layer_sync_connect_close() -> None:
    """InputLayerSync.connect/close delegate through run_sync."""
    from inputlayer.client_sync import InputLayerSync

    with patch("inputlayer.client_sync.InputLayer") as mock_cls:
        mock_client = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.close = AsyncMock()
        mock_client.connected = True
        mock_client.session_id = "sess-1"
        mock_client.server_version = "0.1.0"
        mock_client.role = "admin"
        mock_cls.return_value = mock_client

        sync = InputLayerSync("ws://localhost:8080/ws", username="admin", password="admin")
        sync.connect()
        mock_client.connect.assert_awaited_once()

        assert sync.connected is True
        assert sync.session_id == "sess-1"
        assert sync.server_version == "0.1.0"
        assert sync.role == "admin"

        sync.close()
        mock_client.close.assert_awaited_once()


def test_input_layer_sync_context_manager() -> None:
    """InputLayerSync works as a context manager."""
    from inputlayer.client_sync import InputLayerSync

    with patch("inputlayer.client_sync.InputLayer") as mock_cls:
        mock_client = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.close = AsyncMock()
        mock_cls.return_value = mock_client

        with InputLayerSync("ws://localhost:8080/ws", username="a", password="b") as sync:
            mock_client.connect.assert_awaited_once()
            assert sync is not None

        mock_client.close.assert_awaited_once()


def test_input_layer_sync_knowledge_graph_returns_sync_wrapper() -> None:
    from inputlayer.client_sync import InputLayerSync, KnowledgeGraphSync

    with patch("inputlayer.client_sync.InputLayer") as mock_cls:
        mock_client = MagicMock()
        mock_kg = MagicMock()
        mock_kg.name = "my_kg"
        mock_client.knowledge_graph.return_value = mock_kg
        mock_cls.return_value = mock_client

        sync = InputLayerSync("ws://localhost:8080/ws")
        kg = sync.knowledge_graph("my_kg")

        assert isinstance(kg, KnowledgeGraphSync)
        assert kg.name == "my_kg"
        mock_client.knowledge_graph.assert_called_once_with("my_kg", create=True)


def test_loop_thread_concurrent_shutdown_safe() -> None:
    """Concurrent shutdown calls should not raise."""
    lt = _LoopThread()

    async def coro() -> int:
        return 1

    lt.run(coro())
    # Shutting down from multiple threads concurrently should be safe.
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(lt.shutdown) for _ in range(4)]
        for f in futures:
            f.result()  # should not raise


def test_loop_thread_custom_timeout() -> None:
    """Timeout parameter is respected."""
    lt = _LoopThread(timeout=0.01)

    async def coro() -> None:
        await asyncio.sleep(10)

    with pytest.raises(TimeoutError):
        lt.run(coro())
    lt.shutdown()


def test_input_layer_sync_user_management() -> None:
    from inputlayer.client_sync import InputLayerSync

    with patch("inputlayer.client_sync.InputLayer") as mock_cls:
        mock_client = MagicMock()
        mock_client.create_user = AsyncMock()
        mock_client.drop_user = AsyncMock()
        mock_client.set_password = AsyncMock()
        mock_client.set_role = AsyncMock()
        mock_client.list_users = AsyncMock(return_value=[])
        mock_cls.return_value = mock_client

        sync = InputLayerSync("ws://localhost:8080/ws")
        sync.create_user("alice", "pass123", "editor")
        sync.set_password("alice", "newpass")
        sync.set_role("alice", "admin")
        users = sync.list_users()
        sync.drop_user("alice")

        mock_client.create_user.assert_awaited_once_with("alice", "pass123", "editor")
        mock_client.set_password.assert_awaited_once_with("alice", "newpass")
        mock_client.set_role.assert_awaited_once_with("alice", "admin")
        mock_client.list_users.assert_awaited_once()
        mock_client.drop_user.assert_awaited_once_with("alice")
        assert users == []
