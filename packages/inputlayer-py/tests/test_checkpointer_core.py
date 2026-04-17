"""Core checkpointer tests: setup, put/get, writes, sync, serialization, errors."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from langgraph.checkpoint.base import CheckpointMetadata, empty_checkpoint

from inputlayer.integrations.langgraph import InputLayerCheckpointer
from inputlayer.result import ResultSet

from ._mock_checkpoint_kg import MockKG, make_checkpoint, make_config


class TestSetup:
    async def test_setup_creates_relations(self) -> None:
        kg = AsyncMock()
        kg.execute = AsyncMock(return_value=ResultSet(columns=[], rows=[]))
        cp = InputLayerCheckpointer(kg=kg)
        await cp.setup()
        assert kg.execute.await_count == 2
        executed = [call.args[0] for call in kg.execute.call_args_list]
        assert any("graph_checkpoint(" in q and "thread_id" in q for q in executed), (
            f"setup must define graph_checkpoint relation, got: {executed}"
        )
        assert any("graph_write(" in q and "thread_id" in q for q in executed), (
            f"setup must define graph_write relation, got: {executed}"
        )

    async def test_setup_idempotent(self) -> None:
        kg = AsyncMock()
        kg.execute = AsyncMock(return_value=ResultSet(columns=[], rows=[]))
        cp = InputLayerCheckpointer(kg=kg)
        await cp.setup()
        first_count = kg.execute.await_count
        assert first_count == 2
        await cp.setup()
        assert kg.execute.await_count == first_count


class TestPutAndGet:
    async def test_put_then_get(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        config = make_config("thread-1")
        ckpt = make_checkpoint("ckpt-1")
        meta: CheckpointMetadata = {"source": "input", "step": 0, "writes": {}, "parents": {}}

        new_config = await cp.aput(config, ckpt, meta, {})
        assert new_config["configurable"]["checkpoint_id"] == "ckpt-1"

        get_config = make_config("thread-1", "ckpt-1")
        tup = await cp.aget_tuple(get_config)

        assert tup is not None
        assert tup.checkpoint["id"] == "ckpt-1"
        assert tup.metadata == meta

    async def test_get_latest_when_no_checkpoint_id(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i in range(3):
            await cp.aput(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        tup = await cp.aget_tuple(make_config("thread-1"))
        assert tup is not None
        assert tup.checkpoint["id"] == "ckpt-2"

    async def test_get_returns_none_for_missing(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        tup = await cp.aget_tuple(make_config("nonexistent"))
        assert tup is None


class TestPutWrites:
    async def test_put_writes(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        await cp.aput(
            make_config("thread-1"),
            make_checkpoint("ckpt-1"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )

        config = make_config("thread-1", "ckpt-1")
        await cp.aput_writes(
            config,
            [("messages", "hello"), ("count", 1)],
            task_id="task-1",
        )

        tup = await cp.aget_tuple(config)
        assert tup is not None
        assert len(tup.pending_writes) == 2
        channels = [w[1] for w in tup.pending_writes]
        assert "messages" in channels
        assert "count" in channels
        # Verify task_id is correctly extracted (not task_path)
        task_ids = [w[0] for w in tup.pending_writes]
        assert all(tid == "task-1" for tid in task_ids), (
            f"Expected task_id='task-1' for all writes, got {task_ids}"
        )


class TestPutWritesDeduplication:
    async def test_empty_writes_does_not_clear_existing(self) -> None:
        """aput_writes with empty list must NOT delete prior writes."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        await cp.aput(
            make_config("thread-1"),
            make_checkpoint("ckpt-1"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )
        config = make_config("thread-1", "ckpt-1")
        await cp.aput_writes(config, [("channel", "value")], task_id="task-1")

        await cp.aput_writes(config, [], task_id="task-1")

        tup = await cp.aget_tuple(config)
        assert tup is not None
        assert len(tup.pending_writes) == 1

    async def test_retry_replaces_previous_writes(self) -> None:
        """Calling aput_writes twice with same task_id must replace, not accumulate."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        await cp.aput(
            make_config("thread-1"),
            make_checkpoint("ckpt-1"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )
        config = make_config("thread-1", "ckpt-1")

        await cp.aput_writes(config, [("ch_old", "old_val")], task_id="task-1")
        await cp.aput_writes(config, [("ch_new", "new_val")], task_id="task-1")

        tup = await cp.aget_tuple(config)
        assert tup is not None
        assert len(tup.pending_writes) == 1
        assert tup.pending_writes[0][1] == "ch_new"

    async def test_parent_config_populated(self) -> None:
        """Checkpoints with a parent_id must produce a non-None parent_config."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        await cp.aput(
            make_config("thread-1"),
            make_checkpoint("ckpt-1"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )

        config2 = make_config("thread-1")
        config2["configurable"]["checkpoint_id"] = "ckpt-1"
        await cp.aput(
            config2,
            make_checkpoint("ckpt-2"),
            {"source": "loop", "step": 1, "writes": {}, "parents": {}},
            {},
        )

        tup = await cp.aget_tuple(make_config("thread-1", "ckpt-2"))
        assert tup is not None
        assert tup.parent_config is not None
        assert tup.parent_config["configurable"]["checkpoint_id"] == "ckpt-1"


class TestTimeout:
    async def test_exec_timeout_raises(self) -> None:
        """_exec must raise TimeoutError when KG operation exceeds timeout."""
        import asyncio

        async def slow_execute(iql: str) -> None:
            await asyncio.sleep(10)

        kg = AsyncMock()
        kg.execute = slow_execute
        cp = InputLayerCheckpointer(kg=kg, kg_timeout=0.01)
        cp._setup_done = True

        with pytest.raises(TimeoutError, match="timed out"):
            await cp._exec("?test(X)")

    def test_sync_setup(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        cp.setup_sync()
        assert cp._setup_done is True


class TestPrune:
    async def test_prune_removes_old_checkpoints(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        for i in range(5):
            await cp.aput(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )
        removed = await cp.prune_thread("thread-1", keep_last=2)
        assert removed == 3

        # Verify exactly which checkpoints survived (most recent two)
        remaining = [tup async for tup in cp.alist(make_config("thread-1"))]
        assert len(remaining) == 2
        remaining_ids = {tup.checkpoint["id"] for tup in remaining}
        assert remaining_ids == {"ckpt-4", "ckpt-3"}, (
            f"Expected ckpt-4 and ckpt-3 to survive, got {remaining_ids}"
        )

    async def test_prune_noop_when_under_limit(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        await cp.aput(
            make_config("thread-1"),
            make_checkpoint("ckpt-1"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )
        removed = await cp.prune_thread("thread-1", keep_last=10)
        assert removed == 0

    async def test_prune_invalid_keep_last_raises(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        with pytest.raises(ValueError, match="keep_last"):
            await cp.prune_thread("thread-1", keep_last=0)

    def test_sync_prune(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        for i in range(5):
            cp.put(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )
        removed = cp.prune_thread_sync("thread-1", keep_last=2)
        assert removed == 3


class TestSyncBridge:
    def test_sync_put_and_get(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        config = make_config("thread-1")
        ckpt = make_checkpoint("ckpt-1")
        meta: CheckpointMetadata = {"source": "input", "step": 0, "writes": {}, "parents": {}}

        new_config = cp.put(config, ckpt, meta, {})
        assert new_config["configurable"]["checkpoint_id"] == "ckpt-1"

        get_config = make_config("thread-1", "ckpt-1")
        tup = cp.get_tuple(get_config)
        assert tup is not None
        assert tup.checkpoint["id"] == "ckpt-1"

    def test_sync_put_writes(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        cp.put(
            make_config("thread-1"),
            make_checkpoint("ckpt-1"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )
        config = make_config("thread-1", "ckpt-1")
        cp.put_writes(config, [("ch", "val")], task_id="task-1")

        tup = cp.get_tuple(config)
        assert tup is not None
        assert len(tup.pending_writes) == 1

    def test_sync_list(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i in range(3):
            cp.put(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        results = list(cp.list(make_config("thread-1")))
        assert len(results) == 3


class TestSerialization:
    async def test_complex_state_roundtrip(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        ckpt = empty_checkpoint()
        ckpt["id"] = "ckpt-1"
        ckpt["channel_values"] = {
            "messages": ["hello", "world"],
            "counter": 42,
            "nested": {"key": "value", "list": [1, 2, 3]},
        }

        await cp.aput(
            make_config("thread-1"),
            ckpt,
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )

        tup = await cp.aget_tuple(make_config("thread-1", "ckpt-1"))
        assert tup is not None
        assert tup.checkpoint["channel_values"]["counter"] == 42
        assert tup.checkpoint["channel_values"]["messages"] == ["hello", "world"]
        assert tup.checkpoint["channel_values"]["nested"]["key"] == "value"

    async def test_metadata_with_special_chars(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        meta: CheckpointMetadata = {
            "source": "input",
            "step": 0,
            "writes": {"node1": {"key": 'value with "quotes" and \\ backslashes'}},
            "parents": {},
        }

        await cp.aput(
            make_config("thread-1"),
            make_checkpoint("ckpt-1"),
            meta,
            {},
        )

        tup = await cp.aget_tuple(make_config("thread-1", "ckpt-1"))
        assert tup is not None
        assert tup.metadata == meta


class TestErrorPaths:
    async def test_aput_missing_thread_id_raises(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        with pytest.raises(KeyError, match="thread_id"):
            await cp.aput(
                {"configurable": {}},
                make_checkpoint("ckpt-1"),
                {"source": "input", "step": 0, "writes": {}, "parents": {}},
                {},
            )

    async def test_aput_writes_missing_thread_id_raises(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        with pytest.raises(KeyError, match="thread_id"):
            await cp.aput_writes(
                {"configurable": {}},
                [("channel", "value")],
                task_id="task-1",
            )

    async def test_aput_writes_missing_checkpoint_id_raises(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        with pytest.raises(KeyError, match="checkpoint_id"):
            await cp.aput_writes(
                {"configurable": {"thread_id": "t"}},
                [("channel", "value")],
                task_id="task-1",
            )

    async def test_aget_tuple_missing_thread_id_raises(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        with pytest.raises(KeyError, match="thread_id"):
            await cp.aget_tuple({"configurable": {}})

    async def test_alist_missing_thread_id_raises(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        results = []
        with pytest.raises(KeyError, match="thread_id"):
            async for tup in cp.alist({"configurable": {}}):
                results.append(tup)


class TestRepr:
    def test_repr_includes_kg_name(self) -> None:
        kg = MockKG()
        kg.name = "test_kg"
        cp = InputLayerCheckpointer(kg=kg)
        r = repr(cp)
        assert "test_kg" in r
        assert "setup_done=False" in r

    async def test_repr_after_setup(self) -> None:
        kg = MockKG()
        kg.name = "test_kg"
        cp = InputLayerCheckpointer(kg=kg)
        await cp.setup()
        r = repr(cp)
        assert "setup_done=True" in r


class TestConcurrentSetup:
    async def test_concurrent_setup_is_safe(self) -> None:
        """Multiple concurrent setup() calls should run DDL exactly once."""
        import asyncio

        kg = AsyncMock()
        kg.execute = AsyncMock(return_value=ResultSet(columns=[], rows=[]))
        cp = InputLayerCheckpointer(kg=kg)

        await asyncio.gather(*(cp.setup() for _ in range(10)))

        assert cp._setup_done is True
        # DDL should run exactly 2 times (graph_checkpoint + graph_write schemas)
        assert kg.execute.await_count == 2


class TestConcurrentWrites:
    async def test_concurrent_aput_same_thread_distinct_checkpoints(self) -> None:
        """Ten concurrent aput calls with distinct checkpoint_ids must all land."""
        import asyncio

        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        await cp.setup()

        cfg = make_config("t1")

        async def _put(i: int) -> None:
            ck = empty_checkpoint()
            ck["id"] = f"ck-{i}"
            ck["channel_values"] = {"counter": i}
            await cp.aput(cfg, ck, CheckpointMetadata(step=i), {})

        await asyncio.gather(*(_put(i) for i in range(10)))
        assert len({c[2] for c in kg.checkpoints}) == 10

    async def test_concurrent_aput_writes_distinct_tasks(self) -> None:
        """Concurrent writes for different tasks under the same checkpoint."""
        import asyncio

        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        await cp.setup()

        ck = make_checkpoint("ck-1")
        cfg = make_config("t1", checkpoint_id="ck-1")
        await cp.aput(make_config("t1"), ck, CheckpointMetadata(step=0), {})

        async def _put_writes(task_id: str) -> None:
            await cp.aput_writes(
                cfg,
                writes=[("channel_a", {"val": task_id})],
                task_id=task_id,
            )

        await asyncio.gather(*(_put_writes(f"task-{i}") for i in range(5)))
        task_ids = {w[3] for w in kg.writes}
        assert task_ids == {f"task-{i}" for i in range(5)}


class TestLargePayload:
    async def test_aput_with_large_channel_values(self) -> None:
        """A checkpoint with a >1MB payload must round-trip cleanly."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        await cp.setup()

        big_text = "x" * (1_500_000)
        ck = make_checkpoint("ck-big")
        ck["channel_values"] = {"doc": big_text}
        cfg = make_config("t1")
        await cp.aput(cfg, ck, CheckpointMetadata(step=0), {})

        got = await cp.aget_tuple(make_config("t1", checkpoint_id="ck-big"))
        assert got is not None
        assert got.checkpoint["channel_values"]["doc"] == big_text


class TestUnicode:
    async def test_unicode_in_channel_values(self) -> None:
        """Emoji, RTL marks, zero-width chars must survive serialization."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        await cp.setup()

        payload = {
            "emoji": "hello \U0001f600 world",
            "rtl": "abc \u202eefg",
            "zwj": "woman\u200d\U0001f527",
            "nul_in_text": "before\x00after",
        }
        ck = make_checkpoint("ck-uni")
        ck["channel_values"] = payload
        await cp.aput(make_config("t1"), ck, CheckpointMetadata(step=0), {})

        got = await cp.aget_tuple(make_config("t1", checkpoint_id="ck-uni"))
        assert got is not None
        assert got.checkpoint["channel_values"] == payload


class TestNoneValues:
    async def test_none_channel_value(self) -> None:
        """None values in channel_values must round-trip."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        await cp.setup()

        ck = make_checkpoint("ck-none")
        ck["channel_values"] = {"optional": None, "required": "set"}
        await cp.aput(make_config("t1"), ck, CheckpointMetadata(step=0), {})

        got = await cp.aget_tuple(make_config("t1", checkpoint_id="ck-none"))
        assert got is not None
        assert got.checkpoint["channel_values"]["optional"] is None
        assert got.checkpoint["channel_values"]["required"] == "set"


class TestIqlInjectionCheckpoint:
    async def test_malicious_thread_id_round_trips(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        await cp.setup()

        # Realistic injection payload (avoids ) , < > which
        # validate_thread_id rejects - see memory.py / checkpointer.py).
        tid = 'bad"; DROP; -- graph_checkpoint'
        ck = make_checkpoint("ck-1")
        await cp.aput(make_config(tid), ck, CheckpointMetadata(step=0), {})
        got = await cp.aget_tuple(make_config(tid, checkpoint_id="ck-1"))
        assert got is not None
        assert got.config["configurable"]["thread_id"] == tid

    async def test_checkpointer_round_trips_parser_hostile_thread_id(self) -> None:
        """Thread IDs are b64-encoded on the wire, so parser-hostile chars are safe."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        await cp.setup()

        ck = make_checkpoint("ck-1")
        for tid in ["bad,tid", "bad)tid", "bad<tid", "bad>tid"]:
            await cp.aput(make_config(tid), ck, CheckpointMetadata(step=0), {})
            got = await cp.aget_tuple(make_config(tid, checkpoint_id="ck-1"))
            assert got is not None, f"round-trip failed for {tid!r}"
