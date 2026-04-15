"""Checkpointer validation, error aggregation, and deletion tests."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from inputlayer.integrations.langgraph import InputLayerCheckpointer
from inputlayer.integrations.langgraph._checkpoint_serde import parse_writes, unpack
from inputlayer.result import ResultSet

from ._mock_checkpoint_kg import MockKG, make_checkpoint, make_config


class TestUnpackMalformed:
    def test_unpack_missing_separator_raises(self) -> None:
        """_unpack must raise ValueError for data without '|' separator."""
        serde = JsonPlusSerializer()
        with pytest.raises(ValueError, match="Corrupted checkpoint data"):
            unpack(serde, "no-pipe-here")

    def test_unpack_empty_string_raises(self) -> None:
        serde = JsonPlusSerializer()
        with pytest.raises(ValueError, match="Corrupted checkpoint data"):
            unpack(serde, "")


class TestParseWritesValidation:
    def test_short_row_raises(self) -> None:
        """Rows with fewer than 5 columns must raise ValueError."""
        serde = JsonPlusSerializer()
        with pytest.raises(ValueError, match="row 0 has 3 columns"):
            parse_writes(serde, [["a", "b", "c"]])


class TestRowLengthValidation:
    async def test_short_checkpoint_row_raises_on_get_with_id(self) -> None:
        """aget_tuple with checkpoint_id requires at least 4 columns."""
        kg = AsyncMock()
        kg.execute = AsyncMock(
            return_value=ResultSet(columns=["a", "b"], rows=[["x", "y"]])
        )
        cp = InputLayerCheckpointer(kg=kg)
        cp._setup_done = True

        with pytest.raises(ValueError, match="graph_checkpoint row has 2 columns"):
            await cp.aget_tuple(make_config("t", "ckpt-1"))

    async def test_short_checkpoint_row_raises_on_get_without_id(self) -> None:
        """aget_tuple without checkpoint_id requires at least 5 columns."""
        kg = AsyncMock()
        kg.execute = AsyncMock(
            return_value=ResultSet(columns=["a", "b", "c", "d"], rows=[["w", "x", "y", "z"]])
        )
        cp = InputLayerCheckpointer(kg=kg)
        cp._setup_done = True

        with pytest.raises(ValueError, match="graph_checkpoint row has 4 columns"):
            await cp.aget_tuple(make_config("t"))

    async def test_short_checkpoint_row_raises_on_list(self) -> None:
        """alist must raise ValueError for rows with too few columns."""
        kg = AsyncMock()
        kg.execute = AsyncMock(
            return_value=ResultSet(columns=["a", "b", "c"], rows=[["x", "y", "z"]])
        )
        cp = InputLayerCheckpointer(kg=kg)
        cp._setup_done = True

        with pytest.raises(ValueError, match="graph_checkpoint row has 3 columns"):
            results = []
            async for tup in cp.alist(make_config("t")):
                results.append(tup)


class TestPutWritesErrorAggregation:
    async def test_partial_write_failure_raises(self) -> None:
        """If some writes fail, aput_writes must raise with failure count."""
        call_count = 0

        async def flaky_execute(iql: str) -> ResultSet:
            nonlocal call_count
            call_count += 1
            # Let schema DDL and delete pass, fail on every other insert
            if iql.startswith("+graph_write(") and ":" not in iql and call_count % 2 == 0:
                raise RuntimeError("simulated failure")
            return ResultSet(columns=[], rows=[])

        kg = AsyncMock()
        kg.execute = AsyncMock(side_effect=flaky_execute)
        cp = InputLayerCheckpointer(kg=kg)
        cp._setup_done = True

        config = make_config("t", "ckpt-1")
        writes = [("ch1", "v1"), ("ch2", "v2"), ("ch3", "v3")]
        with pytest.raises(RuntimeError, match="writes failed"):
            await cp.aput_writes(config, writes, task_id="task-1")


class TestDeleteThread:
    async def test_delete_thread(self) -> None:
        """adelete_thread must remove all checkpoints and writes for a thread."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i in range(3):
            await cp.aput(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )
            await cp.aput_writes(
                make_config("thread-1", f"ckpt-{i}"),
                [(f"ch-{i}", f"val-{i}")],
                task_id=f"task-{i}",
            )

        await cp.adelete_thread("thread-1")

        tup = await cp.aget_tuple(make_config("thread-1"))
        assert tup is None
        assert len(kg.checkpoints) == 0
        assert len(kg.writes) == 0
