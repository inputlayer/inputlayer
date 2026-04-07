"""Tests for inputlayer.integrations.langgraph.checkpointer.

Uses an in-memory mock KG that simulates basic Datalog insert/query
semantics for the two relations the checkpointer uses.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock

from langgraph.checkpoint.base import Checkpoint, CheckpointMetadata, empty_checkpoint

from inputlayer.integrations.langgraph import InputLayerCheckpointer
from inputlayer.result import ResultSet


class _BoundString(str):
    """Marker for a quoted string literal in a query (bound value)."""


class _Variable(str):
    """Marker for an unquoted Datalog variable in a query (free variable)."""


# ── Mock KG ──────────────────────────────────────────────────────────


@dataclass
class MockKG:
    """Minimal in-memory KG that handles only the relations the checkpointer uses."""

    checkpoints: list[tuple] = field(default_factory=list)
    writes: list[tuple] = field(default_factory=list)

    async def execute(self, datalog: str) -> ResultSet:
        # Schema definitions — no-op
        if datalog.startswith("+graph_checkpoint(") and ":" in datalog:
            return ResultSet(columns=["x"], rows=[])
        if datalog.startswith("+graph_write(") and ":" in datalog:
            return ResultSet(columns=["x"], rows=[])

        # Insert a graph_checkpoint fact
        if datalog.startswith("+graph_checkpoint("):
            args = self._parse_args(datalog)
            # Strip marker types — store as plain values
            args = [str(a) if isinstance(a, _BoundString) else a for a in args]
            self.checkpoints.append(tuple(args))
            return ResultSet(columns=["x"], rows=[])

        # Insert a graph_write fact
        if datalog.startswith("+graph_write("):
            args = self._parse_args(datalog)
            args = [str(a) if isinstance(a, _BoundString) else a for a in args]
            self.writes.append(tuple(args))
            return ResultSet(columns=["x"], rows=[])

        # Query graph_checkpoint
        if datalog.startswith("?graph_checkpoint("):
            return self._query_checkpoints(datalog)

        # Query graph_write
        if datalog.startswith("?graph_write("):
            return self._query_writes(datalog)

        return ResultSet(columns=["x"], rows=[])

    def _parse_args(self, datalog: str) -> list:
        """Extract argument values from a fact insertion."""
        # Match +relation(arg1, arg2, ...)
        m = re.match(r"\+\w+\((.*)\)$", datalog)
        if not m:
            return []
        args_str = m.group(1)
        # Naive split — assumes no commas inside strings (we control encoding)
        args: list[Any] = []
        depth = 0
        current = ""
        in_str = False
        i = 0
        while i < len(args_str):
            ch = args_str[i]
            if ch == "\\" and i + 1 < len(args_str):
                current += ch + args_str[i + 1]
                i += 2
                continue
            if ch == '"':
                in_str = not in_str
                current += ch
            elif ch == "," and not in_str and depth == 0:
                args.append(self._parse_value(current.strip()))
                current = ""
            else:
                current += ch
            i += 1
        if current.strip():
            args.append(self._parse_value(current.strip()))
        return args

    def _parse_value(self, s: str) -> Any:
        if s.startswith('"') and s.endswith('"'):
            inner = s[1:-1].replace('\\"', '"').replace("\\\\", "\\")
            return _BoundString(inner)
        try:
            return int(s)
        except ValueError:
            try:
                return float(s)
            except ValueError:
                return _Variable(s)

    def _query_checkpoints(self, datalog: str) -> ResultSet:
        """Match query against stored checkpoints."""
        body = datalog[len("?graph_checkpoint("):].rstrip(")")
        parts = self._parse_args("+graph_checkpoint(" + body + ")")

        # Bound values are _BoundString; free variables are _Variable
        thread_id = str(parts[0]) if isinstance(parts[0], _BoundString) else None
        checkpoint_id = (
            str(parts[1]) if len(parts) > 1 and isinstance(parts[1], _BoundString) else None
        )

        rows = []
        for ckpt in self.checkpoints:
            if thread_id and ckpt[0] != thread_id:
                continue
            if checkpoint_id and ckpt[1] != checkpoint_id:
                continue
            # Project: drop bound columns from the result
            if checkpoint_id:
                rows.append(list(ckpt[2:]))
            else:
                rows.append(list(ckpt[1:]))

        cols = (
            ["parent_id", "blob", "metadata", "ts"]
            if checkpoint_id
            else ["checkpoint_id", "parent_id", "blob", "metadata", "ts"]
        )
        return ResultSet(columns=cols, rows=rows)

    def _query_writes(self, datalog: str) -> ResultSet:
        body = datalog[len("?graph_write("):].rstrip(")")
        parts = self._parse_args("+graph_write(" + body + ")")
        thread_id = str(parts[0]) if isinstance(parts[0], _BoundString) else None
        checkpoint_id = (
            str(parts[1]) if len(parts) > 1 and isinstance(parts[1], _BoundString) else None
        )

        rows = []
        for w in self.writes:
            if thread_id and w[0] != thread_id:
                continue
            if checkpoint_id and w[1] != checkpoint_id:
                continue
            rows.append(list(w[2:]))

        return ResultSet(
            columns=["task_id", "idx", "channel", "blob"],
            rows=rows,
        )


# ── Helpers ──────────────────────────────────────────────────────────


def make_config(thread_id: str = "thread-1", checkpoint_id: str | None = None) -> dict:
    config: dict[str, Any] = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    if checkpoint_id:
        config["configurable"]["checkpoint_id"] = checkpoint_id
    return config


def make_checkpoint(checkpoint_id: str = "ckpt-1") -> Checkpoint:
    ckpt = empty_checkpoint()
    ckpt["id"] = checkpoint_id
    return ckpt


# ── Tests ────────────────────────────────────────────────────────────


class TestSetup:
    async def test_setup_creates_relations(self) -> None:
        kg = AsyncMock()
        kg.execute = AsyncMock(return_value=ResultSet(columns=[], rows=[]))
        cp = InputLayerCheckpointer(kg=kg)
        await cp.setup()
        assert kg.execute.await_count >= 2  # graph_checkpoint + graph_write

    async def test_setup_idempotent(self) -> None:
        kg = AsyncMock()
        kg.execute = AsyncMock(return_value=ResultSet(columns=[], rows=[]))
        cp = InputLayerCheckpointer(kg=kg)
        await cp.setup()
        first_count = kg.execute.await_count
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

        # Get back
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
        # Latest = highest ts; since they were inserted sequentially,
        # the last one wins (ckpt-2)
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

        # First put a checkpoint
        await cp.aput(
            make_config("thread-1"),
            make_checkpoint("ckpt-1"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )

        # Then add writes
        config = make_config("thread-1", "ckpt-1")
        await cp.aput_writes(
            config,
            [("messages", "hello"), ("count", 1)],
            task_id="task-1",
        )

        # Get back — pending_writes should include both
        tup = await cp.aget_tuple(config)
        assert tup is not None
        assert len(tup.pending_writes) == 2
        channels = [w[1] for w in tup.pending_writes]
        assert "messages" in channels
        assert "count" in channels


class TestList:
    async def test_list_returns_checkpoints(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i in range(5):
            await cp.aput(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        results = []
        async for tup in cp.alist(make_config("thread-1")):
            results.append(tup)

        assert len(results) == 5

    async def test_list_with_limit(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i in range(5):
            await cp.aput(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        results = []
        async for tup in cp.alist(make_config("thread-1"), limit=2):
            results.append(tup)

        assert len(results) == 2

    async def test_list_none_config_returns_nothing(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        results = [tup async for tup in cp.alist(None)]
        assert results == []


class TestThreadIsolation:
    async def test_threads_are_isolated(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        await cp.aput(
            make_config("thread-A"),
            make_checkpoint("ckpt-A1"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )
        await cp.aput(
            make_config("thread-B"),
            make_checkpoint("ckpt-B1"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )

        a_tup = await cp.aget_tuple(make_config("thread-A"))
        b_tup = await cp.aget_tuple(make_config("thread-B"))

        assert a_tup is not None
        assert b_tup is not None
        assert a_tup.checkpoint["id"] == "ckpt-A1"
        assert b_tup.checkpoint["id"] == "ckpt-B1"

    async def test_list_only_returns_one_thread(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        await cp.aput(
            make_config("thread-A"),
            make_checkpoint("ckpt-A1"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )
        await cp.aput(
            make_config("thread-B"),
            make_checkpoint("ckpt-B1"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )

        a_results = [tup async for tup in cp.alist(make_config("thread-A"))]
        assert len(a_results) == 1
        assert a_results[0].checkpoint["id"] == "ckpt-A1"


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
