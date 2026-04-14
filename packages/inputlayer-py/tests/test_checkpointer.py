"""Tests for inputlayer.integrations.langgraph.checkpointer.

Uses an in-memory mock KG that simulates basic IQL insert/query
semantics for the two relations the checkpointer uses.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock

import pytest
from langgraph.checkpoint.base import Checkpoint, CheckpointMetadata, empty_checkpoint

from inputlayer.integrations.langgraph import InputLayerCheckpointer
from inputlayer.result import ResultSet


class _BoundString(str):
    """Marker for a quoted string literal in a query (bound value)."""


class _Variable(str):
    """Marker for an unquoted IQL variable in a query (free variable)."""


# ── Mock KG ──────────────────────────────────────────────────────────


@dataclass
class MockKG:
    """Minimal in-memory KG that handles only the relations the checkpointer uses."""

    checkpoints: list[tuple] = field(default_factory=list)
    writes: list[tuple] = field(default_factory=list)

    async def execute(self, iql: str) -> ResultSet:
        # Schema definitions, no-op
        if iql.startswith("+graph_checkpoint(") and ":" in iql:
            return ResultSet(columns=["x"], rows=[])
        if iql.startswith("+graph_write(") and ":" in iql:
            return ResultSet(columns=["x"], rows=[])

        # Insert a graph_checkpoint fact
        if iql.startswith("+graph_checkpoint("):
            args = self._parse_args(iql)
            # Strip marker types, store as plain values
            args = [str(a) if isinstance(a, _BoundString) else a for a in args]
            self.checkpoints.append(tuple(args))
            return ResultSet(columns=["x"], rows=[])

        # Insert a graph_write fact
        if iql.startswith("+graph_write("):
            args = self._parse_args(iql)
            args = [str(a) if isinstance(a, _BoundString) else a for a in args]
            self.writes.append(tuple(args))
            return ResultSet(columns=["x"], rows=[])

        # Conditional delete for graph_checkpoint (prune)
        if iql.startswith("-graph_checkpoint(") and "<-" in iql:
            self._delete_checkpoints(iql)
            return ResultSet(columns=["x"], rows=[])

        # Conditional delete for graph_write (deduplication in aput_writes)
        # Syntax: -graph_write(T, C, TaskId, I, Ch, B) <- T = "...", C = "...", TaskId = "..."
        if iql.startswith("-graph_write(") and "<-" in iql:
            self._delete_writes(iql)
            return ResultSet(columns=["x"], rows=[])

        # Query graph_checkpoint
        if iql.startswith("?graph_checkpoint("):
            return self._query_checkpoints(iql)

        # Query graph_write
        if iql.startswith("?graph_write("):
            return self._query_writes(iql)

        return ResultSet(columns=["x"], rows=[])

    def _parse_args(self, iql: str) -> list:
        """Extract argument values from a fact insertion."""
        # Match +relation(arg1, arg2, ...)
        m = re.match(r"\+\w+\((.*)\)$", iql)
        if not m:
            return []
        args_str = m.group(1)
        # Naive split, assumes no commas inside strings (we control encoding)
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
            inner = self._unescape(s[1:-1])
            return _BoundString(inner)
        try:
            return int(s)
        except ValueError:
            try:
                return float(s)
            except ValueError:
                return _Variable(s)

    def _query_checkpoints(self, iql: str) -> ResultSet:
        """Match query against stored checkpoints.

        Schema: (thread_id, checkpoint_ns, checkpoint_id, parent_id, blob, metadata, ts)
        """
        body = iql[len("?graph_checkpoint("):].rstrip(")")
        parts = self._parse_args("+graph_checkpoint(" + body + ")")

        # Bound values are _BoundString; free variables are _Variable
        thread_id = str(parts[0]) if isinstance(parts[0], _BoundString) else None
        checkpoint_ns = (
            str(parts[1]) if len(parts) > 1 and isinstance(parts[1], _BoundString) else None
        )
        checkpoint_id = (
            str(parts[2]) if len(parts) > 2 and isinstance(parts[2], _BoundString) else None
        )

        rows = []
        for ckpt in self.checkpoints:
            # ckpt = (thread_id, checkpoint_ns, checkpoint_id, parent_id, blob, metadata, ts)
            if thread_id and ckpt[0] != thread_id:
                continue
            if checkpoint_ns is not None and ckpt[1] != checkpoint_ns:
                continue
            if checkpoint_id and ckpt[2] != checkpoint_id:
                continue
            # Project: simulate the server dropping bound leading columns
            if checkpoint_id:
                # thread_id, checkpoint_ns, checkpoint_id all bound -> return from parent_id on
                rows.append(list(ckpt[3:]))
            elif checkpoint_ns is not None:
                # thread_id and checkpoint_ns bound -> return from checkpoint_id on
                rows.append(list(ckpt[2:]))
            else:
                # Only thread_id bound -> return from checkpoint_ns on
                rows.append(list(ckpt[1:]))

        if checkpoint_id:
            cols = ["parent_id", "blob", "metadata", "ts"]
        elif checkpoint_ns is not None:
            cols = ["checkpoint_id", "parent_id", "blob", "metadata", "ts"]
        else:
            cols = ["checkpoint_ns", "checkpoint_id", "parent_id", "blob", "metadata", "ts"]
        return ResultSet(columns=cols, rows=rows)

    def _query_writes(self, iql: str) -> ResultSet:
        body = iql[len("?graph_write("):].rstrip(")")
        parts = self._parse_args("+graph_write(" + body + ")")
        thread_id = str(parts[0]) if isinstance(parts[0], _BoundString) else None
        # checkpoint_id may be bound (filter + strip) or free (include in projection)
        checkpoint_id_bound = (
            len(parts) > 1 and isinstance(parts[1], _BoundString)
        )
        checkpoint_id = str(parts[1]) if checkpoint_id_bound else None

        rows = []
        for w in self.writes:
            if thread_id and w[0] != thread_id:
                continue
            if checkpoint_id and w[1] != checkpoint_id:
                continue
            if checkpoint_id_bound:
                # Both thread_id and checkpoint_id are bound: strip both
                rows.append(list(w[2:]))
            else:
                # checkpoint_id is a free variable: strip only thread_id, keep checkpoint_id
                rows.append(list(w[1:]))

        if checkpoint_id_bound:
            cols = ["task_id", "idx", "channel", "blob"]
        else:
            cols = ["checkpoint_id", "task_id", "idx", "channel", "blob"]

        return ResultSet(columns=cols, rows=rows)

    def _delete_checkpoints(self, iql: str) -> None:
        """Handle -graph_checkpoint(...) <- T = "...", Ns = "...", CkptId = "..."."""
        _STR = r'((?:[^"\\]|\\.)*)'
        body = iql.split("<-", 1)[1]
        thread_match = re.search(rf'ThreadId\s*=\s*"{_STR}"', body)
        ns_match = re.search(rf'Ns\s*=\s*"{_STR}"', body)
        ckpt_match = re.search(rf'CkptId\s*=\s*"{_STR}"', body)

        thread_id = self._unescape(thread_match.group(1)) if thread_match else None
        ns = self._unescape(ns_match.group(1)) if ns_match else None
        ckpt_id = self._unescape(ckpt_match.group(1)) if ckpt_match else None

        self.checkpoints = [
            c
            for c in self.checkpoints
            if not (
                (thread_id is None or c[0] == thread_id)
                and (ns is None or c[1] == ns)
                and (ckpt_id is None or c[2] == ckpt_id)
            )
        ]

    def _delete_writes(self, iql: str) -> None:
        """Handle -graph_write(...) <- T = "...", C = "...", TaskId = "..."."""
        _STR = r'((?:[^"\\]|\\.)*)'
        body = iql.split("<-", 1)[1]
        thread_match = re.search(rf'ThreadId\s*=\s*"{_STR}"', body)
        ckpt_match = re.search(rf'CkptId\s*=\s*"{_STR}"', body)
        task_match = re.search(rf'TaskId\s*=\s*"{_STR}"', body)

        thread_id = self._unescape(thread_match.group(1)) if thread_match else None
        ckpt_id = self._unescape(ckpt_match.group(1)) if ckpt_match else None
        task_id = self._unescape(task_match.group(1)) if task_match else None

        self.writes = [
            w
            for w in self.writes
            if not (
                (thread_id is None or w[0] == thread_id)
                and (ckpt_id is None or w[1] == ckpt_id)
                and (task_id is None or w[2] == task_id)
            )
        ]

    @staticmethod
    def _unescape(s: str) -> str:
        r"""Reverse escape_iql via single-pass regex (handles \\n vs \n correctly)."""
        _MAP = {"\\": "\\", '"': '"', "n": "\n", "r": "\r", "t": "\t", "0": "\0"}
        return re.sub(r"\\(.)", lambda m: _MAP.get(m.group(1), "\\" + m.group(1)), s)


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

        # Get back. pending_writes should include both
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

    async def test_list_newest_first(self) -> None:
        """alist must return checkpoints in newest-first (descending ts) order."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i in range(3):
            await cp.aput(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        results = [tup async for tup in cp.alist(make_config("thread-1"))]
        ids = [tup.checkpoint["id"] for tup in results]
        assert ids == ["ckpt-2", "ckpt-1", "ckpt-0"]

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

    async def test_namespaces_are_isolated(self) -> None:
        """Checkpoints in different checkpoint_ns must not bleed into each other."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        def ns_config(thread_id: str, ns: str) -> dict:
            return {"configurable": {"thread_id": thread_id, "checkpoint_ns": ns}}

        # Same thread_id, different namespaces (parent graph vs subgraph)
        await cp.aput(
            ns_config("thread-1", ""),
            make_checkpoint("ckpt-parent"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )
        await cp.aput(
            ns_config("thread-1", "subgraph"),
            make_checkpoint("ckpt-subgraph"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )

        parent_tup = await cp.aget_tuple(ns_config("thread-1", ""))
        sub_tup = await cp.aget_tuple(ns_config("thread-1", "subgraph"))

        assert parent_tup is not None
        assert sub_tup is not None
        # Each namespace sees only its own checkpoint
        assert parent_tup.checkpoint["id"] == "ckpt-parent"
        assert sub_tup.checkpoint["id"] == "ckpt-subgraph"


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

        # Empty write should be a no-op - existing write must survive
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

        # First attempt
        await cp.aput_writes(config, [("ch_old", "old_val")], task_id="task-1")
        # Retry with different data - must replace, not append
        await cp.aput_writes(config, [("ch_new", "new_val")], task_id="task-1")

        tup = await cp.aget_tuple(config)
        assert tup is not None
        assert len(tup.pending_writes) == 1
        assert tup.pending_writes[0][1] == "ch_new"

    async def test_parent_config_populated(self) -> None:
        """Checkpoints with a parent_id must produce a non-None parent_config."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        # First checkpoint (no parent)
        await cp.aput(
            make_config("thread-1"),
            make_checkpoint("ckpt-1"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )

        # Second checkpoint references first as parent
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


class TestListWithWrites:
    async def test_list_with_pending_writes(self) -> None:
        """alist must populate pending_writes from writes associated with each checkpoint.

        This exercises the code path in alist that queries writes with checkpoint_id
        as a free variable (w_row[-5]) and groups them by checkpoint.
        """
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

        results = [tup async for tup in cp.alist(make_config("thread-1"))]
        assert len(results) == 1
        channels = [w[1] for w in results[0].pending_writes]
        assert "messages" in channels
        assert "count" in channels

    async def test_list_multiple_checkpoints_writes_grouped(self) -> None:
        """Writes must be grouped per-checkpoint, not cross-contaminated."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i in range(2):
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

        results = [tup async for tup in cp.alist(make_config("thread-1"))]
        assert len(results) == 2
        # Each checkpoint gets exactly its own write
        for tup in results:
            assert len(tup.pending_writes) == 1


class TestListParentConfig:
    async def test_list_populates_parent_config(self) -> None:
        """alist must set parent_config when the checkpoint has a parent_id."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        # First checkpoint (no parent)
        await cp.aput(
            make_config("thread-1"),
            make_checkpoint("ckpt-1"),
            {"source": "input", "step": 0, "writes": {}, "parents": {}},
            {},
        )
        # Second checkpoint references first as parent
        config2 = make_config("thread-1")
        config2["configurable"]["checkpoint_id"] = "ckpt-1"
        await cp.aput(
            config2,
            make_checkpoint("ckpt-2"),
            {"source": "loop", "step": 1, "writes": {}, "parents": {}},
            {},
        )

        results = [tup async for tup in cp.alist(make_config("thread-1"))]
        assert len(results) == 2

        # Newest first: ckpt-2 has a parent, ckpt-1 does not
        ckpt2 = next(t for t in results if t.checkpoint["id"] == "ckpt-2")
        ckpt1 = next(t for t in results if t.checkpoint["id"] == "ckpt-1")

        assert ckpt2.parent_config is not None
        assert ckpt2.parent_config["configurable"]["checkpoint_id"] == "ckpt-1"
        assert ckpt1.parent_config is None


class TestListFiltering:
    async def test_list_with_metadata_filter(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i, source in enumerate(["input", "loop", "input"]):
            await cp.aput(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": source, "step": i, "writes": {}, "parents": {}},
                {},
            )

        results = [
            tup
            async for tup in cp.alist(
                make_config("thread-1"),
                filter={"source": "input"},
            )
        ]
        assert len(results) == 2
        assert all(tup.metadata["source"] == "input" for tup in results)

    async def test_list_before_cutoff(self) -> None:
        """alist with before= should return only checkpoints older than the cutoff."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i in range(3):
            await cp.aput(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        # Cutoff at ckpt-2 (the newest) - should return ckpt-0 and ckpt-1 only
        before_config = make_config("thread-1", "ckpt-2")
        results = [
            tup async for tup in cp.alist(make_config("thread-1"), before=before_config)
        ]
        ids = {tup.checkpoint["id"] for tup in results}
        assert "ckpt-2" not in ids
        assert len(results) == 2

    async def test_list_before_without_checkpoint_id(self) -> None:
        """before config without checkpoint_id should not filter anything."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i in range(3):
            await cp.aput(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        # before config has no checkpoint_id - no filtering applied
        before_config: dict = {"configurable": {"thread_id": "thread-1"}}
        results = [
            tup async for tup in cp.alist(make_config("thread-1"), before=before_config)
        ]
        assert len(results) == 3

    async def test_list_before_nonexistent_checkpoint(self) -> None:
        """before pointing to a nonexistent checkpoint should not filter anything."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i in range(3):
            await cp.aput(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        before_config = make_config("thread-1", "ckpt-nonexistent")
        results = [
            tup async for tup in cp.alist(make_config("thread-1"), before=before_config)
        ]
        # Nonexistent before checkpoint means no filtering
        assert len(results) == 3


class TestPrune:
    async def test_prune_removes_old_checkpoints(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i in range(10):
            await cp.aput(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        removed = await cp.aprune("thread-1", keep_last=3)
        assert removed == 7

        results = [tup async for tup in cp.alist(make_config("thread-1"))]
        assert len(results) == 3
        # The 3 most recent should survive
        ids = {tup.checkpoint["id"] for tup in results}
        assert "ckpt-9" in ids
        assert "ckpt-8" in ids
        assert "ckpt-7" in ids

    async def test_prune_noop_when_under_threshold(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i in range(3):
            await cp.aput(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        removed = await cp.aprune("thread-1", keep_last=5)
        assert removed == 0

    async def test_prune_also_removes_writes(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i in range(5):
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

        await cp.aprune("thread-1", keep_last=2)

        # Only writes for the 2 most recent checkpoints should remain
        results = [tup async for tup in cp.alist(make_config("thread-1"))]
        total_writes = sum(len(tup.pending_writes) for tup in results)
        assert total_writes == 2

    async def test_prune_invalid_keep_last_raises(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)
        with pytest.raises(ValueError, match="keep_last must be >= 1"):
            await cp.aprune("thread-1", keep_last=0)

    def test_prune_sync(self) -> None:
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        for i in range(5):
            cp.put(
                make_config("thread-1"),
                make_checkpoint(f"ckpt-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        removed = cp.prune("thread-1", keep_last=2)
        assert removed == 3


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
        """Multiple concurrent setup() calls should not duplicate DDL."""
        import asyncio

        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        # Run 10 concurrent setup calls
        await asyncio.gather(*(cp.setup() for _ in range(10)))

        assert cp._setup_done is True


class TestUnpackMalformed:
    def test_unpack_missing_separator_raises(self) -> None:
        """_unpack must raise ValueError for data without '|' separator."""
        from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

        from inputlayer.integrations.langgraph.checkpointer import _unpack

        serde = JsonPlusSerializer()
        with pytest.raises(ValueError, match="Corrupted checkpoint data"):
            _unpack(serde, "no-pipe-here")

    def test_unpack_empty_string_raises(self) -> None:
        from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

        from inputlayer.integrations.langgraph.checkpointer import _unpack

        serde = JsonPlusSerializer()
        with pytest.raises(ValueError, match="Corrupted checkpoint data"):
            _unpack(serde, "")


class TestPruneNamespaceIsolation:
    async def test_prune_only_affects_specified_namespace(self) -> None:
        """Pruning the parent namespace must not touch subgraph checkpoints."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        def ns_config(ns: str) -> dict:
            return {"configurable": {"thread_id": "thread-1", "checkpoint_ns": ns}}

        # Add 5 checkpoints in parent namespace
        for i in range(5):
            await cp.aput(
                ns_config(""),
                make_checkpoint(f"parent-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        # Add 5 checkpoints in subgraph namespace
        for i in range(5):
            await cp.aput(
                ns_config("subgraph"),
                make_checkpoint(f"sub-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        # Prune parent namespace to keep only 2
        removed = await cp.aprune("thread-1", checkpoint_ns="", keep_last=2)
        assert removed == 3

        # Subgraph should be untouched (still 5)
        sub_results = [tup async for tup in cp.alist(ns_config("subgraph"))]
        assert len(sub_results) == 5

        # Parent should have exactly 2
        parent_results = [tup async for tup in cp.alist(ns_config(""))]
        assert len(parent_results) == 2
