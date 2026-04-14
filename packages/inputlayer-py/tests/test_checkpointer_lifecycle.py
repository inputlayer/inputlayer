"""Checkpointer tests: thread/namespace isolation, pruning."""

from __future__ import annotations

import pytest

from inputlayer.integrations.langgraph import InputLayerCheckpointer

from ._mock_checkpoint_kg import MockKG, make_checkpoint, make_config


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
        assert parent_tup.checkpoint["id"] == "ckpt-parent"
        assert sub_tup.checkpoint["id"] == "ckpt-subgraph"


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


class TestPruneNamespaceIsolation:
    async def test_prune_only_affects_specified_namespace(self) -> None:
        """Pruning the parent namespace must not touch subgraph checkpoints."""
        kg = MockKG()
        cp = InputLayerCheckpointer(kg=kg)

        def ns_config(ns: str) -> dict:
            return {"configurable": {"thread_id": "thread-1", "checkpoint_ns": ns}}

        for i in range(5):
            await cp.aput(
                ns_config(""),
                make_checkpoint(f"parent-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        for i in range(5):
            await cp.aput(
                ns_config("subgraph"),
                make_checkpoint(f"sub-{i}"),
                {"source": "input", "step": i, "writes": {}, "parents": {}},
                {},
            )

        removed = await cp.aprune("thread-1", checkpoint_ns="", keep_last=2)
        assert removed == 3

        sub_results = [tup async for tup in cp.alist(ns_config("subgraph"))]
        assert len(sub_results) == 5

        parent_results = [tup async for tup in cp.alist(ns_config(""))]
        assert len(parent_results) == 2
