"""Checkpointer tests: list, filtering, parent config, writes grouping."""

from __future__ import annotations

from inputlayer.integrations.langgraph import InputLayerCheckpointer

from ._mock_checkpoint_kg import MockKG, make_checkpoint, make_config


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


class TestListWithWrites:
    async def test_list_with_pending_writes(self) -> None:
        """alist must populate pending_writes from writes associated with each checkpoint."""
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
        for tup in results:
            assert len(tup.pending_writes) == 1


class TestListParentConfig:
    async def test_list_populates_parent_config(self) -> None:
        """alist must set parent_config when the checkpoint has a parent_id."""
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

        results = [tup async for tup in cp.alist(make_config("thread-1"))]
        assert len(results) == 2

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
        assert len(results) == 3
