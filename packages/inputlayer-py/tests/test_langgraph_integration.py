"""Live integration tests for LangGraph components against a real server.

These run only when ``INPUTLAYER_INTEGRATION=1``. They exercise the full
stack: WebSocket protocol, IQL parsing, schema creation, fact insertion,
rule derivation, and the checkpointer/memory round-trip that mocks cannot
replicate.

Usage::

    INPUTLAYER_INTEGRATION=1 uv run pytest tests/test_langgraph_integration.py -v
"""

from __future__ import annotations

import os
import uuid

import pytest

requires_integration = pytest.mark.skipif(
    os.environ.get("INPUTLAYER_INTEGRATION") != "1",
    reason="set INPUTLAYER_INTEGRATION=1 to enable live-server tests",
)


def _live_url() -> str:
    return os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws")


def _live_user() -> str:
    return os.environ.get("INPUTLAYER_USER", "admin")


def _live_password() -> str:
    return os.environ.get("INPUTLAYER_PASSWORD", "admin")


# ═══════════════════════════════════════════════════════════════════════
#  1. InputLayerCheckpointer against a live server
# ═══════════════════════════════════════════════════════════════════════


@requires_integration
class TestLiveCheckpointer:
    """Full round-trip: setup schema, put checkpoint, get it back."""

    async def test_checkpointer_put_get_round_trip(self) -> None:
        from inputlayer import InputLayer
        from inputlayer.integrations.langgraph import InputLayerCheckpointer

        kg_name = f"il_lg_ckpt_{uuid.uuid4().hex[:8]}"
        async with InputLayer(_live_url(), username=_live_user(), password=_live_password()) as il:
            try:
                kg = il.knowledge_graph(kg_name)
                cp = InputLayerCheckpointer(kg=kg)
                await cp.setup()

                thread_id = "test-thread-1"
                config = {"configurable": {"thread_id": thread_id}}

                checkpoint = {
                    "v": 1,
                    "id": "ckpt-001",
                    "ts": "2026-04-15T00:00:00Z",
                    "channel_values": {"counter": 42, "name": "test"},
                    "channel_versions": {},
                    "versions_seen": {},
                    "pending_sends": [],
                }
                metadata = {"source": "input", "step": 0}

                result_config = await cp.aput(
                    config,
                    checkpoint,
                    metadata,
                    {},
                )
                assert "configurable" in result_config
                assert result_config["configurable"]["thread_id"] == thread_id

                tup = await cp.aget_tuple(config)
                assert tup is not None
                assert tup.checkpoint["id"] == "ckpt-001"
                assert tup.checkpoint["channel_values"]["counter"] == 42
                assert tup.metadata["source"] == "input"
            finally:
                await il.drop_knowledge_graph(kg_name)

    async def test_checkpointer_put_writes_round_trip(self) -> None:
        from inputlayer import InputLayer
        from inputlayer.integrations.langgraph import InputLayerCheckpointer

        kg_name = f"il_lg_ckptw_{uuid.uuid4().hex[:8]}"
        async with InputLayer(_live_url(), username=_live_user(), password=_live_password()) as il:
            try:
                kg = il.knowledge_graph(kg_name)
                cp = InputLayerCheckpointer(kg=kg)
                await cp.setup()

                thread_id = "test-thread-2"
                config = {"configurable": {"thread_id": thread_id}}

                checkpoint = {
                    "v": 1,
                    "id": "ckpt-w01",
                    "ts": "2026-04-15T00:00:00Z",
                    "channel_values": {},
                    "channel_versions": {},
                    "versions_seen": {},
                    "pending_sends": [],
                }
                result_config = await cp.aput(config, checkpoint, {}, {})

                writes = [("messages", "hello"), ("counter", 5)]
                await cp.aput_writes(result_config, writes, "task-001")

                tup = await cp.aget_tuple(config)
                assert tup is not None
                assert len(tup.pending_writes) == 2
                channels = sorted(w[1] for w in tup.pending_writes)
                assert channels == ["counter", "messages"]
            finally:
                await il.drop_knowledge_graph(kg_name)

    async def test_checkpointer_list_and_prune(self) -> None:
        from inputlayer import InputLayer
        from inputlayer.integrations.langgraph import InputLayerCheckpointer

        kg_name = f"il_lg_ckptl_{uuid.uuid4().hex[:8]}"
        async with InputLayer(_live_url(), username=_live_user(), password=_live_password()) as il:
            try:
                kg = il.knowledge_graph(kg_name)
                cp = InputLayerCheckpointer(kg=kg)
                await cp.setup()

                thread_id = "test-thread-3"
                config = {"configurable": {"thread_id": thread_id}}

                for i in range(5):
                    checkpoint = {
                        "v": 1,
                        "id": f"ckpt-{i:03d}",
                        "ts": f"2026-04-15T00:00:{i:02d}Z",
                        "channel_values": {"step": i},
                        "channel_versions": {},
                        "versions_seen": {},
                        "pending_sends": [],
                    }
                    await cp.aput(config, checkpoint, {"step": i}, {})

                listed = []
                async for tup in cp.alist(config):
                    listed.append(tup)
                assert len(listed) == 5

                await cp.prune_thread(thread_id, keep_last=2)

                listed_after = []
                async for tup in cp.alist(config):
                    listed_after.append(tup)
                assert len(listed_after) == 2
            finally:
                await il.drop_knowledge_graph(kg_name)


# ═══════════════════════════════════════════════════════════════════════
#  2. InputLayerMemory against a live server
# ═══════════════════════════════════════════════════════════════════════


@requires_integration
class TestLiveMemory:
    """Full round-trip: setup rules, store turns, recall derived context."""

    async def test_memory_store_and_recall(self) -> None:
        from inputlayer import InputLayer
        from inputlayer.integrations.langgraph import InputLayerMemory

        kg_name = f"il_lg_mem_{uuid.uuid4().hex[:8]}"
        async with InputLayer(_live_url(), username=_live_user(), password=_live_password()) as il:
            try:
                kg = il.knowledge_graph(kg_name)
                mem = InputLayerMemory(kg=kg)
                await mem.setup()

                turn1 = await mem.astore(
                    "live-thread",
                    "user",
                    "I need help with Python machine learning",
                )
                assert turn1 == 1

                turn2 = await mem.astore(
                    "live-thread",
                    "assistant",
                    "Sure! What framework are you using?",
                )
                assert turn2 == 2

                turn3 = await mem.astore(
                    "live-thread",
                    "user",
                    "I want to deploy with Docker",
                    topics=["devops"],
                )
                assert turn3 == 3

                ctx = await mem.arecall("live-thread")

                assert "python" in ctx["topics"]
                assert "ml" in ctx["topics"]
                assert "devops" in ctx["topics"]

                assert len(ctx["recent"]) == 3
                assert ctx["recent"][0]["turn_id"] == 3

                assert "python" in ctx["relevant"] or "ml" in ctx["relevant"]

                assert len(ctx["related_topics"]) > 0
            finally:
                await il.drop_knowledge_graph(kg_name)

    async def test_memory_thread_isolation(self) -> None:
        from inputlayer import InputLayer
        from inputlayer.integrations.langgraph import InputLayerMemory

        kg_name = f"il_lg_memiso_{uuid.uuid4().hex[:8]}"
        async with InputLayer(_live_url(), username=_live_user(), password=_live_password()) as il:
            try:
                kg = il.knowledge_graph(kg_name)
                mem = InputLayerMemory(kg=kg)
                await mem.setup()

                await mem.astore("thread-A", "user", "Python question")
                await mem.astore("thread-B", "user", "Rust question")

                ctx_a = await mem.arecall("thread-A")
                ctx_b = await mem.arecall("thread-B")

                assert "python" in ctx_a["topics"]
                assert "rust" not in ctx_a["topics"]
                assert "rust" in ctx_b["topics"]
                assert "python" not in ctx_b["topics"]
            finally:
                await il.drop_knowledge_graph(kg_name)

    async def test_memory_special_characters(self) -> None:
        """Verify that special characters survive the round-trip through IQL."""
        from inputlayer import InputLayer
        from inputlayer.integrations.langgraph import InputLayerMemory

        kg_name = f"il_lg_memesc_{uuid.uuid4().hex[:8]}"
        async with InputLayer(_live_url(), username=_live_user(), password=_live_password()) as il:
            try:
                kg = il.knowledge_graph(kg_name)
                mem = InputLayerMemory(kg=kg)
                await mem.setup()

                tricky_content = 'She said "hello" and used a \\ backslash'
                await mem.astore(
                    "esc-thread",
                    "user",
                    tricky_content,
                    topics=["python"],
                )

                ctx = await mem.arecall("esc-thread")
                assert len(ctx["recent"]) == 1
                assert ctx["recent"][0]["content"] == tricky_content
            finally:
                await il.drop_knowledge_graph(kg_name)


# ═══════════════════════════════════════════════════════════════════════
#  3. kg_node and kg_router against a live server
# ═══════════════════════════════════════════════════════════════════════


@requires_integration
class TestLiveKgNode:
    """Verify kg_node query/insert/delete against a real KG."""

    async def test_kg_node_query(self) -> None:
        from inputlayer import InputLayer
        from inputlayer.integrations.langgraph import kg_node

        kg_name = f"il_lg_node_{uuid.uuid4().hex[:8]}"
        async with InputLayer(_live_url(), username=_live_user(), password=_live_password()) as il:
            try:
                kg = il.knowledge_graph(kg_name)
                await kg.execute("+lg_item(name: string, score: int)")
                await kg.execute('+lg_item("alpha", 10)')
                await kg.execute('+lg_item("beta", 20)')

                query_node = kg_node(
                    operation="query",
                    query="?lg_item(Name, Score)",
                    state_key="results",
                )
                result = await query_node({"kg": kg})
                result_data = result["results"]
                assert result_data["row_count"] == 2
                cols = result_data["columns"]
                rows_as_dicts = [
                    dict(zip(cols, row, strict=False)) for row in result_data["rows"]
                ]
                names = sorted(r["name"] for r in rows_as_dicts)
                assert names == ["alpha", "beta"]
            finally:
                await il.drop_knowledge_graph(kg_name)


@requires_integration
class TestLiveKgRouter:
    """Verify kg_router conditional routing against a real KG."""

    async def test_kg_router_branches(self) -> None:
        from inputlayer import InputLayer
        from inputlayer.integrations.langgraph import kg_router

        kg_name = f"il_lg_rtr_{uuid.uuid4().hex[:8]}"
        async with InputLayer(_live_url(), username=_live_user(), password=_live_password()) as il:
            try:
                kg = il.knowledge_graph(kg_name)
                await kg.execute("+lg_flag(name: string, active: string)")
                await kg.execute('+lg_flag("ready", "yes")')

                router = kg_router(
                    branches={
                        "process": '?lg_flag("ready", "yes")',
                        "handle_error": '?lg_flag("error", "yes")',
                    },
                    default="wait",
                )
                next_node = await router({"kg": kg})
                assert next_node == "process"

                # Remove the flag, should fall through to default
                await kg.execute('-lg_flag("ready", "yes")')
                next_node2 = await router({"kg": kg})
                assert next_node2 == "wait"
            finally:
                await il.drop_knowledge_graph(kg_name)
