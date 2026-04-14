"""Memory edge case tests: escaping, strict mode, concurrency, eviction."""

from __future__ import annotations

import pytest

from inputlayer.integrations.langgraph import InputLayerMemory

from ._mock_memory_kg import MockMemoryKG, b64e


class TestEscaping:
    async def test_thread_id_with_quotes(self) -> None:
        """Thread IDs still use escape_iql (they're identifiers, not content)."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore('thread-"special"', "user", "Hello Python")
        insert_calls = [
            c for c in kg.executed
            if c.startswith("+memory_turn(") and ":" not in c.split("(", 1)[1]
        ]
        assert len(insert_calls) == 1
        assert r'thread-\"special\"' in insert_calls[0]

    async def test_topic_with_quotes(self) -> None:
        """Topics are base64-encoded, so quotes are safe."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Hello", topics=['say "hi"'])
        topic_calls = [
            c for c in kg.executed
            if c.startswith("+memory_topic(") and ":" not in c.split("(", 1)[1]
        ]
        assert len(topic_calls) == 1
        # Topic is base64-encoded, should contain the b64 of 'say "hi"'
        assert b64e('say "hi"') in topic_calls[0]

    async def test_topic_round_trip_with_special_chars(self) -> None:
        """Topics with special chars must survive base64 round-trip via recall."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Hello", topics=['say "hi"', "new\nline"])

        ctx = await mem.arecall("t")
        assert 'say "hi"' in ctx["topics"]
        assert "new\nline" in ctx["topics"]

    async def test_content_base64_round_trip(self) -> None:
        """Content with special chars must survive base64 encode/decode round-trip."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        thread_id = 'user-"alice"'
        content = 'She said "hello\\world"\nand left'
        await mem.astore(thread_id, "user", content, topics=["test"])

        assert len(kg.turns) == 1
        assert kg.turns[0][0] == thread_id
        assert kg.turns[0][3] == b64e(content)

        ctx = await mem.arecall(thread_id)
        assert len(ctx["recent"]) == 1
        assert ctx["recent"][0]["content"] == content
        assert ctx["recent"][0]["role"] == "user"
        assert "test" in ctx["topics"]

    async def test_backslash_n_round_trip(self) -> None:
        r"""Literal backslash+n (\n) must survive round-trip via base64."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        content = "path\\name"
        await mem.astore("t", "user", content, topics=["test"])

        assert len(kg.turns) == 1
        assert kg.turns[0][3] == b64e(content)

        ctx = await mem.arecall("t")
        assert ctx["recent"][0]["content"] == content

    async def test_unicode_content_round_trip(self) -> None:
        """Unicode content must survive base64 round-trip."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        content = "caf\u00e9 \U0001f600 \u4e16\u754c"
        await mem.astore("t", "user", content)

        ctx = await mem.arecall("t")
        assert ctx["recent"][0]["content"] == content


class TestStrictMode:
    async def test_store_node_strict_missing_thread_id_raises(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        node = mem.store_node(strict=True)
        with pytest.raises(ValueError, match="thread_id"):
            await node({"new_message": {"role": "user", "content": "hi"}})

    async def test_recall_node_strict_missing_thread_id_raises(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        node = mem.recall_node(strict=True)
        with pytest.raises(ValueError, match="thread_id"):
            await node({})

    async def test_store_node_missing_thread_id_falls_back_to_default(self) -> None:
        """Non-strict mode falls back to thread_id='default' with a warning."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        node = mem.store_node(strict=False)

        result = await node({"new_message": {"role": "user", "content": "hello"}})

        assert result == {}
        assert len(kg.turns) == 1
        assert kg.turns[0][0] == "default"

    async def test_recall_node_missing_thread_id_falls_back_to_default(self) -> None:
        """Non-strict recall falls back to thread_id='default' with a warning."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("default", "user", "Hello Python")

        node = mem.recall_node(strict=False)
        result = await node({})

        assert "context" in result
        assert len(result["context"]["recent"]) == 1

    async def test_store_node_non_dict_message_is_skipped(self) -> None:
        """Non-dict messages (e.g., LangChain objects) must be skipped in non-strict mode."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        node = mem.store_node(strict=False)
        result = await node({"thread_id": "t", "new_message": "plain string"})
        assert result == {}
        assert len(kg.turns) == 0

    async def test_store_node_strict_non_dict_raises(self) -> None:
        """Strict mode must raise TypeError for non-dict messages."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        node = mem.store_node(strict=True)
        with pytest.raises(TypeError, match="dict"):
            await node({"thread_id": "t", "new_message": "plain string"})

    async def test_process_restart_counter_resumes_from_kg(self) -> None:
        """Turn counter must resume from KG state, not reset to 1 after restart."""
        kg = MockMemoryKG()
        mem1 = InputLayerMemory(kg=kg)
        await mem1.astore("t", "user", "first")
        await mem1.astore("t", "user", "second")

        mem2 = InputLayerMemory(kg=kg)
        turn_id = await mem2.astore("t", "user", "third after restart")
        assert turn_id == 3

    async def test_concurrent_threads_dont_block_each_other(self) -> None:
        """Per-thread locks must not block unrelated threads."""
        import asyncio

        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)

        results = {}

        async def store_in_thread(thread_id: str, n: int) -> None:
            for _ in range(n):
                await mem.astore(thread_id, "user", "msg")
            ctx = await mem.arecall(thread_id)
            results[thread_id] = len(ctx["recent"])

        await asyncio.gather(
            store_in_thread("thread-a", 3),
            store_in_thread("thread-b", 3),
            store_in_thread("thread-c", 3),
        )

        assert results["thread-a"] == 3
        assert results["thread-b"] == 3
        assert results["thread-c"] == 3


class TestEviction:
    async def test_eviction_triggers_when_max_tracked_exceeded(self) -> None:
        """When max_tracked_threads is exceeded, oldest entries are evicted."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg, max_tracked_threads=4)

        # Store in 4 threads (at capacity)
        for i in range(4):
            await mem.astore(f"thread-{i}", "user", "msg")

        assert len(mem._thread_locks) == 4

        # Store in a 5th thread, triggers eviction
        await mem.astore("thread-4", "user", "msg")

        # Should have evicted oldest half (2 threads), kept 2 + new = 3
        assert len(mem._thread_locks) <= 3

    async def test_repr(self) -> None:
        """__repr__ must include kg name and thread count, not raw thread IDs."""
        kg = MockMemoryKG()
        kg.name = "test_kg"
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t1", "user", "msg")
        r = repr(mem)
        assert "test_kg" in r
        assert "tracked_threads=1" in r
        # Must NOT expose thread IDs
        assert "t1" not in r

    def test_setup_sync(self) -> None:
        """setup_sync must work without an event loop."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        mem.setup_sync()
        assert mem._setup_done is True
        assert len(kg.executed) == 5

    async def test_evicted_thread_reinitializes_from_kg(self) -> None:
        """Evicted threads must re-query the KG and resume correctly."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg, max_tracked_threads=2)

        # Store 2 turns in thread-0
        await mem.astore("thread-0", "user", "first")
        await mem.astore("thread-0", "user", "second")
        assert mem._turn_counters.get("thread-0") == 2

        # Store in 2 more threads to trigger eviction of thread-0
        await mem.astore("thread-1", "user", "msg")
        await mem.astore("thread-2", "user", "msg")

        # thread-0 should have been evicted
        assert "thread-0" not in mem._turn_counters

        # Now store in thread-0 again: should re-init from KG and get turn_id=3
        turn_id = await mem.astore("thread-0", "user", "third after eviction")
        assert turn_id == 3
