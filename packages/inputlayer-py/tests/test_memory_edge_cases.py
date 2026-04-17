"""Memory edge case tests: escaping, strict mode, concurrency, eviction."""

from __future__ import annotations

import pytest

from inputlayer.integrations.langgraph import InputLayerMemory

from ._mock_memory_kg import MockMemoryKG, b64e


class TestEscaping:
    async def test_thread_id_with_quotes_uses_base64(self) -> None:
        """Thread IDs are base64-encoded on the wire so adversarial chars are safe."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        tid = 'thread-"special"'
        await mem.astore(tid, "user", "Hello Python")
        insert_calls = [
            c
            for c in kg.executed
            if c.startswith("+memory_turn(") and ":" not in c.split("(", 1)[1]
        ]
        assert len(insert_calls) == 1
        # The wire form uses the base64 of the thread_id, never the raw quotes.
        assert b64e(tid) in insert_calls[0]
        assert r"thread-\"special\"" not in insert_calls[0]
        # The mock decodes back so downstream tests see the plain value.
        assert kg.turns[0][0] == tid

    async def test_topic_with_quotes(self) -> None:
        """Topics are base64-encoded, so quotes are safe."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Hello", topics=['say "hi"'])
        topic_calls = [
            c
            for c in kg.executed
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
        assert kg.turns[0][3] == content

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
        assert kg.turns[0][3] == content

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


class TestThreadIdValidation:
    async def test_astore_empty_thread_id_raises(self) -> None:
        """astore must reject empty string thread_id."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        with pytest.raises(ValueError, match="non-empty string"):
            await mem.astore("", "user", "hello")

    async def test_arecall_empty_thread_id_raises(self) -> None:
        """arecall must reject empty string thread_id."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        with pytest.raises(ValueError, match="non-empty string"):
            await mem.arecall("")

    async def test_store_sync_empty_thread_id_raises(self) -> None:
        """Sync store must also reject empty thread_id."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        with pytest.raises(ValueError, match="non-empty string"):
            mem.store("", "user", "hello")

    async def test_recall_sync_empty_thread_id_raises(self) -> None:
        """Sync recall must also reject empty thread_id."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        with pytest.raises(ValueError, match="non-empty string"):
            mem.recall("")


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


class TestDeleteThread:
    async def test_delete_thread_clears_turns_and_topics(self) -> None:
        """adelete_thread must remove all turns and topics for the thread."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Hello Python", topics=["python"])
        await mem.astore("t", "user", "Hello Rust", topics=["rust"])

        assert len(kg.turns) == 2
        assert len(kg.topics) == 2

        await mem.adelete_thread("t")

        assert len(kg.turns) == 0
        assert len(kg.topics) == 0

    async def test_delete_thread_clears_in_memory_caches(self) -> None:
        """adelete_thread must clear the turn counter and thread lock."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Hello")
        assert "t" in mem._turn_counters

        await mem.adelete_thread("t")

        assert "t" not in mem._turn_counters
        assert "t" not in mem._thread_locks
        # astore() always releases its refcount before returning.
        assert mem._active_refcount.get("t", 0) == 0

    async def test_delete_thread_recall_empty_after(self) -> None:
        """Recall after delete must return empty context."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Python ML question", topics=["python", "ml"])

        ctx_before = await mem.arecall("t")
        assert len(ctx_before["topics"]) >= 1

        await mem.adelete_thread("t")

        ctx_after = await mem.arecall("t")
        assert ctx_after["topics"] == []
        assert ctx_after["recent"] == []
        assert ctx_after["relevant"] == {}
        assert ctx_after["related_topics"] == []

    async def test_delete_thread_store_restarts_from_one(self) -> None:
        """After deleting a thread, new stores must start from turn_id=1."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "first")
        await mem.astore("t", "user", "second")

        await mem.adelete_thread("t")

        turn_id = await mem.astore("t", "user", "fresh start")
        assert turn_id == 1

    async def test_delete_thread_isolates_other_threads(self) -> None:
        """Deleting one thread must not affect other threads."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("thread-A", "user", "Python question", topics=["python"])
        await mem.astore("thread-B", "user", "Rust question", topics=["rust"])

        await mem.adelete_thread("thread-A")

        ctx_b = await mem.arecall("thread-B")
        assert "rust" in ctx_b["topics"]
        assert len(ctx_b["recent"]) == 1

    async def test_delete_thread_empty_id_raises(self) -> None:
        """adelete_thread must reject empty string thread_id."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        with pytest.raises(ValueError, match="non-empty string"):
            await mem.adelete_thread("")

    def test_delete_thread_sync(self) -> None:
        """Sync delete_thread must work without an event loop."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        mem.store("t", "user", "Hello Python", topics=["python"])
        assert len(kg.turns) == 1
        mem.delete_thread("t")
        assert len(kg.turns) == 0


class TestConcurrentSetup:
    async def test_concurrent_setup_is_safe(self) -> None:
        """Multiple concurrent setup() calls should run DDL exactly once."""
        import asyncio

        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)

        await asyncio.gather(*(mem.setup() for _ in range(10)))

        assert mem._setup_done is True
        # DDL should run exactly 5 times (2 relations + 3 rules)
        assert len(kg.executed) == 5


class TestThreadLockRefcount:
    """Exercises the refcount-based eviction guard.

    Previously _active_threads was a set. Concurrent callers for the
    same thread would overlap their add()/discard() calls, leaving the
    thread unmarked-as-active while a second caller was still using the
    lock - eviction could then replace the lock mid-flight, splitting
    writers across two different Lock objects for the same thread.
    """

    async def test_concurrent_store_same_thread_assigns_unique_turn_ids(self) -> None:
        import asyncio

        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)

        turn_ids = await asyncio.gather(
            *(mem.astore("t", "user", f"msg {i}") for i in range(20)),
        )
        assert sorted(turn_ids) == list(range(1, 21))
        # Refcount must drop to zero after all calls return.
        assert mem._active_refcount.get("t", 0) == 0

    async def test_eviction_skips_thread_with_nonzero_refcount(self) -> None:
        """A thread in use must not be evicted even under capacity pressure."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg, max_tracked_threads=2)
        await mem.setup()

        # Simulate three concurrent threads. The third would normally
        # trigger eviction of the oldest; with refcount, it cannot evict
        # threads currently being used.
        # We acquire locks explicitly to keep the refcount >0.
        lock_a = await mem._acquire_thread_lock("a")
        lock_b = await mem._acquire_thread_lock("b")
        try:
            async with lock_a, lock_b:
                # Now ask for "c". Capacity is full; eviction runs; a and
                # b have refcount 1 so neither can be evicted; c is added
                # on top without deadlocking.
                await mem._acquire_thread_lock("c")
                try:
                    assert "a" in mem._thread_locks
                    assert "b" in mem._thread_locks
                    assert "c" in mem._thread_locks
                finally:
                    await mem._release_thread_lock("c")
        finally:
            await mem._release_thread_lock("a")
            await mem._release_thread_lock("b")

    async def test_refcount_drops_to_zero_on_release(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)

        lock = await mem._acquire_thread_lock("x")
        assert mem._active_refcount["x"] == 1
        lock2 = await mem._acquire_thread_lock("x")
        assert mem._active_refcount["x"] == 2
        assert lock is lock2
        await mem._release_thread_lock("x")
        assert mem._active_refcount["x"] == 1
        await mem._release_thread_lock("x")
        assert "x" not in mem._active_refcount


class TestIqlInjection:
    """Prove that untrusted input cannot escape the IQL literal.

    Content and role are base64-encoded, so arbitrary bytes there are
    safe. Thread_ids go through ``escape_iql`` but IQL's parser
    currently mishandles a few chars inside multi-arg string literals
    (``,``, ``)``, ``<``, ``>``); ``validate_thread_id`` rejects those
    up front so the SDK raises a clear error instead of relaying an
    opaque server-side parse failure. See docs/langgraph.mdx.
    """

    async def test_thread_id_with_quote_is_escaped_on_store(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)

        # Realistic injection payload that stays within the character
        # set the IQL parser actually accepts. Quote + backslash + semi
        # are the classic break-out attempt; escape_iql neutralises them.
        sneaky = 'bad"; DROP; -- '
        await mem.astore(sneaky, "user", "hi")
        assert any(t[0] == sneaky for t in kg.turns)

    async def test_thread_id_with_backslash_round_trips(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)

        tricky = 'back\\slash "quoted"'
        await mem.astore(tricky, "user", "hi", topics=["x"])
        ctx = await mem.arecall(tricky)
        assert ctx["recent"]
        assert ctx["recent"][0]["content"] == "hi"

    async def test_content_with_control_characters_safe(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)

        # Content is base64-encoded, so NUL/bell/escape are harmless.
        nasty = "hello\x00\x07\x1bworld"
        await mem.astore("t", "user", nasty)
        ctx = await mem.arecall("t")
        assert ctx["recent"][0]["content"] == nasty

    async def test_thread_id_with_formerly_unsafe_chars_round_trips(self) -> None:
        """Now that thread_id is base64-encoded on the wire, parser-unsafe
        chars are fine. The old validator rejected them; we dropped the
        validator once we moved to b64. This test pins that behaviour so
        a future regression (re-adding the validator) fails loudly.
        """
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)

        for tid in ["bad,tid", "bad)tid", "bad<tid", "bad>tid", "bad\x00tid"]:
            turn_id = await mem.astore(tid, "user", "hi")
            assert turn_id >= 1
            ctx = await mem.arecall(tid)
            assert ctx["recent"][0]["content"] == "hi"
