"""Core memory tests: setup, store, recall, thread isolation, node factories, topics."""

from __future__ import annotations

from inputlayer.integrations.langgraph import InputLayerMemory

from ._mock_memory_kg import MockMemoryKG, b64e


class TestSetup:
    async def test_setup_creates_relations_and_rules(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.setup()
        # 2 relation definitions + 3 rules = 5 DDL statements
        assert len(kg.executed) == 5
        ddl = kg.executed
        # Verify the two base relations are defined
        assert any("memory_turn(" in q and "thread_id" in q for q in ddl), (
            f"setup must define memory_turn relation, got: {ddl}"
        )
        assert any("memory_topic(" in q and "thread_id" in q for q in ddl), (
            f"setup must define memory_topic relation, got: {ddl}"
        )
        # Verify all three derived rules are defined
        assert any("active_topic(" in q and "<-" in q for q in ddl), (
            f"setup must define active_topic rule, got: {ddl}"
        )
        assert any("relevant_turn(" in q and "<-" in q for q in ddl), (
            f"setup must define relevant_turn rule, got: {ddl}"
        )
        assert any("topic_thread(" in q and "<-" in q for q in ddl), (
            f"setup must define topic_thread rule, got: {ddl}"
        )

    async def test_setup_idempotent(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.setup()
        count = len(kg.executed)
        assert count == 5
        await mem.setup()
        assert len(kg.executed) == count


class TestStore:
    async def test_store_basic(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        turn_id = await mem.astore("thread-1", "user", "Hello world")
        assert turn_id == 1
        assert len(kg.turns) == 1
        assert kg.turns[0][0] == "thread-1"
        assert kg.turns[0][2] == b64e("user")
        assert kg.turns[0][3] == b64e("Hello world")

    async def test_store_auto_extracts_topics(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore(
            "thread-1",
            "user",
            "I need help with Python machine learning",
        )
        topic_names = [t[2] for t in kg.topics]
        assert "python" in topic_names
        assert "ml" in topic_names

    async def test_store_explicit_topics(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore(
            "thread-1",
            "user",
            "Custom message",
            topics=["custom_topic", "another"],
        )
        topic_names = [t[2] for t in kg.topics]
        assert "custom_topic" in topic_names
        assert "another" in topic_names

    async def test_store_increments_turn_id(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        id1 = await mem.astore("t", "user", "first")
        id2 = await mem.astore("t", "user", "second")
        id3 = await mem.astore("t", "user", "third")
        assert id1 == 1
        assert id2 == 2
        assert id3 == 3

    async def test_store_base64_encodes_content(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        content = 'She said "hello" and used a \\ backslash'
        await mem.astore("t", "user", content)
        assert len(kg.turns) == 1
        assert kg.turns[0][3] == b64e(content)

    def test_store_sync(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        turn_id = mem.store("t", "user", "sync message")
        assert turn_id == 1
        assert len(kg.turns) == 1


class TestRecall:
    async def test_recall_topics(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Help with Python ML")
        ctx = await mem.arecall("t")
        assert "python" in ctx["topics"]
        assert "ml" in ctx["topics"]

    async def test_recall_recent_turns(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "First message")
        await mem.astore("t", "assistant", "Response here")
        await mem.astore("t", "user", "Second question")

        ctx = await mem.arecall("t")
        assert len(ctx["recent"]) == 3
        assert ctx["recent"][0]["content"] == "Second question"

    async def test_recall_relevant_turns(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Help with Python")
        await mem.astore("t", "assistant", "Sure, Python is great")
        await mem.astore("t", "user", "What about Rust?")

        ctx = await mem.arecall("t")
        assert "python" in ctx["relevant"]
        python_turns = ctx["relevant"]["python"]
        assert len(python_turns) >= 1

    async def test_recall_related_topics(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore(
            "t",
            "user",
            "I want to use Python for machine learning",
        )
        ctx = await mem.arecall("t")
        flat = {item for pair in ctx["related_topics"] for item in pair}
        assert "python" in flat
        assert "ml" in flat

    async def test_recall_related_topics_deduplicated(self) -> None:
        """Multiple turns with the same topic pair must not produce duplicate pairs."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Python ML is great", topics=["python", "ml"])
        await mem.astore("t", "user", "More Python ML work", topics=["python", "ml"])
        ctx = await mem.arecall("t")
        assert len(ctx["related_topics"]) == 1

    async def test_recall_max_recent(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg, max_recent=3)
        for i in range(10):
            await mem.astore("t", "user", f"Message {i}")

        ctx = await mem.arecall("t")
        assert len(ctx["recent"]) == 3

    async def test_recall_empty_thread(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        ctx = await mem.arecall("nonexistent")
        assert ctx["topics"] == []
        assert ctx["recent"] == []
        assert ctx["relevant"] == {}
        assert ctx["related_topics"] == []

    def test_recall_sync(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        mem.store("t", "user", "Hello Python world")
        ctx = mem.recall("t")
        assert "python" in ctx["topics"]
        assert len(ctx["recent"]) == 1


class TestThreadIsolation:
    async def test_threads_isolated(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("thread-A", "user", "Python question")
        await mem.astore("thread-B", "user", "Rust question")

        ctx_a = await mem.arecall("thread-A")
        ctx_b = await mem.arecall("thread-B")

        assert "python" in ctx_a["topics"]
        assert "rust" not in ctx_a["topics"]
        assert "rust" in ctx_b["topics"]
        assert "python" not in ctx_b["topics"]


class TestNodeFactories:
    async def test_store_node(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        node = mem.store_node()

        state = {
            "thread_id": "t",
            "new_message": {"role": "user", "content": "Test message"},
        }
        await node(state)
        assert len(kg.turns) == 1
        assert kg.turns[0][3] == b64e("Test message")

    async def test_store_node_no_message(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        node = mem.store_node()

        result = await node({"thread_id": "t"})
        assert result == {}
        assert len(kg.turns) == 0

    async def test_recall_node(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Python ML question")

        node = mem.recall_node()
        result = await node({"thread_id": "t"})

        assert "context" in result
        assert "python" in result["context"]["topics"]

    async def test_recall_node_custom_keys(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Hello")

        node = mem.recall_node(state_key="memory", thread_key="tid")
        result = await node({"tid": "t"})
        assert "memory" in result

    async def test_node_names(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        assert mem.store_node().__name__ == "memory_store"
        assert mem.recall_node().__name__ == "memory_recall"


class TestTopicExtraction:
    async def test_python_topics(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "How to use pandas in Python?")
        topics = [t[2] for t in kg.topics]
        assert "python" in topics

    async def test_multiple_topics(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore(
            "t",
            "user",
            "Deploy a machine learning model with Docker and Kubernetes",
        )
        topics = [t[2] for t in kg.topics]
        assert "ml" in topics
        assert "devops" in topics

    async def test_no_topics(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Hello there")
        assert len(kg.topics) == 0

    async def test_no_false_positives(self) -> None:
        """Generic messages must not extract spurious topics."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "I went to the store and bought milk")
        assert len(kg.topics) == 0, (
            f"Expected no topics for generic message, got: {[t[2] for t in kg.topics]}"
        )

    async def test_topic_extraction_matches_expected_set(self) -> None:
        """Verify the exact set of extracted topics, not just that some match."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Deploy a Docker container with Python")
        topics = sorted(t[2] for t in kg.topics)
        assert topics == ["devops", "python"], (
            f"Expected exactly ['devops', 'python'], got {topics}"
        )
