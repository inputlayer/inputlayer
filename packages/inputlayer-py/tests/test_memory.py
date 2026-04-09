"""Tests for inputlayer.integrations.langgraph.memory."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from inputlayer.integrations.langgraph import InputLayerMemory
from inputlayer.result import ResultSet

# ── Mock KG ──────────────────────────────────────────────────────────


@dataclass
class MockMemoryKG:
    """In-memory KG that handles memory_turn, memory_topic, and derived rules."""

    turns: list[tuple[str, int, str, str, int]] = field(default_factory=list)
    topics: list[tuple[str, int, str]] = field(default_factory=list)
    executed: list[str] = field(default_factory=list)

    async def execute(self, datalog: str) -> ResultSet:
        self.executed.append(datalog)

        # Schema / rule definitions — no-op
        if ":" in datalog and datalog.startswith("+memory_"):
            return ResultSet(columns=[], rows=[])
        if datalog.startswith("+active_topic") and "<-" in datalog:
            return ResultSet(columns=[], rows=[])
        if datalog.startswith("+relevant_turn") and "<-" in datalog:
            return ResultSet(columns=[], rows=[])
        if datalog.startswith("+topic_thread") and "<-" in datalog:
            return ResultSet(columns=[], rows=[])

        # Insert memory_turn
        if datalog.startswith("+memory_turn("):
            m = re.match(
                r'\+memory_turn\("([^"]*)", (\d+), "([^"]*)", "((?:[^"\\]|\\.)*)", (\d+)\)',
                datalog,
            )
            if m:
                thread_id = m.group(1)
                turn_id = int(m.group(2))
                role = m.group(3)
                content = m.group(4).replace('\\"', '"').replace("\\\\", "\\")
                ts = int(m.group(5))
                self.turns.append((thread_id, turn_id, role, content, ts))
            return ResultSet(columns=[], rows=[])

        # Insert memory_topic
        if datalog.startswith("+memory_topic("):
            m = re.match(r'\+memory_topic\("([^"]*)", (\d+), "([^"]*)"\)', datalog)
            if m:
                self.topics.append((m.group(1), int(m.group(2)), m.group(3)))
            return ResultSet(columns=[], rows=[])

        # Query active_topic
        if datalog.startswith("?active_topic("):
            thread_id = self._extract_thread(datalog)
            seen = set()
            rows = []
            for t in self.topics:
                if t[0] == thread_id and t[2] not in seen:
                    seen.add(t[2])
                    rows.append([thread_id, t[2]])
            return ResultSet(columns=["thread_id", "topic"], rows=rows)

        # Query memory_turn
        if datalog.startswith("?memory_turn("):
            thread_id = self._extract_thread(datalog)
            rows = [[t[0], t[1], t[2], t[3], t[4]] for t in self.turns if t[0] == thread_id]
            return ResultSet(
                columns=["thread_id", "turn_id", "role", "content", "ts"],
                rows=rows,
            )

        # Query relevant_turn
        if datalog.startswith("?relevant_turn("):
            thread_id = self._extract_thread(datalog)
            rows = []
            for turn in self.turns:
                if turn[0] != thread_id:
                    continue
                for topic in self.topics:
                    if topic[0] == thread_id and topic[1] == turn[1]:
                        rows.append([thread_id, turn[1], turn[2], turn[3], topic[2]])
            return ResultSet(
                columns=["thread_id", "turn_id", "role", "content", "topic"],
                rows=rows,
            )

        # Query topic_thread
        if datalog.startswith("?topic_thread("):
            thread_id = self._extract_thread(datalog)
            thread_topics = {t[2] for t in self.topics if t[0] == thread_id}
            rows = []
            topic_list = sorted(thread_topics)
            for i, a in enumerate(topic_list):
                for b in topic_list[i + 1 :]:
                    rows.append([thread_id, a, b])
            return ResultSet(columns=["thread_id", "topic_a", "topic_b"], rows=rows)

        return ResultSet(columns=[], rows=[])

    def _extract_thread(self, datalog: str) -> str:
        m = re.search(r'"([^"]+)"', datalog)
        return m.group(1) if m else ""


# ── Tests ────────────────────────────────────────────────────────────


class TestSetup:
    async def test_setup_creates_relations_and_rules(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.setup()
        # Should have created 2 relations + 3 rules = 5 execute calls
        assert len(kg.executed) >= 5

    async def test_setup_idempotent(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.setup()
        count = len(kg.executed)
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
        assert kg.turns[0][2] == "user"
        assert kg.turns[0][3] == "Hello world"

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

    async def test_store_escapes_special_chars(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore(
            "t",
            "user",
            'She said "hello" and used a \\ backslash',
        )
        assert len(kg.turns) == 1
        assert '"hello"' in kg.turns[0][3]

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
        # Most recent first
        assert ctx["recent"][0]["content"] == "Second question"

    async def test_recall_relevant_turns(self) -> None:
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Help with Python")
        await mem.astore("t", "assistant", "Sure, Python is great")
        await mem.astore("t", "user", "What about Rust?")

        ctx = await mem.arecall("t")
        # "python" topic should link turns 1 and 2
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
        # Should find (ml, python) as related topics
        flat = {item for pair in ctx["related_topics"] for item in pair}
        assert "python" in flat
        assert "ml" in flat

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
        assert kg.turns[0][3] == "Test message"

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
