"""Tests for inputlayer.integrations.langgraph.memory."""

from __future__ import annotations

import base64
import re
from dataclasses import dataclass, field

import pytest

from inputlayer.integrations.langgraph import InputLayerMemory
from inputlayer.result import ResultSet


def _b64e(s: str) -> str:
    """Helper: base64-encode a string (matches memory.py's _b64e)."""
    return base64.b64encode(s.encode("utf-8")).decode("ascii")

# ── Mock KG ──────────────────────────────────────────────────────────


@dataclass
class MockMemoryKG:
    """In-memory KG that handles memory_turn, memory_topic, and derived rules."""

    turns: list[tuple[str, int, str, str, int]] = field(default_factory=list)
    topics: list[tuple[str, int, str]] = field(default_factory=list)
    executed: list[str] = field(default_factory=list)

    async def execute(self, iql: str) -> ResultSet:
        self.executed.append(iql)

        # Schema / rule definitions, no-op
        if ":" in iql and iql.startswith("+memory_"):
            return ResultSet(columns=[], rows=[])
        if iql.startswith("+active_topic") and "<-" in iql:
            return ResultSet(columns=[], rows=[])
        if iql.startswith("+relevant_turn") and "<-" in iql:
            return ResultSet(columns=[], rows=[])
        if iql.startswith("+topic_thread") and "<-" in iql:
            return ResultSet(columns=[], rows=[])

        # Insert memory_turn
        if iql.startswith("+memory_turn("):
            _STR = r'((?:[^"\\]|\\.)*)'  # matches escaped strings like thread-\"x\"
            m = re.match(
                rf'\+memory_turn\("{_STR}", (\d+), "{_STR}", "{_STR}", (\d+)\)',
                iql,
            )
            if m:
                thread_id = self._unescape(m.group(1))
                turn_id = int(m.group(2))
                role = self._unescape(m.group(3))
                content = self._unescape(m.group(4))
                ts = int(m.group(5))
                self.turns.append((thread_id, turn_id, role, content, ts))
            return ResultSet(columns=[], rows=[])

        # Insert memory_topic
        if iql.startswith("+memory_topic("):
            _STR = r'((?:[^"\\]|\\.)*)'
            m = re.match(rf'\+memory_topic\("{_STR}", (\d+), "{_STR}"\)', iql)
            if m:
                self.topics.append(
                    (self._unescape(m.group(1)), int(m.group(2)), self._unescape(m.group(3)))
                )
            return ResultSet(columns=[], rows=[])

        # Query active_topic
        if iql.startswith("?active_topic("):
            thread_id = self._extract_thread(iql)
            seen = set()
            rows = []
            for t in self.topics:
                if t[0] == thread_id and t[2] not in seen:
                    seen.add(t[2])
                    rows.append([thread_id, t[2]])
            return ResultSet(columns=["thread_id", "topic"], rows=rows)

        # Query memory_turn
        if iql.startswith("?memory_turn("):
            thread_id = self._extract_thread(iql)
            rows = [[t[0], t[1], t[2], t[3], t[4]] for t in self.turns if t[0] == thread_id]
            return ResultSet(
                columns=["thread_id", "turn_id", "role", "content", "ts"],
                rows=rows,
            )

        # Query relevant_turn
        if iql.startswith("?relevant_turn("):
            thread_id = self._extract_thread(iql)
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

        # Query topic_thread - returns raw cross-product pairs (like the real
        # Datalog rule), so the production code's dedup logic is exercised.
        if iql.startswith("?topic_thread("):
            thread_id = self._extract_thread(iql)
            thread_topic_entries = [(t[1], t[2]) for t in self.topics if t[0] == thread_id]
            rows = []
            for _, topic_a in thread_topic_entries:
                for _, topic_b in thread_topic_entries:
                    if topic_a != topic_b:
                        rows.append([thread_id, topic_a, topic_b])
            return ResultSet(columns=["thread_id", "topic_a", "topic_b"], rows=rows)

        return ResultSet(columns=[], rows=[])

    def _extract_thread(self, iql: str) -> str:
        m = re.search(r'"((?:[^"\\]|\\.)*)"', iql)
        return self._unescape(m.group(1)) if m else ""

    @staticmethod
    def _unescape(s: str) -> str:
        r"""Reverse escape_iql using single-pass regex.

        Sequential .replace() can't distinguish \\n (escaped backslash + n)
        from \n (escaped newline). A single-pass regex handles this correctly
        by consuming each \X escape exactly once left-to-right.
        """
        _MAP = {"\\": "\\", '"': '"', "n": "\n", "r": "\r", "t": "\t", "0": "\0"}
        return re.sub(r"\\(.)", lambda m: _MAP.get(m.group(1), "\\" + m.group(1)), s)


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
        # Role and content are base64-encoded in storage
        assert kg.turns[0][2] == _b64e("user")
        assert kg.turns[0][3] == _b64e("Hello world")

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
        # Content is base64-encoded in storage
        assert kg.turns[0][3] == _b64e(content)

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

    async def test_recall_related_topics_deduplicated(self) -> None:
        """Multiple turns with the same topic pair must not produce duplicate pairs."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        # Two turns both mentioning python + ml = same topic pair appears twice in raw data
        await mem.astore("t", "user", "Python ML is great", topics=["python", "ml"])
        await mem.astore("t", "user", "More Python ML work", topics=["python", "ml"])
        ctx = await mem.arecall("t")
        # Dedup: should only have one (ml, python) pair despite two turns
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
        assert kg.turns[0][3] == _b64e("Test message")

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
        """Topics still use escape_iql (they're identifiers, not content)."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("t", "user", "Hello", topics=['say "hi"'])
        topic_calls = [
            c for c in kg.executed
            if c.startswith("+memory_topic(") and ":" not in c.split("(", 1)[1]
        ]
        assert len(topic_calls) == 1
        assert r'say \"hi\"' in topic_calls[0]

    async def test_content_base64_round_trip(self) -> None:
        """Content with special chars must survive base64 encode/decode round-trip."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        thread_id = 'user-"alice"'
        content = 'She said "hello\\world"\nand left'
        await mem.astore(thread_id, "user", content, topics=["test"])

        assert len(kg.turns) == 1
        assert kg.turns[0][0] == thread_id
        # Content is base64-encoded in storage
        assert kg.turns[0][3] == _b64e(content)

        # Round-trip: recall decodes base64 back to original
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
        assert kg.turns[0][3] == _b64e(content)

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
        node = mem.store_node()  # strict=False is default

        result = await node({"new_message": {"role": "user", "content": "hello"}})

        assert result == {}
        assert len(kg.turns) == 1
        assert kg.turns[0][0] == "default"

    async def test_recall_node_missing_thread_id_falls_back_to_default(self) -> None:
        """Non-strict recall falls back to thread_id='default' with a warning."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        await mem.astore("default", "user", "Hello Python")

        node = mem.recall_node()  # strict=False is default
        result = await node({})

        assert "context" in result
        assert len(result["context"]["recent"]) == 1

    async def test_store_node_non_dict_message_is_skipped(self) -> None:
        """Non-dict messages (e.g., LangChain objects) must be skipped, not crash."""
        kg = MockMemoryKG()
        mem = InputLayerMemory(kg=kg)
        node = mem.store_node()
        result = await node({"thread_id": "t", "new_message": "plain string"})
        assert result == {}
        assert len(kg.turns) == 0

    async def test_process_restart_counter_resumes_from_kg(self) -> None:
        """Turn counter must resume from KG state, not reset to 1 after restart."""
        kg = MockMemoryKG()
        mem1 = InputLayerMemory(kg=kg)
        await mem1.astore("t", "user", "first")
        await mem1.astore("t", "user", "second")

        # Simulate process restart: new instance, same KG
        mem2 = InputLayerMemory(kg=kg)
        turn_id = await mem2.astore("t", "user", "third after restart")
        assert turn_id == 3  # must continue from 2, not restart at 1

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
