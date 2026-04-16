"""Shared MockMemoryKG for memory tests.

An in-memory KG simulator that handles memory_turn, memory_topic,
and the derived rules (active_topic, relevant_turn, topic_thread).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from inputlayer.integrations.langgraph._utils import b64e  # noqa: F401 (re-exported)
from inputlayer.result import ResultSet


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
            _STR = r'((?:[^"\\]|\\.)*)'
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

        # Query topic_thread
        if iql.startswith("?topic_thread("):
            thread_id = self._extract_thread(iql)
            thread_topic_entries = [(t[1], t[2]) for t in self.topics if t[0] == thread_id]
            rows = []
            for _, topic_a in thread_topic_entries:
                for _, topic_b in thread_topic_entries:
                    if topic_a != topic_b:
                        rows.append([thread_id, topic_a, topic_b])
            return ResultSet(columns=["thread_id", "topic_a", "topic_b"], rows=rows)

        # Conditional delete for memory_turn
        if iql.startswith("-memory_turn(") and "<-" in iql:
            thread_id = self._extract_delete_thread(iql)
            if thread_id:
                self.turns = [t for t in self.turns if t[0] != thread_id]
            return ResultSet(columns=[], rows=[])

        # Conditional delete for memory_topic
        if iql.startswith("-memory_topic(") and "<-" in iql:
            thread_id = self._extract_delete_thread(iql)
            if thread_id:
                self.topics = [t for t in self.topics if t[0] != thread_id]
            return ResultSet(columns=[], rows=[])

        return ResultSet(columns=[], rows=[])

    def _extract_thread(self, iql: str) -> str:
        m = re.search(r'"((?:[^"\\]|\\.)*)"', iql)
        return self._unescape(m.group(1)) if m else ""

    def _extract_delete_thread(self, iql: str) -> str:
        """Extract ThreadId from a conditional delete: -rel(...) <- ThreadId = "..."."""
        body = iql.split("<-", 1)[1] if "<-" in iql else ""
        m = re.search(r'ThreadId\s*=\s*"((?:[^"\\]|\\.)*)"', body)
        return self._unescape(m.group(1)) if m else ""

    @staticmethod
    def _unescape(s: str) -> str:
        r"""Reverse escape_iql using single-pass regex, including \xHH sequences."""
        _MAP = {"\\": "\\", '"': '"', "n": "\n", "r": "\r", "t": "\t", "0": "\0"}

        def _replace(m: re.Match[str]) -> str:
            captured = m.group(1)
            if captured in _MAP:
                return _MAP[captured]
            # Handle \xHH control character escapes (captured = "xHH")
            if len(captured) == 3 and captured[0] == "x":
                return chr(int(captured[1:], 16))
            return "\\" + captured

        return re.sub(r"\\(x[0-9a-fA-F]{2}|.)", _replace, s)
