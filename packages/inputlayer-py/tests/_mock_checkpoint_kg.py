"""Shared MockKG and helpers for checkpointer tests.

The MockKG simulates basic IQL insert/query/delete semantics for the
two relations the checkpointer uses: graph_checkpoint and graph_write.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from langgraph.checkpoint.base import Checkpoint, empty_checkpoint

from inputlayer.result import ResultSet


class _BoundString(str):
    """Marker for a quoted string literal in a query (bound value)."""


class _Variable(str):
    """Marker for an unquoted IQL variable in a query (free variable)."""


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
        m = re.match(r"\+\w+\((.*)\)$", iql)
        if not m:
            return []
        args_str = m.group(1)
        args: list[Any] = []
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
            elif ch == "," and not in_str:
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
        """Match query against stored checkpoints."""
        body = iql[len("?graph_checkpoint(") :].rstrip(")")
        parts = self._parse_args("+graph_checkpoint(" + body + ")")

        thread_id = str(parts[0]) if isinstance(parts[0], _BoundString) else None
        checkpoint_ns = (
            str(parts[1]) if len(parts) > 1 and isinstance(parts[1], _BoundString) else None
        )
        checkpoint_id = (
            str(parts[2]) if len(parts) > 2 and isinstance(parts[2], _BoundString) else None
        )

        rows = []
        for ckpt in self.checkpoints:
            if thread_id and ckpt[0] != thread_id:
                continue
            if checkpoint_ns is not None and ckpt[1] != checkpoint_ns:
                continue
            if checkpoint_id and ckpt[2] != checkpoint_id:
                continue
            if checkpoint_id:
                rows.append(list(ckpt[3:]))
            elif checkpoint_ns is not None:
                rows.append(list(ckpt[2:]))
            else:
                rows.append(list(ckpt[1:]))

        if checkpoint_id:
            cols = ["parent_id", "blob", "metadata", "ts"]
        elif checkpoint_ns is not None:
            cols = ["checkpoint_id", "parent_id", "blob", "metadata", "ts"]
        else:
            cols = ["checkpoint_ns", "checkpoint_id", "parent_id", "blob", "metadata", "ts"]
        return ResultSet(columns=cols, rows=rows)

    def _query_writes(self, iql: str) -> ResultSet:
        # graph_write(thread_id, checkpoint_ns, checkpoint_id, task_id,
        #             task_path, idx, channel, blob)
        body = iql[len("?graph_write(") :].rstrip(")")
        parts = self._parse_args("+graph_write(" + body + ")")
        thread_id = str(parts[0]) if isinstance(parts[0], _BoundString) else None
        checkpoint_ns = (
            str(parts[1]) if len(parts) > 1 and isinstance(parts[1], _BoundString) else None
        )
        checkpoint_id_bound = len(parts) > 2 and isinstance(parts[2], _BoundString)
        checkpoint_id = str(parts[2]) if checkpoint_id_bound else None

        rows = []
        for w in self.writes:
            if thread_id and w[0] != thread_id:
                continue
            if checkpoint_ns is not None and w[1] != checkpoint_ns:
                continue
            if checkpoint_id and w[2] != checkpoint_id:
                continue
            if checkpoint_id_bound:
                # Return columns after the 3 bound ones: task_id, task_path, idx, channel, blob
                rows.append(list(w[3:]))
            else:
                # Return columns after the 2 bound ones: ckpt_id + task fields
                rows.append(list(w[2:]))

        if checkpoint_id_bound:
            cols = ["task_id", "task_path", "idx", "channel", "blob"]
        else:
            cols = ["checkpoint_id", "task_id", "task_path", "idx", "channel", "blob"]

        return ResultSet(columns=cols, rows=rows)

    def _delete_checkpoints(self, iql: str) -> None:
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
        # graph_write(thread_id, checkpoint_ns, checkpoint_id, task_id,
        #             task_path, idx, channel, blob)
        _STR = r'((?:[^"\\]|\\.)*)'
        body = iql.split("<-", 1)[1]
        thread_match = re.search(rf'ThreadId\s*=\s*"{_STR}"', body)
        ns_match = re.search(rf'Ns\s*=\s*"{_STR}"', body)
        ckpt_match = re.search(rf'CkptId\s*=\s*"{_STR}"', body)
        task_match = re.search(rf'TaskId\s*=\s*"{_STR}"', body)

        thread_id = self._unescape(thread_match.group(1)) if thread_match else None
        ns = self._unescape(ns_match.group(1)) if ns_match else None
        ckpt_id = self._unescape(ckpt_match.group(1)) if ckpt_match else None
        task_id = self._unescape(task_match.group(1)) if task_match else None

        self.writes = [
            w
            for w in self.writes
            if not (
                (thread_id is None or w[0] == thread_id)
                and (ns is None or w[1] == ns)
                and (ckpt_id is None or w[2] == ckpt_id)
                and (task_id is None or w[3] == task_id)
            )
        ]

    @staticmethod
    def _unescape(s: str) -> str:
        r"""Reverse escape_iql via single-pass regex, including \xHH sequences."""
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
