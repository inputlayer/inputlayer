"""Serialization helpers for the InputLayer checkpointer.

Handles packing/unpacking checkpoint and write data for storage as
IQL string facts. All binary data is base64-encoded; structured data
is serialized via LangGraph's SerializerProtocol.
"""

from __future__ import annotations

import base64
import binascii
from typing import Any

from langgraph.checkpoint.serde.base import SerializerProtocol

# ── Shared column index constants ───────────────────────────────────
# Canonical definitions used by checkpointer.py and _checkpointer_mixin.py.
CKPT_TS = -1
CKPT_METADATA = -2
CKPT_BLOB = -3
CKPT_PARENT_ID = -4
CKPT_ID = -5

# Minimum number of columns a graph_write row must have for safe
# negative-index access: task_id(-5), task_path(-4), idx(-3), channel(-2), blob(-1).
_MIN_WRITE_ROW_LEN = 5


def b64_encode(data: bytes) -> str:
    """Encode bytes as base64 string for safe IQL string storage."""
    return base64.b64encode(data).decode("ascii")


def b64_decode(data: str) -> bytes:
    """Decode base64 string back to bytes."""
    try:
        return base64.b64decode(data.encode("ascii"))
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError(
            f"Failed to decode base64 checkpoint data: {data[:40]!r}"
        ) from exc


def pack(serde: SerializerProtocol, obj: Any) -> str:
    """Serialize obj and pack as 'type|base64blob'."""
    type_, blob = serde.dumps_typed(obj)
    return f"{type_}|{b64_encode(blob)}"


def unpack(serde: SerializerProtocol, packed: str) -> Any:
    """Unpack 'type|base64blob' and deserialize."""
    parts = packed.split("|", 1)
    if len(parts) != 2:
        raise ValueError(
            f"Corrupted checkpoint data: expected 'type|base64blob' format, "
            f"got {packed[:200]!r}{'...' if len(packed) > 200 else ''} "
            f"(length={len(packed)})"
        )
    return serde.loads_typed((parts[0], b64_decode(parts[1])))


def parse_writes(
    serde: SerializerProtocol,
    rows: list[Any],
) -> list[tuple[str, str, Any]]:
    """Parse graph_write rows into (task_id, channel, value) triples.

    Rows are sorted by (task_id, idx) to match LangGraph's expected ordering.
    Columns are parsed from the end of each row for resilience to
    bound-column inclusion by the query engine.

    Raises:
        ValueError: If any row has fewer than 5 columns (task_id,
            task_path, idx, channel, blob).
    """
    for i, row in enumerate(rows):
        if len(row) < _MIN_WRITE_ROW_LEN:
            raise ValueError(
                f"graph_write row {i} has {len(row)} columns, "
                f"expected at least {_MIN_WRITE_ROW_LEN}: {row!r}"
            )

    for i, row in enumerate(rows):
        try:
            int(row[-3])
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"graph_write row {i}: idx column (row[-3]) must be an "
                f"integer, got {row[-3]!r}"
            ) from exc

    sorted_rows = sorted(rows, key=lambda r: (str(r[-5]), int(r[-3])))
    result = []
    for row in sorted_rows:
        task_id = str(row[-5])
        channel = str(row[-2])
        value = unpack(serde, str(row[-1]))
        result.append((task_id, channel, value))
    return result
