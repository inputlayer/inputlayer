"""Shared utilities for the LangGraph integration."""

from __future__ import annotations

from typing import Any

__all__ = ["escape_iql", "validate_row_length"]


def escape_iql(s: str) -> str:
    """Escape a string value for safe embedding in an IQL literal.

    Handles backslashes, double-quotes, and control characters that
    would otherwise produce malformed IQL. Always escape backslashes
    first so later replacements don't double-escape.
    """
    return (
        s.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
        .replace("\0", "\\0")
    )


def validate_row_length(
    row: Any, min_len: int, relation: str, context: str,
) -> None:
    """Raise ValueError if a row has fewer columns than expected.

    This guards against negative-index access on unexpectedly short rows
    returned by the KG query engine.
    """
    if len(row) < min_len:
        raise ValueError(
            f"{relation} row has {len(row)} columns, "
            f"expected at least {min_len} ({context}). "
            f"Row: {row!r}"
        )
