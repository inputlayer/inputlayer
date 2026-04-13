"""Shared utilities for the LangGraph integration."""

from __future__ import annotations

__all__ = ["escape_iql"]


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
