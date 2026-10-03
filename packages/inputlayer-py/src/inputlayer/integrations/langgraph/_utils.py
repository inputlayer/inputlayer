"""Shared utilities for the LangGraph integration."""

from __future__ import annotations

import base64
import binascii
import re
from collections.abc import Sequence
from typing import Any

__all__ = [
    "DEFAULT_KG_TIMEOUT",
    "b64d",
    "b64e",
    "escape_iql",
    "validate_row_length",
    "validate_thread_id",
]

DEFAULT_KG_TIMEOUT: float = 30.0


def b64e(s: str) -> str:
    """Encode a string as unpadded base64 for safe IQL string storage.

    The IQL parser currently rejects a ``==`` sequence at the end of a
    quoted string literal inside a multi-arg call. Padding is optional
    for decoding, so we strip it on encode and re-add it on decode.
    """
    return base64.b64encode(s.encode("utf-8")).decode("ascii").rstrip("=")


def b64d(s: str) -> str:
    """Decode an unpadded base64-encoded string back to the original."""
    # Re-pad to a multiple of 4. base64 decoding accepts up to 2 extra
    # `=` characters; any more is invalid input which we let surface.
    missing = (-len(s)) % 4
    padded = s + ("=" * missing)
    try:
        return base64.b64decode(padded.encode("ascii")).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError(f"Failed to decode base64 memory data: {s[:40]!r}") from exc


# Match ASCII control characters not already handled explicitly
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def escape_iql(s: str) -> str:
    """Escape a string value for safe embedding in an IQL literal.

    Handles backslashes, double-quotes, and control characters that
    would otherwise produce malformed IQL. Always escape backslashes
    first so later replacements don't double-escape.
    """
    if not isinstance(s, str):
        raise TypeError(f"escape_iql expects a str, got {type(s).__name__}: {s!r}")
    result = (
        s.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
        .replace("\0", "\\0")
    )
    # Escape remaining ASCII control characters as \xHH
    return _CONTROL_CHARS_RE.sub(
        lambda m: f"\\x{ord(m.group()):02x}",
        result,
    )


def validate_thread_id(thread_id: str, context: str) -> None:
    """Validate that ``thread_id`` is a non-empty string.

    The integration base64-encodes thread_id before embedding it in IQL,
    so arbitrary byte content round-trips safely. We still require a
    non-empty string value so missing/malformed config fails fast with
    a clear message rather than producing empty-string identifiers.
    ``context`` names the caller (for example ``"InputLayerMemory.astore"``).
    """
    if not isinstance(thread_id, str):
        raise TypeError(
            f"{context}: thread_id must be a str, "
            f"got {type(thread_id).__name__}."
        )
    if not thread_id:
        raise ValueError(
            f"{context}: thread_id must be a non-empty string."
        )


def validate_row_length(
    row: Sequence[Any],
    min_len: int,
    relation: str,
    context: str,
) -> None:
    """Raise ValueError if a row has fewer columns than expected.

    This guards against negative-index access on unexpectedly short rows
    returned by the KG query engine.
    """
    if len(row) < min_len:
        row_repr = repr(row)
        if len(row_repr) > 200:
            row_repr = row_repr[:200] + "..."
        raise ValueError(
            f"{relation} row has {len(row)} columns, "
            f"expected at least {min_len} ({context}). "
            f"Row: {row_repr}"
        )
