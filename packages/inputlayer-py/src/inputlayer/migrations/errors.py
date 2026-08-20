"""Migration errors and the engine-result check.

The WebSocket protocol reports engine failures as a result-set with a
single "error" column rather than an exception, so anything that must
know whether a statement actually executed has to check for that shape
explicitly. The migration stack must: silently swallowed errors are how
the recorder shipped broken (write-only applied-state) in the first
place.
"""

from __future__ import annotations

from typing import Any


class MigrationError(Exception):
    """Raised when a migration fails to load, apply, or revert."""


def check_engine_result(result: Any, context: str) -> Any:
    """Raise MigrationError if an execute() result is an engine error frame."""
    columns = list(getattr(result, "columns", None) or [])
    if columns == ["error"]:
        rows = getattr(result, "rows", None) or []
        detail = str(rows[0][0]) if rows and rows[0] else "unknown engine error"
        raise MigrationError(f"engine error while {context}: {detail}")
    return result
