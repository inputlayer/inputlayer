"""Migration errors and the checked engine call."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from inputlayer.exceptions import QueryError

if TYPE_CHECKING:
    from inputlayer.migrations.recorder import KGExecutor


class MigrationError(Exception):
    """Raised when a migration fails to load, apply, or revert."""


def execute_checked(kg: KGExecutor, iql: str, context: str) -> Any:
    """Run *iql*, raising MigrationError (from the engine error) if it failed."""
    try:
        return kg.execute(iql)
    except QueryError as err:
        raise MigrationError(f"engine error while {context}: {err}") from err
