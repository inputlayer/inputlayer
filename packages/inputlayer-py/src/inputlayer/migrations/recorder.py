"""Migration recorder - track applied migrations in the DB."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol

from inputlayer.migrations.errors import execute_checked


class KGExecutor(Protocol):
    """Minimal interface for executing IQL commands."""

    def execute(self, iql: str) -> object: ...


# NOTE: must be a valid relation name (lowercase letter first). The previous
# dunder name was rejected by schema declarations but silently auto-created
# by inserts, leaving recorded state in a relation that queries could not
# read - migration status/revert never worked against a real engine.
MIGRATION_RELATION = "inputlayer_migrations"


class MigrationRecorder:
    """Track which migrations have been applied using an internal relation."""

    def __init__(self, kg: KGExecutor) -> None:
        self._kg = kg

    def _execute(self, iql: str, context: str) -> Any:
        return execute_checked(self._kg, iql, context)

    def ensure_schema(self) -> None:
        """Declare the migration tracking relation (re-declaring is a no-op)."""
        self._execute(
            f"+{MIGRATION_RELATION}(name: string, applied_at: string)",
            "creating the migration tracking relation",
        )

    def get_applied(self) -> list[str]:
        """Return sorted list of applied migration names.

        Plain query form: the engine rejects `?X <- rel(...)` projections
        ("Query cannot contain a rule definition"), which previously made
        this return [] forever and broke status/revert/idempotent apply.
        """
        result = self._execute(
            f"?{MIGRATION_RELATION}(Name, At)", "reading applied migrations"
        )
        rows = getattr(result, "rows", []) or []
        return sorted({str(row[0]) for row in rows})

    def record_applied(self, name: str) -> None:
        """Record that a migration has been applied."""
        now = datetime.now(timezone.utc).isoformat()
        safe = _escape(name)
        self._execute(
            f'+{MIGRATION_RELATION}[("{safe}", "{now}")]',
            f"recording {name} as applied",
        )

    def record_reverted(self, name: str) -> None:
        """Remove the record for a reverted migration."""
        safe = _escape(name)
        self._execute(
            f'-{MIGRATION_RELATION}(Name, At) <- '
            f'{MIGRATION_RELATION}(Name, At), Name = "{safe}"',
            f"recording {name} as reverted",
        )


def _escape(value: str) -> str:
    """Escape a string for interpolation into an IQL string literal."""
    return (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
