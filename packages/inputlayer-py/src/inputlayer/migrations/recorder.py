"""Migration recorder - track applied migrations in the DB."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol


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

    def ensure_schema(self) -> None:
        """Create the migration tracking relation if it doesn't exist."""
        self._kg.execute(f"+{MIGRATION_RELATION}(name: string, applied_at: string)")

    def get_applied(self) -> list[str]:
        """Return sorted list of applied migration names.

        Plain query form: the engine rejects `?X <- rel(...)` projections
        ("Query cannot contain a rule definition"), which previously made
        this return [] forever and broke status/revert/idempotent apply.
        """
        result = self._kg.execute(f"?{MIGRATION_RELATION}(Name, At)")
        rows = getattr(result, "rows", []) or []
        return sorted({str(row[0]) for row in rows})

    def record_applied(self, name: str) -> None:
        """Record that a migration has been applied."""
        now = datetime.now(timezone.utc).isoformat()
        self._kg.execute(f'+{MIGRATION_RELATION}("{name}", "{now}")')

    def record_reverted(self, name: str) -> None:
        """Remove the record for a reverted migration."""
        self._kg.execute(
            f'-{MIGRATION_RELATION}(Name, At) <- '
            f'{MIGRATION_RELATION}(Name, At), Name = "{name}"'
        )
