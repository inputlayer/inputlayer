"""Migration recorder - track applied migrations in the DB."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Protocol


class KGExecutor(Protocol):
    """Minimal interface for executing Datalog commands."""

    def execute(self, datalog: str) -> object: ...


MIGRATION_RELATION = "__inputlayer_migrations__"


class MigrationRecorder:
    """Track which migrations have been applied using an internal relation."""

    def __init__(self, kg: KGExecutor) -> None:
        self._kg = kg

    def ensure_schema(self) -> None:
        """Create the migration tracking relation if it doesn't exist."""
        self._kg.execute(f"+{MIGRATION_RELATION}(name: string, applied_at: string)")

    def get_applied(self) -> list[str]:
        """Return sorted list of applied migration names."""
        result = self._kg.execute(f"?Name, At <- {MIGRATION_RELATION}(Name, At)")
        rows = getattr(result, "rows", []) or []
        return sorted(str(row[0]) for row in rows)

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
