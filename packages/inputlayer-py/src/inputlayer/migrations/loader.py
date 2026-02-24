"""Migration loader - discover and import migration files from a directory."""

from __future__ import annotations

import importlib.util
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from inputlayer.migrations import Migration


@dataclass
class MigrationInfo:
    """Metadata about a loaded migration file."""

    name: str           # e.g. "0001_initial"
    number: int         # e.g. 1
    filename: str       # e.g. "0001_initial.py"
    dependencies: list[str]
    operations: list    # list[Operation]
    state: dict[str, Any]

    @property
    def module_name(self) -> str:
        return self.name


_MIGRATION_RE = re.compile(r"^(\d{4})_.+\.py$")


def load_migrations(directory: str | Path) -> list[MigrationInfo]:
    """Discover and load all migration files from a directory.

    Returns migrations sorted by number.
    """
    directory = Path(directory)
    if not directory.is_dir():
        return []

    migrations: list[MigrationInfo] = []
    for entry in sorted(directory.iterdir()):
        if not entry.is_file():
            continue
        match = _MIGRATION_RE.match(entry.name)
        if not match:
            continue

        number = int(match.group(1))
        name = entry.stem  # e.g. "0001_initial"

        # Import the module dynamically
        spec = importlib.util.spec_from_file_location(f"migrations.{name}", entry)
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Extract the M class
        m_cls = getattr(module, "M", None)
        if m_cls is None or not (isinstance(m_cls, type) and issubclass(m_cls, Migration)):
            continue

        migrations.append(MigrationInfo(
            name=name,
            number=number,
            filename=entry.name,
            dependencies=list(getattr(m_cls, "dependencies", [])),
            operations=list(getattr(m_cls, "operations", [])),
            state=dict(getattr(m_cls, "state", {})),
        ))

    return sorted(migrations, key=lambda m: m.number)


def get_latest_state(directory: str | Path) -> dict[str, Any]:
    """Get the state from the most recent migration, or empty if none exist."""
    migrations = load_migrations(directory)
    if not migrations:
        return {"relations": {}, "rules": {}, "indexes": {}}
    return migrations[-1].state


def get_next_number(directory: str | Path) -> int:
    """Get the next migration number."""
    migrations = load_migrations(directory)
    if not migrations:
        return 1
    return migrations[-1].number + 1
