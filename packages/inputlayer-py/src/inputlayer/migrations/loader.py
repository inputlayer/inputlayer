"""Migration loader - discover and import migration files from a directory."""

from __future__ import annotations

import importlib.util
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


_MIGRATION_RE = re.compile(r"^(\d{4})_.+\.(py|json)$")


def _load_json_migration(entry: Path, name: str, number: int) -> MigrationInfo:
    """Load a language-neutral JSON migration (the current format)."""
    import json

    from inputlayer.migrations.operations import operation_from_dict

    document = json.loads(entry.read_text(encoding="utf-8"))
    state = dict(document.get("state", {}))
    # JSON has no tuples; restore (col, type) pairs so state comparisons in
    # the autodetector see no phantom changes against in-memory model state.
    if "relations" in state:
        state["relations"] = {
            rel: [tuple(col) for col in cols] for rel, cols in state["relations"].items()
        }
    return MigrationInfo(
        name=name,
        number=number,
        filename=entry.name,
        dependencies=list(document.get("dependencies", [])),
        operations=[operation_from_dict(d) for d in document.get("operations", [])],
        state=state,
    )


def _load_py_migration(entry: Path, name: str, number: int) -> MigrationInfo | None:
    """Load a legacy Python migration (read-only back-compat)."""
    spec = importlib.util.spec_from_file_location(f"migrations.{name}", entry)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    m_cls = getattr(module, "M", None)
    if m_cls is None or not (isinstance(m_cls, type) and issubclass(m_cls, Migration)):
        return None

    return MigrationInfo(
        name=name,
        number=number,
        filename=entry.name,
        dependencies=list(getattr(m_cls, "dependencies", [])),
        operations=list(getattr(m_cls, "operations", [])),
        state=dict(getattr(m_cls, "state", {})),
    )


def load_migrations(directory: str | Path) -> list[MigrationInfo]:
    """Discover and load all migration files from a directory.

    JSON migrations are the current, language-neutral format; .py files
    are the legacy format and remain loadable. Returns migrations sorted
    by number.
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

        if entry.suffix == ".json":
            migrations.append(_load_json_migration(entry, name, number))
        else:
            info = _load_py_migration(entry, name, number)
            if info is not None:
                migrations.append(info)

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
