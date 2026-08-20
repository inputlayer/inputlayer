"""Migration loader - discover and load JSON migration files from a directory."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from inputlayer.migrations.errors import MigrationError


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


_MIGRATION_RE = re.compile(r"^(\d{4})_.+\.json$")


def _load_json_migration(entry: Path, name: str, number: int) -> MigrationInfo:
    """Load a language-neutral JSON migration (the current format)."""
    import json

    from inputlayer.migrations.operations import operation_from_dict
    from inputlayer.migrations.writer import MIGRATION_FORMAT

    try:
        document = json.loads(entry.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise MigrationError(f"{entry.name}: invalid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise MigrationError(f"{entry.name}: migration document must be a JSON object")
    fmt = document.get("format")
    if not isinstance(fmt, int) or fmt > MIGRATION_FORMAT:
        raise MigrationError(
            f"{entry.name}: unsupported migration format {fmt!r} "
            f"(this SDK reads up to {MIGRATION_FORMAT} - upgrade the SDK)"
        )
    state = dict(document.get("state", {}))
    # JSON has no tuples; restore (col, type) pairs so state comparisons in
    # the autodetector see no phantom changes against in-memory model state.
    if "relations" in state:
        state["relations"] = {
            rel: [tuple(col) for col in cols] for rel, cols in state["relations"].items()
        }
    try:
        operations = [operation_from_dict(d) for d in document.get("operations", [])]
    except (KeyError, TypeError, ValueError) as exc:
        raise MigrationError(f"{entry.name}: invalid operation: {exc}") from exc
    return MigrationInfo(
        name=name,
        number=number,
        filename=entry.name,
        dependencies=list(document.get("dependencies", [])),
        operations=operations,
        state=state,
    )


def load_migrations(directory: str | Path) -> list[MigrationInfo]:
    """Discover and load all migration files from a directory.

    Migrations are language-neutral JSON documents; that is the only
    format (pre-1.0, no legacy loading). Non-matching files - including
    stray .py files from older SDKs - are ignored. Returns migrations
    sorted by number.
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

        migrations.append(_load_json_migration(entry, name, number))

    migrations.sort(key=lambda m: m.number)

    # Duplicate names or numbers are always a mistake (a stale legacy .py
    # twin of a regenerated .json, or a merge collision) and previously
    # caused the same migration to be applied twice - destructively for
    # Drop/Replace operations. Refuse loudly.
    by_name: dict[str, str] = {}
    by_number: dict[int, str] = {}
    for m in migrations:
        if m.name in by_name:
            raise MigrationError(
                f"duplicate migration name '{m.name}': {by_name[m.name]} and "
                f"{m.filename} - delete the stale one"
            )
        if m.number in by_number:
            raise MigrationError(
                f"duplicate migration number {m.number:04d}: "
                f"{by_number[m.number]} and {m.filename} - renumber one of them"
            )
        by_name[m.name] = m.filename
        by_number[m.number] = m.filename

    return migrations


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
