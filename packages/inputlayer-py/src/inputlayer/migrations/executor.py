"""Migration executor - apply and revert migrations against a KG."""

from __future__ import annotations

from typing import Any

from inputlayer.migrations.loader import MigrationInfo
from inputlayer.migrations.recorder import KGExecutor, MigrationRecorder


class MigrationError(Exception):
    """Raised when a migration fails to apply or revert."""


def apply_migration(kg: KGExecutor, migration: MigrationInfo) -> None:
    """Apply a single migration's forward operations."""
    for op in migration.operations:
        for cmd in op.forward_commands():
            kg.execute(cmd)


def revert_migration(kg: KGExecutor, migration: MigrationInfo) -> None:
    """Revert a single migration's operations in reverse order."""
    for op in reversed(migration.operations):
        for cmd in op.backward_commands():
            kg.execute(cmd)


def migrate(
    kg: KGExecutor,
    migrations: list[MigrationInfo],
    recorder: MigrationRecorder,
    *,
    target: str | None = None,
) -> list[str]:
    """Apply unapplied migrations up to target (or all if target is None).

    Returns list of applied migration names.
    """
    recorder.ensure_schema()
    applied = set(recorder.get_applied())
    applied_names: list[str] = []

    for m in migrations:
        if m.name in applied:
            continue
        if target is not None and m.name == target:
            break

        apply_migration(kg, m)
        recorder.record_applied(m.name)
        applied_names.append(m.name)

        if target is not None and m.name == target:
            break

    return applied_names


def revert_to(
    kg: KGExecutor,
    migrations: list[MigrationInfo],
    recorder: MigrationRecorder,
    target: str,
) -> list[str]:
    """Revert migrations back to (but not including) target.

    Returns list of reverted migration names.
    """
    recorder.ensure_schema()
    applied = set(recorder.get_applied())

    # Find target index
    target_idx = None
    for i, m in enumerate(migrations):
        if m.name == target:
            target_idx = i
            break

    if target_idx is None:
        raise MigrationError(f"Migration {target!r} not found")

    # Revert in reverse order: everything after target that's applied
    to_revert = [
        m for m in reversed(migrations[target_idx + 1:])
        if m.name in applied
    ]

    reverted_names: list[str] = []
    for m in to_revert:
        revert_migration(kg, m)
        recorder.record_reverted(m.name)
        reverted_names.append(m.name)

    return reverted_names
