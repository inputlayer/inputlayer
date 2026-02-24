"""CLI entry point for the migration system."""

from __future__ import annotations

import argparse
import importlib
import inspect
import os
import sys
from pathlib import Path
from typing import Any

from inputlayer.migrations.autodetector import detect_changes
from inputlayer.migrations.loader import get_latest_state, get_next_number, load_migrations
from inputlayer.migrations.state import ModelState
from inputlayer.migrations.writer import generate_migration


def _discover_models(module_path: str) -> tuple[list, list, list]:
    """Import a module and discover Relation, Derived, and HnswIndex objects.

    Returns (relations, derived, indexes).
    """
    from inputlayer.derived import Derived
    from inputlayer.index import HnswIndex
    from inputlayer.relation import Relation

    mod = importlib.import_module(module_path)

    relations = []
    derived = []
    indexes = []

    for _name, obj in inspect.getmembers(mod):
        if isinstance(obj, HnswIndex):
            indexes.append(obj)
        elif isinstance(obj, type) and issubclass(obj, Derived) and obj is not Derived:
            derived.append(obj)
        elif isinstance(obj, type) and issubclass(obj, Relation) and obj is not Relation and not issubclass(obj, Derived):
            relations.append(obj)

    return relations, derived, indexes


def _cmd_makemigrations(args: argparse.Namespace) -> int:
    """Generate a new migration file by diffing current models against last state."""
    migrations_dir = Path(args.migrations_dir)
    migrations_dir.mkdir(parents=True, exist_ok=True)

    # Discover models
    relations, derived_list, indexes = _discover_models(args.models)

    if not relations and not derived_list and not indexes:
        print(f"No models found in {args.models}")
        return 1

    # Build current state
    new_state = ModelState.from_models(
        relations=relations,
        derived=derived_list,
        indexes=indexes,
    )

    # Load previous state
    old_state_dict = get_latest_state(migrations_dir)
    old_state = ModelState.from_dict(old_state_dict)

    # Detect changes
    operations = detect_changes(old_state, new_state)

    if not operations:
        print("No changes detected.")
        return 0

    # Generate migration file
    number = get_next_number(migrations_dir)
    migrations = load_migrations(migrations_dir)
    deps = [migrations[-1].name] if migrations else []

    filename, content = generate_migration(
        number,
        operations,
        new_state.to_dict(),
        deps,
        name_suffix=args.name if hasattr(args, "name") and args.name else None,
    )

    filepath = migrations_dir / filename
    filepath.write_text(content)

    print(f"Created migration: {filepath}")
    for op in operations:
        print(f"  - {op.describe()}")

    return 0


def _cmd_migrate(args: argparse.Namespace) -> int:
    """Apply pending migrations."""
    from inputlayer.client_sync import InputLayerSync
    from inputlayer.migrations.executor import migrate
    from inputlayer.migrations.recorder import MigrationRecorder

    migrations_dir = Path(args.migrations_dir)
    migrations = load_migrations(migrations_dir)

    if not migrations:
        print("No migrations found.")
        return 0

    client = InputLayerSync(
        args.url,
        username=args.username,
        password=args.password,
        api_key=args.api_key,
    )
    with client:
        kg = client.knowledge_graph(args.kg)
        recorder = MigrationRecorder(kg)
        applied = migrate(kg, migrations, recorder)

    if applied:
        print(f"Applied {len(applied)} migration(s):")
        for name in applied:
            print(f"  [X] {name}")
    else:
        print("No migrations to apply.")

    return 0


def _cmd_revert(args: argparse.Namespace) -> int:
    """Revert migrations back to a target."""
    from inputlayer.client_sync import InputLayerSync
    from inputlayer.migrations.executor import revert_to
    from inputlayer.migrations.recorder import MigrationRecorder

    migrations_dir = Path(args.migrations_dir)
    migrations = load_migrations(migrations_dir)

    client = InputLayerSync(
        args.url,
        username=args.username,
        password=args.password,
        api_key=args.api_key,
    )
    with client:
        kg = client.knowledge_graph(args.kg)
        recorder = MigrationRecorder(kg)
        reverted = revert_to(kg, migrations, recorder, args.target)

    if reverted:
        print(f"Reverted {len(reverted)} migration(s):")
        for name in reverted:
            print(f"  [ ] {name}")
    else:
        print("Nothing to revert.")

    return 0


def _cmd_showmigrations(args: argparse.Namespace) -> int:
    """Show migration status."""
    from inputlayer.client_sync import InputLayerSync
    from inputlayer.migrations.recorder import MigrationRecorder

    migrations_dir = Path(args.migrations_dir)
    migrations = load_migrations(migrations_dir)

    if not migrations:
        print("No migrations found.")
        return 0

    client = InputLayerSync(
        args.url,
        username=args.username,
        password=args.password,
        api_key=args.api_key,
    )
    with client:
        kg = client.knowledge_graph(args.kg)
        recorder = MigrationRecorder(kg)
        recorder.ensure_schema()
        applied = set(recorder.get_applied())

    for m in migrations:
        mark = "X" if m.name in applied else " "
        print(f"  [{mark}] {m.name}")

    return 0


def _add_connection_args(parser: argparse.ArgumentParser) -> None:
    """Add common connection arguments to a subparser."""
    parser.add_argument("--url", required=True, help="WebSocket URL (e.g. ws://localhost:8080/ws)")
    parser.add_argument("--kg", required=True, help="Knowledge graph name")
    parser.add_argument("--username", default=None, help="Username for authentication")
    parser.add_argument("--password", default=None, help="Password for authentication")
    parser.add_argument("--api-key", dest="api_key", default=None, help="API key for authentication")


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="inputlayer-migrate",
        description="InputLayer migration management tool",
    )
    parser.add_argument(
        "--migrations-dir",
        default="migrations",
        help="Directory for migration files (default: ./migrations)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # makemigrations
    make = subparsers.add_parser("makemigrations", help="Generate a new migration")
    make.add_argument("--models", required=True, help="Python module path containing models (e.g. myapp.models)")
    make.add_argument("--name", default=None, help="Custom migration name suffix")
    make.set_defaults(func=_cmd_makemigrations)

    # migrate
    mig = subparsers.add_parser("migrate", help="Apply pending migrations")
    _add_connection_args(mig)
    mig.set_defaults(func=_cmd_migrate)

    # revert
    rev = subparsers.add_parser("revert", help="Revert migrations to a target")
    _add_connection_args(rev)
    rev.add_argument("target", help="Migration name to revert to (e.g. 0001_initial)")
    rev.set_defaults(func=_cmd_revert)

    # showmigrations
    show = subparsers.add_parser("showmigrations", help="Show migration status")
    _add_connection_args(show)
    show.set_defaults(func=_cmd_showmigrations)

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
