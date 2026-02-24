"""Tests for inputlayer.migrations.cli - command-line interface."""

import sys
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from inputlayer.migrations.cli import _discover_models, build_parser, main


# ── Parser tests ─────────────────────────────────────────────────────


class TestParser:
    def test_build_parser(self):
        parser = build_parser()
        assert parser.prog == "inputlayer-migrate"

    def test_no_command_returns_1(self):
        assert main([]) == 1

    def test_makemigrations_requires_models(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["makemigrations"])

    def test_migrate_requires_url_and_kg(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["migrate"])

    def test_revert_requires_target(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["revert", "--url", "ws://x", "--kg", "test"])

    def test_showmigrations_parses(self):
        parser = build_parser()
        args = parser.parse_args(["showmigrations", "--url", "ws://x", "--kg", "test"])
        assert args.command == "showmigrations"
        assert args.url == "ws://x"
        assert args.kg == "test"

    def test_migrations_dir_default(self):
        parser = build_parser()
        args = parser.parse_args(["--migrations-dir", "/tmp/m", "makemigrations", "--models", "x"])
        assert args.migrations_dir == "/tmp/m"


# ── _discover_models ─────────────────────────────────────────────────


class TestDiscoverModels:
    def test_discovers_from_module(self, tmp_path):
        # Write a temporary models module
        models_file = tmp_path / "test_models_tmp.py"
        models_file.write_text(textwrap.dedent("""\
            from inputlayer.relation import Relation
            from inputlayer.derived import Derived, From
            from inputlayer.index import HnswIndex
            from typing import ClassVar

            class Employee(Relation):
                id: int
                name: str

            class Edge(Relation):
                src: int
                dst: int

            class Reachable(Derived):
                src: int
                dst: int
                rules: ClassVar[list] = []

            Reachable.rules = [
                From(Edge).select(src=Edge.src, dst=Edge.dst),
            ]

            employee_idx = HnswIndex(
                name="skip",
                relation=Employee,
                column="name",
            )
        """))

        # Add tmp_path to sys.path temporarily
        sys.path.insert(0, str(tmp_path))
        try:
            relations, derived, indexes = _discover_models("test_models_tmp")
            rel_names = {r.__name__ for r in relations}
            der_names = {d.__name__ for d in derived}

            assert "Employee" in rel_names
            assert "Edge" in rel_names
            assert "Reachable" in der_names
            assert len(indexes) == 1
        finally:
            sys.path.pop(0)
            # Clean up imported module
            sys.modules.pop("test_models_tmp", None)


# ── makemigrations (offline, no server) ──────────────────────────────


class TestMakemigrations:
    def test_generates_initial_migration(self, tmp_path):
        # Write models
        models_file = tmp_path / "models_for_cli.py"
        models_file.write_text(textwrap.dedent("""\
            from inputlayer.relation import Relation

            class Employee(Relation):
                id: int
                name: str
        """))

        migrations_dir = tmp_path / "migrations"

        sys.path.insert(0, str(tmp_path))
        try:
            result = main([
                "--migrations-dir", str(migrations_dir),
                "makemigrations",
                "--models", "models_for_cli",
            ])
            assert result == 0
            files = list(migrations_dir.glob("0001_*.py"))
            assert len(files) == 1
            assert "initial" in files[0].name
        finally:
            sys.path.pop(0)
            sys.modules.pop("models_for_cli", None)

    def test_no_changes_detected(self, tmp_path):
        # Write models
        models_file = tmp_path / "models_nochange.py"
        models_file.write_text(textwrap.dedent("""\
            from inputlayer.relation import Relation

            class Employee(Relation):
                id: int
                name: str
        """))

        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()

        # Create an existing migration that matches
        from inputlayer.migrations.writer import generate_migration
        from inputlayer.migrations.operations import CreateRelation

        ops = [CreateRelation("employee", [("id", "int"), ("name", "string")])]
        state = {"relations": {"employee": [("id", "int"), ("name", "string")]}, "rules": {}, "indexes": {}}
        filename, content = generate_migration(1, ops, state, [])
        (migrations_dir / filename).write_text(content)

        sys.path.insert(0, str(tmp_path))
        try:
            result = main([
                "--migrations-dir", str(migrations_dir),
                "makemigrations",
                "--models", "models_nochange",
            ])
            assert result == 0
            # Should not create a second migration
            files = list(migrations_dir.glob("0002_*.py"))
            assert len(files) == 0
        finally:
            sys.path.pop(0)
            sys.modules.pop("models_nochange", None)

    def test_second_migration_depends_on_first(self, tmp_path):
        models_file = tmp_path / "models_v2.py"

        # First version
        models_file.write_text(textwrap.dedent("""\
            from inputlayer.relation import Relation

            class Employee(Relation):
                id: int
                name: str
        """))

        migrations_dir = tmp_path / "migrations"

        sys.path.insert(0, str(tmp_path))
        try:
            # Generate first migration
            main([
                "--migrations-dir", str(migrations_dir),
                "makemigrations",
                "--models", "models_v2",
            ])
            # Clear cached module
            sys.modules.pop("models_v2", None)

            # Update models
            models_file.write_text(textwrap.dedent("""\
                from inputlayer.relation import Relation

                class Employee(Relation):
                    id: int
                    name: str

                class Department(Relation):
                    name: str
                    budget: float
            """))

            # Generate second migration
            result = main([
                "--migrations-dir", str(migrations_dir),
                "makemigrations",
                "--models", "models_v2",
            ])
            assert result == 0

            files = sorted(migrations_dir.glob("*.py"))
            assert len(files) == 2

            # Load and check dependency
            from inputlayer.migrations.loader import load_migrations
            loaded = load_migrations(migrations_dir)
            assert loaded[1].dependencies == [loaded[0].name]
        finally:
            sys.path.pop(0)
            sys.modules.pop("models_v2", None)
