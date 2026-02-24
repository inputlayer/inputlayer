"""Tests for inputlayer.migrations.loader - migration discovery and import."""

import os
import textwrap
from pathlib import Path

import pytest

from inputlayer.migrations.loader import (
    get_latest_state,
    get_next_number,
    load_migrations,
)
from inputlayer.migrations.operations import CreateRelation, CreateRule


# ── Helpers ──────────────────────────────────────────────────────────


def _write_migration(tmp_path: Path, filename: str, content: str) -> Path:
    f = tmp_path / filename
    f.write_text(textwrap.dedent(content))
    return f


# ── load_migrations ──────────────────────────────────────────────────


class TestLoadMigrations:
    def test_empty_directory(self, tmp_path):
        assert load_migrations(tmp_path) == []

    def test_nonexistent_directory(self, tmp_path):
        assert load_migrations(tmp_path / "nope") == []

    def test_loads_single_migration(self, tmp_path):
        _write_migration(tmp_path, "0001_initial.py", """\
            from inputlayer.migrations import Migration
            from inputlayer.migrations import operations as ops

            class M(Migration):
                dependencies = []
                operations = [
                    ops.CreateRelation(name="t", columns=[("a", "int")]),
                ]
                state = {"relations": {"t": [["a", "int"]]}, "rules": {}, "indexes": {}}
        """)
        result = load_migrations(tmp_path)
        assert len(result) == 1
        assert result[0].name == "0001_initial"
        assert result[0].number == 1
        assert len(result[0].operations) == 1
        assert isinstance(result[0].operations[0], CreateRelation)

    def test_loads_multiple_sorted(self, tmp_path):
        _write_migration(tmp_path, "0002_auto.py", """\
            from inputlayer.migrations import Migration
            from inputlayer.migrations import operations as ops

            class M(Migration):
                dependencies = ["0001_initial"]
                operations = []
                state = {"relations": {}, "rules": {}, "indexes": {}}
        """)
        _write_migration(tmp_path, "0001_initial.py", """\
            from inputlayer.migrations import Migration
            from inputlayer.migrations import operations as ops

            class M(Migration):
                dependencies = []
                operations = []
                state = {"relations": {}, "rules": {}, "indexes": {}}
        """)
        result = load_migrations(tmp_path)
        assert len(result) == 2
        assert result[0].number == 1
        assert result[1].number == 2

    def test_ignores_non_migration_files(self, tmp_path):
        _write_migration(tmp_path, "__init__.py", "")
        _write_migration(tmp_path, "README.md", "# hi")
        _write_migration(tmp_path, "helper.py", "x = 1")
        assert load_migrations(tmp_path) == []

    def test_ignores_files_without_M_class(self, tmp_path):
        _write_migration(tmp_path, "0001_bad.py", """\
            # No M class here
            x = 42
        """)
        assert load_migrations(tmp_path) == []

    def test_dependencies_loaded(self, tmp_path):
        _write_migration(tmp_path, "0002_auto.py", """\
            from inputlayer.migrations import Migration
            from inputlayer.migrations import operations as ops

            class M(Migration):
                dependencies = ["0001_initial"]
                operations = []
                state = {}
        """)
        result = load_migrations(tmp_path)
        assert result[0].dependencies == ["0001_initial"]

    def test_state_loaded(self, tmp_path):
        _write_migration(tmp_path, "0001_initial.py", """\
            from inputlayer.migrations import Migration
            from inputlayer.migrations import operations as ops

            class M(Migration):
                dependencies = []
                operations = []
                state = {"relations": {"t": [["a", "int"]]}, "rules": {}, "indexes": {}}
        """)
        result = load_migrations(tmp_path)
        assert "t" in result[0].state["relations"]

    def test_filename_property(self, tmp_path):
        _write_migration(tmp_path, "0001_initial.py", """\
            from inputlayer.migrations import Migration

            class M(Migration):
                dependencies = []
                operations = []
                state = {}
        """)
        result = load_migrations(tmp_path)
        assert result[0].filename == "0001_initial.py"


# ── get_latest_state ─────────────────────────────────────────────────


class TestGetLatestState:
    def test_empty_directory(self, tmp_path):
        state = get_latest_state(tmp_path)
        assert state == {"relations": {}, "rules": {}, "indexes": {}}

    def test_returns_last_migration_state(self, tmp_path):
        _write_migration(tmp_path, "0001_initial.py", """\
            from inputlayer.migrations import Migration

            class M(Migration):
                dependencies = []
                operations = []
                state = {"relations": {"a": []}, "rules": {}, "indexes": {}}
        """)
        _write_migration(tmp_path, "0002_auto.py", """\
            from inputlayer.migrations import Migration

            class M(Migration):
                dependencies = ["0001_initial"]
                operations = []
                state = {"relations": {"a": [], "b": []}, "rules": {}, "indexes": {}}
        """)
        state = get_latest_state(tmp_path)
        assert "a" in state["relations"]
        assert "b" in state["relations"]


# ── get_next_number ──────────────────────────────────────────────────


class TestGetNextNumber:
    def test_empty_directory(self, tmp_path):
        assert get_next_number(tmp_path) == 1

    def test_after_first(self, tmp_path):
        _write_migration(tmp_path, "0001_initial.py", """\
            from inputlayer.migrations import Migration

            class M(Migration):
                dependencies = []
                operations = []
                state = {}
        """)
        assert get_next_number(tmp_path) == 2

    def test_after_multiple(self, tmp_path):
        for i in range(1, 6):
            _write_migration(tmp_path, f"{i:04d}_auto.py", f"""\
                from inputlayer.migrations import Migration

                class M(Migration):
                    dependencies = []
                    operations = []
                    state = {{}}
            """)
        assert get_next_number(tmp_path) == 6


# ── Roundtrip: writer → loader ──────────────────────────────────────


class TestRoundtrip:
    def test_writer_output_loadable(self, tmp_path):
        from inputlayer.migrations.writer import generate_migration

        ops = [CreateRelation("employee", [("id", "int"), ("name", "string")])]
        state = {
            "relations": {"employee": [("id", "int"), ("name", "string")]},
            "rules": {},
            "indexes": {},
        }
        filename, content = generate_migration(1, ops, state, [])
        (tmp_path / filename).write_text(content)

        loaded = load_migrations(tmp_path)
        assert len(loaded) == 1
        assert loaded[0].name == "0001_initial"
        assert len(loaded[0].operations) == 1
        assert isinstance(loaded[0].operations[0], CreateRelation)
        assert loaded[0].state["relations"]["employee"] == [("id", "int"), ("name", "string")]

    def test_chained_migrations_loadable(self, tmp_path):
        from inputlayer.migrations.writer import generate_migration

        # First migration
        ops1 = [CreateRelation("t", [("a", "int")])]
        state1 = {"relations": {"t": [("a", "int")]}, "rules": {}, "indexes": {}}
        f1, c1 = generate_migration(1, ops1, state1, [])
        (tmp_path / f1).write_text(c1)

        # Second migration
        ops2 = [CreateRule("r", ["+r(X) <- t(X)"])]
        state2 = {"relations": {"t": [("a", "int")]}, "rules": {"r": ["+r(X) <- t(X)"]}, "indexes": {}}
        f2, c2 = generate_migration(2, ops2, state2, ["0001_initial"])
        (tmp_path / f2).write_text(c2)

        loaded = load_migrations(tmp_path)
        assert len(loaded) == 2
        assert loaded[1].dependencies == ["0001_initial"]
