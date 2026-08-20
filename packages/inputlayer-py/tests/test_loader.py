"""Tests for inputlayer.migrations.loader - JSON migration discovery."""

import json
from pathlib import Path

from inputlayer.migrations.loader import (
    get_latest_state,
    get_next_number,
    load_migrations,
)
from inputlayer.migrations.operations import CreateRelation, CreateRule
from inputlayer.migrations.writer import generate_migration

# ── Helpers ──────────────────────────────────────────────────────────

EMPTY_STATE = {"relations": {}, "rules": {}, "indexes": {}}


def _write_json(tmp_path: Path, filename: str, **overrides) -> Path:
    document = {
        "format": 1,
        "dependencies": [],
        "operations": [],
        "state": EMPTY_STATE,
    }
    document.update(overrides)
    f = tmp_path / filename
    f.write_text(json.dumps(document))
    return f


# ── load_migrations ──────────────────────────────────────────────────


class TestLoadMigrations:
    def test_empty_directory(self, tmp_path):
        assert load_migrations(tmp_path) == []

    def test_nonexistent_directory(self, tmp_path):
        assert load_migrations(tmp_path / "nope") == []

    def test_loads_single_migration(self, tmp_path):
        _write_json(
            tmp_path,
            "0001_initial.json",
            operations=[
                {"type": "CreateRelation", "name": "employee", "columns": [["id", "int"]]}
            ],
        )
        loaded = load_migrations(tmp_path)
        assert len(loaded) == 1
        assert loaded[0].name == "0001_initial"
        assert loaded[0].number == 1
        assert isinstance(loaded[0].operations[0], CreateRelation)

    def test_loads_multiple_sorted(self, tmp_path):
        # Written out of order; loaded sorted by number.
        _write_json(tmp_path, "0002_auto.json")
        _write_json(tmp_path, "0001_initial.json")
        loaded = load_migrations(tmp_path)
        assert [m.number for m in loaded] == [1, 2]

    def test_ignores_non_migration_files(self, tmp_path):
        (tmp_path / "README.md").write_text("not a migration")
        (tmp_path / "helper.json").write_text("{}")
        _write_json(tmp_path, "0001_initial.json")
        assert len(load_migrations(tmp_path)) == 1

    def test_ignores_stray_py_files(self, tmp_path):
        # Pre-1.0: JSON is the only migration format. A .py file from an
        # older SDK is inert - not loaded, not an error.
        (tmp_path / "0001_initial.py").write_text("raise RuntimeError('never imported')")
        _write_json(tmp_path, "0002_auto.json")
        loaded = load_migrations(tmp_path)
        assert [m.name for m in loaded] == ["0002_auto"]

    def test_dependencies_loaded(self, tmp_path):
        _write_json(tmp_path, "0002_auto.json", dependencies=["0001_initial"])
        _write_json(tmp_path, "0001_initial.json")
        loaded = load_migrations(tmp_path)
        assert loaded[1].dependencies == ["0001_initial"]

    def test_state_loaded_with_tuple_columns(self, tmp_path):
        state = {
            "relations": {"employee": [["id", "int"], ["name", "string"]]},
            "rules": {},
            "indexes": {},
        }
        _write_json(tmp_path, "0001_initial.json", state=state)
        loaded = load_migrations(tmp_path)
        assert loaded[0].state["relations"]["employee"] == [("id", "int"), ("name", "string")]

    def test_filename_property(self, tmp_path):
        _write_json(tmp_path, "0001_initial.json")
        assert load_migrations(tmp_path)[0].filename == "0001_initial.json"


# ── get_latest_state ─────────────────────────────────────────────────


class TestGetLatestState:
    def test_empty_directory(self, tmp_path):
        assert get_latest_state(tmp_path) == EMPTY_STATE

    def test_returns_last_migration_state(self, tmp_path):
        _write_json(tmp_path, "0001_initial.json", state={
            "relations": {"a": [["x", "int"]]}, "rules": {}, "indexes": {},
        })
        _write_json(tmp_path, "0002_auto.json", state={
            "relations": {"a": [["x", "int"]], "b": [["y", "string"]]},
            "rules": {}, "indexes": {},
        })
        state = get_latest_state(tmp_path)
        assert set(state["relations"]) == {"a", "b"}


# ── get_next_number ──────────────────────────────────────────────────


class TestGetNextNumber:
    def test_empty_directory(self, tmp_path):
        assert get_next_number(tmp_path) == 1

    def test_after_first(self, tmp_path):
        _write_json(tmp_path, "0001_initial.json")
        assert get_next_number(tmp_path) == 2

    def test_after_multiple(self, tmp_path):
        _write_json(tmp_path, "0001_initial.json")
        _write_json(tmp_path, "0002_auto.json")
        _write_json(tmp_path, "0003_more.json")
        assert get_next_number(tmp_path) == 4


# ── Writer output round-trips through the loader ─────────────────────


class TestWriterLoaderRoundtrip:
    def test_writer_output_loadable(self, tmp_path):
        ops = [
            CreateRelation("employee", [("id", "int"), ("name", "string")]),
            CreateRule("r", ["+r(X) <- employee(X, _)"]),
        ]
        state = {
            "relations": {"employee": [("id", "int"), ("name", "string")]},
            "rules": {"r": ["+r(X) <- employee(X, _)"]},
            "indexes": {},
        }
        filename, content = generate_migration(1, ops, state, [])
        (tmp_path / filename).write_text(content)

        loaded = load_migrations(tmp_path)
        assert len(loaded) == 1
        assert loaded[0].name == "0001_initial"
        assert len(loaded[0].operations) == 2
        assert isinstance(loaded[0].operations[0], CreateRelation)
        assert loaded[0].state["relations"]["employee"] == [("id", "int"), ("name", "string")]

    def test_chained_migrations_loadable(self, tmp_path):
        f1, c1 = generate_migration(1, [], EMPTY_STATE, [])
        (tmp_path / f1).write_text(c1)
        f2, c2 = generate_migration(2, [], EMPTY_STATE, ["0001_initial"])
        (tmp_path / f2).write_text(c2)

        loaded = load_migrations(tmp_path)
        assert [m.name for m in loaded] == ["0001_initial", "0002_auto"]
        assert loaded[1].dependencies == ["0001_initial"]
