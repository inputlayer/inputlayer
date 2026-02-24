"""Tests for inputlayer.migrations.writer - migration file generation."""

import pytest

from inputlayer.migrations.operations import (
    CreateIndex,
    CreateRelation,
    CreateRule,
    DropRelation,
    DropRule,
    ReplaceRule,
    RunDatalog,
)
from inputlayer.migrations.writer import generate_migration


# ── Filename generation ──────────────────────────────────────────────


class TestFilename:
    def test_first_migration_named_initial(self):
        filename, _ = generate_migration(1, [], {}, [])
        assert filename == "0001_initial.py"

    def test_subsequent_named_auto(self):
        filename, _ = generate_migration(2, [], {}, [])
        assert filename == "0002_auto.py"

    def test_custom_suffix(self):
        filename, _ = generate_migration(3, [], {}, [], name_suffix="add_users")
        assert filename == "0003_add_users.py"

    def test_number_zero_padded(self):
        filename, _ = generate_migration(42, [], {}, [])
        assert filename == "0042_auto.py"


# ── Content structure ────────────────────────────────────────────────


class TestContentStructure:
    def test_imports_present(self):
        _, content = generate_migration(1, [], {}, [])
        assert "from inputlayer.migrations import Migration" in content
        assert "from inputlayer.migrations import operations as ops" in content

    def test_class_definition(self):
        _, content = generate_migration(1, [], {}, [])
        assert "class M(Migration):" in content

    def test_empty_dependencies(self):
        _, content = generate_migration(1, [], {}, [])
        assert "dependencies = []" in content

    def test_dependencies_listed(self):
        _, content = generate_migration(2, [], {}, ["0001_initial"])
        assert '"0001_initial"' in content

    def test_empty_operations(self):
        _, content = generate_migration(1, [], {}, [])
        assert "operations = []" in content

    def test_state_section(self):
        state = {"relations": {}, "rules": {}, "indexes": {}}
        _, content = generate_migration(1, [], state, [])
        assert '"relations"' in content
        assert '"rules"' in content
        assert '"indexes"' in content


# ── Operation rendering ──────────────────────────────────────────────


class TestOperationRendering:
    def test_create_relation(self):
        ops = [CreateRelation("employee", [("id", "int"), ("name", "string")])]
        _, content = generate_migration(1, ops, {}, [])
        assert "ops.CreateRelation" in content
        assert '"employee"' in content
        assert '"id"' in content
        assert '"int"' in content

    def test_create_rule(self):
        ops = [CreateRule("reach", ["+reach(X, Y) <- edge(X, Y)"])]
        _, content = generate_migration(1, ops, {}, [])
        assert "ops.CreateRule" in content
        assert '"reach"' in content

    def test_replace_rule(self):
        ops = [ReplaceRule("r", ["old"], ["new"])]
        _, content = generate_migration(1, ops, {}, [])
        assert "ops.ReplaceRule" in content
        assert "old_clauses" in content
        assert "new_clauses" in content

    def test_create_index(self):
        ops = [CreateIndex("idx", "doc", "emb", metric="l2", m=32)]
        _, content = generate_migration(1, ops, {}, [])
        assert "ops.CreateIndex" in content
        assert '"idx"' in content
        assert '"l2"' in content
        assert "m=32" in content

    def test_run_datalog(self):
        ops = [RunDatalog(forward=["+x(1)"], backward=["-x(1)"])]
        _, content = generate_migration(1, ops, {}, [])
        assert "ops.RunDatalog" in content

    def test_multiple_operations(self):
        ops = [
            CreateRelation("t", [("a", "int")]),
            CreateRule("r", ["+r(X) <- t(X)"]),
        ]
        _, content = generate_migration(1, ops, {}, [])
        assert "ops.CreateRelation" in content
        assert "ops.CreateRule" in content


# ── State rendering ──────────────────────────────────────────────────


class TestStateRendering:
    def test_relations_in_state(self):
        state = {
            "relations": {"employee": [("id", "int"), ("name", "string")]},
            "rules": {},
            "indexes": {},
        }
        _, content = generate_migration(1, [], state, [])
        assert '"employee"' in content
        assert '("id", "int")' in content

    def test_rules_in_state(self):
        state = {
            "relations": {},
            "rules": {"r": ["+r(X) <- t(X)"]},
            "indexes": {},
        }
        _, content = generate_migration(1, [], state, [])
        assert '"r":' in content

    def test_indexes_in_state(self):
        state = {
            "relations": {},
            "rules": {},
            "indexes": {"idx": {"relation": "doc", "column": "emb"}},
        }
        _, content = generate_migration(1, [], state, [])
        assert '"idx"' in content
        assert '"relation"' in content


# ── Valid Python ─────────────────────────────────────────────────────


class TestValidPython:
    def test_generated_file_is_valid_python(self):
        ops = [
            CreateRelation("employee", [("id", "int"), ("name", "string"), ("salary", "float")]),
            CreateRule("senior", ['+senior(Name) <- employee(_, Name, Salary), Salary > 100000']),
        ]
        state = {
            "relations": {"employee": [("id", "int"), ("name", "string"), ("salary", "float")]},
            "rules": {"senior": ['+senior(Name) <- employee(_, Name, Salary), Salary > 100000']},
            "indexes": {},
        }
        _, content = generate_migration(1, ops, state, [])
        # Should be valid Python
        compile(content, "<test>", "exec")

    def test_empty_migration_is_valid_python(self):
        _, content = generate_migration(1, [], {"relations": {}, "rules": {}, "indexes": {}}, [])
        compile(content, "<test>", "exec")
