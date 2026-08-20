"""Tests for inputlayer.migrations.writer - JSON migration file generation.

Migrations are language-neutral data: typed operations plus a state
snapshot, serialized as JSON. Any SDK language (and the il CLI natively)
can read and apply them.
"""

import json

from inputlayer.migrations.operations import (
    CreateIndex,
    CreateRelation,
    CreateRule,
    ReplaceRule,
    RunIQL,
    operation_from_dict,
)
from inputlayer.migrations.writer import MIGRATION_FORMAT, generate_migration

STATE = {"relations": {}, "rules": {}, "indexes": {}}

# ── Filename generation ──────────────────────────────────────────────


class TestFilename:
    def test_first_migration_named_initial(self):
        filename, _ = generate_migration(1, [], STATE, [])
        assert filename == "0001_initial.json"

    def test_subsequent_named_auto(self):
        filename, _ = generate_migration(2, [], STATE, [])
        assert filename == "0002_auto.json"

    def test_custom_suffix(self):
        filename, _ = generate_migration(3, [], STATE, [], name_suffix="add_users")
        assert filename == "0003_add_users.json"

    def test_number_zero_padded(self):
        filename, _ = generate_migration(42, [], STATE, [])
        assert filename.startswith("0042_")


# ── Document structure ───────────────────────────────────────────────


class TestDocumentStructure:
    def test_valid_json_with_format_marker(self):
        _, content = generate_migration(1, [], STATE, [])
        document = json.loads(content)
        assert document["format"] == MIGRATION_FORMAT

    def test_empty_dependencies(self):
        _, content = generate_migration(1, [], STATE, [])
        assert json.loads(content)["dependencies"] == []

    def test_dependencies_listed(self):
        _, content = generate_migration(2, [], STATE, ["0001_initial"])
        assert json.loads(content)["dependencies"] == ["0001_initial"]

    def test_empty_operations(self):
        _, content = generate_migration(1, [], STATE, [])
        assert json.loads(content)["operations"] == []

    def test_state_embedded(self):
        state = {
            "relations": {"employee": [("id", "int"), ("name", "string")]},
            "rules": {},
            "indexes": {},
        }
        _, content = generate_migration(1, [], state, [])
        document = json.loads(content)
        assert document["state"]["relations"]["employee"] == [["id", "int"], ["name", "string"]]

    def test_no_language_specific_content(self):
        # The whole point: nothing in the file requires a Python runtime.
        ops = [CreateRelation("employee", [("id", "int")])]
        _, content = generate_migration(1, ops, STATE, [])
        assert "import" not in content
        assert "class " not in content


# ── Operation serialization roundtrip ────────────────────────────────


class TestOperationRoundtrip:
    def _roundtrip(self, op):
        _, content = generate_migration(1, [op], STATE, [])
        serialized = json.loads(content)["operations"]
        assert len(serialized) == 1
        return operation_from_dict(serialized[0])

    def test_create_relation(self):
        op = CreateRelation("employee", [("id", "int"), ("name", "string")])
        assert self._roundtrip(op) == op

    def test_create_rule(self):
        op = CreateRule("reachable", ["+reachable(X, Y) <- edge(X, Y)"])
        assert self._roundtrip(op) == op

    def test_replace_rule(self):
        op = ReplaceRule("r", old_clauses=["+r(X) <- a(X)"], new_clauses=["+r(X) <- b(X)"])
        assert self._roundtrip(op) == op

    def test_create_index(self):
        op = CreateIndex(
            name="emb_idx", relation="doc", column="embedding",
            metric="cosine", m=16, ef_construction=200, ef_search=50,
        )
        assert self._roundtrip(op) == op

    def test_run_iql(self):
        op = RunIQL(forward=['+seed[("a",)]'], backward=['-seed("a")'])
        assert self._roundtrip(op) == op

    def test_multiple_operations_preserve_order(self):
        ops = [
            CreateRelation("a", [("x", "int")]),
            CreateRelation("b", [("y", "string")]),
        ]
        _, content = generate_migration(1, ops, STATE, [])
        names = [d["name"] for d in json.loads(content)["operations"]]
        assert names == ["a", "b"]

    def test_roundtrip_commands_survive(self):
        # The deserialized operation must produce identical IQL.
        op = CreateRelation("employee", [("id", "int"), ("name", "string")])
        loaded = self._roundtrip(op)
        assert loaded.forward_commands() == op.forward_commands()
        assert loaded.backward_commands() == op.backward_commands()
