"""Tests for inputlayer.migrations.autodetector - diff engine."""

import pytest

from inputlayer.migrations.autodetector import detect_changes
from inputlayer.migrations.operations import (
    CreateIndex,
    CreateRelation,
    CreateRule,
    DropIndex,
    DropRelation,
    DropRule,
    ReplaceRule,
)
from inputlayer.migrations.state import ModelState


# ── Helpers ──────────────────────────────────────────────────────────


def _state(
    relations=None,
    rules=None,
    indexes=None,
) -> ModelState:
    return ModelState(
        relations=relations or {},
        rules=rules or {},
        indexes=indexes or {},
    )


# ── No changes ───────────────────────────────────────────────────────


class TestNoChanges:
    def test_empty_to_empty(self):
        assert detect_changes(_state(), _state()) == []

    def test_identical_states(self):
        s = _state(
            relations={"t": [("a", "int")]},
            rules={"r": ["+r(X) <- t(X)"]},
        )
        assert detect_changes(s, s) == []


# ── Relation changes ─────────────────────────────────────────────────


class TestRelationChanges:
    def test_add_relation(self):
        old = _state()
        new = _state(relations={"employee": [("id", "int"), ("name", "string")]})
        ops = detect_changes(old, new)
        assert len(ops) == 1
        assert isinstance(ops[0], CreateRelation)
        assert ops[0].name == "employee"
        assert ops[0].columns == [("id", "int"), ("name", "string")]

    def test_drop_relation(self):
        old = _state(relations={"employee": [("id", "int")]})
        new = _state()
        ops = detect_changes(old, new)
        assert len(ops) == 1
        assert isinstance(ops[0], DropRelation)
        assert ops[0].name == "employee"
        assert ops[0].columns == [("id", "int")]

    def test_modify_relation_columns(self):
        old = _state(relations={"t": [("a", "int")]})
        new = _state(relations={"t": [("a", "int"), ("b", "string")]})
        ops = detect_changes(old, new)
        # Modified = drop + create
        assert len(ops) == 2
        assert isinstance(ops[0], DropRelation)
        assert isinstance(ops[1], CreateRelation)
        assert ops[0].columns == [("a", "int")]
        assert ops[1].columns == [("a", "int"), ("b", "string")]

    def test_add_multiple_relations(self):
        old = _state()
        new = _state(relations={"a": [("x", "int")], "b": [("y", "string")]})
        ops = detect_changes(old, new)
        assert len(ops) == 2
        # Sorted order
        assert ops[0].name == "a"
        assert ops[1].name == "b"

    def test_rename_column_type(self):
        old = _state(relations={"t": [("a", "int")]})
        new = _state(relations={"t": [("a", "float")]})
        ops = detect_changes(old, new)
        assert len(ops) == 2
        assert isinstance(ops[0], DropRelation)
        assert isinstance(ops[1], CreateRelation)


# ── Rule changes ─────────────────────────────────────────────────────


class TestRuleChanges:
    def test_add_rule(self):
        old = _state()
        new = _state(rules={"r": ["+r(X) <- t(X)"]})
        ops = detect_changes(old, new)
        assert len(ops) == 1
        assert isinstance(ops[0], CreateRule)
        assert ops[0].name == "r"

    def test_drop_rule(self):
        old = _state(rules={"r": ["+r(X) <- t(X)"]})
        new = _state()
        ops = detect_changes(old, new)
        assert len(ops) == 1
        assert isinstance(ops[0], DropRule)
        assert ops[0].clauses == ["+r(X) <- t(X)"]

    def test_modify_rule(self):
        old = _state(rules={"r": ["+r(X) <- t(X)"]})
        new = _state(rules={"r": ["+r(X) <- t(X)", "+r(X) <- s(X)"]})
        ops = detect_changes(old, new)
        assert len(ops) == 1
        assert isinstance(ops[0], ReplaceRule)
        assert ops[0].old_clauses == ["+r(X) <- t(X)"]
        assert ops[0].new_clauses == ["+r(X) <- t(X)", "+r(X) <- s(X)"]

    def test_replace_rule_clauses(self):
        old = _state(rules={"r": ["old_clause"]})
        new = _state(rules={"r": ["new_clause"]})
        ops = detect_changes(old, new)
        assert len(ops) == 1
        assert isinstance(ops[0], ReplaceRule)


# ── Index changes ────────────────────────────────────────────────────


class TestIndexChanges:
    def test_add_index(self):
        old = _state()
        new = _state(indexes={"idx": {
            "relation": "doc", "column": "emb",
            "metric": "cosine", "m": 16,
            "ef_construction": 100, "ef_search": 50,
        }})
        ops = detect_changes(old, new)
        assert len(ops) == 1
        assert isinstance(ops[0], CreateIndex)
        assert ops[0].name == "idx"
        assert ops[0].relation == "doc"

    def test_drop_index(self):
        old = _state(indexes={"idx": {
            "relation": "doc", "column": "emb",
            "metric": "cosine", "m": 16,
            "ef_construction": 100, "ef_search": 50,
        }})
        new = _state()
        ops = detect_changes(old, new)
        assert len(ops) == 1
        assert isinstance(ops[0], DropIndex)
        assert ops[0].name == "idx"

    def test_modify_index(self):
        old = _state(indexes={"idx": {
            "relation": "doc", "column": "emb",
            "metric": "cosine", "m": 16,
            "ef_construction": 100, "ef_search": 50,
        }})
        new = _state(indexes={"idx": {
            "relation": "doc", "column": "emb",
            "metric": "l2", "m": 32,
            "ef_construction": 200, "ef_search": 100,
        }})
        ops = detect_changes(old, new)
        assert len(ops) == 2
        assert isinstance(ops[0], DropIndex)
        assert isinstance(ops[1], CreateIndex)
        assert ops[1].metric == "l2"


# ── Ordering ─────────────────────────────────────────────────────────


class TestOrdering:
    def test_create_relation_before_rule(self):
        """New relations must be created before rules that reference them."""
        old = _state()
        new = _state(
            relations={"t": [("x", "int")]},
            rules={"r": ["+r(X) <- t(X)"]},
        )
        ops = detect_changes(old, new)
        assert isinstance(ops[0], CreateRelation)
        assert isinstance(ops[1], CreateRule)

    def test_drop_rule_before_relation(self):
        """Rules must be dropped before relations they depend on."""
        old = _state(
            relations={"t": [("x", "int")]},
            rules={"r": ["+r(X) <- t(X)"]},
        )
        new = _state()
        ops = detect_changes(old, new)
        # DropRule comes before DropRelation
        drop_rule_idx = next(i for i, o in enumerate(ops) if isinstance(o, DropRule))
        drop_rel_idx = next(i for i, o in enumerate(ops) if isinstance(o, DropRelation))
        assert drop_rule_idx < drop_rel_idx


# ── Combined changes ─────────────────────────────────────────────────


class TestCombinedChanges:
    def test_full_migration(self):
        old = _state(
            relations={"old_table": [("a", "int")]},
            rules={"old_rule": ["clause"]},
        )
        new = _state(
            relations={"new_table": [("b", "string")]},
            rules={"new_rule": ["new_clause"]},
        )
        ops = detect_changes(old, new)
        types = [type(op).__name__ for op in ops]
        # Should have: CreateRelation, DropRule, CreateRule, DropRelation
        assert "CreateRelation" in types
        assert "DropRule" in types
        assert "CreateRule" in types
        assert "DropRelation" in types

    def test_add_relation_modify_rule_add_index(self):
        old = _state(
            relations={"t": [("a", "int")]},
            rules={"r": ["old"]},
        )
        new = _state(
            relations={"t": [("a", "int")], "doc": [("id", "int"), ("emb", "vector[128]")]},
            rules={"r": ["new"]},
            indexes={"idx": {
                "relation": "doc", "column": "emb",
                "metric": "cosine", "m": 16,
                "ef_construction": 100, "ef_search": 50,
            }},
        )
        ops = detect_changes(old, new)
        types = [type(op).__name__ for op in ops]
        assert "CreateRelation" in types  # doc
        assert "ReplaceRule" in types     # r: old→new
        assert "CreateIndex" in types     # idx
