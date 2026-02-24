"""Tests for inputlayer.migrations.state - ModelState introspection and serialization."""

from typing import ClassVar

import pytest

from inputlayer.derived import Derived, From
from inputlayer.index import HnswIndex
from inputlayer.migrations.state import ModelState
from inputlayer.relation import Relation
from inputlayer.types import Timestamp, Vector


# ── Test models ──────────────────────────────────────────────────────


class Employee(Relation):
    id: int
    name: str
    salary: float


class Department(Relation):
    name: str
    budget: float


class Edge(Relation):
    src: int
    dst: int


class Reachable(Derived):
    src: int
    dst: int

    rules: ClassVar[list] = []  # Set after class definition


# Set rules after class is defined (self-reference)
Reachable.rules = [
    From(Edge).select(src=Edge.src, dst=Edge.dst),
    From(Reachable, Edge)
        .where(lambda r, e: r.dst == e.src)
        .select(src=Reachable.src, dst=Edge.dst),
]


class Document(Relation):
    id: int
    title: str
    embedding: Vector[128]


class Event(Relation):
    id: int
    ts: Timestamp
    label: str


# ── from_models ──────────────────────────────────────────────────────


class TestFromModels:
    def test_single_relation(self):
        state = ModelState.from_models(relations=[Employee])
        assert "employee" in state.relations
        cols = state.relations["employee"]
        assert cols == [("id", "int"), ("name", "string"), ("salary", "float")]

    def test_multiple_relations(self):
        state = ModelState.from_models(relations=[Employee, Department])
        assert "employee" in state.relations
        assert "department" in state.relations

    def test_derived_adds_relation_and_rules(self):
        state = ModelState.from_models(derived=[Reachable])
        assert "reachable" in state.relations
        assert "reachable" in state.rules
        assert len(state.rules["reachable"]) == 2

    def test_derived_rule_is_compiled_datalog(self):
        state = ModelState.from_models(derived=[Reachable])
        clauses = state.rules["reachable"]
        # First clause: base case from Edge
        assert "edge" in clauses[0]
        assert "reachable" in clauses[0]
        # Second clause: recursive
        assert "reachable" in clauses[1]
        assert "edge" in clauses[1]

    def test_indexes(self):
        idx = HnswIndex(name="doc_idx", relation=Document, column="embedding")
        state = ModelState.from_models(relations=[Document], indexes=[idx])
        assert "doc_idx" in state.indexes
        assert state.indexes["doc_idx"]["relation"] == "document"
        assert state.indexes["doc_idx"]["column"] == "embedding"
        assert state.indexes["doc_idx"]["metric"] == "cosine"

    def test_index_custom_params(self):
        idx = HnswIndex(
            name="idx", relation=Document, column="embedding",
            metric="l2", m=32, ef_construction=200, ef_search=100,
        )
        state = ModelState.from_models(indexes=[idx])
        info = state.indexes["idx"]
        assert info["metric"] == "l2"
        assert info["m"] == 32
        assert info["ef_construction"] == 200
        assert info["ef_search"] == 100

    def test_empty_state(self):
        state = ModelState.from_models()
        assert state.is_empty()

    def test_vector_column_type(self):
        state = ModelState.from_models(relations=[Document])
        cols = state.relations["document"]
        types = {c[0]: c[1] for c in cols}
        assert types["embedding"] == "vector[128]"

    def test_timestamp_column_type(self):
        state = ModelState.from_models(relations=[Event])
        cols = state.relations["event"]
        types = {c[0]: c[1] for c in cols}
        assert types["ts"] == "timestamp"

    def test_combined_relations_and_derived(self):
        state = ModelState.from_models(relations=[Edge], derived=[Reachable])
        assert "edge" in state.relations
        assert "reachable" in state.relations
        assert "reachable" in state.rules
        assert "edge" not in state.rules


# ── Serialization roundtrip ──────────────────────────────────────────


class TestSerialization:
    def test_empty_roundtrip(self):
        state = ModelState()
        d = state.to_dict()
        restored = ModelState.from_dict(d)
        assert restored.is_empty()

    def test_full_roundtrip(self):
        idx = HnswIndex(name="doc_idx", relation=Document, column="embedding")
        state = ModelState.from_models(
            relations=[Employee, Document],
            derived=[Reachable],
            indexes=[idx],
        )
        d = state.to_dict()
        restored = ModelState.from_dict(d)
        assert restored.relations == state.relations
        assert restored.rules == state.rules
        assert restored.indexes == state.indexes

    def test_dict_format(self):
        state = ModelState.from_models(relations=[Employee])
        d = state.to_dict()
        assert "relations" in d
        assert "rules" in d
        assert "indexes" in d
        # Columns are serialized as lists (not tuples) for JSON compat
        assert isinstance(d["relations"]["employee"][0], list)

    def test_from_dict_converts_to_tuples(self):
        d = {
            "relations": {"t": [["a", "int"], ["b", "string"]]},
            "rules": {},
            "indexes": {},
        }
        state = ModelState.from_dict(d)
        assert state.relations["t"] == [("a", "int"), ("b", "string")]

    def test_from_dict_missing_keys(self):
        state = ModelState.from_dict({})
        assert state.is_empty()


# ── is_empty ─────────────────────────────────────────────────────────


class TestIsEmpty:
    def test_empty(self):
        assert ModelState().is_empty()

    def test_with_relation(self):
        s = ModelState(relations={"t": [("a", "int")]})
        assert not s.is_empty()

    def test_with_rules(self):
        s = ModelState(rules={"r": ["clause"]})
        assert not s.is_empty()

    def test_with_indexes(self):
        s = ModelState(indexes={"i": {"relation": "t"}})
        assert not s.is_empty()
