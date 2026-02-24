"""Tests for inputlayer.relation - Relation base class, schema introspection."""

import pytest

from inputlayer._proxy import ColumnProxy, RelationRef
from inputlayer.relation import Relation
from inputlayer.types import Timestamp, Vector


class Employee(Relation):
    id: int
    name: str
    department: str
    salary: float
    active: bool


class UserProfile(Relation):
    user_id: int
    bio: str


class CustomName(Relation):
    __relation_name__ = "my_custom_rel"
    x: int
    y: int


class Document(Relation):
    id: int
    title: str
    embedding: Vector[128]
    created_at: Timestamp


class TestResolveName:
    def test_simple(self):
        assert Relation._resolve_name(Employee) == "employee"

    def test_two_words(self):
        assert Relation._resolve_name(UserProfile) == "user_profile"

    def test_custom(self):
        assert Relation._resolve_name(CustomName) == "my_custom_rel"


class TestGetColumns:
    def test_employee(self):
        cols = Relation._get_columns(Employee)
        assert cols == ["id", "name", "department", "salary", "active"]

    def test_document(self):
        cols = Relation._get_columns(Document)
        assert cols == ["id", "title", "embedding", "created_at"]


class TestGetColumnTypes:
    def test_employee(self):
        types = Relation._get_column_types(Employee)
        assert types["id"] is int
        assert types["name"] is str
        assert types["salary"] is float
        assert types["active"] is bool

    def test_vector_type(self):
        types = Relation._get_column_types(Document)
        assert types["embedding"] is Vector[128]
        assert types["created_at"] is Timestamp


class TestFrozen:
    def test_immutable(self):
        e = Employee(id=1, name="Alice", department="eng", salary=100000.0, active=True)
        with pytest.raises(Exception):  # ValidationError from Pydantic
            e.name = "Bob"  # type: ignore


class TestInstantiation:
    def test_create(self):
        e = Employee(id=1, name="Alice", department="eng", salary=100000.0, active=True)
        assert e.id == 1
        assert e.name == "Alice"
        assert e.salary == 100000.0

    def test_equality(self):
        e1 = Employee(id=1, name="Alice", department="eng", salary=100000.0, active=True)
        e2 = Employee(id=1, name="Alice", department="eng", salary=100000.0, active=True)
        assert e1 == e2


class TestRefs:
    def test_refs_count(self):
        refs = Employee.refs(2)
        assert len(refs) == 2
        assert all(isinstance(r, RelationRef) for r in refs)

    def test_refs_alias(self):
        r1, r2 = Employee.refs(2)
        assert r1.alias == "employee_1"
        assert r2.alias == "employee_2"

    def test_refs_proxy(self):
        r1, r2 = Employee.refs(2)
        col = r1.name
        assert isinstance(col, ColumnProxy)
        assert col.ref_alias == "employee_1"
        assert col.name == "name"

    def test_refs_relation_name(self):
        r1, = Employee.refs(1)
        assert r1.relation_name == "employee"
