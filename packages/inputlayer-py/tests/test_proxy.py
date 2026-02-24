"""Tests for inputlayer._proxy - Column proxy, operator overloading, RelationProxy."""

import pytest

from inputlayer._ast import (
    And,
    Arithmetic,
    Column as AstColumn,
    Comparison,
    InExpr,
    Literal,
    NegatedIn,
    Not,
    Or,
    OrderedColumn,
)
from inputlayer._proxy import ColumnProxy, RelationProxy


class TestColumnProxyComparisons:
    def test_eq(self):
        c = ColumnProxy("employee", "id")
        result = c == 42
        assert isinstance(result, Comparison)
        assert result.op == "="
        assert isinstance(result.left, AstColumn)
        assert isinstance(result.right, Literal)
        assert result.right.value == 42

    def test_ne(self):
        c = ColumnProxy("employee", "dept")
        result = c != "sales"
        assert result.op == "!="

    def test_lt(self):
        c = ColumnProxy("employee", "salary")
        result = c < 50000
        assert result.op == "<"

    def test_le(self):
        c = ColumnProxy("employee", "salary")
        result = c <= 50000
        assert result.op == "<="

    def test_gt(self):
        c = ColumnProxy("employee", "salary")
        result = c > 50000
        assert result.op == ">"

    def test_ge(self):
        c = ColumnProxy("employee", "salary")
        result = c >= 50000
        assert result.op == ">="

    def test_eq_column_to_column(self):
        c1 = ColumnProxy("employee", "dept_id")
        c2 = ColumnProxy("department", "id")
        result = c1 == c2
        assert isinstance(result, Comparison)
        assert isinstance(result.left, AstColumn)
        assert isinstance(result.right, AstColumn)


class TestColumnProxyArithmetic:
    def test_add(self):
        c = ColumnProxy("e", "salary")
        result = c + 1000
        assert isinstance(result, Arithmetic)
        assert result.op == "+"

    def test_radd(self):
        c = ColumnProxy("e", "salary")
        result = 1000 + c
        assert isinstance(result, Arithmetic)
        assert result.op == "+"
        assert isinstance(result.left, Literal)

    def test_sub(self):
        c = ColumnProxy("e", "salary")
        result = c - 500
        assert isinstance(result, Arithmetic)
        assert result.op == "-"

    def test_mul(self):
        c = ColumnProxy("e", "salary")
        result = c * 1.1
        assert isinstance(result, Arithmetic)
        assert result.op == "*"

    def test_truediv(self):
        c = ColumnProxy("e", "salary")
        result = c / 12
        assert isinstance(result, Arithmetic)
        assert result.op == "/"

    def test_mod(self):
        c = ColumnProxy("e", "id")
        result = c % 10
        assert isinstance(result, Arithmetic)
        assert result.op == "%"


class TestColumnProxyOrdering:
    def test_asc(self):
        c = ColumnProxy("e", "salary")
        result = c.asc()
        assert isinstance(result, OrderedColumn)
        assert result.descending is False

    def test_desc(self):
        c = ColumnProxy("e", "salary")
        result = c.desc()
        assert isinstance(result, OrderedColumn)
        assert result.descending is True


class TestColumnProxyMembership:
    def test_in(self):
        c1 = ColumnProxy("e", "id")
        c2 = ColumnProxy("banned", "user_id")
        result = c1.in_(c2)
        assert isinstance(result, InExpr)

    def test_negated_in(self):
        c1 = ColumnProxy("e", "id")
        c2 = ColumnProxy("banned", "user_id")
        result = (~c1).in_(c2)
        assert isinstance(result, NegatedIn)


class TestBoolExprCombinators:
    def test_and(self):
        c = ColumnProxy("e", "salary")
        result = (c > 50000) & (c < 100000)
        assert isinstance(result, And)

    def test_or(self):
        c = ColumnProxy("e", "dept")
        result = (c == "eng") | (c == "sales")
        assert isinstance(result, Or)

    def test_not(self):
        c = ColumnProxy("e", "active")
        result = ~(c == True)  # noqa: E712
        assert isinstance(result, Not)

    def test_complex(self):
        c = ColumnProxy("e", "salary")
        d = ColumnProxy("e", "dept")
        result = (c > 50000) & (d == "eng")
        assert isinstance(result, And)


class TestRelationProxy:
    def test_attr_access(self):
        p = RelationProxy("employee")
        col = p.name
        assert isinstance(col, ColumnProxy)
        assert col.name == "name"
        assert col.relation == "employee"

    def test_private_attr_raises(self):
        p = RelationProxy("employee")
        with pytest.raises(AttributeError):
            _ = p._private

    def test_with_alias(self):
        p = RelationProxy("employee", ref_alias="e1")
        col = p.salary
        assert col.ref_alias == "e1"
        assert col.relation == "employee"

    def test_repr(self):
        p = RelationProxy("employee")
        assert "employee" in repr(p)


class TestColumnProxyRepr:
    def test_repr(self):
        c = ColumnProxy("employee", "name")
        assert "employee" in repr(c)
        assert "name" in repr(c)

    def test_repr_with_alias(self):
        c = ColumnProxy("employee", "name", ref_alias="e1")
        assert "e1" in repr(c)
