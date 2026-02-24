"""Tests for inputlayer._ast - AST node construction and properties."""

from inputlayer._ast import (
    AggExpr,
    And,
    Arithmetic,
    BoolExpr,
    Column,
    Comparison,
    Expr,
    FuncCall,
    InExpr,
    Literal,
    MatchExpr,
    NegatedIn,
    Not,
    Or,
    OrderedColumn,
)


class TestLeafNodes:
    def test_column(self):
        c = Column("employee", "name")
        assert c.relation == "employee"
        assert c.name == "name"
        assert c.ref_alias is None
        assert c.qualified == "employee.name"

    def test_column_with_alias(self):
        c = Column("employee", "name", ref_alias="e1")
        assert c.qualified == "e1.name"

    def test_literal_int(self):
        lit = Literal(42)
        assert lit.value == 42

    def test_literal_str(self):
        lit = Literal("hello")
        assert lit.value == "hello"

    def test_literal_none(self):
        lit = Literal(None)
        assert lit.value is None

    def test_literal_bool(self):
        lit = Literal(True)
        assert lit.value is True

    def test_literal_vector(self):
        lit = Literal([1.0, 2.0, 3.0])
        assert lit.value == [1.0, 2.0, 3.0]


class TestArithmetic:
    def test_add(self):
        e = Arithmetic("+", Column("e", "salary"), Literal(1000))
        assert e.op == "+"
        assert isinstance(e.left, Column)
        assert isinstance(e.right, Literal)


class TestFuncCall:
    def test_simple(self):
        fc = FuncCall("upper", (Column("e", "name"),))
        assert fc.name == "upper"
        assert len(fc.args) == 1

    def test_multi_arg(self):
        fc = FuncCall("cosine", (Column("d", "v1"), Column("d", "v2")))
        assert len(fc.args) == 2


class TestAggExpr:
    def test_count(self):
        agg = AggExpr(func="count", column=Column("e", "id"))
        assert agg.func == "count"
        assert agg.column is not None

    def test_top_k(self):
        agg = AggExpr(
            func="top_k",
            params=(5,),
            passthrough=(Column("d", "id"),),
            order_column=Column("d", "score"),
            desc=True,
        )
        assert agg.params == (5,)
        assert len(agg.passthrough) == 1


class TestBoolExpr:
    def test_comparison(self):
        c = Comparison("=", Column("e", "id"), Literal(1))
        assert c.op == "="
        assert isinstance(c, BoolExpr)

    def test_and(self):
        left = Comparison("=", Column("e", "a"), Literal(1))
        right = Comparison(">", Column("e", "b"), Literal(2))
        a = And(left, right)
        assert isinstance(a.left, Comparison)
        assert isinstance(a.right, Comparison)

    def test_or(self):
        left = Comparison("=", Column("e", "a"), Literal(1))
        right = Comparison("=", Column("e", "a"), Literal(2))
        o = Or(left, right)
        assert isinstance(o, BoolExpr)

    def test_not(self):
        inner = Comparison("=", Column("e", "a"), Literal(1))
        n = Not(inner)
        assert isinstance(n.operand, Comparison)

    def test_in_expr(self):
        ie = InExpr(Column("e", "id"), Column("f", "user_id"))
        assert isinstance(ie, BoolExpr)

    def test_match_expr(self):
        me = MatchExpr(
            "banned",
            {"user_id": Column("e", "id")},
            negated=True,
        )
        assert me.negated is True
        assert me.relation == "banned"


class TestOrderedColumn:
    def test_asc(self):
        oc = OrderedColumn(Column("e", "salary"), descending=False)
        assert oc.descending is False

    def test_desc(self):
        oc = OrderedColumn(Column("e", "salary"), descending=True)
        assert oc.descending is True


class TestFrozen:
    def test_column_immutable(self):
        c = Column("e", "name")
        try:
            c.name = "other"  # type: ignore
            assert False, "Should raise"
        except AttributeError:
            pass

    def test_comparison_immutable(self):
        c = Comparison("=", Column("e", "a"), Literal(1))
        try:
            c.op = "!="  # type: ignore
            assert False, "Should raise"
        except AttributeError:
            pass
