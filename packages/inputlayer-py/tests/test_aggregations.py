"""Tests for inputlayer.aggregations - aggregation functions → AggExpr."""

from inputlayer._ast import AggExpr, Column as AstColumn
from inputlayer._proxy import ColumnProxy
from inputlayer.aggregations import (
    avg,
    count,
    count_distinct,
    max_,
    min_,
    sum_,
    top_k,
    top_k_threshold,
    within_radius,
)
from inputlayer.compiler import _VarEnv, compile_expr


def _col(relation: str, name: str) -> ColumnProxy:
    return ColumnProxy(relation, name)


class TestCount:
    def test_count_column(self):
        result = count(_col("e", "id"))
        assert isinstance(result, AggExpr)
        assert result.func == "count"

    def test_count_none(self):
        result = count()
        assert result.func == "count"
        assert result.column is None

    def test_count_compiles(self):
        env = _VarEnv()
        result = count(_col("e", "id"))
        text = compile_expr(result, env)
        assert text == "count<Id>"


class TestCountDistinct:
    def test_basic(self):
        result = count_distinct(_col("e", "department"))
        assert result.func == "count_distinct"

    def test_compiles(self):
        env = _VarEnv()
        text = compile_expr(count_distinct(_col("e", "department")), env)
        assert text == "count_distinct<Department>"


class TestSum:
    def test_basic(self):
        result = sum_(_col("e", "salary"))
        assert result.func == "sum"

    def test_compiles(self):
        env = _VarEnv()
        text = compile_expr(sum_(_col("e", "salary")), env)
        assert text == "sum<Salary>"


class TestMinMax:
    def test_min(self):
        result = min_(_col("e", "salary"))
        assert result.func == "min"

    def test_max(self):
        result = max_(_col("e", "salary"))
        assert result.func == "max"


class TestAvg:
    def test_basic(self):
        env = _VarEnv()
        text = compile_expr(avg(_col("e", "salary")), env)
        assert text == "avg<Salary>"


class TestTopK:
    def test_basic(self):
        result = top_k(5, _col("d", "id"), order_by=_col("d", "score"))
        assert result.func == "top_k"
        assert result.params == (5,)
        assert len(result.passthrough) == 1

    def test_compiles(self):
        env = _VarEnv()
        result = top_k(5, _col("d", "id"), order_by=_col("d", "score"))
        text = compile_expr(result, env)
        assert text == "top_k<5, Id, Score:desc>"

    def test_asc(self):
        env = _VarEnv()
        result = top_k(3, order_by=_col("d", "score"), desc=False)
        text = compile_expr(result, env)
        assert text == "top_k<3, Score:asc>"


class TestTopKThreshold:
    def test_basic(self):
        result = top_k_threshold(10, 0.5, _col("d", "id"), order_by=_col("d", "dist"))
        assert result.func == "top_k_threshold"
        assert result.params == (10, 0.5)


class TestWithinRadius:
    def test_basic(self):
        result = within_radius(0.5, _col("d", "id"), distance=_col("d", "dist"))
        assert result.func == "within_radius"
        assert result.params == (0.5,)

    def test_compiles(self):
        env = _VarEnv()
        result = within_radius(0.5, _col("d", "id"), distance=_col("d", "dist"))
        text = compile_expr(result, env)
        # within_radius<0.5, Id, Dist:asc>
        assert "within_radius<0.5, Id, Dist:asc>" == text
