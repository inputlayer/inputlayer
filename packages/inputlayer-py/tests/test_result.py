"""Tests for inputlayer.result - ResultSet iteration, conversion, scalar."""

import pytest

from inputlayer.result import ResultSet


class TestResultSetBasic:
    def test_len(self):
        rs = ResultSet(columns=["a", "b"], rows=[[1, 2], [3, 4]])
        assert len(rs) == 2

    def test_bool_true(self):
        rs = ResultSet(columns=["a"], rows=[[1]])
        assert bool(rs) is True

    def test_bool_false(self):
        rs = ResultSet(columns=["a"], rows=[])
        assert bool(rs) is False

    def test_getitem(self):
        rs = ResultSet(columns=["x", "y"], rows=[[1, 2], [3, 4]])
        item = rs[0]
        assert item.x == 1
        assert item.y == 2

    def test_getitem_negative(self):
        rs = ResultSet(columns=["x"], rows=[[1], [2], [3]])
        assert rs[-1].x == 3


class TestResultSetIteration:
    def test_iter(self):
        rs = ResultSet(columns=["name", "age"], rows=[["Alice", 30], ["Bob", 25]])
        items = list(rs)
        assert len(items) == 2
        assert items[0].name == "Alice"
        assert items[1].age == 25


class TestResultSetFirst:
    def test_first_exists(self):
        rs = ResultSet(columns=["x"], rows=[[42]])
        assert rs.first().x == 42

    def test_first_empty(self):
        rs = ResultSet(columns=["x"], rows=[])
        assert rs.first() is None


class TestResultSetScalar:
    def test_scalar(self):
        rs = ResultSet(columns=["count"], rows=[[42]])
        assert rs.scalar() == 42

    def test_scalar_empty(self):
        rs = ResultSet(columns=["count"], rows=[])
        with pytest.raises(ValueError):
            rs.scalar()


class TestResultSetConversion:
    def test_to_dicts(self):
        rs = ResultSet(columns=["a", "b"], rows=[[1, 2], [3, 4]])
        dicts = rs.to_dicts()
        assert dicts == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]

    def test_to_tuples(self):
        rs = ResultSet(columns=["a", "b"], rows=[[1, 2], [3, 4]])
        tuples = rs.to_tuples()
        assert tuples == [(1, 2), (3, 4)]

    def test_to_df(self):
        rs = ResultSet(columns=["name", "age"], rows=[["Alice", 30], ["Bob", 25]])
        df = rs.to_df()
        assert list(df.columns) == ["name", "age"]
        assert len(df) == 2
        assert df.iloc[0]["name"] == "Alice"


class TestResultSetMetadata:
    def test_post_init_defaults(self):
        rs = ResultSet(columns=["x"], rows=[[1], [2]])
        assert rs.row_count == 2
        assert rs.total_count == 2

    def test_explicit_counts(self):
        rs = ResultSet(columns=["x"], rows=[[1]], row_count=1, total_count=100, truncated=True)
        assert rs.row_count == 1
        assert rs.total_count == 100
        assert rs.truncated is True

    def test_provenance(self):
        rs = ResultSet(
            columns=["x"],
            rows=[[1]],
            row_provenance=["persistent"],
            has_ephemeral=False,
        )
        assert rs.row_provenance == ["persistent"]
        assert rs.has_ephemeral is False

    def test_warnings(self):
        rs = ResultSet(columns=["x"], rows=[[1]], warnings=["test warning"])
        assert rs.warnings == ["test warning"]
