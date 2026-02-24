"""Tests for inputlayer.types - Vector, VectorInt8, Timestamp, type mapping."""

from datetime import datetime, timezone

import pytest

from inputlayer.types import (
    Timestamp,
    Vector,
    VectorInt8,
    python_type_to_datalog,
)


# ── Vector ────────────────────────────────────────────────────────────

class TestVector:
    def test_unparameterized(self):
        v = Vector([1.0, 2.0, 3.0])
        assert isinstance(v, list)
        assert len(v) == 3

    def test_parameterized(self):
        V3 = Vector[3]
        assert repr(V3) == "Vector[3]"

    def test_parameterized_cached(self):
        assert Vector[3] is Vector[3]

    def test_invalid_dim(self):
        with pytest.raises(TypeError):
            Vector[-1]

    def test_invalid_dim_type(self):
        with pytest.raises(TypeError):
            Vector["abc"]

    def test_isinstance_check(self):
        V3 = Vector[3]
        assert isinstance([1.0, 2.0, 3.0], V3)
        assert not isinstance([1.0, 2.0], V3)
        assert not isinstance("abc", V3)


# ── VectorInt8 ────────────────────────────────────────────────────────

class TestVectorInt8:
    def test_unparameterized(self):
        v = VectorInt8([1, 2, 3])
        assert isinstance(v, list)

    def test_parameterized(self):
        V4 = VectorInt8[4]
        assert repr(V4) == "VectorInt8[4]"

    def test_cached(self):
        assert VectorInt8[4] is VectorInt8[4]

    def test_isinstance_check(self):
        V2 = VectorInt8[2]
        assert isinstance([1, 2], V2)
        assert not isinstance([1, 2, 3], V2)

    def test_range_check(self):
        V2 = VectorInt8[2]
        assert isinstance([-128, 127], V2)
        assert not isinstance([-129, 0], V2)
        assert not isinstance([0, 128], V2)


# ── Timestamp ─────────────────────────────────────────────────────────

class TestTimestamp:
    def test_now(self):
        ts = Timestamp.now()
        assert isinstance(ts, int)
        assert ts > 0

    def test_from_datetime(self):
        dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        ts = Timestamp.from_datetime(dt)
        assert ts == 1704067200000

    def test_to_datetime(self):
        ts = Timestamp(1704067200000)
        dt = ts.to_datetime()
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 1

    def test_roundtrip(self):
        dt = datetime(2024, 6, 15, 12, 30, 45, tzinfo=timezone.utc)
        ts = Timestamp.from_datetime(dt)
        dt2 = ts.to_datetime()
        # Millisecond precision
        assert abs((dt2 - dt).total_seconds()) < 0.001


# ── Type mapping ──────────────────────────────────────────────────────

class TestTypeMap:
    def test_int(self):
        assert python_type_to_datalog(int) == "int"

    def test_float(self):
        assert python_type_to_datalog(float) == "float"

    def test_str(self):
        assert python_type_to_datalog(str) == "string"

    def test_bool(self):
        assert python_type_to_datalog(bool) == "bool"

    def test_timestamp(self):
        assert python_type_to_datalog(Timestamp) == "timestamp"

    def test_vector(self):
        assert python_type_to_datalog(Vector) == "vector"

    def test_vector_dim(self):
        assert python_type_to_datalog(Vector[128]) == "vector[128]"

    def test_vector_int8(self):
        assert python_type_to_datalog(VectorInt8) == "vector_int8"

    def test_vector_int8_dim(self):
        assert python_type_to_datalog(VectorInt8[64]) == "vector_int8[64]"

    def test_unsupported(self):
        with pytest.raises(TypeError):
            python_type_to_datalog(dict)
