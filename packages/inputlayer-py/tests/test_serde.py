"""Direct unit tests for _checkpoint_serde: pack, unpack, parse_writes."""

from __future__ import annotations

import pytest
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from inputlayer.integrations.langgraph._checkpoint_serde import (
    b64_decode,
    b64_encode,
    pack,
    parse_writes,
    unpack,
)


class TestB64EncodeDecode:
    def test_round_trip(self) -> None:
        original = b"hello world"
        assert b64_decode(b64_encode(original)) == original

    def test_empty_bytes(self) -> None:
        assert b64_decode(b64_encode(b"")) == b""

    def test_binary_data(self) -> None:
        data = bytes(range(256))
        assert b64_decode(b64_encode(data)) == data

    def test_decode_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Failed to decode"):
            b64_decode("not-valid-base64!!!")


class TestPackUnpack:
    def test_round_trip_dict(self) -> None:
        serde = JsonPlusSerializer()
        obj = {"key": "value", "nested": [1, 2, 3]}
        packed = pack(serde, obj)
        assert isinstance(packed, str)
        assert "|" in packed
        result = unpack(serde, packed)
        assert result == obj

    def test_round_trip_string(self) -> None:
        serde = JsonPlusSerializer()
        result = unpack(serde, pack(serde, "hello"))
        assert result == "hello"

    def test_round_trip_int(self) -> None:
        serde = JsonPlusSerializer()
        result = unpack(serde, pack(serde, 42))
        assert result == 42

    def test_round_trip_none(self) -> None:
        serde = JsonPlusSerializer()
        result = unpack(serde, pack(serde, None))
        assert result is None

    def test_round_trip_nested_complex(self) -> None:
        serde = JsonPlusSerializer()
        obj = {
            "messages": ["hello", "world"],
            "counter": 42,
            "nested": {"key": "value", "list": [1, 2, 3]},
            "empty": {},
        }
        assert unpack(serde, pack(serde, obj)) == obj

    def test_round_trip_special_chars(self) -> None:
        serde = JsonPlusSerializer()
        obj = {"text": 'quotes "here" and \\ backslashes\nnewlines'}
        assert unpack(serde, pack(serde, obj)) == obj

    def test_unpack_no_separator_raises(self) -> None:
        serde = JsonPlusSerializer()
        with pytest.raises(ValueError, match="Corrupted"):
            unpack(serde, "no-pipe-here")

    def test_unpack_empty_raises(self) -> None:
        serde = JsonPlusSerializer()
        with pytest.raises(ValueError, match="Corrupted"):
            unpack(serde, "")


class TestParseWrites:
    def test_basic_parse(self) -> None:
        serde = JsonPlusSerializer()
        packed_v1 = pack(serde, "val1")
        packed_v2 = pack(serde, "val2")
        rows = [
            ["task-1", "path", 0, "channel_a", packed_v1],
            ["task-1", "path", 1, "channel_b", packed_v2],
        ]
        result = parse_writes(serde, rows)
        assert len(result) == 2
        assert result[0] == ("task-1", "channel_a", "val1")
        assert result[1] == ("task-1", "channel_b", "val2")

    def test_sorted_by_task_then_idx(self) -> None:
        serde = JsonPlusSerializer()
        rows = [
            ["task-2", "p", 0, "ch", pack(serde, "t2")],
            ["task-1", "p", 1, "ch", pack(serde, "t1-1")],
            ["task-1", "p", 0, "ch", pack(serde, "t1-0")],
        ]
        result = parse_writes(serde, rows)
        assert result[0][0] == "task-1"
        assert result[0][2] == "t1-0"
        assert result[1][0] == "task-1"
        assert result[1][2] == "t1-1"
        assert result[2][0] == "task-2"

    def test_empty_rows(self) -> None:
        serde = JsonPlusSerializer()
        assert parse_writes(serde, []) == []

    def test_short_row_raises(self) -> None:
        serde = JsonPlusSerializer()
        with pytest.raises(ValueError, match="row 0 has 3 columns"):
            parse_writes(serde, [["a", "b", "c"]])

    def test_non_numeric_idx_raises(self) -> None:
        serde = JsonPlusSerializer()
        row = ["task", "path", "not-int", "ch", pack(serde, "v")]
        with pytest.raises(ValueError, match="idx column"):
            parse_writes(serde, [row])

    def test_extra_columns_handled(self) -> None:
        """Rows with more than 5 columns should still parse correctly."""
        serde = JsonPlusSerializer()
        packed = pack(serde, "value")
        row = ["extra1", "extra2", "task-1", "path", 0, "channel", packed]
        result = parse_writes(serde, [row])
        assert len(result) == 1
        assert result[0] == ("task-1", "channel", "value")
