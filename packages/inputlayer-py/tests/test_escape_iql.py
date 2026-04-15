"""Tests for escape_iql and validate_row_length utilities."""

from __future__ import annotations

import pytest

from inputlayer.integrations.langgraph import escape_iql
from inputlayer.integrations.langgraph._utils import validate_row_length


class TestEscapeIql:
    def test_backslash_escaped_first(self) -> None:
        assert escape_iql('\\') == '\\\\'

    def test_double_quote_escaped(self) -> None:
        assert escape_iql('"hello"') == '\\"hello\\"'

    def test_backslash_then_quote(self) -> None:
        assert escape_iql('\\"') == '\\\\\\"'

    def test_newline_escaped(self) -> None:
        assert escape_iql("line1\nline2") == "line1\\nline2"

    def test_carriage_return_escaped(self) -> None:
        assert escape_iql("a\rb") == "a\\rb"

    def test_tab_escaped(self) -> None:
        assert escape_iql("a\tb") == "a\\tb"

    def test_nul_byte_escaped(self) -> None:
        assert escape_iql("a\x00b") == "a\\0b"

    def test_plain_string_unchanged(self) -> None:
        assert escape_iql("hello world 123") == "hello world 123"

    def test_unicode_passthrough(self) -> None:
        assert escape_iql("cafe\u0301") == "cafe\u0301"
        assert escape_iql("\U0001f600") == "\U0001f600"

    def test_empty_string(self) -> None:
        assert escape_iql("") == ""

    def test_all_control_chars_in_one_string(self) -> None:
        result = escape_iql('\\"test\n\r\t\x00')
        assert result == '\\\\\\"test\\n\\r\\t\\0'

    def test_bell_char_escaped(self) -> None:
        assert escape_iql("a\x07b") == "a\\x07b"

    def test_backspace_escaped(self) -> None:
        assert escape_iql("a\x08b") == "a\\x08b"

    def test_vertical_tab_escaped(self) -> None:
        assert escape_iql("a\x0bb") == "a\\x0bb"

    def test_form_feed_escaped(self) -> None:
        assert escape_iql("a\x0cb") == "a\\x0cb"

    def test_escape_char_escaped(self) -> None:
        assert escape_iql("a\x1bb") == "a\\x1bb"

    def test_single_quotes_passthrough(self) -> None:
        """Single quotes are safe inside double-quoted IQL strings."""
        assert escape_iql("it's fine") == "it's fine"

    def test_parentheses_passthrough(self) -> None:
        assert escape_iql("fn()") == "fn()"

    def test_non_string_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="escape_iql expects a str"):
            escape_iql(42)  # type: ignore[arg-type]

    def test_long_string_with_mixed_special_chars(self) -> None:
        original = 'He said "hello\\world"\nand\tthen\x00left\x07quickly'
        escaped = escape_iql(original)
        assert '\\"' in escaped
        assert '\\\\' in escaped
        assert '\\n' in escaped
        assert '\\t' in escaped
        assert '\\0' in escaped
        assert '\\x07' in escaped
        # Verify no raw control chars survive
        for ch in escaped:
            assert ord(ch) >= 0x20, f"Unexpected control char: {ch!r}"

    def test_consecutive_backslashes(self) -> None:
        assert escape_iql('\\\\') == '\\\\\\\\'

    def test_quote_at_boundaries(self) -> None:
        assert escape_iql('"') == '\\"'
        assert escape_iql('""') == '\\"\\"'


class TestValidateRowLength:
    def test_valid_row_passes(self) -> None:
        validate_row_length(["a", "b", "c"], 3, "test_rel", "test_ctx")

    def test_row_longer_than_min_passes(self) -> None:
        validate_row_length(["a", "b", "c", "d"], 3, "test_rel", "test_ctx")

    def test_short_row_raises(self) -> None:
        with pytest.raises(ValueError, match="test_rel row has 2 columns"):
            validate_row_length(["a", "b"], 3, "test_rel", "test_ctx")

    def test_error_includes_context(self) -> None:
        with pytest.raises(ValueError, match="my_context"):
            validate_row_length([], 1, "my_rel", "my_context")

    def test_empty_row_raises(self) -> None:
        with pytest.raises(ValueError, match="0 columns"):
            validate_row_length([], 1, "rel", "ctx")

    def test_min_zero_always_passes(self) -> None:
        validate_row_length([], 0, "rel", "ctx")

    def test_long_row_repr_truncated(self) -> None:
        long_row = ["x" * 100] * 5
        with pytest.raises(ValueError, match=r"\.\.\."):
            validate_row_length(long_row, 10, "rel", "ctx")
