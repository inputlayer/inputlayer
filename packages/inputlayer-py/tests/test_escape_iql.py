"""Tests for escape_iql string escaping utility."""

from __future__ import annotations

from inputlayer.integrations.langgraph import escape_iql


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
