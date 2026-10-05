"""The meta command table and reply parsers, against the shared fixture (R-META).

``packages/conformance/meta-commands.json`` is the same fixture the JS SDK
renders its table against, so a command spelling cannot be wrong in one SDK
only. ``tests/compile_rules_live.py`` sends every command to an engine.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from inputlayer import _meta
from inputlayer._protocol import ResultResponse
from inputlayer.session import Session

FIXTURE = json.loads(
    (Path(__file__).parents[2] / "conformance" / "meta-commands.json").read_text("utf-8")
)


def test_renders_every_fixture_command_exactly() -> None:
    for command in FIXTURE["commands"]:
        build = _meta.COMMANDS[command["name"]]
        assert build(*command["args"]) == command["text"], command["name"]


def test_has_no_command_the_fixture_does_not_pin() -> None:
    pinned = {c["name"] for c in FIXTURE["commands"]}
    assert set(_meta.COMMANDS) == pinned


def test_never_renders_a_spelling_the_engine_rejects() -> None:
    rendered = {c["text"] for c in FIXTURE["commands"]}
    for rejected in FIXTURE["rejected"]:
        assert rejected["text"] not in rendered


def test_reads_session_rules_from_a_session_reply() -> None:
    for reply in FIXTURE["replies"]["session_list"]:
        assert _meta.session_rules(reply["rows"]) == reply["rules"]


def test_reads_rules_from_a_rule_list_reply() -> None:
    for reply in FIXTURE["replies"]["rule_list"]:
        expected = [(r["name"], r["clause_count"]) for r in reply["rules"]]
        assert _meta.rule_infos(reply["rows"]) == expected


def test_reads_clauses_from_a_rule_def_reply() -> None:
    for reply in FIXTURE["replies"]["rule_def"]:
        assert _meta.rule_clauses(reply["rows"]) == reply["clauses"]


def _reply(rows: list[list[str]]) -> ResultResponse:
    return ResultResponse(
        columns=["message"],
        rows=rows,
        row_count=len(rows),
        total_count=len(rows),
        truncated=False,
        execution_time_ms=0,
    )


class TestSessionCommands:
    @pytest.fixture
    def conn(self) -> AsyncMock:
        conn = AsyncMock()
        listing = next(r for r in FIXTURE["replies"]["session_list"] if r["rules"])
        # hop has two clauses; hop2 one, after them.
        rows = [*listing["rows"], ["  3. hop2(X) <- edge(X, _)"]]
        conn.execute.return_value = _reply(rows)
        return conn

    async def test_list_rules_sends_session_and_parses_the_clauses(self, conn: AsyncMock) -> None:
        rules = await Session(conn).list_rules()
        conn.execute.assert_awaited_once_with(".session", timeout=None)
        assert rules == [
            "hop(X, Y) <- edge(X, Y)",
            "hop(X, Y) <- edge(Y, X)",
            "hop2(X) <- edge(X, _)",
        ]

    async def test_drop_rule_by_name(self, conn: AsyncMock) -> None:
        await Session(conn).drop_rule("hop")
        conn.execute.assert_awaited_once_with(".session drop hop")

    async def test_drop_clause_resolves_its_position_among_all_session_rules(
        self, conn: AsyncMock
    ) -> None:
        await Session(conn).drop_rule("hop2", index=1)
        assert conn.execute.await_args_list[-1].args == (".session drop 3",)

    async def test_drop_clause_out_of_range(self, conn: AsyncMock) -> None:
        with pytest.raises(IndexError, match="2 clause"):
            await Session(conn).drop_rule("hop", index=3)

    async def test_clear(self, conn: AsyncMock) -> None:
        await Session(conn).clear()
        conn.execute.assert_awaited_once_with(".session clear")
