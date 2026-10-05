"""Writes send their values as parameters (protocol version 4): the program
text holds ``$pN`` references only, and the engine binds the values without
parsing them."""

from __future__ import annotations

import asyncio
import json
import math
from datetime import datetime, timezone
from typing import Any

import pytest

from inputlayer import CompileError, KnowledgeGraph, Relation
from inputlayer._literal import collect_params, encode, literal, params_of
from inputlayer._protocol import ExecuteMessage, ResultResponse
from inputlayer.compiler import compile_insert
from inputlayer.connection import Connection
from inputlayer.exceptions import ConnectionError

from ._wire import ScriptedWire, attach


class Row(Relation):
    id: int
    name: str
    score: float
    ok: bool


class Attempt(Relation):
    order: str
    tool: str
    attempt: str


HOSTILE = 'x"), +evil(1) <- a(1)\n?b(X)'


class _Recorder:
    """A connection that records each program and its params."""

    def __init__(self, rows: list[list[Any]] | None = None) -> None:
        self.sent: list[tuple[str, dict[str, Any] | None]] = []
        self._rows = rows or []

    async def execute(
        self, iql: str, *, timeout: float | None = None, params: dict[str, Any] | None = None
    ) -> ResultResponse:
        self.sent.append((iql, params))
        return ResultResponse(
            columns=[], rows=self._rows, row_count=len(self._rows),
            total_count=len(self._rows), truncated=False, execution_time_ms=0,
        )


def _kg(conn: _Recorder) -> KnowledgeGraph:
    return KnowledgeGraph("kg", conn)  # type: ignore[arg-type]


class TestCollectParams:
    def test_each_value_is_typed_as_its_literal(self) -> None:
        when = datetime(2026, 10, 10, tzinfo=timezone.utc)
        with collect_params() as params:
            text = " ".join(
                encode(v) for v in [True, 'a"b', 42, 0.1, 2.0, 1e20, when, [0.5, 2], -(2**63), -0.0]
            )
        assert text == "$p0 $p1 $p2 $p3 $p4 $p5 $p6 $p7 $p8 $p9"
        assert params == {
            "p0": True,
            "p1": 'a"b',
            "p2": 42,
            "p3": 0.1,
            "p4": 2.0,
            "p5": {"float": 1e20},
            "p6": 1_791_590_400_000,
            "p7": [0.5, 2.0],
            "p8": -(2**63),
            "p9": {"float": -0.0},
        }
        # JSON keeps every float a float, and every int an int.
        assert json.loads(json.dumps(params)) == params
        assert json.dumps(params["p4"]) == "2.0"
        assert json.dumps(params["p9"]) == '{"float": -0.0}'

    def test_equal_literals_share_one_name(self) -> None:
        with collect_params() as params:
            text = " ".join(encode(v) for v in ["x", 1, "x", 1, "1", True])
        assert text == "$p0 $p1 $p0 $p1 $p2 $p3"
        assert params == {"p0": "x", "p1": 1, "p2": "1", "p3": True}

    def test_refusals_still_hold(self) -> None:
        for bad in [math.nan, math.inf, -math.inf, None, 2**63, [1.0, math.nan], "\ud800"]:
            with pytest.raises(CompileError), collect_params():
                encode(bad)

    def test_literal_outside_the_scope_and_for_syntax(self) -> None:
        assert encode("x") == '"x"'
        with collect_params() as params:
            assert literal(5) == "5"
            with collect_params() as inner:
                assert encode("a") == "$p0"
            assert encode("b") == "$p0"
        assert params == {"p0": "b"} and inner == {"p0": "a"}
        assert encode("x") == '"x"'

    async def test_concurrent_tasks_never_share_a_scope(self) -> None:
        async def compile_in_scope(value: str) -> tuple[str, dict[str, Any]]:
            with collect_params() as params:
                first = encode(value)
                await asyncio.sleep(0)
                text = f"{first} {encode(value + '!')}"
            return text, params

        results = await asyncio.gather(*(compile_in_scope(f"v{i}") for i in range(20)))
        for i, (text, params) in enumerate(results):
            assert text == "$p0 $p1"
            assert params == {"p0": f"v{i}", "p1": f"v{i}!"}

    def test_params_of_names_what_one_statement_references(self) -> None:
        params = {"p0": "a", "p1": 2, "p2": "c"}
        assert params_of("+r($p2, $p0)\n", params) == {"p2": "c", "p0": "a"}
        assert params_of(".rule clear r", params) == {}

    def test_literal_compilers_are_literal_outside_the_scope(self) -> None:
        assert compile_insert(Row(id=1, name="n", score=0.5, ok=True)) == '+row(1, "n", 0.5, true)'


class TestWrites:
    async def test_insert_bulk_insert_and_delete(self) -> None:
        conn = _Recorder([["Inserted 1 fact(s) into 'row'."]])
        row = Row(id=1, name=HOSTILE, score=2.0, ok=False)
        await _kg(conn).insert(row)
        await _kg(conn).insert([row, Row(id=3, name=HOSTILE, score=2.0, ok=False)])
        await _kg(conn).delete(row)
        assert [iql for iql, _ in conn.sent] == [
            "+row($p0, $p1, $p2, $p3)",
            "+row[($p0, $p1, $p2, $p3), ($p4, $p1, $p2, $p3)]",
            "-row($p0, $p1, $p2, $p3)",
        ]
        assert conn.sent[0][1] == {"p0": 1, "p1": HOSTILE, "p2": 2.0, "p3": False}
        assert conn.sent[1][1] == {"p0": 1, "p1": HOSTILE, "p2": 2.0, "p3": False, "p4": 3}

    async def test_conditional_delete(self) -> None:
        conn = _Recorder()
        await _kg(conn).delete(Row, where=lambda r: r.name == HOSTILE)
        iql, params = conn.sent[0]
        assert "evil" not in iql
        assert list((params or {}).values()) == [HOSTILE]

    async def test_guarded_program_and_claim(self) -> None:
        conn = _Recorder()
        kg = _kg(conn)
        kg._guard_relations_declared = True
        with pytest.raises(Exception):  # noqa: B017 - the recorder answers no statement
            await (
                kg.program()
                .insert(Attempt(order=HOSTILE, tool="t", attempt="a1"))
                .when(Attempt.any(order="ORD-1"), ~Attempt.any(order="ORD-1", tool="t"))
                .commit()
            )
        iql, params = conn.sent[-1]
        assert not any(v in iql for v in ["evil", "ORD-1", '"t"', "a1", '"t-'])
        values = list((params or {}).values())
        assert {HOSTILE, "ORD-1", "t", "a1"} <= set(values)
        assert any(isinstance(v, str) and v.startswith("t-") for v in values)

        conn = _Recorder()
        await _kg(conn).claim(Attempt(order=HOSTILE, tool="t", attempt="a1"), key=["order"])
        iql, params = conn.sent[0]
        assert "evil" not in iql and "a1" not in iql
        assert {HOSTILE, "t", "a1"} <= set((params or {}).values())

    async def test_queries_keep_literals(self) -> None:
        conn = _Recorder()
        await _kg(conn).query(Row, where=lambda r: r.name == "n")
        iql, params = conn.sent[0]
        assert '"n"' in iql and not params


class TestWire:
    def test_execute_frame_carries_params_only_when_given(self) -> None:
        frame = json.loads(ExecuteMessage(program="+r($p0)", params={"p0": "x"}).to_json())
        assert frame == {"type": "execute", "program": "+r($p0)", "params": {"p0": "x"}}
        bare = json.loads(ExecuteMessage(program="?r(X)", params={}).to_json())
        assert "params" not in bare
        with pytest.raises(ValueError):
            ExecuteMessage(program="+r($p0)", params={"p0": math.nan}).to_json()

    async def test_params_need_protocol_4(self) -> None:
        wire = ScriptedWire()
        conn = attach(Connection("ws://unused", api_key="k"), wire)
        conn._protocol_version = 3
        with pytest.raises(ConnectionError, match="protocol 3; parameters need version 4"):
            await conn.execute("+r($p0)", params={"p0": 1})
        assert wire.sent == []
        await conn.close()
