"""Engine failures through the public API: no failure reads as data.

Frames are shaped exactly as the engine sends them (`src/protocol/rest/
handlers/ws.rs`): an `error` frame for a failed one-statement program and
`errors[]` on `result`/`result_start` for failed statements of a longer one.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from inputlayer import (
    OutcomeUnknownError,
    Relation,
    StatementError,
    StatementFailedError,
    StoreReadOnlyError,
)
from inputlayer.connection import Connection
from inputlayer.exceptions import InternalError, QueryError
from inputlayer.knowledge_graph import KnowledgeGraph
from inputlayer.migrations.errors import MigrationError
from inputlayer.migrations.recorder import MigrationRecorder


class Demo(Relation):
    x: int


class ScriptedWire:
    """A WebSocket that answers each recv() with the next scripted frame."""

    def __init__(self, *frames: dict[str, Any]) -> None:
        self._frames = [json.dumps(f) for f in frames]
        self.sent: list[str] = []

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw)["program"])

    async def recv(self) -> str:
        return self._frames.pop(0)

    @property
    def drained(self) -> bool:
        return not self._frames


def _kg(wire: ScriptedWire, current_kg: str = "default") -> KnowledgeGraph:
    conn = Connection("ws://unused")
    conn._ws = wire  # type: ignore[assignment]
    conn._connected = True
    conn._current_kg = current_kg
    return KnowledgeGraph("default", conn)


def _messages(*rows: str, errors: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "type": "result",
        "columns": ["message"],
        "rows": [[r] for r in rows],
        "row_count": len(rows),
        "total_count": len(rows),
        "truncated": False,
        "execution_time_ms": 0,
        "errors": errors or [],
    }


WAL_FAILURE = {
    "type": "error",
    "message": "WAL append failed: No such file or directory",
    "code": "internal",
}
ARITY = "Insert rejected for 'demo': arity mismatch"


class TestErrorFrame:
    @pytest.mark.asyncio
    async def test_failed_insert_raises_instead_of_counting_a_row(self) -> None:
        # https://github.com/inputlayer/inputlayer/issues/93: this returned
        # InsertResult(count=1).
        wire = ScriptedWire(WAL_FAILURE)
        with pytest.raises(QueryError) as caught:
            await _kg(wire).insert(Demo(x=1))
        assert caught.value.code == "internal"
        assert caught.value.message.startswith("WAL append failed")
        assert caught.value.query == wire.sent[0]

    @pytest.mark.asyncio
    async def test_parse_errors_keep_validation_details(self) -> None:
        details = [{"line": 1, "statement_index": 0, "error": "unexpected token"}]
        wire = ScriptedWire({
            "type": "error",
            "message": "Program has 1 parse error(s)",
            "validation_errors": details,
            "code": "validation",
        })
        with pytest.raises(QueryError) as caught:
            await _kg(wire).execute("+demo(")
        assert caught.value.code == "validation"
        assert caught.value.validation_errors == details

    @pytest.mark.asyncio
    async def test_error_without_statement_cause_has_no_code(self) -> None:
        wire = ScriptedWire({"type": "error", "message": "Server shutting down"})
        with pytest.raises(QueryError) as caught:
            await _kg(wire).execute("?demo(X)")
        assert caught.value.code is None

    @pytest.mark.asyncio
    async def test_a_relation_named_error_is_data(self) -> None:
        wire = ScriptedWire({
            "type": "result", "columns": ["error"], "rows": [["disk full"]],
            "row_count": 1, "total_count": 1, "truncated": False,
            "execution_time_ms": 0, "errors": [],
        })
        result = await _kg(wire).execute("?error(E)")
        assert result.rows == [["disk full"]]


class TestStatementErrors:
    @pytest.mark.asyncio
    async def test_partial_failure_lists_every_failed_statement(self) -> None:
        errors = [
            {"index": 1, "code": "not_found", "message": "Relation 'x' not found."},
            {"index": 2, "code": "validation", "message": ARITY},
        ]
        wire = ScriptedWire(_messages(
            "Inserted 1 fact(s) into 'demo'.",
            "Relation 'x' not found.",
            ARITY,
            errors=errors,
        ))
        with pytest.raises(StatementFailedError) as caught:
            await _kg(wire).execute("+demo(1).\n.rel drop x\n+demo(1, 2).")
        err = caught.value
        assert err.errors == [StatementError(**e) for e in errors]
        assert err.code == "not_found"
        assert len(err.result.rows) == 3
        assert isinstance(err, QueryError)

    @pytest.mark.asyncio
    async def test_partial_failure_still_tracks_a_kg_switch(self) -> None:
        frame = _messages(
            "Switched to knowledge graph: other",
            "Relation 'x' not found.",
            errors=[{"index": 1, "code": "not_found", "message": "Relation 'x' not found."}],
        )
        frame["switched_kg"] = "other"
        kg = _kg(ScriptedWire(frame))
        with pytest.raises(StatementFailedError):
            await kg.execute(".kg use other\n.rel drop x")
        assert kg._conn.current_kg == "other"

    @pytest.mark.asyncio
    async def test_load_raises_on_a_failed_statement(self, tmp_path) -> None:
        path = tmp_path / "seed.iql"
        path.write_text("+demo(1).\n+demo(1, 2).\n")
        wire = ScriptedWire(_messages(
            "Inserted 1 fact(s) into 'demo'.",
            ARITY,
            errors=[{"index": 1, "code": "validation", "message": ARITY}],
        ))
        with pytest.raises(StatementFailedError) as caught:
            await _kg(wire).load(str(path))
        assert [e.index for e in caught.value.errors] == [1]


class TestChunkedResults:
    def _stream(self, errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "type": "result_start", "columns": ["x"], "total_count": 3,
                "truncated": False, "execution_time_ms": 4,
                "timing_breakdown": {"total_us": 7}, "errors": errors,
            },
            {"type": "result_chunk", "rows": [[1], [2]], "chunk_index": 0},
            {
                "type": "persistent_update", "seq": 1, "timestamp_ms": 0,
                "knowledge_graph": "default", "relation": "demo",
                "operation": "insert", "count": 1,
            },
            {"type": "result_chunk", "rows": [[3]], "chunk_index": 1},
            {"type": "result_end", "row_count": 3, "chunk_count": 2},
        ]

    @pytest.mark.asyncio
    async def test_errors_on_result_start_survive_assembly(self) -> None:
        errors = [{"index": 0, "code": "validation", "message": ARITY}]
        wire = ScriptedWire(*self._stream(errors))
        with pytest.raises(StatementFailedError) as caught:
            await _kg(wire).execute("+demo(1, 2).\n?demo(X)")
        assert caught.value.errors == [StatementError(**errors[0])]
        assert caught.value.result.rows == [[1], [2], [3]]
        # The whole stream is consumed, so the next call reads its own reply.
        assert wire.drained

    @pytest.mark.asyncio
    async def test_successful_stream_keeps_timing(self) -> None:
        wire = ScriptedWire(*self._stream([]))
        result = await _kg(wire).execute("?demo(X)")
        assert result.rows == [[1], [2], [3]]
        assert result.timing_breakdown == {"total_us": 7}

    @pytest.mark.asyncio
    async def test_error_frame_mid_stream_raises_typed(self) -> None:
        start, chunk = self._stream([])[:2]
        wire = ScriptedWire(start, chunk, {"type": "error", "message": "Server shutting down"})
        with pytest.raises(QueryError, match="Server shutting down"):
            await _kg(wire).execute("?demo(X)")


class TestInsertCount:
    @pytest.mark.asyncio
    async def test_count_is_the_engines_stored_count(self) -> None:
        wire = ScriptedWire(_messages("Inserted 2 fact(s) into 'demo'."))
        result = await _kg(wire).insert(Demo, data=[{"x": 1}, {"x": 2}, {"x": 2}])
        assert result.count == 2

    @pytest.mark.asyncio
    async def test_duplicate_insert_counts_zero(self) -> None:
        wire = ScriptedWire(_messages("Inserted 0 fact(s) into 'demo'."))
        assert (await _kg(wire).insert(Demo(x=1))).count == 0

    @pytest.mark.asyncio
    async def test_unrecognised_reply_is_not_success(self) -> None:
        wire = ScriptedWire(_messages("Something else entirely"))
        with pytest.raises(InternalError):
            await _kg(wire).insert(Demo(x=1))


class TestKgSwitch:
    @pytest.mark.asyncio
    async def test_missing_kg_is_created_on_not_found(self) -> None:
        wire = ScriptedWire(
            {"type": "error", "message": "Knowledge graph not found", "code": "not_found"},
            _messages("Knowledge graph 'default' created."),
            {**_messages("Switched to knowledge graph: default"), "switched_kg": "default"},
            _messages("Inserted 1 fact(s) into 'demo'."),
        )
        kg = _kg(wire, current_kg="other")
        assert (await kg.insert(Demo(x=1))).count == 1
        assert wire.sent[:3] == [".kg use default", ".kg create default", ".kg use default"]

    @pytest.mark.asyncio
    async def test_other_switch_failures_raise(self) -> None:
        wire = ScriptedWire({"type": "error", "message": "Permission denied", "code": "validation"})
        with pytest.raises(QueryError) as caught:
            await _kg(wire, current_kg="other").insert(Demo(x=1))
        assert caught.value.query == ".kg use default"
        assert wire.sent == [".kg use default"]


class TestMigrationRecorder:
    def test_schema_failure_raises(self) -> None:
        class FailingKG:
            def execute(self, iql: str) -> Any:
                raise QueryError("WAL append failed", code="internal")

        with pytest.raises(MigrationError, match="WAL append failed"):
            MigrationRecorder(FailingKG()).ensure_schema()


@pytest.mark.asyncio
@pytest.mark.parametrize("code,error_type", [
    ("outcome_unknown", OutcomeUnknownError),
    ("store_read_only", StoreReadOnlyError),
])
@pytest.mark.parametrize("shape", ["error", "result", "stream"])
async def test_durability_outcomes_preserve_switch_and_drain(code, error_type, shape):
    message = "write outcome unknown, store read-only until restart recovery"
    errors = [{"index": 1, "code": code, "message": message}]
    if shape == "error":
        frames = [{"type": "error", "code": code, "message": message}]
    elif shape == "result":
        frames = [{**_messages(message, errors=errors), "switched_kg": "other"}]
    else:
        frames = TestChunkedResults()._stream(errors)
        frames[0]["switched_kg"] = "other"
    wire = ScriptedWire(*frames)
    kg = _kg(wire)
    with pytest.raises(error_type) as caught:
        await kg.execute("+demo(1)\n+demo(2)")
    assert caught.value.code == code
    assert not isinstance(caught.value, StatementFailedError)
    assert wire.drained
    if shape != "error":
        assert kg._conn.current_kg == "other"
        assert caught.value.result is not None
