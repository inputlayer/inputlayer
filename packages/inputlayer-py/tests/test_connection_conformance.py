"""Replays the shared connection fixtures (packages/conformance/connection)
through a mock server against the real connection core."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from inputlayer.connection import Connection
from inputlayer.exceptions import (
    Cancelled,
    ConnectionLost,
    DeadlineExceeded,
    InternalError,
    OutcomeUnknownError,
    ProtocolError,
    QueryError,
    StatementFailedError,
)

from ._mock_server import MockServer, Peer

FIXTURES = Path(__file__).resolve().parents[2] / "conformance" / "connection"

ERROR_KINDS: dict[str, type[BaseException]] = {
    "statement_failed": StatementFailedError,
    "deadline_exceeded": DeadlineExceeded,
    "cancelled": Cancelled,
    "outcome_unknown": OutcomeUnknownError,
    "protocol": ProtocolError,
    "internal": InternalError,
    "connection_lost": ConnectionLost,
}


def _substitute(value: Any, ids: dict[str, str]) -> Any:
    if isinstance(value, str) and value.startswith("$"):
        return ids[value[1:]]
    if isinstance(value, dict):
        return {k: _substitute(v, ids) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute(v, ids) for v in value]
    return value


def _play(steps: list[dict[str, Any]], done: asyncio.Event) -> Any:
    async def handler(peer: Peer) -> None:
        await peer.authenticate()
        ids: dict[str, str] = {}
        for step in steps:
            if "recv" in step:
                frame = await peer.recv_type(step["recv"]["type"])
                for key, want in step["recv"].items():
                    assert frame.get(key) == want, f"{key}: {frame.get(key)!r} != {want!r}"
                if "as" in step:
                    ids[step["as"]] = frame["id"]
            elif "send" in step:
                await peer.send(_substitute(step["send"], ids))
            elif "close" in step:
                await peer.close(step["close"])
        done.set()
        await peer.serve_results()

    return handler


def _check_call(expect: dict[str, Any], outcome: Any) -> None:
    if "error" not in expect:
        assert not isinstance(outcome, BaseException), outcome
        assert outcome.rows == expect["rows"]
        if "columns" in expect:
            assert outcome.columns == expect["columns"]
        return
    kind = expect["error"]
    assert isinstance(outcome, BaseException), f"expected {kind}, got {outcome!r}"
    if kind == "query":
        assert type(outcome) is QueryError, repr(outcome)
    else:
        assert isinstance(outcome, ERROR_KINDS[kind]), repr(outcome)
    if "code" in expect:
        assert outcome.code == expect["code"]  # type: ignore[attr-defined]
    if "may_have_committed" in expect:
        assert outcome.may_have_committed is expect["may_have_committed"]  # type: ignore[attr-defined]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fixture", sorted(FIXTURES.glob("*.json")), ids=lambda p: p.stem
)
async def test_connection_fixture(fixture: Path) -> None:
    spec = json.loads(fixture.read_text())
    done = asyncio.Event()
    async with MockServer(_play(spec["server"], done)) as server:
        conn = Connection(server.url, username="u", password="p", auto_reconnect=False)
        notifications: list[int] = []
        events: list[str] = []
        pushes: list[dict[str, Any]] = []
        conn.dispatcher.on(callback=lambda e: notifications.append(e.seq))
        conn.events.on(callback=lambda e: events.append(e.type))
        for sub in spec.get("routes", []):
            conn.add_route(
                sub,
                lambda f: pushes.append(
                    {"subscription": f.subscription, "generation": f.generation, "seq": f.seq}
                ),
            )
        await conn.connect()
        calls = [
            asyncio.ensure_future(conn.execute(call["execute"], timeout=10))
            for call in spec["calls"]
        ]
        outcomes = await asyncio.gather(*calls, return_exceptions=True)
        await asyncio.wait_for(done.wait(), 5)
        await asyncio.sleep(0.05)  # let pushes sent after the last reply arrive
        for call, outcome in zip(spec["calls"], outcomes, strict=True):
            _check_call(call["expect"], outcome)
        expect = spec.get("expect", {})
        if "notifications" in expect:
            assert notifications == expect["notifications"]
        if "pushes" in expect:
            assert pushes == expect["pushes"]
        if "stale_pushes" in expect:
            assert conn.stale_pushes == expect["stale_pushes"]
        if "events" in expect:
            assert [e for e in events if e != "closed"] == expect["events"]
        await conn.close()
