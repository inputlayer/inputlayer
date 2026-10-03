from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock

import pytest

from ingest import demo
from ingest.engine import EngineError, EngineUnavailable
from ingest.watch import Delta


class ControlledWatcher:
    def __init__(self, failure: Exception | None = None) -> None:
        self.release = asyncio.Event()
        self.closed = asyncio.Event()
        self.failure = failure

    async def deltas(self) -> AsyncIterator[Delta]:
        try:
            yield Delta("attention", 1, 1, [[1, "Acme", "inv_100"]], [])
            await self.release.wait()
            if self.failure is not None:
                raise self.failure
        finally:
            self.closed.set()


@pytest.mark.parametrize("phase", ["expected", "quiet", "final"])
@pytest.mark.parametrize(
    "failure", [None, EngineError("subscription failed"), EngineUnavailable("connection closed")]
)
async def test_self_check_observes_collector_termination(
    monkeypatch: pytest.MonkeyPatch, phase: str, failure: Exception | None
) -> None:
    monkeypatch.setenv("WEBHOOK_SECRET_BILLING", "whsec_dGVzdC1vbmx5")
    monkeypatch.setattr(demo, "QUIET_WINDOW", 0.01)
    watcher = ControlledWatcher(failure)
    driver = demo.Demo(AsyncMock(), watcher, AsyncMock())

    async def scenario() -> None:
        await driver.step("first delta")
        await driver.expect(inserted=[[1, "Acme", "inv_100"]])
        watcher.release.set()
        if phase == "expected":
            await driver.expect(retracted=[[1, "Acme", "inv_100"]])
        elif phase == "quiet":
            await driver.expect_quiet()

    monkeypatch.setattr(driver, "run", scenario)
    with pytest.raises(ExceptionGroup) as raised:
        await asyncio.wait_for(driver.check(), 1)
    error = raised.value.exceptions[0]
    if failure is None:
        assert isinstance(error, demo.DemoFailure)
    else:
        assert error is failure
    assert watcher.closed.is_set()


async def test_self_check_awaits_collector_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WEBHOOK_SECRET_BILLING", "whsec_dGVzdC1vbmx5")
    monkeypatch.setattr(demo, "QUIET_WINDOW", 0.01)
    watcher = ControlledWatcher()
    driver = demo.Demo(AsyncMock(), watcher, AsyncMock())

    async def scenario() -> None:
        await driver.step("first delta")
        await driver.expect(inserted=[[1, "Acme", "inv_100"]])
        await driver.expect_quiet()

    monkeypatch.setattr(driver, "run", scenario)
    await driver.check()
    assert watcher.closed.is_set()
