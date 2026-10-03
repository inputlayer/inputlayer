"""Apply fact changes exactly once per revision.

The applier is the single writer of the relations it owns: one adapter
process per set of relations. For each batch it reads the stored revision of
every key, drops changes at or below it (replays, late deliveries), and
commits the rest with an engine-side revision check in one transaction. A
retry after any failure, including a broken connection that leaves the outcome
unknown, applies each change at most once.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

import aiohttp

from . import iql
from .changes import FactChange, Relation, latest_per_key
from .config import EngineSettings
from .engine import EngineSession, EngineUnavailable, Result

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Outcome:
    applied: int
    skipped: int


class Applier:
    def __init__(self, http: aiohttp.ClientSession, settings: EngineSettings) -> None:
        self._http = http
        self._settings = settings
        self._session: EngineSession | None = None
        self._lock = asyncio.Lock()

    async def declare(self, relations: tuple[Relation, ...]) -> None:
        """Declare the owned relations' schemas (idempotent)."""
        async with self._lock:
            await self._execute(iql.schema_program(relations))

    async def apply(self, changes: list[FactChange]) -> Outcome:
        started = time.perf_counter()
        latest = latest_per_key(changes)
        async with self._lock:
            fresh = [c for c in latest if c.revision > await self._stored_revision(c)]
            if fresh:
                await self._execute(iql.apply_program(fresh))
        outcome = Outcome(applied=len(fresh), skipped=len(changes) - len(fresh))
        log.info(
            "applied=%d skipped=%d in %.1f ms",
            outcome.applied,
            outcome.skipped,
            (time.perf_counter() - started) * 1000,
        )
        return outcome

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _stored_revision(self, change: FactChange) -> int:
        """The last applied revision for the change's key, or -1 if none."""
        rows = (await self._execute(iql.revision_query(change))).rows
        return max((int(row[-1]) for row in rows), default=-1)

    async def _execute(self, program: str) -> Result:
        if self._session is None:
            self._session = await EngineSession.open(self._http, self._settings)
        try:
            return await self._session.execute(program)
        except EngineUnavailable:
            # Reconnect on the next request; the caller's retry is safe.
            self._session = None
            raise
