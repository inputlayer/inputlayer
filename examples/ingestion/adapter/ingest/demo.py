"""End-to-end demo and self-check of the ingestion recipe.

Changes Postgres rows and sends billing webhooks, and asserts that a watching
client subscribed to the derived `needs_attention` answer receives exactly the
expected deltas, in order: replays, no-op updates and late out-of-order events
must push nothing. Run it on a fresh stack: `./demo.sh` at the example root.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

import aiohttp
import psycopg

from . import config
from .engine import EngineSession
from .signing import Signer
from .watch import Delta, Watcher

QUERY = "?needs_attention(Id, Name, Invoice)"
RULES = Path(__file__).with_name("rules.iql")
DELTA_TIMEOUT = 15.0
"""Upper bound for one change to reach the watcher (includes Debezium polling)."""
QUIET_WINDOW = 1.5
"""How long a step that must not change the answer waits for a stray delta."""


class DemoFailure(AssertionError):
    pass


class Demo:
    def __init__(self, http: aiohttp.ClientSession, watcher: Watcher, db: Any) -> None:
        self._http = http
        self._watcher = watcher
        self._db = db
        self._adapter = os.environ.get("ADAPTER_URL", "http://adapter:8090")
        self._billing = Signer(os.environ["WEBHOOK_SECRET_BILLING"])
        self._deltas: asyncio.Queue[Delta] = asyncio.Queue()
        self._step = 0

    async def collect(self) -> None:
        async for delta in self._watcher.deltas():
            await self._deltas.put(delta)

    async def run(self) -> None:
        await self.step("billing webhook: payment.failed inv_100 for Acme (enterprise)")
        await self.webhook("payment.failed", 1, "inv_100", 1, applied=1)
        await self.expect(inserted=[[1, "Acme", "inv_100"]])

        await self.step("the same webhook delivered again (a SaaS retry)")
        await self.webhook("payment.failed", 1, "inv_100", 1, applied=0)
        await self.expect_quiet()

        await self.step("billing webhook: payment.failed inv_200 for Globex (free tier)")
        await self.webhook("payment.failed", 1, "inv_200", 2, applied=1)
        await self.expect_quiet()

        await self.step("Postgres: UPDATE customers SET tier = 'enterprise' WHERE id = 2")
        await self.sql("UPDATE customers SET tier = 'enterprise' WHERE id = 2")
        await self.expect(inserted=[[2, "Globex", "inv_200"]])

        await self.step("Postgres: UPDATE customers SET tier = tier WHERE id = 1 (no-op)")
        await self.sql("UPDATE customers SET tier = tier WHERE id = 1")
        await self.expect_quiet()

        await self.step("Postgres: UPDATE customers SET name = 'Globex Corp' WHERE id = 2")
        await self.sql("UPDATE customers SET name = 'Globex Corp' WHERE id = 2")
        await self.expect(
            inserted=[[2, "Globex Corp", "inv_200"]], retracted=[[2, "Globex", "inv_200"]]
        )

        await self.step("Postgres: DELETE FROM customers WHERE id = 1")
        await self.sql("DELETE FROM customers WHERE id = 1")
        await self.expect(retracted=[[1, "Acme", "inv_100"]])

        await self.step("billing webhook: payment.succeeded inv_200 (sequence 3)")
        await self.webhook("payment.succeeded", 3, "inv_200", 2, applied=1)
        await self.expect(retracted=[[2, "Globex Corp", "inv_200"]])

        await self.step("late webhook: payment.failed inv_200 (sequence 2) arrives after it")
        await self.webhook("payment.failed", 2, "inv_200", 2, applied=0)
        await self.expect_quiet()

    async def step(self, title: str) -> None:
        self._step += 1
        self._started = time.perf_counter()
        print(f"\n[{self._step}] {title}", flush=True)

    async def webhook(
        self, kind: str, sequence: int, invoice: str, customer: int, *, applied: int
    ) -> None:
        event_id = f"evt_{uuid.uuid4().hex[:12]}"
        event = {
            "id": event_id,
            "type": kind,
            "sequence": sequence,
            "data": {"invoice": invoice, "customer_id": customer, "amount_cents": 4200},
        }
        body = json.dumps(event).encode()
        headers = {"content-type": "application/json", **self._billing.headers(event_id, body)}
        url = f"{self._adapter}/webhooks/billing"
        async with self._http.post(url, data=body, headers=headers) as response:
            reply = await response.json()
        print(f"    adapter: {reply}", flush=True)
        if response.status != 200 or reply["applied"] != applied:
            raise DemoFailure(f"expected applied={applied}, got {response.status} {reply}")

    async def sql(self, statement: str) -> None:
        await self._db.execute(statement)

    async def expect(
        self, inserted: list[list[Any]] | None = None, retracted: list[list[Any]] | None = None
    ) -> None:
        want_in, want_out = _rows(inserted), _rows(retracted)
        got_in: set[str] = set()
        got_out: set[str] = set()
        deadline = time.perf_counter() + DELTA_TIMEOUT
        while (got_in, got_out) != (want_in, want_out):
            remaining = deadline - time.perf_counter()
            try:
                delta = await asyncio.wait_for(self._deltas.get(), max(remaining, 0.001))
            except TimeoutError:
                raise DemoFailure(
                    f"timed out: expected +{sorted(want_in)} -{sorted(want_out)},"
                    f" got +{sorted(got_in)} -{sorted(got_out)}"
                ) from None
            got_in |= _rows(delta.inserted)
            got_out |= _rows(delta.retracted)
            if not got_in <= want_in or not got_out <= want_out:
                raise DemoFailure(f"unexpected delta seq={delta.seq}: {delta}")
            elapsed = (time.perf_counter() - self._started) * 1000
            for row in delta.retracted:
                print(f"    watcher: seq={delta.seq} - {row}  ({elapsed:.0f} ms)", flush=True)
            for row in delta.inserted:
                print(f"    watcher: seq={delta.seq} + {row}  ({elapsed:.0f} ms)", flush=True)

    async def expect_quiet(self) -> None:
        try:
            delta = await asyncio.wait_for(self._deltas.get(), QUIET_WINDOW)
        except TimeoutError:
            print(f"    watcher: no delta within {QUIET_WINDOW}s, as expected", flush=True)
            return
        raise DemoFailure(f"expected no change to the answer, got {delta}")


def _rows(rows: list[list[Any]] | None) -> set[str]:
    return {json.dumps(row) for row in rows or []}


async def _prepare(http: aiohttp.ClientSession, settings: config.EngineSettings) -> None:
    """Load the rules and wait until Debezium's initial snapshot is ingested."""
    session = await EngineSession.open(http, settings)
    try:
        await session.execute(RULES.read_text())
        deadline = time.perf_counter() + 120
        while len((await session.execute("?customer(Id, Name, Tier)")).rows) < 3:
            if time.perf_counter() > deadline:
                raise DemoFailure("the Postgres snapshot did not reach the engine in 120s")
            await asyncio.sleep(0.5)
    finally:
        await session.close()


async def main() -> None:
    settings = config.engine_from_env()
    async with aiohttp.ClientSession() as http:
        print("loading rules and waiting for the Postgres snapshot ...", flush=True)
        await _prepare(http, settings)
        watcher = await Watcher.open(http, settings)
        snapshot = await watcher.subscribe("attention", QUERY)
        print(f"watching {QUERY}; snapshot: {snapshot.rows}", flush=True)
        if snapshot.rows:
            raise DemoFailure("the demo needs a fresh stack: run ./demo.sh")
        db = await psycopg.AsyncConnection.connect(os.environ["DATABASE_URL"], autocommit=True)
        demo = Demo(http, watcher, db)
        collector = asyncio.create_task(demo.collect())
        try:
            await demo.run()
            await demo.expect_quiet()
        finally:
            collector.cancel()
            await db.close()
            await watcher.close()
    print("\nall deltas matched: CDC and webhooks drive the standing query.", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
