"""A watching client: subscribe to a standing query and stream its deltas.

    python -m ingest.watch '?needs_attention(Id, Name, Invoice)'

This is what an AI agent does to react to derived conclusions instead of
polling: `.subscribe` returns the current result, then the engine pushes the
rows that enter and leave it after every commit that can change it.
"""

from __future__ import annotations

import asyncio
import sys
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import aiohttp

from . import config
from .engine import EngineError, EngineSession, Result


@dataclass(frozen=True, slots=True)
class Delta:
    subscription: str
    seq: int
    revision: int
    """The knowledge graph revision the answer reaches with this delta."""
    inserted: list[list[Any]]
    retracted: list[list[Any]]


@dataclass(slots=True)
class _Tracked:
    generation: int
    last_seq: int = 0


class Watcher:
    """Standing queries on one connection, with their delta streams checked.

    A push for an older generation of a subscription (from before an
    unsubscribe and resubscribe under the same name) is dropped; a gap in a
    subscription's delta `seq` means a delta was lost and raises, because the
    client's copy of the answer can no longer be trusted: resubscribe.
    """

    def __init__(self, session: EngineSession) -> None:
        self._session = session
        self._subscriptions: dict[str, _Tracked] = {}

    @classmethod
    async def open(cls, http: aiohttp.ClientSession, settings: config.EngineSettings) -> Watcher:
        keep = frozenset({"subscription_delta", "subscription_error"})
        return cls(await EngineSession.open(http, settings, keep_pushes=keep))

    async def subscribe(self, subscription: str, query: str) -> Result:
        """Register the standing query; returns its current result."""
        result = await self._session.execute(f".subscribe {subscription} {query}")
        self._subscriptions[subscription] = _Tracked(result.frame["subscribed"]["generation"])
        return result

    async def deltas(self) -> AsyncIterator[Delta]:
        """Deltas of every subscription on this connection, in push order."""
        async for push in self._session.pushes():
            tracked = self._subscriptions.get(push["subscription"])
            if tracked is None or push["generation"] != tracked.generation:
                continue
            if push["type"] == "subscription_error":
                raise EngineError(f"subscription {push['subscription']}: {push['message']}")
            if push["seq"] != tracked.last_seq + 1:
                raise EngineError(
                    f"subscription {push['subscription']}: delta {tracked.last_seq + 1} lost"
                    f" (got {push['seq']}); resubscribe"
                )
            tracked.last_seq = push["seq"]
            yield Delta(
                push["subscription"],
                push["seq"],
                push["revision"],
                push["inserted"],
                push["retracted"],
            )

    async def close(self) -> None:
        await self._session.close()


async def _watch(query: str) -> None:
    async with aiohttp.ClientSession() as http:
        watcher = await Watcher.open(http, config.engine_from_env())
        snapshot = await watcher.subscribe("watch", query)
        revision = snapshot.frame["subscribed"]["revision"]
        print(f"snapshot at revision {revision} {snapshot.columns}: {snapshot.rows}", flush=True)
        async for delta in watcher.deltas():
            stamp = f"{time.strftime('%H:%M:%S')} revision={delta.revision}"
            for row in delta.retracted:
                print(f"{stamp} - {row}", flush=True)
            for row in delta.inserted:
                print(f"{stamp} + {row}", flush=True)


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m ingest.watch '<?query>'")
    asyncio.run(_watch(sys.argv[1]))


if __name__ == "__main__":
    main()
