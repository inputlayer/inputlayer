"""HTTP front of the adapter: Debezium's HTTP sink and SaaS webhooks post here.

POST /cdc                 Debezium change event(s)
POST /webhooks/<source>   one signed webhook event
GET  /healthz             200 once the adapter's schemas are declared

For acknowledgment, skip and error semantics, see the Failure behavior section
of docs/content/docs/guides/ingestion.mdx. Retry safety is enforced by applier.py;
every write is signed per Standard Webhooks (signing.py).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any

import aiohttp
from aiohttp import web

from . import config, debezium, mappings, webhooks
from .applier import Applier
from .changes import FactChange, MappingError
from .engine import EngineError, EngineUnavailable
from .signing import Signer

log = logging.getLogger(__name__)

APPLIER = web.AppKey("applier", Applier)
SETTINGS = web.AppKey("settings", config.Settings)
READY = web.AppKey("ready", asyncio.Event)


async def cdc(request: web.Request) -> web.Response:
    body = await _verified_json(request, request.app[SETTINGS].cdc_signer)
    return await _apply(request, lambda: debezium.changes_from_body(body, mappings.TABLES))


async def webhook(request: web.Request) -> web.Response:
    source = request.app[SETTINGS].webhook_sources.get(request.match_info["source"])
    if source is None:
        raise web.HTTPNotFound(text="unknown webhook source")
    event = await _verified_json(request, source.signer)

    def changes() -> list[FactChange]:
        change = webhooks.change_from_event(event, source)
        return [] if change is None else [change]

    return await _apply(request, changes)


async def healthz(request: web.Request) -> web.Response:
    if not request.app[READY].is_set():
        raise web.HTTPServiceUnavailable(text="declaring schemas")
    return web.json_response({"status": "ok"})


async def _apply(request: web.Request, mapper: Callable[[], list[FactChange]]) -> web.Response:
    if not request.app[READY].is_set():
        raise web.HTTPServiceUnavailable(text="declaring schemas")
    try:
        changes = mapper()
        outcome = await request.app[APPLIER].apply(changes)
    except (MappingError, EngineError) as err:
        log.warning("rejected %s: %s", request.path, err)
        raise web.HTTPUnprocessableEntity(text=str(err)) from err
    except EngineUnavailable as err:
        log.warning("engine unavailable for %s: %s", request.path, err)
        raise web.HTTPServiceUnavailable(text=str(err)) from err
    return web.json_response({"applied": outcome.applied, "skipped": outcome.skipped})


async def _verified_json(request: web.Request, signer: Signer) -> Any:
    raw = await request.read()
    if not signer.verify(request.headers, raw):
        raise web.HTTPUnauthorized(text="missing, stale or bad webhook signature")
    if not raw.strip():
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as err:
        raise web.HTTPBadRequest(text=f"invalid JSON: {err}") from err


async def _declare_until_ready(app: web.Application) -> None:
    """Declare the owned schemas, retrying while the engine starts."""
    delay = 0.5
    while True:
        try:
            await app[APPLIER].declare(mappings.RELATIONS)
            app[READY].set()
            log.info("schemas declared; ready")
            return
        except (EngineUnavailable, EngineError) as err:
            log.warning("engine not ready (%s); retrying in %.1fs", err, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10.0)


def build_app(settings: config.Settings) -> web.Application:
    app = web.Application()
    app[SETTINGS] = settings
    app[READY] = asyncio.Event()
    app.router.add_post("/cdc", cdc)
    app.router.add_post("/webhooks/{source}", webhook)
    app.router.add_get("/healthz", healthz)

    async def lifecycle(app: web.Application) -> Any:
        async with aiohttp.ClientSession() as http:
            app[APPLIER] = Applier(http, settings.engine)
            declaring = asyncio.create_task(_declare_until_ready(app))
            yield
            declaring.cancel()
            await app[APPLIER].close()

    app.cleanup_ctx.append(lifecycle)
    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    settings = config.from_env()
    web.run_app(build_app(settings), host=settings.host, port=settings.port, print=None)


if __name__ == "__main__":
    main()
