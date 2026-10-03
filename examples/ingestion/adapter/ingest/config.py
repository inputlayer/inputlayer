"""Adapter settings, read once from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass

from . import mappings
from .signing import Signer
from .webhooks import WebhookSource


@dataclass(frozen=True, slots=True)
class EngineSettings:
    url: str
    """WebSocket endpoint, e.g. `ws://inputlayer:8080/ws`."""
    api_key: str
    knowledge_graph: str


@dataclass(frozen=True, slots=True)
class Settings:
    engine: EngineSettings
    host: str
    port: int
    cdc_signer: Signer
    webhook_sources: dict[str, WebhookSource]


def engine_from_env() -> EngineSettings:
    return EngineSettings(
        url=os.environ.get("ENGINE_URL", "ws://localhost:8080/ws"),
        api_key=_required("ENGINE_API_KEY"),
        knowledge_graph=os.environ.get("ENGINE_KG", "shop"),
    )


def from_env() -> Settings:
    """Settings for the adapter service; every write endpoint needs its secret.

    Secrets are Standard Webhooks `whsec_...` strings: `CDC_WEBHOOK_SECRET` for
    Debezium, `WEBHOOK_SECRET_BILLING` for webhook source `billing`, and so on.
    """
    sources = {
        name: WebhookSource(
            signer=Signer(_required(f"WEBHOOK_SECRET_{name.upper()}")),
            revision_field=mappings.WEBHOOK_REVISION_FIELDS[name],
            events=events,
        )
        for name, events in mappings.WEBHOOK_EVENTS.items()
    }
    return Settings(
        engine=engine_from_env(),
        host=os.environ.get("ADAPTER_HOST", "0.0.0.0"),
        port=int(os.environ.get("ADAPTER_PORT", "8090")),
        cdc_signer=Signer(_required("CDC_WEBHOOK_SECRET")),
        webhook_sources=sources,
    )


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"environment variable {name} is required")
    return value
