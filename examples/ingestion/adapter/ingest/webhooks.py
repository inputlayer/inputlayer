"""Map signed SaaS webhook events to fact changes.

A webhook source posts JSON events such as
`{"id": "evt_1", "type": "payment.failed", "sequence": 7, "data": {...}}`.
`type` selects the mapping, `data` holds the fact's fields, and the
`revision_field` (a per-object version or sequence the SaaS provides) orders
deliveries for the same object. Requests are signed per Standard Webhooks
(see signing.py). Event types without a mapping are ignored:
SaaS products send many event types a knowledge graph does not need.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .changes import FactChange, MappingError, Relation
from .signing import Signer


@dataclass(frozen=True, slots=True)
class EventMapping:
    """An event type sets (`retract=False`) or retracts the fact for its key."""

    relation: Relation
    retract: bool = False


@dataclass(frozen=True, slots=True)
class WebhookSource:
    """One SaaS sender: its signature verifier and the event types it maps."""

    signer: Signer
    revision_field: str
    events: dict[str, EventMapping]


def change_from_event(event: Any, source: WebhookSource) -> FactChange | None:
    """The fact change for one event, or None when its type is not mapped."""
    if not isinstance(event, dict):
        raise MappingError("webhook event must be a JSON object")
    mapping = source.events.get(str(event.get("type")))
    if mapping is None:
        return None
    revision = event.get(source.revision_field)
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise MappingError(
            f"webhook field '{source.revision_field}' must be a non-negative integer,"
            f" got {revision!r}"
        )
    data = event.get("data")
    if not isinstance(data, dict):
        raise MappingError("webhook event has no 'data' object")
    relation = mapping.relation
    row = None if mapping.retract else relation.row_from(data)
    return FactChange(relation, relation.key_from(data), revision, row)
