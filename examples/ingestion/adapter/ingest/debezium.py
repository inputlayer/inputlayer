"""Map Debezium change events (Postgres connector) to fact changes.

Accepts the plain envelope that Debezium Server's HTTP sink posts with
`debezium.format.value.schemas.enable=false`, a list of them, or the
`{"schema": ..., "payload": envelope}` form that Kafka Connect's JSON converter
produces with schemas enabled. The revision is the change's Postgres LSN
(`source.lsn`), which increases with every change to the same row.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .changes import FactChange, MappingError, Relation

_UPSERT_OPS = {"r", "c", "u"}  # snapshot read, create, update
_DELETE_OP = "d"


def changes_from_body(body: Any, tables: Mapping[str, Relation]) -> list[FactChange]:
    """All fact changes in a request body (one envelope or a list of them)."""
    events = body if isinstance(body, list) else [body]
    changes = (change_from_event(event, tables) for event in events)
    return [change for change in changes if change is not None]


def change_from_event(event: Any, tables: Mapping[str, Relation]) -> FactChange | None:
    """The fact change for one envelope, or None for events that carry none."""
    if event is None:
        return None  # a delete tombstone: the preceding "d" event did the work
    if not isinstance(event, dict):
        raise MappingError(f"Debezium event must be a JSON object, got {type(event).__name__}")
    if "op" not in event and isinstance(event.get("payload"), dict):
        event = event["payload"]
    op = event.get("op")
    if op not in _UPSERT_OPS and op != _DELETE_OP:
        # "t" (truncate) and "m" (logical message) cannot be mapped to keyed changes;
        # Debezium skips truncates by default (skipped.operations=t).
        raise MappingError(f"unsupported Debezium operation {op!r}")

    source = event.get("source")
    if not isinstance(source, dict) or "lsn" not in source:
        raise MappingError("Debezium event has no source.lsn")
    table = f"{source.get('schema')}.{source.get('table')}"
    relation = tables.get(table)
    if relation is None:
        raise MappingError(f"no relation is mapped for table {table}")
    revision = _revision(source["lsn"])

    if op == _DELETE_OP:
        before = _record(event, "before")
        return FactChange(relation, relation.key_from(before), revision, row=None)
    after = _record(event, "after")
    return FactChange(relation, relation.key_from(after), revision, relation.row_from(after))


def _record(event: Mapping[str, Any], field: str) -> Mapping[str, Any]:
    record = event.get(field)
    if not isinstance(record, dict):
        raise MappingError(f"Debezium '{event.get('op')}' event has no '{field}' record")
    return record


def _revision(lsn: Any) -> int:
    if isinstance(lsn, bool) or not isinstance(lsn, int) or lsn < 0:
        raise MappingError(f"source.lsn must be a non-negative integer, got {lsn!r}")
    return lsn
