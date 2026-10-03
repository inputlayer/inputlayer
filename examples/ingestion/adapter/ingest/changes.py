"""The adapter's canonical change record: one keyed fact upsert or retraction.

Every source (Postgres CDC, webhooks) is mapped to `FactChange`s; everything
downstream (revision gating, IQL rendering) only ever sees this type.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass

Value = int | float | bool | str
"""A fact column value. IQL has no null: mapped source columns must be set."""

COLUMN_TYPES: dict[str, type | tuple[type, ...]] = {
    "int": int,
    "float": (int, float),
    "bool": bool,
    "string": str,
}


class MappingError(ValueError):
    """A source event cannot be mapped to facts (unknown shape, null value, ...)."""


@dataclass(frozen=True, slots=True)
class Relation:
    """An InputLayer relation fed by the adapter, identified by its key columns.

    `columns` is the ordered `(name, iql_type)` schema; `key` names the columns
    whose values identify one fact, like a primary key: an upsert replaces the
    fact with the same key and a retraction removes it.
    """

    name: str
    columns: tuple[tuple[str, str], ...]
    key: tuple[str, ...]

    def __post_init__(self) -> None:
        names = self.column_names
        unknown_types = [t for _, t in self.columns if t not in COLUMN_TYPES]
        if unknown_types:
            raise ValueError(f"{self.name}: unsupported column types {unknown_types}")
        if not self.key or any(k not in names for k in self.key):
            raise ValueError(f"{self.name}: key {self.key} must name columns of {names}")

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.columns)

    def row_from(self, record: Mapping[str, object]) -> tuple[Value, ...]:
        """The fact for `record`, taking each column from the field of the same name."""
        return tuple(self._value(record, name, iql_type) for name, iql_type in self.columns)

    def key_from(self, record: Mapping[str, object]) -> tuple[Value, ...]:
        """The key values of `record` (other columns may be absent or null)."""
        types = dict(self.columns)
        return tuple(self._value(record, name, types[name]) for name in self.key)

    def _value(self, record: Mapping[str, object], column: str, iql_type: str) -> Value:
        if column not in record:
            raise MappingError(f"{self.name}: field '{column}' missing from source record")
        value = record[column]
        if value is None:
            raise MappingError(f"{self.name}: field '{column}' is null; IQL facts have no null")
        expected = COLUMN_TYPES[iql_type]
        # bool is an int subclass in Python: never accept it for a numeric column.
        is_bool_mismatch = isinstance(value, bool) and iql_type != "bool"
        if is_bool_mismatch or not isinstance(value, expected):
            raise MappingError(
                f"{self.name}: field '{column}' = {value!r} is not an IQL {iql_type}"
            )
        if iql_type == "float":
            number = float(value)  # type: ignore[arg-type]
            if not math.isfinite(number):
                raise MappingError(f"{self.name}: field '{column}' = {value!r} is not finite")
            return number
        return value  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class FactChange:
    """Set `relation`'s fact for `key` to `row`, or retract it when `row` is None.

    `revision` orders changes to the same key (a Postgres LSN, a webhook
    sequence number). A change is applied only when its revision is greater than
    the last one applied for the key, which makes replays and late,
    out-of-order deliveries no-ops.
    """

    relation: Relation
    key: tuple[Value, ...]
    revision: int
    row: tuple[Value, ...] | None

    @property
    def identity(self) -> tuple[str, str]:
        """`(relation, encoded key)`: the unit that revisions are tracked for."""
        return (self.relation.name, encode_key(self.key))


def encode_key(key: tuple[Value, ...]) -> str:
    """A canonical string for a key; `1` and `"1"` stay distinct."""
    return json.dumps(list(key), separators=(",", ":"), ensure_ascii=False)


def latest_per_key(changes: list[FactChange]) -> list[FactChange]:
    """Keep only the highest-revision change per key, in first-seen key order."""
    latest: dict[tuple[str, str], FactChange] = {}
    for change in changes:
        current = latest.get(change.identity)
        if current is None or change.revision > current.revision:
            latest[change.identity] = change
    return list(latest.values())
