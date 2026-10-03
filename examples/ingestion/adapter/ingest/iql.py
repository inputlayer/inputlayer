"""Render adapter changes as IQL programs. Pure functions, no I/O.

A batch of changes becomes one program, which the engine commits as one
transaction: the facts and their revision watermarks become visible together,
so a crash or a failed statement never leaves data without its revision (or
the reverse). Revision statements come last, so even an engine that applied
statements one at a time would only advance a revision after its data.
"""

from __future__ import annotations

import math
from collections.abc import Iterable

from .changes import FactChange, Relation, Value

REVISIONS = Relation(
    name="ingest_revision",
    columns=(("relation", "string"), ("key", "string"), ("revision", "int")),
    key=("relation", "key"),
)
"""Last applied source revision per `(relation, key)`; kept after a retraction
as a tombstone so a replayed older upsert cannot resurrect the fact."""

_ESCAPES = str.maketrans({"\\": "\\\\", '"': '\\"', "\n": "\\n", "\r": "\\r", "\t": "\\t"})


def literal(value: Value) -> str:
    """An IQL literal for one column value."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"IQL has no literal for {value!r}")
        return repr(value)
    return '"' + value.translate(_ESCAPES) + '"'


def schema_program(relations: Iterable[Relation]) -> str:
    """Persistent schema declarations; re-declaring an identical schema is a no-op."""
    declarations = [*relations, REVISIONS]
    return "\n".join(
        f"+{r.name}(" + ", ".join(f"{name}: {t}" for name, t in r.columns) + ")"
        for r in declarations
    )


def revision_query(change: FactChange) -> str:
    """Query returning the stored revision for the change's key (zero or one row)."""
    relation, key = change.identity
    return f"?{REVISIONS.name}({literal(relation)}, {literal(key)}, Revision)"


def apply_program(changes: list[FactChange]) -> str:
    """One program that applies every change and records its revision."""
    statements = []
    for change in changes:
        statements.append(_retract_key(change.relation, change.key))
        if change.row is not None:
            statements.append(_assert(change.relation.name, change.row))
    for change in changes:
        relation, key = change.identity
        statements.append(_retract_key(REVISIONS, (relation, key)))
        statements.append(_assert(REVISIONS.name, (relation, key, change.revision)))
    return "\n".join(statements)


def _assert(relation: str, row: tuple[Value, ...]) -> str:
    return f"+{relation}(" + ", ".join(literal(v) for v in row) + ")"


def _retract_key(relation: Relation, key: tuple[Value, ...]) -> str:
    """Conditional delete of whatever fact holds `key`, whatever its other columns."""
    keyed = dict(zip(relation.key, key, strict=True))
    args = ", ".join(
        literal(keyed[name]) if name in keyed else f"V{i}"
        for i, name in enumerate(relation.column_names)
    )
    atom = f"{relation.name}({args})"
    return f"-{atom} <- {atom}"
