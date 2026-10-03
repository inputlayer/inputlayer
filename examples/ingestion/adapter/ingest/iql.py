"""Render adapter changes as IQL programs. Pure functions, no I/O.

A batch of changes becomes one program, which the engine commits as one
transaction: the facts and their revision watermarks become visible together.
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
    """One revision-conditional transaction for all changes."""
    statements = []
    for change in changes:
        relation, key = change.identity
        prefix = f"{REVISIONS.name}({literal(relation)}, {literal(key)}, "
        initial = prefix + "-1)"
        stored = prefix + "Revision)"
        guard = f"{stored}, Revision < {change.revision}"
        statements.append(f"+{initial}")
        statements.append(f"-{initial} <- {stored}, Revision >= 0")
        atom = _key_atom(change.relation, change.key)
        statements.append(f"-{atom} <- {atom}, {guard}")
        heads = [f"-{stored}"]
        if change.row is not None:
            heads.append(_assert(change.relation.name, change.row))
        heads.append(_assert(REVISIONS.name, (relation, key, change.revision)))
        statements.append(f"{', '.join(heads)} <- {guard}")
    return "\n".join(statements)


def _assert(relation: str, row: tuple[Value, ...]) -> str:
    return f"+{relation}(" + ", ".join(literal(v) for v in row) + ")"


def _key_atom(relation: Relation, key: tuple[Value, ...]) -> str:
    keyed = dict(zip(relation.key, key, strict=True))
    args = ", ".join(
        literal(keyed[name]) if name in keyed else f"V{i}"
        for i, name in enumerate(relation.column_names)
    )
    return f"{relation.name}({args})"
