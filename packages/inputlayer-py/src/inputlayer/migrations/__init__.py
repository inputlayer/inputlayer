"""InputLayer migration system - Django-style schema versioning."""

from __future__ import annotations

from typing import Any

from inputlayer.migrations.operations import (
    CreateIndex,
    CreateRelation,
    CreateRule,
    DropIndex,
    DropRelation,
    DropRule,
    Operation,
    ReplaceRule,
    RunIQL,
    operation_from_dict,
)
from inputlayer.migrations.state import ModelState


class Migration:
    """Base class for migration files.

    Subclass as ``M`` in each migration file::

        class M(Migration):
            dependencies = ["0001_initial"]
            operations = [ops.CreateRelation(...)]
            state = {"relations": {...}, "rules": {...}, "indexes": {}}
    """

    dependencies: list[str] = []  # noqa: RUF012
    operations: list[Operation] = []  # noqa: RUF012
    state: dict[str, Any] = {}  # noqa: RUF012


__all__ = [
    "CreateIndex",
    "CreateRelation",
    "CreateRule",
    "DropIndex",
    "DropRelation",
    "DropRule",
    "Migration",
    "ModelState",
    "Operation",
    "ReplaceRule",
    "RunIQL",
    "operation_from_dict",
]
