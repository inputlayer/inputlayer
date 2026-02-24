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
    RunDatalog,
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

    dependencies: list[str] = []
    operations: list[Operation] = []
    state: dict[str, Any] = {}


__all__ = [
    "Migration",
    "ModelState",
    "CreateRelation",
    "DropRelation",
    "CreateRule",
    "DropRule",
    "ReplaceRule",
    "CreateIndex",
    "DropIndex",
    "RunDatalog",
    "Operation",
    "operation_from_dict",
]
