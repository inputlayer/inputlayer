"""Relation base class - user-facing schema definition via Pydantic models."""

from __future__ import annotations

from typing import Any, ClassVar, get_type_hints

from pydantic import BaseModel, ConfigDict
from pydantic._internal._model_construction import ModelMetaclass

from inputlayer._naming import camel_to_snake
from inputlayer._proxy import ColumnProxy, RelationRef


class _RelationMeta(ModelMetaclass):
    """Metaclass that adds ColumnProxy attribute access to Relation subclasses.

    When you write ``Employee.name`` on the class (not an instance), this
    metaclass intercepts the lookup and returns a ColumnProxy for query building.
    Pydantic's own ``ModelMetaclass.__getattr__`` blocks this, so we override it.
    """

    def __getattr__(cls, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        # model_fields is a dict after Pydantic model construction.
        # During construction it may be a descriptor, so guard carefully.
        try:
            fields = super().__getattribute__("model_fields")
        except AttributeError:
            fields = None
        if isinstance(fields, dict) and name in fields:
            rel_name = _resolve_name(cls)
            return ColumnProxy(rel_name, name)
        raise AttributeError(f"type object '{cls.__name__}' has no attribute '{name}'")


def _resolve_name(cls: type) -> str:
    """Get the Datalog relation name for a Relation subclass."""
    rn = getattr(cls, "__relation_name__", None)
    if rn is not None:
        return rn
    return camel_to_snake(cls.__name__)


class Relation(BaseModel, metaclass=_RelationMeta):
    """Base class for all InputLayer relations.

    Subclass this with typed fields to define a relation schema::

        class Employee(Relation):
            id: int
            name: str
            department: str
            salary: float
            active: bool

    Column proxy access:
        ``Employee.name`` returns a ``ColumnProxy`` for query building.
    """

    model_config = ConfigDict(frozen=True)

    __relation_name__: ClassVar[str | None] = None

    # ── Class-level introspection ─────────────────────────────────────

    @classmethod
    def _resolve_name(cls, relation_cls: type[Relation] | None = None) -> str:
        """Get the Datalog relation name for a Relation subclass."""
        target = relation_cls or cls
        return _resolve_name(target)

    @classmethod
    def _get_columns(cls, relation_cls: type[Relation] | None = None) -> list[str]:
        """Get ordered column names (excludes Pydantic internals)."""
        target = relation_cls or cls
        return list(target.model_fields.keys())

    @classmethod
    def _get_column_types(cls, relation_cls: type[Relation] | None = None) -> dict[str, type]:
        """Get column name → Python type mapping."""
        target = relation_cls or cls
        hints = get_type_hints(target)
        return {k: hints[k] for k in target.model_fields}

    # ── Self-join support ─────────────────────────────────────────────

    @classmethod
    def refs(cls, n: int) -> tuple[RelationRef, ...]:
        """Create n independent references for self-joins.

        Usage::

            r1, r2 = Follow.refs(2)
            kg.query(r1.follower, r2.followee,
                     join=[r1, r2],
                     on=lambda a, b: a.followee == b.follower)
        """
        return tuple(
            RelationRef(cls, f"{_resolve_name(cls)}_{i}")
            for i in range(1, n + 1)
        )
