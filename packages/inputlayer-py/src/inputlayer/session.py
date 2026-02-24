"""Session - ephemeral facts and rules (no + prefix)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from inputlayer.compiler import compile_bulk_insert, compile_insert, compile_rule
from inputlayer.relation import Relation

if TYPE_CHECKING:
    from inputlayer.connection import Connection
    from inputlayer.derived import Derived
    from inputlayer.result import ResultSet


class Session:
    """Manage session-scoped (ephemeral) data.

    Session inserts and rules omit the ``+`` prefix, making them ephemeral
    (cleared on disconnect or KG switch).
    """

    def __init__(self, connection: Connection) -> None:
        self._conn = connection

    async def insert(self, facts: Relation | list[Relation]) -> None:
        """Insert ephemeral session facts (no + prefix)."""
        if isinstance(facts, list):
            if not facts:
                return
            datalog = compile_bulk_insert(type(facts[0]), facts, persistent=False)
        else:
            datalog = compile_insert(facts, persistent=False)
        await self._conn.execute(datalog)

    async def define_rules(self, *targets: type[Derived]) -> None:
        """Define session-scoped rules (no + prefix)."""
        from inputlayer.derived import Derived

        for target in targets:
            head_name = Relation._resolve_name(target)
            head_columns = Relation._get_columns(target)
            for clause in target.rules:
                datalog = compile_rule(
                    head_name,
                    head_columns,
                    clause.select_map,
                    clause.relations,
                    clause.condition,
                    persistent=False,
                )
                await self._conn.execute(datalog)

    async def list_rules(self) -> list[str]:
        """List session rules."""
        result = await self._conn.execute(".session list")
        return [row[0] for row in result.rows] if result.rows else []

    async def drop_rule(
        self,
        name: str | None = None,
        *,
        index: int | None = None,
    ) -> None:
        """Drop a session rule by name, or a specific clause by index."""
        if name and index is not None:
            await self._conn.execute(f".session remove {name} {index}")
        elif name:
            await self._conn.execute(f".session drop {name}")
        else:
            raise ValueError("Must provide rule name")

    async def clear(self) -> None:
        """Clear all session facts and rules."""
        await self._conn.execute(".session clear")
