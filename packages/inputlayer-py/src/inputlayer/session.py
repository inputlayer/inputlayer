"""Session - ephemeral facts and rules (no + prefix)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from inputlayer import _meta
from inputlayer._literal import collect_params, params_of
from inputlayer.compiler import compile_insert, compile_rule_clause
from inputlayer.relation import Relation

if TYPE_CHECKING:
    from inputlayer.connection import Connection
    from inputlayer.derived import Derived


class Session:
    """Manage session-scoped (ephemeral) data.

    Session inserts and rules omit the ``+`` prefix, making them ephemeral
    (cleared on disconnect or KG switch).
    """

    def __init__(self, connection: Connection) -> None:
        self._conn = connection

    async def insert(self, facts: Relation | list[Relation]) -> None:
        """Insert ephemeral session facts (no + prefix), one program per fact.

        The engine has no bulk form for session facts, and it keeps session
        facts only from a single-statement program: in a multi-statement
        program they last only for that request.
        """
        batch = facts if isinstance(facts, list) else [facts]
        for fact in batch:
            with collect_params() as params:
                iql = compile_insert(fact, persistent=False)
            await self._conn.execute(iql, params=params)

    async def define_rules(self, *targets: type[Derived]) -> None:
        """Define session-scoped rules (no + prefix), one program per clause.

        The engine keeps session rules only from a single-statement program:
        in a multi-statement program they last only for that request.
        """
        for target in targets:
            head_name = Relation._resolve_name(target)
            head_columns = Relation._get_columns(target)
            for clause in target.rules:
                with collect_params() as params:
                    compiled = compile_rule_clause(
                        head_name,
                        head_columns,
                        clause.select_map,
                        clause.relations,
                        clause.condition,
                        persistent=False,
                    )
                # Constants a negated atom binds through are session facts too.
                for statement in (*compiled.constants, compiled.clause):
                    await self._conn.execute(statement, params=params_of(statement, params))

    async def list_rules(self, *, timeout: float | None = None) -> list[str]:
        """List session rules, one clause per entry, in definition order."""
        result = await self._conn.execute(_meta.session_list(), timeout=timeout)
        return _meta.session_rules(result.rows)

    async def drop_rule(
        self,
        name: str | None = None,
        *,
        index: int | None = None,
    ) -> None:
        """Drop a session rule by name, or one of its clauses by index (1-based)."""
        if not name:
            raise ValueError("Must provide rule name")
        if index is None:
            await self._conn.execute(_meta.session_drop(name))
            return
        # The engine drops a clause by its position among all session
        # rules, so find that position.
        positions = [
            i
            for i, rule in enumerate(await self.list_rules(), start=1)
            if rule.split("(", 1)[0].strip() == name
        ]
        if not 1 <= index <= len(positions):
            raise IndexError(
                f"Session rule {name!r} has {len(positions)} clause(s); "
                f"index {index} is out of range"
            )
        await self._conn.execute(_meta.session_drop(positions[index - 1]))

    async def clear(self) -> None:
        """Clear all session facts and rules."""
        await self._conn.execute(_meta.session_clear())
