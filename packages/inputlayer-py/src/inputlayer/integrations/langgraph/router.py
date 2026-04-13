"""kg_router: conditional edge routing based on IQL query results."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def kg_router(
    *,
    branches: dict[str, str],
    default: str = "end",
    kg_key: str = "kg",
) -> Callable[[dict[str, Any]], Any]:
    """Create a LangGraph conditional edge function driven by IQL queries.

    Each branch maps a target node name to an IQL query. The first branch
    whose query returns non-empty results wins. If no branch matches, the
    ``default`` is returned.

    This lets the KG's derived facts control the graph's execution path.
    Routing decisions are declarative rules, not imperative Python.

    Usage::

        route = kg_router(
            branches={
                "answer": "?ready_to_answer(X)",
                "gather": "?missing_info(X)",
                "escalate": "?needs_human(X)",
            },
            default="gather",
        )
        graph.add_conditional_edges("reason", route)

    **Parameterized branches.** Queries can reference state values by using
    a callable instead of a string::

        route = kg_router(
            branches={
                "found": lambda s: f'?result("{s["query"]}", X)',
                "not_found": "?empty_result(X)",
            },
        )

    Args:
        branches: Mapping of ``{target_node: iql_query}``. Queries can be
            strings or callables ``(state) -> str``.
        default: Node to route to if no branch matches.
        kg_key: State key where the KnowledgeGraph handle lives.

    Returns:
        An async function compatible with ``add_conditional_edges()``.
    """
    if not branches:
        raise ValueError("Must provide at least one branch")

    async def _router(state: dict[str, Any]) -> str:
        kg = state[kg_key]

        for target, query in branches.items():
            q = query(state) if callable(query) else query
            result = await kg.execute(q)
            if result.rows:
                return target

        return default

    _router.__name__ = "kg_router"
    _router.__qualname__ = "kg_router"

    return _router
