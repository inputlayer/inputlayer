"""kg_router: conditional edge routing based on IQL query results."""

from __future__ import annotations

import logging
from collections.abc import Callable, Coroutine
from typing import Any

logger = logging.getLogger(__name__)


def kg_router(
    *,
    branches: dict[str, str | Callable[[dict[str, Any]], str]],
    default: str = "end",
    kg_key: str = "kg",
) -> Callable[[dict[str, Any]], Coroutine[Any, Any, str]]:
    """Create a LangGraph conditional edge function driven by IQL queries.

    Each branch maps a target node name to an IQL query. Branches are
    evaluated in insertion order (Python dict ordering, guaranteed since
    Python 3.7). The first branch whose query returns non-empty results
    wins. If no branch matches, ``default`` is returned.

    Exceptions from individual branch queries are caught and logged as
    warnings; that branch is skipped and evaluation continues to the next.
    This prevents a single failing query from crashing the entire graph.

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
    a callable instead of a string. Always escape user-supplied values::

        from inputlayer.integrations.langgraph import escape_iql

        route = kg_router(
            branches={
                "found": lambda s: f'?result("{escape_iql(s["query"])}", X)',
                "not_found": "?empty_result(X)",
            },
        )

    Args:
        branches: Mapping of ``{target_node: iql_query}``. Queries can be
            strings or callables ``(state) -> str``. Evaluated in insertion
            order; first match wins.
        default: Node to route to if no branch matches.
        kg_key: State key where the KnowledgeGraph handle lives. Must be
            present in state when the router executes.

    Returns:
        An async function compatible with ``add_conditional_edges()``.
    """
    if not branches:
        raise ValueError("Must provide at least one branch")

    async def _router(state: dict[str, Any]) -> str:
        if kg_key not in state:
            raise KeyError(
                f"kg_router requires state['{kg_key}'] to be a KnowledgeGraph handle, "
                f"but '{kg_key}' was not found in state. "
                f"Add the KG handle to your state dict or change kg_key= to match "
                f"the key you're using."
            )
        kg = state[kg_key]

        for target, query in branches.items():
            try:
                q = query(state) if callable(query) else query
                result = await kg.execute(q)
                if result.rows:
                    return target
            except Exception as exc:
                logger.warning(
                    "kg_router: branch %r query raised %s: %s - skipping to next branch",
                    target,
                    type(exc).__name__,
                    exc,
                )

        return default

    _router.__name__ = "kg_router"
    _router.__qualname__ = "kg_router"

    return _router
