"""kg_node: factory for LangGraph-compatible KG query/mutation nodes."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any, Literal

from inputlayer.integrations.langgraph._utils import escape_iql


def kg_node(
    *,
    query: str | Callable[[dict[str, Any]], str] | None = None,
    relation: type | None = None,
    operation: Literal["query", "insert", "delete"] = "query",
    state_key: str = "results",
    kg_key: str = "kg",
) -> Callable[[dict[str, Any]], Coroutine[Any, Any, dict[str, Any]]]:
    """Create a LangGraph node that queries or mutates the KnowledgeGraph.

    The returned async function has the signature ``(state) -> partial_state``
    expected by LangGraph's ``StateGraph``.

    **Query mode** (default)::

        search = kg_node(
            query="?article(Id, Title, Content, Cat, Emb)",
            state_key="articles",
        )
        graph.add_node("search", search)

    **Parameterized query.** The query can be a callable that reads state.
    Always escape user-supplied values with ``escape_iql()``::

        from inputlayer.integrations.langgraph import escape_iql

        search = kg_node(
            query=lambda s: f'?article(Id, T, C, "{escape_iql(s["category"])}", E)',
            state_key="articles",
        )

    **Insert mode.** Reads data from state and inserts into the KG::

        store = kg_node(
            relation=Finding,
            operation="insert",
            state_key="new_findings",
        )

    Args:
        query: IQL query string, or a callable ``(state) -> str``.
        relation: Relation class for insert/delete operations.
        operation: One of "query", "insert", "delete".
        state_key: State key to read from (insert/delete) or write to (query).
        kg_key: State key where the KnowledgeGraph handle lives. Must be
            present in state when the node executes.

    Returns:
        An async function compatible with ``StateGraph.add_node()``.
    """
    if operation == "query" and query is None:
        raise ValueError("Must provide 'query' for query operations")
    if operation in ("insert", "delete") and relation is None:
        raise ValueError(f"Must provide 'relation' for {operation} operations")

    async def _node(state: dict[str, Any]) -> dict[str, Any]:
        if kg_key not in state:
            raise KeyError(
                f"kg_node requires state['{kg_key}'] to be a KnowledgeGraph handle, "
                f"but '{kg_key}' was not found in state. "
                f"Add the KG handle to your state dict or change kg_key= to match "
                f"the key you're using."
            )
        kg = state[kg_key]

        if operation == "query":
            q = query(state) if callable(query) else query
            result = await kg.execute(q)
            return {
                state_key: {
                    "columns": result.columns,
                    "rows": result.rows,
                    "row_count": result.row_count,
                }
            }

        elif operation == "insert":
            data = state.get(state_key)
            if not data:
                return {}
            if isinstance(data, list):
                if isinstance(data[0], dict):
                    # List of plain dicts: insert via (relation_class, list_of_dicts)
                    await kg.insert(relation, data)
                else:
                    # List of Relation instances: insert directly, relation param unused
                    await kg.insert(data)
            elif isinstance(data, dict):
                await kg.insert(relation, data)
            return {}

        elif operation == "delete":
            data = state.get(state_key)
            if data is None:
                return {}
            if isinstance(data, list):
                for item in data:
                    await kg.delete(item)
            else:
                await kg.delete(data)
            return {}

        return {}

    # Preserve useful metadata for debugging
    label: str = operation
    if operation == "query" and isinstance(query, str):
        label = f"query: {query[:50]}"
    elif operation in ("insert", "delete") and relation is not None:
        label = f"{operation}: {relation.__name__}"
    _node.__name__ = f"kg_{label}"
    _node.__qualname__ = f"kg_{label}"

    return _node


# Re-export so callers can do: from inputlayer.integrations.langgraph import escape_iql
__all__ = ["escape_iql", "kg_node"]
