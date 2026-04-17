"""kg_node: factory for LangGraph-compatible KG query/mutation nodes."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any, Literal

from inputlayer.integrations.langgraph._utils import check_error_response
from inputlayer.relation import Relation


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

    **Insert mode.** Reads data from state and inserts into the KG.
    Accepts a list of dicts, list of Relation instances, a single dict,
    or a single Relation instance::

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
    if operation not in ("query", "insert", "delete"):
        raise ValueError(
            f"kg_node: operation must be 'query', 'insert', or 'delete', "
            f"got {operation!r}."
        )
    if operation == "query" and query is None:
        raise ValueError("Must provide 'query' for query operations")
    if operation in ("insert", "delete") and relation is None:
        raise ValueError(f"Must provide 'relation' for {operation} operations")
    if operation == "query" and relation is not None:
        import warnings

        warnings.warn(
            "kg_node: 'relation' is ignored in query mode. "
            "Did you mean operation='insert' or operation='delete'?",
            UserWarning,
            stacklevel=2,
        )
    if operation in ("insert", "delete") and query is not None:
        import warnings

        warnings.warn(
            f"kg_node: 'query' is ignored in {operation} mode. "
            "Did you mean operation='query'?",
            UserWarning,
            stacklevel=2,
        )

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
            if q is None or q == "":
                raise ValueError(
                    "kg_node: query callable returned "
                    f"{q!r}. Expected a non-empty IQL string. "
                    "Make sure your query function returns a valid query "
                    "for all reachable states."
                )
            result = await kg.execute(q)
            check_error_response(result, "kg_node", q)
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
                first = data[0]
                if isinstance(first, dict):
                    await kg.insert(relation, data)
                elif isinstance(first, Relation):
                    await kg.insert(data)
                else:
                    raise TypeError(
                        f"kg_node insert: state['{state_key}'] is a list whose first "
                        f"item is {type(first).__name__}. Expected a list of dicts or "
                        "a list of Relation instances."
                    )
            elif isinstance(data, dict):
                await kg.insert(relation, data)
            elif isinstance(data, Relation):
                await kg.insert([data])
            else:
                raise TypeError(
                    f"kg_node insert: state['{state_key}'] must be a dict, list of dicts, "
                    f"Relation instance, or list of Relation instances, "
                    f"got {type(data).__name__}"
                )
            return {}

        elif operation == "delete":
            data = state.get(state_key)
            if not data:
                return {}
            if isinstance(data, list):
                for i, item in enumerate(data):
                    if not isinstance(item, Relation):
                        raise TypeError(
                            f"kg_node delete: state['{state_key}'][{i}] must be a "
                            f"Relation instance, got {type(item).__name__}."
                        )
                    await kg.delete(item)
            elif isinstance(data, Relation):
                await kg.delete(data)
            else:
                raise TypeError(
                    f"kg_node delete: state['{state_key}'] must be a Relation "
                    f"instance or list of Relation instances, got {type(data).__name__}."
                )
            return {}

        else:
            raise ValueError(
                f"kg_node: unknown operation {operation!r}. "
                f"Expected 'query', 'insert', or 'delete'."
            )

    # Preserve useful metadata for debugging
    label: str = operation
    if operation == "query" and isinstance(query, str):
        label = f"query: {query[:50]}"
    elif operation in ("insert", "delete") and relation is not None:
        name = getattr(relation, "__name__", None) or str(relation)
        label = f"{operation}: {name}"
    _node.__name__ = f"kg_{label}"
    _node.__qualname__ = f"kg_{label}"

    return _node


# Re-export so callers can do: from inputlayer.integrations.langgraph import escape_iql
__all__ = ["kg_node"]
