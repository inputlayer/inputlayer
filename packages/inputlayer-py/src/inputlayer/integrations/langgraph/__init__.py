"""LangGraph integration for InputLayer.

Provides:
- kg_node: Factory for KG query/mutation graph nodes
- kg_router: Conditional edge routing based on IQL queries
- InputLayerState: TypedDict mixin with KG handle
- InputLayerCheckpointer: Checkpoint persistence backed by an InputLayer KG,
  with ``prune_thread()``/``prune()`` for storage management
- InputLayerMemory: Semantic long-term memory backed by a KG
- escape_iql: String escaping for safe IQL interpolation

``escape_iql``, ``kg_node``, ``kg_router``, and ``InputLayerState`` are
always available. ``InputLayerCheckpointer`` and ``InputLayerMemory``
require the ``langgraph`` optional dependency group.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from inputlayer.integrations.langgraph._utils import escape_iql
from inputlayer.integrations.langgraph.nodes import kg_node
from inputlayer.integrations.langgraph.router import kg_router
from inputlayer.integrations.langgraph.state import InputLayerState

if TYPE_CHECKING:
    from inputlayer.integrations.langgraph.checkpointer import InputLayerCheckpointer
    from inputlayer.integrations.langgraph.memory import InputLayerMemory

__all__ = [
    "InputLayerCheckpointer",
    "InputLayerMemory",
    "InputLayerState",
    "escape_iql",
    "kg_node",
    "kg_router",
]


def __getattr__(name: str) -> object:
    """Lazy-import heavy classes that require langchain_core/langgraph."""
    if name == "InputLayerCheckpointer":
        from inputlayer.integrations.langgraph.checkpointer import InputLayerCheckpointer
        return InputLayerCheckpointer
    if name == "InputLayerMemory":
        from inputlayer.integrations.langgraph.memory import InputLayerMemory
        return InputLayerMemory
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
