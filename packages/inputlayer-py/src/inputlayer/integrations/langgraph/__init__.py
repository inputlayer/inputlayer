"""LangGraph integration for InputLayer.

Provides:
- kg_node: Factory for KG query/mutation graph nodes
- kg_router: Conditional edge routing based on IQL queries
- InputLayerState: TypedDict mixin with KG handle
- InputLayerCheckpointer: Checkpoint persistence backed by an InputLayer KG,
  with ``aprune()``/``prune()`` for storage management
- InputLayerMemory: Semantic long-term memory backed by a KG
- escape_iql: String escaping for safe IQL interpolation
"""

from inputlayer.integrations.langgraph._utils import escape_iql
from inputlayer.integrations.langgraph.checkpointer import InputLayerCheckpointer
from inputlayer.integrations.langgraph.memory import InputLayerMemory
from inputlayer.integrations.langgraph.nodes import kg_node
from inputlayer.integrations.langgraph.router import kg_router
from inputlayer.integrations.langgraph.state import InputLayerState

__all__ = [
    "InputLayerCheckpointer",
    "InputLayerMemory",
    "InputLayerState",
    "escape_iql",
    "kg_node",
    "kg_router",
]
