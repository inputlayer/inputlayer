"""LangGraph integration for InputLayer.

Provides:
- kg_node: Factory for KG query/mutation graph nodes
- kg_router: Conditional edge routing based on Datalog queries
- InputLayerState: TypedDict mixin with KG handle
"""

from inputlayer.integrations.langgraph.nodes import kg_node
from inputlayer.integrations.langgraph.router import kg_router
from inputlayer.integrations.langgraph.state import InputLayerState

__all__ = ["InputLayerState", "kg_node", "kg_router"]
