"""LangGraph integration for InputLayer.

Provides:
- kg_node: Factory for KG query/mutation graph nodes
- kg_router: Conditional edge routing based on Datalog queries
- InputLayerState: TypedDict mixin with KG handle
- InputLayerCheckpointer: BaseCheckpointSaver backed by an InputLayer KG
"""

from inputlayer.integrations.langgraph.checkpointer import InputLayerCheckpointer
from inputlayer.integrations.langgraph.nodes import kg_node
from inputlayer.integrations.langgraph.router import kg_router
from inputlayer.integrations.langgraph.state import InputLayerState

__all__ = [
    "InputLayerCheckpointer",
    "InputLayerState",
    "kg_node",
    "kg_router",
]
