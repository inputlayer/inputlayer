"""LangChain integration for InputLayer.

Provides:
- ``InputLayerVectorStore``: LangChain VectorStore backed by a Relation.
- ``InputLayerRetriever``: BaseRetriever with embeddings or IQL modes.
- ``tools_from_relations``: structured tools generated from Relation schemas.
- ``InputLayerIQLTool``: raw InputLayer Query Language tool (escape hatch).
- ``bind_params`` / ``iql_literal``: safe IQL parameter binding.
"""

from inputlayer.integrations.langchain.params import bind_params, iql_literal
from inputlayer.integrations.langchain.retriever import InputLayerRetriever
from inputlayer.integrations.langchain.tool import (
    InputLayerIQLTool,
    tools_from_relations,
)
from inputlayer.integrations.langchain.vector_store import InputLayerVectorStore

__all__ = [
    "InputLayerIQLTool",
    "InputLayerRetriever",
    "InputLayerVectorStore",
    "bind_params",
    "iql_literal",
    "tools_from_relations",
]
