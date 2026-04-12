"""LangChain integration for InputLayer.

Provides:
- ``InputLayerVectorStore``: LangChain VectorStore backed by a Relation.
- ``InputLayerRetriever``: BaseRetriever with embeddings or IQL modes.
- ``tools_from_relations``: structured tools generated from Relation schemas.
- ``InputLayerIQLTool``: raw InputLayer Query Language tool (escape hatch).
- ``bind_params`` / ``iql_literal``: safe IQL parameter binding.

Requires the ``langchain`` extra::

    pip install inputlayer-client-dev[langchain]
"""

try:
    import langchain_core  # noqa: F401
except ImportError as exc:
    raise ImportError(
        "The LangChain integration requires langchain-core. "
        "Install it with: pip install inputlayer-client-dev[langchain]"
    ) from exc

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
