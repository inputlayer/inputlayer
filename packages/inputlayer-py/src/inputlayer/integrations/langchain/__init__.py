"""LangChain integration for InputLayer.

Provides:
- InputLayerRetriever: BaseRetriever that queries a KnowledgeGraph
- InputLayerTool: BaseTool for agent-based KG queries
"""

from inputlayer.integrations.langchain.retriever import InputLayerRetriever
from inputlayer.integrations.langchain.tool import InputLayerTool

__all__ = ["InputLayerRetriever", "InputLayerTool"]
