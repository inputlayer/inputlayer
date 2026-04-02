"""InputLayerState — TypedDict with KG handle for LangGraph state."""

from __future__ import annotations

from typing import Any, TypedDict


class InputLayerState(TypedDict, total=False):
    """Base state for LangGraph graphs that use InputLayer.

    Extend this with your own fields::

        class MyState(InputLayerState):
            question: str
            context: list[str]
            answer: str

    The ``kg`` field carries the KnowledgeGraph handle through the graph.
    Nodes created by ``kg_node()`` read it automatically.
    """

    kg: Any  # KnowledgeGraph handle — typed as Any for flexibility
    results: list[Any]
