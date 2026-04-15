"""InputLayerState: TypedDict with KG handle for LangGraph state."""

from __future__ import annotations

import sys
from typing import Any, TypedDict

if sys.version_info >= (3, 11):
    from typing import Required
else:
    from typing_extensions import Required


class _InputLayerStateRequired(TypedDict):
    """Required fields for InputLayerState."""

    kg: Required[Any]  # KnowledgeGraph handle - REQUIRED


class InputLayerState(_InputLayerStateRequired, total=False):
    """Base state for LangGraph graphs that use InputLayer.

    Extend this with your own fields::

        class MyState(InputLayerState):
            question: str
            context: list[str]
            answer: str

    Then pass ``kg`` when invoking the graph::

        kg = il.knowledge_graph("my_kg")
        await app.ainvoke({"kg": kg, "question": "..."})

    The ``kg`` field is required and carries the KnowledgeGraph handle
    through the graph. Nodes created by ``kg_node()`` read it
    automatically. ``results`` and any subclass fields are optional.
    """

    results: dict[str, Any]  # populated by kg_node() query: {columns, rows, row_count}
