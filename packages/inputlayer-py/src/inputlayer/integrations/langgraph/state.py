"""InputLayerState: TypedDict with KG handle for LangGraph state."""

from __future__ import annotations

from typing import Any, TypedDict


class InputLayerState(TypedDict, total=False):
    """Base state for LangGraph graphs that use InputLayer.

    Extend this with your own fields::

        class MyState(InputLayerState):
            question: str
            context: list[str]
            answer: str

    Then pass ``kg`` when invoking the graph::

        kg = il.knowledge_graph("my_kg")
        await app.ainvoke({"kg": kg, "question": "..."})

    **Important**: ``total=False`` makes all fields optional at the type-checker
    level so that subclass fields can also be optional. In practice, ``kg``
    is *always required* - every node that touches InputLayer reads it from
    state, and ``kg_node()``/``kg_router()`` will raise ``KeyError`` if it is
    missing.  Always include ``"kg": kg`` in your initial state dict.

    The ``kg`` field carries the KnowledgeGraph handle through the graph.
    Nodes created by ``kg_node()`` read it automatically.
    """

    kg: Any  # KnowledgeGraph handle - REQUIRED in practice despite total=False
    results: dict[str, Any]  # populated by kg_node() query: {columns, rows, row_count}
