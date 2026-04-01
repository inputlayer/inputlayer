"""InputLayerTool - LangChain BaseTool for agent-based KG queries."""

from __future__ import annotations

from typing import Any

from langchain_core.callbacks import (
    AsyncCallbackManagerForToolRun,
    CallbackManagerForToolRun,
)
from langchain_core.tools import BaseTool
from pydantic import Field

from inputlayer._sync import run_sync


class InputLayerTool(BaseTool):
    """Tool that lets a LangChain agent query an InputLayer KnowledgeGraph.

    The agent provides a Datalog query string as input, and receives
    the result rows formatted as text.

    Usage::

        tool = InputLayerTool(
            kg=kg,
            name="query_knowledge_graph",
            description="Query the knowledge graph using Datalog. "
                        "Input should be a valid Datalog query like: "
                        "?name, dept <- employee(name, dept, salary), salary > 100000",
        )
        agent = create_tool_calling_agent(llm, [tool], prompt)

    For constrained usage, provide a ``query_template`` with an ``{input}``
    placeholder — the agent's input is interpolated rather than used as
    raw Datalog::

        tool = InputLayerTool(
            kg=kg,
            name="search_docs",
            description="Search documents by topic",
            query_template='?Title, Content <- search("{input}", Title, Content)',
        )
    """

    name: str = "inputlayer_query"
    description: str = (
        "Query an InputLayer knowledge graph using Datalog. Input should be a Datalog query string."
    )
    kg: Any  # KnowledgeGraph — typed as Any for Pydantic compatibility
    query_template: str | None = Field(
        default=None,
        description="Optional Datalog template with {input} placeholder",
    )
    max_rows: int = Field(default=50, description="Maximum rows to return")

    model_config = {"arbitrary_types_allowed": True}  # noqa: RUF012

    # ── Sync path ────────────────────────────────────────────────────

    def _run(
        self,
        query: str,
        run_manager: CallbackManagerForToolRun | None = None,
    ) -> str:
        return run_sync(self._arun(query, run_manager=run_manager))

    # ── Async path (native) ──────────────────────────────────────────

    async def _arun(
        self,
        query: str,
        run_manager: AsyncCallbackManagerForToolRun | CallbackManagerForToolRun | None = None,
    ) -> str:
        datalog = self._build_query(query)
        result = await self.kg.execute(datalog)

        if not result.rows:
            return "No results found."

        rows = result.rows[: self.max_rows]
        lines = ["\t".join(str(v) for v in result.columns)]
        for row in rows:
            lines.append("\t".join(str(v) for v in row))

        summary = "\n".join(lines)
        if result.row_count > self.max_rows:
            summary += f"\n... ({result.row_count - self.max_rows} more rows)"
        return summary

    # ── Query building ───────────────────────────────────────────────

    def _build_query(self, user_input: str) -> str:
        if self.query_template is not None:
            return self.query_template.replace("{input}", user_input)
        return user_input
