"""Structured tools generated from Relation schemas (no IQL for the agent)."""

import asyncio

from examples.langchain._common import *
from examples.langchain._common import Article, UserInterest


async def run(kg):
    """Generate one StructuredTool per relation.

    The LLM sees typed argument schemas - no IQL to hallucinate. Each
    call to a generated tool composes an IQL query with equality / range
    filters and dispatches it via ``kg.execute``.
    """
    header("Structured tools from Relations", 3)

    tools = tools_from_relations(kg, [Article, UserInterest])

    print(f"\n{DIM}  Generated {len(tools)} tools:{RESET}")
    for t in tools:
        fields = ", ".join(t.args_schema.model_fields.keys())
        print(f"  {CYAN}{t.name}{RESET}({DIM}{fields}{RESET})")

    article_tool, interest_tool = tools

    subheader("Filter articles by category='ml':")
    print(f"{DIM}  > search_article(category='ml'){RESET}\n")
    result = await article_tool.ainvoke({"category": "ml"})
    tool_table(result)

    subheader("Filter articles by id range:")
    print(f"{DIM}  > search_article(min_id=2, max_id=4){RESET}\n")
    result = await article_tool.ainvoke({"min_id": 2, "max_id": 4})
    tool_table(result)

    subheader("Look up alice's interests:")
    print(f"{DIM}  > search_user_interest(user='alice'){RESET}\n")
    result = await interest_tool.ainvoke({"user": "alice"})
    tool_table(result)


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
