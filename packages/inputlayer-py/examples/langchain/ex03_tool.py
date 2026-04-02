"""Tool for agent queries."""

import asyncio

from examples.langchain._common import *


async def run(kg):
    """Use InputLayerTool to let an agent query the KG."""
    header("Tool for agent queries", 3)

    general_tool = InputLayerTool(
        kg=kg,
        name="query_articles",
        description=(
            "Query the articles knowledge graph using Datalog. "
            "Available relations: article(id, title, content, category, embedding), "
            "user_interest(user, category). "
            "Example: ?article(Id, Title, Content, Category, Emb)"
        ),
    )

    search_tool = InputLayerTool(
        kg=kg,
        name="search_by_category",
        description="Search articles by category. Input should be a category name.",
        query_template='?article(Id, Title, Content, "{input}", Emb)',
    )

    subheader("General tool — all articles:")
    print(f"{DIM}  > ?article(Id, Title, Content, Category, Emb){RESET}\n")
    result = await general_tool.ainvoke("?article(Id, Title, Content, Category, Emb)")
    tool_table(result)

    subheader("Search tool — articles in 'ml' category:")
    print(f'{DIM}  > search_by_category("ml"){RESET}\n')
    result = await search_tool.ainvoke("ml")
    tool_table(result)


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
