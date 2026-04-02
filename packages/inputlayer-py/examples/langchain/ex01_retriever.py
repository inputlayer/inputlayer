"""Retriever with Datalog query."""

import asyncio

from examples.langchain._common import *


async def run(kg):
    """Use a raw Datalog query to retrieve documents."""
    header("Retriever with Datalog query", 1)

    print(f"\n{DIM}  Query: article JOIN user_interest on category, filtered by user{RESET}")

    retriever = InputLayerRetriever(
        kg=kg,
        query='?article(Id, Title, Content, Cat, Emb), user_interest("{input}", Cat)',
        page_content_columns=["content"],
        metadata_columns=["title", "category"],
    )

    docs = await retriever.ainvoke("alice")

    subheader(f"Articles relevant to alice ({len(docs)} found):")
    for doc in docs:
        doc_row(
            title=doc.metadata.get("title", ""),
            content=doc.page_content,
            tag=doc.metadata.get("category", ""),
        )


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
