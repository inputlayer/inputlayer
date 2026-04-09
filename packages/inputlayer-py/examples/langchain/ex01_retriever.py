"""Retriever with a parameterized IQL query."""

import asyncio

from examples.langchain._common import *


async def run(kg):
    """Use an IQL query with a safe :input placeholder.

    The query joins ``article`` and ``user_interest`` and filters by the
    user name passed at invoke time. The ``:input`` placeholder is
    escaped before substitution, so quotes and backslashes in the input
    cannot break out of the literal.
    """
    header("Retriever with parameterized IQL query", 1)

    print(f"\n{DIM}  Query: article JOIN user_interest, filtered by user (:input){RESET}")

    retriever = InputLayerRetriever(
        kg=kg,
        query=(
            "?article(I, T, C, Cat, E), user_interest(:input, Cat)"
        ),
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
