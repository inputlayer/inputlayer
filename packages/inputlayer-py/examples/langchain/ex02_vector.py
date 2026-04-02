"""Retriever with vector search."""

import asyncio

from examples.langchain._common import *


async def run(kg):
    """Use vector similarity search to retrieve documents."""
    header("Retriever with vector search", 2)

    print(f"\n{DIM}  Query: cosine similarity < 0.5 against ML-like vector{RESET}")

    retriever = InputLayerRetriever(
        kg=kg,
        query=(
            "?article(Id, Title, Content, Category, Emb), Dist = cosine(Emb, [{input}]), Dist < 0.5"
        ),
        page_content_columns=["Content"],
        metadata_columns=["Title", "Category"],
        score_column="Dist",
    )

    docs = await retriever.ainvoke("0.12, 0.88, 0.03")

    subheader(f"Similar to ML query vector ({len(docs)} found):")
    for doc in docs:
        doc_row(
            title=doc.metadata.get("Title", ""),
            content=doc.page_content,
            tag=doc.metadata.get("Category", ""),
            score=doc.metadata.get("score", ""),
        )


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
