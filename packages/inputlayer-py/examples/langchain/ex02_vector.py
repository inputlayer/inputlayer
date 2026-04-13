"""Retriever with vector search and a real Embeddings instance."""

import asyncio

from examples.langchain._common import *


async def run(kg):
    """Vector similarity search driven by a LangChain Embeddings instance.

    The retriever now accepts a natural-language query string and embeds
    it internally - same contract as every other LangChain retriever.
    """
    header("Retriever with vector search", 2)

    print(f"\n{DIM}  Embedding the query and running cosine top-k against Article{RESET}")

    retriever = InputLayerRetriever(
        kg=kg,
        relation=Article,
        embeddings=DemoEmbeddings(),  # swap in OpenAIEmbeddings() in real code
        k=3,
        metric="cosine",
        page_content_columns=["content"],
        metadata_columns=["title", "category"],
    )

    docs = await retriever.ainvoke("teach me about neural networks")

    subheader(f"Top-{len(docs)} similar articles:")
    for doc in docs:
        doc_row(
            title=doc.metadata.get("title", ""),
            content=doc.page_content,
            tag=doc.metadata.get("category", ""),
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
