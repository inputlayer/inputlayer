"""RAG pipeline: vector + structured hybrid search."""

import asyncio

from inputlayer import HnswIndex, InputLayer, Relation, Vector


class Document(Relation):
    id: int
    title: str
    category: str
    embedding: Vector[3]


async def main():
    async with InputLayer("ws://localhost:8080/ws", username="admin", password="admin") as il:
        kg = il.knowledge_graph("rag")

        await kg.define(Document)

        # Create HNSW index
        await kg.create_index(HnswIndex(
            name="doc_emb_idx",
            relation=Document,
            column="embedding",
            metric="cosine",
        ))

        # Insert documents with embeddings
        await kg.insert([
            Document(id=1, title="Intro to ML", category="ml", embedding=[0.1, 0.9, 0.0]),
            Document(id=2, title="Deep Learning", category="ml", embedding=[0.2, 0.8, 0.1]),
            Document(id=3, title="Web Development", category="web", embedding=[0.9, 0.1, 0.0]),
            Document(id=4, title="Database Design", category="db", embedding=[0.5, 0.5, 0.5]),
        ])

        # Vector search: find 2 most similar documents to a query
        results = await kg.vector_search(Document, [0.15, 0.85, 0.05], k=2)
        print("Top 2 similar documents:")
        for doc in results:
            print(f"  {doc}")

        await il.drop_knowledge_graph("rag")


if __name__ == "__main__":
    asyncio.run(main())
