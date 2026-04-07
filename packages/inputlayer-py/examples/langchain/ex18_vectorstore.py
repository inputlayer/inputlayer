"""InputLayerVectorStore: drop-in LangChain VectorStore backed by InputLayer.

Demonstrates the standard LangChain VectorStore interface — the same one
Chroma, Pinecone, Weaviate, FAISS, etc. implement. This means any
existing LangChain RAG tutorial works with InputLayer just by changing
the import.

Steps shown:
1. from_texts — bulk-load documents
2. similarity_search — query by text
3. similarity_search_with_score — get distances
4. get_by_ids / delete — manage documents
5. as_retriever — use as a retriever in an LCEL chain
"""

import asyncio

from examples.langchain._common import *

from inputlayer.integrations.langchain import InputLayerVectorStore


async def run(kg) -> None:
    header("InputLayerVectorStore (LangChain interface)", 18)

    if not check_llm():
        print(f"\n  {YELLOW}LLM server not available — using fake embeddings{RESET}")

        # Tiny deterministic embedding for offline runs
        from langchain_core.embeddings import Embeddings

        class FakeEmbeddings(Embeddings):
            def embed_documents(self, texts):
                return [self._embed(t) for t in texts]

            def embed_query(self, text):
                return self._embed(text)

            def _embed(self, text):
                t = text.lower()
                return [
                    1.0 if "ml" in t or "learning" in t else 0.0,
                    1.0 if "rust" in t or "language" in t else 0.0,
                    1.0 if "data" in t or "database" in t else 0.0,
                ]

        embeddings = FakeEmbeddings()
    else:
        from langchain_openai import OpenAIEmbeddings

        embeddings = OpenAIEmbeddings(
            base_url=os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1"),
            api_key="lm-studio",
            model=os.environ.get("LLM_EMBED_MODEL", "text-embedding-nomic-embed-text-v1.5"),
            check_embedding_ctx_length=False,
        )

    # ── Step 1: from_texts — bulk load ───────────────────────────────

    subheader("Step 1: from_texts — bulk load documents")

    texts = [
        "Machine learning is a subset of AI focused on learning from data.",
        "Deep learning uses multi-layer neural networks for complex tasks.",
        "Rust is a systems programming language with memory safety.",
        "Python is a high-level programming language popular in ML.",
        "Vector databases store embeddings for similarity search.",
        "PostgreSQL is a relational database with JSON support.",
    ]

    metadatas = [
        {"category": "ml", "id": 1},
        {"category": "ml", "id": 2},
        {"category": "language", "id": 3},
        {"category": "language", "id": 4},
        {"category": "database", "id": 5},
        {"category": "database", "id": 6},
    ]

    store = await InputLayerVectorStore.afrom_texts(
        texts=texts,
        embedding=embeddings,
        metadatas=metadatas,
        kg=kg,
        collection_name="vector_demo",
    )

    print(f"  {DIM}Indexed {len(texts)} documents into 'vector_demo'{RESET}")

    # ── Step 2: similarity_search ────────────────────────────────────

    subheader("Step 2: similarity_search('machine learning', k=3)")

    results = await store.asimilarity_search("machine learning", k=3)
    for doc in results:
        cat = doc.metadata.get("category", "?")
        doc_row(
            title=doc.page_content[:60],
            content=f"category={cat}",
            tag=cat,
        )

    # ── Step 3: similarity_search_with_score ─────────────────────────

    subheader("Step 3: similarity_search_with_score (with distances)")

    scored = await store.asimilarity_search_with_score("Rust language", k=3)
    for doc, score in scored:
        cat = doc.metadata.get("category", "?")
        print(
            f"  {YELLOW}[{cat}]{RESET} {GREEN}{doc.page_content[:60]}{RESET} "
            f"{DIM}(dist={score:.4f}){RESET}"
        )

    # ── Step 4: as_retriever in an LCEL chain ────────────────────────

    subheader("Step 4: as_retriever — use in LCEL chains")
    print(f"  {DIM}retriever = store.as_retriever(search_kwargs={{'k': 2}}){RESET}")
    print(f"  {DIM}chain = retriever | prompt | llm | StrOutputParser(){RESET}")

    retriever = store.as_retriever(search_kwargs={"k": 2})
    docs = await retriever.ainvoke("vector storage")

    print(f"\n  {WHITE}Retrieved {len(docs)} documents:{RESET}")
    for doc in docs:
        cat = doc.metadata.get("category", "?")
        print(f"    {YELLOW}[{cat}]{RESET} {DIM}{doc.page_content[:60]}{RESET}")

    # ── Step 5: full LCEL chain (if LLM available) ───────────────────

    if check_llm():
        subheader("Step 5: Full LCEL chain — retriever → prompt → LLM")

        from langchain_core.output_parsers import StrOutputParser
        from langchain_core.prompts import ChatPromptTemplate

        llm = get_llm()
        prompt = ChatPromptTemplate.from_template(
            "Use these documents to answer briefly:\n\n{context}\n\nQuestion: {question}"
        )

        def format_docs(docs):
            return "\n\n".join(d.page_content for d in docs)

        chain = (
            {
                "context": retriever | format_docs,
                "question": lambda x: x,
            }
            | prompt
            | llm
            | StrOutputParser()
        )

        question = "What programming languages are mentioned and what are they used for?"
        print(f'  {DIM}Question: "{question}"{RESET}\n')

        answer = await chain.ainvoke(question)
        print(f"  {GREEN}{answer.strip()}{RESET}")

    # ── Cleanup ──────────────────────────────────────────────────────

    await store.adelete([d[0] for d in await _list_all_ids(kg)])
    success("Done!")


async def _list_all_ids(kg) -> list[tuple[str]]:
    """Best-effort: list all doc ids in the collection."""
    try:
        r = await kg.execute("?vector_demo(Id, Content, Metadata, Emb)")
        return [(row[-4],) for row in r.rows]
    except Exception:
        return []


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
