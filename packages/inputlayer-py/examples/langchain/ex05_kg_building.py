"""KG building -- extract facts from documents."""

import asyncio

from examples.langchain._common import *


async def run(kg):
    """Extract structured facts from text using an LLM and insert into the KG.

    This is the reverse of retrieval: Document -> LLM -> KG.
    The LLM extracts entities matching our Relation schema, then we insert them.
    """
    header("KG building — extract facts from documents", 5)

    if not check_llm():
        print(f"\n{DIM}  No LLM server detected — skipping.{RESET}")
        return

    from pydantic import Field as PydanticField

    llm = get_llm()

    # ── Define extraction schema (Pydantic model for structured output) ──

    from pydantic import BaseModel

    class ExtractedArticle(BaseModel):
        title: str = PydanticField(description="Title of the article or topic")
        content: str = PydanticField(description="One-sentence summary")
        category: str = PydanticField(description="Category: ml, web, db, or other")

    class ExtractedArticles(BaseModel):
        articles: list[ExtractedArticle] = PydanticField(
            description="List of distinct topics extracted from the text"
        )

    # ── Source document ──────────────────────────────────────────────

    document = """
    Our engineering blog had a busy quarter. We published a deep dive into
    Natural Language Processing, explaining how transformer architectures
    revolutionized text understanding and generation. Another popular post
    covered microservice architecture patterns, specifically how to decompose
    monoliths into scalable, independently deployable services. Our data team
    wrote about vector databases and their growing role in AI applications —
    particularly how approximate nearest neighbor search enables real-time
    similarity matching at scale. Finally, we released a tutorial on
    reinforcement learning from human feedback (RLHF), the technique behind
    training modern language models to follow instructions.
    """

    print(f"\n{DIM}  Source document ({len(document.split())} words):{RESET}")
    print(f'{DIM}  "{document.strip()[:100]}..."{RESET}')

    # ── Step 1: Extract structured facts with LLM ────────────────────

    subheader("Step 1: LLM extracts structured facts")
    print(f"{DIM}  Model: {model}{RESET}\n")

    structured_llm = llm.with_structured_output(ExtractedArticles)
    extracted = structured_llm.invoke(
        "Extract distinct articles/topics from this text.\n"
        "For each topic, you MUST provide ALL three fields:\n"
        "- title: a short descriptive title\n"
        "- content: a one-sentence summary of what the topic covers (NEVER empty)\n"
        "- category: one of ml, web, or db\n\n"
        f"Text: {document}"
    )

    for a in extracted.articles:
        print(f"  {YELLOW}[{a.category}]{RESET} {GREEN}{a.title}{RESET}")
        print(f"  {DIM}{a.content}{RESET}")

    # ── Step 2: Persist via InputLayerVectorStore ────────────────────

    subheader("Step 2: Embed + insert via InputLayerVectorStore")
    print(f"{DIM}  Embedder: DemoEmbeddings (3-dim, deterministic){RESET}\n")

    # In real code:
    #   embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
    #   vs = InputLayerVectorStore(kg=kg, relation=Article, embeddings=embeddings)
    # The store handles embed -> insert in one call.
    vs = InputLayerVectorStore(
        kg=kg,
        relation=Article,
        embeddings=DemoEmbeddings(),
        ensure_schema=False,  # already defined by setup()
    )

    texts = [a.content for a in extracted.articles]
    metadatas = [{"title": a.title, "category": a.category} for a in extracted.articles]
    ids = [100 + i for i in range(len(extracted.articles))]  # int IDs (Article.id: int)
    inserted_ids = await vs.aadd_texts(texts=texts, metadatas=metadatas, ids=ids)
    print(f"  {GREEN}Persisted {len(inserted_ids)} documents through the vector store{RESET}")

    # ── Step 4: Verify — query the KG for the new articles ───────────

    subheader("Step 4: Verify — query new articles from KG")

    result = await kg.execute("?article(Id, Title, Content, Category, Emb), Id >= 100")
    print()
    for row in result.rows:
        art_id, title, content, category = row[0], row[1], row[2], row[3]
        print(f"  {YELLOW}[{category}]{RESET} {GREEN}id={art_id}{RESET} {title}")
        print(f"  {DIM}{content}{RESET}")


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
