"""KG building -- extract facts from documents."""

import asyncio

from examples.langchain._common import *


async def run(kg):
    """Extract structured facts from text using an LLM and insert into the KG.

    This is the reverse of retrieval: Document -> LLM -> KG.
    The LLM extracts entities matching our Relation schema, then we insert them.
    """
    header("KG building — extract facts from documents", 5)

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")
    embed_model = os.environ.get("LLM_EMBED_MODEL", "text-embedding-nomic-embed-text-v1.5")

    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
    except Exception:
        print(f"\n{DIM}  No LLM server detected at {base_url} — skipping.{RESET}")
        return

    from langchain_openai import ChatOpenAI, OpenAIEmbeddings
    from pydantic import Field as PydanticField

    llm = ChatOpenAI(base_url=base_url, api_key="lm-studio", model=model, temperature=0)
    embeddings = OpenAIEmbeddings(
        base_url=base_url,
        api_key="lm-studio",
        model=embed_model,
        check_embedding_ctx_length=False,
    )

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

    # ── Step 2: Generate embeddings ──────────────────────────────────

    subheader("Step 2: Generate embeddings")
    print(f"{DIM}  Model: {embed_model}{RESET}\n")

    texts_to_embed = [f"{a.title}: {a.content}" for a in extracted.articles]
    vectors = embeddings.embed_documents(texts_to_embed)

    print(f"  {DIM}Generated {len(vectors)} vectors ({len(vectors[0])} dims each){RESET}")

    # ── Step 3: Insert into KG ───────────────────────────────────────

    subheader("Step 3: Insert into InputLayer KG")

    next_id = 100  # start IDs above the seed data
    new_articles = []
    for i, a in enumerate(extracted.articles):
        new_articles.append(
            Article(
                id=next_id + i,
                title=a.title,
                content=a.content,
                category=a.category,
                embedding=vectors[i][:3],  # truncate to 3 dims to match our schema
            )
        )

    await kg.insert(new_articles)
    print(f"\n  {GREEN}Inserted {len(new_articles)} new articles into the KG{RESET}")

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
