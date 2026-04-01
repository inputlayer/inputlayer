"""LangChain integration: retriever + tool with InputLayer.

This example shows three patterns:

1. InputLayerRetriever with raw Datalog in an LCEL chain
2. InputLayerRetriever with vector search
3. InputLayerTool for agent-based KG queries

Prerequisites:
    pip install inputlayer-client-dev[langchain]
    pip install langchain-anthropic  # or langchain-openai, etc.

    # Start the InputLayer server
    inputlayer-server
"""

import asyncio
import os

from inputlayer import (
    Derived,
    From,
    HnswIndex,
    InputLayer,
    Relation,
    Vector,
)
from inputlayer.integrations.langchain import InputLayerRetriever, InputLayerTool

# ── ANSI colors ──────────────────────────────────────────────────────

BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
MAGENTA = "\033[35m"
BLUE = "\033[34m"
WHITE = "\033[97m"


def header(title: str, num: int) -> None:
    print()
    print(f"{CYAN}{BOLD}{'━' * 64}{RESET}")
    print(f"{CYAN}{BOLD}  Example {num}: {title}{RESET}")
    print(f"{CYAN}{BOLD}{'━' * 64}{RESET}")


def subheader(text: str) -> None:
    print(f"\n{WHITE}{BOLD}  {text}{RESET}")


def doc_row(
    title: str,
    content: str,
    tag: str = "",
    score: str | float = "",
) -> None:
    tag_str = f"{YELLOW}[{tag}]{RESET} " if tag else ""
    score_str = f" {DIM}(dist={score:.6f}){RESET}" if isinstance(score, float) else ""
    print(f"  {tag_str}{GREEN}{title}{RESET}{score_str}")
    print(f"  {DIM}{content[:90]}{RESET}")


def tool_table(text: str) -> None:
    """Pretty-print a tab-separated tool result as a table."""
    lines = text.strip().split("\n")
    if not lines:
        return

    rows = [line.split("\t") for line in lines]
    # Calculate column widths (cap at 40 chars)
    widths = [0] * len(rows[0])
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(widths):
                widths[i] = min(max(widths[i], len(cell)), 40)

    def fmt_row(row: list[str], color: str = "") -> str:
        cells = []
        for i, cell in enumerate(row):
            w = widths[i] if i < len(widths) else 10
            cells.append(cell[:w].ljust(w))
        return f"  {color}{'  '.join(cells)}{RESET}"

    # Header
    print(fmt_row(rows[0], f"{BOLD}{WHITE}"))
    print(f"  {DIM}{'  '.join('─' * w for w in widths)}{RESET}")
    # Data rows
    for row in rows[1:]:
        if row[0].startswith("..."):
            print(f"  {DIM}{row[0]}{RESET}")
        else:
            print(fmt_row(row))


def success(text: str) -> None:
    print(f"\n{GREEN}{BOLD}  {text}{RESET}\n")


# ── Schema definitions ───────────────────────────────────────────────


class Article(Relation):
    id: int
    title: str
    content: str
    category: str
    embedding: Vector


class UserInterest(Relation):
    user: str
    category: str


class RelevantArticle(Derived):
    """Articles relevant to a user — derived via Datalog rules."""

    title: str
    content: str
    category: str

    rules = [  # noqa: RUF012
        From(Article, UserInterest)
        .where(lambda a, u: a.category == u.category)
        .select(title=Article.title, content=Article.content, category=Article.category),
    ]


# ── Setup: populate the knowledge graph ──────────────────────────────


async def setup(il: InputLayer):
    kg = il.knowledge_graph("langchain_demo")

    print(f"\n{DIM}  Setting up knowledge graph...{RESET}")

    await kg.define(Article, UserInterest)

    await kg.create_index(
        HnswIndex(
            name="article_emb_idx",
            relation=Article,
            column="embedding",
            metric="cosine",
        )
    )

    await kg.insert(
        [
            Article(
                id=1,
                title="Introduction to Machine Learning",
                content="ML is a subset of AI that enables systems to learn from data.",
                category="ml",
                embedding=[0.1, 0.9, 0.0],
            ),
            Article(
                id=2,
                title="Deep Learning with Neural Networks",
                content="Deep learning uses multi-layer neural networks for complex tasks.",
                category="ml",
                embedding=[0.2, 0.8, 0.1],
            ),
            Article(
                id=3,
                title="Building REST APIs",
                content="REST APIs use HTTP methods to create, read, update, and delete resources.",
                category="web",
                embedding=[0.9, 0.1, 0.0],
            ),
            Article(
                id=4,
                title="Graph Databases Explained",
                content="Graph databases store data as nodes and edges, ideal for connected data.",
                category="db",
                embedding=[0.5, 0.5, 0.5],
            ),
            Article(
                id=5,
                title="Reinforcement Learning Basics",
                content="RL trains agents to make decisions by rewarding desired behaviors.",
                category="ml",
                embedding=[0.15, 0.85, 0.05],
            ),
        ]
    )

    await kg.insert(
        [
            UserInterest(user="alice", category="ml"),
            UserInterest(user="alice", category="db"),
            UserInterest(user="bob", category="web"),
        ]
    )

    await kg.define_rules(RelevantArticle)

    print(f"{DIM}  Inserted 5 articles, 3 user interests, 1 derived rule{RESET}")

    return kg


# ── Example 1: Retriever with Datalog query ─────────────────────────


async def example_retriever_datalog(kg):
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


# ── Example 2: Retriever with vector search ─────────────────────────


async def example_retriever_vector(kg):
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


# ── Example 3: Tool for agent queries ───────────────────────────────


async def example_tool(kg):
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


# ── Example 4: LCEL chain (requires an LLM) ─────────────────────────


async def example_lcel_chain(kg):
    """Full LCEL chain: retriever | prompt | llm | parser.

    Auto-detects LM Studio at localhost:1234. Set LLM_BASE_URL and LLM_MODEL
    env vars to override. Skips gracefully if no LLM is available.
    """
    header("LCEL chain with LLM", 4)

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")

    # Check if LM Studio (or compatible server) is running
    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
        print(f"\n{DIM}  LLM server detected at {base_url}{RESET}")
        print(f"{DIM}  Model: {model}{RESET}")
    except Exception:
        print(f"\n{DIM}  No LLM server detected at {base_url} — skipping.{RESET}")
        print(f"{DIM}  Start LM Studio and load a model, or set LLM_BASE_URL/LLM_MODEL.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_openai import ChatOpenAI

    llm = ChatOpenAI(
        base_url=base_url,
        api_key="lm-studio",
        model=model,
        temperature=0.7,
    )

    retriever = InputLayerRetriever(
        kg=kg,
        query='?article(Id, Title, Content, "{input}", Emb)',
        page_content_columns=["Content"],
        metadata_columns=["Title"],
    )

    prompt = ChatPromptTemplate.from_template(
        "You are a helpful assistant. Based on the following articles from a "
        "knowledge graph, answer the question concisely.\n\n"
        "Articles:\n{context}\n\n"
        "Question: What are the main topics covered in the {question} category?"
    )

    chain = {"context": retriever, "question": lambda x: x} | prompt | llm | StrOutputParser()

    question = "ml"
    print(f"\n{DIM}  Chain: retriever | prompt | llm | parser{RESET}")
    print(f'{DIM}  Question: "What are the main topics in the {question} category?"{RESET}')

    subheader("LLM Response:")
    answer = await chain.ainvoke(question)
    print(f"\n{GREEN}  {answer.strip()}{RESET}")


# ── Example 5: KG building — extract facts from documents ────────────


async def example_kg_building(kg):
    """Extract structured facts from text using an LLM and insert into the KG.

    This is the reverse of retrieval: Document → LLM → KG.
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


# ── Example 6: Explainable RAG with .why() ──────────────────────────


class Expertise(Relation):
    person: str
    skill: str


class TeamMember(Relation):
    person: str
    team: str


class TeamExpert(Derived):
    """People who are experts in their team's domain — derived via rules."""

    person: str
    team: str
    skill: str

    rules = [  # noqa: RUF012
        From(TeamMember, Expertise)
        .where(lambda tm, e: tm.person == e.person)
        .select(person=TeamMember.person, team=TeamMember.team, skill=Expertise.skill),
    ]


async def example_explainable_rag(kg):
    """Explainable RAG: retrieve results AND their proof trees.

    Uses .why() to get structured explanations of WHY each result was derived,
    then feeds both the results and the reasoning chain to the LLM.
    """
    header("Explainable RAG with .why()", 6)

    # ── Setup additional relations for this example ──────────────────

    await kg.define(Expertise, TeamMember)

    await kg.insert(
        [
            Expertise(person="alice", skill="machine-learning"),
            Expertise(person="alice", skill="python"),
            Expertise(person="bob", skill="rust"),
            Expertise(person="bob", skill="databases"),
            Expertise(person="carol", skill="machine-learning"),
            Expertise(person="carol", skill="deployment"),
            Expertise(person="dave", skill="javascript"),
            Expertise(person="dave", skill="react"),
        ]
    )

    await kg.insert(
        [
            TeamMember(person="alice", team="ml-platform"),
            TeamMember(person="bob", team="infrastructure"),
            TeamMember(person="carol", team="ml-platform"),
            TeamMember(person="dave", team="frontend"),
        ]
    )

    await kg.define_rules(TeamExpert)

    # ── Step 1: Query with .why() to get proof trees ─────────────────

    subheader("Step 1: Query with proof trees (.why)")

    raw_result = await kg._conn.execute(".why ?team_expert(Person, Team, Skill)")
    rows = raw_result.rows
    proof_trees = raw_result.proof_trees or []

    print(f"\n{DIM}  Found {len(rows)} team experts with proof trees{RESET}\n")

    for i, row in enumerate(rows):
        person, team, skill = row[0], row[1], row[2]
        print(f"  {GREEN}{person}{RESET} on {CYAN}{team}{RESET} — skill: {YELLOW}{skill}{RESET}")

        if i < len(proof_trees):
            tree = proof_trees[i]
            if "children" in tree:
                for child in tree["children"]:
                    rel = child.get("relation", "?")
                    vals = child.get("values", [])
                    print(f"    {DIM}because {rel}({', '.join(str(v) for v in vals)}){RESET}")

    # ── Step 2: Feed results + provenance to the LLM ────────────────

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")

    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
    except Exception:
        print(f"\n{DIM}  No LLM server detected — skipping LLM step.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_openai import ChatOpenAI

    llm = ChatOpenAI(base_url=base_url, api_key="lm-studio", model=model, temperature=0)

    # Build context with proof trees
    context_lines = []
    for i, row in enumerate(rows):
        person, team, skill = row[0], row[1], row[2]
        line = f"- {person} is a {skill} expert on team {team}"
        if i < len(proof_trees) and "children" in proof_trees[i]:
            reasons = []
            for child in proof_trees[i]["children"]:
                rel = child.get("relation", "")
                vals = child.get("values", [])
                reasons.append(f"{rel}({', '.join(str(v) for v in vals)})")
            line += f" [derived from: {'; '.join(reasons)}]"
        context_lines.append(line)

    context = "\n".join(context_lines)

    subheader("Step 2: LLM reasons over results + provenance")
    print(f"{DIM}  Context includes proof trees so the LLM can cite its reasoning{RESET}")

    prompt = ChatPromptTemplate.from_template(
        "You are analyzing a team's expertise based on a knowledge graph.\n"
        "Each result includes the derivation chain showing WHY it was concluded.\n\n"
        "Team experts and their derivations:\n{context}\n\n"
        "Question: {question}\n\n"
        "Answer concisely, citing the specific facts that support your answer."
    )

    chain = prompt | llm | StrOutputParser()

    question = "Who should lead a machine learning project, and why?"
    print(f'{DIM}  Question: "{question}"{RESET}')

    subheader("LLM Response (with provenance-backed reasoning):")
    answer = await chain.ainvoke({"context": context, "question": question})
    print(f"\n{GREEN}  {answer.strip()}{RESET}")

    # ── Step 3: .why_not — explain missing results ───────────────────

    subheader("Step 3: .why_not — why isn't dave an ML expert?")

    why_not_query = '.why_not team_expert("dave", "frontend", "machine-learning")'
    why_not_result = await kg._conn.execute(why_not_query)

    print()
    for row in why_not_result.rows:
        text = str(row[0])
        if text.startswith("  "):
            if "Blocker" in text:
                print(f"  {YELLOW}{text.strip()}{RESET}")
            else:
                print(f"  {DIM}{text.strip()}{RESET}")
        elif text:
            print(f"  {WHITE}{text}{RESET}")


# ── Main ─────────────────────────────────────────────────────────────


async def main():
    print(f"\n{BOLD}{BLUE}  InputLayer + LangChain Integration Demo{RESET}")

    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    ) as il:
        kg = await setup(il)

        await example_retriever_datalog(kg)
        await example_retriever_vector(kg)
        await example_tool(kg)
        await example_lcel_chain(kg)
        await example_kg_building(kg)
        await example_explainable_rag(kg)

        # Cleanup
        await il.drop_knowledge_graph("langchain_demo")
        success("Done! Knowledge graph cleaned up.")


if __name__ == "__main__":
    asyncio.run(main())
