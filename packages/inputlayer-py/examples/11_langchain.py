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


# ── Example 7: Multi-hop reasoning ───────────────────────────────────


async def example_multihop_reasoning(kg):
    """Multi-hop graph reasoning: the KG computes transitive closure,
    the LLM interprets the results.

    Shows that complex graph traversal happens in Datalog rules,
    not in the LLM — the LLM just reads the derived conclusions.
    """
    header("Multi-hop reasoning", 7)

    # ── Setup: company org graph ─────────────────────────────────────

    await kg.execute("+reports_to(employee: string, manager: string)")
    await kg.execute("+collaborates(person_a: string, person_b: string)")
    await kg.execute("+employee_info(name: string, role: string, team: string)")

    # Org structure:
    #   CEO (grace)
    #    ├── VP Eng (henry)
    #    │    ├── alice (ml-platform)
    #    │    └── bob (infrastructure)
    #    └── VP Product (irene)
    #         ├── carol (ml-platform)
    #         └── dave (frontend)
    #   frank (intern, reports to alice)

    await kg.execute(
        '+employee_info[("grace", "CEO", "executive"), '
        '("henry", "VP Engineering", "engineering"), '
        '("irene", "VP Product", "product"), '
        '("alice", "Senior Engineer", "ml-platform"), '
        '("bob", "Senior Engineer", "infrastructure"), '
        '("carol", "Product Manager", "ml-platform"), '
        '("dave", "Frontend Lead", "frontend"), '
        '("frank", "Intern", "ml-platform")]'
    )

    await kg.execute(
        '+reports_to[("henry", "grace"), ("irene", "grace"), '
        '("alice", "henry"), ("bob", "henry"), '
        '("carol", "irene"), ("dave", "irene"), '
        '("frank", "alice")]'
    )

    await kg.execute(
        '+collaborates[("alice", "carol"), ("carol", "alice"), '
        '("bob", "dave"), ("dave", "bob"), '
        '("alice", "bob"), ("bob", "alice")]'
    )

    # ── Recursive rule: chain of command (transitive closure) ────────

    # Direct reports
    await kg.execute("+chain_of_command(E, M) <- reports_to(E, M)")
    # Transitive: if E reports to X and X is under M, then E is under M
    await kg.execute("+chain_of_command(E, M) <- reports_to(E, X), chain_of_command(X, M)")

    # Cross-team influence: people who collaborate AND share a chain of command
    await kg.execute(
        "+can_influence(A, B) <- collaborates(A, B), "
        "chain_of_command(A, Mgr), chain_of_command(B, Mgr)"
    )

    # ── Step 1: Show derived chain of command ────────────────────────

    subheader("Step 1: Chain of command (transitive closure)")
    print(f"{DIM}  Rules: reports_to(E, M) => chain_of_command(E, M){RESET}")
    print(f"{DIM}         reports_to(E, X), chain_of_command(X, M)")
    print(f"           => chain_of_command(E, M){RESET}\n")

    r = await kg.execute("?chain_of_command(E, M)")
    # Group by manager
    by_mgr: dict[str, list[str]] = {}
    for row in r.rows:
        by_mgr.setdefault(row[1], []).append(row[0])

    for mgr, reports in sorted(by_mgr.items()):
        names = ", ".join(sorted(reports))
        print(f"  {GREEN}{mgr}{RESET} has authority over: {CYAN}{names}{RESET}")

    # ── Step 2: Multi-hop proof — how does frank reach grace? ────────

    subheader("Step 2: Proof tree — how does frank report to grace?")

    raw = await kg._conn.execute('.why ?chain_of_command("frank", "grace")')
    trees = raw.proof_trees or []

    if trees:
        _print_proof_tree(trees[0], depth=0)

    # ── Step 3: Cross-team influence ─────────────────────────────────

    subheader("Step 3: Cross-team influence (collaboration + shared authority)")
    print(f"{DIM}  Rule: collaborates(A, B) AND shared manager => can_influence(A, B){RESET}\n")

    r = await kg.execute("?can_influence(A, B)")
    for row in r.rows:
        print(f"  {GREEN}{row[0]}{RESET} can influence {CYAN}{row[1]}{RESET}")

    # ── Step 4: LLM analyzes the org graph ───────────────────────────

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

    # Build context from derived facts
    chain_lines = []
    for mgr, reports in sorted(by_mgr.items()):
        chain_lines.append(f"- {mgr} has authority over: {', '.join(sorted(reports))}")

    influence_result = await kg.execute("?can_influence(A, B)")
    influence_lines = [f"- {r[0]} can influence {r[1]}" for r in influence_result.rows]

    employee_result = await kg.execute("?employee_info(Name, Role, Team)")
    emp_lines = [f"- {r[0]}: {r[1]} on {r[2]}" for r in employee_result.rows]

    context = (
        "Employees:\n" + "\n".join(emp_lines) + "\n\n"
        "Chain of command (derived via multi-hop reasoning):\n" + "\n".join(chain_lines) + "\n\n"
        "Cross-team influence (derived from collaboration + shared authority):\n"
        + "\n".join(influence_lines)
    )

    prompt = ChatPromptTemplate.from_template(
        "You are an organizational analyst. The following facts were derived "
        "from a knowledge graph using Datalog rules — the chain of command "
        "and influence relationships were computed automatically via "
        "transitive closure, not manually defined.\n\n"
        "{context}\n\n"
        "Question: {question}\n\n"
        "Answer concisely based on the derived facts."
    )

    chain = prompt | llm | StrOutputParser()

    question = (
        "If we need to staff a cross-functional ML project requiring "
        "both engineering and product input, who are the key people "
        "and what's the shortest approval chain?"
    )

    subheader("Step 4: LLM analyzes the org graph")
    print(f'{DIM}  Question: "{question}"{RESET}')

    subheader("LLM Response:")
    answer = await chain.ainvoke({"context": context, "question": question})
    print(f"\n{GREEN}  {answer.strip()}{RESET}")


def _print_proof_tree(tree: dict, depth: int) -> None:
    """Recursively print a proof tree with indentation."""
    indent = "    " * depth
    node_type = tree.get("node_type", "")

    if node_type == "base_fact":
        rel = tree.get("relation", "?")
        vals = tree.get("values", [])
        print(f"  {indent}{YELLOW}fact:{RESET} {rel}({', '.join(str(v) for v in vals)})")
    elif node_type == "rule_application":
        rule = tree.get("rule_name", "?")
        bindings = tree.get("bindings", [])
        bind_str = ", ".join(f"{b['variable']}={b['value']}" for b in bindings)
        clause = tree.get("clause_text", "")
        print(f"  {indent}{CYAN}rule:{RESET} {rule}({bind_str})")
        if clause:
            print(f"  {indent}{DIM}{clause}{RESET}")
        for child in tree.get("children", []):
            _print_proof_tree(child, depth + 1)
    else:
        # Generic: may have alternative proofs
        alt_count = tree.get("clause_text", "")
        if "alternative" in str(alt_count):
            print(f"  {indent}{DIM}{alt_count}{RESET}")
        for child in tree.get("children", []):
            _print_proof_tree(child, depth + 1)


# ── Example 8: Conversational memory as facts ────────────────────────


async def example_conversational_memory(kg):
    """Conversational memory backed by KG facts and Datalog rules.

    Each message turn is stored as a fact. The LLM extracts topics and
    entities per turn. Datalog rules derive active context, conversation
    threads, and relevant history — the LLM uses these derived facts
    to produce context-aware responses.

    This positions InputLayer as a structured memory backend, not just
    a vector store for chat history.
    """
    header("Conversational memory as facts", 8)

    # ── Schema for conversation memory ───────────────────────────────

    await kg.execute("+chat_message(id: int, role: string, content: string)")
    await kg.execute("+topic_mention(msg_id: int, topic: string)")
    await kg.execute("+entity_mention(msg_id: int, entity: string, kind: string)")

    # ── Rules that derive context from conversation history ──────────

    # Active topics: topics mentioned anywhere in the conversation
    await kg.execute("+active_topic(Topic) <- topic_mention(MsgId, Topic)")

    # Relevant messages: messages that mention an active topic
    await kg.execute(
        "+relevant_history(Id, Role, Content, Topic) <- "
        "chat_message(Id, Role, Content), topic_mention(Id, Topic)"
    )

    # Entity registry: all mentioned entities by type
    await kg.execute("+known_entity(Entity, Kind) <- entity_mention(MsgId, Entity, Kind)")

    # Cross-references: messages that share topics
    await kg.execute(
        "+related_turns(IdA, IdB, Topic) <- "
        "topic_mention(IdA, Topic), topic_mention(IdB, Topic), IdA != IdB"
    )

    subheader("Rules defined")
    print(f"{DIM}  active_topic(T) <- topic_mention(_, T){RESET}")
    print(f"{DIM}  relevant_history(Id, Role, Content, Topic)")
    print(f"    <- chat_message(Id, Role, Content), topic_mention(Id, Topic){RESET}")
    print(f"{DIM}  known_entity(E, Kind) <- entity_mention(_, E, Kind){RESET}")
    print(f"{DIM}  related_turns(A, B, Topic)")
    print(f"    <- topic_mention(A, Topic), topic_mention(B, Topic), A != B{RESET}")

    # ── Simulate a multi-turn conversation ───────────────────────────

    conversation = [
        (1, "user", "I need help optimizing our ML training pipeline."),
        (
            2,
            "assistant",
            "I can help with that. Are you seeing issues with "
            "data loading, model training, or GPU utilization?",
        ),
        (
            3,
            "user",
            "Mainly GPU utilization. We're using 4 A100s but only seeing 60% usage.",
        ),
        (
            4,
            "assistant",
            "Low GPU utilization often comes from data loading "
            "bottlenecks or small batch sizes. What framework?",
        ),
        (
            5,
            "user",
            "PyTorch with a custom DataLoader. We also noticed the loss plateauing after epoch 50.",
        ),
        (
            6,
            "assistant",
            "The loss plateau suggests a learning rate issue. "
            "Try cosine annealing. For GPU, check DataLoader workers.",
        ),
        (
            7,
            "user",
            "Good point. Can we also use mixed precision training to speed things up?",
        ),
    ]

    # Topics and entities extracted per turn (in production, the LLM does this)
    turn_topics = {
        1: ["ml-pipeline", "optimization"],
        3: ["gpu-utilization", "a100", "hardware"],
        4: ["data-loading", "batch-size", "gpu-utilization"],
        5: ["pytorch", "dataloader", "loss-plateau", "training"],
        6: ["learning-rate", "cosine-annealing", "dataloader", "gpu-utilization"],
        7: ["mixed-precision", "performance"],
    }

    turn_entities = {
        1: [("ml-pipeline", "system")],
        3: [("a100", "hardware"), ("gpu-cluster", "infrastructure")],
        5: [("pytorch", "framework"), ("dataloader", "component")],
        6: [("cosine-annealing", "technique"), ("dataloader", "component")],
        7: [("mixed-precision", "technique")],
    }

    # ── Step 1: Insert conversation turns as facts ───────────────────

    subheader("Step 1: Insert conversation as facts")

    for msg_id, role, content in conversation:
        escaped = content.replace('"', '\\"')
        await kg.execute(f'+chat_message({msg_id}, "{role}", "{escaped}")')

    for msg_id, topics in turn_topics.items():
        for topic in topics:
            await kg.execute(f'+topic_mention({msg_id}, "{topic}")')

    for msg_id, entities in turn_entities.items():
        for entity, kind in entities:
            await kg.execute(f'+entity_mention({msg_id}, "{entity}", "{kind}")')

    print(f"\n  {GREEN}Inserted {len(conversation)} messages,")
    topic_count = sum(len(t) for t in turn_topics.values())
    entity_count = sum(len(e) for e in turn_entities.values())
    print(f"  {topic_count} topic mentions, {entity_count} entity mentions{RESET}")

    # ── Step 2: Query derived context ────────────────────────────────

    subheader("Step 2: Derived context (computed by Datalog rules)")

    r = await kg.execute("?active_topic(T)")
    topics = sorted(row[0] for row in r.rows)
    print(f"\n  {WHITE}Active topics:{RESET}")
    for t in topics:
        print(f"    {YELLOW}{t}{RESET}")

    r = await kg.execute("?known_entity(E, Kind)")
    print(f"\n  {WHITE}Known entities:{RESET}")
    for row in r.rows:
        print(f"    {GREEN}{row[0]}{RESET} {DIM}({row[1]}){RESET}")

    r = await kg.execute("?related_turns(A, B, Topic)")
    print(f"\n  {WHITE}Cross-referenced turns:{RESET}")
    shown = set()
    for row in r.rows:
        pair = (min(row[0], row[1]), max(row[0], row[1]), row[2])
        if pair not in shown:
            shown.add(pair)
            a, b, topic = pair
            print(f"    Turn {CYAN}{a}{RESET} <-> Turn {CYAN}{b}{RESET} via {YELLOW}{topic}{RESET}")

    # ── Step 3: Context-aware response using derived facts ───────────

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

    # Build context from derived facts (NOT raw message history)
    r = await kg.execute("?relevant_history(Id, Role, Content, Topic)")
    history_by_topic: dict[str, list[str]] = {}
    for row in r.rows:
        topic = row[3]
        content = f"[Turn {row[0]}, {row[1]}] {row[2]}"
        history_by_topic.setdefault(topic, []).append(content)

    # The user's latest question is about mixed precision
    # Rules automatically link it to related topics
    r = await kg.execute("?related_turns(7, OtherId, Topic)")
    related_topics = sorted({row[2] for row in r.rows})

    context_parts = [f"Current question (turn 7): {conversation[-1][2]}"]
    context_parts.append(f"\nTopics related to this question: {', '.join(related_topics)}")
    context_parts.append("\nRelevant prior discussion:")
    for topic in related_topics:
        if topic in history_by_topic:
            for line in history_by_topic[topic]:
                context_parts.append(f"  {line}")

    context_parts.append(f"\nKnown entities: {', '.join(topics)}")
    context = "\n".join(context_parts)

    subheader("Step 3: LLM responds using rule-derived context")
    print(f"{DIM}  Context assembled from Datalog-derived facts, not raw history{RESET}")

    prompt = ChatPromptTemplate.from_template(
        "You are a helpful ML engineering assistant. The following context was "
        "assembled from a knowledge graph that tracks conversation topics and "
        "entities across turns. Use it to give a relevant, contextual answer.\n\n"
        "{context}\n\n"
        "Provide a concise, helpful response to the user's latest question."
    )

    chain = prompt | llm | StrOutputParser()
    answer = await chain.ainvoke({"context": context})

    subheader("LLM Response (context from derived facts):")
    print(f"\n{GREEN}  {answer.strip()}{RESET}")


# ── Example 9: Access-controlled RAG ─────────────────────────────────

RED = "\033[31m"


async def example_access_controlled_rag(kg):
    """Access-controlled RAG: Datalog rules enforce document visibility.

    Documents have clearance levels. Users have roles. Rules compute
    which documents each user can see. The retriever automatically
    respects these rules — no application-level filtering needed.

    This is impossible with a plain vector store: you'd need to
    manually filter results after retrieval, risking data leaks.
    With InputLayer, access control IS the query.
    """
    header("Access-controlled RAG", 9)

    # ── Schema ───────────────────────────────────────────────────────

    await kg.execute("+classified_doc(id: int, title: string, content: string, clearance: string)")
    await kg.execute("+acl_user(username: string, role: string)")
    await kg.execute("+clearance_grants(role: string, level: string)")

    # ── Data: documents with clearance levels ────────────────────────

    docs = [
        (1, "Q4 Revenue Report", "Revenue grew 12% YoY to $4.2B.", "public"),
        (2, "Product Roadmap 2025", "Launch AI assistant in Q2, expand to APAC in Q3.", "internal"),
        (3, "Competitive Analysis", "Competitor X is 6 months behind on AI features.", "internal"),
        (4, "M&A Target List", "Evaluating acquisition of StartupY for $50M.", "confidential"),
        (5, "Board Compensation", "CEO base: $800K, stock: 2M shares.", "restricted"),
        (6, "Engineering Blog Draft", "How we scaled ML pipeline to 10x.", "public"),
        (7, "Security Audit Results", "3 critical vulns found in auth.", "restricted"),
        (8, "HR Investigation", "Complaint filed regarding team lead conduct.", "restricted"),
    ]

    for doc_id, title, content, clearance in docs:
        escaped_title = title.replace('"', '\\"')
        escaped_content = content.replace('"', '\\"')
        await kg.execute(
            f'+classified_doc({doc_id}, "{escaped_title}", "{escaped_content}", "{clearance}")'
        )

    # ── Users with roles ─────────────────────────────────────────────

    users = [
        ("alice", "executive"),
        ("bob", "engineer"),
        ("carol", "contractor"),
        ("dave", "board_member"),
    ]

    for username, role in users:
        await kg.execute(f'+acl_user("{username}", "{role}")')

    # ── Clearance hierarchy ──────────────────────────────────────────

    grants = [
        ("contractor", "public"),
        ("engineer", "public"),
        ("engineer", "internal"),
        ("executive", "public"),
        ("executive", "internal"),
        ("executive", "confidential"),
        ("board_member", "public"),
        ("board_member", "internal"),
        ("board_member", "confidential"),
        ("board_member", "restricted"),
    ]

    for role, level in grants:
        await kg.execute(f'+clearance_grants("{role}", "{level}")')

    # The access control query joins three relations in Datalog:
    #   classified_doc(Id, Title, Content, Clr),
    #   acl_user("<user>", Role),
    #   clearance_grants(Role, Clr)
    # No application-level filtering — access control IS the query.

    def _acl_query(username: str) -> str:
        return (
            f"?classified_doc(Id, Title, Content, Clr), "
            f'acl_user("{username}", Role), '
            f"clearance_grants(Role, Clr)"
        )

    subheader("Setup complete")
    print(f"  {DIM}8 documents across 4 clearance levels{RESET}")
    print(f"  {DIM}4 users: executive, engineer, contractor, board member{RESET}")
    print(f"{DIM}  Access query: classified_doc JOIN acl_user JOIN clearance_grants{RESET}")

    # ── Step 1: Show what each user can see ──────────────────────────

    subheader("Step 1: Document visibility per user")

    clearance_colors = {
        "public": GREEN,
        "internal": YELLOW,
        "confidential": MAGENTA,
        "restricted": RED,
    }

    for username, role in users:
        r = await kg.execute(_acl_query(username))
        count = len(r.rows)
        print(f"\n  {GREEN}{username}{RESET} ({role}) — {count} documents:")
        for row in r.rows:
            clr = row[3]
            color = clearance_colors.get(clr, DIM)
            print(f"    {color}[{clr}]{RESET} {row[1]}")

    # ── Step 2: Same query, different users, different results ───────

    subheader("Step 2: Same retriever, different users")
    print(
        f"{DIM}  The retriever query is identical — access control is in the Datalog rules{RESET}"
    )

    for username, _role in users:
        retriever = InputLayerRetriever(
            kg=kg,
            query=_acl_query(username),
            page_content_columns=["Content"],
            metadata_columns=["Title", "Clr"],
        )

        lc_docs = await retriever.ainvoke(username)
        blocked = len(docs) - len(lc_docs)
        print(
            f"\n  {GREEN}{username}{RESET}: {len(lc_docs)} visible, {RED}{blocked} blocked{RESET}"
        )
        for d in lc_docs:
            clr = d.metadata.get("Clr", "")
            color = clearance_colors.get(clr, DIM)
            print(f"    {color}[{clr}]{RESET} {d.metadata.get('Title', '')}")

    # ── Step 3: Prove access denial ─────────────────────────────────

    subheader("Step 3: Why can't bob see the M&A target list?")

    # Bob is an engineer — try to access a confidential doc
    r = await kg.execute('?clearance_grants("engineer", "confidential")')
    has_access = len(r.rows) > 0

    print(f"\n  {DIM}Query: does engineer role grant confidential access?{RESET}")
    if has_access:
        print(f"  {GREEN}Yes — clearance granted{RESET}")
    else:
        print(
            f'  {RED}No — clearance_grants("engineer", "confidential") has no matching facts{RESET}'
        )
        print(f"  {DIM}Engineer role only grants: public, internal{RESET}")
        print(f"  {YELLOW}The join query returns zero rows for bob + confidential docs{RESET}")

    # ── Step 4: LLM answers using only visible docs ──────────────────

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

    prompt = ChatPromptTemplate.from_template(
        "You are a corporate assistant. You can ONLY use the documents "
        "provided — do not hallucinate or reference information not shown.\n\n"
        "Visible documents for this user:\n{context}\n\n"
        "Question: {question}\n\n"
        "If the answer requires information not in the visible documents, "
        "say you don't have access to that information."
    )

    question = "What are our strategic plans and financial performance?"

    subheader("Step 4: LLM answers — same question, different access")

    for username, role in [("carol", "contractor"), ("alice", "executive")]:
        r = await kg.execute(_acl_query(username))
        context = "\n".join(f"- [{row[3]}] {row[1]}: {row[2]}" for row in r.rows)

        chain = prompt | llm | StrOutputParser()
        answer = await chain.ainvoke({"context": context, "question": question})

        print(f"\n  {GREEN}{username}{RESET} ({role}):")
        print(f"{GREEN}  {answer.strip()}{RESET}")


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
        await example_multihop_reasoning(kg)
        await example_conversational_memory(kg)
        await example_access_controlled_rag(kg)

        # Cleanup
        await il.drop_knowledge_graph("langchain_demo")
        success("Done! Knowledge graph cleaned up.")


if __name__ == "__main__":
    asyncio.run(main())
