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


# ── Example 10: Multi-agent with shared KG ───────────────────────────


async def example_multi_agent(kg):
    """Two LLM agents collaborate through a shared KG.

    Agent 1 (Researcher): extracts claims from articles.
    Agent 2 (Fact-checker): cross-references claims against known facts.
    Datalog rules automatically detect verified claims, contradictions,
    and novel (unverifiable) claims.

    The KG is the shared reasoning layer — agents don't talk to each
    other directly, they read/write facts and the rules do the rest.
    """
    header("Multi-agent with shared KG", 10)

    # ── Schema ───────────────────────────────────────────────────────

    await kg.execute(
        "+claim(id: int, source: string, subject: string, "
        "predicate: string, object: string, confidence: float)"
    )
    await kg.execute("+known_fact(subject: string, predicate: string, object: string)")

    # ── Rules: automatic fact-checking ───────────────────────────────

    # Verified: claim matches a known fact
    await kg.execute(
        "+verified(Id, Src, Subj, Pred, Obj) <- "
        "claim(Id, Src, Subj, Pred, Obj, Conf), "
        "known_fact(Subj, Pred, Obj)"
    )

    # Contradicted: claim conflicts with a known fact
    await kg.execute(
        "+contradicted(Id, Src, Subj, Pred, Claimed, Known) <- "
        "claim(Id, Src, Subj, Pred, Claimed, Conf), "
        "known_fact(Subj, Pred, Known), Claimed != Known"
    )

    subheader("Schema & rules defined")
    print(f"{DIM}  claim(id, source, subject, predicate, object, confidence){RESET}")
    print(f"{DIM}  known_fact(subject, predicate, object){RESET}")
    print(f"{DIM}  verified <- claim MATCHES known_fact{RESET}")
    print(f"{DIM}  contradicted <- claim CONFLICTS with known_fact{RESET}")

    # ── Seed known facts (ground truth) ──────────────────────────────

    known_facts = [
        ("Python", "created_by", "Guido van Rossum"),
        ("Python", "first_released", "1991"),
        ("Rust", "created_by", "Graydon Hoare"),
        ("Rust", "first_released", "2010"),
        ("JavaScript", "created_by", "Brendan Eich"),
        ("Linux", "created_by", "Linus Torvalds"),
        ("Git", "created_by", "Linus Torvalds"),
        ("PostgreSQL", "license", "BSD"),
        ("TensorFlow", "developed_by", "Google"),
        ("PyTorch", "developed_by", "Meta"),
    ]

    for subj, pred, obj in known_facts:
        escaped_obj = obj.replace('"', '\\"')
        await kg.execute(f'+known_fact("{subj}", "{pred}", "{escaped_obj}")')

    subheader("Step 1: Ground truth loaded")
    print(f"  {DIM}{len(known_facts)} known facts in the KG{RESET}")

    # ── Agent 1: Researcher — extract claims from an article ─────────

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")

    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
    except Exception:
        print(f"\n{DIM}  No LLM server — using hardcoded claims.{RESET}")
        await _insert_hardcoded_claims(kg)
        await _show_results(kg)
        return

    from langchain_openai import ChatOpenAI
    from pydantic import BaseModel
    from pydantic import Field as PydanticField

    llm = ChatOpenAI(base_url=base_url, api_key="lm-studio", model=model, temperature=0)

    article = (
        "Python was created by Guido van Rossum and first released in 1991. "
        "Rust was developed by Mozilla and released in 2015. "
        "JavaScript was created by Brendan Eich at Netscape in 1995. "
        "TensorFlow is an open-source ML framework developed by Google. "
        "PyTorch was created by OpenAI and is widely used for research. "
        "Git was invented by Linus Torvalds to manage the Linux kernel."
    )

    subheader("Step 2: Researcher agent extracts claims")
    print(f'{DIM}  Source: "{article[:70]}..."{RESET}\n')

    class Claim(BaseModel):
        subject: str = PydanticField(description="The entity")
        predicate: str = PydanticField(
            description="Relationship type (created_by, first_released, developed_by, license)"
        )
        object: str = PydanticField(description="The value")
        confidence: float = PydanticField(description="Confidence 0.0-1.0")

    class Claims(BaseModel):
        claims: list[Claim] = PydanticField(description="Extracted factual claims")

    structured_llm = llm.with_structured_output(Claims)
    extracted = structured_llm.invoke(
        "Extract factual claims from this text as structured data. "
        "Use predicates: created_by, first_released, developed_by. "
        "Rate your confidence for each claim.\n\n"
        f"Text: {article}"
    )

    for i, c in enumerate(extracted.claims):
        await kg.execute(
            f'+claim({i + 1}, "article", "{c.subject}", '
            f'"{c.predicate}", "{c.object}", {c.confidence})'
        )
        status = f"{DIM}conf={c.confidence}{RESET}"
        print(
            f"  {GREEN}{c.subject}{RESET} "
            f"{DIM}{c.predicate}{RESET} "
            f"{CYAN}{c.object}{RESET} {status}"
        )

    # ── Agent 2: Fact-checker — rules already fired ──────────────────

    subheader("Step 3: Fact-checker (Datalog rules — instant)")
    print(f"{DIM}  Rules fired automatically when claims were inserted{RESET}")

    await _show_results(kg)

    # ── Agent 3: LLM summarizes the findings ─────────────────────────

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate

    r_verified = await kg.execute("?verified(Id, Src, Subj, Pred, Obj)")
    r_contra = await kg.execute("?contradicted(Id, Src, Subj, Pred, Claimed, Known)")

    summary_parts = ["Verified claims:"]
    for row in r_verified.rows:
        summary_parts.append(f"  - {row[2]} {row[3]} {row[4]}")

    summary_parts.append("\nContradicted claims:")
    for row in r_contra.rows:
        summary_parts.append(f'  - {row[2]} {row[3]}: claimed "{row[4]}", known "{row[5]}"')

    context = "\n".join(summary_parts)

    prompt = ChatPromptTemplate.from_template(
        "You are a fact-checking editor. A researcher extracted claims "
        "from an article, and an automated system cross-referenced them "
        "against known facts.\n\n{context}\n\n"
        "Write a brief editorial note summarizing the accuracy of the "
        "article and highlighting any corrections needed."
    )

    chain = prompt | llm | StrOutputParser()

    subheader("Step 4: Editor agent summarizes findings")
    answer = await chain.ainvoke({"context": context})
    print(f"\n{GREEN}  {answer.strip()}{RESET}")


async def _insert_hardcoded_claims(kg):
    """Fallback when no LLM is available."""
    claims = [
        (1, "Python", "created_by", "Guido van Rossum", 0.95),
        (2, "Python", "first_released", "1991", 0.9),
        (3, "Rust", "created_by", "Mozilla", 0.8),
        (4, "Rust", "first_released", "2015", 0.7),
        (5, "JavaScript", "created_by", "Brendan Eich", 0.95),
        (6, "TensorFlow", "developed_by", "Google", 0.9),
        (7, "PyTorch", "developed_by", "OpenAI", 0.6),
        (8, "Git", "created_by", "Linus Torvalds", 0.95),
    ]

    subheader("Step 2: Researcher claims (hardcoded)")
    for cid, subj, pred, obj, conf in claims:
        await kg.execute(f'+claim({cid}, "article", "{subj}", "{pred}", "{obj}", {conf})')
        print(
            f"  {GREEN}{subj}{RESET} {DIM}{pred}{RESET} {CYAN}{obj}{RESET} {DIM}conf={conf}{RESET}"
        )

    subheader("Step 3: Fact-checker (Datalog rules — instant)")


async def _show_results(kg):
    """Display verified, contradicted, and novel claims."""
    r = await kg.execute("?verified(Id, Src, Subj, Pred, Obj)")
    print(f"\n  {GREEN}Verified ({len(r.rows)}):{RESET}")
    for row in r.rows:
        print(f"    {GREEN}+{RESET} {row[2]} {DIM}{row[3]}{RESET} {row[4]}")

    r = await kg.execute("?contradicted(Id, Src, Subj, Pred, Claimed, Known)")
    print(f"\n  {RED}Contradicted ({len(r.rows)}):{RESET}")
    for row in r.rows:
        print(
            f"    {RED}x{RESET} {row[2]} {DIM}{row[3]}{RESET}: "
            f'claimed {RED}"{row[4]}"{RESET}, '
            f'known {GREEN}"{row[5]}"{RESET}'
        )


# ── Example 11: Anomaly detection ────────────────────────────────────


async def example_anomaly_detection(kg):
    """Rule-based anomaly detection: Datalog rules define expected patterns,
    automatically flag violations, and the LLM explains them.

    No vector DB can do this — anomaly detection requires logical rules
    that compare values, check ranges, and cross-reference relationships.
    """
    header("Anomaly detection", 11)

    # ── Schema ───────────────────────────────────────────────────────

    await kg.execute(
        "+staff(name: string, role: string, salary: int, dept: string, hire_year: int)"
    )
    await kg.execute("+salary_band(role: string, min_sal: int, max_sal: int)")
    await kg.execute("+dept_budget(dept: string, budget: int)")
    await kg.execute("+dept_headcount_limit(dept: string, max_hc: int)")

    # ── Salary bands (expected ranges per role) ──────────────────────

    bands = [
        ("intern", 40000, 65000),
        ("junior_engineer", 70000, 110000),
        ("senior_engineer", 110000, 180000),
        ("staff_engineer", 170000, 250000),
        ("manager", 130000, 220000),
        ("director", 200000, 350000),
        ("vp", 300000, 500000),
    ]

    for role, mn, mx in bands:
        await kg.execute(f'+salary_band("{role}", {mn}, {mx})')

    # ── Department constraints ───────────────────────────────────────

    await kg.execute(
        '+dept_budget[("engineering", 2000000), ("product", 800000), ("data", 600000)]'
    )
    await kg.execute('+dept_headcount_limit[("engineering", 8), ("product", 4), ("data", 3)]')

    # ── Employee data (with deliberate anomalies) ────────────────────

    employees = [
        # Normal employees
        ("alice", "senior_engineer", 160000, "engineering", 2020),
        ("bob", "junior_engineer", 95000, "engineering", 2022),
        ("carol", "manager", 185000, "product", 2019),
        ("dave", "senior_engineer", 145000, "engineering", 2021),
        ("eve", "intern", 55000, "engineering", 2024),
        # ANOMALIES:
        ("frank", "intern", 150000, "engineering", 2024),  # intern salary way too high
        ("grace", "director", 120000, "data", 2018),  # director underpaid
        ("henry", "senior_engineer", 170000, "data", 2020),
        ("irene", "junior_engineer", 85000, "data", 2023),
        ("jack", "manager", 200000, "product", 2021),
        ("kate", "junior_engineer", 90000, "product", 2023),
        ("leo", "staff_engineer", 400000, "engineering", 2017),  # way over band
    ]

    for name, role, salary, dept, year in employees:
        await kg.execute(f'+staff("{name}", "{role}", {salary}, "{dept}", {year})')

    subheader("Data loaded")
    print(f"  {DIM}{len(employees)} employees, {len(bands)} salary bands{RESET}")
    print(f"  {DIM}3 departments with budget and headcount limits{RESET}")

    # ── Anomaly rules ────────────────────────────────────────────────

    # Rule 1: Salary above band maximum
    await kg.execute(
        "+overpaid(Name, Role, Salary, Max) <- "
        "staff(Name, Role, Salary, Dept, Year), "
        "salary_band(Role, Min, Max), Salary > Max"
    )

    # Rule 2: Salary below band minimum
    await kg.execute(
        "+underpaid(Name, Role, Salary, Min) <- "
        "staff(Name, Role, Salary, Dept, Year), "
        "salary_band(Role, Min, Max), Salary < Min"
    )

    subheader("Anomaly rules")
    print(f"  {DIM}overpaid(Name, Role, Salary, Max)")
    print(f"    <- staff salary > salary_band max{RESET}")
    print(f"  {DIM}underpaid(Name, Role, Salary, Min)")
    print(f"    <- staff salary < salary_band min{RESET}")

    # ── Step 1: Show detected anomalies ──────────────────────────────

    subheader("Step 1: Detected anomalies (instant — Datalog rules)")

    r = await kg.execute("?overpaid(Name, Role, Salary, Max)")
    print(f"\n  {RED}Overpaid ({len(r.rows)}):{RESET}")
    for row in r.rows:
        excess = row[2] - row[3]
        pct = (excess / row[3]) * 100
        print(
            f"    {RED}!{RESET} {GREEN}{row[0]}{RESET} ({row[1]}): "
            f"${row[2]:,} — {RED}${excess:,} over max "
            f"(+{pct:.0f}%){RESET}"
        )

    r = await kg.execute("?underpaid(Name, Role, Salary, Min)")
    print(f"\n  {YELLOW}Underpaid ({len(r.rows)}):{RESET}")
    for row in r.rows:
        deficit = row[3] - row[2]
        pct = (deficit / row[3]) * 100
        print(
            f"    {YELLOW}!{RESET} {GREEN}{row[0]}{RESET} ({row[1]}): "
            f"${row[2]:,} — {YELLOW}${deficit:,} under min "
            f"(-{pct:.0f}%){RESET}"
        )

    # ── Step 2: Show normal employees for contrast ───────────────────

    subheader("Step 2: Normal employees (within salary bands)")

    r_over = await kg.execute("?overpaid(Name, Role, Salary, Max)")
    r_under = await kg.execute("?underpaid(Name, Role, Salary, Min)")
    flagged = {row[0] for row in r_over.rows} | {row[0] for row in r_under.rows}

    print()
    for name, role, salary, dept, _year in employees:
        if name not in flagged:
            print(f"    {GREEN}ok{RESET} {name} ({role}): ${salary:,} {DIM}[{dept}]{RESET}")

    # ── Step 3: LLM explains anomalies ───────────────────────────────

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")

    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
    except Exception:
        print(f"\n{DIM}  No LLM server — skipping explanation.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_openai import ChatOpenAI

    llm = ChatOpenAI(base_url=base_url, api_key="lm-studio", model=model, temperature=0)

    # Build context from all anomalies
    context_parts = ["Salary band violations detected:\n"]

    r = await kg.execute("?overpaid(Name, Role, Salary, Max)")
    for row in r.rows:
        context_parts.append(
            f"- OVERPAID: {row[0]} ({row[1]}): ${row[2]:,} (max for role: ${row[3]:,})"
        )

    r = await kg.execute("?underpaid(Name, Role, Salary, Min)")
    for row in r.rows:
        context_parts.append(
            f"- UNDERPAID: {row[0]} ({row[1]}): ${row[2]:,} (min for role: ${row[3]:,})"
        )

    context_parts.append("\nSalary bands:")
    for role, mn, mx in bands:
        context_parts.append(f"- {role}: ${mn:,} - ${mx:,}")

    context = "\n".join(context_parts)

    prompt = ChatPromptTemplate.from_template(
        "You are an HR analytics system. Datalog rules in a knowledge "
        "graph automatically detected the following salary anomalies "
        "by comparing employee data against defined salary bands.\n\n"
        "{context}\n\n"
        "For each anomaly, provide a brief assessment: is this likely "
        "a data error, a misclassification, or something that needs "
        "HR review? Be concise."
    )

    chain = prompt | llm | StrOutputParser()

    subheader("Step 3: LLM explains anomalies")
    answer = await chain.ainvoke({"context": context})
    print(f"\n{GREEN}  {answer.strip()}{RESET}")


# ── Example 12: Hallucination detection ──────────────────────────────


async def example_hallucination_detection(kg):
    """Ground LLM outputs against KG facts to detect hallucinations.

    The LLM generates an answer, we extract claims from it, then
    Datalog rules cross-reference each claim against known facts.
    Claims are classified as: grounded, hallucinated, or unverifiable.
    """
    header("Hallucination detection / answer grounding", 12)

    # ── Ground truth knowledge base ──────────────────────────────────

    await kg.execute("+ground_truth(subject: string, predicate: string, object: string)")

    facts = [
        ("Python", "created_by", "Guido van Rossum"),
        ("Python", "first_released", "1991"),
        ("Python", "typing", "dynamically typed"),
        ("Rust", "created_by", "Graydon Hoare"),
        ("Rust", "first_released", "2010"),
        ("Rust", "typing", "statically typed"),
        ("JavaScript", "created_by", "Brendan Eich"),
        ("JavaScript", "first_released", "1995"),
        ("Go", "created_by", "Rob Pike and Ken Thompson"),
        ("Go", "first_released", "2009"),
        ("TypeScript", "created_by", "Microsoft"),
        ("TypeScript", "superset_of", "JavaScript"),
        ("Linux", "created_by", "Linus Torvalds"),
        ("Git", "created_by", "Linus Torvalds"),
    ]

    for subj, pred, obj in facts:
        escaped = obj.replace('"', '\\"')
        await kg.execute(f'+ground_truth("{subj}", "{pred}", "{escaped}")')

    # ── Claims extracted from LLM output ─────────────────────────────

    await kg.execute("+llm_claim(id: int, subject: string, predicate: string, object: string)")

    # ── Verification rules ───────────────────────────────────────────

    # Grounded: claim exactly matches a known fact
    await kg.execute(
        "+grounded_claim(Id, Subj, Pred, Obj) <- "
        "llm_claim(Id, Subj, Pred, Obj), "
        "ground_truth(Subj, Pred, Obj)"
    )

    # Hallucinated: claim contradicts a known fact
    await kg.execute(
        "+hallucinated_claim(Id, Subj, Pred, Claimed, Actual) <- "
        "llm_claim(Id, Subj, Pred, Claimed), "
        "ground_truth(Subj, Pred, Actual), Claimed != Actual"
    )

    subheader("Setup")
    print(f"  {DIM}{len(facts)} ground truth facts loaded{RESET}")
    print(f"  {DIM}Rules: grounded_claim, hallucinated_claim{RESET}")

    # ── Step 1: Generate an LLM answer ───────────────────────────────

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")

    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
    except Exception:
        print(f"\n{DIM}  No LLM — using hardcoded claims.{RESET}")
        await _insert_hallucination_claims(kg)
        await _show_grounding_results(kg)
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_openai import ChatOpenAI
    from pydantic import BaseModel
    from pydantic import Field as PydanticField

    llm = ChatOpenAI(base_url=base_url, api_key="lm-studio", model=model, temperature=0.7)

    # Ask the LLM a question it might hallucinate on
    question = (
        "Tell me about the history of Python, Rust, and Go "
        "programming languages — who created them and when."
    )

    subheader("Step 1: LLM generates an answer")
    print(f'{DIM}  Question: "{question}"{RESET}')

    prompt = ChatPromptTemplate.from_template(
        "Answer this question concisely in 3-4 sentences: {question}"
    )
    chain = prompt | llm | StrOutputParser()
    llm_answer = await chain.ainvoke({"question": question})
    print(f"\n  {WHITE}{llm_answer.strip()}{RESET}")

    # ── Step 2: Extract claims from the answer ───────────────────────

    subheader("Step 2: Extract claims from the answer")

    class Claim(BaseModel):
        subject: str = PydanticField(description="Programming language")
        predicate: str = PydanticField(description="One of: created_by, first_released, typing")
        object: str = PydanticField(description="The claimed value")

    class ClaimList(BaseModel):
        claims: list[Claim] = PydanticField(description="Factual claims extracted from the text")

    extractor = llm.with_structured_output(ClaimList)
    extracted = extractor.invoke(
        "Extract factual claims about programming languages from "
        "this text. Use predicates: created_by, first_released.\n\n"
        f"Text: {llm_answer}"
    )

    for i, c in enumerate(extracted.claims):
        escaped_obj = c.object.replace('"', '\\"')
        await kg.execute(f'+llm_claim({i + 1}, "{c.subject}", "{c.predicate}", "{escaped_obj}")')
        print(f"  {DIM}claim {i + 1}:{RESET} {c.subject} {DIM}{c.predicate}{RESET} {c.object}")

    # ── Step 3: Datalog rules verify claims ──────────────────────────

    subheader("Step 3: Datalog rules verify claims (instant)")
    await _show_grounding_results(kg)

    # ── Step 4: Produce grounded summary ─────────────────────────────

    r_halluc = await kg.execute("?hallucinated_claim(Id, Subj, Pred, Claimed, Actual)")

    corrections = []
    for row in r_halluc.rows:
        corrections.append(f'- WRONG: {row[1]} {row[2]} "{row[3]}" → CORRECT: "{row[4]}"')

    if corrections:
        subheader("Step 4: LLM corrects its answer")

        fix_prompt = ChatPromptTemplate.from_template(
            "Your previous answer contained factual errors. "
            "Here are the corrections:\n\n{corrections}\n\n"
            "Original answer: {original}\n\n"
            "Rewrite the answer with the corrections applied. "
            "Be concise."
        )
        fix_chain = fix_prompt | llm | StrOutputParser()
        fixed = await fix_chain.ainvoke(
            {
                "corrections": "\n".join(corrections),
                "original": llm_answer,
            }
        )
        print(f"\n{GREEN}  {fixed.strip()}{RESET}")
    else:
        print(f"\n  {GREEN}All claims verified — no corrections needed.{RESET}")


async def _insert_hallucination_claims(kg):
    """Fallback claims when no LLM is available."""
    claims = [
        (1, "Python", "created_by", "Guido van Rossum"),  # correct
        (2, "Python", "first_released", "1989"),  # wrong (1991)
        (3, "Rust", "created_by", "Mozilla"),  # wrong (Graydon Hoare)
        (4, "Rust", "first_released", "2010"),  # correct
        (5, "Go", "created_by", "Google"),  # wrong (Rob Pike...)
        (6, "Go", "first_released", "2009"),  # correct
    ]

    subheader("Claims to verify (hardcoded)")
    for cid, subj, pred, obj in claims:
        await kg.execute(f'+llm_claim({cid}, "{subj}", "{pred}", "{obj}")')
        print(f"  {DIM}claim {cid}:{RESET} {subj} {DIM}{pred}{RESET} {obj}")

    subheader("Datalog rules verify claims (instant)")


async def _show_grounding_results(kg):
    """Display grounded vs hallucinated claims."""
    r = await kg.execute("?grounded_claim(Id, Subj, Pred, Obj)")
    print(f"\n  {GREEN}Grounded ({len(r.rows)}):{RESET}")
    for row in r.rows:
        print(f"    {GREEN}ok{RESET} {row[1]} {DIM}{row[2]}{RESET} {row[3]}")

    r = await kg.execute("?hallucinated_claim(Id, Subj, Pred, Claimed, Actual)")
    print(f"\n  {RED}Hallucinated ({len(r.rows)}):{RESET}")
    for row in r.rows:
        print(
            f"    {RED}!!{RESET} {row[1]} {DIM}{row[2]}{RESET}: "
            f'said {RED}"{row[3]}"{RESET}, '
            f'actually {GREEN}"{row[4]}"{RESET}'
        )

    # Count unverifiable (claims with no matching subject+predicate)
    r_all = await kg.execute("?llm_claim(Id, Subj, Pred, Obj)")
    r_grounded = await kg.execute("?grounded_claim(Id, S, P, O)")
    r_halluc = await kg.execute("?hallucinated_claim(Id, S, P, C, A)")
    verified_ids = {row[0] for row in r_grounded.rows} | {row[0] for row in r_halluc.rows}
    unverifiable = [row for row in r_all.rows if row[0] not in verified_ids]

    if unverifiable:
        print(f"\n  {YELLOW}Unverifiable ({len(unverifiable)}):{RESET}")
        for row in unverifiable:
            print(
                f"    {YELLOW}?{RESET} {row[1]} {DIM}{row[2]}{RESET} "
                f"{row[3]} {DIM}(no matching fact){RESET}"
            )


# ── Example 13: Guardrails / output safety ───────────────────────────


async def example_guardrails(kg):
    """Rule-based guardrails: Datalog rules define content policies,
    LLM output is checked against them before returning to the user.

    Policies are declarative facts — add/remove rules without code changes.
    The KG enforces them automatically via joins.
    """
    header("Guardrails / output safety", 13)

    # ── Policy definitions as facts ──────────────────────────────────

    await kg.execute("+blocked_topic(topic: string, reason: string, severity: string)")
    await kg.execute("+required_disclaimer(topic: string, disclaimer: string)")
    await kg.execute("+pii_pattern(pattern_name: string, example: string)")

    # Blocked topics
    blocked = [
        ("weapons_manufacturing", "Safety policy", "critical"),
        ("illegal_activity", "Legal compliance", "critical"),
        ("self_harm", "Safety policy", "critical"),
        ("competitor_disparagement", "Brand policy", "warning"),
        ("price_fixing", "Antitrust compliance", "critical"),
    ]

    for topic, reason, severity in blocked:
        await kg.execute(f'+blocked_topic("{topic}", "{reason}", "{severity}")')

    # Required disclaimers
    disclaimers = [
        ("medical", "This is not medical advice. Consult a doctor."),
        ("financial", "Not financial advice. Consult a professional."),
        ("legal", "Not legal advice. Consult a licensed attorney."),
    ]

    for topic, disc in disclaimers:
        escaped = disc.replace('"', '\\"')
        await kg.execute(f'+required_disclaimer("{topic}", "{escaped}")')

    # PII patterns
    pii = [
        ("ssn", "123-45-6789"),
        ("credit_card", "4111-1111-1111-1111"),
        ("email_address", "user@example.com"),
        ("phone_number", "+1-555-0123"),
    ]

    for name, example in pii:
        await kg.execute(f'+pii_pattern("{name}", "{example}")')

    # ── Content to check (simulating LLM outputs) ────────────────────

    await kg.execute("+output_content(id: int, text: string)")
    await kg.execute("+output_topic(content_id: int, topic: string)")
    await kg.execute("+output_contains_pii(content_id: int, pattern_name: string)")

    # Simulated LLM outputs with their detected topics/PII
    outputs = [
        (1, "Python is great for data science and ML."),
        (2, "You should take 500mg of ibuprofen daily."),
        (3, "Buy ACME stock now, it will double."),
        (4, "Here is how to pick a lock to enter a building."),
        (5, "Our competitor's product is terrible and overpriced."),
        (6, "Your account number is 4111-1111-1111-1111."),
        (7, "Consider consulting a financial advisor for this."),
    ]

    for oid, text in outputs:
        escaped = text.replace('"', '\\"')
        await kg.execute(f'+output_content({oid}, "{escaped}")')

    # Topic classifications (in production, the LLM classifies these)
    topic_tags = [
        (1, "programming"),
        (2, "medical"),
        (3, "financial"),
        (4, "illegal_activity"),
        (5, "competitor_disparagement"),
        (6, "customer_service"),
        (7, "financial"),
    ]

    for cid, topic in topic_tags:
        await kg.execute(f'+output_topic({cid}, "{topic}")')

    # PII detection
    await kg.execute('+output_contains_pii(6, "credit_card")')

    # ── Guardrail rules ──────────────────────────────────────────────

    # Rule: blocked content
    await kg.execute(
        "+policy_violation(ContentId, Topic, Reason, Severity) <- "
        "output_topic(ContentId, Topic), "
        "blocked_topic(Topic, Reason, Severity)"
    )

    # Rule: needs disclaimer
    await kg.execute(
        "+needs_disclaimer(ContentId, Topic, Disclaimer) <- "
        "output_topic(ContentId, Topic), "
        "required_disclaimer(Topic, Disclaimer)"
    )

    # Rule: PII leak
    await kg.execute(
        "+pii_violation(ContentId, Pattern, Example) <- "
        "output_contains_pii(ContentId, Pattern), "
        "pii_pattern(Pattern, Example)"
    )

    subheader("Policies loaded")
    print(f"  {DIM}{len(blocked)} blocked topics{RESET}")
    print(f"  {DIM}{len(disclaimers)} required disclaimers{RESET}")
    print(f"  {DIM}{len(pii)} PII patterns{RESET}")

    # ── Step 1: Check all outputs ────────────────────────────────────

    subheader("Step 1: Scan 7 LLM outputs against policies")

    # Show each output with its status
    r_violations = await kg.execute("?policy_violation(Id, Topic, Reason, Severity)")
    r_disclaimers = await kg.execute("?needs_disclaimer(Id, Topic, Disclaimer)")
    r_pii = await kg.execute("?pii_violation(Id, Pattern, Example)")

    violation_ids = {row[0] for row in r_violations.rows}
    disclaimer_ids = {row[0] for row in r_disclaimers.rows}
    pii_ids = {row[0] for row in r_pii.rows}

    print()
    for oid, text in outputs:
        if oid in violation_ids:
            violations = [r for r in r_violations.rows if r[0] == oid]
            for v in violations:
                sev_color = RED if v[3] == "critical" else YELLOW
                print(f'  {sev_color}BLOCKED{RESET} [{v[3]}] "{text[:60]}"')
                print(f"    {DIM}Reason: {v[2]} (topic: {v[1]}){RESET}")
        elif oid in pii_ids:
            pii_matches = [r for r in r_pii.rows if r[0] == oid]
            for p in pii_matches:
                print(f'  {RED}PII LEAK{RESET} "{text[:60]}"')
                print(f"    {DIM}Detected: {p[1]} pattern{RESET}")
        elif oid in disclaimer_ids:
            discs = [r for r in r_disclaimers.rows if r[0] == oid]
            for d in discs:
                print(f'  {YELLOW}DISCLAIMER{RESET} "{text[:60]}"')
                print(f"    {DIM}Append: {d[2]}{RESET}")
        else:
            print(f'  {GREEN}PASS{RESET} "{text[:60]}"')

    # ── Step 2: Summary stats ────────────────────────────────────────

    subheader("Step 2: Summary")
    total = len(outputs)
    blocked_count = len(violation_ids) + len(pii_ids)
    disclaimer_count = len(disclaimer_ids - violation_ids - pii_ids)
    passed = total - blocked_count - disclaimer_count
    print(f"\n  {GREEN}Passed: {passed}{RESET}")
    print(f"  {YELLOW}Need disclaimer: {disclaimer_count}{RESET}")
    print(f"  {RED}Blocked: {blocked_count}{RESET}")

    # ── Step 3: LLM rewrites blocked content safely ──────────────────

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")

    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
    except Exception:
        print(f"\n{DIM}  No LLM — skipping rewrite step.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_openai import ChatOpenAI

    llm = ChatOpenAI(base_url=base_url, api_key="lm-studio", model=model, temperature=0)

    # Pick a blocked output and ask the LLM to rewrite safely
    blocked_output = next((text for oid, text in outputs if oid in violation_ids), None)

    if blocked_output:
        subheader("Step 3: LLM rewrites blocked content safely")
        print(f'  {RED}Original: "{blocked_output}"{RESET}')

        prompt = ChatPromptTemplate.from_template(
            "The following response was blocked by a safety policy. "
            "Rewrite it to be helpful but safe — decline the "
            "specific request while offering a legitimate alternative.\n\n"
            "Blocked response: {blocked}\n\n"
            "Safe rewrite:"
        )
        chain = prompt | llm | StrOutputParser()
        safe = await chain.ainvoke({"blocked": blocked_output})
        print(f"  {GREEN}Rewrite: {safe.strip()}{RESET}")


# ── Example 14: GraphRAG ─────────────────────────────────────────────


async def example_graphrag(kg):
    """GraphRAG: build a knowledge graph from documents, compute entity
    communities via Datalog rules, then summarize communities for
    high-level questions.

    This is the Microsoft Research GraphRAG pattern implemented with
    InputLayer's Datalog engine instead of a custom graph pipeline.
    """
    header("GraphRAG — entity graph from documents", 14)

    # ── Schema ───────────────────────────────────────────────────────

    await kg.execute("+doc_chunk(id: int, source: string, text: string)")
    await kg.execute("+kg_entity(name: string, kind: string, doc_id: int)")
    await kg.execute("+kg_relationship(src: string, rel: string, dst: string, doc_id: int)")

    # ── Source documents ─────────────────────────────────────────────

    chunks = [
        (
            1,
            "ml_blog",
            "Transformers revolutionized NLP by introducing the attention "
            "mechanism. BERT, a transformer model, achieves state-of-the-art "
            "results on many NLP benchmarks. GPT uses transformers for "
            "generative text tasks.",
        ),
        (
            2,
            "dl_survey",
            "Deep learning encompasses CNNs for image recognition and "
            "transformers for sequence modeling. Transfer learning allows "
            "pre-trained models like BERT and GPT to be fine-tuned on "
            "downstream tasks.",
        ),
        (
            3,
            "infra_blog",
            "Apache Spark powers large-scale ETL pipelines. Data lakes "
            "store raw data for batch and stream processing. Spark "
            "integrates with Delta Lake for ACID transactions.",
        ),
        (
            4,
            "mlops_guide",
            "MLflow tracks ML experiments and model versions. Kubeflow "
            "orchestrates ML pipelines on Kubernetes. Both tools support "
            "model deployment and monitoring.",
        ),
    ]

    for cid, source, text in chunks:
        escaped = text.replace('"', '\\"')
        await kg.execute(f'+doc_chunk({cid}, "{source}", "{escaped}")')

    subheader("Step 1: Extract entities and relationships")

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")

    has_llm = False
    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
        has_llm = True
    except Exception:
        pass

    if has_llm:
        from langchain_openai import ChatOpenAI
        from pydantic import BaseModel
        from pydantic import Field as PydanticField

        llm = ChatOpenAI(
            base_url=base_url,
            api_key="lm-studio",
            model=model,
            temperature=0,
        )

        class Entity(BaseModel):
            name: str = PydanticField(description="Entity name (lowercase)")
            kind: str = PydanticField(
                description="Type: model, framework, technique, task, field, infrastructure"
            )

        class Relationship(BaseModel):
            src: str = PydanticField(description="Source entity")
            rel: str = PydanticField(
                description="Relationship: uses, is_a, applied_to, "
                "part_of, integrates_with, supports"
            )
            dst: str = PydanticField(description="Target entity")

        class Extraction(BaseModel):
            entities: list[Entity]
            relationships: list[Relationship]

        extractor = llm.with_structured_output(Extraction)

        all_entities: list[tuple[str, str, int]] = []
        all_rels: list[tuple[str, str, str, int]] = []

        for cid, source, text in chunks:
            extracted = extractor.invoke(
                "Extract entities and relationships from this text. "
                "Use lowercase names.\n\n"
                f"Text: {text}"
            )
            for e in extracted.entities:
                all_entities.append((e.name, e.kind, cid))
            for r in extracted.relationships:
                all_rels.append((r.src, r.rel, r.dst, cid))
            print(
                f"  {DIM}Doc {cid} ({source}):{RESET} "
                f"{len(extracted.entities)} entities, "
                f"{len(extracted.relationships)} rels"
            )

        for name, kind, doc_id in all_entities:
            n = name.replace('"', '\\"')
            await kg.execute(f'+kg_entity("{n}", "{kind}", {doc_id})')
        for src, rel, dst, doc_id in all_rels:
            s = src.replace('"', '\\"')
            d = dst.replace('"', '\\"')
            await kg.execute(f'+kg_relationship("{s}", "{rel}", "{d}", {doc_id})')
    else:
        # Hardcoded extraction fallback
        entities = [
            ("transformer", "model", 1),
            ("attention", "technique", 1),
            ("bert", "model", 1),
            ("gpt", "model", 1),
            ("nlp", "field", 1),
            ("deep_learning", "field", 2),
            ("cnn", "model", 2),
            ("image_recognition", "task", 2),
            ("transfer_learning", "technique", 2),
            ("bert", "model", 2),
            ("gpt", "model", 2),
            ("transformer", "model", 2),
            ("spark", "framework", 3),
            ("etl", "process", 3),
            ("data_lake", "infrastructure", 3),
            ("delta_lake", "infrastructure", 3),
            ("mlflow", "framework", 4),
            ("kubeflow", "framework", 4),
            ("kubernetes", "infrastructure", 4),
        ]
        rels = [
            ("transformer", "uses", "attention", 1),
            ("bert", "is_a", "transformer", 1),
            ("gpt", "is_a", "transformer", 1),
            ("bert", "applied_to", "nlp", 1),
            ("cnn", "applied_to", "image_recognition", 2),
            ("transformer", "part_of", "deep_learning", 2),
            ("transfer_learning", "uses", "bert", 2),
            ("transfer_learning", "uses", "gpt", 2),
            ("spark", "performs", "etl", 3),
            ("data_lake", "integrates_with", "spark", 3),
            ("delta_lake", "integrates_with", "spark", 3),
            ("kubeflow", "runs_on", "kubernetes", 4),
            ("mlflow", "supports", "kubeflow", 4),
        ]
        for name, kind, doc_id in entities:
            await kg.execute(f'+kg_entity("{name}", "{kind}", {doc_id})')
        for s, r, d, doc_id in rels:
            await kg.execute(f'+kg_relationship("{s}", "{r}", "{d}", {doc_id})')
        print(f"  {DIM}Hardcoded: {len(entities)} entities, {len(rels)} relationships{RESET}")

    # ── Community detection rules ────────────────────────────────────

    # Entities connected via relationships (undirected)
    await kg.execute("+connected_entity(A, B) <- kg_relationship(A, _, B, _)")
    await kg.execute("+connected_entity(A, B) <- kg_relationship(B, _, A, _)")

    # Transitive: A and C are in the same community if connected
    await kg.execute(
        "+same_cluster(A, C) <- connected_entity(A, B), connected_entity(B, C), A != C"
    )

    subheader("Step 2: Entity graph built")

    r = await kg.execute("?kg_entity(Name, Kind, DocId)")
    unique_entities = {row[0] for row in r.rows}
    r = await kg.execute("?kg_relationship(S, R, D, DocId)")
    print(f"  {DIM}{len(unique_entities)} unique entities{RESET}")
    print(f"  {DIM}{len(r.rows)} relationships{RESET}")

    # ── Show communities ─────────────────────────────────────────────

    subheader("Step 3: Communities (via transitive connectivity)")

    # Find clusters by checking what each entity is connected to
    r = await kg.execute("?connected_entity(A, B)")
    graph: dict[str, set[str]] = {}
    for row in r.rows:
        graph.setdefault(row[0], set()).add(row[1])
        graph.setdefault(row[1], set()).add(row[0])

    # Simple BFS community detection
    visited: set[str] = set()
    communities: list[set[str]] = []
    for node in sorted(graph.keys()):
        if node in visited:
            continue
        community: set[str] = set()
        queue = [node]
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            community.add(current)
            for neighbor in graph.get(current, set()):
                if neighbor not in visited:
                    queue.append(neighbor)
        if community:
            communities.append(community)

    # Also find isolated entities
    all_in_graph = set(graph.keys())
    r_all = await kg.execute("?kg_entity(Name, Kind, DocId)")
    all_entities_set = {row[0] for row in r_all.rows}
    isolated = all_entities_set - all_in_graph

    colors = [CYAN, MAGENTA, YELLOW, GREEN, BLUE]
    print()
    for i, community in enumerate(communities):
        color = colors[i % len(colors)]
        members = ", ".join(sorted(community))
        print(f"  {color}Community {i + 1}:{RESET} {members}")

    if isolated:
        print(f"  {DIM}Isolated: {', '.join(sorted(isolated))}{RESET}")

    # ── Step 4: Community summaries via LLM ──────────────────────────

    if not has_llm:
        print(f"\n{DIM}  No LLM — skipping community summaries.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate

    subheader("Step 4: LLM summarizes each community")

    prompt = ChatPromptTemplate.from_template(
        "You are a knowledge graph analyst. A community of related "
        "entities was discovered via graph analysis. Summarize what "
        "this community represents in one sentence.\n\n"
        "Entities: {entities}\n"
        "Relationships:\n{relationships}\n\n"
        "One-sentence summary:"
    )
    chain = prompt | llm | StrOutputParser()

    for i, community in enumerate(communities):
        # Get relationships within this community
        r = await kg.execute("?kg_relationship(S, R, D, DocId)")
        community_rels = [
            f"  {row[0]} —{row[1]}→ {row[2]}"
            for row in r.rows
            if row[0] in community or row[2] in community
        ]

        color = colors[i % len(colors)]
        members = ", ".join(sorted(community))
        summary = await chain.ainvoke(
            {
                "entities": members,
                "relationships": "\n".join(community_rels[:10]),
            }
        )
        print(f"\n  {color}Community {i + 1}{RESET} ({len(community)} entities):")
        print(f"  {DIM}Members: {members}{RESET}")
        print(f"  {GREEN}{summary.strip()}{RESET}")

    # ── Step 5: Answer a high-level question ─────────────────────────

    subheader("Step 5: High-level question over communities")

    community_summaries = []
    for i, community in enumerate(communities):
        members = ", ".join(sorted(community))
        community_summaries.append(f"Community {i + 1} ({members})")

    overview_prompt = ChatPromptTemplate.from_template(
        "Based on these entity communities discovered in a document "
        "corpus:\n\n{communities}\n\n"
        "Question: {question}\n\n"
        "Answer concisely based on the community structure."
    )
    overview_chain = overview_prompt | llm | StrOutputParser()

    question = "What are the main technical domains in this corpus?"
    print(f'{DIM}  Question: "{question}"{RESET}')

    answer = await overview_chain.ainvoke(
        {
            "communities": "\n".join(community_summaries),
            "question": question,
        }
    )
    print(f"\n{GREEN}  {answer.strip()}{RESET}")


# ── Example 15: Semantic caching ─────────────────────────────────────


async def example_semantic_caching(kg):
    """Semantic caching: cache LLM responses as KG facts.

    Rules match incoming queries to cached answers based on topic
    overlap. Exact matches return instantly; topic matches suggest
    relevant cached answers that may avoid an LLM call.
    """
    header("Semantic caching", 15)

    # ── Schema ───────────────────────────────────────────────────────

    await kg.execute("+llm_cache(question: string, answer: string, timestamp: int)")
    await kg.execute("+cache_topic(question: string, topic: string)")

    # ── Rules ────────────────────────────────────────────────────────

    # Exact cache hit
    await kg.execute("+cache_exact_hit(Question, Answer) <- llm_cache(Question, Answer, Ts)")

    # Topic-based match: incoming question shares a topic with cache
    await kg.execute(
        "+cache_topic_hit(IncomingQ, CachedQ, Answer, Topic) <- "
        "cache_topic(IncomingQ, Topic), "
        "cache_topic(CachedQ, Topic), "
        "llm_cache(CachedQ, Answer, Ts), "
        "IncomingQ != CachedQ"
    )

    subheader("Rules defined")
    print(f"{DIM}  cache_exact_hit <- direct question match{RESET}")
    print(f"{DIM}  cache_topic_hit <- same topic, different question{RESET}")

    # ── Seed the cache ───────────────────────────────────────────────

    cached = [
        ("What is Python?", "Python is a high-level programming language.", 1000),
        ("Explain transformers", "Transformers use self-attention for sequences.", 1001),
        ("What is Rust?", "Rust is a memory-safe systems language.", 1002),
        ("How does BERT work?", "BERT uses masked language modeling on transformers.", 1003),
        ("What is Docker?", "Docker containers package apps with dependencies.", 1004),
    ]

    topics = [
        ("What is Python?", "python"),
        ("What is Python?", "programming"),
        ("Explain transformers", "transformers"),
        ("Explain transformers", "nlp"),
        ("What is Rust?", "rust"),
        ("What is Rust?", "programming"),
        ("How does BERT work?", "bert"),
        ("How does BERT work?", "transformers"),
        ("How does BERT work?", "nlp"),
        ("What is Docker?", "docker"),
        ("What is Docker?", "devops"),
    ]

    for q, a, ts in cached:
        eq = q.replace('"', '\\"')
        ea = a.replace('"', '\\"')
        await kg.execute(f'+llm_cache("{eq}", "{ea}", {ts})')

    for q, t in topics:
        eq = q.replace('"', '\\"')
        await kg.execute(f'+cache_topic("{eq}", "{t}")')

    subheader("Step 1: Cache populated")
    print(f"  {DIM}{len(cached)} cached responses, {len(topics)} topic tags{RESET}")

    # ── Step 2: Test cache hits ──────────────────────────────────────

    subheader("Step 2: Query the cache")

    # Exact hit
    r = await kg.execute('?cache_exact_hit("What is Python?", Answer)')
    print(f'\n  {WHITE}Exact: "What is Python?"{RESET}')
    if r.rows:
        print(f"  {GREEN}HIT: {r.rows[0][1]}{RESET}")
    else:
        print(f"  {RED}MISS{RESET}")

    # Miss
    r = await kg.execute('?cache_exact_hit("What is Go?", Answer)')
    print(f'\n  {WHITE}Exact: "What is Go?"{RESET}')
    if r.rows:
        print(f"  {GREEN}HIT: {r.rows[0][1]}{RESET}")
    else:
        print(f"  {YELLOW}MISS — no exact match{RESET}")

    # Topic match — new question about transformers
    new_q = "How do transformers handle long sequences?"
    await kg.execute(f'+cache_topic("{new_q}", "transformers")')

    r = await kg.execute(f'?cache_topic_hit("{new_q}", CachedQ, Answer, Topic)')
    print(f'\n  {WHITE}Topic: "{new_q}"{RESET}')
    if r.rows:
        seen = set()
        for row in r.rows:
            cached_q = row[1]
            if cached_q not in seen:
                seen.add(cached_q)
                print(f'  {CYAN}RELATED:{RESET} "{cached_q}" {DIM}(topic: {row[3]}){RESET}')
                print(f"    {DIM}{row[2]}{RESET}")
    else:
        print(f"  {YELLOW}No related cache entries{RESET}")

    # ── Step 3: Show savings ─────────────────────────────────────────

    subheader("Step 3: Cache efficiency")

    queries = [
        "What is Python?",
        "Explain transformers",
        "What is Kubernetes?",
        "How does BERT work?",
        "What is Go?",
    ]

    hits = 0
    misses = 0
    for q in queries:
        eq = q.replace('"', '\\"')
        r = await kg.execute(f'?cache_exact_hit("{eq}", Answer)')
        if r.rows:
            hits += 1
            print(f'  {GREEN}HIT{RESET}  "{q}"')
        else:
            misses += 1
            print(f'  {RED}MISS{RESET} "{q}"')

    rate = (hits / len(queries)) * 100
    print(
        f"\n  {WHITE}Hit rate: {hits}/{len(queries)} ({rate:.0f}%) — saved {hits} LLM calls{RESET}"
    )


# ── Example 16: Recommendation engine ────────────────────────────────


async def example_recommendation(kg):
    """Collaborative filtering via Datalog rules.

    Users rate items. Rules derive similar users (shared high ratings)
    and recommendations (items liked by similar users). The LLM
    explains recommendations in natural language.
    """
    header("Recommendation engine", 16)

    # ── Schema ───────────────────────────────────────────────────────

    await kg.execute("+user_rating(user: string, item: string, score: int)")
    await kg.execute("+item_info(name: string, category: string)")

    # ── Data ─────────────────────────────────────────────────────────

    ratings = [
        ("alice", "python_masterclass", 5),
        ("alice", "rust_handbook", 4),
        ("alice", "ml_fundamentals", 5),
        ("alice", "deep_learning_101", 5),
        ("bob", "python_masterclass", 5),
        ("bob", "go_concurrency", 4),
        ("bob", "ml_fundamentals", 4),
        ("bob", "data_engineering", 5),
        ("carol", "rust_handbook", 5),
        ("carol", "systems_programming", 5),
        ("carol", "linux_internals", 4),
        ("dave", "ml_fundamentals", 5),
        ("dave", "deep_learning_101", 5),
        ("dave", "nlp_with_transformers", 5),
        ("dave", "python_masterclass", 4),
    ]

    items = [
        ("python_masterclass", "programming"),
        ("rust_handbook", "programming"),
        ("go_concurrency", "programming"),
        ("ml_fundamentals", "machine_learning"),
        ("deep_learning_101", "machine_learning"),
        ("nlp_with_transformers", "machine_learning"),
        ("data_engineering", "data"),
        ("systems_programming", "systems"),
        ("linux_internals", "systems"),
    ]

    for user, item, score in ratings:
        await kg.execute(f'+user_rating("{user}", "{item}", {score})')
    for name, cat in items:
        await kg.execute(f'+item_info("{name}", "{cat}")')

    # ── Rules ────────────────────────────────────────────────────────

    # Similar users: both rated the same item >= 4
    await kg.execute(
        "+similar_users(A, B, SharedItem) <- "
        "user_rating(A, SharedItem, ScoreA), "
        "user_rating(B, SharedItem, ScoreB), "
        "A != B, ScoreA >= 4, ScoreB >= 4"
    )

    # Recommend: items that similar users rated highly
    await kg.execute(
        "+raw_recommendation(TargetUser, Item, Via, Score) <- "
        "similar_users(TargetUser, Via, SharedItem), "
        "user_rating(Via, Item, Score), "
        "Score >= 4, TargetUser != Via"
    )

    subheader("Data loaded")
    print(f"  {DIM}{len(ratings)} ratings, {len(items)} items, 4 users{RESET}")

    # ── Step 1: Similar users ────────────────────────────────────────

    subheader("Step 1: Similar users (shared high ratings)")

    for user in ["alice", "bob", "carol", "dave"]:
        r = await kg.execute(f'?similar_users("{user}", Other, SharedItem)')
        others: dict[str, list[str]] = {}
        for row in r.rows:
            others.setdefault(row[1], []).append(row[2])
        if others:
            parts = []
            for other, shared in sorted(others.items()):
                parts.append(f"{other} ({', '.join(shared)})")
            print(f"  {GREEN}{user}{RESET}: {DIM}{'; '.join(parts)}{RESET}")

    # ── Step 2: Recommendations ──────────────────────────────────────

    subheader("Step 2: Recommendations for alice")

    r = await kg.execute('?raw_recommendation("alice", Item, Via, Score)')
    # Deduplicate and filter out items alice already rated
    r_alice = await kg.execute('?user_rating("alice", Item, Score)')
    alice_items = {row[0] for row in r_alice.rows}

    recs: dict[str, list[str]] = {}
    for row in r.rows:
        item, via = row[0], row[1]
        if item not in alice_items:
            recs.setdefault(item, []).append(via)

    print()
    for item, recommenders in sorted(recs.items()):
        r_info = await kg.execute(f'?item_info("{item}", Category)')
        cat = r_info.rows[0][1] if r_info.rows else "?"
        print(
            f"  {GREEN}{item}{RESET} {DIM}[{cat}]{RESET} — "
            f"recommended by {CYAN}{', '.join(sorted(set(recommenders)))}{RESET}"
        )

    # ── Step 3: LLM explains recommendations ─────────────────────────

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")

    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
    except Exception:
        print(f"\n{DIM}  No LLM — skipping explanation.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_openai import ChatOpenAI

    llm = ChatOpenAI(base_url=base_url, api_key="lm-studio", model=model, temperature=0)

    context_parts = ["Alice's ratings:"]
    for row in r_alice.rows:
        context_parts.append(f"  - {row[0]}: {row[1]}/5")

    context_parts.append("\nRecommendations (from collaborative filtering):")
    for item, recommenders in sorted(recs.items()):
        context_parts.append(f"  - {item} (recommended by {', '.join(sorted(set(recommenders)))})")

    prompt = ChatPromptTemplate.from_template(
        "You are a learning recommendation system. Based on "
        "collaborative filtering results from a knowledge graph, "
        "explain why these items are recommended for Alice.\n\n"
        "{context}\n\n"
        "Write a brief, personalized recommendation message."
    )

    chain = prompt | llm | StrOutputParser()

    subheader("Step 3: LLM explains recommendations")
    answer = await chain.ainvoke({"context": "\n".join(context_parts)})
    print(f"\n{GREEN}  {answer.strip()}{RESET}")


# ── Example 17: Data lineage ────────────────────────────────────────


async def example_data_lineage(kg):
    """Data lineage: track provenance of every derived fact.

    Source documents have reliability ratings. Facts are linked to their
    sources. Rules compute: multi-sourced facts (higher confidence),
    single-sourced facts (need verification), and reliability scores.
    The LLM generates an audit report.
    """
    header("Data lineage and provenance", 17)

    # ── Schema ───────────────────────────────────────────────────────

    await kg.execute(
        "+data_source(id: int, name: string, source_type: string, reliability: string)"
    )
    await kg.execute(
        "+sourced_claim(id: int, subject: string, predicate: string, "
        "object: string, source_id: int)"
    )

    # ── Sources with reliability ─────────────────────────────────────

    sources = [
        (1, "arxiv_2024_001", "paper", "high"),
        (2, "tech_blog_post", "blog", "medium"),
        (3, "twitter_thread", "social", "low"),
        (4, "official_docs", "documentation", "high"),
        (5, "arxiv_2024_002", "paper", "high"),
    ]

    for sid, name, stype, rel in sources:
        await kg.execute(f'+data_source({sid}, "{name}", "{stype}", "{rel}")')

    # ── Facts with source attribution ────────────────────────────────

    claims = [
        # Confirmed by multiple sources
        (1, "GPT-4", "architecture", "transformer", 1),
        (2, "GPT-4", "architecture", "transformer", 2),
        (3, "GPT-4", "architecture", "transformer", 4),
        # Single high-reliability source
        (4, "GPT-4", "context_window", "128k", 1),
        # Conflicting claims
        (5, "GPT-4", "parameters", "1.8T", 3),  # low reliability
        (6, "GPT-4", "parameters", "unknown", 4),  # high reliability
        # Single low-reliability source
        (7, "GPT-5", "release_date", "2025", 3),
    ]

    for cid, subj, pred, obj, src in claims:
        escaped_obj = obj.replace('"', '\\"')
        await kg.execute(f'+sourced_claim({cid}, "{subj}", "{pred}", "{escaped_obj}", {src})')

    # ── Lineage rules ────────────────────────────────────────────────

    # Fact with its source reliability
    await kg.execute(
        "+claim_reliability(ClaimId, Subj, Pred, Obj, SrcName, Reliability) <- "
        "sourced_claim(ClaimId, Subj, Pred, Obj, SrcId), "
        "data_source(SrcId, SrcName, SrcType, Reliability)"
    )

    # Multi-sourced: same fact from 2+ different sources
    await kg.execute(
        "+multi_sourced(Subj, Pred, Obj, SrcA, SrcB) <- "
        "sourced_claim(IdA, Subj, Pred, Obj, SrcA), "
        "sourced_claim(IdB, Subj, Pred, Obj, SrcB), "
        "SrcA != SrcB"
    )

    # Conflicting claims: same subject+predicate, different objects
    await kg.execute(
        "+conflict(Subj, Pred, ObjA, SrcA, ObjB, SrcB) <- "
        "sourced_claim(IdA, Subj, Pred, ObjA, SrcA), "
        "sourced_claim(IdB, Subj, Pred, ObjB, SrcB), "
        "ObjA != ObjB, SrcA != SrcB"
    )

    subheader("Data loaded")
    print(f"  {DIM}{len(sources)} sources, {len(claims)} sourced claims{RESET}")
    print(f"  {DIM}Rules: claim_reliability, multi_sourced, conflict{RESET}")

    # ── Step 1: Lineage view ─────────────────────────────────────────

    subheader("Step 1: Fact lineage (source attribution)")

    r = await kg.execute("?claim_reliability(Id, Subj, Pred, Obj, SrcName, Rel)")

    # Group by fact
    by_fact: dict[str, list[tuple[str, str]]] = {}
    for row in r.rows:
        key = f"{row[1]} {row[2]} {row[3]}"
        by_fact.setdefault(key, []).append((row[4], row[5]))

    print()
    for fact, sources_list in sorted(by_fact.items()):
        src_strs = []
        for src_name, rel in sources_list:
            rel_color = GREEN if rel == "high" else (YELLOW if rel == "medium" else RED)
            src_strs.append(f"{src_name} ({rel_color}{rel}{RESET})")
        print(f"  {WHITE}{fact}{RESET}")
        for s in src_strs:
            print(f"    {DIM}from{RESET} {s}")

    # ── Step 2: Multi-sourced facts ──────────────────────────────────

    subheader("Step 2: Multi-sourced facts (high confidence)")

    r = await kg.execute("?multi_sourced(Subj, Pred, Obj, SrcA, SrcB)")
    seen: set[str] = set()
    print()
    for row in r.rows:
        key = f"{row[0]} {row[1]} {row[2]}"
        if key not in seen:
            seen.add(key)
            print(
                f"  {GREEN}confirmed{RESET} {WHITE}{key}{RESET} {DIM}({row[3]} + {row[4]}){RESET}"
            )

    # ── Step 3: Conflicts ────────────────────────────────────────────

    subheader("Step 3: Conflicting claims")

    r = await kg.execute("?conflict(Subj, Pred, ObjA, SrcA, ObjB, SrcB)")
    seen_conflicts: set[str] = set()
    print()
    for row in r.rows:
        key = f"{row[0]}.{row[1]}"
        pair = tuple(sorted([str(row[2]), str(row[4])]))
        conflict_key = f"{key}.{pair}"
        if conflict_key not in seen_conflicts:
            seen_conflicts.add(conflict_key)
            print(f"  {RED}conflict{RESET} {WHITE}{row[0]} {row[1]}{RESET}")
            print(
                f'    source {row[3]}: {YELLOW}"{row[2]}"{RESET} vs '
                f'source {row[5]}: {YELLOW}"{row[4]}"{RESET}'
            )

    # ── Step 4: LLM audit report ─────────────────────────────────────

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")

    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
    except Exception:
        print(f"\n{DIM}  No LLM — skipping audit report.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_openai import ChatOpenAI

    llm = ChatOpenAI(base_url=base_url, api_key="lm-studio", model=model, temperature=0)

    # Build audit context
    context_parts = ["Data lineage audit:\n"]
    context_parts.append("Multi-sourced (confirmed):")
    for fact in sorted(seen):
        context_parts.append(f"  - {fact}")

    context_parts.append("\nConflicting claims:")
    r = await kg.execute("?conflict(Subj, Pred, ObjA, SrcA, ObjB, SrcB)")
    seen_c: set[str] = set()
    for row in r.rows:
        pair = tuple(sorted([str(row[2]), str(row[4])]))
        key = f"{row[0]}.{row[1]}.{pair}"
        if key not in seen_c:
            seen_c.add(key)
            context_parts.append(
                f'  - {row[0]} {row[1]}: src {row[3]} says "{row[2]}", src {row[5]} says "{row[4]}"'
            )

    context_parts.append("\nSingle low-reliability claims:")
    context_parts.append("  - GPT-5 release_date 2025 (from twitter_thread, low)")

    prompt = ChatPromptTemplate.from_template(
        "You are a data quality auditor. Based on the lineage analysis "
        "from a knowledge graph, write a brief audit summary. Flag "
        "risks and recommend actions.\n\n"
        "{context}\n\n"
        "Audit summary (3-5 bullet points):"
    )
    chain = prompt | llm | StrOutputParser()

    subheader("Step 4: LLM audit report")
    answer = await chain.ainvoke({"context": "\n".join(context_parts)})
    print(f"\n{GREEN}  {answer.strip()}{RESET}")


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
        await example_multi_agent(kg)
        await example_anomaly_detection(kg)
        await example_hallucination_detection(kg)
        await example_guardrails(kg)
        await example_graphrag(kg)
        await example_semantic_caching(kg)
        await example_recommendation(kg)
        await example_data_lineage(kg)

        # Cleanup
        await il.drop_knowledge_graph("langchain_demo")
        success("Done! Knowledge graph cleaned up.")


if __name__ == "__main__":
    asyncio.run(main())
