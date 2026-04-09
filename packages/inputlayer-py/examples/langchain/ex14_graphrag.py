"""GraphRAG -- entity graph from documents."""

import asyncio

from examples.langchain._common import *


async def run(kg):
    """GraphRAG: build a knowledge graph from documents, compute entity
    communities via IQL rules, then summarize communities for
    high-level questions.

    This is the Microsoft Research GraphRAG pattern implemented with
    InputLayer's IQL engine instead of a custom graph pipeline.
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


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
