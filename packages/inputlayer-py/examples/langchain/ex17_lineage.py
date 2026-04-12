"""Data lineage and provenance."""

import asyncio

from examples.langchain._common import *
from inputlayer.integrations.langchain.params import iql_literal


async def run(kg):
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
        await kg.execute(f"+data_source({sid}, {iql_literal(name)}, {iql_literal(stype)}, {iql_literal(rel)})")

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
        await kg.execute(f"+sourced_claim({cid}, {iql_literal(subj)}, {iql_literal(pred)}, {iql_literal(obj)}, {src})")

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

    if not check_llm():
        print(f"\n{DIM}  No LLM — skipping audit report.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate

    llm = get_llm()

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


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
