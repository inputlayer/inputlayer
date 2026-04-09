"""Multi-agent with shared KG."""

import asyncio

from examples.langchain._common import *


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

    subheader("Step 3: Fact-checker (IQL rules — instant)")


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


async def run(kg):
    """Two LLM agents collaborate through a shared KG.

    Agent 1 (Researcher): extracts claims from articles.
    Agent 2 (Fact-checker): cross-references claims against known facts.
    IQL rules automatically detect verified claims, contradictions,
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

    subheader("Step 3: Fact-checker (IQL rules — instant)")
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


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
